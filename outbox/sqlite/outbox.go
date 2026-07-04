package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vercly/eh-sqlite/backoff"
	"github.com/vercly/eh-sqlite/context/sqlite"
	"github.com/vercly/eh-sqlite/internal/deadletter"
	"golang.org/x/sync/errgroup"

	jsoniter "github.com/json-iterator/go"
	_ "github.com/mattn/go-sqlite3"
	eh "github.com/vercly/eventhorizon"
	ehcodec "github.com/vercly/eventhorizon/codec/json"
	"github.com/vercly/eventhorizon/uuid"
)

var (
	// PeriodicSweepInterval Interval in which to do a sweep of various unprocessed events.
	PeriodicSweepInterval = 15 * time.Second

	// PeriodicSweepAge Settings for how old different kind of unprocessed events needs to be
	// to be processed by the periodic sweep.
	PeriodicSweepAge = 15 * time.Second
)

const metadataAvailableAtKey = "outbox.available_at"

type availableAtContextKey struct{}

// WithDelay returns a context that asks the outbox to make the event available
// after the provided delay. Non-positive delays are treated as immediate.
func WithDelay(ctx context.Context, delay time.Duration) context.Context {
	if delay <= 0 {
		return WithAvailableAt(ctx, time.Now())
	}
	return WithAvailableAt(ctx, time.Now().Add(delay))
}

// WithAvailableAt returns a context that asks the outbox to make the event
// available at the provided time. Event metadata key "outbox.available_at" takes
// precedence over this context value.
func WithAvailableAt(ctx context.Context, availableAt time.Time) context.Context {
	return context.WithValue(ctx, availableAtContextKey{}, availableAt)
}

// Outbox implements an eventhorizon.Outbox for SQLite.
type Outbox struct {
	db              *sql.DB
	outboxTable     string
	deadLetterTable string
	handlers        []*matcherHandler
	handlersByType  map[eh.EventHandlerType]*matcherHandler
	handlersMu      sync.RWMutex
	watchCh         chan *outboxDoc
	scheduleCh      chan struct{}
	errCh           chan error
	cctx            context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	started         atomic.Bool
	codec           eh.EventCodec
	maxRetries      int
	maxGoroutines   int
	retryBackoff    backoff.Config

	insertEventStmt      *sql.Stmt
	selectEventsStmt     *sql.Stmt
	updateTakenAtStmt    *sql.Stmt
	deleteEventStmt      *sql.Stmt
	updateHandlersStmt   *sql.Stmt
	scheduleRetryStmt    *sql.Stmt
	insertDeadLetterStmt *sql.Stmt
	nextAvailableAtStmt  *sql.Stmt
}

type matcherHandler struct {
	eh.EventMatcher
	eh.EventHandler
}

// NewOutbox creates a new Outbox.
func NewOutbox(db *sql.DB, options ...Option) (*Outbox, error) {
	ctx, cancel := context.WithCancel(context.Background())

	o := &Outbox{
		db:              db,
		outboxTable:     "outbox",
		deadLetterTable: "dead_letters",
		handlersByType:  map[eh.EventHandlerType]*matcherHandler{},
		watchCh:         make(chan *outboxDoc, 100),
		scheduleCh:      make(chan struct{}, 1),
		errCh:           make(chan error, 100),
		cctx:            ctx,
		cancel:          cancel,
		codec:           &ehcodec.EventCodec{},
		maxRetries:      10, // Default to 10
		maxGoroutines:   10, // Default to 10 concurrent HTTP handlers
		retryBackoff:    backoff.FixedConfig(10, PeriodicSweepAge),
	}

	for _, option := range options {
		if err := option(o); err != nil {
			return nil, fmt.Errorf("error while applying option: %w", err)
		}
	}

	// Create the outbox table if it doesn't exist.
	if _, err := o.db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %[1]s (
				id TEXT PRIMARY KEY,

				-- --- Promoted, Indexed Columns for Querying ---
				event_type TEXT NOT NULL,
				aggregate_id TEXT NOT NULL,
				created_at TIMESTAMP NOT NULL,
				available_at TIMESTAMP,
				taken_at TIMESTAMP,

				handlers TEXT NOT NULL, 

				-- --- Blob Column for the rest of the event data ---
				-- This will store a JSON object containing the full event,
				-- including data, metadata, version, etc.
				event_blob TEXT NOT NULL,
				retry_count INTEGER DEFAULT 0
		);

		-- Index the columns you will query.
		CREATE INDEX IF NOT EXISTS idx_%[1]s_created_at ON %[1]s (created_at);
		CREATE INDEX IF NOT EXISTS idx_%[1]s_taken_at ON %[1]s (taken_at);
	`, o.outboxTable)); err != nil {
		return nil, fmt.Errorf("could not create outbox table: %w", err)
	}

	if err := o.ensureSchema(); err != nil {
		return nil, err
	}

	if err := o.prepareStatements(); err != nil {
		return nil, fmt.Errorf("could not prepare statements: %w", err)
	}

	return o, nil
}

type Option func(*Outbox) error

func WithTableName(outbox string) Option {
	return func(o *Outbox) error {
		o.outboxTable = outbox
		return nil
	}
}

func (o *Outbox) ensureSchema() error {
	for _, stmt := range []string{
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN retry_count INTEGER DEFAULT 0;`, o.outboxTable),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN available_at TIMESTAMP;`, o.outboxTable),
	} {
		if _, err := o.db.Exec(stmt); err != nil && !isDuplicateColumnError(err) {
			return fmt.Errorf("could not migrate outbox table: %w", err)
		}
	}

	if _, err := o.db.Exec(fmt.Sprintf(`UPDATE %s SET available_at = created_at WHERE available_at IS NULL;`, o.outboxTable)); err != nil {
		return fmt.Errorf("could not backfill outbox available_at: %w", err)
	}
	if _, err := o.db.Exec(fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_taken_available ON %s (taken_at, available_at);`, o.outboxTable, o.outboxTable)); err != nil {
		return fmt.Errorf("could not create outbox availability index: %w", err)
	}

	if err := deadletter.EnsureSchema(o.db, o.deadLetterTable); err != nil {
		return err
	}

	return nil
}

func isDuplicateColumnError(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "duplicate column")
}

// WithMaxRetries sets the maximum number of error retries before an event is dropped.
func WithMaxRetries(retries int) Option {
	return func(o *Outbox) error {
		o.maxRetries = retries
		return nil
	}
}

// WithRetryBackoff sets the retry delay curve. The pattern accepts STD, EXP,
// PROG, FIXED, or comma-separated durations.
func WithRetryBackoff(pattern string) Option {
	return func(o *Outbox) error {
		cfg := backoff.ParseConfig(pattern)
		o.retryBackoff = cfg
		if cfg.MaxRetries > 0 {
			o.maxRetries = cfg.MaxRetries
		}
		return nil
	}
}

// WithMaxGoroutines limits the number of concurrent handlers spawned during a single outbox sweep.
func WithMaxGoroutines(max int) Option {
	return func(o *Outbox) error {
		o.maxGoroutines = max
		return nil
	}
}

func (o *Outbox) prepareStatements() (err error) {
	if o.insertEventStmt, err = o.db.Prepare(fmt.Sprintf(`
		INSERT INTO %s (id, event_type, aggregate_id, created_at, available_at, taken_at, handlers, event_blob)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, o.outboxTable)); err != nil {
		return fmt.Errorf("could not prepare insert event statement: %w", err)
	}

	if o.selectEventsStmt, err = o.db.Prepare(fmt.Sprintf(`
		SELECT id, event_type, aggregate_id, created_at, available_at, taken_at, handlers, event_blob, retry_count
		FROM %s
		WHERE (taken_at IS NULL OR taken_at < ?) AND available_at <= ?
		ORDER BY created_at ASC, id ASC LIMIT 50
	`, o.outboxTable)); err != nil {
		return fmt.Errorf("could not prepare select events statement: %w", err)
	}

	if o.updateTakenAtStmt, err = o.db.Prepare(fmt.Sprintf(`
		UPDATE %s SET taken_at = ? WHERE id = ?`, o.outboxTable)); err != nil {
		return fmt.Errorf("could not prepare update taken_at statement: %w", err)
	}

	if o.deleteEventStmt, err = o.db.Prepare(fmt.Sprintf(`DELETE FROM %s WHERE id = ?`, o.outboxTable)); err != nil {
		return fmt.Errorf("could not prepare delete event statement: %w", err)
	}

	if o.updateHandlersStmt, err = o.db.Prepare(fmt.Sprintf(`UPDATE %s SET handlers = ? WHERE id = ?`, o.outboxTable)); err != nil {
		return fmt.Errorf("could not prepare update handlers statement: %w", err)
	}

	if o.scheduleRetryStmt, err = o.db.Prepare(fmt.Sprintf(`
		UPDATE %s SET retry_count = retry_count + 1, available_at = ?, taken_at = NULL WHERE id = ?`, o.outboxTable)); err != nil {
		return fmt.Errorf("could not prepare schedule retry statement: %w", err)
	}
	if o.insertDeadLetterStmt, err = o.db.Prepare(fmt.Sprintf(`
		INSERT INTO %s (id, source, event_type, aggregate_id, handler_type, outbox_id, remaining_handlers, blob, error, retry_count, created_at, dead_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, o.deadLetterTable)); err != nil {
		return fmt.Errorf("could not prepare insert dead letter statement: %w", err)
	}
	if o.nextAvailableAtStmt, err = o.db.Prepare(fmt.Sprintf(`
		SELECT available_at
		FROM %s
		WHERE (taken_at IS NULL OR taken_at < ?) AND available_at > ?
		ORDER BY available_at ASC, id ASC LIMIT 1
	`, o.outboxTable)); err != nil {
		return fmt.Errorf("could not prepare next available statement: %w", err)
	}
	return nil
}

// HandlerType implements the HandlerType method of the eventhorizon.EventHandler interface.
func (o *Outbox) HandlerType() eh.EventHandlerType {
	return "outbox"
}

// AddHandler implements the AddHandler method of the eventhorizon.Outbox interface.
func (o *Outbox) AddHandler(ctx context.Context, m eh.EventMatcher, h eh.EventHandler) error {
	if m == nil {
		return eh.ErrMissingMatcher
	}
	if h == nil {
		return eh.ErrMissingHandler
	}

	o.handlersMu.Lock()
	defer o.handlersMu.Unlock()

	if _, ok := o.handlersByType[h.HandlerType()]; ok {
		return eh.ErrHandlerAlreadyAdded
	}

	mh := &matcherHandler{m, h}
	o.handlers = append(o.handlers, mh)
	o.handlersByType[h.HandlerType()] = mh

	return nil
}

// outboxDoc is the DB representation of an outbox entry.
type outboxDoc struct {
	ID    uuid.UUID
	Event eh.Event
	// Ctx is the context of the event, which is not persisted to the database.
	Ctx         context.Context
	Handlers    []string
	CreatedAt   time.Time
	AvailableAt time.Time
	TakenAt     sql.NullTime
	RetryCount  int
}

// HandleEvent implements the HandleEvent method of the eventhorizon.EventHandler interface.
// Start must be called before publishing.
func (o *Outbox) HandleEvent(ctx context.Context, event eh.Event) error {
	if !o.started.Load() {
		return ErrOutboxNotStarted
	}

	eventBlob, err := o.codec.MarshalEvent(ctx, event)
	if err != nil {
		return fmt.Errorf("could not marshal event: %w", err)
	}
	now := time.Now()
	availableAt, err := availableAtFor(ctx, event, now)
	if err != nil {
		return err
	}

	o.handlersMu.RLock()
	matchingHandlers := make([]string, 0)
	for _, mh := range o.handlers {
		if mh.Match(event) {
			matchingHandlers = append(matchingHandlers, mh.EventHandler.HandlerType().String())
		}
	}
	o.handlersMu.RUnlock()

	tx, txOk := sqlite.TxFromContext(ctx)
	if !txOk {
		tx, err = o.db.Begin()
		if err != nil {
			return fmt.Errorf("could not begin transaction: %w", err)
		}
		defer tx.Rollback()
	}

	if len(matchingHandlers) == 0 {
		if err := o.insertNoMatchDeadLetter(ctx, tx, event, eventBlob, now); err != nil {
			return err
		}
		if !txOk {
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("could not commit transaction: %w", err)
			}
		}
		return nil
	}

	handlersBlob, err := jsoniter.Marshal(matchingHandlers)
	if err != nil {
		return fmt.Errorf("could not marshal handlers: %w", err)
	}

	outboxID := uuid.New()
	// Insert the promoted fields AND the blob.
	if _, err := tx.Stmt(o.insertEventStmt).Exec(
		outboxID.String(),
		event.EventType().String(),
		event.AggregateID().String(),
		now,
		availableAt,
		sql.NullTime{},
		string(handlersBlob),
		string(eventBlob),
	); err != nil {
		return fmt.Errorf("could not insert event into outbox: %w", err)
	}

	if !txOk {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("could not commit transaction: %w", err)
		}
	}

	r := &outboxDoc{
		ID:          outboxID,
		Event:       event,
		Ctx:         ctx,
		Handlers:    matchingHandlers,
		CreatedAt:   now,
		AvailableAt: availableAt,
	}

	if !txOk {
		o.notify(r)
	}

	return nil
}

func availableAtFor(ctx context.Context, event eh.Event, now time.Time) (time.Time, error) {
	if metadata := event.Metadata(); metadata != nil {
		if raw, ok := metadata[metadataAvailableAtKey]; ok {
			availableAt, err := parseAvailableAt(raw)
			if err != nil {
				return time.Time{}, fmt.Errorf("invalid %s metadata: %w", metadataAvailableAtKey, err)
			}
			if availableAt.Before(now) {
				return now, nil
			}
			return availableAt, nil
		}
	}

	if raw, ok := ctx.Value(availableAtContextKey{}).(time.Time); ok {
		if raw.Before(now) {
			return now, nil
		}
		return raw, nil
	}

	return now, nil
}

func parseAvailableAt(raw any) (time.Time, error) {
	switch v := raw.(type) {
	case time.Time:
		return v, nil
	case string:
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return t, nil
		}
		return time.Parse(time.RFC3339, v)
	default:
		return time.Time{}, fmt.Errorf("unsupported value type %T", raw)
	}
}

func (o *Outbox) insertNoMatchDeadLetter(ctx context.Context, tx *sql.Tx, event eh.Event, eventBlob []byte, now time.Time) error {
	if _, err := tx.Stmt(o.insertDeadLetterStmt).ExecContext(
		ctx,
		uuid.New().String(),
		"outbox",
		event.EventType().String(),
		event.AggregateID().String(),
		"no_match",
		sql.NullString{},
		"[]",
		string(eventBlob),
		"no matching handlers",
		0,
		now,
		now,
	); err != nil {
		return fmt.Errorf("could not insert no-match dead letter: %w", err)
	}
	return nil
}

// NotifyAfterCommit wakes the outbox processor after an external transaction
// using this outbox has committed. Callers using sqlite.NewContextWithTx with
// their own transaction must call this after commit; EventStore.Save does it for
// in-transaction handlers automatically.
func (o *Outbox) NotifyAfterCommit(ctx context.Context) {
	o.notify(&outboxDoc{Ctx: ctx})
}

func (o *Outbox) notify(r *outboxDoc) {
	select {
	case o.watchCh <- r:
	default:
		o.notifySchedule()
	}
}

func (o *Outbox) notifySchedule() {
	select {
	case o.scheduleCh <- struct{}{}:
	default:
	}
}

// Start launches one unified processor. Calling Start more than once is safe.
func (o *Outbox) Start() {
	if !o.started.CompareAndSwap(false, true) {
		return
	}
	o.wg.Add(1) // Only one worker goroutine.
	go o.runUnifiedProcessor()
}

// Close stops the outbox processor and closes prepared statements.
// The caller owns the shared *sql.DB and is responsible for closing it.
func (o *Outbox) Close() error {
	o.cancel()
	o.wg.Wait()

	var closeErr error
	for _, stmt := range []*sql.Stmt{
		o.insertEventStmt,
		o.selectEventsStmt,
		o.updateTakenAtStmt,
		o.deleteEventStmt,
		o.updateHandlersStmt,
		o.scheduleRetryStmt,
		o.insertDeadLetterStmt,
		o.nextAvailableAtStmt,
	} {
		if stmt == nil {
			continue
		}
		if err := stmt.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}

	return closeErr
}

func (o *Outbox) runUnifiedProcessor() {
	defer o.wg.Done()

	ticker := time.NewTicker(PeriodicSweepInterval)
	defer ticker.Stop()
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	var timerCh <-chan time.Time
	resetTimer := func() {
		timerCh = nil
		next, ok, err := o.nextAvailableAt(o.cctx)
		if err != nil {
			o.sendError(err, nil, o.cctx)
			return
		}
		if !ok {
			return
		}
		delay := time.Until(next)
		if delay < 0 {
			delay = 0
		}
		timer.Reset(delay)
		timerCh = timer.C
	}
	stopTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
	defer stopTimer()
	resetTimer()

	// The main loop now only cares about being woken up.
	// The actual processing loop is inside the handler for the triggers.
	for {
		select {
		// --- Trigger 1: The Fast Path (from watchCh) ---
		case r := <-o.watchCh:
			stopTimer()
			// A signal has arrived. We know there is at least one new event.
			// We will now enter a "work loop" that continues until the outbox is empty.
			o.processUntilEmpty(r.Ctx)
			resetTimer()

		// --- Trigger 2: The Slow Path (from ticker) ---
		case <-ticker.C:
			stopTimer()
			// The ticker is our safety net. It also triggers the same work loop.
			o.processUntilEmpty(o.cctx)
			resetTimer()

		case <-timerCh:
			stopTimer()
			o.processUntilEmpty(o.cctx)
			resetTimer()

		case <-o.scheduleCh:
			stopTimer()
			o.processUntilEmpty(o.cctx)
			resetTimer()

		// --- Trigger 3: Shutdown ---
		case <-o.cctx.Done():
			return
		}
	}
}

func (o *Outbox) nextAvailableAt(ctx context.Context) (time.Time, bool, error) {
	var next sql.NullTime
	now := time.Now()
	if err := o.nextAvailableAtStmt.QueryRowContext(ctx, now.Add(-PeriodicSweepAge), now).Scan(&next); err != nil {
		if err == sql.ErrNoRows {
			return time.Time{}, false, nil
		}
		return time.Time{}, false, fmt.Errorf("could not query next available outbox event: %w", err)
	}
	if !next.Valid {
		return time.Time{}, false, nil
	}
	return next.Time, true, nil
}

// Add this new method to the Outbox.
func (o *Outbox) processUntilEmpty(ctx context.Context) {
	// Before starting, quickly drain the channel of any other pending signals
	// to prevent this loop from being triggered multiple times unnecessarily.
	o.drainWatchChannel()

	for {
		// processBatch is the same transactional function as before,
		// but we will modify it to return the number of events it processed.
		processedCount, err := o.processBatch(ctx)
		if err != nil {
			o.sendError(err, nil, ctx)
			// On error, we stop processing this batch to avoid hammering a failing system.
			// The ticker will try again later.
			return
		}

		// If the last batch processed zero events, the outbox is now empty.
		// We can exit our work loop and go back to sleep.
		if processedCount == 0 {
			return
		}

		// Optional: If we are in a tight loop processing many batches,
		// it's good practice to yield to the Go scheduler briefly to
		// prevent this single goroutine from starving others.
		time.Sleep(10 * time.Millisecond)
	}
}

// Helper to drain the channel.
func (o *Outbox) drainWatchChannel() {
	for {
		select {
		case <-o.watchCh:
			// Keep draining until it's empty.
		default:
			return
		}
	}
}

// Helper to send errors.
func (o *Outbox) sendError(err error, event eh.Event, ctx context.Context) {
	select {
	case o.errCh <- &eh.OutboxError{Err: err, Ctx: ctx, Event: event}:
	default:
		log.Printf("eventhorizon: missed error in SQLite outbox processing: %s", err)
	}
}

// Errors implements the Errors method of the eventhorizon.EventBus interface.
func (o *Outbox) Errors() <-chan error {
	return o.errCh
}

func (o *Outbox) scanOutboxDoc(rows *sql.Rows) (*outboxDoc, error) {
	var id, eventType, aggregateID, handlersBlob, eventBlob string
	var createdAt, availableAt, takenAt sql.NullTime
	var retryCount int

	// Fallback to not failing if the schema is old and retryCount hasn't been added yet in a result set
	// Note: We're selecting specific columns, so we must add retry_count to the select statement.
	if err := rows.Scan(&id, &eventType, &aggregateID, &createdAt, &availableAt, &takenAt, &handlersBlob, &eventBlob, &retryCount); err != nil {
		return nil, fmt.Errorf("could not scan row: %w", err)
	}

	event, ctx, err := o.codec.UnmarshalEvent(o.cctx, []byte(eventBlob))
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal event blob: %w", err)
	}

	var handlers []string
	if err := jsoniter.Unmarshal([]byte(handlersBlob), &handlers); err != nil {
		return nil, fmt.Errorf("could not unmarshal handlers: %w", err)
	}

	return &outboxDoc{
		ID:          uuid.MustParse(id),
		Event:       event,
		Ctx:         ctx,
		Handlers:    handlers,
		CreatedAt:   createdAt.Time,
		AvailableAt: availableAt.Time,
		TakenAt:     takenAt,
		RetryCount:  retryCount,
	}, nil
}

type processedResult struct {
	r                      *outboxDoc
	successfulHandlers     []string
	failedRetryable        int
	failedFatal            int
	errorMessage           string
	failedDropDueToRetries bool
}

// processBatch - main procesing batch
func (o *Outbox) processBatch(ctx context.Context) (int, error) {
	eventsToProcess, err := o.fetchAndLockEvents(ctx)
	if err != nil {
		return 0, err
	}
	if len(eventsToProcess) == 0 {
		return 0, nil
	}

	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(o.maxGoroutines)

	// Since we are interacting with the database, we want to serialize the queries to avoid 'database is locked' errors under WAL.
	// But we DO want to run dispatchEvent asynchronously.
	// So we create channels to funnel the results of dispatchEvents back into a single routine that updates DB records.
	resultsCh := make(chan processedResult, len(eventsToProcess))

	for _, req := range eventsToProcess {
		req := req // capture loop var
		g.Go(func() error {
			// dispatchEvent runs multiple handlers for ONE event and returns how many succeeded/failed
			successfulHandlers, failedHandlers, fatalError, errMsg := o.dispatchEvent(req)

			res := processedResult{
				r:                  req,
				successfulHandlers: successfulHandlers,
				errorMessage:       errMsg,
			}

			if fatalError {
				res.failedFatal = failedHandlers
			} else if failedHandlers > 0 {
				res.failedRetryable = failedHandlers
				if req.RetryCount >= o.maxRetries { // Check poison pill logic limit
					res.failedDropDueToRetries = true
				}
			}

			// We don't abort errgroup on handler errors; we want to process all messages in batch.
			resultsCh <- res
			return nil
		})
	}

	// Wait in a separate goroutine so we can close results channel
	go func() {
		g.Wait()
		close(resultsCh)
	}()

	o.updateEventsDB(ctx, resultsCh)

	return len(eventsToProcess), nil
}

func (o *Outbox) fetchAndLockEvents(ctx context.Context) ([]*outboxDoc, error) {
	tx, err := o.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	now := time.Now()
	rows, err := tx.StmtContext(ctx, o.selectEventsStmt).Query(now.Add(-PeriodicSweepAge), now)
	if err != nil {
		return nil, err
	}

	var eventsToProcess []*outboxDoc
	for rows.Next() {
		r, err := o.scanOutboxDoc(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		eventsToProcess = append(eventsToProcess, r)
	}
	rows.Close() // It's important to close before committing.

	if len(eventsToProcess) == 0 {
		tx.Commit()
		return nil, nil
	}

	for _, r := range eventsToProcess {
		if _, err := tx.Stmt(o.updateTakenAtStmt).ExecContext(ctx, now, r.ID.String()); err != nil {
			return nil, err
		}
	}

	// End the transaction that locks the rows.
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("could not commit transaction locking events: %w", err)
	}

	return eventsToProcess, nil
}

func (o *Outbox) updateEventsDB(ctx context.Context, resultsCh <-chan processedResult) {
	// Read results back and perform serial SQLite updates
	for res := range resultsCh {
		r := res.r

		// If nothing worked and it's a poison pill due to max retries or a fatal error
		if res.failedDropDueToRetries || res.failedFatal > 0 {
			o.sendError(fmt.Errorf("event dropped outbox: max retries reached or fatal error encountered"), r.Event, ctx)
			if err := o.insertTerminalDeadLetter(ctx, r, res.errorMessage); err != nil {
				o.sendError(err, r.Event, ctx)
			}
			if _, err := o.deleteEventStmt.ExecContext(ctx, r.ID.String()); err != nil {
				o.sendError(fmt.Errorf("could not delete fully processed/dropped event: %w", err), r.Event, ctx)
			}
			continue
		}

		// Update the list of remaining handlers
		remainingHandlers := make([]string, 0, len(r.Handlers))
		for _, required := range r.Handlers {
			if !slices.Contains(res.successfulHandlers, required) {
				remainingHandlers = append(remainingHandlers, required)
			}
		}

		if len(remainingHandlers) == 0 {
			// All handlers have finished working, remove the event.
			if _, err := o.deleteEventStmt.ExecContext(ctx, r.ID.String()); err != nil {
				o.sendError(fmt.Errorf("could not delete fully processed event: %w", err), r.Event, ctx)
			}
		} else {
			if res.failedRetryable > 0 {
				nextRetryCount := r.RetryCount + 1
				availableAt := time.Now().Add(o.retryBackoff.DelayFunc(int64(nextRetryCount)))
				if _, err := o.scheduleRetryStmt.ExecContext(ctx, availableAt, r.ID.String()); err != nil {
					o.sendError(fmt.Errorf("could not schedule event retry: %w", err), r.Event, ctx)
				}
				o.notifySchedule()
			}

			newHandlersBlob, err := jsoniter.Marshal(remainingHandlers)
			if err != nil {
				o.sendError(fmt.Errorf("could not marshal remaining handlers: %w", err), r.Event, ctx)
				continue
			}
			if _, err := o.updateHandlersStmt.ExecContext(ctx, string(newHandlersBlob), r.ID.String()); err != nil {
				o.sendError(fmt.Errorf("could not update remaining handlers: %w", err), r.Event, ctx)
			}
		}
	}
}

func (o *Outbox) insertTerminalDeadLetter(ctx context.Context, r *outboxDoc, reason string) error {
	eventBlob, err := o.codec.MarshalEvent(ctx, r.Event)
	if err != nil {
		return fmt.Errorf("could not marshal dead letter event: %w", err)
	}
	remainingHandlers, err := jsoniter.Marshal(r.Handlers)
	if err != nil {
		return fmt.Errorf("could not marshal dead letter handlers: %w", err)
	}
	if reason == "" {
		reason = "max retries reached or fatal handler error"
	}
	now := time.Now()
	if _, err := o.insertDeadLetterStmt.ExecContext(
		ctx,
		uuid.New().String(),
		"outbox",
		r.Event.EventType().String(),
		r.Event.AggregateID().String(),
		strings.Join(r.Handlers, ","),
		r.ID.String(),
		string(remainingHandlers),
		string(eventBlob),
		reason,
		r.RetryCount,
		r.CreatedAt,
		now,
	); err != nil {
		return fmt.Errorf("could not insert outbox dead letter: %w", err)
	}
	return nil
}

// dispatchEvent - Returns a list of handlers that completed successfully.
// Also returns count of failed handlers and a boolean indicating if ANY failure was Fatal.
func (o *Outbox) dispatchEvent(r *outboxDoc) ([]string, int, bool, string) {
	var successfulHandlers []string
	var failedHandlers int
	var fatalError uint32 // use uint32 for atomic ops across goroutines, though here it's 1 event 1 goroutine
	var firstError string

	handlerSet := make(map[string]struct{})
	for _, h := range r.Handlers {
		handlerSet[h] = struct{}{}
	}

	o.handlersMu.RLock()
	defer o.handlersMu.RUnlock()

	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, mh := range o.handlers {
		mh := mh // local variable for closure
		// Check if this handler is even on the list of handlers to be processed for this event.
		if _, ok := handlerSet[mh.HandlerType().String()]; !ok {
			continue
		}

		if !mh.Match(r.Event) {
			continue
		}

		wg.Add(1)
		go func(hnd *matcherHandler) {
			defer wg.Done()
			if err := hnd.HandleEvent(r.Ctx, r.Event); err != nil {
				severity := GetSeverity(err)

				if severity == SeverityFatal {
					atomic.StoreUint32(&fatalError, 1)
				}
				err = fmt.Errorf("could not handle event (%s): %w", hnd.HandlerType(), err)
				o.sendError(err, r.Event, r.Ctx)

				mu.Lock()
				failedHandlers++
				if firstError == "" {
					firstError = err.Error()
				}
				mu.Unlock()
			} else {
				mu.Lock()
				successfulHandlers = append(successfulHandlers, hnd.HandlerType().String())
				mu.Unlock()
			}
		}(mh)
	}

	wg.Wait()
	return successfulHandlers, failedHandlers, atomic.LoadUint32(&fatalError) == 1, firstError
}
