package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vercly/eh-sqlite/backoff"
	"github.com/vercly/eh-sqlite/context/sqlite"
	dl "github.com/vercly/eh-sqlite/deadletter"
	"github.com/vercly/eh-sqlite/internal/deadletter"
	"golang.org/x/sync/errgroup"

	jsoniter "github.com/json-iterator/go"
	// Register the sqlite3 database/sql driver used by this package.
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

type doneContext struct {
	done <-chan struct{}
}

func (c doneContext) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (c doneContext) Done() <-chan struct{} {
	return c.done
}

func (c doneContext) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}

func (c doneContext) Value(any) any {
	return nil
}

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
	db               *sql.DB
	outboxTable      string
	deadLetterTable  string
	handlers         []*matcherHandler
	handlersByType   map[eh.EventHandlerType]*matcherHandler
	handlersMu       sync.RWMutex
	watchCh          chan struct{}
	scheduleCh       chan struct{}
	errCh            chan error
	done             <-chan struct{}
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	started          atomic.Bool
	codec            eh.EventCodec
	maxRetries       int
	maxGoroutines    int
	retryBackoff     backoff.Config
	deadLetterExport dl.Exporter

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
	dispatchMode    DispatchMode
	partitionShards int
}

// DispatchMode controls ordering and concurrency for a registered event handler.
type DispatchMode int

const (
	// Serial dispatches at most one event at a time for a handler.
	Serial DispatchMode = iota
	// PartitionByAggregate dispatches one ordered queue per aggregate shard.
	PartitionByAggregate
)

const defaultPartitionShards = 16

// HandlerOption configures per-handler dispatch behaviour.
type HandlerOption func(*handlerOptions) error

type handlerOptions struct {
	dispatchMode    DispatchMode
	partitionShards int
}

func defaultHandlerOptions() handlerOptions {
	return handlerOptions{
		dispatchMode:    Serial,
		partitionShards: defaultPartitionShards,
	}
}

// WithDispatchMode sets the dispatch mode for a handler. AddHandler defaults
// to Serial for RabbitMQ CONCURRENCY=1 parity.
func WithDispatchMode(mode DispatchMode) HandlerOption {
	return func(opts *handlerOptions) error {
		switch mode {
		case Serial, PartitionByAggregate:
			opts.dispatchMode = mode
			return nil
		default:
			return fmt.Errorf("%w: %d", errUnknownDispatchMode, mode)
		}
	}
}

// WithPartitionShards sets the number of shards used by PartitionByAggregate.
// Values below one are rejected.
func WithPartitionShards(shards int) HandlerOption {
	return func(opts *handlerOptions) error {
		if shards < 1 {
			return errInvalidPartitionShards
		}
		opts.partitionShards = shards
		return nil
	}
}

// NewOutbox creates a new Outbox.
func NewOutbox(db *sql.DB, options ...Option) (*Outbox, error) {
	ctx, cancel := context.WithCancel(context.Background())

	o := &Outbox{
		db:              db,
		outboxTable:     "outbox",
		deadLetterTable: "dead_letters",
		handlersByType:  map[eh.EventHandlerType]*matcherHandler{},
		watchCh:         make(chan struct{}, 100),
		scheduleCh:      make(chan struct{}, 1),
		errCh:           make(chan error, 100),
		done:            ctx.Done(),
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
func WithMaxGoroutines(limit int) Option {
	return func(o *Outbox) error {
		o.maxGoroutines = limit
		return nil
	}
}

// WithDeadLetterExporter registers a best-effort exporter called after a
// dead_letters row is inserted. Export failures do not roll back the DB write.
func WithDeadLetterExporter(exporter dl.Exporter) Option {
	return func(o *Outbox) error {
		o.deadLetterExport = exporter
		return nil
	}
}

func (o *Outbox) prepareStatements() error {
	var err error
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
	return o.AddHandlerWithOptions(ctx, m, h)
}

// AddHandlerWithOptions registers an event handler with explicit dispatch
// options. The default AddHandler path uses Serial dispatch.
func (o *Outbox) AddHandlerWithOptions(_ context.Context, m eh.EventMatcher, h eh.EventHandler, options ...HandlerOption) error {
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

	handlerOptions := defaultHandlerOptions()
	for _, option := range options {
		if err := option(&handlerOptions); err != nil {
			return err
		}
	}

	mh := &matcherHandler{
		EventMatcher:    m,
		EventHandler:    h,
		dispatchMode:    handlerOptions.dispatchMode,
		partitionShards: handlerOptions.partitionShards,
	}
	o.handlers = append(o.handlers, mh)
	o.handlersByType[h.HandlerType()] = mh

	return nil
}

// outboxDoc is the DB representation of an outbox entry.
type outboxDoc struct {
	ID          uuid.UUID
	Event       eh.Event
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

	matchingHandlers := o.matchingHandlers(event)
	return o.storeEvent(ctx, event, eventBlob, now, availableAt, matchingHandlers)
}

func (o *Outbox) matchingHandlers(event eh.Event) []string {
	o.handlersMu.RLock()
	defer o.handlersMu.RUnlock()

	matchingHandlers := make([]string, 0)
	for _, mh := range o.handlers {
		if mh.Match(event) {
			matchingHandlers = append(matchingHandlers, mh.EventHandler.HandlerType().String())
		}
	}
	return matchingHandlers
}

func (o *Outbox) storeEvent(ctx context.Context, event eh.Event, eventBlob []byte, now, availableAt time.Time, matchingHandlers []string) error {
	tx, txOk, err := o.txForEvent(ctx)
	if err != nil {
		return err
	}
	if !txOk {
		defer rollbackTx(tx)
	}

	if len(matchingHandlers) == 0 {
		return o.storeNoMatchEvent(ctx, tx, txOk, event, eventBlob, now)
	}
	return o.storeMatchedEvent(ctx, tx, txOk, event, eventBlob, now, availableAt, matchingHandlers)
}

func (o *Outbox) storeNoMatchEvent(ctx context.Context, tx *sql.Tx, txOk bool, event eh.Event, eventBlob []byte, now time.Time) error {
	if err := o.insertNoMatchDeadLetter(ctx, tx, event, eventBlob, now); err != nil {
		return err
	}
	return commitOwnedTx(tx, txOk)
}

func (o *Outbox) storeMatchedEvent(ctx context.Context, tx *sql.Tx, txOk bool, event eh.Event, eventBlob []byte, now, availableAt time.Time, matchingHandlers []string) error {
	handlersBlob, err := jsoniter.Marshal(matchingHandlers)
	if err != nil {
		return fmt.Errorf("could not marshal handlers: %w", err)
	}

	outboxID := uuid.New()
	if err := o.insertOutboxEvent(ctx, tx, outboxID, event, eventBlob, handlersBlob, now, availableAt); err != nil {
		return err
	}

	if err := commitOwnedTx(tx, txOk); err != nil {
		return err
	}

	if !txOk {
		o.notify()
	}

	return nil
}

func (o *Outbox) insertOutboxEvent(ctx context.Context, tx *sql.Tx, outboxID uuid.UUID, event eh.Event, eventBlob, handlersBlob []byte, now, availableAt time.Time) error {
	insertStmt := tx.StmtContext(ctx, o.insertEventStmt)
	defer func() {
		if err := insertStmt.Close(); err != nil {
			log.Printf("eventhorizon: could not close SQLite outbox insert statement: %s", err)
		}
	}()

	if _, err := insertStmt.ExecContext(
		ctx,
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
	return nil
}

func commitOwnedTx(tx *sql.Tx, txOk bool) error {
	if txOk {
		return nil
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}
	return nil
}

func (o *Outbox) txForEvent(ctx context.Context) (*sql.Tx, bool, error) {
	tx, txOk := sqlite.TxFromContext(ctx)
	if txOk {
		return tx, true, nil
	}
	tx, err := o.db.Begin()
	if err != nil {
		return nil, false, fmt.Errorf("could not begin transaction: %w", err)
	}
	return tx, false, nil
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
		return time.Time{}, fmt.Errorf("%w: %T", errUnsupportedAvailableAtType, raw)
	}
}

func (o *Outbox) insertNoMatchDeadLetter(ctx context.Context, tx *sql.Tx, event eh.Event, eventBlob []byte, now time.Time) error {
	insertStmt := tx.StmtContext(ctx, o.insertDeadLetterStmt)
	defer func() {
		if err := insertStmt.Close(); err != nil {
			log.Printf("eventhorizon: could not close SQLite outbox dead letter statement: %s", err)
		}
	}()

	if _, err := insertStmt.ExecContext(
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
	_ = ctx
	o.notify()
}

func (o *Outbox) notify() {
	select {
	case o.watchCh <- struct{}{}:
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

func (o *Outbox) runContext() context.Context {
	return doneContext{done: o.done}
}

func rollbackTx(tx *sql.Tx) {
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		log.Printf("eventhorizon: could not roll back SQLite outbox transaction: %s", err)
	}
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
	defer stopProcessorTimer(timer)
	o.resetProcessorTimer(timer, &timerCh)

	for {
		select {
		case <-o.watchCh:
		case <-ticker.C:
		case <-timerCh:
		case <-o.scheduleCh:
		case <-o.done:
			return
		}
		stopProcessorTimer(timer)
		o.processUntilEmpty(o.runContext())
		o.resetProcessorTimer(timer, &timerCh)
	}
}

func (o *Outbox) resetProcessorTimer(timer *time.Timer, timerCh *<-chan time.Time) {
	*timerCh = nil
	next, ok, err := o.nextAvailableAt(o.runContext())
	if err != nil {
		o.sendError(o.runContext(), err, nil)
		return
	}
	if !ok {
		return
	}
	delay := max(time.Until(next), 0)
	timer.Reset(delay)
	*timerCh = timer.C
}

func stopProcessorTimer(timer *time.Timer) {
	if timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

func (o *Outbox) nextAvailableAt(ctx context.Context) (time.Time, bool, error) {
	var next sql.NullTime
	now := time.Now()
	if err := o.nextAvailableAtStmt.QueryRowContext(ctx, now.Add(-PeriodicSweepAge), now).Scan(&next); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
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
			o.sendError(ctx, err, nil)
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
func (o *Outbox) sendError(ctx context.Context, err error, event eh.Event) {
	recordErrorSeverity(GetSeverity(err))
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

func (o *Outbox) scanOutboxDoc(rows *sql.Rows) (*outboxDoc, map[string]any, error) {
	var id, eventType, aggregateID, handlersBlob, eventBlob string
	var createdAt, availableAt, takenAt sql.NullTime
	var retryCount int

	// Fallback to not failing if the schema is old and retryCount hasn't been added yet in a result set
	// Note: We're selecting specific columns, so we must add retry_count to the select statement.
	if err := rows.Scan(&id, &eventType, &aggregateID, &createdAt, &availableAt, &takenAt, &handlersBlob, &eventBlob, &retryCount); err != nil {
		return nil, nil, fmt.Errorf("could not scan row: %w", err)
	}

	event, _, err := o.codec.UnmarshalEvent(o.runContext(), []byte(eventBlob))
	if err != nil {
		return nil, nil, fmt.Errorf("could not unmarshal event blob: %w", err)
	}
	eventContext, err := eventContextValues([]byte(eventBlob))
	if err != nil {
		return nil, nil, err
	}

	var handlers []string
	if err := jsoniter.Unmarshal([]byte(handlersBlob), &handlers); err != nil {
		return nil, nil, fmt.Errorf("could not unmarshal handlers: %w", err)
	}

	return &outboxDoc{
		ID:          uuid.MustParse(id),
		Event:       event,
		Handlers:    handlers,
		CreatedAt:   createdAt.Time,
		AvailableAt: availableAt.Time,
		TakenAt:     takenAt,
		RetryCount:  retryCount,
	}, eventContext, nil
}

func eventContextValues(eventBlob []byte) (map[string]any, error) {
	var raw struct {
		Context map[string]any `json:"context"`
	}
	if err := jsoniter.Unmarshal(eventBlob, &raw); err != nil {
		return nil, fmt.Errorf("could not unmarshal event context: %w", err)
	}
	return raw.Context, nil
}

type processedResult struct {
	r                  *outboxDoc
	successfulHandlers []string
	failedHandlers     map[string]handlerFailure
}

type handlerFailure struct {
	err   string
	fatal bool
}

type dispatchItem struct {
	event   *outboxDoc
	handler *matcherHandler
}

type dispatchQueue struct {
	items []dispatchItem
}

type handlerDispatchResult struct {
	eventID     uuid.UUID
	handlerType string
	err         error
	fatal       bool
}

// processBatch - main procesing batch
func (o *Outbox) processBatch(ctx context.Context) (int, error) {
	eventsToProcess, eventContexts, err := o.fetchAndLockEvents(ctx)
	if err != nil {
		return 0, err
	}
	if len(eventsToProcess) == 0 {
		return 0, nil
	}

	results := make(map[string]*processedResult, len(eventsToProcess))
	for _, event := range eventsToProcess {
		results[event.ID.String()] = &processedResult{
			r:              event,
			failedHandlers: map[string]handlerFailure{},
		}
	}

	if err := o.dispatchEvents(ctx, eventsToProcess, eventContexts, results); err != nil {
		return 0, err
	}

	resultsCh := make(chan processedResult, len(eventsToProcess))
	for _, event := range eventsToProcess {
		res := results[event.ID.String()]
		resultsCh <- *res
	}
	close(resultsCh)

	o.updateEventsDB(ctx, resultsCh)

	return len(eventsToProcess), nil
}

func (o *Outbox) dispatchEvents(ctx context.Context, eventsToProcess []*outboxDoc, eventContexts map[string]map[string]any, results map[string]*processedResult) error {
	queues, itemCount := o.buildDispatchQueues(eventsToProcess)
	if itemCount == 0 {
		return nil
	}

	handlerResultsCh := make(chan handlerDispatchResult, itemCount)
	waitErrCh := make(chan error, 1)
	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(max(o.maxGoroutines, 1))

	for _, queue := range queues {
		g.Go(func() error {
			for _, item := range queue.items {
				eventCtx := eh.UnmarshalContext(ctx, eventContexts[item.event.ID.String()])
				handlerResultsCh <- o.dispatchHandler(eventCtx, item)
			}
			return nil
		})
	}

	go func() {
		waitErrCh <- g.Wait()
		close(handlerResultsCh)
	}()

	o.collectHandlerResults(handlerResultsCh, results)
	return <-waitErrCh
}

func (o *Outbox) collectHandlerResults(handlerResultsCh <-chan handlerDispatchResult, results map[string]*processedResult) {
	for handlerResult := range handlerResultsCh {
		res := results[handlerResult.eventID.String()]
		if res == nil {
			continue
		}
		if handlerResult.err != nil {
			recordHandlerFailure(res, handlerResult)
			continue
		}
		res.successfulHandlers = append(res.successfulHandlers, handlerResult.handlerType)
	}
}

func recordHandlerFailure(res *processedResult, handlerResult handlerDispatchResult) {
	if _, ok := res.failedHandlers[handlerResult.handlerType]; ok {
		return
	}
	res.failedHandlers[handlerResult.handlerType] = handlerFailure{
		err:   handlerResult.err.Error(),
		fatal: handlerResult.fatal,
	}
}

func (o *Outbox) buildDispatchQueues(eventsToProcess []*outboxDoc) ([]*dispatchQueue, int) {
	handlersByType := o.snapshotHandlersByType()
	queuesByKey := make(map[string]*dispatchQueue)
	queues := make([]*dispatchQueue, 0)
	itemCount := 0

	for _, event := range eventsToProcess {
		for _, handlerType := range event.Handlers {
			handler := handlersByType[handlerType]
			if handler == nil || !handler.Match(event.Event) {
				continue
			}
			key := dispatchQueueKey(handler, event.Event)
			queue := queuesByKey[key]
			if queue == nil {
				queue = &dispatchQueue{}
				queues = append(queues, queue)
				queuesByKey[key] = queue
			}
			queue.items = append(queue.items, dispatchItem{
				event:   event,
				handler: handler,
			})
			itemCount++
		}
	}

	return queues, itemCount
}

func (o *Outbox) snapshotHandlersByType() map[string]*matcherHandler {
	o.handlersMu.RLock()
	defer o.handlersMu.RUnlock()

	handlers := make(map[string]*matcherHandler, len(o.handlersByType))
	for handlerType, handler := range o.handlersByType {
		handlers[handlerType.String()] = handler
	}
	return handlers
}

func dispatchQueueKey(handler *matcherHandler, event eh.Event) string {
	handlerType := handler.HandlerType().String()
	if handler.dispatchMode != PartitionByAggregate {
		return handlerType
	}

	key := eventPartitionKey(event)
	if key == "" {
		return handlerType
	}

	shards := handler.partitionShards
	if shards < 1 {
		shards = defaultPartitionShards
	}
	return fmt.Sprintf("%s:%d", handlerType, hashPartition(key, shards))
}

func eventPartitionKey(event eh.Event) string {
	if aggregateID := event.AggregateID(); aggregateID != uuid.Nil {
		return aggregateID.String()
	}
	metadata := event.Metadata()
	for _, key := range []string{"correlation_id", "CorrelationId", "correlationId", "x-correlation-id"} {
		if value, ok := metadata[key]; ok {
			if partitionKey := metadataPartitionKey(value); partitionKey != "" {
				return partitionKey
			}
		}
	}
	return ""
}

func metadataPartitionKey(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

func hashPartition(key string, shards int) uint64 {
	if shards <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return uint64(h.Sum32()) % uint64(shards)
}

func (o *Outbox) dispatchHandler(ctx context.Context, item dispatchItem) handlerDispatchResult {
	handlerType := item.handler.HandlerType().String()
	if err := item.handler.HandleEvent(ctx, item.event.Event); err != nil {
		severity := GetSeverity(err)
		wrappedErr := fmt.Errorf("could not handle event (%s): %w", item.handler.HandlerType(), err)
		o.sendError(ctx, wrappedErr, item.event.Event)
		return handlerDispatchResult{
			eventID:     item.event.ID,
			handlerType: handlerType,
			err:         wrappedErr,
			fatal:       severity == SeverityFatal,
		}
	}
	return handlerDispatchResult{
		eventID:     item.event.ID,
		handlerType: handlerType,
	}
}

func (o *Outbox) fetchAndLockEvents(ctx context.Context) ([]*outboxDoc, map[string]map[string]any, error) {
	tx, err := o.db.Begin()
	if err != nil {
		return nil, nil, err
	}
	defer rollbackTx(tx)

	now := time.Now()
	eventsToProcess, eventContexts, err := o.selectEventsForProcessing(ctx, tx, now)
	if err != nil {
		return nil, nil, err
	}

	if len(eventsToProcess) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, nil, fmt.Errorf("could not commit empty event lock transaction: %w", err)
		}
		return nil, nil, nil
	}

	updateStmt := tx.StmtContext(ctx, o.updateTakenAtStmt)
	defer func() {
		if err := updateStmt.Close(); err != nil {
			log.Printf("eventhorizon: could not close SQLite outbox update statement: %s", err)
		}
	}()

	for _, r := range eventsToProcess {
		if _, err := updateStmt.ExecContext(ctx, now, r.ID.String()); err != nil {
			return nil, nil, err
		}
	}

	// End the transaction that locks the rows.
	if err := tx.Commit(); err != nil {
		return nil, nil, fmt.Errorf("could not commit transaction locking events: %w", err)
	}

	return eventsToProcess, eventContexts, nil
}

func (o *Outbox) selectEventsForProcessing(ctx context.Context, tx *sql.Tx, now time.Time) ([]*outboxDoc, map[string]map[string]any, error) {
	selectStmt := tx.StmtContext(ctx, o.selectEventsStmt)
	defer func() {
		if err := selectStmt.Close(); err != nil {
			log.Printf("eventhorizon: could not close SQLite outbox select statement: %s", err)
		}
	}()

	rows, err := selectStmt.QueryContext(ctx, now.Add(-PeriodicSweepAge), now)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("eventhorizon: could not close SQLite outbox rows: %s", err)
		}
	}()

	eventsToProcess := make([]*outboxDoc, 0)
	eventContexts := make(map[string]map[string]any)
	for rows.Next() {
		r, eventCtx, err := o.scanOutboxDoc(rows)
		if err != nil {
			return nil, nil, err
		}
		eventContexts[r.ID.String()] = eventCtx
		eventsToProcess = append(eventsToProcess, r)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return eventsToProcess, eventContexts, nil
}

func (o *Outbox) updateEventsDB(ctx context.Context, resultsCh <-chan processedResult) {
	for res := range resultsCh {
		o.updateEventDB(ctx, res)
	}
}

func (o *Outbox) updateEventDB(ctx context.Context, res processedResult) {
	r := res.r
	successful := stringSet(res.successfulHandlers)
	terminalFailures, retryableFailures := o.classifyFailures(r, res.failedHandlers)
	intendedRemainingHandlers := remainingHandlers(r.Handlers, successful, stringSetFromMap(terminalFailures))
	terminalRemoved := o.moveTerminalFailures(ctx, r, terminalFailures, intendedRemainingHandlers)
	remaining := remainingHandlers(r.Handlers, successful, terminalRemoved)

	if len(remaining) == 0 {
		o.deleteProcessedEvent(ctx, r)
		return
	}
	o.updateRemainingEventHandlers(ctx, r, remaining, retryableFailures)
}

func (o *Outbox) classifyFailures(r *outboxDoc, failures map[string]handlerFailure) (map[string]handlerFailure, map[string]handlerFailure) {
	terminalFailures := make(map[string]handlerFailure)
	retryableFailures := make(map[string]handlerFailure)
	for handlerType, failure := range failures {
		if failure.fatal || r.RetryCount >= o.maxRetries {
			terminalFailures[handlerType] = failure
			continue
		}
		retryableFailures[handlerType] = failure
	}
	return terminalFailures, retryableFailures
}

func (o *Outbox) moveTerminalFailures(ctx context.Context, r *outboxDoc, terminalFailures map[string]handlerFailure, intendedRemainingHandlers []string) map[string]bool {
	terminalRemoved := make(map[string]bool, len(terminalFailures))
	if len(terminalFailures) == 0 {
		return terminalRemoved
	}

	o.sendError(ctx, errEventHandlerMovedToDeadLetters, r.Event)
	for handlerType, failure := range terminalFailures {
		if err := o.insertOutboxDeadLetter(ctx, r, handlerType, intendedRemainingHandlers, failure.err); err != nil {
			o.sendError(ctx, err, r.Event)
			continue
		}
		terminalRemoved[handlerType] = true
	}
	return terminalRemoved
}

func (o *Outbox) deleteProcessedEvent(ctx context.Context, r *outboxDoc) {
	if _, err := o.deleteEventStmt.ExecContext(ctx, r.ID.String()); err != nil {
		o.sendError(ctx, fmt.Errorf("could not delete fully processed event: %w", err), r.Event)
	}
}

func (o *Outbox) updateRemainingEventHandlers(ctx context.Context, r *outboxDoc, remainingHandlers []string, retryableFailures map[string]handlerFailure) {
	if len(retryableFailures) > 0 {
		o.scheduleRetry(ctx, r)
	}

	newHandlersBlob, err := jsoniter.Marshal(remainingHandlers)
	if err != nil {
		o.sendError(ctx, fmt.Errorf("could not marshal remaining handlers: %w", err), r.Event)
		return
	}
	if _, err := o.updateHandlersStmt.ExecContext(ctx, string(newHandlersBlob), r.ID.String()); err != nil {
		o.sendError(ctx, fmt.Errorf("could not update remaining handlers: %w", err), r.Event)
	}
}

func (o *Outbox) scheduleRetry(ctx context.Context, r *outboxDoc) {
	nextRetryCount := r.RetryCount + 1
	availableAt := time.Now().Add(o.retryBackoff.DelayFunc(int64(nextRetryCount)))
	if _, err := o.scheduleRetryStmt.ExecContext(ctx, availableAt, r.ID.String()); err != nil {
		o.sendError(ctx, fmt.Errorf("could not schedule event retry: %w", err), r.Event)
	}
	o.notifySchedule()
}

func stringSet(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	return set
}

func stringSetFromMap[T any](values map[string]T) map[string]bool {
	set := make(map[string]bool, len(values))
	for value := range values {
		set[value] = true
	}
	return set
}

func remainingHandlers(requiredHandlers []string, successful, removed map[string]bool) []string {
	remaining := make([]string, 0, len(requiredHandlers))
	for _, required := range requiredHandlers {
		if successful[required] || removed[required] {
			continue
		}
		remaining = append(remaining, required)
	}
	return remaining
}

func (o *Outbox) insertOutboxDeadLetter(ctx context.Context, r *outboxDoc, handlerType string, remainingHandlers []string, reason string) error {
	eventBlob, err := o.codec.MarshalEvent(ctx, r.Event)
	if err != nil {
		return fmt.Errorf("could not marshal dead letter event: %w", err)
	}
	remainingHandlersBlob, err := jsoniter.Marshal(remainingHandlers)
	if err != nil {
		return fmt.Errorf("could not marshal dead letter handlers: %w", err)
	}
	if reason == "" {
		reason = "max retries reached or fatal handler error"
	}
	now := time.Now()
	record := dl.Record{
		ID:                uuid.New().String(),
		Source:            "outbox",
		EventType:         r.Event.EventType().String(),
		AggregateID:       r.Event.AggregateID().String(),
		HandlerType:       handlerType,
		OutboxID:          r.ID.String(),
		RemainingHandlers: string(remainingHandlersBlob),
		Blob:              string(eventBlob),
		Error:             reason,
		RetryCount:        r.RetryCount,
		CreatedAt:         r.CreatedAt,
		DeadAt:            now,
	}
	if _, err := o.insertDeadLetterStmt.ExecContext(ctx,
		record.ID,
		record.Source,
		record.EventType,
		record.AggregateID,
		record.HandlerType,
		record.OutboxID,
		record.RemainingHandlers,
		record.Blob,
		record.Error,
		record.RetryCount,
		record.CreatedAt,
		record.DeadAt,
	); err != nil {
		return fmt.Errorf("could not insert outbox dead letter: %w", err)
	}
	if o.deadLetterExport != nil {
		if err := o.deadLetterExport.ExportDeadLetter(ctx, record); err != nil {
			o.sendError(ctx, fmt.Errorf("could not export outbox dead letter: %w", err), r.Event)
		} else if err := o.markDeadLetterExported(ctx, record.ID, time.Now()); err != nil {
			o.sendError(ctx, fmt.Errorf("could not mark outbox dead letter exported: %w", err), r.Event)
		}
	}
	return nil
}

func (o *Outbox) markDeadLetterExported(ctx context.Context, id string, exportedAt time.Time) error {
	if _, err := o.db.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET exported_at = ? WHERE id = ?`, o.deadLetterTable), exportedAt, id); err != nil {
		return err
	}
	return nil
}
