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

// maxFetchBatch is the upper bound on rows considered in one claim transaction.
// Effective claim size is min(maxFetchBatch, admission free slots).
const maxFetchBatch = 50

// Outbox implements an eventhorizon.Outbox for SQLite.
type Outbox struct {
	db              *sql.DB
	outboxTable     string
	deadLetterTable string
	handlers        []*matcherHandler
	handlersByType  map[eh.EventHandlerType]*matcherHandler
	handlersMu      sync.RWMutex
	watchCh         chan struct{}
	scheduleCh      chan struct{}
	errCh           chan error
	done            <-chan struct{}
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	// registrationClosed is set on the first Start/StartChecked attempt and
	// never reopened. AddHandler fails once this is true.
	registrationClosed atomic.Bool
	// processorRunning is true only after a successful reset and fetcher launch.
	// HandleEvent (publish) requires this flag so fail-closed Start does not
	// accept traffic before the processor is up. Retry StartChecked may set it.
	processorRunning atomic.Bool
	// shuttingDown is set in Close; fetcher stops admitting and workers stop
	// starting new HandleEvent work.
	shuttingDown     atomic.Bool
	codec            eh.EventCodec
	maxRetries       int
	maxGoroutines    int
	queueDepth       int
	retryBackoff     backoff.Config
	deadLetterExport dl.Exporter
	admission        *recordAdmission
	dispatch         *dispatchRegistry
	dispatchStats    DispatchStats
	// handleSem bounds concurrent HandleEvent calls (FIFO fair across keys).
	handleSem *fairSem

	insertEventStmt      *sql.Stmt
	selectEventsStmt     *sql.Stmt
	updateTakenAtStmt    *sql.Stmt
	deleteEventStmt      *sql.Stmt
	updateHandlersStmt   *sql.Stmt
	scheduleRetryStmt    *sql.Stmt
	insertDeadLetterStmt *sql.Stmt
	nextAvailableAtStmt  *sql.Stmt
	resetTakenAtStmt     *sql.Stmt
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
		maxGoroutines:   10, // Global concurrent HandleEvent permits
		queueDepth:      defaultQueueDepth,
		retryBackoff:    backoff.FixedConfig(10, PeriodicSweepAge),
		// Default admission capacity matches the fetch batch upper bound:
		// bounds concurrent record coordinators (not channel depth).
		admission: newRecordAdmission(maxFetchBatch),
	}

	for _, option := range options {
		if err := option(o); err != nil {
			return nil, fmt.Errorf("error while applying option: %w", err)
		}
	}

	o.handleSem = newFairSem(max(o.maxGoroutines, 1))
	o.dispatch = newDispatchRegistry(o, o.queueDepth, o.dispatchStats)

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
	// Migrate older outbox shapes via PRAGMA table_info (never error-text match).
	if err := deadletter.AddColumnIfAbsent(o.db, o.outboxTable, "retry_count", "INTEGER DEFAULT 0"); err != nil {
		return fmt.Errorf("could not migrate outbox retry_count: %w", err)
	}
	if err := deadletter.AddColumnIfAbsent(o.db, o.outboxTable, "available_at", "TIMESTAMP"); err != nil {
		return fmt.Errorf("could not migrate outbox available_at: %w", err)
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

// WithMaxGoroutines sets the global concurrent HandleEvent limit for the whole
// processor (not per sweep). Fairness is per dispatch key: each key runs one
// worker that holds at most one permit while inside HandleEvent.
func WithMaxGoroutines(limit int) Option {
	return func(o *Outbox) error {
		if limit < 1 {
			limit = 1
		}
		o.maxGoroutines = limit
		return nil
	}
}

// WithQueueDepth sets the bounded depth of each long-lived per-dispatch-key
// queue (enqueued + reserved). Values below 1 are rejected with
// ErrInvalidQueueDepth (no silent clamping). Default is DefaultQueueDepth (32).
func WithQueueDepth(depth int) Option {
	return func(o *Outbox) error {
		if depth < 1 {
			return fmt.Errorf("%w: %d", ErrInvalidQueueDepth, depth)
		}
		o.queueDepth = depth
		return nil
	}
}

// WithDispatchStats registers a transport-neutral observer for per-key queue
// depth and HandleEvent in-flight gauges. Nil is ignored. Callbacks run outside
// queue locks and must not block delivery.
func WithDispatchStats(collector DispatchStats) Option {
	return func(o *Outbox) error {
		o.dispatchStats = collector
		return nil
	}
}

// WithAdmissionLimit sets how many outbox records may be admitted (claimed and
// in-flight) at once in this process. Admission is process-local and is not a
// multi-process lock. Values below 1 are treated as 1. Default is maxFetchBatch.
func WithAdmissionLimit(limit int) Option {
	return func(o *Outbox) error {
		if o.admission != nil {
			o.admission.setLimit(limit)
		} else {
			o.admission = newRecordAdmission(limit)
		}
		return nil
	}
}

// WithDeadLetterExporter registers a best-effort exporter called after the
// atomic finalize transaction commits. Export failures do not roll back the DB write.
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

	// available_at first so delayed/retry rows re-enter FIFO by eligibility time
	// (retry loses its original position among first attempts).
	// OFFSET pages within one claim TX so a full dispatch-key backlog larger than
	// one page cannot hide later idle-key rows (bounded page size, not one big LIMIT).
	if o.selectEventsStmt, err = o.db.Prepare(fmt.Sprintf(`
		SELECT id, event_type, aggregate_id, created_at, available_at, taken_at, handlers, event_blob, retry_count
		FROM %s
		WHERE (taken_at IS NULL OR taken_at < ?) AND available_at <= ?
		ORDER BY available_at ASC, created_at ASC, id ASC LIMIT ? OFFSET ?
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
	// ON CONFLICT makes terminal DLQ inserts idempotent under the unique key
	// (source, outbox_id, handler_type). NULL outbox_id rows (no-match) never
	// conflict with each other in SQLite.
	if o.insertDeadLetterStmt, err = o.db.Prepare(fmt.Sprintf(`
		INSERT INTO %s (id, source, event_type, aggregate_id, handler_type, outbox_id, remaining_handlers, blob, error, retry_count, created_at, dead_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(source, outbox_id, handler_type) DO NOTHING
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
	if o.resetTakenAtStmt, err = o.db.Prepare(fmt.Sprintf(`
		UPDATE %s SET taken_at = NULL WHERE taken_at IS NOT NULL
	`, o.outboxTable)); err != nil {
		return fmt.Errorf("could not prepare reset taken_at statement: %w", err)
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
// options. The default AddHandler path uses Serial dispatch. Registration is
// only allowed before Start; after Start this returns ErrOutboxAlreadyStarted.
func (o *Outbox) AddHandlerWithOptions(_ context.Context, m eh.EventMatcher, h eh.EventHandler, options ...HandlerOption) error {
	if m == nil {
		return eh.ErrMissingMatcher
	}
	if h == nil {
		return eh.ErrMissingHandler
	}
	if o.registrationClosed.Load() {
		return ErrOutboxAlreadyStarted
	}

	o.handlersMu.Lock()
	defer o.handlersMu.Unlock()

	// Re-check under the handlers lock so a concurrent Start cannot race a late
	// registration into an already running processor.
	if o.registrationClosed.Load() {
		return ErrOutboxAlreadyStarted
	}

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
// Publishing requires a successful Start/StartChecked (processor running). A
// fail-closed Start that closed registration but did not start the fetcher still
// returns ErrOutboxNotStarted.
func (o *Outbox) HandleEvent(ctx context.Context, event eh.Event) error {
	if !o.processorRunning.Load() {
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

// Start implements eh.Outbox. It is a fail-closed wrapper around StartChecked:
// startup errors are sent on Errors() and the fetcher is not started until a
// later successful Start/StartChecked. Prefer StartChecked when the caller can
// propagate the error (EhInfra boot path).
func (o *Outbox) Start() {
	if err := o.StartChecked(); err != nil {
		o.sendError(context.Background(), err, nil)
	}
}

// StartChecked closes handler registration, resets taken_at in one transaction,
// then starts the fetcher only after a successful reset. On reset failure the
// fetcher is not started and publish stays blocked (ErrOutboxNotStarted);
// registration stays closed and a later StartChecked may retry the reset.
// After a successful start, further calls are no-ops.
// Scope is one process per DB file; multi-process locking is not provided.
func (o *Outbox) StartChecked() error {
	o.handlersMu.Lock()
	// Close registration before reset so late AddHandler cannot race in.
	o.registrationClosed.Store(true)
	if o.processorRunning.Load() {
		o.handlersMu.Unlock()
		return nil
	}
	o.handlersMu.Unlock()

	if err := o.resetTakenAtClaims(context.Background()); err != nil {
		return fmt.Errorf("could not reset outbox taken_at on start: %w", err)
	}

	o.handlersMu.Lock()
	if o.processorRunning.Load() {
		o.handlersMu.Unlock()
		return nil
	}
	o.processorRunning.Store(true)
	o.handlersMu.Unlock()

	o.wg.Add(1)
	go o.runUnifiedProcessor()
	return nil
}

// resetTakenAtClaims clears durable claims left by a previous process so the
// new process can redispatch at-least-once after crash or stop-first restart.
func (o *Outbox) resetTakenAtClaims(ctx context.Context) error {
	tx, err := o.db.Begin()
	if err != nil {
		return fmt.Errorf("could not begin taken_at reset transaction: %w", err)
	}
	defer rollbackTx(tx)

	stmt := tx.StmtContext(ctx, o.resetTakenAtStmt)
	defer func() {
		if err := stmt.Close(); err != nil {
			log.Printf("eventhorizon: could not close SQLite outbox reset taken_at statement: %s", err)
		}
	}()
	if _, err := stmt.ExecContext(ctx); err != nil {
		return fmt.Errorf("could not reset outbox taken_at: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit taken_at reset: %w", err)
	}
	return nil
}

// Close stops accepting new work, lets in-flight HandleEvent calls finish and
// finalize, abandons queued deliveries (not marked done; redelivered after
// startup taken_at reset), then closes prepared statements.
// The caller owns the shared *sql.DB and is responsible for closing it.
func (o *Outbox) Close() error {
	o.shuttingDown.Store(true)
	o.cancel()
	o.wg.Wait()
	if o.dispatch != nil {
		// Wake idle workers so they observe stop; in-flight HandleEvent must
		// return on their own (handlers should not block forever in production).
		o.dispatch.wakeAll()
		o.dispatch.waitWorkers()
	}

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
		o.resetTakenAtStmt,
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

	// Initial sweep immediately after startup reset so pending rows are claimed
	// without waiting for watchCh, scheduleCh, or the periodic ticker.
	o.processUntilEmpty(context.Background())
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
		// Non-canceled context for in-flight work so Close can wait for the
		// current batch (including finalize) without aborting SQL mid-flight.
		// The loop still exits on o.done between batches.
		o.processUntilEmpty(context.Background())
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

func (o *Outbox) processUntilEmpty(ctx context.Context) {
	o.drainWatchChannel()

	for {
		select {
		case <-o.done:
			return
		default:
		}
		if o.shuttingDown.Load() {
			return
		}

		// Fetch is decoupled from completion: do not wait for handlers here.
		// Coordinators notify() when they free admission slots.
		claimed, err := o.fetchAndDispatch(ctx, false)
		if err != nil {
			o.sendError(ctx, err, nil)
			return
		}
		if claimed == 0 {
			return
		}

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

type handlerDispatchResult struct {
	eventID     uuid.UUID
	handlerType string
	err         error
	fatal       bool
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

// processBatch claims and dispatches a wave of records, then waits for those
// records' coordinators to finalize. Used by unit tests for synchronous
// progress. The live processor uses fetchAndDispatch without waiting.
func (o *Outbox) processBatch(ctx context.Context) (int, error) {
	return o.fetchAndDispatch(ctx, true)
}

// fetchAndDispatch admits records only when every delivery queue for the record
// can reserve a slot, claims taken_at, then enqueues non-blocking. Partial
// admission is per record in the fetch wave, never a partial handler set.
// If wait is true, blocks until claimed records finalize (test helper path).
func (o *Outbox) fetchAndDispatch(ctx context.Context, wait bool) (int, error) {
	if o.shuttingDown.Load() {
		return 0, nil
	}

	planned, err := o.planAndClaim(ctx)
	if err != nil {
		return 0, err
	}
	if len(planned) == 0 {
		return 0, nil
	}

	coords := make([]*recordCoordinator, 0, len(planned))
	for _, p := range planned {
		// planAndClaim only admits records with at least one delivery (including
		// rematch of empty-handlers rows). Never claim-and-delete unmatched work.
		if len(p.deliveries) == 0 {
			o.admission.release(p.record.ID.String())
			continue
		}
		coord := newRecordCoordinator(o, p.record, p.eventCtx, len(p.deliveries))
		for _, d := range p.deliveries {
			d.coord = coord
			o.dispatch.enqueue(d)
		}
		coords = append(coords, coord)
	}

	if wait {
		for _, c := range coords {
			c.wait()
		}
	}
	return len(planned), nil
}

type plannedRecord struct {
	record     *outboxDoc
	eventCtx   map[string]any
	deliveries []*delivery
	keys       []string
	// persistHandlers is set for rematch claims so the matched handler list is
	// written in the claim transaction before dispatch/finalize.
	persistHandlers bool
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

// dispatchQueueIdentity returns the internal queue key plus stable stats labels.
// shardLabel is "none" for Serial; for partitions it is the numeric shard index
// (never a correlation/aggregate id).
func dispatchQueueIdentity(handler *matcherHandler, event eh.Event) (queueKey, handlerLabel, shardLabel string) {
	handlerLabel = handler.HandlerType().String()
	if handler.dispatchMode != PartitionByAggregate {
		return handlerLabel, handlerLabel, serialShardLabel
	}

	partKey := eventPartitionKey(event)
	if partKey == "" {
		return handlerLabel, handlerLabel, serialShardLabel
	}

	shards := handler.partitionShards
	if shards < 1 {
		shards = defaultPartitionShards
	}
	shard := hashPartition(partKey, shards)
	shardLabel = fmt.Sprintf("%d", shard)
	queueKey = fmt.Sprintf("%s:%s", handlerLabel, shardLabel)
	return queueKey, handlerLabel, shardLabel
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

// planAndClaim pages due rows (ORDER BY available_at, created_at, id) with a
// bounded OFFSET/LIMIT within one claim transaction until admission is full or
// due candidates are exhausted. A full dispatch-key backlog larger than one page
// cannot hide later idle-key rows. Never claims a record that cannot fully
// reserve its delivery queues, and never claims a record whose stored handlers
// cannot all be resolved.
func (o *Outbox) planAndClaim(ctx context.Context) ([]plannedRecord, error) {
	free := o.admission.freeSlots()
	if free == 0 {
		return nil, nil
	}
	admitCap := free
	if admitCap > maxFetchBatch {
		admitCap = maxFetchBatch
	}
	pageSize := maxFetchBatch
	if pageSize < 1 {
		pageSize = 1
	}

	tx, err := o.db.Begin()
	if err != nil {
		return nil, err
	}
	defer rollbackTx(tx)

	now := time.Now()
	planned := make([]plannedRecord, 0, admitCap)
	offset := 0

	for len(planned) < admitCap {
		candidates, eventContexts, err := o.selectEventsForProcessing(ctx, tx, now, pageSize, offset)
		if err != nil {
			return nil, err
		}
		if len(candidates) == 0 {
			break
		}
		offset += len(candidates)

		for _, r := range candidates {
			if len(planned) >= admitCap {
				break
			}
			id := r.ID.String()
			if o.admission.contains(id) {
				continue
			}
			var (
				keys       []string
				deliveries []*delivery
				missing    []string
			)
			persistHandlers := false
			if len(r.Handlers) == 0 {
				// Rematch contract: empty handlers means re-match against the
				// current registration at claim time (replay of no_match DLQ).
				keys, deliveries = o.planRematchDeliveries(r, eventContexts[id])
				if len(deliveries) == 0 {
					o.sendError(ctx, fmt.Errorf(
						"outbox record %s rematch found no handlers (leaving unclaimed)",
						id,
					), r.Event)
					continue
				}
				// Invariant: finalize remainingHandlers(r.Handlers, …) and
				// per-handler retry/DLQ require the matched set on the record.
				// Leaving Handlers=[] would treat any outcome as fully done and
				// silently delete the row (including retryable failures).
				matched := make([]string, 0, len(deliveries))
				for _, d := range deliveries {
					matched = append(matched, d.handler.HandlerType().String())
				}
				r.Handlers = matched
				persistHandlers = true
			} else {
				keys, deliveries, missing = o.planDeliveries(r, eventContexts[id])
				if len(missing) > 0 {
					// No silent loss: leave unclaimed for operator/restart visibility.
					o.sendError(ctx, fmt.Errorf(
						"outbox record %s has unresolvable handlers %v (leaving unclaimed)",
						id, missing,
					), r.Event)
					continue
				}
			}
			// Atomic whole-record reservation: all delivery queues + coordinator.
			if !o.dispatch.tryReserveKeys(keys, deliveries) {
				continue
			}
			if !o.admission.tryAdmit(id) {
				o.dispatch.releaseKeys(keys)
				continue
			}
			planned = append(planned, plannedRecord{
				record:          r,
				eventCtx:        eventContexts[id],
				deliveries:      deliveries,
				keys:            keys,
				persistHandlers: persistHandlers,
			})
		}

		if len(candidates) < pageSize {
			break // exhausted due set
		}
	}

	if len(planned) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("could not commit empty admission transaction: %w", err)
		}
		return nil, nil
	}

	updateStmt := tx.StmtContext(ctx, o.updateTakenAtStmt)
	defer func() { _ = updateStmt.Close() }()
	handlersStmt := tx.StmtContext(ctx, o.updateHandlersStmt)
	defer func() { _ = handlersStmt.Close() }()

	for _, p := range planned {
		// Persist rematched handler list in the same claim TX as taken_at so a
		// crash after claim still has correct remaining semantics on restart.
		if p.persistHandlers {
			handlersBlob, err := jsoniter.Marshal(p.record.Handlers)
			if err != nil {
				o.rollbackPlanned(planned)
				return nil, fmt.Errorf("could not marshal rematched handlers: %w", err)
			}
			if _, err := handlersStmt.ExecContext(ctx, string(handlersBlob), p.record.ID.String()); err != nil {
				o.rollbackPlanned(planned)
				return nil, fmt.Errorf("could not persist rematched handlers: %w", err)
			}
		}
		if _, err := updateStmt.ExecContext(ctx, now, p.record.ID.String()); err != nil {
			o.rollbackPlanned(planned)
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		o.rollbackPlanned(planned)
		return nil, fmt.Errorf("could not commit transaction locking events: %w", err)
	}

	return planned, nil
}

func (o *Outbox) rollbackPlanned(planned []plannedRecord) {
	for _, p := range planned {
		o.dispatch.releaseKeys(p.keys)
		o.admission.release(p.record.ID.String())
	}
}

func (o *Outbox) selectEventsForProcessing(ctx context.Context, tx *sql.Tx, now time.Time, limit, offset int) ([]*outboxDoc, map[string]map[string]any, error) {
	if limit < 1 {
		limit = 1
	}
	if offset < 0 {
		offset = 0
	}
	stmt := tx.StmtContext(ctx, o.selectEventsStmt)
	defer func() { _ = stmt.Close() }()

	rows, err := stmt.QueryContext(ctx, now.Add(-PeriodicSweepAge), now, limit, offset)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = rows.Close() }()

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

func (o *Outbox) updateEventDB(ctx context.Context, res processedResult) {
	r := res.r
	// Admission is released by the record coordinator after finalize (or abandon).

	successful := stringSet(res.successfulHandlers)
	terminalFailures, retryableFailures := o.classifyFailures(r, res.failedHandlers)
	// Terminal handlers are always removed on successful finalize; partial DLQ
	// success is no longer possible because inserts share the same transaction.
	remaining := remainingHandlers(r.Handlers, successful, stringSetFromMap(terminalFailures))
	intendedRemainingForDLQ := remaining

	pending, err := o.finalizeEvent(ctx, r, terminalFailures, intendedRemainingForDLQ, remaining, len(retryableFailures) > 0)
	if err != nil {
		o.sendError(ctx, err, r.Event)
		return
	}
	if len(terminalFailures) > 0 {
		o.sendError(ctx, errEventHandlerMovedToDeadLetters, r.Event)
	}
	o.exportDeadLetters(ctx, r.Event, pending)
	if len(retryableFailures) > 0 {
		// Wake the delayed-dispatch timer after a committed retry schedule.
		o.notifySchedule()
	}
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

// finalizeEvent commits DLQ rows, handler list updates (or delete), and retry
// fields in a single transaction. File export is intentionally deferred to the
// caller so it only runs after a successful commit.
func (o *Outbox) finalizeEvent(
	ctx context.Context,
	r *outboxDoc,
	terminalFailures map[string]handlerFailure,
	intendedRemainingHandlers []string,
	remaining []string,
	scheduleRetry bool,
) ([]dl.Record, error) {
	tx, err := o.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("could not begin finalize transaction: %w", err)
	}
	defer rollbackTx(tx)

	pending := make([]dl.Record, 0, len(terminalFailures))
	for handlerType, failure := range terminalFailures {
		record, err := o.insertOutboxDeadLetterTx(ctx, tx, r, handlerType, intendedRemainingHandlers, failure.err)
		if err != nil {
			return nil, err
		}
		pending = append(pending, record)
	}

	if len(remaining) == 0 {
		deleteStmt := tx.StmtContext(ctx, o.deleteEventStmt)
		defer func() {
			if err := deleteStmt.Close(); err != nil {
				log.Printf("eventhorizon: could not close SQLite outbox delete statement: %s", err)
			}
		}()
		if _, err := deleteStmt.ExecContext(ctx, r.ID.String()); err != nil {
			return nil, fmt.Errorf("could not delete fully processed event: %w", err)
		}
	} else {
		newHandlersBlob, err := jsoniter.Marshal(remaining)
		if err != nil {
			return nil, fmt.Errorf("could not marshal remaining handlers: %w", err)
		}
		updateStmt := tx.StmtContext(ctx, o.updateHandlersStmt)
		defer func() {
			if err := updateStmt.Close(); err != nil {
				log.Printf("eventhorizon: could not close SQLite outbox update handlers statement: %s", err)
			}
		}()
		if _, err := updateStmt.ExecContext(ctx, string(newHandlersBlob), r.ID.String()); err != nil {
			return nil, fmt.Errorf("could not update remaining handlers: %w", err)
		}
		if scheduleRetry {
			nextRetryCount := r.RetryCount + 1
			availableAt := time.Now().Add(o.retryBackoff.DelayFunc(int64(nextRetryCount)))
			retryStmt := tx.StmtContext(ctx, o.scheduleRetryStmt)
			defer func() {
				if err := retryStmt.Close(); err != nil {
					log.Printf("eventhorizon: could not close SQLite outbox schedule retry statement: %s", err)
				}
			}()
			if _, err := retryStmt.ExecContext(ctx, availableAt, r.ID.String()); err != nil {
				return nil, fmt.Errorf("could not schedule event retry: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("could not commit finalize transaction: %w", err)
	}
	return pending, nil
}

func (o *Outbox) exportDeadLetters(ctx context.Context, event eh.Event, pending []dl.Record) {
	if o.deadLetterExport == nil || len(pending) == 0 {
		return
	}
	for _, key := range pending {
		// After ON CONFLICT the durable row may predate this attempt. Always
		// export the exact DB record, never the in-memory insert candidate.
		resolved, ok, err := o.loadDeadLetter(ctx, key.Source, key.OutboxID, key.HandlerType)
		if err != nil {
			o.sendError(ctx, fmt.Errorf("could not resolve outbox dead letter for export: %w", err), event)
			continue
		}
		if !ok {
			continue
		}
		if resolved.ExportedAt != nil {
			continue
		}
		if err := o.deadLetterExport.ExportDeadLetter(ctx, resolved); err != nil {
			o.sendError(ctx, fmt.Errorf("could not export outbox dead letter: %w", err), event)
			continue
		}
		if err := o.markDeadLetterExported(ctx, resolved.ID, time.Now()); err != nil {
			o.sendError(ctx, fmt.Errorf("could not mark outbox dead letter exported: %w", err), event)
		}
	}
}

// loadDeadLetter loads the durable dead_letters row for the unique key
// (source, outbox_id, handler_type). Export must use this row so body/error/
// timestamps match what is stored after an idempotent ON CONFLICT insert.
func (o *Outbox) loadDeadLetter(ctx context.Context, source, outboxID, handlerType string) (dl.Record, bool, error) {
	var record dl.Record
	var exportedAt sql.NullTime
	err := o.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT id, source, event_type, aggregate_id, handler_type, outbox_id,
		       remaining_handlers, blob, error, retry_count, created_at, dead_at, exported_at
		FROM %s
		WHERE source = ? AND outbox_id = ? AND handler_type = ?
	`, o.deadLetterTable), source, outboxID, handlerType).Scan(
		&record.ID,
		&record.Source,
		&record.EventType,
		&record.AggregateID,
		&record.HandlerType,
		&record.OutboxID,
		&record.RemainingHandlers,
		&record.Blob,
		&record.Error,
		&record.RetryCount,
		&record.CreatedAt,
		&record.DeadAt,
		&exportedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return dl.Record{}, false, nil
	}
	if err != nil {
		return dl.Record{}, false, err
	}
	if exportedAt.Valid {
		t := exportedAt.Time
		record.ExportedAt = &t
	}
	return record, true, nil
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

func (o *Outbox) insertOutboxDeadLetterTx(ctx context.Context, tx *sql.Tx, r *outboxDoc, handlerType string, remainingHandlers []string, reason string) (dl.Record, error) {
	eventBlob, err := o.codec.MarshalEvent(ctx, r.Event)
	if err != nil {
		return dl.Record{}, fmt.Errorf("could not marshal dead letter event: %w", err)
	}
	remainingHandlersBlob, err := jsoniter.Marshal(remainingHandlers)
	if err != nil {
		return dl.Record{}, fmt.Errorf("could not marshal dead letter handlers: %w", err)
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
	insertStmt := tx.StmtContext(ctx, o.insertDeadLetterStmt)
	defer func() {
		if err := insertStmt.Close(); err != nil {
			log.Printf("eventhorizon: could not close SQLite outbox dead letter statement: %s", err)
		}
	}()
	if _, err := insertStmt.ExecContext(ctx,
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
		return dl.Record{}, fmt.Errorf("could not insert outbox dead letter: %w", err)
	}
	return record, nil
}

func (o *Outbox) markDeadLetterExported(ctx context.Context, id string, exportedAt time.Time) error {
	if _, err := o.db.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET exported_at = ? WHERE id = ?`, o.deadLetterTable), exportedAt, id); err != nil {
		return err
	}
	return nil
}
