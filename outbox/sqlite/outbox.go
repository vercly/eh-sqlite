package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vercly/eh-sqlite/backoff"
	"github.com/vercly/eh-sqlite/context/sqlite"
	dl "github.com/vercly/eh-sqlite/deadletter"
	"github.com/vercly/eh-sqlite/internal/deadletter"
	"github.com/vercly/eh-sqlite/schema"

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

	// PeriodicSweepAge is the taken_at timeout: a claimed delivery older than
	// this and not admitted in this process is treated as stale and re-claimed.
	PeriodicSweepAge = 15 * time.Second
)

const metadataAvailableAtKey = "outbox.available_at"

// MetadataPartitionKey is the explicit partition source for PartitionByAggregate.
// When present it wins over the aggregate id and any correlation metadata; an
// empty value means "no partition" (Serial fallback), never a random shard.
// Publishers whose correlation is not an eventhorizon id (e.g. the legacy
// Rabbit bridge) set it from the raw correlation string.
const MetadataPartitionKey = "outbox.partition_key"

// PartitionKeyResolver derives a partition key from an event envelope
// (metadata plus aggregate id text) for publishers whose correlation is not
// expressed as the aggregate id or as MetadataPartitionKey — typically rows
// published before that key existed. It runs after the explicit metadata key
// and before the aggregate-id/correlation-metadata fallbacks, on the live
// publish path and in the startup reconcile of existing publications.
// Returning ok=false defers to the fallbacks; returning ("", true) means
// "no partition" (Serial).
type PartitionKeyResolver func(metadata map[string]any, aggregateID string) (key string, ok bool)

// claimQuantum bounds how many deliveries one dispatch key may claim per
// round-robin turn. Internal constant by agreement (no ENV).
const claimQuantum = 8

// maxFetchBatch is the historical upper bound of deliveries claimed per pass
// and the default admission limit (in-flight deliveries).
const maxFetchBatch = 50

// finalizeRetryDelays is the bounded backoff for finalize SQL retries with the
// saved handler outcome (the handler is never re-run to retry finalization).
var finalizeRetryDelays = []time.Duration{50 * time.Millisecond, 200 * time.Millisecond, time.Second}

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

// Outbox implements an eventhorizon.Outbox for SQLite (v2: one delivery row
// per recipient sharing one publication payload).
type Outbox struct {
	db *sql.DB
	// outboxTable is the table prefix (and the v1 table name before migration).
	outboxTable       string
	publicationsTable string
	deliveriesTable   string
	deadLetterTable   string
	handlers          []*matcherHandler
	handlersByType    map[eh.EventHandlerType]*matcherHandler
	handlersMu        sync.RWMutex
	watchCh           chan struct{}
	scheduleCh        chan struct{}
	errCh             chan error
	done              <-chan struct{}
	cancel            context.CancelFunc
	wg                sync.WaitGroup
	// registrationClosed is set on the first Start/StartChecked attempt and
	// never reopened. AddHandler fails once this is true.
	registrationClosed atomic.Bool
	// processorRunning is true only after a successful startup and fetcher
	// launch. HandleEvent (publish) requires this flag.
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
	partitionKey     PartitionKeyResolver
	admission        *recordAdmission
	dispatch         *dispatchRegistry
	dispatchStats    DispatchStats
	admissionStats   AdmissionStats
	// handleSem bounds concurrent HandleEvent calls (FIFO fair across keys).
	handleSem *fairSem
	// claimMu serializes claim passes (fetcher goroutine and test helpers) and
	// protects claimCursor.
	claimMu     sync.Mutex
	claimCursor string
	// startMu serializes StartChecked so two concurrent starts cannot run the
	// startup reset after the first start already launched the fetcher.
	startMu sync.Mutex
	// claimKeys is the sorted set of every dispatch key the registration can
	// produce (handler, or handler plus each shard). Computed at start; the
	// claim pass probes each key with one indexed SELECT instead of scanning
	// the whole due set.
	claimKeys []string

	// beforeFinalizeCommit is a test hook invoked before each finalize commit
	// attempt (attempt is 1-based); a non-nil error aborts that attempt.
	beforeFinalizeCommit func(deliveryID string, attempt int) error
	// beforeReleaseClaim is a test hook invoked before the safe claim release.
	beforeReleaseClaim func(deliveryID string) error
	// beforeClaimCommit is a test hook invoked before a claim pass commits
	// (after reservations/admissions were taken); a non-nil error aborts the
	// pass, which must release every reservation it took.
	beforeClaimCommit func(reserved int) error
	// finalizeSleep lets tests shorten the finalize retry backoff.
	finalizeSleep func(time.Duration)

	insertPublicationStmt *sql.Stmt
	insertDeliveryStmt    *sql.Stmt
	insertDeadLetterStmt  *sql.Stmt
	claimKeyStmt          *sql.Stmt
	staleClaimsStmt       *sql.Stmt
	resetStaleStmt        *sql.Stmt
	sentinelsStmt         *sql.Stmt
	updateTakenAtStmt     *sql.Stmt
	deleteDeliveryStmt    *sql.Stmt
	gcPublicationStmt     *sql.Stmt
	scheduleRetryStmt     *sql.Stmt
	releaseClaimStmt      *sql.Stmt
	markUnresolvedStmt    *sql.Stmt
	nextAvailableAtStmt   *sql.Stmt
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

// NewOutbox creates a new Outbox. It creates the v2 tables (publications,
// deliveries, migration bookkeeping, dead letters) but does not migrate a v1
// table nor start the processor; StartChecked does both.
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
		// Default admission capacity: in-flight deliveries in this process.
		admission:     newRecordAdmission(maxFetchBatch),
		finalizeSleep: time.Sleep,
	}

	for _, option := range options {
		if err := option(o); err != nil {
			cancel()
			return nil, fmt.Errorf("error while applying option: %w", err)
		}
	}
	o.publicationsTable = publicationsTableFor(o.outboxTable)
	o.deliveriesTable = deliveriesTableFor(o.outboxTable)
	if o.dispatchStats != nil {
		if as, ok := o.dispatchStats.(AdmissionStats); ok {
			o.admissionStats = as
		}
	}

	o.handleSem = newFairSem(max(o.maxGoroutines, 1))
	o.dispatch = newDispatchRegistry(o, o.queueDepth, o.dispatchStats)

	if err := ensureV2Tables(context.Background(), db, o.outboxTable); err != nil {
		cancel()
		return nil, err
	}
	if err := deadletter.EnsureSchema(o.db, o.deadLetterTable); err != nil {
		cancel()
		return nil, err
	}
	if err := o.prepareStatements(); err != nil {
		cancel()
		return nil, fmt.Errorf("could not prepare statements: %w", err)
	}

	return o, nil
}

type Option func(*Outbox) error

// WithTableName sets the table prefix: <prefix>_publications, <prefix>_deliveries
// and the v1 source table <prefix>. Default "outbox".
func WithTableName(outbox string) Option {
	return func(o *Outbox) error {
		if err := schema.ValidateIdent(outbox); err != nil {
			return err
		}
		o.outboxTable = outbox
		return nil
	}
}

// WithMaxRetries sets the maximum number of error retries before a delivery is dead-lettered.
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
// queue locks and must not block delivery. A collector that also implements
// AdmissionStats receives admission, claim-skip and finalize observations.
func WithDispatchStats(collector DispatchStats) Option {
	return func(o *Outbox) error {
		o.dispatchStats = collector
		return nil
	}
}

// WithAdmissionLimit sets how many deliveries may be admitted (claimed and
// in-flight: queued or executing, not yet finalized) at once in this process.
// Admission is process-local and is not a multi-process lock. Values below 1
// are treated as 1. Default is 50.
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

// WithPartitionKeyResolver installs a PartitionKeyResolver (see its doc).
// StartChecked re-derives partition_key for every publication that still has
// deliveries and recomputes their dispatch keys before dispatch, so changing
// the resolver never leaves stale shards behind.
func WithPartitionKeyResolver(resolver PartitionKeyResolver) Option {
	return func(o *Outbox) error {
		o.partitionKey = resolver
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
	pubs, dels := o.publicationsTable, o.deliveriesTable
	prepare := func(dst **sql.Stmt, what, query string) bool {
		if err != nil {
			return false
		}
		*dst, err = o.db.Prepare(query)
		if err != nil {
			err = fmt.Errorf("could not prepare %s statement: %w", what, err)
			return false
		}
		return true
	}

	prepare(&o.insertPublicationStmt, "insert publication", fmt.Sprintf(`
		INSERT INTO %s (publication_id, event_type, aggregate_id, partition_key, event_blob, created_at, origin, origin_ref)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, pubs))
	prepare(&o.insertDeliveryStmt, "insert delivery", fmt.Sprintf(`
		INSERT INTO %s (id, publication_id, handler_type, dispatch_key, dispatch_config, event_type, aggregate_id,
		                created_at, available_at, taken_at, retry_count, unresolved_at, legacy_outbox_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL, ?)`, dels))
	// ON CONFLICT makes terminal DLQ inserts idempotent under the unique key
	// (source, outbox_id, handler_type). NULL outbox_id rows (no-match) never
	// conflict with each other in SQLite.
	prepare(&o.insertDeadLetterStmt, "insert dead letter", fmt.Sprintf(`
		INSERT INTO %s (id, source, event_type, aggregate_id, handler_type, outbox_id, remaining_handlers, blob, error,
		                retry_count, created_at, dead_at, publication_id, legacy_outbox_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(source, outbox_id, handler_type) DO NOTHING`, o.deadLetterTable))
	// Per-key FIFO: available_at first so delayed/retry rows re-enter by
	// eligibility time, then the stable sequence. Served entirely by
	// idx_<dels>_claim (dispatch_key =, taken_at IS NULL, available_at range,
	// seq order): no sort, no scan of other keys. Stale claims are reset by
	// reclaimStale before this runs, so only taken_at IS NULL is selected.
	// Active ids are appended as an extra NOT IN clause at call time.
	prepare(&o.claimKeyStmt, "claim key", o.claimKeyQuery(0))
	// Stale claims: rows claimed longer than PeriodicSweepAge ago. Range on
	// idx_<dels>_taken_available (IS NOT NULL lower bound, < stale upper bound).
	prepare(&o.staleClaimsStmt, "stale claims", fmt.Sprintf(`
		SELECT id FROM %s WHERE taken_at IS NOT NULL AND taken_at < ?`, dels))
	prepare(&o.resetStaleStmt, "reset stale claim", fmt.Sprintf(`
		UPDATE %s SET taken_at = NULL WHERE id = ? AND taken_at IS NOT NULL AND taken_at < ?`, dels))
	prepare(&o.sentinelsStmt, "rematch sentinels", fmt.Sprintf(`
		SELECT d.seq, d.id, d.publication_id, '', d.event_type, d.aggregate_id, d.created_at, d.available_at, d.taken_at,
		       d.retry_count, d.legacy_outbox_id, p.event_blob, p.partition_key
		FROM %[1]s d INDEXED BY idx_%[1]s_sentinel JOIN %[2]s p ON p.publication_id = d.publication_id
		WHERE d.handler_type IS NULL AND d.unresolved_at IS NULL AND d.taken_at IS NULL AND d.available_at <= ?
		ORDER BY d.available_at ASC, d.seq ASC LIMIT ?`, dels, pubs))
	prepare(&o.updateTakenAtStmt, "update taken_at", fmt.Sprintf(`UPDATE %s SET taken_at = ? WHERE id = ?`, dels))
	prepare(&o.deleteDeliveryStmt, "delete delivery", fmt.Sprintf(`DELETE FROM %s WHERE id = ?`, dels))
	prepare(&o.gcPublicationStmt, "gc publication", fmt.Sprintf(`
		DELETE FROM %s WHERE publication_id = ? AND NOT EXISTS (SELECT 1 FROM %s WHERE publication_id = ?)`, pubs, dels))
	// Absolute retry_count (saved outcome) so a retried finalize after an
	// uncertain commit cannot double-increment.
	prepare(&o.scheduleRetryStmt, "schedule retry", fmt.Sprintf(`
		UPDATE %s SET retry_count = ?, available_at = ?, taken_at = NULL WHERE id = ?`, dels))
	prepare(&o.releaseClaimStmt, "release claim", fmt.Sprintf(`
		UPDATE %s SET taken_at = NULL, available_at = ? WHERE id = ?`, dels))
	prepare(&o.markUnresolvedStmt, "mark unresolved", fmt.Sprintf(`
		UPDATE %s SET unresolved_at = COALESCE(unresolved_at, ?), dispatch_key = NULL, taken_at = NULL WHERE id = ?`, dels))
	// Walks idx_<dels>_available from the first row after now; due rows are
	// skipped by the index range, not by a scan.
	prepare(&o.nextAvailableAtStmt, "next available", fmt.Sprintf(`
		SELECT available_at
		FROM %s INDEXED BY idx_%s_available
		WHERE available_at > ?
		  AND unresolved_at IS NULL AND (dispatch_key IS NOT NULL OR handler_type IS NULL)
		  AND (taken_at IS NULL OR taken_at < ?)
		ORDER BY available_at ASC, seq ASC LIMIT 1`, dels, dels))
	return err
}

// claimKeyQuery builds the per-key claim SELECT with n placeholders for active
// delivery ids to exclude (n == 0 → no exclusion clause).
func (o *Outbox) claimKeyQuery(n int) string {
	exclude := ""
	if n > 0 {
		exclude = " AND d.id NOT IN (?" + strings.Repeat(",?", n-1) + ")"
	}
	return fmt.Sprintf(`
		SELECT d.seq, d.id, d.publication_id, d.handler_type, d.event_type, d.aggregate_id, d.created_at, d.available_at, d.taken_at,
		       d.retry_count, d.legacy_outbox_id, p.event_blob, p.partition_key
		FROM %s d JOIN %s p ON p.publication_id = d.publication_id
		WHERE d.dispatch_key = ? AND d.taken_at IS NULL AND d.unresolved_at IS NULL
		  AND d.available_at <= ?%s
		ORDER BY d.available_at ASC, d.seq ASC LIMIT ?`, o.deliveriesTable, o.publicationsTable, exclude)
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

// deliveryDoc is the in-memory representation of a claimed delivery row plus
// its decoded publication payload.
type deliveryDoc struct {
	Seq            int64
	ID             string
	PublicationID  string
	HandlerType    string // "" for a rematch sentinel
	EventType      string
	AggregateID    string
	CreatedAt      time.Time
	AvailableAt    time.Time
	TakenAt        sql.NullTime
	RetryCount     int
	LegacyOutboxID sql.NullString
	PartitionKey   string
	EventBlob      string
	Event          eh.Event
	EventCtx       map[string]any
}

// HandleEvent implements the HandleEvent method of the eventhorizon.EventHandler interface.
// Publishing requires a successful Start/StartChecked (processor running).
func (o *Outbox) HandleEvent(ctx context.Context, event eh.Event) error {
	if !o.processorRunning.Load() {
		return ErrOutboxNotStarted
	}

	eventBlob, err := o.codec.MarshalEvent(ctx, event)
	if err != nil {
		return fmt.Errorf("could not marshal event: %w", err)
	}
	now := schema.UTC(time.Now())
	availableAt, err := availableAtFor(ctx, event, now)
	if err != nil {
		return err
	}

	matching := o.matchingHandlers(event)
	return o.storeEvent(ctx, event, eventBlob, now, availableAt, matching)
}

func (o *Outbox) matchingHandlers(event eh.Event) []*matcherHandler {
	o.handlersMu.RLock()
	defer o.handlersMu.RUnlock()

	matching := make([]*matcherHandler, 0)
	for _, mh := range o.handlers {
		if mh.Match(event) {
			matching = append(matching, mh)
		}
	}
	return matching
}

func (o *Outbox) storeEvent(ctx context.Context, event eh.Event, eventBlob []byte, now, availableAt time.Time, matching []*matcherHandler) error {
	tx, txOk, err := o.txForEvent(ctx)
	if err != nil {
		return err
	}
	if !txOk {
		defer rollbackTx(tx)
	}

	if len(matching) == 0 {
		if err := o.insertNoMatchDeadLetter(ctx, tx, event, eventBlob, now); err != nil {
			return err
		}
		return commitOwnedTx(tx, txOk)
	}

	publicationID := uuid.New().String()
	partitionKey := o.partitionKeyFor(event.Metadata(), event.AggregateID().String())
	pubStmt := tx.StmtContext(ctx, o.insertPublicationStmt)
	defer closeStmt(pubStmt, "insert publication")
	if _, err := pubStmt.ExecContext(ctx,
		publicationID, event.EventType().String(), event.AggregateID().String(), partitionKey,
		string(eventBlob), now, originPublish, sql.NullString{}); err != nil {
		return fmt.Errorf("could not insert publication into outbox: %w", err)
	}
	delStmt := tx.StmtContext(ctx, o.insertDeliveryStmt)
	defer closeStmt(delStmt, "insert delivery")
	for _, mh := range matching {
		key, _, _ := dispatchKeyFor(mh, partitionKey)
		if _, err := delStmt.ExecContext(ctx,
			uuid.New().String(), publicationID, mh.HandlerType().String(), key, dispatchConfigFor(mh),
			event.EventType().String(), event.AggregateID().String(),
			now, availableAt, 0, sql.NullString{}); err != nil {
			return fmt.Errorf("could not insert delivery into outbox: %w", err)
		}
	}

	if err := commitOwnedTx(tx, txOk); err != nil {
		return err
	}
	if !txOk {
		o.notify()
	}
	return nil
}

func closeStmt(stmt *sql.Stmt, what string) {
	if err := stmt.Close(); err != nil {
		log.Printf("eventhorizon: could not close SQLite outbox %s statement: %s", what, err)
	}
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

// availableAtFor resolves the delivery eligibility time (UTC). Metadata key
// "outbox.available_at" wins over the context value; past values clamp to now.
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
			return schema.UTC(availableAt), nil
		}
	}

	if raw, ok := ctx.Value(availableAtContextKey{}).(time.Time); ok {
		if raw.Before(now) {
			return now, nil
		}
		return schema.UTC(raw), nil
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
	defer closeStmt(insertStmt, "dead letter")

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
		sql.NullString{},
		sql.NullString{},
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

// StartChecked closes handler registration, migrates a v1 table if present,
// then in one startup transaction resets every taken_at, clears sentinel
// unresolved markers and recomputes dispatch keys / unresolved state for every
// delivery row against the current registration, and only then starts the
// fetcher. On any failure the fetcher is not started and publish stays blocked
// (ErrOutboxNotStarted); registration stays closed and a later StartChecked
// may retry. After a successful start, further calls are no-ops.
// Scope is one process per DB file; multi-process locking is not provided.
func (o *Outbox) StartChecked() error {
	o.startMu.Lock()
	defer o.startMu.Unlock()

	o.handlersMu.Lock()
	// Close registration before startup work so late AddHandler cannot race in.
	o.registrationClosed.Store(true)
	if o.processorRunning.Load() {
		o.handlersMu.Unlock()
		return nil
	}
	o.handlersMu.Unlock()

	ctx := context.Background()
	if _, err := Migrate(ctx, o.db, o.outboxTable); err != nil {
		return fmt.Errorf("could not migrate outbox schema on start: %w", err)
	}
	if err := verifyMigrated(ctx, o.db, o.outboxTable); err != nil {
		return err
	}
	unresolved, err := o.startupReconcile(ctx)
	if err != nil {
		return fmt.Errorf("could not reconcile outbox deliveries on start: %w", err)
	}
	for handlerType, count := range unresolved {
		o.sendError(ctx, fmt.Errorf("%w: handler %q has %d pending deliveries (left unclaimed)", ErrUnresolvedHandler, handlerType, count), nil)
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

// registeredDispatchKeys enumerates every key the registration can produce.
// Caller holds handlersMu.
func (o *Outbox) registeredDispatchKeys() []string {
	keys := make([]string, 0, len(o.handlers))
	for _, mh := range o.handlers {
		handler := mh.HandlerType().String()
		keys = append(keys, handler) // Serial, or partition fallback for an empty partition key
		if mh.dispatchMode == PartitionByAggregate {
			shards := mh.partitionShards
			if shards < 1 {
				shards = defaultPartitionShards
			}
			for i := range shards {
				keys = append(keys, fmt.Sprintf("%s:%d", handler, i))
			}
		}
	}
	sort.Strings(keys)
	return keys
}

// startupReconcile is the single fail-closed startup transaction:
//  1. reset ALL taken_at (previous process claims are void);
//  2. clear unresolved_at on rematch sentinels (new registration may match);
//  3. for every delivery with a handler type: recompute dispatch_key when the
//     registered handler's dispatch config changed or the key is NULL, mark
//     unresolved when the handler is not registered.
//
// Returns unresolved delivery counts per handler type.
func (o *Outbox) startupReconcile(ctx context.Context) (map[string]int64, error) {
	tx, err := o.beginWriteTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not begin startup transaction: %w", err)
	}
	defer rollbackTx(tx)

	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET taken_at = NULL WHERE taken_at IS NOT NULL`, o.deliveriesTable)); err != nil {
		return nil, fmt.Errorf("could not reset outbox taken_at: %w", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET unresolved_at = NULL WHERE handler_type IS NULL AND unresolved_at IS NOT NULL`, o.deliveriesTable)); err != nil {
		return nil, fmt.Errorf("could not reset sentinel unresolved markers: %w", err)
	}

	if err := o.reconcilePartitionKeys(ctx, tx); err != nil {
		return nil, err
	}

	handlers := o.snapshotHandlersByType()
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`
		SELECT d.id, d.handler_type, d.dispatch_key, d.dispatch_config, d.unresolved_at, p.partition_key
		FROM %s d JOIN %s p ON p.publication_id = d.publication_id
		WHERE d.handler_type IS NOT NULL`, o.deliveriesTable, o.publicationsTable))
	if err != nil {
		return nil, fmt.Errorf("could not read deliveries for reconcile: %w", err)
	}
	type fix struct {
		id, key, config string
		unresolved      bool
	}
	var fixes []fix
	unresolvedCounts := map[string]int64{}
	for rows.Next() {
		var (
			id, handlerType, partitionKey string
			key, config                   sql.NullString
			unresolvedAt                  sql.NullTime
		)
		if err := rows.Scan(&id, &handlerType, &key, &config, &unresolvedAt, &partitionKey); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("could not scan delivery for reconcile: %w", err)
		}
		mh := handlers[handlerType]
		if mh == nil {
			unresolvedCounts[handlerType]++
			if !unresolvedAt.Valid || key.Valid {
				fixes = append(fixes, fix{id: id, unresolved: true})
			}
			continue
		}
		wantKey, _, _ := dispatchKeyFor(mh, partitionKey)
		wantConfig := dispatchConfigFor(mh)
		if !key.Valid || key.String != wantKey || !config.Valid || config.String != wantConfig || unresolvedAt.Valid {
			fixes = append(fixes, fix{id: id, key: wantKey, config: wantConfig})
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	now := schema.UTC(time.Now())
	for _, f := range fixes {
		if f.unresolved {
			if _, err := tx.ExecContext(ctx, fmt.Sprintf(
				`UPDATE %s SET unresolved_at = COALESCE(unresolved_at, ?), dispatch_key = NULL WHERE id = ?`, o.deliveriesTable),
				now, f.id); err != nil {
				return nil, fmt.Errorf("could not mark delivery %s unresolved: %w", f.id, err)
			}
			continue
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(
			`UPDATE %s SET dispatch_key = ?, dispatch_config = ?, unresolved_at = NULL WHERE id = ?`, o.deliveriesTable),
			f.key, f.config, f.id); err != nil {
			return nil, fmt.Errorf("could not recompute dispatch key for delivery %s: %w", f.id, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("could not commit startup transaction: %w", err)
	}
	// The dispatch-key ring follows the registration reconciled here.
	o.handlersMu.Lock()
	o.claimKeys = o.registeredDispatchKeys()
	o.handlersMu.Unlock()
	return unresolvedCounts, nil
}

// reconcilePartitionKeys re-derives partition_key for every publication that
// still has deliveries, from its stored envelope and the current resolver
// rules, so rows published under older rules (or before MetadataPartitionKey
// existed) are re-sharded consistently before dispatch. Dispatch keys are
// recomputed by the caller from the updated partition keys.
func (o *Outbox) reconcilePartitionKeys(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`
		SELECT p.publication_id, p.partition_key, p.aggregate_id, p.event_blob
		FROM %s p
		WHERE EXISTS (SELECT 1 FROM %s d WHERE d.publication_id = p.publication_id)`,
		o.publicationsTable, o.deliveriesTable))
	if err != nil {
		return fmt.Errorf("could not read publications for partition reconcile: %w", err)
	}
	type fix struct{ id, key string }
	var fixes []fix
	for rows.Next() {
		var id, stored, aggregateID, blob string
		if err := rows.Scan(&id, &stored, &aggregateID, &blob); err != nil {
			_ = rows.Close()
			return fmt.Errorf("could not scan publication for partition reconcile: %w", err)
		}
		metadata, err := storedEnvelopeMetadata([]byte(blob))
		if err != nil {
			// Undecodable envelope: keep the stored key; claim flags the
			// delivery visibly when it cannot decode the event.
			continue
		}
		if want := o.partitionKeyFor(metadata, aggregateID); want != stored {
			fixes = append(fixes, fix{id: id, key: want})
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, f := range fixes {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET partition_key = ? WHERE publication_id = ?`, o.publicationsTable), f.key, f.id); err != nil {
			return fmt.Errorf("could not update partition key of publication %s: %w", f.id, err)
		}
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
		o.insertPublicationStmt,
		o.insertDeliveryStmt,
		o.insertDeadLetterStmt,
		o.claimKeyStmt,
		o.staleClaimsStmt,
		o.resetStaleStmt,
		o.sentinelsStmt,
		o.updateTakenAtStmt,
		o.deleteDeliveryStmt,
		o.gcPublicationStmt,
		o.scheduleRetryStmt,
		o.releaseClaimStmt,
		o.markUnresolvedStmt,
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

// AdmissionSnapshot returns the number of admitted (in-flight) deliveries and
// the admission limit.
func (o *Outbox) AdmissionSnapshot() (used, limit int) {
	return o.admission.snapshot()
}

func (o *Outbox) runContext() context.Context {
	return doneContext{done: o.done}
}

// beginWriteTx starts a transaction and immediately takes the write lock with
// a no-op write, before any read. A deferred transaction that reads first and
// upgrades to a write later gets SQLITE_BUSY without the busy handler when
// another connection committed in between; taking the lock first routes the
// wait through _busy_timeout instead. Equivalent to BEGIN IMMEDIATE without
// depending on the DSN's _txlock setting.
func (o *Outbox) beginWriteTx(ctx context.Context) (*sql.Tx, error) {
	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET seq = seq WHERE seq < 0`, o.deliveriesTable)); err != nil {
		rollbackTx(tx)
		return nil, fmt.Errorf("could not acquire write lock: %w", err)
	}
	return tx, nil
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

	// Initial sweep immediately after startup so pending rows are claimed
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
		// current pass without aborting SQL mid-flight.
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
	now := schema.UTC(time.Now())
	if err := o.nextAvailableAtStmt.QueryRowContext(ctx, now, now.Add(-PeriodicSweepAge)).Scan(&next); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return time.Time{}, false, nil
		}
		return time.Time{}, false, fmt.Errorf("could not query next available outbox delivery: %w", err)
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
		// Deliveries notify() when they free admission slots.
		claimed, err := o.fetchAndDispatch(ctx, false)
		if err != nil {
			o.sendError(ctx, err, nil)
			return
		}
		if claimed == 0 {
			return
		}
		// No pacing sleep: each pass does real work and stops on its own when
		// admission is full or nothing is due; completions wake the loop.
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

func (o *Outbox) observeAdmission() {
	if o.admissionStats == nil {
		return
	}
	used, limit := o.admission.snapshot()
	safeDispatchObserve(func() { o.admissionStats.ObserveAdmission(used, limit) })
}

func (o *Outbox) observeSkip(reason ClaimSkipReason, n int) {
	if o.admissionStats == nil || n <= 0 {
		return
	}
	safeDispatchObserve(func() {
		for range n {
			o.admissionStats.ObserveClaimSkip(reason)
		}
	})
}

func (o *Outbox) observeFinalize(outcome FinalizeOutcome, d time.Duration, retried bool, err error) {
	if o.admissionStats == nil {
		return
	}
	safeDispatchObserve(func() { o.admissionStats.ObserveFinalize(outcome, d, retried, err) })
}

// scanDeliveryRow scans one row of claimKeyQuery / sentinelsStmt shape and
// decodes the payload.
func (o *Outbox) scanDeliveryRow(rows *sql.Rows) (*deliveryDoc, error) {
	var (
		d           deliveryDoc
		handlerType sql.NullString
		createdAt   sql.NullTime
		availableAt sql.NullTime
	)
	if err := rows.Scan(&d.Seq, &d.ID, &d.PublicationID, &handlerType, &d.EventType, &d.AggregateID,
		&createdAt, &availableAt, &d.TakenAt, &d.RetryCount, &d.LegacyOutboxID, &d.EventBlob, &d.PartitionKey); err != nil {
		return nil, fmt.Errorf("could not scan delivery row: %w", err)
	}
	d.HandlerType = handlerType.String
	d.CreatedAt = createdAt.Time
	d.AvailableAt = availableAt.Time
	return &d, nil
}

// decodeDelivery unmarshals the payload into d.Event / d.EventCtx.
func (o *Outbox) decodeDelivery(d *deliveryDoc) error {
	event, _, err := o.codec.UnmarshalEvent(o.runContext(), []byte(d.EventBlob))
	if err != nil {
		return fmt.Errorf("could not unmarshal event blob: %w", err)
	}
	eventCtx, err := eventContextValues([]byte(d.EventBlob))
	if err != nil {
		return err
	}
	d.Event = event
	d.EventCtx = eventCtx
	return nil
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

type handlerDispatchResult struct {
	err   error
	fatal bool
}

// processBatch claims one pass and waits for the claimed deliveries to
// finalize. Used by unit tests for synchronous progress. The live processor
// uses fetchAndDispatch without waiting.
func (o *Outbox) processBatch(ctx context.Context) (int, error) {
	return o.fetchAndDispatch(ctx, true)
}

// fetchAndDispatch runs one claim pass (round-robin over active dispatch keys
// plus rematch sentinels) and enqueues the claimed deliveries. Returns the
// number of deliveries claimed plus sentinels expanded (so callers loop while
// progress is made). If wait is true, blocks until the claimed deliveries
// finalize (test helper path).
func (o *Outbox) fetchAndDispatch(ctx context.Context, wait bool) (int, error) {
	if o.shuttingDown.Load() {
		return 0, nil
	}

	planned, expanded, err := o.claimPass(ctx)
	if err != nil {
		return 0, err
	}
	for _, d := range planned {
		o.dispatch.enqueue(d)
	}
	if wait {
		for _, d := range planned {
			d.wait()
		}
	}
	return len(planned) + expanded, nil
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

// dispatchKeyFor returns the queue key plus stable stats labels for a handler
// and a publication partition key. shardLabel is "none" for Serial; for
// partitions it is the numeric shard index (never a correlation/aggregate id).
func dispatchKeyFor(handler *matcherHandler, partitionKey string) (queueKey, handlerLabel, shardLabel string) {
	handlerLabel = handler.HandlerType().String()
	if handler.dispatchMode != PartitionByAggregate || partitionKey == "" {
		return handlerLabel, handlerLabel, serialShardLabel
	}
	shards := handler.partitionShards
	if shards < 1 {
		shards = defaultPartitionShards
	}
	shard := hashPartition(partitionKey, shards)
	shardLabel = fmt.Sprintf("%d", shard)
	queueKey = fmt.Sprintf("%s:%s", handlerLabel, shardLabel)
	return queueKey, handlerLabel, shardLabel
}

// dispatchConfigFor is the fingerprint stored with a delivery so a changed
// mode/shard configuration is detected at startup.
func dispatchConfigFor(handler *matcherHandler) string {
	if handler.dispatchMode != PartitionByAggregate {
		return "mode=serial"
	}
	shards := handler.partitionShards
	if shards < 1 {
		shards = defaultPartitionShards
	}
	return fmt.Sprintf("mode=partition;shards=%d", shards)
}

// dispatchQueueIdentity is kept for callers holding an event (tests); it is
// dispatchKeyFor on the event's partition key.
func dispatchQueueIdentity(handler *matcherHandler, event eh.Event) (queueKey, handlerLabel, shardLabel string) {
	return dispatchKeyFor(handler, eventPartitionKey(event))
}

// partitionKeyFor resolves the partition key for a live or stored envelope:
// explicit metadata key, then the configured resolver, then the aggregate id,
// then correlation metadata, else "" (Serial).
func (o *Outbox) partitionKeyFor(metadata map[string]any, aggregateID string) string {
	if explicit, ok := explicitPartitionKey(metadata); ok {
		return explicit
	}
	if o.partitionKey != nil {
		if key, ok := o.partitionKey(metadata, aggregateID); ok {
			return strings.TrimSpace(key)
		}
	}
	return fallbackPartitionKey(metadata, aggregateID)
}

func eventPartitionKey(event eh.Event) string {
	if explicit, ok := explicitPartitionKey(event.Metadata()); ok {
		return explicit
	}
	return fallbackPartitionKey(event.Metadata(), event.AggregateID().String())
}

// fallbackPartitionKey is the historical derivation: aggregate id, else
// correlation metadata, else none.
func fallbackPartitionKey(metadata map[string]any, aggregateID string) string {
	if id, err := uuid.Parse(aggregateID); err == nil && id != uuid.Nil {
		return id.String()
	}
	for _, key := range []string{"correlation_id", "CorrelationId", "correlationId", "x-correlation-id"} {
		if value, ok := metadata[key]; ok {
			if partitionKey := metadataPartitionKey(value); partitionKey != "" {
				return partitionKey
			}
		}
	}
	return ""
}

// explicitPartitionKey reports the MetadataPartitionKey value when the key is
// present (an empty string is a deliberate Serial fallback).
func explicitPartitionKey(metadata map[string]any) (string, bool) {
	if metadata == nil {
		return "", false
	}
	value, ok := metadata[MetadataPartitionKey]
	if !ok {
		return "", false
	}
	if value == nil {
		return "", true
	}
	return metadataPartitionKey(value), true
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

func (o *Outbox) dispatchHandler(ctx context.Context, d *delivery) handlerDispatchResult {
	if err := d.handler.HandleEvent(ctx, d.doc.Event); err != nil {
		severity := GetSeverity(err)
		wrappedErr := fmt.Errorf("could not handle event (%s): %w", d.handler.HandlerType(), err)
		o.sendError(ctx, wrappedErr, d.doc.Event)
		return handlerDispatchResult{err: wrappedErr, fatal: severity == SeverityFatal}
	}
	return handlerDispatchResult{}
}

// claimPass is one round-robin claim transaction. It never claims a delivery
// that cannot reserve its queue slot and admission, never claims an
// unresolved delivery, and marks deliveries whose handler no longer resolves
// as unresolved (visible, never deleted). Rematch sentinels are expanded into
// per-handler deliveries in the same transaction. Every reservation taken in
// a pass is released if the pass fails.
func (o *Outbox) claimPass(ctx context.Context) (planned []*delivery, expanded int, err error) {
	o.claimMu.Lock()
	defer o.claimMu.Unlock()

	free := o.admission.freeSlots()
	if free == 0 {
		o.observeSkip(SkipAdmissionFull, 1)
		return nil, 0, nil
	}

	tx, err := o.beginWriteTx(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("could not begin claim transaction: %w", err)
	}
	defer rollbackTx(tx)

	// reserved is the authoritative list of (queue reservation + admission)
	// pairs taken in this pass. It is a separate local so an early
	// `return nil, 0, err` (which zeroes the named result) can never hide
	// reservations from the rollback.
	var reserved []*delivery
	defer func() {
		if err != nil {
			o.rollbackPlanned(reserved)
			planned = nil
			expanded = 0
		}
	}()

	now := schema.UTC(time.Now())
	stale := now.Add(-PeriodicSweepAge)
	handlers := o.snapshotHandlersByType()

	// Stale claims (taken_at older than PeriodicSweepAge and not admitted in
	// this process) are reset first so the per-key selection below only needs
	// taken_at IS NULL (index range, no OR, no sort). Rare path: in steady
	// state the range holds at most the admitted rows.
	if err := o.reclaimStale(ctx, tx, stale); err != nil {
		return nil, 0, err
	}

	// Rematch sentinels are expanded BEFORE any normal claim in this pass, and
	// all due sentinels are expanded (in bounded batches inside this
	// transaction), so an older due sentinel can never be overtaken by a newer
	// normal delivery on the same key. Expanded rows are claimed by the normal
	// per-key selection below in the same pass (they are due).
	expanded, err = o.expandSentinels(ctx, tx, now, handlers)
	if err != nil {
		return nil, 0, err
	}

	keys := rotateAfter(o.claimKeys, o.claimCursor)

	takenStmt := tx.StmtContext(ctx, o.updateTakenAtStmt)
	defer closeStmt(takenStmt, "update taken_at")
	unresolvedStmt := tx.StmtContext(ctx, o.markUnresolvedStmt)
	defer closeStmt(unresolvedStmt, "mark unresolved")

	// Rounds over the ring until admission is full or a full round claims
	// nothing. Adaptive quantum per round: when free admission is smaller than
	// the ring, every key gets at most one delivery per round so a lightly
	// loaded key is served every round instead of waiting for heavy keys'
	// full quanta; later rounds hand the remaining slots to keys that still
	// have backlog.
	for free > 0 {
		perKey := max(1, min(claimQuantum, free/max(len(keys), 1)))
		claimedThisRound := 0
		for _, key := range keys {
			if free <= 0 {
				o.observeSkip(SkipAdmissionFull, 1)
				break
			}
			quantum := min(perKey, free, o.dispatch.freeCapacity(key))
			if quantum <= 0 {
				o.observeSkip(SkipQueueFull, 1)
				continue
			}
			candidates, err := o.selectKeyCandidates(ctx, tx, key, now, quantum)
			if err != nil {
				return nil, 0, err
			}
			for _, doc := range candidates {
				if free <= 0 {
					break
				}
				if o.admission.contains(doc.ID) {
					continue
				}
				if err := o.decodeDelivery(doc); err != nil {
					// Visible, never silent: flag as unresolved and report.
					if _, uerr := unresolvedStmt.ExecContext(ctx, now, doc.ID); uerr != nil {
						return nil, 0, fmt.Errorf("could not flag undecodable delivery %s: %w", doc.ID, uerr)
					}
					o.observeSkip(SkipDecodeFailed, 1)
					o.sendError(ctx, fmt.Errorf("%w: delivery %s: %v", ErrUnresolvedHandler, doc.ID, err), nil)
					continue
				}
				mh := handlers[doc.HandlerType]
				if mh == nil || !mh.Match(doc.Event) {
					if _, uerr := unresolvedStmt.ExecContext(ctx, now, doc.ID); uerr != nil {
						return nil, 0, fmt.Errorf("could not flag unresolved delivery %s: %w", doc.ID, uerr)
					}
					o.observeSkip(SkipUnresolved, 1)
					o.sendError(ctx, fmt.Errorf("%w: delivery %s handler %q (leaving unclaimed)", ErrUnresolvedHandler, doc.ID, doc.HandlerType), doc.Event)
					continue
				}
				queueKey, handlerLabel, shardLabel := dispatchKeyFor(mh, doc.PartitionKey)
				d := newDelivery(doc, mh, queueKey, handlerLabel, shardLabel)
				if !o.dispatch.tryReserve(d) {
					o.observeSkip(SkipQueueFull, 1)
					break
				}
				if !o.admission.tryAdmitKey(doc.ID, queueKey) {
					o.dispatch.releaseReserve(queueKey)
					o.observeSkip(SkipAdmissionFull, 1)
					break
				}
				reserved = append(reserved, d)
				if _, err := takenStmt.ExecContext(ctx, now, doc.ID); err != nil {
					return nil, 0, fmt.Errorf("could not claim delivery %s: %w", doc.ID, err)
				}
				doc.TakenAt = sql.NullTime{Time: now, Valid: true}
				planned = append(planned, d)
				free--
				claimedThisRound++
			}
			o.claimCursor = key
		}
		if claimedThisRound == 0 {
			break
		}
		keys = rotateAfter(o.claimKeys, o.claimCursor)
	}

	if len(planned) == 0 && expanded == 0 {
		if err := tx.Commit(); err != nil {
			return nil, 0, fmt.Errorf("could not commit empty claim transaction: %w", err)
		}
		return nil, 0, nil
	}
	if o.beforeClaimCommit != nil {
		if err := o.beforeClaimCommit(len(reserved)); err != nil {
			return nil, 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, 0, fmt.Errorf("could not commit claim transaction: %w", err)
	}
	o.observeAdmission()
	return planned, expanded, nil
}

func (o *Outbox) rollbackPlanned(planned []*delivery) {
	for _, d := range planned {
		o.dispatch.releaseReserve(d.queueKey)
		o.admission.release(d.doc.ID)
	}
	o.observeAdmission()
}

// reclaimStale resets taken_at on deliveries claimed before stale that are
// not admitted in this process (crash leftovers are already reset at start;
// this covers FinalizeStuck rows and any future multi-start edge). An
// actively executing delivery is never reset.
func (o *Outbox) reclaimStale(ctx context.Context, tx *sql.Tx, stale time.Time) error {
	stmt := tx.StmtContext(ctx, o.staleClaimsStmt)
	defer closeStmt(stmt, "stale claims")
	rows, err := stmt.QueryContext(ctx, stale)
	if err != nil {
		return fmt.Errorf("could not list stale claims: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return err
		}
		if !o.admission.contains(id) {
			ids = append(ids, id)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	_ = rows.Close()
	if len(ids) == 0 {
		return nil
	}
	reset := tx.StmtContext(ctx, o.resetStaleStmt)
	defer closeStmt(reset, "reset stale claim")
	for _, id := range ids {
		if _, err := reset.ExecContext(ctx, id, stale); err != nil {
			return fmt.Errorf("could not reset stale claim %s: %w", id, err)
		}
	}
	return nil
}

// rotateAfter returns keys (sorted) rotated so iteration starts with the first
// key greater than cursor (round-robin across passes).
func rotateAfter(keys []string, cursor string) []string {
	if len(keys) < 2 || cursor == "" {
		return keys
	}
	i := sort.SearchStrings(keys, cursor)
	if i < len(keys) && keys[i] == cursor {
		i++
	}
	if i >= len(keys) || i == 0 {
		return keys
	}
	rotated := make([]string, 0, len(keys))
	rotated = append(rotated, keys[i:]...)
	rotated = append(rotated, keys[:i]...)
	return rotated
}

func (o *Outbox) selectKeyCandidates(ctx context.Context, tx *sql.Tx, key string, now time.Time, limit int) ([]*deliveryDoc, error) {
	active := o.admission.idsForKey(key)
	args := make([]any, 0, 3+len(active))
	args = append(args, key, now)
	for _, id := range active {
		args = append(args, id)
	}
	args = append(args, limit)

	var rows *sql.Rows
	var err error
	if len(active) == 0 {
		stmt := tx.StmtContext(ctx, o.claimKeyStmt)
		defer closeStmt(stmt, "claim key")
		rows, err = stmt.QueryContext(ctx, args...)
	} else {
		rows, err = tx.QueryContext(ctx, o.claimKeyQuery(len(active)), args...)
	}
	if err != nil {
		return nil, fmt.Errorf("could not select deliveries for key %s: %w", key, err)
	}
	defer rows.Close()
	var docs []*deliveryDoc
	for rows.Next() {
		doc, err := o.scanDeliveryRow(rows)
		if err != nil {
			return nil, err
		}
		docs = append(docs, doc)
	}
	return docs, rows.Err()
}

// expandSentinels replaces due rematch sentinels (handler_type NULL) with one
// delivery per currently matching handler. A sentinel that matches nothing is
// flagged unresolved (cleared again at next start) and reported.
func (o *Outbox) expandSentinels(ctx context.Context, tx *sql.Tx, now time.Time, handlers map[string]*matcherHandler) (int, error) {
	_ = handlers
	stmt := tx.StmtContext(ctx, o.sentinelsStmt)
	defer closeStmt(stmt, "rematch sentinels")
	unresolvedStmt := tx.StmtContext(ctx, o.markUnresolvedStmt)
	defer closeStmt(unresolvedStmt, "mark unresolved")
	deleteStmt := tx.StmtContext(ctx, o.deleteDeliveryStmt)
	defer closeStmt(deleteStmt, "delete delivery")
	insertStmt := tx.StmtContext(ctx, o.insertDeliveryStmt)
	defer closeStmt(insertStmt, "insert delivery")

	o.handlersMu.RLock()
	ordered := append([]*matcherHandler(nil), o.handlers...)
	o.handlersMu.RUnlock()

	expanded := 0
	for {
		// Each batch removes or flags every sentinel it reads, so the loop
		// terminates once no due, unflagged sentinel remains.
		batch, err := o.selectSentinels(ctx, stmt, now)
		if err != nil {
			return expanded, err
		}
		if len(batch) == 0 {
			return expanded, nil
		}
		n, err := o.expandSentinelBatch(ctx, batch, ordered, now, unresolvedStmt, deleteStmt, insertStmt)
		expanded += n
		if err != nil {
			return expanded, err
		}
	}
}

func (o *Outbox) selectSentinels(ctx context.Context, stmt *sql.Stmt, now time.Time) ([]*deliveryDoc, error) {
	rows, err := stmt.QueryContext(ctx, now, claimQuantum)
	if err != nil {
		return nil, fmt.Errorf("could not select rematch sentinels: %w", err)
	}
	defer rows.Close()
	var sentinels []*deliveryDoc
	for rows.Next() {
		doc, err := o.scanDeliveryRow(rows)
		if err != nil {
			return nil, err
		}
		sentinels = append(sentinels, doc)
	}
	return sentinels, rows.Err()
}

func (o *Outbox) expandSentinelBatch(ctx context.Context, sentinels []*deliveryDoc, ordered []*matcherHandler, now time.Time, unresolvedStmt, deleteStmt, insertStmt *sql.Stmt) (int, error) {
	expanded := 0
	for _, doc := range sentinels {
		if err := o.decodeDelivery(doc); err != nil {
			if _, uerr := unresolvedStmt.ExecContext(ctx, now, doc.ID); uerr != nil {
				return 0, fmt.Errorf("could not flag undecodable sentinel %s: %w", doc.ID, uerr)
			}
			o.observeSkip(SkipDecodeFailed, 1)
			o.sendError(ctx, fmt.Errorf("%w: sentinel %s: %v", ErrUnresolvedHandler, doc.ID, err), nil)
			continue
		}
		var matched []*matcherHandler
		for _, mh := range ordered {
			if mh != nil && mh.Match(doc.Event) {
				matched = append(matched, mh)
			}
		}
		if len(matched) == 0 {
			if _, uerr := unresolvedStmt.ExecContext(ctx, now, doc.ID); uerr != nil {
				return 0, fmt.Errorf("could not flag unmatched sentinel %s: %w", doc.ID, uerr)
			}
			o.observeSkip(SkipRematchNoMatch, 1)
			o.sendError(ctx, fmt.Errorf("%w: rematch of delivery %s found no handlers (leaving unclaimed)", ErrUnresolvedHandler, doc.ID), doc.Event)
			continue
		}
		if _, err := deleteStmt.ExecContext(ctx, doc.ID); err != nil {
			return 0, fmt.Errorf("could not remove rematch sentinel %s: %w", doc.ID, err)
		}
		for _, mh := range matched {
			key, _, _ := dispatchKeyFor(mh, doc.PartitionKey)
			if _, err := insertStmt.ExecContext(ctx,
				uuid.New().String(), doc.PublicationID, mh.HandlerType().String(), key, dispatchConfigFor(mh),
				doc.EventType, doc.AggregateID, schema.UTC(doc.CreatedAt), schema.UTC(doc.AvailableAt), 0, doc.LegacyOutboxID); err != nil {
				return 0, fmt.Errorf("could not insert rematched delivery for %s: %w", doc.ID, err)
			}
		}
		expanded++
	}
	return expanded, nil
}

// abandonDelivery is called for queued deliveries at shutdown: the row keeps
// taken_at and is redelivered after the next startup reset.
func (o *Outbox) abandonDelivery(d *delivery) {
	if d.abandoned.Swap(true) {
		return
	}
	o.admission.release(d.doc.ID)
	o.observeAdmission()
	d.finish()
}

// executeDelivery runs one claimed delivery end to end: global permit, handler,
// finalize with the saved outcome, admission release, fetcher wake-up.
func (o *Outbox) executeDelivery(d *delivery) {
	if d.abandoned.Load() {
		return
	}
	// Global HandleEvent permit (FIFO fair across dispatch keys).
	if err := o.handleSem.acquire(o.runContext()); err != nil {
		o.abandonDelivery(d)
		return
	}
	if o.shuttingDown.Load() {
		o.handleSem.release()
		o.abandonDelivery(d)
		return
	}

	handlerLabel := d.handlerLabel
	shardLabel := d.shardLabel
	if o.dispatch != nil {
		o.dispatch.addInFlight(handlerLabel, shardLabel, 1)
	}
	eventCtx := eh.UnmarshalContext(context.Background(), d.doc.EventCtx)
	res := o.dispatchHandler(eventCtx, d)
	if o.dispatch != nil {
		o.dispatch.addInFlight(handlerLabel, shardLabel, -1)
	}
	o.handleSem.release()

	o.finalizeDelivery(context.Background(), d, res)
	o.admission.release(d.doc.ID)
	o.observeAdmission()
	d.finish()
	// Wake fetcher after completion (decoupled from claim).
	o.notify()
}

// finalizeDelivery commits the saved handler outcome for one delivery:
// completed → delete (+ publication GC); retryable → retry_count+1 and
// available_at backoff; fatal/exhausted → dead letter + delete. Finalize SQL
// failures are retried with the same outcome (bounded backoff); the handler is
// never re-run. After exhaustion the claim is released so the delivery becomes
// eligible again after PeriodicSweepAge; if release fails too, the taken_at
// timeout recovers the row later (FinalizeStuck).
func (o *Outbox) finalizeDelivery(ctx context.Context, d *delivery, res handlerDispatchResult) {
	started := time.Now()
	outcome := FinalizeCompleted
	reason := ""
	if res.err != nil {
		reason = res.err.Error()
		if res.fatal || d.doc.RetryCount >= o.maxRetries {
			outcome = FinalizeDeadLetter
		} else {
			outcome = FinalizeRetry
		}
	}

	var (
		pending []dl.Record
		err     error
		retried bool
	)
	attempts := 1 + len(finalizeRetryDelays)
	for attempt := 1; attempt <= attempts; attempt++ {
		pending, err = o.finalizeTx(ctx, d.doc, outcome, reason, attempt)
		if err == nil {
			break
		}
		if attempt < attempts {
			retried = true
			delay := finalizeRetryDelays[attempt-1]
			o.finalizeSleep(delay)
		}
	}
	if err != nil {
		o.sendError(ctx, fmt.Errorf("could not finalize delivery %s (%s) after %d attempts: %w", d.doc.ID, outcome, attempts, err), d.doc.Event)
		releaseErr := o.releaseClaim(ctx, d.doc)
		if releaseErr != nil {
			o.sendError(ctx, fmt.Errorf("%w: delivery %s: %v", ErrFinalizeStuck, d.doc.ID, releaseErr), d.doc.Event)
			o.observeFinalize(FinalizeStuck, time.Since(started), retried, releaseErr)
			return
		}
		o.observeFinalize(FinalizeReleased, time.Since(started), retried, err)
		o.notifySchedule()
		return
	}

	o.observeFinalize(outcome, time.Since(started), retried, nil)
	switch outcome {
	case FinalizeDeadLetter:
		o.sendError(ctx, errEventHandlerMovedToDeadLetters, d.doc.Event)
		o.exportDeadLetters(ctx, d.doc.Event, pending)
	case FinalizeRetry:
		// Wake the delayed-dispatch timer after a committed retry schedule.
		o.notifySchedule()
	}
}

func (o *Outbox) finalizeTx(ctx context.Context, doc *deliveryDoc, outcome FinalizeOutcome, reason string, attempt int) ([]dl.Record, error) {
	tx, err := o.beginWriteTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not begin finalize transaction: %w", err)
	}
	defer rollbackTx(tx)

	var pending []dl.Record
	switch outcome {
	case FinalizeCompleted:
		if err := o.deleteDeliveryTx(ctx, tx, doc); err != nil {
			return nil, err
		}
	case FinalizeRetry:
		nextRetryCount := doc.RetryCount + 1
		availableAt := schema.UTC(time.Now().Add(o.retryBackoff.DelayFunc(int64(nextRetryCount))))
		stmt := tx.StmtContext(ctx, o.scheduleRetryStmt)
		defer closeStmt(stmt, "schedule retry")
		if _, err := stmt.ExecContext(ctx, nextRetryCount, availableAt, doc.ID); err != nil {
			return nil, fmt.Errorf("could not schedule delivery retry: %w", err)
		}
	case FinalizeDeadLetter:
		record, err := o.insertOutboxDeadLetterTx(ctx, tx, doc, reason)
		if err != nil {
			return nil, err
		}
		pending = append(pending, record)
		if err := o.deleteDeliveryTx(ctx, tx, doc); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown finalize outcome %q", outcome)
	}

	if o.beforeFinalizeCommit != nil {
		if err := o.beforeFinalizeCommit(doc.ID, attempt); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("could not commit finalize transaction: %w", err)
	}
	return pending, nil
}

func (o *Outbox) deleteDeliveryTx(ctx context.Context, tx *sql.Tx, doc *deliveryDoc) error {
	deleteStmt := tx.StmtContext(ctx, o.deleteDeliveryStmt)
	defer closeStmt(deleteStmt, "delete delivery")
	if _, err := deleteStmt.ExecContext(ctx, doc.ID); err != nil {
		return fmt.Errorf("could not delete finalized delivery: %w", err)
	}
	gcStmt := tx.StmtContext(ctx, o.gcPublicationStmt)
	defer closeStmt(gcStmt, "gc publication")
	if _, err := gcStmt.ExecContext(ctx, doc.PublicationID, doc.PublicationID); err != nil {
		return fmt.Errorf("could not garbage collect publication: %w", err)
	}
	return nil
}

// releaseClaim is the safe fallback after finalize exhaustion: the delivery
// becomes claimable again after PeriodicSweepAge without touching retry_count.
func (o *Outbox) releaseClaim(ctx context.Context, doc *deliveryDoc) error {
	if o.beforeReleaseClaim != nil {
		if err := o.beforeReleaseClaim(doc.ID); err != nil {
			return err
		}
	}
	availableAt := schema.UTC(time.Now().Add(PeriodicSweepAge))
	if _, err := o.releaseClaimStmt.ExecContext(ctx, availableAt, doc.ID); err != nil {
		return err
	}
	return nil
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
		if err := o.markDeadLetterExported(ctx, resolved.ID, schema.UTC(time.Now())); err != nil {
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
	var publicationID, legacyOutboxID sql.NullString
	err := o.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT id, source, event_type, aggregate_id, handler_type, outbox_id,
		       remaining_handlers, blob, error, retry_count, created_at, dead_at, exported_at,
		       publication_id, legacy_outbox_id
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
		&publicationID,
		&legacyOutboxID,
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
	record.PublicationID = publicationID.String
	record.LegacyOutboxID = legacyOutboxID.String
	return record, true, nil
}

func (o *Outbox) insertOutboxDeadLetterTx(ctx context.Context, tx *sql.Tx, doc *deliveryDoc, reason string) (dl.Record, error) {
	if reason == "" {
		reason = "max retries reached or fatal handler error"
	}
	now := schema.UTC(time.Now())
	record := dl.Record{
		ID:                uuid.New().String(),
		Source:            "outbox",
		EventType:         doc.EventType,
		AggregateID:       doc.AggregateID,
		HandlerType:       doc.HandlerType,
		OutboxID:          doc.ID,
		RemainingHandlers: "[]",
		Blob:              doc.EventBlob,
		Error:             reason,
		RetryCount:        doc.RetryCount,
		CreatedAt:         schema.UTC(doc.CreatedAt),
		DeadAt:            now,
		PublicationID:     doc.PublicationID,
		LegacyOutboxID:    doc.LegacyOutboxID.String,
	}
	insertStmt := tx.StmtContext(ctx, o.insertDeadLetterStmt)
	defer closeStmt(insertStmt, "dead letter")
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
		nullString(record.PublicationID),
		nullString(record.LegacyOutboxID),
	); err != nil {
		return dl.Record{}, fmt.Errorf("could not insert outbox dead letter: %w", err)
	}
	return record, nil
}

func nullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

func (o *Outbox) markDeadLetterExported(ctx context.Context, id string, exportedAt time.Time) error {
	if _, err := o.db.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET exported_at = ? WHERE id = ?`, o.deadLetterTable), schema.UTC(exportedAt), id); err != nil {
		return err
	}
	return nil
}
