package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"slices"
	"sync"
	"time"

	"github.com/vercly/eh-sqlite/context/sqlite"

	codec "github.com/vercly/eh-sqlite/codec/json"

	jsoniter "github.com/json-iterator/go"
	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/uuid"
	_ "github.com/mattn/go-sqlite3"
)

var (
	// PeriodicSweepInterval Interval in which to do a sweep of various unprocessed events.
	PeriodicSweepInterval = 15 * time.Second

	// PeriodicSweepAge Settings for how old different kind of unprocessed events needs to be
	// to be processed by the periodic sweep.
	PeriodicSweepAge = 15 * time.Second
)

// Outbox implements an eventhorizon.Outbox for SQLite.
type Outbox struct {
	db             *sql.DB
	outboxTable    string
	handlers       []*matcherHandler
	handlersByType map[eh.EventHandlerType]*matcherHandler
	handlersMu     sync.RWMutex
	watchCh        chan *outboxDoc
	errCh          chan error
	cctx           context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	codec          eh.EventCodec

	insertEventStmt    *sql.Stmt
	selectEventsStmt   *sql.Stmt
	updateTakenAtStmt  *sql.Stmt
	deleteEventStmt    *sql.Stmt
	updateHandlersStmt *sql.Stmt
}

type matcherHandler struct {
	eh.EventMatcher
	eh.EventHandler
}

// NewOutbox creates a new Outbox.
func NewOutbox(db *sql.DB, options ...Option) (*Outbox, error) {
	ctx, cancel := context.WithCancel(context.Background())

	o := &Outbox{
		db:             db,
		outboxTable:    "outbox",
		handlersByType: map[eh.EventHandlerType]*matcherHandler{},
		watchCh:        make(chan *outboxDoc, 100),
		errCh:          make(chan error, 100),
		cctx:           ctx,
		cancel:         cancel,
		codec:          &codec.JSON{},
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
				taken_at TIMESTAMP,

				handlers TEXT NOT NULL, 

				-- --- Blob Column for the rest of the event data ---
				-- This will store a JSON object containing the full event,
				-- including data, metadata, version, etc.
				event_blob TEXT NOT NULL
		);

		-- Index the columns you will query.
		CREATE INDEX IF NOT EXISTS idx_%[1]s_created_at ON %[1]s (created_at);
		CREATE INDEX IF NOT EXISTS idx_%[1]s_taken_at ON %[1]s (taken_at);
	`, o.outboxTable)); err != nil {
		return nil, fmt.Errorf("could not create outbox table: %w", err)
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

func (o *Outbox) prepareStatements() (err error) {
	if o.insertEventStmt, err = o.db.Prepare(fmt.Sprintf(`
		INSERT INTO %s (id, event_type, aggregate_id, created_at, taken_at, handlers, event_blob)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, o.outboxTable)); err != nil {
		return fmt.Errorf("could not prepare insert event statement: %w", err)
	}

	if o.selectEventsStmt, err = o.db.Prepare(fmt.Sprintf(`
		SELECT id, event_type, aggregate_id, created_at, taken_at, handlers, event_blob
		FROM %s
		WHERE taken_at IS NULL OR taken_at < ?
		ORDER BY created_at ASC LIMIT 50
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
	Ctx       context.Context
	Handlers  []string
	CreatedAt time.Time
	TakenAt   sql.NullTime
}

// HandleEvent implements the HandleEvent method of the eventhorizon.EventHandler interface.
func (o *Outbox) HandleEvent(ctx context.Context, event eh.Event) error {
	eventBlob, err := o.codec.MarshalEvent(ctx, event)
	if err != nil {
		return fmt.Errorf("could not marshal event: %w", err)
	}

	o.handlersMu.RLock()
	matchingHandlers := make([]string, 0)
	for _, mh := range o.handlers {
		if mh.Match(event) {
			matchingHandlers = append(matchingHandlers, mh.EventHandler.HandlerType().String())
		}
	}
	o.handlersMu.RUnlock()

	// if none handler matches there is no need to process it
	if len(matchingHandlers) == 0 {
		return nil
	}

	handlersBlob, err := jsoniter.Marshal(matchingHandlers)
	if err != nil {
		return fmt.Errorf("could not marshal handlers: %w", err)
	}

	tx, txOk := sqlite.TxFromContext(ctx)
	if !txOk {
		tx, err = o.db.Begin()
		if err != nil {
			return fmt.Errorf("could not begin transaction: %w", err)
		}
		defer tx.Rollback()
	}

	// Insert the promoted fields AND the blob.
	if _, err := tx.Stmt(o.insertEventStmt).Exec(
		uuid.New().String(),
		event.EventType().String(),
		event.AggregateID().String(),
		time.Now(),
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
		ID:        uuid.New(),
		Event:     event,
		Ctx:       ctx,
		Handlers:  matchingHandlers,
		CreatedAt: time.Now(),
	}

	// Signal our worker that there's immediate work to do.
	// This is the equivalent of the channel push, but it's just a notification.
	select {
	case o.watchCh <- r:
	default: // If channel is full, the periodic sweep will pick it up.
	}

	return nil
}

// Start method launches ONE unified processor.
func (o *Outbox) Start() {
	o.wg.Add(1) // Only one worker goroutine.
	go o.runUnifiedProcessor()
}

// Close implements the Close method of the eventhorizon.EventBus interface.
func (o *Outbox) Close() error {
	o.cancel()
	o.wg.Wait()
	o.insertEventStmt.Close()
	o.selectEventsStmt.Close()
	o.updateTakenAtStmt.Close()
	o.deleteEventStmt.Close()
	o.updateHandlersStmt.Close()
	return o.db.Close()
}

func (o *Outbox) runUnifiedProcessor() {
	defer o.wg.Done()

	ticker := time.NewTicker(PeriodicSweepInterval)
	defer ticker.Stop()

	// The main loop now only cares about being woken up.
	// The actual processing loop is inside the handler for the triggers.
	for {
		select {
		// --- Trigger 1: The Fast Path (from watchCh) ---
		case r := <-o.watchCh:
			// A signal has arrived. We know there is at least one new event.
			// We will now enter a "work loop" that continues until the outbox is empty.
			o.processUntilEmpty(r.Ctx)

		// --- Trigger 2: The Slow Path (from ticker) ---
		case <-ticker.C:
			// The ticker is our safety net. It also triggers the same work loop.
			o.processUntilEmpty(o.cctx)

		// --- Trigger 3: Shutdown ---
		case <-o.cctx.Done():
			return
		}
	}
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
	var createdAt, takenAt sql.NullTime

	if err := rows.Scan(&id, &eventType, &aggregateID, &createdAt, &takenAt, &handlersBlob, &eventBlob); err != nil {
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
		ID:        uuid.MustParse(id),
		Event:     event,
		Ctx:       ctx,
		Handlers:  handlers,
		CreatedAt: createdAt.Time,
		TakenAt:   takenAt,
	}, nil
}

// processBatch - main procesing batch
func (o *Outbox) processBatch(ctx context.Context) (int, error) {
	tx, err := o.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	rows, err := tx.StmtContext(ctx, o.selectEventsStmt).Query(time.Now().Add(-PeriodicSweepAge))
	if err != nil {
		return 0, err
	}

	var eventsToProcess []*outboxDoc
	for rows.Next() {
		r, err := o.scanOutboxDoc(rows)
		if err != nil {
			rows.Close()
			return 0, err
		}
		eventsToProcess = append(eventsToProcess, r)
	}
	rows.Close() // It's important to close before committing.

	if len(eventsToProcess) == 0 {
		tx.Commit()
		return 0, nil
	}

	now := time.Now()
	for _, r := range eventsToProcess {
		if _, err := tx.Stmt(o.updateTakenAtStmt).ExecContext(ctx, now, r.ID.String()); err != nil {
			return 0, err
		}
	}

	// End the transaction that locks the rows.
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("could not commit transaction locking events: %w", err)
	}

	for _, r := range eventsToProcess {
		processedHandlers := o.dispatchEvent(r)
		//
		// If nothing worked, move on to the next one. We'll try again later.
		if len(processedHandlers) == 0 {
			continue
		}

		// Update the list of remaining handlers
		remainingHandlers := make([]string, 0, len(r.Handlers))
		for _, required := range r.Handlers {
			if !slices.Contains(processedHandlers, required) {
				remainingHandlers = append(remainingHandlers, required)
			}
		}

		if len(remainingHandlers) == 0 {
			// All handlers have finished working, remove the event.
			if _, err := o.deleteEventStmt.ExecContext(ctx, r.ID.String()); err != nil {
				o.sendError(fmt.Errorf("could not delete fully processed event: %w", err), r.Event, ctx)
			}
		} else {
			// Update the list of handlers in the database
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

	return len(eventsToProcess), nil
}

// dispatchEvent - Returns a list of handlers that completed successfully.
func (o *Outbox) dispatchEvent(r *outboxDoc) []string {
	var successfulHandlers []string

	handlerSet := make(map[string]struct{})
	for _, h := range r.Handlers {
		handlerSet[h] = struct{}{}
	}

	o.handlersMu.RLock()
	defer o.handlersMu.RUnlock()

	for _, mh := range o.handlers {
		// Check if this handler is even on the list of handlers to be processed for this event.
		if _, ok := handlerSet[mh.HandlerType().String()]; !ok {
			continue
		}

		if !mh.Match(r.Event) {
			continue
		}

		if err := mh.HandleEvent(r.Ctx, r.Event); err != nil {
			err = fmt.Errorf("could not handle event (%s): %w", mh.HandlerType(), err)
			o.sendError(err, r.Event, r.Ctx)
		} else {
			successfulHandlers = append(successfulHandlers, mh.HandlerType().String())
		}
	}

	return successfulHandlers
}
