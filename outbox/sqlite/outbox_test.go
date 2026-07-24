package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	dl "github.com/vercly/eh-sqlite/deadletter"

	txctx "github.com/vercly/eh-sqlite/context/sqlite"
	eh "github.com/vercly/eventhorizon"
	"github.com/vercly/eventhorizon/mocks"
	"github.com/vercly/eventhorizon/outbox"
	"github.com/vercly/eventhorizon/uuid"
)

func init() {
	eh.RegisterEventData(mocks.EventOtherType, func() eh.EventData { return &mocks.EventData{} })
}

func TestOutboxAddHandler(t *testing.T) {
	db := newTestDB(t)
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}

	outbox.TestAddHandler(t, o, context.Background())
}

// TestOutboxIntegrationStaticRegistration covers the former AcceptanceTest
// surface under the static-registration contract (all handlers before Start):
// context marshal/unmarshal into handlers, multi-handler fan-out, and async
// handler errors on Errors().
func TestOutboxIntegrationStaticRegistration(t *testing.T) {
	restoreSweepInterval := setPeriodicSweepInterval(t, 2*time.Second)
	defer restoreSweepInterval()
	restoreSweepAge := setPeriodicSweepAge(t, 2*time.Second)
	defer restoreSweepAge()

	db := newTestDB(t)
	ctx := mocks.WithContextOne(context.Background(), "testval")

	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := mocks.NewEventHandler("static_handler")
	anotherHandler := mocks.NewEventHandler("static_another_handler")
	otherHandler := mocks.NewEventHandler("static_other_handler")
	errorHandler := mocks.NewEventHandler("static_error_handler")
	errorHandler.Err = errors.New("handler error")

	// All registrations before Start.
	for _, h := range []eh.EventHandler{handler, anotherHandler} {
		if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, h); err != nil {
			t.Fatal(err)
		}
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventOtherType}, otherHandler); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchAll{}, errorHandler); err != nil {
		t.Fatal(err)
	}

	if err := o.StartChecked(); err != nil {
		t.Fatal(err)
	}

	id := uuid.New()
	timestamp := time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
	event2 := eh.NewEvent(mocks.EventType, &mocks.EventData{Content: "event2"}, timestamp,
		eh.ForAggregate(mocks.AggregateType, id, 2),
		eh.WithMetadata(map[string]any{"meta": "data", "num": 42.0}),
	)
	if err := o.HandleEvent(ctx, event2); err != nil {
		t.Fatal(err)
	}

	if !handler.Wait(3 * time.Second) {
		t.Fatal("handler did not receive event")
	}
	handler.Lock()
	if len(handler.Events) != 1 {
		t.Fatalf("handler events = %d, want 1", len(handler.Events))
	}
	if val, ok := mocks.ContextOne(handler.Context); !ok || val != "testval" {
		t.Fatalf("handler context = %v, want testval", handler.Context)
	}
	handler.Unlock()

	if !anotherHandler.Wait(3 * time.Second) {
		t.Fatal("another handler did not receive event")
	}
	anotherHandler.Lock()
	if val, ok := mocks.ContextOne(anotherHandler.Context); !ok || val != "testval" {
		t.Fatalf("another handler context = %v, want testval", anotherHandler.Context)
	}
	anotherHandler.Unlock()

	// MatchAll error handler also sees the event; async error must appear.
	// Drain until we see the handler error (other Errors may exist).
	deadline := time.After(3 * time.Second)
	found := false
	for !found {
		select {
		case err := <-o.Errors():
			if err != nil && strings.Contains(err.Error(), "handler error") {
				found = true
			}
		case <-deadline:
			t.Fatal("expected async handler error on Errors()")
		}
	}

	// Event without data to the other-type handler.
	eventOther := eh.NewEvent(mocks.EventOtherType, nil, timestamp,
		eh.ForAggregate(mocks.AggregateType, uuid.New(), 1))
	if err := o.HandleEvent(ctx, eventOther); err != nil {
		t.Fatal(err)
	}
	if !otherHandler.Wait(3 * time.Second) {
		t.Fatal("other handler did not receive event")
	}
}

func TestWithTableNameIntegration(t *testing.T) {
	db := newTestDB(t)

	o, err := NewOutbox(db, WithTableName("foo_outbox"))
	if err != nil {
		t.Fatal(err)
	}

	defer o.Close()

	if o == nil {
		t.Fatal("there should be a store")
	}

	if o.outboxTable != "foo_outbox" {
		t.Fatal("table name should use custom table name")
	}
}

func TestOutboxCloseDoesNotCloseSharedDB(t *testing.T) {
	db := newTestDB(t)

	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}

	if err := o.Close(); err != nil {
		t.Fatal(err)
	}

	if err := db.Ping(); err != nil {
		t.Fatalf("shared db should remain open after outbox close: %v", err)
	}
}

func TestOutboxHandleEventRequiresStart(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, mocks.NewEventHandler("start_guard_handler")); err != nil {
		t.Fatal(err)
	}

	err = o.HandleEvent(ctx, newTestEvent("before-start"))
	if !errors.Is(err, ErrOutboxNotStarted) {
		t.Fatalf("HandleEvent before Start error = %v, want ErrOutboxNotStarted", err)
	}
	if got := outboxRowCount(t, db); got != 0 {
		t.Fatalf("outbox rows before Start = %d, want 0", got)
	}
	if got := deadLetterRowCount(t, db); got != 0 {
		t.Fatalf("dead letter rows before Start = %d, want 0", got)
	}

	if err := o.StartChecked(); err != nil {
		t.Fatal(err)
	}
	if err := o.StartChecked(); err != nil {
		t.Fatal(err)
	}

	if err := o.HandleEvent(WithDelay(ctx, time.Hour), newTestEvent("after-start")); err != nil {
		t.Fatal(err)
	}
	if got := outboxRowCount(t, db); got != 1 {
		t.Fatalf("outbox rows after Start = %d, want 1", got)
	}

	// Registration is closed after Start.
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, mocks.NewEventHandler("late_handler")); !errors.Is(err, ErrOutboxAlreadyStarted) {
		t.Fatalf("AddHandler after Start error = %v, want ErrOutboxAlreadyStarted", err)
	}
}

func TestOutboxNotifySignalsScheduleWhenWatchChannelFull(t *testing.T) {
	db := newTestDB(t)
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	for range cap(o.watchCh) {
		o.watchCh <- struct{}{}
	}

	o.notify()

	select {
	case <-o.scheduleCh:
	default:
		t.Fatal("schedule signal was not queued when watchCh was full")
	}
}

func TestOutboxProcessesStaleTakenAtAfterRestart(t *testing.T) {
	restoreSweepAge := setPeriodicSweepAge(t, 25*time.Millisecond)
	defer restoreSweepAge()

	db := newTestDB(t)
	ctx := context.Background()

	firstOutbox, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	seedOutboxEvent(t, db, firstOutbox, newTestEvent("stale"), "stale_handler", time.Now(), time.Now(), sql.NullTime{Time: time.Now().Add(-2 * PeriodicSweepAge), Valid: true})
	if err := firstOutbox.Close(); err != nil {
		t.Fatal(err)
	}

	restartedOutbox, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer restartedOutbox.Close()

	restartedHandler := mocks.NewEventHandler("stale_handler")
	if err := restartedOutbox.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, restartedHandler); err != nil {
		t.Fatal(err)
	}

	processed, err := restartedOutbox.processBatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("processed count = %d, want 1", processed)
	}
	if !restartedHandler.Wait(time.Second) {
		t.Fatal("stale outbox event was not dispatched")
	}
	if got := outboxRowCount(t, db); got != 0 {
		t.Fatalf("outbox rows after processing = %d, want 0", got)
	}
}

func TestOutboxDoesNotProcessFreshTakenAtBeforeSweepAge(t *testing.T) {
	restoreSweepAge := setPeriodicSweepAge(t, time.Hour)
	defer restoreSweepAge()

	db := newTestDB(t)
	ctx := context.Background()

	firstOutbox, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	seedOutboxEvent(t, db, firstOutbox, newTestEvent("fresh"), "fresh_handler", time.Now(), time.Now(), sql.NullTime{Time: time.Now(), Valid: true})
	if err := firstOutbox.Close(); err != nil {
		t.Fatal(err)
	}

	restartedOutbox, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer restartedOutbox.Close()

	restartedHandler := mocks.NewEventHandler("fresh_handler")
	if err := restartedOutbox.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, restartedHandler); err != nil {
		t.Fatal(err)
	}

	processed, err := restartedOutbox.processBatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 0 {
		t.Fatalf("processed count = %d, want 0", processed)
	}
	if restartedHandler.Wait(50 * time.Millisecond) {
		t.Fatal("freshly taken event should not be dispatched before PeriodicSweepAge")
	}
	if got := outboxRowCount(t, db); got != 1 {
		t.Fatalf("outbox rows after skipped processing = %d, want 1", got)
	}
}

func TestOutboxConcurrentPublishDoesNotLoseRows(t *testing.T) {
	db := newTestDB(t)
	db.SetMaxOpenConns(8)
	ctx := context.Background()

	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, mocks.NewEventHandler("concurrent_handler")); err != nil {
		t.Fatal(err)
	}
	o.Start()

	const events = 100
	var wg sync.WaitGroup
	errCh := make(chan error, events)
	for i := range events {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := o.HandleEvent(WithDelay(ctx, time.Hour), newTestEvent(fmt.Sprintf("event-%d", i))); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		t.FailNow()
	}

	if got := outboxRowCount(t, db); got != events {
		t.Fatalf("outbox rows after concurrent publish = %d, want %d", got, events)
	}
}

func TestOutboxAvailableAtMigrationBackfillsExistingRows(t *testing.T) {
	db := newTestDB(t)
	createdAt := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)

	if _, err := db.Exec(`
		CREATE TABLE outbox (
			id TEXT PRIMARY KEY,
			event_type TEXT NOT NULL,
			aggregate_id TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL,
			taken_at TIMESTAMP,
			handlers TEXT NOT NULL,
			event_blob TEXT NOT NULL,
			retry_count INTEGER DEFAULT 0
		);
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO outbox (id, event_type, aggregate_id, created_at, handlers, event_blob, retry_count)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, uuid.New().String(), mocks.EventType.String(), uuid.New().String(), createdAt, "[]", "{}", 0); err != nil {
		t.Fatal(err)
	}

	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	var availableAt time.Time
	if err := db.QueryRow(`SELECT available_at FROM outbox LIMIT 1`).Scan(&availableAt); err != nil {
		t.Fatal(err)
	}
	if !availableAt.Equal(createdAt) {
		t.Fatalf("available_at = %s, want %s", availableAt, createdAt)
	}
}

func TestOutboxDelayedEventWaitsForAvailableAtTimer(t *testing.T) {
	restoreSweepInterval := setPeriodicSweepInterval(t, time.Hour)
	defer restoreSweepInterval()

	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := mocks.NewEventHandler("delayed_handler")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	o.Start()

	if err := o.HandleEvent(WithDelay(ctx, 300*time.Millisecond), newTestEvent("delayed")); err != nil {
		t.Fatal(err)
	}
	if handler.Wait(100 * time.Millisecond) {
		t.Fatal("delayed event was dispatched before available_at")
	}
	if !handler.Wait(2 * time.Second) {
		t.Fatal("delayed event was not dispatched by the available_at timer")
	}
}

func TestOutboxMetadataAvailableAtOverridesContextDelay(t *testing.T) {
	restoreSweepInterval := setPeriodicSweepInterval(t, time.Hour)
	defer restoreSweepInterval()

	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := mocks.NewEventHandler("metadata_delay_handler")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	o.Start()

	availableAt := time.Now().Add(300 * time.Millisecond)
	event := eh.NewEvent(mocks.EventType, &mocks.EventData{Content: "metadata-delay"}, time.Now(),
		eh.ForAggregate(mocks.AggregateType, uuid.New(), 1),
		eh.WithMetadata(map[string]any{metadataAvailableAtKey: availableAt.Format(time.RFC3339Nano)}),
	)
	if err := o.HandleEvent(WithDelay(ctx, time.Hour), event); err != nil {
		t.Fatal(err)
	}
	if handler.Wait(100 * time.Millisecond) {
		t.Fatal("metadata-delayed event was dispatched before available_at")
	}
	if !handler.Wait(2 * time.Second) {
		t.Fatal("metadata available_at did not override context delay")
	}
}

func TestOutboxNoMatchWritesDeadLetter(t *testing.T) {
	db := newTestDB(t)
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	o.Start()

	if err := o.HandleEvent(context.Background(), newTestEvent("no-match")); err != nil {
		t.Fatal(err)
	}

	if got := outboxRowCount(t, db); got != 0 {
		t.Fatalf("outbox rows = %d, want 0", got)
	}
	if got := deadLetterRowCount(t, db); got != 1 {
		t.Fatalf("dead letter rows = %d, want 1", got)
	}

	var handlerType, source, errMsg string
	if err := db.QueryRow(`SELECT handler_type, source, error FROM dead_letters LIMIT 1`).Scan(&handlerType, &source, &errMsg); err != nil {
		t.Fatal(err)
	}
	if handlerType != "no_match" || source != "outbox" || errMsg != "no matching handlers" {
		t.Fatalf("dead letter = (%s, %s, %s), want no_match/outbox/no matching handlers", handlerType, source, errMsg)
	}
}

// TestOutboxEmptyHandlersRematch dispatches rows with handlers=[] against the
// current registration (replay rematch contract). Still unmatched rows stay
// unclaimed and are never deleted.
func TestOutboxEmptyHandlersRematch(t *testing.T) {
	db := newTestDB(t)
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := mocks.NewEventHandler("rematch_handler")
	if err := o.AddHandler(context.Background(), eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}
	if err := o.StartChecked(); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	matchedID := uuid.New().String()
	unmatchedID := uuid.New().String()
	seedOutboxEventWithID(t, db, o, matchedID, newTestEvent("rematch-hit"), []string{}, now, now, sql.NullTime{})
	// EventOtherType has no matching handler in this outbox.
	other := eh.NewEvent(mocks.EventOtherType, &mocks.EventData{Content: "nope"}, now,
		eh.ForAggregate(mocks.AggregateType, uuid.New(), 1))
	seedOutboxEventWithID(t, db, o, unmatchedID, other, []string{}, now, now, sql.NullTime{})

	processAllBatches(t, o, context.Background())
	// Allow in-flight handler completion.
	if !handler.Wait(3 * time.Second) {
		t.Fatal("rematch handler did not receive event")
	}

	// Matched rematch row should complete and leave outbox.
	var matchedLeft int
	if err := db.QueryRow(`SELECT COUNT(*) FROM outbox WHERE id = ?`, matchedID).Scan(&matchedLeft); err != nil {
		t.Fatal(err)
	}
	if matchedLeft != 0 {
		t.Fatalf("matched rematch row still in outbox")
	}

	// Unmatched rematch stays unclaimed (never deleted).
	var unmatchedHandlers string
	var unmatchedTaken sql.NullTime
	if err := db.QueryRow(`SELECT handlers, taken_at FROM outbox WHERE id = ?`, unmatchedID).
		Scan(&unmatchedHandlers, &unmatchedTaken); err != nil {
		t.Fatal(err)
	}
	if unmatchedHandlers != "[]" || unmatchedTaken.Valid {
		t.Fatalf("unmatched rematch: handlers=%s taken=%v (want [] unclaimed)", unmatchedHandlers, unmatchedTaken)
	}
}

// TestOutboxRematchPartialSuccessRetryable keeps the failed handler after rematch.
// Without persisting matched handlers at claim time, remainingHandlers([]) would
// delete the row and silently drop retryable work.
func TestOutboxRematchPartialSuccessRetryable(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithMaxRetries(2), WithRetryBackoff("FIXED:2:200ms"))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	successHandler := mocks.NewEventHandler("rematch_ok_handler")
	retryHandler := mocks.NewEventHandler("rematch_retry_handler")
	retryHandler.Err = errors.New("temporary rematch failure")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, successHandler); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, retryHandler); err != nil {
		t.Fatal(err)
	}

	id := uuid.New().String()
	createdAt := time.Now().Add(-time.Minute)
	// Rematch sentinel: empty handlers list.
	seedOutboxEventWithID(t, db, o, id, newTestEvent("rematch-partial-retry"), []string{}, createdAt, createdAt, sql.NullTime{})

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}

	if !successHandler.Wait(2 * time.Second) {
		t.Fatal("success handler did not run")
	}
	// Row must survive with only the retryable handler remaining (no silent delete).
	if got := outboxRowCount(t, db); got != 1 {
		t.Fatalf("outbox rows = %d, want 1 (retryable work kept)", got)
	}
	assertOutboxHandlers(t, db, id, []string{retryHandler.Type})
	var retryCount int
	var availableAt time.Time
	var takenAt sql.NullTime
	if err := db.QueryRow(`SELECT retry_count, available_at, taken_at FROM outbox WHERE id = ?`, id).
		Scan(&retryCount, &availableAt, &takenAt); err != nil {
		t.Fatal(err)
	}
	if retryCount != 1 {
		t.Fatalf("retry_count = %d, want 1", retryCount)
	}
	if !availableAt.After(time.Now().Add(100 * time.Millisecond)) {
		t.Fatalf("available_at = %s, want future retry backoff", availableAt)
	}
	if takenAt.Valid {
		t.Fatal("taken_at should be cleared for retry")
	}
	if got := deadLetterRowCount(t, db); got != 0 {
		t.Fatalf("dead letter rows = %d, want 0 for retryable failure", got)
	}
}

// TestOutboxRematchPartialSuccessFatal DLQs only the fatal handler after rematch
// and deletes the row when no retryable work remains (success was removed).
func TestOutboxRematchPartialSuccessFatal(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	successHandler := mocks.NewEventHandler("rematch_fatal_ok_handler")
	fatalHandler := mocks.NewEventHandler("rematch_fatal_bad_handler")
	fatalHandler.Err = fatalOutboxError{err: errors.New("fatal rematch failure")}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, successHandler); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, fatalHandler); err != nil {
		t.Fatal(err)
	}

	id := uuid.New().String()
	createdAt := time.Now().Add(-time.Minute)
	seedOutboxEventWithID(t, db, o, id, newTestEvent("rematch-partial-fatal"), []string{}, createdAt, createdAt, sql.NullTime{})

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}

	if !successHandler.Wait(2 * time.Second) {
		t.Fatal("success handler did not run")
	}
	if got := outboxRowCount(t, db); got != 0 {
		t.Fatalf("outbox rows = %d, want 0 after success+terminal", got)
	}
	assertDeadLetterHandlers(t, db, []string{fatalHandler.Type})
	record := deadLetterRecordByHandler(t, db, fatalHandler.Type)
	if record.OutboxID != id {
		t.Fatalf("outbox_id = %s, want %s", record.OutboxID, id)
	}
	if record.RemainingHandlers != "[]" {
		t.Fatalf("remaining_handlers = %s, want []", record.RemainingHandlers)
	}
}

// TestOutboxRematchFatalLeavesRetryable mirrors mixed terminal+retryable finalize
// after rematch from handlers=[] (persist matched set at claim).
func TestOutboxRematchFatalLeavesRetryable(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithRetryBackoff("FIXED:2:200ms"))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	fatalHandler := mocks.NewEventHandler("rematch_mixed_fatal")
	fatalHandler.Err = fatalOutboxError{err: errors.New("fatal rematch")}
	retryableHandler := mocks.NewEventHandler("rematch_mixed_retry")
	retryableHandler.Err = errors.New("temporary rematch")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, fatalHandler); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, retryableHandler); err != nil {
		t.Fatal(err)
	}

	id := uuid.New().String()
	createdAt := time.Now().Add(-time.Minute)
	seedOutboxEventWithID(t, db, o, id, newTestEvent("rematch-fatal-retry"), []string{}, createdAt, createdAt, sql.NullTime{})

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}

	assertDeadLetterHandlers(t, db, []string{fatalHandler.Type})
	assertOutboxHandlers(t, db, id, []string{retryableHandler.Type})
	var retryCount int
	var availableAt time.Time
	var takenAt sql.NullTime
	if err := db.QueryRow(`SELECT retry_count, available_at, taken_at FROM outbox WHERE id = ?`, id).
		Scan(&retryCount, &availableAt, &takenAt); err != nil {
		t.Fatal(err)
	}
	if retryCount != 1 {
		t.Fatalf("retry_count = %d, want 1", retryCount)
	}
	if takenAt.Valid {
		t.Fatal("taken_at should be cleared for retry")
	}
	if !availableAt.After(time.Now().Add(100 * time.Millisecond)) {
		t.Fatalf("available_at = %s, want future retry", availableAt)
	}
	record := deadLetterRecordByHandler(t, db, fatalHandler.Type)
	if record.RemainingHandlers != fmt.Sprintf(`["%s"]`, retryableHandler.Type) {
		t.Fatalf("remaining_handlers = %s, want retryable only", record.RemainingHandlers)
	}
}

func TestOutboxNoMatchDeadLetterRollsBackWithTransaction(t *testing.T) {
	db := newTestDB(t)
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	o.Start()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := o.HandleEvent(txctx.NewContextWithTx(context.Background(), tx), newTestEvent("rollback-no-match")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if got := deadLetterRowCount(t, db); got != 0 {
		t.Fatalf("dead letter rows after rollback = %d, want 0", got)
	}
	if got := outboxRowCount(t, db); got != 0 {
		t.Fatalf("outbox rows after rollback = %d, want 0", got)
	}
}

func TestOutboxRetryableErrorSchedulesBackoff(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	o, err := NewOutbox(db, WithMaxRetries(2), WithRetryBackoff("FIXED:2:200ms"))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	handler := mocks.NewEventHandler("retry_handler")
	handler.Err = errors.New("temporary failure")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}
	// Allow HandleEvent without starting the processor (unit path uses processBatch).
	o.processorRunning.Store(true)

	if err := o.HandleEvent(ctx, newTestEvent("retry")); err != nil {
		t.Fatal(err)
	}
	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed count = %d, want 1", processed)
	}

	var retryCount int
	var availableAt time.Time
	var takenAt sql.NullTime
	if err := db.QueryRow(`SELECT retry_count, available_at, taken_at FROM outbox LIMIT 1`).Scan(&retryCount, &availableAt, &takenAt); err != nil {
		t.Fatal(err)
	}
	if retryCount != 1 {
		t.Fatalf("retry_count = %d, want 1", retryCount)
	}
	if !availableAt.After(time.Now().Add(100 * time.Millisecond)) {
		t.Fatalf("available_at = %s, want retry backoff in the future", availableAt)
	}
	if takenAt.Valid {
		t.Fatalf("taken_at valid = true, want retry row released")
	}
	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 0 {
		t.Fatalf("immediate processed count = %d, want 0", processed)
	}
}

func TestOutboxTerminalFailureWritesDeadLetter(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	o, err := NewOutbox(db, WithRetryBackoff("FIXED:1:1ms"), WithMaxRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	handler := mocks.NewEventHandler("terminal_handler")
	handler.Err = errors.New("permanent failure")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}
	// Allow HandleEvent without starting the processor (unit path uses processBatch).
	o.processorRunning.Store(true)

	if err := o.HandleEvent(ctx, newTestEvent("terminal")); err != nil {
		t.Fatal(err)
	}
	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed count = %d, want 1", processed)
	}

	if got := outboxRowCount(t, db); got != 0 {
		t.Fatalf("outbox rows = %d, want 0", got)
	}
	if got := deadLetterRowCount(t, db); got != 1 {
		t.Fatalf("dead letter rows = %d, want 1", got)
	}
	var source, handlerType string
	if err := db.QueryRow(`SELECT source, handler_type FROM dead_letters LIMIT 1`).Scan(&source, &handlerType); err != nil {
		t.Fatal(err)
	}
	if source != "outbox" || handlerType != "terminal_handler" {
		t.Fatalf("dead letter = (%s, %s), want outbox/terminal_handler", source, handlerType)
	}
}

func TestOutboxFatalHandlerDeadLetterDoesNotDropSuccessfulHandler(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	successHandler := mocks.NewEventHandler("fatal_success_handler")
	fatalHandler := mocks.NewEventHandler("fatal_failed_handler")
	fatalHandler.Err = fatalOutboxError{err: errors.New("fatal handler failure")}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, successHandler); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, fatalHandler); err != nil {
		t.Fatal(err)
	}

	id := uuid.New().String()
	createdAt := time.Now().Add(-time.Minute)
	seedOutboxEventWithID(t, db, o, id, newTestEvent("fatal-success"), []string{successHandler.Type, fatalHandler.Type}, createdAt, createdAt, sql.NullTime{})

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}

	if got := outboxRowCount(t, db); got != 0 {
		t.Fatalf("outbox rows = %d, want 0", got)
	}
	assertDeadLetterHandlers(t, db, []string{fatalHandler.Type})
	record := deadLetterRecordByHandler(t, db, fatalHandler.Type)
	if record.OutboxID != id {
		t.Fatalf("outbox_id = %s, want %s", record.OutboxID, id)
	}
	if record.RemainingHandlers != "[]" {
		t.Fatalf("remaining_handlers = %s, want []", record.RemainingHandlers)
	}
}

func TestOutboxFatalHandlerDeadLetterLeavesRetryableHandler(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithRetryBackoff("FIXED:2:200ms"))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	fatalHandler := mocks.NewEventHandler("mixed_fatal_handler")
	fatalHandler.Err = fatalOutboxError{err: errors.New("fatal handler failure")}
	retryableHandler := mocks.NewEventHandler("mixed_retry_handler")
	retryableHandler.Err = errors.New("temporary handler failure")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, fatalHandler); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, retryableHandler); err != nil {
		t.Fatal(err)
	}

	id := uuid.New().String()
	createdAt := time.Now().Add(-time.Minute)
	seedOutboxEventWithID(t, db, o, id, newTestEvent("fatal-retry"), []string{fatalHandler.Type, retryableHandler.Type}, createdAt, createdAt, sql.NullTime{})

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}

	assertDeadLetterHandlers(t, db, []string{fatalHandler.Type})
	assertOutboxHandlers(t, db, id, []string{retryableHandler.Type})
	var retryCount int
	var availableAt time.Time
	var takenAt sql.NullTime
	if err := db.QueryRow(`SELECT retry_count, available_at, taken_at FROM outbox WHERE id = ?`, id).Scan(&retryCount, &availableAt, &takenAt); err != nil {
		t.Fatal(err)
	}
	if retryCount != 1 {
		t.Fatalf("retry_count = %d, want 1", retryCount)
	}
	if !availableAt.After(time.Now().Add(100 * time.Millisecond)) {
		t.Fatalf("available_at = %s, want future retry", availableAt)
	}
	if takenAt.Valid {
		t.Fatal("taken_at should be cleared for retry")
	}
	record := deadLetterRecordByHandler(t, db, fatalHandler.Type)
	if record.RemainingHandlers != fmt.Sprintf(`["%s"]`, retryableHandler.Type) {
		t.Fatalf("remaining_handlers = %s, want retryable handler only", record.RemainingHandlers)
	}
}

func TestOutboxExhaustedRetriesWritesDeadLetterPerFailedHandler(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithMaxRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	first := mocks.NewEventHandler("exhausted_first_handler")
	first.Err = errors.New("first failure")
	second := mocks.NewEventHandler("exhausted_second_handler")
	second.Err = errors.New("second failure")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, first); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, second); err != nil {
		t.Fatal(err)
	}

	id := uuid.New().String()
	createdAt := time.Now().Add(-time.Minute)
	seedOutboxEventWithID(t, db, o, id, newTestEvent("exhausted"), []string{first.Type, second.Type}, createdAt, createdAt, sql.NullTime{})

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}

	if got := outboxRowCount(t, db); got != 0 {
		t.Fatalf("outbox rows = %d, want 0", got)
	}
	assertDeadLetterHandlers(t, db, []string{first.Type, second.Type})
	if got := deadLetterRowCount(t, db); got != 2 {
		t.Fatalf("dead letter rows = %d, want 2", got)
	}
}

func TestOutboxPermanentFailureDoesNotBlockRemainingReceiver(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	fatalHandler := mocks.NewEventHandler("first_permanent_handler")
	fatalHandler.Err = fatalOutboxError{err: errors.New("fatal handler failure")}
	remainingHandler := mocks.NewEventHandler("remaining_handler")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, fatalHandler); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, remainingHandler); err != nil {
		t.Fatal(err)
	}

	id := uuid.New().String()
	createdAt := time.Now().Add(-time.Minute)
	seedOutboxEventWithID(t, db, o, id, newTestEvent("remaining"), []string{fatalHandler.Type}, createdAt, createdAt, sql.NullTime{})
	seedOutboxEventWithID(t, db, o, uuid.New().String(), newTestEvent("next"), []string{remainingHandler.Type}, createdAt, createdAt, sql.NullTime{})

	processAllBatches(t, o, ctx)

	if got := outboxRowCount(t, db); got != 0 {
		t.Fatalf("outbox rows = %d, want 0", got)
	}
	if !remainingHandler.Wait(time.Second) {
		t.Fatal("remaining receiver did not process its event")
	}
	assertDeadLetterHandlers(t, db, []string{fatalHandler.Type})
}

func TestOutboxDeadLetterExporterBestEffort(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	exporter := &recordingDeadLetterExporter{err: errors.New("disk full")}
	o, err := NewOutbox(db, WithMaxRetries(0), WithDeadLetterExporter(exporter))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	first := mocks.NewEventHandler("export_first_handler")
	first.Err = errors.New("first failure")
	second := mocks.NewEventHandler("export_second_handler")
	second.Err = errors.New("second failure")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, first); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, second); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	seedOutboxEventWithID(t, db, o, uuid.New().String(), newTestEvent("export"), []string{first.Type, second.Type}, createdAt, createdAt, sql.NullTime{})

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}

	if got := deadLetterRowCount(t, db); got != 2 {
		t.Fatalf("dead letter rows = %d, want 2", got)
	}
	if got := len(exporter.Records()); got != 2 {
		t.Fatalf("exported records = %d, want 2", got)
	}
	if got := exportedDeadLetterCount(t, db); got != 0 {
		t.Fatalf("exported dead letters = %d, want 0 after exporter error", got)
	}
}

func TestOutboxDeadLetterExporterMarksExportedAt(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	exporter := &recordingDeadLetterExporter{}
	o, err := NewOutbox(db, WithMaxRetries(0), WithDeadLetterExporter(exporter))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := mocks.NewEventHandler("export_success_handler")
	handler.Err = errors.New("handler failure")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	seedOutboxEventWithID(t, db, o, uuid.New().String(), newTestEvent("export-success"), []string{handler.Type}, createdAt, createdAt, sql.NullTime{})

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}

	if got := deadLetterRowCount(t, db); got != 1 {
		t.Fatalf("dead letter rows = %d, want 1", got)
	}
	if got := exportedDeadLetterCount(t, db); got != 1 {
		t.Fatalf("exported dead letters = %d, want 1", got)
	}
}

// TestOutboxDeadLetterExportAfterFinalize asserts that file export runs only after
// the atomic finalize transaction has committed (DLQ + handlers/retry/delete).
// The exporter observes the post-finalize DB state: the terminal handler is no
// longer present on the outbox row (or the row is gone).
func TestOutboxDeadLetterExportAfterFinalize(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	var observedHandlers []string
	var outboxMissing bool
	var deadLetterPresent bool
	exporter := dl.ExportFunc(func(_ context.Context, record dl.Record) error {
		var handlersBlob string
		err := db.QueryRow(`SELECT handlers FROM outbox WHERE id = ?`, record.OutboxID).Scan(&handlersBlob)
		if errors.Is(err, sql.ErrNoRows) {
			outboxMissing = true
		} else if err != nil {
			t.Errorf("exporter could not read outbox: %v", err)
			return nil
		} else {
			if err := json.Unmarshal([]byte(handlersBlob), &observedHandlers); err != nil {
				t.Errorf("exporter could not unmarshal handlers: %v", err)
			}
		}
		var count int
		if err := db.QueryRow(`
			SELECT COUNT(*) FROM dead_letters
			WHERE source = ? AND outbox_id = ? AND handler_type = ?
		`, record.Source, record.OutboxID, record.HandlerType).Scan(&count); err != nil {
			t.Errorf("exporter could not read dead_letters: %v", err)
			return nil
		}
		deadLetterPresent = count == 1
		return nil
	})

	o, err := NewOutbox(db, WithMaxRetries(0), WithDeadLetterExporter(exporter))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := mocks.NewEventHandler("export_after_finalize_handler")
	handler.Err = errors.New("terminal failure")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	outboxID := uuid.New().String()
	createdAt := time.Now().Add(-time.Minute)
	seedOutboxEventWithID(t, db, o, outboxID, newTestEvent("export-after-finalize"), []string{handler.Type}, createdAt, createdAt, sql.NullTime{})

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}

	if !deadLetterPresent {
		t.Fatal("exporter must observe the committed dead_letters row")
	}
	if !outboxMissing {
		// Single-handler terminal: outbox row must already be deleted when export runs.
		t.Fatalf("exporter observed outbox handlers %v; want row deleted after finalize commit", observedHandlers)
	}
}

// TestOutboxDeadLetterUniqueSourceOutboxHandler enforces uniqueness of
// (source, outbox_id, handler_type) so a crash-replay cannot create duplicate DLQs.
func TestOutboxDeadLetterUniqueSourceOutboxHandler(t *testing.T) {
	db := newTestDB(t)
	// EnsureSchema runs via NewOutbox.
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	now := time.Now()
	if _, err := db.Exec(`
		INSERT INTO dead_letters (id, source, event_type, aggregate_id, handler_type, outbox_id, remaining_handlers, blob, error, retry_count, created_at, dead_at)
		VALUES (?, 'outbox', 'Event', 'agg', 'handler_a', 'outbox-1', '[]', '{}', 'err', 0, ?, ?)
	`, uuid.New().String(), now, now); err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		INSERT INTO dead_letters (id, source, event_type, aggregate_id, handler_type, outbox_id, remaining_handlers, blob, error, retry_count, created_at, dead_at)
		VALUES (?, 'outbox', 'Event', 'agg', 'handler_a', 'outbox-1', '[]', '{}', 'err-dup', 0, ?, ?)
	`, uuid.New().String(), now, now)
	if err == nil {
		t.Fatal("expected unique constraint violation for duplicate (source, outbox_id, handler_type)")
	}

	// Distinct handler_type for the same outbox id must still be allowed.
	if _, err := db.Exec(`
		INSERT INTO dead_letters (id, source, event_type, aggregate_id, handler_type, outbox_id, remaining_handlers, blob, error, retry_count, created_at, dead_at)
		VALUES (?, 'outbox', 'Event', 'agg', 'handler_b', 'outbox-1', '[]', '{}', 'err', 0, ?, ?)
	`, uuid.New().String(), now, now); err != nil {
		t.Fatalf("distinct handler_type insert failed: %v", err)
	}
}

// TestOutboxFinalizeIdempotentOnReplay simulates a crash after DLQ insert but before
// handlers were updated: the DLQ row already exists. Re-processing must not create a
// second dead letter and must still remove the terminal handler (and apply retry fields).
func TestOutboxFinalizeIdempotentOnReplay(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithRetryBackoff("FIXED:2:200ms"))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	fatalHandler := mocks.NewEventHandler("replay_fatal_handler")
	fatalHandler.Err = fatalOutboxError{err: errors.New("fatal again")}
	retryHandler := mocks.NewEventHandler("replay_retry_handler")
	retryHandler.Err = errors.New("retryable again")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, fatalHandler); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, retryHandler); err != nil {
		t.Fatal(err)
	}

	outboxID := uuid.New().String()
	createdAt := time.Now().Add(-time.Minute)
	event := newTestEvent("replay-finalize")
	seedOutboxEventWithID(t, db, o, outboxID, event, []string{fatalHandler.Type, retryHandler.Type}, createdAt, createdAt, sql.NullTime{})

	// Pre-seed a dead letter as if the previous attempt wrote DLQ then crashed.
	eventBlob, err := o.codec.MarshalEvent(ctx, event)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if _, err := db.Exec(`
		INSERT INTO dead_letters (id, source, event_type, aggregate_id, handler_type, outbox_id, remaining_handlers, blob, error, retry_count, created_at, dead_at)
		VALUES (?, 'outbox', ?, ?, ?, ?, ?, ?, 'previous fatal', 0, ?, ?)
	`, uuid.New().String(), event.EventType().String(), event.AggregateID().String(), fatalHandler.Type, outboxID, fmt.Sprintf(`["%s"]`, retryHandler.Type), string(eventBlob), createdAt, now); err != nil {
		t.Fatal(err)
	}

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}

	if got := deadLetterRowCount(t, db); got != 1 {
		t.Fatalf("dead letter rows = %d, want 1 (no duplicate on replay)", got)
	}
	assertOutboxHandlers(t, db, outboxID, []string{retryHandler.Type})
	var retryCount int
	var availableAt time.Time
	var takenAt sql.NullTime
	if err := db.QueryRow(`SELECT retry_count, available_at, taken_at FROM outbox WHERE id = ?`, outboxID).Scan(&retryCount, &availableAt, &takenAt); err != nil {
		t.Fatal(err)
	}
	if retryCount != 1 {
		t.Fatalf("retry_count = %d, want 1", retryCount)
	}
	if !availableAt.After(time.Now().Add(100 * time.Millisecond)) {
		t.Fatalf("available_at = %s, want future backoff", availableAt)
	}
	if takenAt.Valid {
		t.Fatal("taken_at should be cleared after atomic finalize with retry")
	}
}

// TestOutboxFinalizeAtomicMixedTerminalAndRetry checks that a single finalize
// transaction leaves DLQ, remaining handlers, and retry fields consistent.
func TestOutboxFinalizeAtomicMixedTerminalAndRetry(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithRetryBackoff("FIXED:2:300ms"))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	fatalHandler := mocks.NewEventHandler("atomic_fatal_handler")
	fatalHandler.Err = fatalOutboxError{err: errors.New("fatal")}
	retryHandler := mocks.NewEventHandler("atomic_retry_handler")
	retryHandler.Err = errors.New("retryable")
	successHandler := mocks.NewEventHandler("atomic_success_handler")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, fatalHandler); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, retryHandler); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, successHandler); err != nil {
		t.Fatal(err)
	}

	outboxID := uuid.New().String()
	createdAt := time.Now().Add(-time.Minute)
	seedOutboxEventWithID(t, db, o, outboxID, newTestEvent("atomic-mixed"), []string{
		fatalHandler.Type, retryHandler.Type, successHandler.Type,
	}, createdAt, createdAt, sql.NullTime{})

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}

	// Atomic outcome: exactly one DLQ for the fatal handler, retryable remains,
	// successful removed, retry scheduled.
	if got := deadLetterRowCount(t, db); got != 1 {
		t.Fatalf("dead letter rows = %d, want 1", got)
	}
	assertDeadLetterHandlers(t, db, []string{fatalHandler.Type})
	assertOutboxHandlers(t, db, outboxID, []string{retryHandler.Type})
	var retryCount int
	var availableAt time.Time
	var takenAt sql.NullTime
	if err := db.QueryRow(`SELECT retry_count, available_at, taken_at FROM outbox WHERE id = ?`, outboxID).Scan(&retryCount, &availableAt, &takenAt); err != nil {
		t.Fatal(err)
	}
	if retryCount != 1 {
		t.Fatalf("retry_count = %d, want 1", retryCount)
	}
	if !availableAt.After(time.Now().Add(100 * time.Millisecond)) {
		t.Fatalf("available_at = %s, want future backoff", availableAt)
	}
	if takenAt.Valid {
		t.Fatal("taken_at should be cleared")
	}
	record := deadLetterRecordByHandler(t, db, fatalHandler.Type)
	if record.OutboxID != outboxID {
		t.Fatalf("outbox_id = %s, want %s", record.OutboxID, outboxID)
	}
	if record.RemainingHandlers != fmt.Sprintf(`["%s"]`, retryHandler.Type) {
		t.Fatalf("remaining_handlers = %s, want only retry handler", record.RemainingHandlers)
	}
}

// TestOutboxFinalizeAtomicAllTerminalDeletesOutbox ensures multi-handler permanent
// failure writes all DLQ rows and deletes the outbox row together.
func TestOutboxFinalizeAtomicAllTerminalDeletesOutbox(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	exporter := &recordingDeadLetterExporter{}
	o, err := NewOutbox(db, WithMaxRetries(0), WithDeadLetterExporter(exporter))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	first := mocks.NewEventHandler("all_terminal_a")
	first.Err = errors.New("a failed")
	second := mocks.NewEventHandler("all_terminal_b")
	second.Err = errors.New("b failed")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, first); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, second); err != nil {
		t.Fatal(err)
	}

	outboxID := uuid.New().String()
	createdAt := time.Now().Add(-time.Minute)
	seedOutboxEventWithID(t, db, o, outboxID, newTestEvent("all-terminal"), []string{first.Type, second.Type}, createdAt, createdAt, sql.NullTime{})

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}

	if got := outboxRowCount(t, db); got != 0 {
		t.Fatalf("outbox rows = %d, want 0", got)
	}
	if got := deadLetterRowCount(t, db); got != 2 {
		t.Fatalf("dead letter rows = %d, want 2", got)
	}
	if got := len(exporter.Records()); got != 2 {
		t.Fatalf("exported records = %d, want 2 (after commit)", got)
	}
	if got := exportedDeadLetterCount(t, db); got != 2 {
		t.Fatalf("exported_at set = %d, want 2", got)
	}
}

// TestOutboxFinalizeRollbackOnHandlersUpdateAbort proves the finalize transaction
// rolls back when a later step fails: DLQ INSERT runs first, then UPDATE handlers
// is aborted by a SQLite trigger. No partial DLQ, handlers/retry fields, or export.
func TestOutboxFinalizeRollbackOnHandlersUpdateAbort(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	exporter := &recordingDeadLetterExporter{}
	o, err := NewOutbox(db, WithRetryBackoff("FIXED:2:500ms"), WithDeadLetterExporter(exporter))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	// Abort the handlers list update after any preceding DLQ insert in the same TX.
	if _, err := db.Exec(`
		CREATE TRIGGER abort_finalize_handlers
		BEFORE UPDATE OF handlers ON outbox
		BEGIN
			SELECT RAISE(ABORT, 'forced finalize abort');
		END
	`); err != nil {
		t.Fatal(err)
	}

	fatalHandler := mocks.NewEventHandler("rollback_fatal_handler")
	fatalHandler.Err = fatalOutboxError{err: errors.New("fatal during finalize")}
	retryHandler := mocks.NewEventHandler("rollback_retry_handler")
	retryHandler.Err = errors.New("retryable during finalize")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, fatalHandler); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, retryHandler); err != nil {
		t.Fatal(err)
	}

	outboxID := uuid.New().String()
	createdAt := time.Now().Add(-time.Minute).UTC().Truncate(time.Millisecond)
	availableAt := createdAt
	originalHandlers := []string{fatalHandler.Type, retryHandler.Type}
	seedOutboxEventWithID(t, db, o, outboxID, newTestEvent("finalize-rollback"), originalHandlers, createdAt, availableAt, sql.NullTime{})

	// Drain any residual errors, then process.
	drainOutboxErrors(o)
	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}

	if got := deadLetterRowCount(t, db); got != 0 {
		t.Fatalf("dead letter rows = %d, want 0 after finalize rollback", got)
	}
	if got := len(exporter.Records()); got != 0 {
		t.Fatalf("exporter calls = %d, want 0 after finalize rollback", got)
	}
	assertOutboxHandlers(t, db, outboxID, originalHandlers)

	var retryCount int
	var gotAvailableAt time.Time
	var takenAt sql.NullTime
	if err := db.QueryRow(`SELECT retry_count, available_at, taken_at FROM outbox WHERE id = ?`, outboxID).Scan(&retryCount, &gotAvailableAt, &takenAt); err != nil {
		t.Fatal(err)
	}
	if retryCount != 0 {
		t.Fatalf("retry_count = %d, want 0 (retry fields rolled back)", retryCount)
	}
	if !gotAvailableAt.Equal(availableAt) {
		t.Fatalf("available_at = %s, want original %s", gotAvailableAt, availableAt)
	}
	// Claim (taken_at) commits in a separate transaction before finalize; after a
	// finalize rollback the claim must remain so the row is not double-dispatched
	// until PeriodicSweepAge releases it.
	if !takenAt.Valid {
		t.Fatal("taken_at should remain set after claim; finalize rollback must not clear it")
	}

	finalizeErrs := drainOutboxErrors(o)
	if !containsErrorSubstring(finalizeErrs, "forced finalize abort") && !containsErrorSubstring(finalizeErrs, "could not update remaining handlers") {
		t.Fatalf("expected finalize abort on Errors(), got: %v", finalizeErrs)
	}
}

// TestOutboxExportUsesDurableDeadLetterOnReplay ensures that after ON CONFLICT the
// exporter receives the durable dead_letters row (blob/error/id), not a freshly
// synthesized candidate, and marks exported_at on that row's id.
func TestOutboxExportUsesDurableDeadLetterOnReplay(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	exporter := &recordingDeadLetterExporter{}
	o, err := NewOutbox(db, WithMaxRetries(0), WithDeadLetterExporter(exporter))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := mocks.NewEventHandler("export_durable_handler")
	handler.Err = errors.New("new terminal failure from this attempt")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	outboxID := uuid.New().String()
	createdAt := time.Now().Add(-time.Minute)
	event := newTestEvent("export-durable")
	seedOutboxEventWithID(t, db, o, outboxID, event, []string{handler.Type}, createdAt, createdAt, sql.NullTime{})

	existingID := uuid.New().String()
	existingBlob := `{"durable":"previous-blob-not-from-this-attempt"}`
	existingError := "previous durable dead letter error"
	existingRemaining := `[]`
	existingCreatedAt := createdAt.Add(-time.Hour)
	existingDeadAt := createdAt.Add(-30 * time.Minute)
	if _, err := db.Exec(`
		INSERT INTO dead_letters (id, source, event_type, aggregate_id, handler_type, outbox_id, remaining_handlers, blob, error, retry_count, created_at, dead_at)
		VALUES (?, 'outbox', ?, ?, ?, ?, ?, ?, ?, 3, ?, ?)
	`, existingID, event.EventType().String(), event.AggregateID().String(), handler.Type, outboxID, existingRemaining, existingBlob, existingError, existingCreatedAt, existingDeadAt); err != nil {
		t.Fatal(err)
	}

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}

	if got := deadLetterRowCount(t, db); got != 1 {
		t.Fatalf("dead letter rows = %d, want 1", got)
	}
	records := exporter.Records()
	if len(records) != 1 {
		t.Fatalf("exported records = %d, want 1", len(records))
	}
	got := records[0]
	if got.ID != existingID {
		t.Fatalf("exported id = %s, want durable id %s", got.ID, existingID)
	}
	if got.Blob != existingBlob {
		t.Fatalf("exported blob = %q, want durable blob %q", got.Blob, existingBlob)
	}
	if got.Error != existingError {
		t.Fatalf("exported error = %q, want durable error %q", got.Error, existingError)
	}
	if got.RetryCount != 3 {
		t.Fatalf("exported retry_count = %d, want 3 from durable row", got.RetryCount)
	}
	if got.RemainingHandlers != existingRemaining {
		t.Fatalf("exported remaining_handlers = %q, want %q", got.RemainingHandlers, existingRemaining)
	}

	var exportedID string
	var exportedAt sql.NullTime
	if err := db.QueryRow(`SELECT id, exported_at FROM dead_letters WHERE source = 'outbox' AND outbox_id = ? AND handler_type = ?`, outboxID, handler.Type).Scan(&exportedID, &exportedAt); err != nil {
		t.Fatal(err)
	}
	if exportedID != existingID {
		t.Fatalf("stored id = %s, want %s", exportedID, existingID)
	}
	if !exportedAt.Valid {
		t.Fatal("exported_at should be set on the durable row id")
	}
}

// TestOutboxSuccessPathFinalizeDeletesWithoutDeadLetter covers pure success.
func TestOutboxSuccessPathFinalizeDeletesWithoutDeadLetter(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := mocks.NewEventHandler("success_only_handler")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	outboxID := uuid.New().String()
	createdAt := time.Now().Add(-time.Minute)
	seedOutboxEventWithID(t, db, o, outboxID, newTestEvent("success-only"), []string{handler.Type}, createdAt, createdAt, sql.NullTime{})

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}
	if got := outboxRowCount(t, db); got != 0 {
		t.Fatalf("outbox rows = %d, want 0", got)
	}
	if got := deadLetterRowCount(t, db); got != 0 {
		t.Fatalf("dead letter rows = %d, want 0", got)
	}
}

func TestOutboxSerialDispatchPreservesCreatedAtIDOrder(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := newOrderingHandler("serial_order_handler")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	for range 100 {
		id := uuid.New().String()
		seedOutboxEventWithID(t, db, o, id, newTestEvent(id), []string{handler.Type}, createdAt, createdAt, sql.NullTime{})
	}
	expected := outboxIDsInDispatchOrder(t, db)

	processAllBatches(t, o, ctx)

	if got := handler.Contents(); !slicesEqual(got, expected) {
		t.Fatalf("serial order mismatch\n got: %v\nwant: %v", got, expected)
	}
}

func TestOutboxSerialDispatchDoesNotOverlapHandler(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := newConcurrencyHandler("serial_no_overlap_handler", time.Millisecond)
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	for i := range 20 {
		id := uuid.New().String()
		seedOutboxEventWithID(t, db, o, id, newTestEvent(fmt.Sprintf("event-%d", i)), []string{handler.Type}, createdAt, createdAt, sql.NullTime{})
	}

	processAllBatches(t, o, ctx)

	if got := handler.MaxInFlight(); got != 1 {
		t.Fatalf("max in-flight = %d, want 1", got)
	}
}

func TestOutboxPartitionByAggregatePreservesSameAggregateOrder(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := newOrderingHandler("partition_same_aggregate_handler")
	if err := o.AddHandlerWithOptions(ctx, eh.MatchEvents{mocks.EventType}, handler, WithDispatchMode(PartitionByAggregate), WithPartitionShards(8)); err != nil {
		t.Fatal(err)
	}

	aggregateID := uuid.New()
	createdAt := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	for range 50 {
		id := uuid.New().String()
		seedOutboxEventWithID(t, db, o, id, newTestEventForAggregate(id, aggregateID), []string{handler.Type}, createdAt, createdAt, sql.NullTime{})
	}
	expected := outboxIDsInDispatchOrder(t, db)

	processAllBatches(t, o, ctx)

	if got := handler.Contents(); !slicesEqual(got, expected) {
		t.Fatalf("partition order mismatch\n got: %v\nwant: %v", got, expected)
	}
}

func TestOutboxPartitionByAggregateAllowsDifferentAggregatesInParallel(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithMaxGoroutines(2))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	entered := make(chan string, 2)
	release := make(chan struct{})
	handler := newBlockingHandler("partition_parallel_handler", entered, release)
	if err := o.AddHandlerWithOptions(ctx, eh.MatchEvents{mocks.EventType}, handler, WithDispatchMode(PartitionByAggregate), WithPartitionShards(16)); err != nil {
		t.Fatal(err)
	}

	aggregates := aggregateIDsForDistinctShards(t, 2, 16)
	createdAt := time.Now().Add(-time.Minute)
	for i, aggregateID := range aggregates {
		id := uuid.New().String()
		seedOutboxEventWithID(t, db, o, id, newTestEventForAggregate(fmt.Sprintf("event-%d", i), aggregateID), []string{handler.Type}, createdAt, createdAt, sql.NullTime{})
	}

	done := make(chan error, 1)
	go func() {
		_, err := o.processBatch(ctx)
		done <- err
	}()

	waitForEntries(t, entered, 2)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := handler.MaxInFlight(); got < 2 {
		t.Fatalf("max in-flight = %d, want at least 2", got)
	}
}

func TestOutboxPartitionByAggregateUsesCorrelationIDFallback(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithMaxGoroutines(2))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	entered := make(chan string, 2)
	release := make(chan struct{})
	handler := newBlockingHandler("partition_correlation_handler", entered, release)
	if err := o.AddHandlerWithOptions(ctx, eh.MatchEvents{mocks.EventType}, handler, WithDispatchMode(PartitionByAggregate), WithPartitionShards(16)); err != nil {
		t.Fatal(err)
	}

	correlationIDs := correlationIDsForDistinctShards(t, 2, 16)
	createdAt := time.Now().Add(-time.Minute)
	for i, correlationID := range correlationIDs {
		id := uuid.New().String()
		event := eh.NewEvent(mocks.EventType, &mocks.EventData{Content: fmt.Sprintf("event-%d", i)}, time.Now(),
			eh.WithMetadata(map[string]any{"correlation_id": correlationID}),
		)
		seedOutboxEventWithID(t, db, o, id, event, []string{handler.Type}, createdAt, createdAt, sql.NullTime{})
	}

	done := make(chan error, 1)
	go func() {
		_, err := o.processBatch(ctx)
		done <- err
	}()

	waitForEntries(t, entered, 2)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := handler.MaxInFlight(); got < 2 {
		t.Fatalf("max in-flight = %d, want at least 2", got)
	}
}

func TestOutboxDispatchRespectsGlobalMaxGoroutines(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithMaxGoroutines(2))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := newConcurrencyHandler("partition_cap_handler", 10*time.Millisecond)
	if err := o.AddHandlerWithOptions(ctx, eh.MatchEvents{mocks.EventType}, handler, WithDispatchMode(PartitionByAggregate), WithPartitionShards(16)); err != nil {
		t.Fatal(err)
	}

	aggregates := aggregateIDsForDistinctShards(t, 8, 16)
	createdAt := time.Now().Add(-time.Minute)
	for i, aggregateID := range aggregates {
		id := uuid.New().String()
		seedOutboxEventWithID(t, db, o, id, newTestEventForAggregate(fmt.Sprintf("event-%d", i), aggregateID), []string{handler.Type}, createdAt, createdAt, sql.NullTime{})
	}

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != len(aggregates) {
		t.Fatalf("processed = %d, want %d", processed, len(aggregates))
	}
	if got := handler.MaxInFlight(); got > 2 {
		t.Fatalf("max in-flight = %d, want <= 2", got)
	}
}

func TestOutboxPartialSuccessKeepsOnlyFailedHandlerForRetry(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithRetryBackoff("FIXED:2:200ms"))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	successHandler := mocks.NewEventHandler("partial_success_handler")
	failingHandler := mocks.NewEventHandler("partial_failing_handler")
	failingHandler.Err = errors.New("temporary failure")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, successHandler); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, failingHandler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	id := uuid.New().String()
	seedOutboxEventWithID(t, db, o, id, newTestEvent("partial"), []string{successHandler.Type, failingHandler.Type}, createdAt, createdAt, sql.NullTime{})

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}

	var handlersBlob string
	var retryCount int
	var takenAt sql.NullTime
	if err := db.QueryRow(`SELECT handlers, retry_count, taken_at FROM outbox WHERE id = ?`, id).Scan(&handlersBlob, &retryCount, &takenAt); err != nil {
		t.Fatal(err)
	}
	var handlers []string
	if err := json.Unmarshal([]byte(handlersBlob), &handlers); err != nil {
		t.Fatal(err)
	}
	if !slicesEqual(handlers, []string{failingHandler.Type}) {
		t.Fatalf("remaining handlers = %v, want [%s]", handlers, failingHandler.Type)
	}
	if retryCount != 1 {
		t.Fatalf("retry_count = %d, want 1", retryCount)
	}
	if takenAt.Valid {
		t.Fatal("taken_at should be cleared for retry")
	}
}

func newTestDB(t testing.TB) *sql.DB {
	t.Helper()

	f, err := os.CreateTemp("", "outbox-*.db")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Get a new SQLite database.
	db, err := sql.Open("sqlite3", f.Name()+"?_journal=wal&_busy_timeout=5000&_synchronous=normal&_fk=1&_loc=auto")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		db.Close()
		os.Remove(f.Name())
	})

	return db
}

func setPeriodicSweepInterval(t testing.TB, interval time.Duration) func() {
	t.Helper()

	previous := PeriodicSweepInterval
	PeriodicSweepInterval = interval
	return func() {
		PeriodicSweepInterval = previous
	}
}

func setPeriodicSweepAge(t testing.TB, age time.Duration) func() {
	t.Helper()

	previous := PeriodicSweepAge
	PeriodicSweepAge = age
	return func() {
		PeriodicSweepAge = previous
	}
}

func newTestEvent(content string) eh.Event {
	return eh.NewEvent(mocks.EventType, &mocks.EventData{Content: content}, time.Now(),
		eh.ForAggregate(mocks.AggregateType, uuid.New(), 1),
	)
}

func newTestEventForAggregate(content string, aggregateID uuid.UUID) eh.Event {
	return eh.NewEvent(mocks.EventType, &mocks.EventData{Content: content}, time.Now(),
		eh.ForAggregate(mocks.AggregateType, aggregateID, 1),
	)
}

func seedOutboxEvent(t testing.TB, db *sql.DB, o *Outbox, event eh.Event, handlerType string, createdAt, availableAt time.Time, takenAt sql.NullTime) {
	t.Helper()

	eventBlob, err := o.codec.MarshalEvent(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	handlersBlob := fmt.Sprintf(`["%s"]`, handlerType)

	if _, err := db.Exec(`
		INSERT INTO outbox (id, event_type, aggregate_id, created_at, available_at, taken_at, handlers, event_blob, retry_count)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, uuid.New().String(), event.EventType().String(), event.AggregateID().String(), createdAt, availableAt, takenAt, handlersBlob, string(eventBlob), 0); err != nil {
		t.Fatal(err)
	}
}

func seedOutboxEventWithID(t testing.TB, db *sql.DB, o *Outbox, id string, event eh.Event, handlers []string, createdAt, availableAt time.Time, takenAt sql.NullTime) {
	t.Helper()

	eventBlob, err := o.codec.MarshalEvent(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	handlersBlob, err := json.Marshal(handlers)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`
		INSERT INTO outbox (id, event_type, aggregate_id, created_at, available_at, taken_at, handlers, event_blob, retry_count)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, id, event.EventType().String(), event.AggregateID().String(), createdAt, availableAt, takenAt, string(handlersBlob), string(eventBlob), 0); err != nil {
		t.Fatal(err)
	}
}

func processAllBatches(t testing.TB, o *Outbox, ctx context.Context) {
	t.Helper()

	for {
		processed, err := o.processBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if processed == 0 {
			return
		}
	}
}

func outboxIDsInDispatchOrder(t testing.TB, db *sql.DB) []string {
	t.Helper()

	rows, err := db.Query(`SELECT id FROM outbox ORDER BY created_at ASC, id ASC`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}

func aggregateIDsForDistinctShards(t testing.TB, count, shards int) []uuid.UUID {
	t.Helper()

	seen := map[uint64]bool{}
	var ids []uuid.UUID
	for attempts := 0; len(ids) < count && attempts < 10000; attempts++ {
		id := uuid.New()
		shard := hashPartition(id.String(), shards)
		if seen[shard] {
			continue
		}
		seen[shard] = true
		ids = append(ids, id)
	}
	if len(ids) != count {
		t.Fatalf("generated %d distinct shard aggregate IDs, want %d", len(ids), count)
	}
	return ids
}

func correlationIDsForDistinctShards(t testing.TB, count, shards int) []string {
	t.Helper()

	seen := map[uint64]bool{}
	var ids []string
	for i := 0; len(ids) < count && i < 10000; i++ {
		id := fmt.Sprintf("correlation-%d", i)
		shard := hashPartition(id, shards)
		if seen[shard] {
			continue
		}
		seen[shard] = true
		ids = append(ids, id)
	}
	if len(ids) != count {
		t.Fatalf("generated %d distinct shard correlation IDs, want %d", len(ids), count)
	}
	return ids
}

type orderingHandler struct {
	Type string
	mu   sync.Mutex
	seen []string
}

func newOrderingHandler(handlerType string) *orderingHandler {
	return &orderingHandler{Type: handlerType}
}

func (h *orderingHandler) HandlerType() eh.EventHandlerType {
	return eh.EventHandlerType(h.Type)
}

func (h *orderingHandler) HandleEvent(_ context.Context, event eh.Event) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.seen = append(h.seen, eventContent(event))
	return nil
}

func (h *orderingHandler) Contents() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	return append([]string(nil), h.seen...)
}

type concurrencyHandler struct {
	Type     string
	delay    time.Duration
	inFlight atomic.Int64
	max      atomic.Int64
}

func newConcurrencyHandler(handlerType string, delay time.Duration) *concurrencyHandler {
	return &concurrencyHandler{Type: handlerType, delay: delay}
}

func (h *concurrencyHandler) HandlerType() eh.EventHandlerType {
	return eh.EventHandlerType(h.Type)
}

func (h *concurrencyHandler) HandleEvent(context.Context, eh.Event) error {
	current := h.inFlight.Add(1)
	updateMax(&h.max, current)
	if h.delay > 0 {
		time.Sleep(h.delay)
	}
	h.inFlight.Add(-1)
	return nil
}

func (h *concurrencyHandler) MaxInFlight() int64 {
	return h.max.Load()
}

type blockingHandler struct {
	Type     string
	entered  chan<- string
	release  <-chan struct{}
	inFlight atomic.Int64
	max      atomic.Int64
}

func newBlockingHandler(handlerType string, entered chan<- string, release <-chan struct{}) *blockingHandler {
	return &blockingHandler{Type: handlerType, entered: entered, release: release}
}

func (h *blockingHandler) HandlerType() eh.EventHandlerType {
	return eh.EventHandlerType(h.Type)
}

func (h *blockingHandler) HandleEvent(_ context.Context, event eh.Event) error {
	current := h.inFlight.Add(1)
	updateMax(&h.max, current)
	h.entered <- eventContent(event)
	<-h.release
	h.inFlight.Add(-1)
	return nil
}

func (h *blockingHandler) MaxInFlight() int64 {
	return h.max.Load()
}

func updateMax(max *atomic.Int64, value int64) {
	for {
		current := max.Load()
		if value <= current {
			return
		}
		if max.CompareAndSwap(current, value) {
			return
		}
	}
}

func waitForEntries(t testing.TB, entered <-chan string, count int) {
	t.Helper()

	deadline := time.After(2 * time.Second)
	for i := 0; i < count; i++ {
		select {
		case <-entered:
		case <-deadline:
			t.Fatalf("timed out waiting for %d parallel entries, got %d", count, i)
		}
	}
}

func eventContent(event eh.Event) string {
	if data, ok := event.Data().(*mocks.EventData); ok {
		return data.Content
	}
	return fmt.Sprint(event.Data())
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func outboxRowCount(t testing.TB, db *sql.DB) int {
	t.Helper()

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM outbox`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func deadLetterRowCount(t testing.TB, db *sql.DB) int {
	t.Helper()

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM dead_letters`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func drainOutboxErrors(o *Outbox) []error {
	var errs []error
	for {
		select {
		case err := <-o.Errors():
			if err != nil {
				errs = append(errs, err)
			}
		default:
			return errs
		}
	}
}

func containsErrorSubstring(errs []error, substr string) bool {
	for _, err := range errs {
		if err != nil && strings.Contains(err.Error(), substr) {
			return true
		}
	}
	return false
}

func exportedDeadLetterCount(t testing.TB, db *sql.DB) int {
	t.Helper()

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM dead_letters WHERE exported_at IS NOT NULL`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

type fatalOutboxError struct {
	err error
}

func (e fatalOutboxError) Error() string {
	return e.err.Error()
}

func (e fatalOutboxError) Unwrap() error {
	return e.err
}

func (e fatalOutboxError) OutboxSeverity() ErrorSeverity {
	return SeverityFatal
}

type recordingDeadLetterExporter struct {
	mu      sync.Mutex
	records []dl.Record
	err     error
}

func (e *recordingDeadLetterExporter) ExportDeadLetter(_ context.Context, record dl.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.records = append(e.records, record)
	return e.err
}

func (e *recordingDeadLetterExporter) Records() []dl.Record {
	e.mu.Lock()
	defer e.mu.Unlock()

	return append([]dl.Record(nil), e.records...)
}

func assertDeadLetterHandlers(t testing.TB, db *sql.DB, want []string) {
	t.Helper()

	rows, err := db.Query(`SELECT handler_type FROM dead_letters WHERE source = 'outbox' ORDER BY handler_type ASC`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var got []string
	for rows.Next() {
		var handlerType string
		if err := rows.Scan(&handlerType); err != nil {
			t.Fatal(err)
		}
		got = append(got, handlerType)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	want = append([]string(nil), want...)
	sortStrings(want)
	if !slicesEqual(got, want) {
		t.Fatalf("dead letter handlers = %v, want %v", got, want)
	}
}

func assertOutboxHandlers(t testing.TB, db *sql.DB, outboxID string, want []string) {
	t.Helper()

	var handlersBlob string
	if err := db.QueryRow(`SELECT handlers FROM outbox WHERE id = ?`, outboxID).Scan(&handlersBlob); err != nil {
		t.Fatal(err)
	}
	var got []string
	if err := json.Unmarshal([]byte(handlersBlob), &got); err != nil {
		t.Fatal(err)
	}
	if !slicesEqual(got, want) {
		t.Fatalf("outbox handlers = %v, want %v", got, want)
	}
}

type deadLetterRow struct {
	OutboxID          string
	RemainingHandlers string
}

func deadLetterRecordByHandler(t testing.TB, db *sql.DB, handlerType string) deadLetterRow {
	t.Helper()

	var record deadLetterRow
	if err := db.QueryRow(`
		SELECT outbox_id, remaining_handlers
		FROM dead_letters
		WHERE source = 'outbox' AND handler_type = ?
	`, handlerType).Scan(&record.OutboxID, &record.RemainingHandlers); err != nil {
		t.Fatal(err)
	}
	return record
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}
