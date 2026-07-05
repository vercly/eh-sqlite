package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
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

func TestOutboxIntegration(t *testing.T) {
	// Shorter sweeps for testing
	restoreSweepInterval := setPeriodicSweepInterval(t, 2*time.Second)
	defer restoreSweepInterval()
	restoreSweepAge := setPeriodicSweepAge(t, 2*time.Second)
	defer restoreSweepAge()

	db := newTestDB(t)

	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}

	o.Start()

	outbox.AcceptanceTest(t, o, context.Background(), "none")

	if err := o.Close(); err != nil {
		t.Error("there should be no error:", err)
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

	o.Start()
	o.Start()

	if err := o.HandleEvent(WithDelay(ctx, time.Hour), newTestEvent("after-start")); err != nil {
		t.Fatal(err)
	}
	if got := outboxRowCount(t, db); got != 1 {
		t.Fatalf("outbox rows after Start = %d, want 1", got)
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
	o.started.Store(true)

	handler := mocks.NewEventHandler("retry_handler")
	handler.Err = errors.New("temporary failure")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

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
	o.started.Store(true)

	handler := mocks.NewEventHandler("terminal_handler")
	handler.Err = errors.New("permanent failure")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

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
