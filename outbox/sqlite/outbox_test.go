package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

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
		o.watchCh <- &outboxDoc{Ctx: context.Background()}
	}

	o.notify(&outboxDoc{Ctx: context.Background()})

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
