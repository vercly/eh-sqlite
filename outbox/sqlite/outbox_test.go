package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

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
	PeriodicSweepInterval = 2 * time.Second
	PeriodicSweepAge = 2 * time.Second

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

func TestOutboxProcessesStaleTakenAtAfterRestart(t *testing.T) {
	restoreSweepAge := setPeriodicSweepAge(t, 25*time.Millisecond)
	defer restoreSweepAge()

	db := newTestDB(t)
	ctx := context.Background()

	firstOutbox, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	firstHandler := mocks.NewEventHandler("stale_handler")
	if err := firstOutbox.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, firstHandler); err != nil {
		t.Fatal(err)
	}
	if err := firstOutbox.HandleEvent(ctx, newTestEvent("stale")); err != nil {
		t.Fatal(err)
	}
	if err := firstOutbox.Close(); err != nil {
		t.Fatal(err)
	}

	staleTakenAt := time.Now().Add(-2 * PeriodicSweepAge)
	if _, err := db.Exec(`UPDATE outbox SET taken_at = ?`, staleTakenAt); err != nil {
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
	firstHandler := mocks.NewEventHandler("fresh_handler")
	if err := firstOutbox.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, firstHandler); err != nil {
		t.Fatal(err)
	}
	if err := firstOutbox.HandleEvent(ctx, newTestEvent("fresh")); err != nil {
		t.Fatal(err)
	}
	if err := firstOutbox.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`UPDATE outbox SET taken_at = ?`, time.Now()); err != nil {
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

	const events = 100
	var wg sync.WaitGroup
	errCh := make(chan error, events)
	for i := range events {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := o.HandleEvent(ctx, newTestEvent(fmt.Sprintf("event-%d", i))); err != nil {
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

func outboxRowCount(t testing.TB, db *sql.DB) int {
	t.Helper()

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM outbox`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
