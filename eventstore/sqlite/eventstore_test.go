package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	outboxsqlite "github.com/vercly/eh-sqlite/outbox/sqlite"
	eh "github.com/vercly/eventhorizon"
	"github.com/vercly/eventhorizon/eventstore"
	"github.com/vercly/eventhorizon/mocks"
	"github.com/vercly/eventhorizon/uuid"
)

// SnapshotData is a mock snapshot data type for testing.
type SnapshotData struct {
	Content string `json:"content"`
}

func init() {
	eh.RegisterEventData(mocks.EventOtherType, func() eh.EventData { return &mocks.EventData{} })
}

func TestEventStore(t *testing.T) {
	store, err := newTestEventStore(t)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}
	if store == nil {
		t.Fatal("there should be a store")
	}
	defer store.Close()

	ctx := context.Background()

	eventstore.AcceptanceTest(t, store, ctx)
	eventstore.SnapshotAcceptanceTest(t, store, ctx)
}

func TestWithTableNames(t *testing.T) {
	db := newTestDB(t)
	eventsTable := "foo_events"
	streamsTable := "bar_streams"
	snapshotsTable := "baz_snapshots"

	store, err := NewEventStore(db,
		WithTableNames(eventsTable, streamsTable, snapshotsTable),
	)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}
	if store == nil {
		t.Fatal("there should be a store")
	}
	defer store.Close()

	if store.eventsTable != eventsTable {
		t.Fatal("events table should use custom table name")
	}
	if store.streamsTable != streamsTable {
		t.Fatal("streams table should use custom table name")
	}
	if store.snapshotsTable != snapshotsTable {
		t.Fatal("snapshots table should use custom table name")
	}
}

func TestWithEventHandler(t *testing.T) {
	store, err := newTestEventStore(t)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}
	if store == nil {
		t.Fatal("there should be a store")
	}
	defer store.Close()

	h := &mocks.EventBus{}
	store.eventHandlerAfterSave = h

	ctx := context.Background()

	// The event handler should be called.
	id1 := uuid.New()
	event1 := eh.NewEventForAggregate(mocks.EventType, &mocks.EventData{Content: "event1"},
		time.Now(), mocks.AggregateType, id1, 1)

	err = store.Save(ctx, []eh.Event{event1}, 0)
	if err != nil {
		t.Error("there should be no error:", err)
	}

	// The saved events should be ok.
	events, err := store.Load(ctx, id1)
	if err != nil {
		t.Error("there should be no error:", err)
	}

	expected := []eh.Event{event1}

	// The stored events should be ok.
	if len(events) != len(expected) {
		t.Errorf("incorrect number of loaded events: %d", len(events))
	}

	// The handled events should be ok.
	if len(h.Events) != len(expected) {
		t.Errorf("incorrect number of handled events: %d", len(h.Events))
	}
}

func TestEventStoreCloseDoesNotCloseSharedDB(t *testing.T) {
	db := newTestDB(t)
	store, err := NewEventStore(db)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	if err := db.Ping(); err != nil {
		t.Fatalf("shared db should remain open after event store close: %v", err)
	}
}

func TestEventStoreNotifiesInTXOutboxAfterCommit(t *testing.T) {
	restoreSweepInterval := setOutboxSweepInterval(t, time.Hour)
	defer restoreSweepInterval()

	db := newTestDB(t)
	o, err := outboxsqlite.NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := mocks.NewEventHandler("after_commit_handler")
	if err := o.AddHandler(context.Background(), eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}
	o.Start()

	store, err := NewEventStore(db, WithEventHandlerInTX(o))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	event := eh.NewEventForAggregate(mocks.EventType, &mocks.EventData{Content: "after-commit"},
		time.Now(), mocks.AggregateType, uuid.New(), 1)
	if err := store.Save(context.Background(), []eh.Event{event}, 0); err != nil {
		t.Fatal(err)
	}

	if !handler.Wait(100 * time.Millisecond) {
		t.Fatal("in-TX outbox event was not dispatched quickly after commit")
	}
}

func TestEventStoreRollsBackInTXNoMatchDeadLetter(t *testing.T) {
	db := newTestDB(t)
	o, err := outboxsqlite.NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	o.Start()

	store, err := NewEventStore(db, WithEventHandlerInTX(failingAfterOutboxHandler{outbox: o}))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	event := eh.NewEventForAggregate(mocks.EventType, &mocks.EventData{Content: "rollback-no-match"},
		time.Now(), mocks.AggregateType, uuid.New(), 1)
	err = store.Save(context.Background(), []eh.Event{event}, 0)
	if err == nil {
		t.Fatal("expected save error")
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM dead_letters`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("dead letter rows after rollback = %d, want 0", count)
	}
}

func newTestEventStore(t testing.TB) (*EventStore, error) {
	t.Helper()

	return NewEventStore(newTestDB(t))
}

func newTestDB(t testing.TB) *sql.DB {
	t.Helper()

	f, err := os.CreateTemp("", "eventstore-*.db")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

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

type failingAfterOutboxHandler struct {
	outbox *outboxsqlite.Outbox
}

func (h failingAfterOutboxHandler) HandlerType() eh.EventHandlerType {
	return "failing_after_outbox"
}

func (h failingAfterOutboxHandler) HandleEvent(ctx context.Context, event eh.Event) error {
	if err := h.outbox.HandleEvent(ctx, event); err != nil {
		return err
	}
	return errors.New("force rollback after outbox")
}

func setOutboxSweepInterval(t testing.TB, interval time.Duration) func() {
	t.Helper()

	previous := outboxsqlite.PeriodicSweepInterval
	outboxsqlite.PeriodicSweepInterval = interval
	return func() {
		outboxsqlite.PeriodicSweepInterval = previous
	}
}
