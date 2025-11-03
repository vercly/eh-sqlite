package sqlite

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/eventstore"
	"github.com/looplab/eventhorizon/mocks"
	"github.com/looplab/eventhorizon/uuid"
	_ "github.com/mattn/go-sqlite3"
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

	db, err := sql.Open("sqlite3", f.Name())
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		db.Close()
		os.Remove(f.Name())
	})

	return db
}
