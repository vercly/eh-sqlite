package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	eh "github.com/vercly/eventhorizon"
	"github.com/vercly/eventhorizon/mocks"
	"github.com/vercly/eventhorizon/uuid"
)

// probeData writes through a second connection that never waits for a lock
// while it is being serialized: the write fails at once if Save already holds
// the write lock.
type probeData struct {
	probe *sql.DB
	err   *error
}

func (d probeData) MarshalJSON() ([]byte, error) {
	_, *d.err = d.probe.Exec(`INSERT INTO save_probe (n) VALUES (1)`)
	return []byte(`{}`), nil
}

func TestSaveSerializesEventsOutsideWriteLock(t *testing.T) {
	db := newTestDB(t)
	store, err := NewEventStore(db)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var path string
	if err := db.QueryRow(`SELECT file FROM pragma_database_list WHERE name = 'main'`).Scan(&path); err != nil {
		t.Fatal(err)
	}
	probe, err := sql.Open("sqlite3", path+"?_journal=wal&_busy_timeout=0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = probe.Close() })
	if _, err := probe.Exec(`CREATE TABLE save_probe (n INTEGER)`); err != nil {
		t.Fatal(err)
	}

	var probeErr error
	event := eh.NewEvent(mocks.EventType, probeData{probe: probe, err: &probeErr}, time.Now(),
		eh.ForAggregate(mocks.AggregateType, uuid.New(), 1))
	if err := store.Save(context.Background(), []eh.Event{event}, 0); err != nil {
		t.Fatal(err)
	}
	if probeErr != nil {
		t.Fatalf("write during event serialization failed (write lock already held): %v", probeErr)
	}
}

func TestLoadFromHonorsCanceledContext(t *testing.T) {
	store, err := newTestEventStore(t)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = store.LoadFrom(ctx, uuid.New(), 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("LoadFrom error = %v, want context.Canceled", err)
	}
}
