package sqlite

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	eh "github.com/vercly/eventhorizon"
	"github.com/vercly/eventhorizon/mocks"
	"github.com/vercly/eventhorizon/uuid"
)

const claimTestEventType eh.EventType = "OutboxClaimTestEvent"

type claimTestEventData struct {
	Content string `json:"content"`
}

func init() {
	eh.RegisterEventData(claimTestEventType, func() eh.EventData { return &claimTestEventData{} })
}

func sharedDBPath(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp("", "outbox-claim-*.db")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	t.Cleanup(func() { os.Remove(f.Name()) })
	return f.Name()
}

func openSharedDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", addTimestampParams(path))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// insertOutboxRow inserts a row directly so tests can control taken_at precisely.
func insertOutboxRow(t *testing.T, db *sql.DB, codec eh.EventCodec, takenAt sql.NullTime) string {
	t.Helper()
	id := uuid.New()
	event := eh.NewEventForAggregate(claimTestEventType, &claimTestEventData{Content: "x"},
		time.Now(), mocks.AggregateType, uuid.New(), 1)
	blob, err := codec.MarshalEvent(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO outbox (id, event_type, aggregate_id, created_at, taken_at, handlers, event_blob, retry_count) VALUES (?, ?, ?, ?, ?, ?, ?, 0)`,
		id.String(), string(claimTestEventType), event.AggregateID().String(), time.Now(), takenAt, `["h"]`, string(blob),
	); err != nil {
		t.Fatal(err)
	}
	return id.String()
}

// TestClaimNoDoubleClaim is the core Phase 2 assertion: two Outbox instances on
// separate connections to the same database never claim the same row.
func TestClaimNoDoubleClaim(t *testing.T) {
	restore := PeriodicSweepAge
	PeriodicSweepAge = 30 * time.Second
	defer func() { PeriodicSweepAge = restore }()

	path := sharedDBPath(t)
	db1 := openSharedDB(t, path)
	db2 := openSharedDB(t, path)

	o1, err := NewOutbox(db1)
	if err != nil {
		t.Fatal(err)
	}
	o2, err := NewOutbox(db2)
	if err != nil {
		t.Fatal(err)
	}

	const n = 40
	for i := 0; i < n; i++ {
		insertOutboxRow(t, db1, o1.codec, sql.NullTime{}) // taken_at NULL => claimable
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	claimed := map[string]int{}
	errs := make(chan error, 2)

	claim := func(o *Outbox) {
		defer wg.Done()
		docs, err := o.fetchAndLockEvents(context.Background())
		if err != nil {
			errs <- err
			return
		}
		mu.Lock()
		for _, d := range docs {
			claimed[d.ID.String()]++
		}
		mu.Unlock()
	}

	wg.Add(2)
	go claim(o1)
	go claim(o2)
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("claim error: %v", err)
	}

	for id, c := range claimed {
		if c > 1 {
			t.Errorf("row %s was claimed %d times (expected at most 1)", id, c)
		}
	}
	if len(claimed) != n {
		t.Fatalf("expected %d distinct claimed rows, got %d", n, len(claimed))
	}
}

// TestClaimSkipsFreshTakenRow verifies a row taken recently is not re-claimed.
func TestClaimSkipsFreshTakenRow(t *testing.T) {
	restore := PeriodicSweepAge
	PeriodicSweepAge = 30 * time.Second
	defer func() { PeriodicSweepAge = restore }()

	db := openSharedDB(t, sharedDBPath(t))
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}

	insertOutboxRow(t, db, o.codec, sql.NullTime{Time: time.Now(), Valid: true})

	docs, err := o.fetchAndLockEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 0 {
		t.Fatalf("a freshly-taken row must not be claimed, got %d", len(docs))
	}
}

// TestClaimReclaimsStaleRow verifies a row taken longer ago than PeriodicSweepAge
// becomes claimable again (crash recovery of an in-flight row).
func TestClaimReclaimsStaleRow(t *testing.T) {
	restore := PeriodicSweepAge
	PeriodicSweepAge = 5 * time.Second
	defer func() { PeriodicSweepAge = restore }()

	db := openSharedDB(t, sharedDBPath(t))
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}

	staleID := insertOutboxRow(t, db, o.codec,
		sql.NullTime{Time: time.Now().Add(-1 * time.Minute), Valid: true})

	docs, err := o.fetchAndLockEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 1 {
		t.Fatalf("a stale row must be reclaimable, got %d", len(docs))
	}
	if docs[0].ID.String() != staleID {
		t.Fatalf("expected to reclaim %s, got %s", staleID, docs[0].ID.String())
	}
}
