package replay

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
	"github.com/vercly/eventhorizon/mocks"
	"github.com/vercly/eventhorizon/uuid"
)

func TestReplayDeadLetterWithRealOutboxSchema(t *testing.T) {
	db := integrationDB(t)
	ctx := context.Background()
	failing, err := outboxsqlite.NewOutbox(db, outboxsqlite.WithMaxRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	fatal := mocks.NewEventHandler("replay-real-handler")
	fatal.Err = errors.New("permanent failure")
	if err := failing.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, fatal); err != nil {
		t.Fatal(err)
	}
	if err := failing.StartChecked(); err != nil {
		t.Fatal(err)
	}
	event := eh.NewEvent(mocks.EventType, &mocks.EventData{Content: "replay"}, time.Now(), eh.ForAggregate(mocks.AggregateType, uuid.New(), 1))
	if err := failing.HandleEvent(ctx, event); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool {
		var n int
		_ = db.QueryRow(`SELECT COUNT(*) FROM dead_letters WHERE source='outbox'`).Scan(&n)
		return n == 1
	})
	if err := failing.Close(); err != nil {
		t.Fatal(err)
	}
	var dlqID, blob, publicationID string
	if err := db.QueryRow(`SELECT id,blob,publication_id FROM dead_letters WHERE source='outbox'`).Scan(&dlqID, &blob, &publicationID); err != nil {
		t.Fatal(err)
	}
	if blob == "" || publicationID == "" {
		t.Fatalf("DLQ must keep self-contained payload and publication provenance: blob=%d pub=%q", len(blob), publicationID)
	}
	var pubs, deliveries int
	if err := db.QueryRow(`SELECT COUNT(*) FROM outbox_publications`).Scan(&pubs); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM outbox_deliveries`).Scan(&deliveries); err != nil {
		t.Fatal(err)
	}
	if pubs != 0 || deliveries != 0 {
		t.Fatalf("fatal finalization must GC publication: pubs=%d deliveries=%d", pubs, deliveries)
	}
	res, err := ReplayDeadLetters(ctx, db, Options{Apply: true, Actor: "ops", IDs: []string{dlqID}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Replayed != 1 {
		t.Fatalf("%+v", res)
	}
	success, err := outboxsqlite.NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	good := mocks.NewEventHandler("replay-real-handler")
	if err := success.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, good); err != nil {
		t.Fatal(err)
	}
	if err := success.StartChecked(); err != nil {
		t.Fatal(err)
	}
	if !good.Wait(2 * time.Second) {
		_ = success.Close()
		t.Fatal("replayed delivery was not handled")
	}
	if err := success.Close(); err != nil {
		t.Fatal(err)
	}
	_ = db.QueryRow(`SELECT COUNT(*) FROM outbox_publications`).Scan(&pubs)
	_ = db.QueryRow(`SELECT COUNT(*) FROM outbox_deliveries`).Scan(&deliveries)
	if pubs != 0 || deliveries != 0 {
		t.Fatalf("replayed completion must GC publication: pubs=%d deliveries=%d", pubs, deliveries)
	}
}

func integrationDB(t *testing.T) *sql.DB {
	t.Helper()
	f, err := os.CreateTemp("", "replay-integration-*.db")
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	db, err := sql.Open("sqlite3", f.Name()+"?_journal=wal&_busy_timeout=5000&_fk=1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE async_tasks (task_uuid TEXT PRIMARY KEY, command_type TEXT, command_blob TEXT, status TEXT, retry_count INTEGER, max_retries INTEGER, created_at TIMESTAMP, updated_at TIMESTAMP, locked_by TEXT, locked_at TIMESTAMP, next_retry_at TIMESTAMP, last_error TEXT)`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close(); _ = os.Remove(f.Name()) })
	return db
}
func waitFor(t *testing.T, timeout time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not reached")
}
