package drain

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	eh "github.com/vercly/eventhorizon"
	ehcodec "github.com/vercly/eventhorizon/codec/json"
	"github.com/vercly/eventhorizon/uuid"
)

func TestDrainV2SeparatesPublicationsAndGroupsSiblingDeliveries(t *testing.T) {
	db := openV2DrainDB(t)
	blob := v2EventBlob(t, []byte(`same-event`))
	seedV2Publication(t, db, "p-1", blob, "d-1", "d-2")
	seedV2Publication(t, db, "p-2", blob, "d-3")
	pub := &fakePublisher{}
	res, err := DrainOutbox(context.Background(), db, pub, Options{Apply: true, Actor: "ops", IDs: []string{"p-1", "p-2"}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Drained != 2 || pub.calls != 2 {
		t.Fatalf("want one publish per publication: %+v calls=%d", res, pub.calls)
	}
	assertV2DrainEmpty(t, db)
}

func TestDrainV2ConfirmFailurePreservesPublicationAndEveryDelivery(t *testing.T) {
	db := openV2DrainDB(t)
	seedV2Publication(t, db, "p-1", v2EventBlob(t, []byte(`body`)), "d-1", "d-2")
	pub := &fakePublisher{err: os.ErrDeadlineExceeded}
	res, err := DrainOutbox(context.Background(), db, pub, Options{Apply: true, Actor: "ops", IDs: []string{"p-1"}})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Aborted || res.Drained != 0 || pub.calls != 1 {
		t.Fatalf("result=%+v calls=%d", res, pub.calls)
	}
	var pubs, deliveries, audits int
	if err := db.QueryRow(`SELECT COUNT(*) FROM outbox_publications`).Scan(&pubs); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM outbox_deliveries`).Scan(&deliveries); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM outbox_drain_audit`).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if pubs != 1 || deliveries != 2 || audits != 0 {
		t.Fatalf("pubs=%d deliveries=%d audits=%d", pubs, deliveries, audits)
	}
}

func openV2DrainDB(t *testing.T) *sql.DB {
	t.Helper()
	f, err := os.CreateTemp("", "drain-v2-*.db")
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	db, err := sql.Open("sqlite3", f.Name())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close(); _ = os.Remove(f.Name()) })
	if _, err := db.Exec(`CREATE TABLE outbox_publications (publication_id TEXT PRIMARY KEY,event_type TEXT NOT NULL,aggregate_id TEXT NOT NULL,event_blob TEXT NOT NULL,created_at TIMESTAMP NOT NULL); CREATE TABLE outbox_deliveries (seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE,publication_id TEXT NOT NULL,available_at TIMESTAMP NOT NULL);`); err != nil {
		t.Fatal(err)
	}
	return db
}
func seedV2Publication(t *testing.T, db *sql.DB, publicationID, blob string, deliveryIDs ...string) {
	t.Helper()
	now := time.Now().UTC()
	if _, err := db.Exec(`INSERT INTO outbox_publications VALUES (?, 'legacy.event','aggregate',?,?)`, publicationID, blob, now); err != nil {
		t.Fatal(err)
	}
	for _, id := range deliveryIDs {
		if _, err := db.Exec(`INSERT INTO outbox_deliveries (id,publication_id,available_at) VALUES (?,?,?)`, id, publicationID, now); err != nil {
			t.Fatal(err)
		}
	}
}
func assertV2DrainEmpty(t *testing.T, db *sql.DB) {
	t.Helper()
	var p, d, a int
	_ = db.QueryRow(`SELECT COUNT(*) FROM outbox_publications`).Scan(&p)
	_ = db.QueryRow(`SELECT COUNT(*) FROM outbox_deliveries`).Scan(&d)
	_ = db.QueryRow(`SELECT COUNT(*) FROM outbox_drain_audit`).Scan(&a)
	if p != 0 || d != 0 || a != 2 {
		t.Fatalf("pub=%d del=%d audit=%d", p, d, a)
	}
}
func v2EventBlob(t *testing.T, body []byte) string {
	t.Helper()
	event := eh.NewEvent("legacy.event", nil, time.Now(), eh.WithMetadata(map[string]any{metaBodyBase64: "eyJ4IjoxfQ==", metaRoutingKey: "route"}), eh.ForAggregate("aggregate", uuid.New(), 1))
	b, err := (&ehcodec.EventCodec{}).MarshalEvent(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	_ = body
	return string(b)
}
