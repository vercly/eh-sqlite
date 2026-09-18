package replay

import (
	"context"
	"database/sql"
	"testing"
	"time"

	eh "github.com/vercly/eventhorizon"
	ehcodec "github.com/vercly/eventhorizon/codec/json"
	"github.com/vercly/eventhorizon/uuid"
)

func TestReplayV2TerminalRestoresRecipientPartitionAndProvenanceIdempotently(t *testing.T) {
	db := openDB(t)
	enableV2Replay(t, db)
	if _, err := db.Exec(`ALTER TABLE dead_letters ADD COLUMN publication_id TEXT; ALTER TABLE dead_letters ADD COLUMN legacy_outbox_id TEXT`); err != nil {
		t.Fatal(err)
	}
	agg := uuid.New()
	blob := replayEventBlob(t, agg)
	seedDLQ(t, db, "dlq-1", SourceOutbox, "evt", agg.String(), "legacy-handler", "v1-outbox", blob)
	if _, err := db.Exec(`UPDATE dead_letters SET publication_id='old-v2-publication', legacy_outbox_id='v1-parent' WHERE id='dlq-1'`); err != nil {
		t.Fatal(err)
	}
	res, err := ReplayDeadLetters(context.Background(), db, Options{Apply: true, Actor: "ops", IDs: []string{"dlq-1"}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Replayed != 1 {
		t.Fatalf("%+v", res)
	}
	var handler, partition, origin, ref, legacy string
	if err := db.QueryRow(`SELECT d.handler_type,p.partition_key,p.origin,p.origin_ref,d.legacy_outbox_id FROM outbox_deliveries d JOIN outbox_publications p USING(publication_id)`).Scan(&handler, &partition, &origin, &ref, &legacy); err != nil {
		t.Fatal(err)
	}
	if handler != "legacy-handler" || partition != agg.String() || origin != "replay" || ref != "dlq-1" || legacy != "v1-parent" {
		t.Fatalf("handler=%q partition=%q origin=%q ref=%q legacy=%q", handler, partition, origin, ref, legacy)
	}
	res, err = ReplayDeadLetters(context.Background(), db, Options{Apply: true, Actor: "ops", IDs: []string{"dlq-1"}})
	if err != nil {
		t.Fatal(err)
	}
	if res.AlreadyReplayed != 1 {
		t.Fatalf("%+v", res)
	}
	var count int
	_ = db.QueryRow(`SELECT COUNT(*) FROM outbox_deliveries`).Scan(&count)
	if count != 1 {
		t.Fatalf("deliveries=%d", count)
	}
}

func TestReplayV2NoMatchCreatesOnlyRematchSentinel(t *testing.T) {
	db := openDB(t)
	enableV2Replay(t, db)
	blob := replayEventBlob(t, uuid.Nil)
	seedDLQ(t, db, "dlq-no-match", SourceOutbox, "evt", "agg", HandlerNoMatch, "", blob)
	res, err := ReplayDeadLetters(context.Background(), db, Options{Apply: true, Actor: "ops", IDs: []string{"dlq-no-match"}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Replayed != 1 {
		t.Fatalf("%+v", res)
	}
	var handler, key, unresolved interface{}
	if err := db.QueryRow(`SELECT handler_type,dispatch_key,unresolved_at FROM outbox_deliveries`).Scan(&handler, &key, &unresolved); err != nil {
		t.Fatal(err)
	}
	if handler != nil || key != nil || unresolved != nil {
		t.Fatalf("want NULL sentinel: %#v %#v %#v", handler, key, unresolved)
	}
}

func enableV2Replay(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`CREATE TABLE eh_sqlite_migrations (component TEXT,name TEXT,applied_at TIMESTAMP,PRIMARY KEY(component,name)); INSERT INTO eh_sqlite_migrations VALUES ('outbox','v2_deliveries','2026-01-01 00:00:00+00:00'); CREATE TABLE outbox_publications (publication_id TEXT PRIMARY KEY,event_type TEXT NOT NULL,aggregate_id TEXT NOT NULL,partition_key TEXT NOT NULL,event_blob TEXT NOT NULL,created_at TIMESTAMP NOT NULL,origin TEXT NOT NULL,origin_ref TEXT); CREATE TABLE outbox_deliveries (seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE,publication_id TEXT NOT NULL,handler_type TEXT,dispatch_key TEXT,dispatch_config TEXT,event_type TEXT NOT NULL,aggregate_id TEXT NOT NULL,created_at TIMESTAMP NOT NULL,available_at TIMESTAMP NOT NULL,taken_at TIMESTAMP,retry_count INTEGER NOT NULL,unresolved_at TIMESTAMP,legacy_outbox_id TEXT);`); err != nil {
		t.Fatal(err)
	}
}
func replayEventBlob(t *testing.T, aggregate uuid.UUID) string {
	t.Helper()
	e := eh.NewEvent("evt", nil, time.Now(), eh.WithMetadata(map[string]any{"correlation_id": "corr"}), eh.ForAggregate("agg", aggregate, 1))
	b, err := (&ehcodec.EventCodec{}).MarshalEvent(context.Background(), e)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
