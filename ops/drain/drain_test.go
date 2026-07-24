package drain

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	eh "github.com/vercly/eventhorizon"
	ehcodec "github.com/vercly/eventhorizon/codec/json"
	"github.com/vercly/eventhorizon/uuid"
)

func TestDrainDryRunNoWrites(t *testing.T) {
	db := openDB(t)
	id := seedLegacyOutbox(t, db, []byte(`{"ok":true}`), "route.a")

	res, err := DrainOutbox(context.Background(), db, nil, Options{IDs: []string{id}})
	if err != nil {
		t.Fatal(err)
	}
	if !res.DryRun || res.Drained != 0 || len(res.Items) != 1 || res.Items[0].Status != StatusWouldDrain {
		t.Fatalf("unexpected result: %+v", res)
	}
	if outboxCount(t, db) != 1 {
		t.Fatal("dry-run deleted outbox row")
	}
	// openDB used to call EnsureSchema which created audit; dry-run path must not
	// require or create it. If present from older helpers, still no new audit rows.
	if tableExists(t, db, "outbox_drain_audit") && auditCount(t, db) != 0 {
		t.Fatal("dry-run wrote audit")
	}
}

func TestDrainDryRunDoesNotCreateAuditTable(t *testing.T) {
	// Outbox only — no audit table (old / pre-tool schema).
	f, err := os.CreateTemp("", "drain-old-*.db")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	t.Cleanup(func() { os.Remove(f.Name()) })
	db, err := sql.Open("sqlite3", f.Name()+"?_journal=wal&_busy_timeout=5000&_fk=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(`
		CREATE TABLE outbox (
			id TEXT PRIMARY KEY,
			event_type TEXT NOT NULL,
			aggregate_id TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL,
			available_at TIMESTAMP,
			taken_at TIMESTAMP,
			handlers TEXT NOT NULL,
			event_blob TEXT NOT NULL,
			retry_count INTEGER DEFAULT 0
		);
	`); err != nil {
		t.Fatal(err)
	}
	id := seedLegacyOutbox(t, db, []byte(`x`), "rk")
	beforeVer := schemaVersion(t, db)
	if tableExists(t, db, "outbox_drain_audit") {
		t.Fatal("precondition: no audit table")
	}

	res, err := DrainOutbox(context.Background(), db, nil, Options{IDs: []string{id}})
	if err != nil {
		t.Fatal(err)
	}
	if !res.DryRun || res.Items[0].Status != StatusWouldDrain {
		t.Fatalf("%+v", res)
	}
	afterVer := schemaVersion(t, db)
	if afterVer != beforeVer {
		t.Fatalf("schema_version changed on dry-run: %d -> %d", beforeVer, afterVer)
	}
	if tableExists(t, db, "outbox_drain_audit") {
		t.Fatal("dry-run must not CREATE outbox_drain_audit")
	}
	if outboxCount(t, db) != 1 {
		t.Fatal("dry-run must not delete outbox")
	}
}

func TestDrainApplyCreatesAuditTable(t *testing.T) {
	f, err := os.CreateTemp("", "drain-apply-*.db")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	t.Cleanup(func() { os.Remove(f.Name()) })
	db, err := sql.Open("sqlite3", f.Name()+"?_journal=wal&_busy_timeout=5000&_fk=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(`
		CREATE TABLE outbox (
			id TEXT PRIMARY KEY,
			event_type TEXT NOT NULL,
			aggregate_id TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL,
			available_at TIMESTAMP,
			taken_at TIMESTAMP,
			handlers TEXT NOT NULL,
			event_blob TEXT NOT NULL,
			retry_count INTEGER DEFAULT 0
		);
	`); err != nil {
		t.Fatal(err)
	}
	id := seedLegacyOutbox(t, db, []byte(`x`), "rk")
	beforeVer := schemaVersion(t, db)
	pub := &fakePublisher{}
	res, err := DrainOutbox(context.Background(), db, pub, Options{
		Apply: true, Actor: "ops", IDs: []string{id},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Drained != 1 {
		t.Fatalf("%+v", res)
	}
	if !tableExists(t, db, "outbox_drain_audit") {
		t.Fatal("apply must create audit table")
	}
	if schemaVersion(t, db) <= beforeVer {
		t.Fatal("apply should bump schema_version when creating audit table")
	}
	if auditCount(t, db) != 1 || outboxCount(t, db) != 0 {
		t.Fatal("apply must audit and delete outbox")
	}
}

func TestDrainApplyRequiresActorIDsPublisher(t *testing.T) {
	db := openDB(t)
	if _, err := DrainOutbox(context.Background(), db, &fakePublisher{}, Options{Apply: true, IDs: []string{"x"}}); err == nil {
		t.Fatal("want actor error")
	}
	if _, err := DrainOutbox(context.Background(), db, &fakePublisher{}, Options{Apply: true, Actor: "a"}); err == nil {
		t.Fatal("want ids error")
	}
	if _, err := DrainOutbox(context.Background(), db, nil, Options{Apply: true, Actor: "a", IDs: []string{"x"}}); err == nil {
		t.Fatal("want publisher error")
	}
}

func TestDrainApplyWhitespaceOnlyIDsDoesNotBroadApply(t *testing.T) {
	// Safety: IDs:[]{" "} must not pass len>0 then become broad after dedupe.
	db := openDB(t)
	_ = seedLegacyOutbox(t, db, []byte(`should-not-drain`), "rk")
	pub := &fakePublisher{}
	_, err := DrainOutbox(context.Background(), db, pub, Options{
		Apply: true, Actor: "ops", IDs: []string{" ", "\t", ""}, AllowBroad: false,
	})
	if err == nil {
		t.Fatal("want --apply requires explicit --id error")
	}
	if !strings.Contains(err.Error(), "explicit --id") {
		t.Fatalf("err = %v", err)
	}
	if pub.calls != 0 {
		t.Fatalf("must not publish (broad apply): calls=%d", pub.calls)
	}
	if outboxCount(t, db) != 1 {
		t.Fatal("outbox must remain; whitespace IDs must not open broad drain")
	}
}

func TestDrainConfirmSuccessDeletesAndAudits(t *testing.T) {
	db := openDB(t)
	body := []byte{0x00, 0x01, 0xff, '{', '}'}
	id := seedLegacyOutbox(t, db, body, "rk.one", "rk.two")
	pub := &fakePublisher{}

	res, err := DrainOutbox(context.Background(), db, pub, Options{
		Apply: true, Actor: "ops@test", IDs: []string{id},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Drained != 1 || res.Errors != 0 {
		t.Fatalf("%+v", res)
	}
	if pub.calls != 1 {
		t.Fatalf("publish calls = %d", pub.calls)
	}
	if string(pub.last.Body) != string(body) {
		t.Fatalf("body not bit-for-bit: %v vs %v", pub.last.Body, body)
	}
	if len(pub.last.RoutingKeys) != 2 {
		t.Fatalf("routing = %v", pub.last.RoutingKeys)
	}
	if outboxCount(t, db) != 0 {
		t.Fatal("outbox row should be deleted after ACK")
	}
	if auditCount(t, db) != 1 {
		t.Fatal("want audit row")
	}
}

func TestDrainPublishFailureLeavesRow(t *testing.T) {
	db := openDB(t)
	id := seedLegacyOutbox(t, db, []byte(`x`), "rk")
	pub := &fakePublisher{err: errors.New("broker nack")}

	res, err := DrainOutbox(context.Background(), db, pub, Options{
		Apply: true, Actor: "ops", IDs: []string{id},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Aborted || res.Drained != 0 || res.Errors != 1 {
		t.Fatalf("%+v", res)
	}
	if outboxCount(t, db) != 1 {
		t.Fatal("row must remain without ACK")
	}
	if auditCount(t, db) != 0 {
		t.Fatal("no audit without ACK")
	}
}

// TestDrainUnroutableLeavesRow models mandatory basic.return: publisher must
// return error so commitDrain never runs (outbox stays, no audit).
func TestDrainUnroutableLeavesRow(t *testing.T) {
	db := openDB(t)
	id := seedLegacyOutbox(t, db, []byte(`x`), "rk.unbound")
	pub := &fakePublisher{err: errors.New("mandatory publish returned (unroutable; no queue binding)")}

	res, err := DrainOutbox(context.Background(), db, pub, Options{
		Apply: true, Actor: "ops", IDs: []string{id},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Drained != 0 || res.Errors != 1 || !res.Aborted {
		t.Fatalf("%+v", res)
	}
	if outboxCount(t, db) != 1 || auditCount(t, db) != 0 {
		t.Fatal("unroutable must not delete outbox or write audit")
	}
}

func TestDrainNoACKSimulatedCrashLeavesRow(t *testing.T) {
	// Publisher returns error before confirm (no ACK) — same as crash mid-publish.
	db := openDB(t)
	id := seedLegacyOutbox(t, db, []byte(`x`), "rk")
	pub := &fakePublisher{err: errors.New("connection lost before confirm")}

	res, err := DrainOutbox(context.Background(), db, pub, Options{
		Apply: true, Actor: "ops", IDs: []string{id},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Drained != 0 || outboxCount(t, db) != 1 {
		t.Fatalf("want no drain and row kept: %+v count=%d", res, outboxCount(t, db))
	}
}

func TestDrainIdempotentAfterAudit(t *testing.T) {
	db := openDB(t)
	id := seedLegacyOutbox(t, db, []byte(`x`), "rk")
	pub := &fakePublisher{}
	opts := Options{Apply: true, Actor: "ops", IDs: []string{id}}
	if _, err := DrainOutbox(context.Background(), db, pub, opts); err != nil {
		t.Fatal(err)
	}
	// Re-insert would not happen; second drain sees audit / missing outbox.
	res, err := DrainOutbox(context.Background(), db, pub, opts)
	if err != nil {
		t.Fatal(err)
	}
	if res.Already != 1 || res.Drained != 0 || pub.calls != 1 {
		t.Fatalf("second pass: %+v calls=%d", res, pub.calls)
	}
}

func TestDrainUnsupportedNativeAbortsWithoutPublish(t *testing.T) {
	db := openDB(t)
	// Two rows: second is native (no legacy body). Preflight aborts whole batch.
	id1 := seedLegacyOutbox(t, db, []byte(`ok`), "rk")
	id2 := seedNativeOutbox(t, db)
	pub := &fakePublisher{}

	res, err := DrainOutbox(context.Background(), db, pub, Options{
		Apply: true, Actor: "ops", IDs: []string{id1, id2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Aborted || pub.calls != 0 {
		t.Fatalf("want abort without publish: %+v calls=%d", res, pub.calls)
	}
	if outboxCount(t, db) != 2 {
		t.Fatal("no rows should be deleted on preflight abort")
	}
}

func TestDrainMissingRoutingKeyAborts(t *testing.T) {
	db := openDB(t)
	id := seedLegacyOutboxNoRoute(t, db, []byte(`x`))
	res, err := DrainOutbox(context.Background(), db, &fakePublisher{}, Options{
		Apply: true, Actor: "ops", IDs: []string{id},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Aborted {
		t.Fatalf("want abort: %+v", res)
	}
}

func TestDrainMissingExplicitIDReportsError(t *testing.T) {
	db := openDB(t)
	res, err := DrainOutbox(context.Background(), db, &fakePublisher{}, Options{
		Apply: true, Actor: "ops", IDs: []string{"no-such-outbox"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Missing != 1 || res.Errors != 1 || res.Drained != 0 || res.Already != 0 {
		t.Fatalf("%+v", res)
	}
	if res.Items[0].Status != StatusSkipped || res.Items[0].Detail != "outbox id not found" {
		t.Fatalf("%+v", res.Items[0])
	}
}

func TestDrainDedupesExplicitIDs(t *testing.T) {
	db := openDB(t)
	id := seedLegacyOutbox(t, db, []byte(`once`), "rk")
	pub := &fakePublisher{}
	res, err := DrainOutbox(context.Background(), db, pub, Options{
		Apply: true, Actor: "ops", IDs: []string{id, id, id},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Drained != 1 || len(res.Items) != 1 {
		t.Fatalf("dedupe result: %+v", res)
	}
	if pub.calls != 1 {
		t.Fatalf("publish calls = %d, want 1 (no double publish before audit)", pub.calls)
	}
}

func TestDrainEnsureSchemaRejectsEmptyDB(t *testing.T) {
	f, err := os.CreateTemp("", "drain-empty-*.db")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	t.Cleanup(func() { os.Remove(f.Name()) })
	db, err := sql.Open("sqlite3", f.Name())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	err = EnsureSchema(db)
	if err == nil || !strings.Contains(err.Error(), "outbox") {
		t.Fatalf("want missing outbox error, got %v", err)
	}
}

func TestDrainRestoresWhitelistedLegacyHeaders(t *testing.T) {
	db := openDB(t)
	body := []byte(`{"payload":true}`)
	meta := map[string]any{
		metaBodyBase64: base64.StdEncoding.EncodeToString(body),
		metaRoutingKey: "rk.headers",
		metaLegacyType: "legacy.test",
		// Application headers as stored by legacyPublishMetadata.
		metaHeaderPrefix + "x-vercly-instance-id": "inst-abc",
		metaHeaderPrefix + "x-vercly-domain":      "prod",
		// Not on allow-list — must not be restored.
		metaHeaderPrefix + "authorization":             "Bearer secret-token",
		metaHeaderPrefix + "x-api-key":                 "super-secret",
		metaHeaderPrefix + "x-vercly-original-dataset": "ds-1",
		metaCorrelationID:                              "corr-1",
	}
	id := uuid.New().String()
	insertOutbox(t, db, id, "legacy.test", marshalEvent(t, "legacy.test", meta))
	pub := &fakePublisher{}

	res, err := DrainOutbox(context.Background(), db, pub, Options{
		Apply: true, Actor: "ops", IDs: []string{id},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Drained != 1 {
		t.Fatalf("%+v", res)
	}
	if pub.last.Headers["x-vercly-instance-id"] != "inst-abc" {
		t.Fatalf("instance id header = %v", pub.last.Headers["x-vercly-instance-id"])
	}
	if pub.last.Headers["x-vercly-domain"] != "prod" {
		t.Fatalf("domain header = %v", pub.last.Headers["x-vercly-domain"])
	}
	if pub.last.Headers["x-vercly-original-dataset"] != "ds-1" {
		t.Fatalf("dataset header = %v", pub.last.Headers["x-vercly-original-dataset"])
	}
	if _, ok := pub.last.Headers["authorization"]; ok {
		t.Fatal("secret authorization header must not be restored")
	}
	if _, ok := pub.last.Headers["x-api-key"]; ok {
		t.Fatal("secret x-api-key header must not be restored")
	}
	if pub.last.Headers[metaCorrelationID] != "corr-1" {
		t.Fatalf("correlation = %v", pub.last.Headers[metaCorrelationID])
	}
}

func TestDrainConstraintConflictIsTyped(t *testing.T) {
	// isSQLiteConstraintConflict must recognize primary-key conflicts without
	// string matching on the error text.
	db := openDB(t)
	if err := EnsureSchema(db); err != nil {
		t.Fatal(err)
	}
	id := seedLegacyOutbox(t, db, []byte(`x`), "rk")
	// Pre-seed audit so the second insert hits PRIMARY KEY constraint.
	if _, err := db.Exec(`
		INSERT INTO outbox_drain_audit (outbox_id, drained_at, drained_by, event_type, routing_keys)
		VALUES (?, ?, 'other', 't', '[]')
	`, id, time.Now()); err != nil {
		t.Fatal(err)
	}
	// Direct commitDrain after a successful "publish" should tolerate conflict.
	msg := Message{OutboxID: id, EventType: "t", RoutingKeys: []string{"rk"}, Body: []byte(`x`)}
	if err := commitDrain(context.Background(), db, "ops", msg); err != nil {
		t.Fatalf("constraint conflict should be tolerated: %v", err)
	}
	if outboxCount(t, db) != 0 {
		t.Fatal("outbox row should still be deleted")
	}
}

func TestIsSQLiteConstraintConflict(t *testing.T) {
	if isSQLiteConstraintConflict(errors.New("UNIQUE constraint failed: foo")) {
		t.Fatal("plain text unique error must not match (no typed sqlite3.Error)")
	}
	if isSQLiteConstraintConflict(fmt.Errorf("wrap: %w", errors.New("UNIQUE constraint failed"))) {
		t.Fatal("wrapped text must not match")
	}
}

func TestIsSQLiteConstraintConflictIgnoresNonUniqueConstraints(t *testing.T) {
	db := openDB(t)
	// Table with CHECK / NOT NULL so we can produce typed non-unique constraint errors.
	if _, err := db.Exec(`
		CREATE TABLE constraint_probe (
			id TEXT PRIMARY KEY,
			n INTEGER NOT NULL CHECK (n > 0)
		);
	`); err != nil {
		t.Fatal(err)
	}

	// NOT NULL
	_, err := db.Exec(`INSERT INTO constraint_probe (id, n) VALUES ('a', NULL)`)
	if err == nil {
		t.Fatal("want NOT NULL error")
	}
	if isSQLiteConstraintConflict(err) {
		t.Fatalf("NOT NULL must not be treated as unique/PK conflict: %v", err)
	}

	// CHECK
	_, err = db.Exec(`INSERT INTO constraint_probe (id, n) VALUES ('b', 0)`)
	if err == nil {
		t.Fatal("want CHECK error")
	}
	if isSQLiteConstraintConflict(err) {
		t.Fatalf("CHECK must not be treated as unique/PK conflict: %v", err)
	}

	// Real PK conflict still matches.
	if _, err := db.Exec(`INSERT INTO constraint_probe (id, n) VALUES ('c', 1)`); err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO constraint_probe (id, n) VALUES ('c', 2)`)
	if err == nil {
		t.Fatal("want PRIMARY KEY conflict")
	}
	if !isSQLiteConstraintConflict(err) {
		t.Fatalf("PRIMARY KEY must match: %v", err)
	}
}

func TestCommitDrainNonUniqueConstraintDoesNotDeleteOutbox(t *testing.T) {
	// If audit insert fails for a non-unique constraint reason, outbox must stay.
	db := openDB(t)
	if err := EnsureSchema(db); err != nil {
		t.Fatal(err)
	}
	id := seedLegacyOutbox(t, db, []byte(`x`), "rk")
	// Break audit table so INSERT fails with NOT NULL (drained_by NOT NULL).
	if _, err := db.Exec(`DROP TABLE outbox_drain_audit`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		CREATE TABLE outbox_drain_audit (
			outbox_id TEXT PRIMARY KEY,
			drained_at TIMESTAMP NOT NULL,
			drained_by TEXT NOT NULL,
			event_type TEXT,
			routing_keys TEXT,
			CHECK (length(drained_by) > 0)
		);
	`); err != nil {
		t.Fatal(err)
	}
	// Empty actor would violate NOT NULL/CHECK if we passed ""; commitDrain uses actor as drained_by.
	// Force CHECK failure by inserting via a path that still hits commitDrain with invalid audit —
	// simpler: call commitDrain then simulate by replacing insert semantics isn't possible;
	// instead verify commitDrain returns error when drained_by is empty string if CHECK forbids it.
	msg := Message{OutboxID: id, EventType: "t", RoutingKeys: []string{"rk"}, Body: []byte(`x`)}
	// drained_by="" violates CHECK (length > 0) and must abort before DELETE.
	if err := commitDrain(context.Background(), db, "", msg); err == nil {
		t.Fatal("want commitDrain error on CHECK/NOT NULL audit failure")
	}
	if outboxCount(t, db) != 1 {
		t.Fatal("outbox row must remain when audit insert fails for non-unique constraint")
	}
}

func TestDrainAckButAuditFailureReportsAtLeastOnce(t *testing.T) {
	// After ACK, commitDrain fails if outbox table is broken — simulate by
	// deleting the audit table mid-flight via a publisher that drops the schema.
	db := openDB(t)
	id := seedLegacyOutbox(t, db, []byte(`x`), "rk")
	pub := &fakePublisher{after: func() {
		_, _ = db.Exec(`DROP TABLE outbox_drain_audit`)
	}}
	res, err := DrainOutbox(context.Background(), db, pub, Options{
		Apply: true, Actor: "ops", IDs: []string{id},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Aborted || res.Errors != 1 {
		t.Fatalf("want at-least-once error: %+v", res)
	}
	// Message was ACKed; outbox may still exist if DELETE never ran.
	if pub.calls != 1 {
		t.Fatalf("calls=%d", pub.calls)
	}
}

type fakePublisher struct {
	err   error
	calls int32
	last  Message
	after func()
}

func (f *fakePublisher) PublishConfirmed(ctx context.Context, msg Message) error {
	atomic.AddInt32(&f.calls, 1)
	f.last = msg
	if f.after != nil {
		f.after()
	}
	return f.err
}

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	f, err := os.CreateTemp("", "drain-*.db")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	db, err := sql.Open("sqlite3", f.Name()+"?_journal=wal&_busy_timeout=5000&_fk=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Close()
		os.Remove(f.Name())
	})
	if _, err := db.Exec(`
		CREATE TABLE outbox (
			id TEXT PRIMARY KEY,
			event_type TEXT NOT NULL,
			aggregate_id TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL,
			available_at TIMESTAMP,
			taken_at TIMESTAMP,
			handlers TEXT NOT NULL,
			event_blob TEXT NOT NULL,
			retry_count INTEGER DEFAULT 0
		);
	`); err != nil {
		t.Fatal(err)
	}
	// Do not auto-migrate audit table: dry-run tests rely on optional absence.
	// Apply tests call MigrateSchema via DrainOutbox --apply.
	return db
}

func schemaVersion(t *testing.T, db *sql.DB) int {
	t.Helper()
	var v int
	if err := db.QueryRow(`PRAGMA schema_version`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func tableExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var n int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, name,
	).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n > 0
}

func seedLegacyOutbox(t *testing.T, db *sql.DB, body []byte, routes ...string) string {
	t.Helper()
	id := uuid.New().String()
	meta := map[string]any{
		metaBodyBase64: base64.StdEncoding.EncodeToString(body),
		metaLegacyType: "legacy.test",
	}
	if len(routes) > 0 {
		meta[metaRoutingKey] = routes[0]
	}
	if len(routes) > 1 {
		meta[metaRoutingKeys] = routes
	}
	blob := marshalEvent(t, "legacy.test", meta)
	insertOutbox(t, db, id, "legacy.test", blob)
	return id
}

func seedLegacyOutboxNoRoute(t *testing.T, db *sql.DB, body []byte) string {
	t.Helper()
	id := uuid.New().String()
	meta := map[string]any{
		metaBodyBase64: base64.StdEncoding.EncodeToString(body),
	}
	blob := marshalEvent(t, "legacy.noroute", meta)
	insertOutbox(t, db, id, "legacy.noroute", blob)
	return id
}

func seedNativeOutbox(t *testing.T, db *sql.DB) string {
	t.Helper()
	id := uuid.New().String()
	// Native EH-shaped event without legacy.body_base64.
	meta := map[string]any{"eh.native": true}
	blob := marshalEvent(t, "native.event", meta)
	insertOutbox(t, db, id, "native.event", blob)
	return id
}

func marshalEvent(t *testing.T, eventType string, meta map[string]any) string {
	t.Helper()
	event := eh.NewEvent(
		eh.EventType(eventType),
		nil,
		time.Now(),
		eh.WithMetadata(meta),
		eh.ForAggregate("legacy-message", uuid.New(), 1),
	)
	codec := &ehcodec.EventCodec{}
	b, err := codec.MarshalEvent(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	// Ensure JSON shape is stable.
	var check map[string]any
	if err := json.Unmarshal(b, &check); err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func insertOutbox(t *testing.T, db *sql.DB, id, eventType, blob string) {
	t.Helper()
	now := time.Now()
	if _, err := db.Exec(`
		INSERT INTO outbox (id, event_type, aggregate_id, created_at, available_at, handlers, event_blob, retry_count)
		VALUES (?, ?, 'agg', ?, ?, '[]', ?, 0)
	`, id, eventType, now, now, blob); err != nil {
		t.Fatal(err)
	}
}

func outboxCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM outbox`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func auditCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	if !tableExists(t, db, "outbox_drain_audit") {
		return 0
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM outbox_drain_audit`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
