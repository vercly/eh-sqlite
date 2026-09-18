package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/vercly/eh-sqlite/schema"
	eh "github.com/vercly/eventhorizon"
	ehcodec "github.com/vercly/eventhorizon/codec/json"
	"github.com/vercly/eventhorizon/uuid"
)

// Shared v2 test helpers. Test files must not create new helpers with these
// names; add file-local helpers with distinct names instead.

// v1OutboxDDL is the historical v1 table shape used to seed migration tests.
const v1OutboxDDL = `
CREATE TABLE IF NOT EXISTS %s (
	id TEXT PRIMARY KEY,
	event_type TEXT NOT NULL,
	aggregate_id TEXT NOT NULL,
	created_at TIMESTAMP NOT NULL,
	available_at TIMESTAMP,
	taken_at TIMESTAMP,
	handlers TEXT NOT NULL,
	event_blob TEXT NOT NULL,
	retry_count INTEGER DEFAULT 0
)`

// createV1Table creates the v1 outbox table (default prefix "outbox") so a test
// can seed legacy rows before NewOutbox/StartChecked migrates them.
func createV1Table(t testing.TB, db *sql.DB, prefix string) {
	t.Helper()
	if _, err := db.Exec(fmt.Sprintf(v1OutboxDDL, prefix)); err != nil {
		t.Fatal(err)
	}
}

// v1Seed describes one legacy row for insertV1Row.
type v1Seed struct {
	ID          string // "" → new uuid
	Event       eh.Event
	Handlers    []string // nil/empty → rematch sentinel ("[]")
	CreatedAt   time.Time
	AvailableAt time.Time // zero → CreatedAt
	TakenAt     time.Time // zero → NULL
	RetryCount  int
	// RawCreatedAt / RawAvailableAt override the bound time with literal text
	// (to seed non-UTC or odd spellings for migration tests).
	RawCreatedAt   string
	RawAvailableAt string
}

// insertV1Row seeds one v1 row and returns its id.
func insertV1Row(t testing.TB, db *sql.DB, prefix string, seed v1Seed) string {
	t.Helper()
	if seed.ID == "" {
		seed.ID = uuid.New().String()
	}
	if seed.AvailableAt.IsZero() {
		seed.AvailableAt = seed.CreatedAt
	}
	codec := &ehcodec.EventCodec{}
	blob, err := codec.MarshalEvent(context.Background(), seed.Event)
	if err != nil {
		t.Fatal(err)
	}
	handlers := seed.Handlers
	if handlers == nil {
		handlers = []string{}
	}
	handlersBlob, err := jsoniter.Marshal(handlers)
	if err != nil {
		t.Fatal(err)
	}
	var createdAt, availableAt any = seed.CreatedAt, seed.AvailableAt
	if seed.RawCreatedAt != "" {
		createdAt = seed.RawCreatedAt
	}
	if seed.RawAvailableAt != "" {
		availableAt = seed.RawAvailableAt
	}
	var takenAt any = nil
	if !seed.TakenAt.IsZero() {
		takenAt = seed.TakenAt
	}
	if _, err := db.Exec(fmt.Sprintf(`
		INSERT INTO %s (id, event_type, aggregate_id, created_at, available_at, taken_at, handlers, event_blob, retry_count)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, prefix),
		seed.ID, seed.Event.EventType().String(), seed.Event.AggregateID().String(),
		createdAt, availableAt, takenAt, string(handlersBlob), string(blob), seed.RetryCount); err != nil {
		t.Fatal(err)
	}
	return seed.ID
}

// deliverySeed describes one v2 delivery for insertDeliveryDirect.
type deliverySeed struct {
	Event       eh.Event
	HandlerType string // "" → rematch sentinel
	CreatedAt   time.Time
	AvailableAt time.Time // zero → CreatedAt
	TakenAt     time.Time // zero → NULL
	RetryCount  int
}

// insertDeliveryDirect writes a publication plus one delivery bypassing
// HandleEvent (for claim/stale/retry fixtures). dispatch_key is left NULL;
// StartChecked (or o.reconcileForTest) computes it against the registration.
// Returns (publicationID, deliveryID).
func insertDeliveryDirect(t testing.TB, o *Outbox, seed deliverySeed) (string, string) {
	t.Helper()
	if seed.AvailableAt.IsZero() {
		seed.AvailableAt = seed.CreatedAt
	}
	blob, err := o.codec.MarshalEvent(context.Background(), seed.Event)
	if err != nil {
		t.Fatal(err)
	}
	publicationID := uuid.New().String()
	deliveryID := uuid.New().String()
	if _, err := o.db.Exec(fmt.Sprintf(`
		INSERT INTO %s (publication_id, event_type, aggregate_id, partition_key, event_blob, created_at, origin, origin_ref)
		VALUES (?, ?, ?, ?, ?, ?, 'publish', NULL)`, o.publicationsTable),
		publicationID, seed.Event.EventType().String(), seed.Event.AggregateID().String(),
		eventPartitionKey(seed.Event), string(blob), schema.UTC(seed.CreatedAt)); err != nil {
		t.Fatal(err)
	}
	var handlerType any = nil
	if seed.HandlerType != "" {
		handlerType = seed.HandlerType
	}
	var takenAt any = nil
	if !seed.TakenAt.IsZero() {
		takenAt = schema.UTC(seed.TakenAt)
	}
	if _, err := o.db.Exec(fmt.Sprintf(`
		INSERT INTO %s (id, publication_id, handler_type, dispatch_key, dispatch_config, event_type, aggregate_id,
		                created_at, available_at, taken_at, retry_count, unresolved_at, legacy_outbox_id)
		VALUES (?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, ?, NULL, NULL)`, o.deliveriesTable),
		deliveryID, publicationID, handlerType, seed.Event.EventType().String(), seed.Event.AggregateID().String(),
		schema.UTC(seed.CreatedAt), schema.UTC(seed.AvailableAt), takenAt, seed.RetryCount); err != nil {
		t.Fatal(err)
	}
	return publicationID, deliveryID
}

// reconcileForTest runs the startup reconcile (taken_at reset + dispatch key
// computation + claim-key ring) without starting the fetcher, for tests that
// drive processBatch manually after seeding rows with insertDeliveryDirect.
// Rows seeded with a TakenAt that must survive should be re-stamped after.
func reconcileForTest(t testing.TB, o *Outbox) {
	t.Helper()
	if _, err := o.startupReconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// prepareWithoutFetcher closes registration, allows publishing and reconciles,
// but never launches the background fetcher: tests drive processBatch /
// fetchAndDispatch manually (deterministic fault injection, no races with a
// live processor).
func prepareWithoutFetcher(t testing.TB, o *Outbox) {
	t.Helper()
	o.registrationClosed.Store(true)
	reconcileForTest(t, o)
	o.processorRunning.Store(true)
}

// setTakenAt stamps taken_at on one delivery (for stale-claim fixtures).
func setTakenAt(t testing.TB, o *Outbox, deliveryID string, takenAt time.Time) {
	t.Helper()
	if _, err := o.db.Exec(fmt.Sprintf(`UPDATE %s SET taken_at = ? WHERE id = ?`, o.deliveriesTable), schema.UTC(takenAt), deliveryID); err != nil {
		t.Fatal(err)
	}
}

// deliveryRow is a readable projection of one outbox_deliveries row.
type deliveryRow struct {
	Seq            int64
	ID             string
	PublicationID  string
	HandlerType    string // "" = sentinel
	DispatchKey    string // "" = NULL
	AvailableAt    time.Time
	TakenAt        sql.NullTime
	RetryCount     int
	UnresolvedAt   sql.NullTime
	LegacyOutboxID string
}

// listDeliveries returns every delivery ordered by seq.
func listDeliveries(t testing.TB, o *Outbox) []deliveryRow {
	t.Helper()
	rows, err := o.db.Query(fmt.Sprintf(`
		SELECT seq, id, publication_id, COALESCE(handler_type, ''), COALESCE(dispatch_key, ''), available_at, taken_at,
		       retry_count, unresolved_at, COALESCE(legacy_outbox_id, '')
		FROM %s ORDER BY seq ASC`, o.deliveriesTable))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []deliveryRow
	for rows.Next() {
		var r deliveryRow
		var availableAt sql.NullTime
		if err := rows.Scan(&r.Seq, &r.ID, &r.PublicationID, &r.HandlerType, &r.DispatchKey, &availableAt, &r.TakenAt,
			&r.RetryCount, &r.UnresolvedAt, &r.LegacyOutboxID); err != nil {
			t.Fatal(err)
		}
		r.AvailableAt = availableAt.Time
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// countDeliveries / countPublications / countDeadLetters are quick assertions.
func countDeliveries(t testing.TB, o *Outbox) int {
	t.Helper()
	return countRows(t, o.db, o.deliveriesTable)
}

func countPublications(t testing.TB, o *Outbox) int {
	t.Helper()
	return countRows(t, o.db, o.publicationsTable)
}

func countDeadLetters(t testing.TB, o *Outbox) int {
	t.Helper()
	return countRows(t, o.db, o.deadLetterTable)
}

func countRows(t testing.TB, db *sql.DB, table string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// waitUntil polls cond until true or the deadline passes (only for progress
// of background workers; correctness assertions use channels/barriers).
func waitUntil(t testing.TB, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("condition not met before timeout")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// storedText runs a scalar query and returns its text. For TIMESTAMP columns
// the driver parses values into time.Time, so canonical-text assertions must
// select `CAST(col AS TEXT)`.
func storedText(t testing.TB, db *sql.DB, query string, args ...any) string {
	t.Helper()
	var raw string
	if err := db.QueryRow(query, args...).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	return raw
}
