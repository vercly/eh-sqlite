package deadletter

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vercly/eh-sqlite/schema"
)

func TestEnsureSchemaDedupePrefersExportedAt(t *testing.T) {
	db := newTestDB(t)

	// Old schema without the uniqueness index, as left by pre-step-1 installs.
	if _, err := db.Exec(`
		CREATE TABLE dead_letters (
			id TEXT PRIMARY KEY,
			source TEXT NOT NULL,
			event_type TEXT NOT NULL,
			aggregate_id TEXT NOT NULL,
			handler_type TEXT NOT NULL,
			outbox_id TEXT,
			remaining_handlers TEXT,
			blob TEXT NOT NULL,
			error TEXT NOT NULL,
			retry_count INTEGER DEFAULT 0,
			created_at TIMESTAMP NOT NULL,
			dead_at TIMESTAMP NOT NULL,
			exported_at TIMESTAMP
		);
		CREATE INDEX idx_dead_letters_source_created ON dead_letters (source, created_at);
	`); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	// Earlier row (lower rowid) is NOT exported; later duplicate IS exported.
	// MIN(rowid) alone would keep the unexported row and drop the export marker.
	if _, err := db.Exec(`
		INSERT INTO dead_letters (id, source, event_type, aggregate_id, handler_type, outbox_id, remaining_handlers, blob, error, retry_count, created_at, dead_at, exported_at)
		VALUES
			('early-unexported', 'outbox', 'Event', 'agg', 'handler_a', 'outbox-1', '[]', 'blob-early', 'err-early', 0, ?, ?, NULL),
			('late-exported', 'outbox', 'Event', 'agg', 'handler_a', 'outbox-1', '[]', 'blob-late', 'err-late', 1, ?, ?, ?)
	`, now, now, now.Add(time.Minute), now.Add(time.Minute), now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}

	if err := EnsureSchema(db, "dead_letters"); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM dead_letters WHERE source = 'outbox' AND outbox_id = 'outbox-1' AND handler_type = 'handler_a'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("rows after dedupe = %d, want 1", count)
	}

	var id string
	var exportedAt sql.NullTime
	var blob, errMsg string
	if err := db.QueryRow(`
		SELECT id, exported_at, blob, error
		FROM dead_letters
		WHERE source = 'outbox' AND outbox_id = 'outbox-1' AND handler_type = 'handler_a'
	`).Scan(&id, &exportedAt, &blob, &errMsg); err != nil {
		t.Fatal(err)
	}
	if id != "late-exported" {
		t.Fatalf("kept id = %s, want late-exported (row with exported_at)", id)
	}
	if !exportedAt.Valid {
		t.Fatal("kept row must preserve exported_at")
	}
	if blob != "blob-late" || errMsg != "err-late" {
		t.Fatalf("kept row body = (%q, %q), want late exported values", blob, errMsg)
	}

	// Uniqueness must be active after migration.
	_, err := db.Exec(`
		INSERT INTO dead_letters (id, source, event_type, aggregate_id, handler_type, outbox_id, remaining_handlers, blob, error, retry_count, created_at, dead_at)
		VALUES ('dup', 'outbox', 'Event', 'agg', 'handler_a', 'outbox-1', '[]', 'x', 'y', 0, ?, ?)
	`, now, now)
	if err == nil {
		t.Fatal("expected unique constraint violation after EnsureSchema")
	}
}

func TestAddColumnIfAbsentUsesPragmaNotErrorText(t *testing.T) {
	db := newTestDB(t)
	if _, err := db.Exec(`CREATE TABLE t (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if err := AddColumnIfAbsent(db, "t", "exported_at", "TIMESTAMP"); err != nil {
		t.Fatal(err)
	}
	// Second add must be a no-op via PRAGMA table_info, not ALTER+string match.
	if err := AddColumnIfAbsent(db, "t", "exported_at", "TIMESTAMP"); err != nil {
		t.Fatal(err)
	}
	has, err := ColumnExists(db, "t", "exported_at")
	if err != nil || !has {
		t.Fatalf("column missing: has=%v err=%v", has, err)
	}
	has, err = ColumnExists(db, "t", "nope")
	if err != nil || has {
		t.Fatalf("unexpected column: has=%v err=%v", has, err)
	}
}

func newTestDB(t testing.TB) *sql.DB {
	t.Helper()

	f, err := os.CreateTemp("", "deadletter-*.db")
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

// legacyDeadLettersDDL is the pre-UTC schema shape: the columns exist but the
// text in them was written with whatever offset the writing process had.
const legacyDeadLettersDDL = `
	CREATE TABLE dead_letters (
		id TEXT PRIMARY KEY,
		source TEXT NOT NULL,
		event_type TEXT NOT NULL,
		aggregate_id TEXT NOT NULL,
		handler_type TEXT NOT NULL,
		outbox_id TEXT,
		remaining_handlers TEXT,
		blob TEXT NOT NULL,
		error TEXT NOT NULL,
		retry_count INTEGER DEFAULT 0,
		created_at TIMESTAMP NOT NULL,
		dead_at TIMESTAMP NOT NULL,
		exported_at TIMESTAMP,
		replayed_at TIMESTAMP,
		replayed_by TEXT
	);`

func insertRawDeadLetter(t testing.TB, db *sql.DB, id, createdAt, deadAt string, exportedAt, replayedAt any) {
	t.Helper()
	if _, err := db.Exec(`
		INSERT INTO dead_letters (id, source, event_type, aggregate_id, handler_type, outbox_id, remaining_handlers, blob, error, retry_count, created_at, dead_at, exported_at, replayed_at)
		VALUES (?, 'outbox', 'Event', 'agg', 'handler_a', ?, '[]', 'blob', 'err', 0, ?, ?, ?, ?)
	`, id, id, createdAt, deadAt, exportedAt, replayedAt); err != nil {
		t.Fatal(err)
	}
}

func rawText(t testing.TB, db *sql.DB, column, id string) string {
	t.Helper()
	var raw sql.NullString
	if err := db.QueryRow(`SELECT CAST(`+column+` AS TEXT) FROM dead_letters WHERE id = ?`, id).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	return raw.String
}

func TestEnsureSchemaNormalizesTimestampsToCanonicalUTC(t *testing.T) {
	db := newTestDB(t)
	if _, err := db.Exec(legacyDeadLettersDDL); err != nil {
		t.Fatal(err)
	}

	// Same instant, three legacy spellings: fixed offset, "Z", and "T".
	want := time.Date(2025, 3, 4, 10, 30, 0, 500000000, time.UTC)
	insertRawDeadLetter(t, db, "offset", "2025-03-04 12:30:00.5+02:00", "2025-03-04 12:30:00.5+02:00",
		"2025-03-04 12:30:00.5+02:00", "2025-03-04 12:30:00.5+02:00")
	insertRawDeadLetter(t, db, "zulu", "2025-03-04 10:30:00.5Z", "2025-03-04 10:30:00.5Z", nil, nil)
	insertRawDeadLetter(t, db, "tsep", "2025-03-04T10:30:00.5Z", "2025-03-04T10:30:00.5Z", nil, nil)

	if err := EnsureSchema(db, "dead_letters"); err != nil {
		t.Fatal(err)
	}

	canonical := schema.FormatStored(want)
	for _, id := range []string{"offset", "zulu", "tsep"} {
		for _, col := range []string{"created_at", "dead_at"} {
			if got := rawText(t, db, col, id); got != canonical {
				t.Fatalf("%s.%s = %q, want %q", id, col, got, canonical)
			}
		}
	}
	for _, col := range []string{"exported_at", "replayed_at"} {
		if got := rawText(t, db, col, "offset"); got != canonical {
			t.Fatalf("offset.%s = %q, want %q", col, got, canonical)
		}
		if got := rawText(t, db, col, "zulu"); got != "" {
			t.Fatalf("zulu.%s = %q, want NULL", col, got)
		}
	}

	applied, err := schema.IsApplied(context.Background(), db, "deadletter", "utc_timestamps")
	if err != nil {
		t.Fatal(err)
	}
	if !applied {
		t.Fatal("migration marker deadletter/utc_timestamps missing after EnsureSchema")
	}
}

func TestEnsureSchemaAddsPublicationColumns(t *testing.T) {
	db := newTestDB(t)
	if _, err := db.Exec(legacyDeadLettersDDL); err != nil {
		t.Fatal(err)
	}
	if err := EnsureSchema(db, "dead_letters"); err != nil {
		t.Fatal(err)
	}
	for _, col := range []string{"publication_id", "legacy_outbox_id"} {
		has, err := ColumnExists(db, "dead_letters", col)
		if err != nil {
			t.Fatal(err)
		}
		if !has {
			t.Fatalf("column %s missing after EnsureSchema", col)
		}
	}
}

func TestEnsureSchemaSecondRunIsNoOp(t *testing.T) {
	db := newTestDB(t)
	if err := EnsureSchema(db, "dead_letters"); err != nil {
		t.Fatal(err)
	}
	// A row written after the marker exists must survive a second EnsureSchema
	// byte-for-byte even when its text is not canonical: the migration is
	// marker-guarded and must not run again.
	insertRawDeadLetter(t, db, "post-marker", "2025-03-04 12:30:00.5+02:00", "2025-03-04 12:30:00.5+02:00", nil, nil)
	if err := EnsureSchema(db, "dead_letters"); err != nil {
		t.Fatal(err)
	}
	if got := rawText(t, db, "created_at", "post-marker"); got != "2025-03-04 12:30:00.5+02:00" {
		t.Fatalf("created_at = %q, want untouched legacy text", got)
	}

	var markers int
	if err := db.QueryRow(`SELECT COUNT(*) FROM eh_sqlite_migrations WHERE component = 'deadletter' AND name = 'utc_timestamps'`).Scan(&markers); err != nil {
		t.Fatal(err)
	}
	if markers != 1 {
		t.Fatalf("markers = %d, want 1", markers)
	}
}

func TestEnsureSchemaFailsClosedOnUnparseableTimestamp(t *testing.T) {
	db := newTestDB(t)
	if _, err := db.Exec(legacyDeadLettersDDL); err != nil {
		t.Fatal(err)
	}
	insertRawDeadLetter(t, db, "good", "2025-03-04 12:30:00.5+02:00", "2025-03-04 12:30:00.5+02:00", nil, nil)
	insertRawDeadLetter(t, db, "bad", "not a timestamp", "2025-03-04 10:30:00.5Z", nil, nil)

	err := EnsureSchema(db, "dead_letters")
	if err == nil {
		t.Fatal("EnsureSchema error = nil, want ErrTimestampAmbiguous")
	}
	if !errors.Is(err, schema.ErrTimestampAmbiguous) {
		t.Fatalf("EnsureSchema error = %v, want ErrTimestampAmbiguous", err)
	}
	// Nothing rewritten, no marker: the whole migration rolled back.
	if got := rawText(t, db, "created_at", "good"); got != "2025-03-04 12:30:00.5+02:00" {
		t.Fatalf("created_at = %q, want untouched legacy text after rollback", got)
	}
	applied, aerr := schema.IsApplied(context.Background(), db, "deadletter", "utc_timestamps")
	if aerr != nil {
		t.Fatal(aerr)
	}
	if applied {
		t.Fatal("marker written although the migration failed")
	}
}
