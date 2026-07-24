package deadletter

import (
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
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
