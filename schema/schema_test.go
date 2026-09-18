package schema

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "schema.db")
	db, err := sql.Open("sqlite3", "file:"+path+"?_journal=wal&_busy_timeout=5000&_loc=auto")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestParseStoredAcceptsDriverAndSQLiteSpellings(t *testing.T) {
	want := time.Date(2026, 3, 29, 1, 30, 0, 500_000_000, time.UTC)
	cases := map[string]string{
		"driver utc":       "2026-03-29 01:30:00.5+00:00",
		"driver local":     "2026-03-29 03:30:00.5+02:00",
		"rfc3339 zulu":     "2026-03-29T01:30:00.5Z",
		"rfc3339 offset":   "2026-03-29T03:30:00.5+02:00",
		"space zulu":       "2026-03-29 01:30:00.5Z",
		"fixed fraction":   "2026-03-29 01:30:00.500000000+00:00",
		"sqlite no offset": "2026-03-29 01:30:00.5",
		"sqlite millisecs": "2026-03-29 01:30:00.500",
	}
	for name, raw := range cases {
		got, err := ParseStored(raw)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !got.Equal(want) {
			t.Fatalf("%s: got %s want %s", name, got, want)
		}
		if got.Location() != time.UTC {
			t.Fatalf("%s: location %v, want UTC", name, got.Location())
		}
	}
	noFraction, err := ParseStored("2026-03-29 01:30:00")
	if err != nil || !noFraction.Equal(time.Date(2026, 3, 29, 1, 30, 0, 0, time.UTC)) {
		t.Fatalf("offset-less value must be UTC: %v %v", noFraction, err)
	}
}

func TestParseStoredFailsClosedOnGarbage(t *testing.T) {
	for _, raw := range []string{"", "yesterday", "2026-13-45 99:99:99", "1700000000"} {
		if _, err := ParseStored(raw); !errors.Is(err, ErrTimestampAmbiguous) {
			t.Fatalf("%q: err = %v, want ErrTimestampAmbiguous", raw, err)
		}
	}
}

func TestFormatStoredMatchesDriverBinding(t *testing.T) {
	db := openDB(t)
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, at TIMESTAMP)`); err != nil {
		t.Fatal(err)
	}
	warsaw, err := time.LoadLocation("Europe/Warsaw")
	if err != nil {
		t.Skip("tzdata unavailable")
	}
	for i, ts := range []time.Time{
		time.Date(2026, 10, 25, 2, 30, 0, 123_456_789, warsaw), // DST fall-back hour
		time.Date(2026, 10, 25, 2, 30, 0, 0, warsaw),
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	} {
		if _, err := db.Exec(`INSERT INTO t (id, at) VALUES (?, ?)`, i, UTC(ts)); err != nil {
			t.Fatal(err)
		}
		var raw string
		if err := db.QueryRow(`SELECT CAST(at AS TEXT) FROM t WHERE id = ?`, i).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		if raw != FormatStored(ts) {
			t.Fatalf("driver wrote %q, FormatStored gives %q", raw, FormatStored(ts))
		}
		parsed, err := ParseStored(raw)
		if err != nil || !parsed.Equal(ts) {
			t.Fatalf("round trip lost precision: %q -> %v (%v)", raw, parsed, err)
		}
	}
}

func TestNormalizeColumnUTCRewritesEverySpellingAndPreservesOrder(t *testing.T) {
	db := openDB(t)
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, at TIMESTAMP, note TEXT)`); err != nil {
		t.Fatal(err)
	}
	// Instants in true chronological order 1..6, spelled so that raw text order
	// disagrees with time order (the bug class this migration fixes).
	rows := []struct {
		id  int
		raw string
	}{
		{1, "2026-10-25 02:15:00+02:00"},           // 00:15Z (before DST fall-back)
		{2, "2026-10-25 00:30:00Z"},                // 00:30Z zulu spelling
		{3, "2026-10-25T00:45:00.250000000+00:00"}, // 00:45Z T + fixed fraction
		{4, "2026-10-25 02:00:00+01:00"},           // 01:00Z after fall-back, local text goes backwards
		{5, "2026-10-25 01:15:00"},                 // 01:15Z offset-less, SQLite contract
		{6, "2026-10-25 01:30:00.5+00:00"},         // already canonical
	}
	for _, r := range rows {
		if _, err := db.Exec(`INSERT INTO t (id, at) VALUES (?, ?)`, r.id, r.raw); err != nil {
			t.Fatal(err)
		}
	}
	// Prove the text order is wrong before normalization.
	var firstBefore int
	if err := db.QueryRow(`SELECT id FROM t ORDER BY at ASC LIMIT 1`).Scan(&firstBefore); err != nil {
		t.Fatal(err)
	}
	if firstBefore == 1 {
		t.Fatalf("test fixture does not exercise mis-ordering (first row before normalization = %d)", firstBefore)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	n, err := NormalizeColumnUTC(context.Background(), tx, "t", "at")
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("rewritten = %d, want 5 (row 6 already canonical)", n)
	}
	got, err := db.Query(`SELECT id, CAST(at AS TEXT) FROM t ORDER BY at ASC`)
	if err != nil {
		t.Fatal(err)
	}
	defer got.Close()
	var order []int
	for got.Next() {
		var id int
		var raw string
		if err := got.Scan(&id, &raw); err != nil {
			t.Fatal(err)
		}
		order = append(order, id)
		parsed, err := ParseStored(raw)
		if err != nil {
			t.Fatal(err)
		}
		if raw != FormatStored(parsed) {
			t.Fatalf("row %d not canonical after normalization: %q", id, raw)
		}
	}
	for i, id := range order {
		if id != i+1 {
			t.Fatalf("order after normalization = %v", order)
		}
	}
	// Text comparison against a driver-bound UTC parameter now agrees with time.
	var due int
	cut := time.Date(2026, 10, 25, 1, 0, 0, 0, time.UTC)
	if err := db.QueryRow(`SELECT COUNT(*) FROM t WHERE at <= ?`, cut).Scan(&due); err != nil {
		t.Fatal(err)
	}
	if due != 4 {
		t.Fatalf("rows due at %s = %d, want 4", cut, due)
	}

	// Idempotent.
	tx, _ = db.Begin()
	n, err = NormalizeColumnUTC(context.Background(), tx, "t", "at")
	_ = tx.Commit()
	if err != nil || n != 0 {
		t.Fatalf("second pass rewrote %d rows, err %v", n, err)
	}
}

func TestNormalizeColumnUTCFailsClosedWithoutWriting(t *testing.T) {
	db := openDB(t)
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, at TIMESTAMP)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO t VALUES (1, '2026-01-01 10:00:00+02:00'), (2, 'not a time'), (3, NULL)`); err != nil {
		t.Fatal(err)
	}
	tx, _ := db.Begin()
	_, err := NormalizeColumnUTC(context.Background(), tx, "t", "at")
	if !errors.Is(err, ErrTimestampAmbiguous) {
		t.Fatalf("err = %v, want ErrTimestampAmbiguous", err)
	}
	_ = tx.Rollback()
	var raw string
	if err := db.QueryRow(`SELECT CAST(at AS TEXT) FROM t WHERE id = 1`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if raw != "2026-01-01 10:00:00+02:00" {
		t.Fatalf("row 1 was rewritten despite failure: %q", raw)
	}
}

func TestApplyRecordsMarkerTransactionallyAndIsIdempotent(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	applied, err := IsApplied(ctx, db, "outbox", "x")
	if err != nil || applied {
		t.Fatalf("IsApplied without table = %v, %v", applied, err)
	}
	boom := errors.New("boom")
	if _, err := Apply(ctx, db, "outbox", "x", func(tx *sql.Tx) error {
		if _, err := tx.Exec(`CREATE TABLE created_by_failed (id INTEGER)`); err != nil {
			return err
		}
		return boom
	}); !errors.Is(err, boom) {
		t.Fatalf("Apply err = %v", err)
	}
	if exists, _ := TableExists(ctx, db, "created_by_failed"); exists {
		t.Fatal("failed migration left its table behind")
	}
	if applied, _ := IsApplied(ctx, db, "outbox", "x"); applied {
		t.Fatal("marker recorded for a failed migration")
	}

	runs := 0
	for i := range 2 {
		applied, err := Apply(ctx, db, "outbox", "x", func(tx *sql.Tx) error {
			runs++
			_, err := tx.Exec(`CREATE TABLE created_by_ok (id INTEGER)`)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		if applied != (i == 0) {
			t.Fatalf("Apply #%d reported applied=%v", i+1, applied)
		}
	}
	if runs != 1 {
		t.Fatalf("migration body ran %d times, want 1", runs)
	}
	if applied, _ := IsApplied(ctx, db, "outbox", "x"); !applied {
		t.Fatal("marker missing after Apply")
	}
	if applied, _ := IsApplied(ctx, db, "durable", "x"); applied {
		t.Fatal("marker leaked across components")
	}
	var raw string
	if err := db.QueryRow(`SELECT CAST(applied_at AS TEXT) FROM eh_sqlite_migrations`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if parsed, err := ParseStored(raw); err != nil || raw != FormatStored(parsed) {
		t.Fatalf("applied_at not canonical UTC: %q", raw)
	}
}
