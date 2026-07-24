package replay

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vercly/eh-sqlite/internal/deadletter"
	"github.com/vercly/eventhorizon/uuid"
)

func TestEnsureSchemaAddsReplayColumnsOnOldTable(t *testing.T) {
	db := openEmptyDB(t)
	// Old event-store shape: required tables present, dead_letters without replayed_*.
	createMinimalOutboxAndAsync(t, db)
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
			dead_at TIMESTAMP NOT NULL
		);
	`); err != nil {
		t.Fatal(err)
	}
	if err := EnsureSchema(db); err != nil {
		t.Fatal(err)
	}
	// Columns usable.
	if _, err := db.Exec(`UPDATE dead_letters SET replayed_at = ?, replayed_by = ? WHERE 0`, time.Now(), "actor"); err != nil {
		t.Fatalf("replay columns missing: %v", err)
	}
	// Must not invent a second partial outbox definition — table already existed.
	ok, err := deadletter.TableExists(db, "outbox")
	if err != nil || !ok {
		t.Fatalf("outbox should still exist: %v", err)
	}
}

func TestEnsureSchemaRejectsEmptyDatabase(t *testing.T) {
	db := openEmptyDB(t)
	err := EnsureSchema(db)
	if err == nil {
		t.Fatal("want error on empty DB without required tables")
	}
	if !strings.Contains(err.Error(), "missing") {
		t.Fatalf("err = %v, want missing table message", err)
	}
}

func TestEnsureSchemaRejectsPartialDatabase(t *testing.T) {
	db := openEmptyDB(t)
	if _, err := db.Exec(`CREATE TABLE dead_letters (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	// outbox/async_tasks missing — must not CREATE stubs.
	err := EnsureSchema(db)
	if err == nil {
		t.Fatal("want error when outbox/async_tasks missing")
	}
	ok, _ := deadletter.TableExists(db, "outbox")
	if ok {
		t.Fatal("EnsureSchema must not create outbox stub")
	}
	ok, _ = deadletter.TableExists(db, "async_tasks")
	if ok {
		t.Fatal("EnsureSchema must not create async_tasks stub")
	}
}

func TestReplayTerminalOutboxRestoresHandler(t *testing.T) {
	db := openDB(t)
	if err := EnsureSchema(db); err != nil {
		t.Fatal(err)
	}
	outboxID := uuid.New().String()
	// Stale attempt state that must not survive replay.
	future := time.Now().Add(2 * time.Hour)
	seedOutboxWithRetry(t, db, outboxID, `["other"]`, `{"event":"blob"}`, 5, future)
	dlqID := uuid.New().String()
	seedDLQWithRetry(t, db, dlqID, SourceOutbox, "evt", "agg", "failed_handler", outboxID, `{"event":"blob"}`, 5)

	before := time.Now().Add(-time.Second)
	res, err := ReplayDeadLetters(context.Background(), db, Options{
		Apply: true, Actor: "ops@test", IDs: []string{dlqID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Replayed != 1 {
		t.Fatalf("replayed = %d, want 1", res.Replayed)
	}
	var handlers string
	var retryCount int
	var availableAt time.Time
	var takenAt sql.NullTime
	if err := db.QueryRow(`SELECT handlers, retry_count, available_at, taken_at FROM outbox WHERE id = ?`, outboxID).
		Scan(&handlers, &retryCount, &availableAt, &takenAt); err != nil {
		t.Fatal(err)
	}
	var list []string
	_ = json.Unmarshal([]byte(handlers), &list)
	if !contains(list, "failed_handler") || !contains(list, "other") {
		t.Fatalf("handlers = %v, want failed_handler and other", list)
	}
	if retryCount != 0 {
		t.Fatalf("retry_count = %d, want 0 (fresh attempt)", retryCount)
	}
	if takenAt.Valid {
		t.Fatal("taken_at must be cleared")
	}
	if availableAt.Before(before) || availableAt.After(time.Now().Add(time.Minute)) {
		t.Fatalf("available_at = %s, want ~now for immediate reprocess", availableAt)
	}
	assertReplayed(t, db, dlqID, "ops@test")
}

func TestReplayTerminalRecreatesWithFreshAttempt(t *testing.T) {
	db := openDB(t)
	if err := EnsureSchema(db); err != nil {
		t.Fatal(err)
	}
	outboxID := uuid.New().String()
	dlqID := uuid.New().String()
	// No existing outbox row; DLQ carries exhausted retry_count that must not
	// be copied into the recreated outbox row.
	seedDLQWithRetry(t, db, dlqID, SourceOutbox, "evt", "agg", "failed_handler", outboxID, `{"event":"blob"}`, 9)

	before := time.Now().Add(-time.Second)
	res, err := ReplayDeadLetters(context.Background(), db, Options{
		Apply: true, Actor: "ops", IDs: []string{dlqID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Replayed != 1 {
		t.Fatalf("%+v", res)
	}
	var handlers string
	var retryCount int
	var availableAt time.Time
	if err := db.QueryRow(`SELECT handlers, retry_count, available_at FROM outbox WHERE id = ?`, outboxID).
		Scan(&handlers, &retryCount, &availableAt); err != nil {
		t.Fatal(err)
	}
	if handlers != `["failed_handler"]` {
		t.Fatalf("handlers = %s", handlers)
	}
	if retryCount != 0 {
		t.Fatalf("retry_count = %d, want 0 (not DLQ exhausted count)", retryCount)
	}
	if availableAt.Before(before) || availableAt.After(time.Now().Add(time.Minute)) {
		t.Fatalf("available_at = %s, want ~now", availableAt)
	}
}

func TestReplayMissingExplicitIDReportsError(t *testing.T) {
	db := openDB(t)
	if err := EnsureSchema(db); err != nil {
		t.Fatal(err)
	}
	res, err := ReplayDeadLetters(context.Background(), db, Options{
		Apply: true, Actor: "ops", IDs: []string{"does-not-exist"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Missing != 1 || res.Errors != 1 || res.Replayed != 0 {
		t.Fatalf("%+v", res)
	}
	if len(res.Items) != 1 || res.Items[0].Status != StatusSkipped || res.Items[0].Detail != "dead letter id not found" {
		t.Fatalf("items = %+v", res.Items)
	}
}

func TestReplayDedupesExplicitIDs(t *testing.T) {
	db := openDB(t)
	if err := EnsureSchema(db); err != nil {
		t.Fatal(err)
	}
	dlqID := uuid.New().String()
	seedDLQ(t, db, dlqID, SourceOutbox, "e", "a", HandlerNoMatch, "", `{}`)
	res, err := ReplayDeadLetters(context.Background(), db, Options{
		Apply: true, Actor: "ops", IDs: []string{dlqID, dlqID, dlqID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Replayed != 1 || len(res.Items) != 1 {
		t.Fatalf("dedupe: %+v", res)
	}
}

func TestReplayNoMatchInsertsEmptyHandlers(t *testing.T) {
	db := openDB(t)
	if err := EnsureSchema(db); err != nil {
		t.Fatal(err)
	}
	dlqID := uuid.New().String()
	seedDLQ(t, db, dlqID, SourceOutbox, "evt", "agg", HandlerNoMatch, "", `{"event":"x"}`)

	res, err := ReplayDeadLetters(context.Background(), db, Options{
		Apply: true, Actor: "ops", IDs: []string{dlqID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Replayed != 1 {
		t.Fatalf("replayed = %d", res.Replayed)
	}
	var handlers string
	if err := db.QueryRow(`SELECT handlers FROM outbox WHERE id = ?`, dlqID).Scan(&handlers); err != nil {
		t.Fatal(err)
	}
	if handlers != "[]" {
		t.Fatalf("handlers = %q, want []", handlers)
	}
}

func TestReplayCommandInsertsAsyncTask(t *testing.T) {
	db := openDB(t)
	if err := EnsureSchema(db); err != nil {
		t.Fatal(err)
	}
	dlqID := uuid.New().String()
	seedDLQ(t, db, dlqID, SourceCommand, "MyCommand", "agg", "command_handler", "", `{"cmd":1}`)

	res, err := ReplayDeadLetters(context.Background(), db, Options{
		Apply: true, Actor: "ops", IDs: []string{dlqID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Replayed != 1 {
		t.Fatal(res)
	}
	var status, blob string
	if err := db.QueryRow(`SELECT status, command_blob FROM async_tasks WHERE task_uuid = ?`, "dlq-replay-"+dlqID).
		Scan(&status, &blob); err != nil {
		t.Fatal(err)
	}
	if status != "new" || blob != `{"cmd":1}` {
		t.Fatalf("task status=%s blob=%s", status, blob)
	}
}

func TestReplayIdempotentSecondPass(t *testing.T) {
	db := openDB(t)
	if err := EnsureSchema(db); err != nil {
		t.Fatal(err)
	}
	dlqID := uuid.New().String()
	outboxID := uuid.New().String()
	seedOutbox(t, db, outboxID, `[]`, `{}`)
	seedDLQ(t, db, dlqID, SourceOutbox, "e", "a", "h1", outboxID, `{}`)

	opts := Options{Apply: true, Actor: "ops", IDs: []string{dlqID}}
	if _, err := ReplayDeadLetters(context.Background(), db, opts); err != nil {
		t.Fatal(err)
	}
	res, err := ReplayDeadLetters(context.Background(), db, opts)
	if err != nil {
		t.Fatal(err)
	}
	if res.AlreadyReplayed != 1 || res.Replayed != 0 {
		t.Fatalf("second pass: %+v", res)
	}
}

func TestReplayDryRunNoWrites(t *testing.T) {
	db := openDB(t)
	if err := EnsureSchema(db); err != nil {
		t.Fatal(err)
	}
	dlqID := uuid.New().String()
	seedDLQ(t, db, dlqID, SourceOutbox, "e", "a", HandlerNoMatch, "", `{}`)

	res, err := ReplayDeadLetters(context.Background(), db, Options{IDs: []string{dlqID}})
	if err != nil {
		t.Fatal(err)
	}
	if !res.DryRun || res.Items[0].Status != StatusWouldReplay {
		t.Fatalf("%+v", res)
	}
	var n int
	_ = db.QueryRow(`SELECT COUNT(*) FROM outbox`).Scan(&n)
	if n != 0 {
		t.Fatalf("dry-run wrote outbox rows = %d", n)
	}
	var replayed sql.NullTime
	_ = db.QueryRow(`SELECT replayed_at FROM dead_letters WHERE id = ?`, dlqID).Scan(&replayed)
	if replayed.Valid {
		t.Fatal("dry-run marked replayed")
	}
}

func TestReplayDryRunDoesNotMigrateOldSchema(t *testing.T) {
	db := openEmptyDB(t)
	createMinimalOutboxAndAsync(t, db)
	// Old dead_letters without replayed_*.
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
			dead_at TIMESTAMP NOT NULL
		);
	`); err != nil {
		t.Fatal(err)
	}
	dlqID := uuid.New().String()
	seedDLQ(t, db, dlqID, SourceOutbox, "e", "a", HandlerNoMatch, "", `{}`)

	beforeVer := schemaVersion(t, db)
	beforeCols := deadLetterColumns(t, db)

	res, err := ReplayDeadLetters(context.Background(), db, Options{IDs: []string{dlqID}})
	if err != nil {
		t.Fatal(err)
	}
	if !res.DryRun || res.Items[0].Status != StatusWouldReplay {
		t.Fatalf("%+v", res)
	}

	afterVer := schemaVersion(t, db)
	afterCols := deadLetterColumns(t, db)
	if afterVer != beforeVer {
		t.Fatalf("schema_version changed on dry-run: %d -> %d", beforeVer, afterVer)
	}
	if afterCols["replayed_at"] || afterCols["replayed_by"] {
		t.Fatalf("dry-run must not add replayed_* columns: %v", afterCols)
	}
	if len(afterCols) != len(beforeCols) {
		t.Fatalf("column set changed on dry-run: before=%v after=%v", beforeCols, afterCols)
	}
	var n int
	_ = db.QueryRow(`SELECT COUNT(*) FROM outbox`).Scan(&n)
	if n != 0 {
		t.Fatal("dry-run wrote outbox data")
	}
}

func TestReplayApplyMigratesOldSchema(t *testing.T) {
	db := openEmptyDB(t)
	createMinimalOutboxAndAsync(t, db)
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
			dead_at TIMESTAMP NOT NULL
		);
	`); err != nil {
		t.Fatal(err)
	}
	dlqID := uuid.New().String()
	seedDLQ(t, db, dlqID, SourceOutbox, "e", "a", HandlerNoMatch, "", `{}`)

	beforeVer := schemaVersion(t, db)
	res, err := ReplayDeadLetters(context.Background(), db, Options{
		Apply: true, Actor: "ops", IDs: []string{dlqID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Replayed != 1 {
		t.Fatalf("%+v", res)
	}
	afterVer := schemaVersion(t, db)
	if afterVer <= beforeVer {
		t.Fatalf("apply should migrate schema: version %d -> %d", beforeVer, afterVer)
	}
	cols := deadLetterColumns(t, db)
	if !cols["replayed_at"] || !cols["replayed_by"] {
		t.Fatalf("apply must add replayed_*: %v", cols)
	}
	assertReplayed(t, db, dlqID, "ops")
}

func TestReplayApplyRequiresActorAndIDs(t *testing.T) {
	db := openDB(t)
	if _, err := ReplayDeadLetters(context.Background(), db, Options{Apply: true, IDs: []string{"x"}}); err == nil {
		t.Fatal("want actor error")
	}
	if _, err := ReplayDeadLetters(context.Background(), db, Options{Apply: true, Actor: "a"}); err == nil {
		t.Fatal("want ids error")
	}
}

func TestReplayApplyWhitespaceOnlyIDsDoesNotBroadApply(t *testing.T) {
	db := openDB(t)
	if err := EnsureSchema(db); err != nil {
		t.Fatal(err)
	}
	// Pending DLQ that would be touched by a broad apply.
	seedDLQ(t, db, uuid.New().String(), SourceOutbox, "e", "a", HandlerNoMatch, "", `{}`)
	_, err := ReplayDeadLetters(context.Background(), db, Options{
		Apply: true, Actor: "ops", IDs: []string{" ", "\t"}, AllowBroad: false,
	})
	if err == nil || !strings.Contains(err.Error(), "explicit --id") {
		t.Fatalf("want explicit --id error, got %v", err)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM outbox`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("must not broad-replay; outbox rows=%d", n)
	}
}

func TestReplayRestoreFailureDoesNotMarkReplayed(t *testing.T) {
	db := openDB(t)
	if err := EnsureSchema(db); err != nil {
		t.Fatal(err)
	}
	// Terminal without outbox_id fails restore; TX must roll back mark.
	dlqID := uuid.New().String()
	seedDLQ(t, db, dlqID, SourceOutbox, "e", "a", "h1", "", `{}`)

	res, err := ReplayDeadLetters(context.Background(), db, Options{
		Apply: true, Actor: "ops", IDs: []string{dlqID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Errors != 1 || res.Replayed != 0 {
		t.Fatalf("%+v", res)
	}
	var replayed sql.NullTime
	if err := db.QueryRow(`SELECT replayed_at FROM dead_letters WHERE id = ?`, dlqID).Scan(&replayed); err != nil {
		t.Fatal(err)
	}
	if replayed.Valid {
		t.Fatal("failed restore must not mark replayed")
	}
}

func TestReplayCommandIdempotentTaskUUID(t *testing.T) {
	db := openDB(t)
	if err := EnsureSchema(db); err != nil {
		t.Fatal(err)
	}
	dlqID := uuid.New().String()
	seedDLQ(t, db, dlqID, SourceCommand, "Cmd", "agg", "command_handler", "", `{"v":1}`)
	opts := Options{Apply: true, Actor: "ops", IDs: []string{dlqID}}
	if _, err := ReplayDeadLetters(context.Background(), db, opts); err != nil {
		t.Fatal(err)
	}
	// Clear replay marker to force re-apply path; task_uuid must not duplicate.
	if _, err := db.Exec(`UPDATE dead_letters SET replayed_at = NULL, replayed_by = NULL WHERE id = ?`, dlqID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE async_tasks SET status = 'failed_permanent' WHERE task_uuid = ?`, "dlq-replay-"+dlqID); err != nil {
		t.Fatal(err)
	}
	res, err := ReplayDeadLetters(context.Background(), db, opts)
	if err != nil {
		t.Fatal(err)
	}
	if res.Replayed != 1 {
		t.Fatalf("%+v", res)
	}
	var n int
	var status string
	if err := db.QueryRow(`SELECT COUNT(*), MAX(status) FROM async_tasks WHERE task_uuid = ?`, "dlq-replay-"+dlqID).
		Scan(&n, &status); err != nil {
		t.Fatal(err)
	}
	if n != 1 || status != "new" {
		t.Fatalf("tasks n=%d status=%s", n, status)
	}
}

func TestReplaySourceFilter(t *testing.T) {
	db := openDB(t)
	if err := EnsureSchema(db); err != nil {
		t.Fatal(err)
	}
	idOut := uuid.New().String()
	idCmd := uuid.New().String()
	seedDLQ(t, db, idOut, SourceOutbox, "e", "a", HandlerNoMatch, "", `{}`)
	seedDLQ(t, db, idCmd, SourceCommand, "C", "a", "h", "", `{}`)

	res, err := ReplayDeadLetters(context.Background(), db, Options{Source: SourceCommand})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Items) != 1 || res.Items[0].DeadLetterID != idCmd {
		t.Fatalf("%+v", res.Items)
	}
}

func TestReplayConcurrentOnlyOneWins(t *testing.T) {
	db := openDB(t)
	if err := EnsureSchema(db); err != nil {
		t.Fatal(err)
	}
	dlqID := uuid.New().String()
	seedDLQ(t, db, dlqID, SourceOutbox, "e", "a", HandlerNoMatch, "", `{"x":1}`)

	const n = 8
	type outcome struct {
		replayed int
		already  int
		err      error
	}
	ch := make(chan outcome, n)
	for range n {
		go func() {
			res, err := ReplayDeadLetters(context.Background(), db, Options{
				Apply: true, Actor: "racer", IDs: []string{dlqID},
			})
			if err != nil {
				ch <- outcome{err: err}
				return
			}
			ch <- outcome{replayed: res.Replayed, already: res.AlreadyReplayed}
		}()
	}
	var totalReplay, totalAlready int
	for range n {
		o := <-ch
		if o.err != nil {
			// SQLITE_BUSY under concurrent writers is acceptable; not a correctness bug.
			if !containsErrBusy(o.err) {
				t.Fatalf("unexpected error: %v", o.err)
			}
			continue
		}
		totalReplay += o.replayed
		totalAlready += o.already
	}
	if totalReplay != 1 {
		t.Fatalf("replayed sum=%d already=%d, want exactly one winner", totalReplay, totalAlready)
	}
	var nOutbox int
	if err := db.QueryRow(`SELECT COUNT(*) FROM outbox`).Scan(&nOutbox); err != nil {
		t.Fatal(err)
	}
	if nOutbox != 1 {
		t.Fatalf("outbox rows=%d, want 1", nOutbox)
	}
}

func containsErrBusy(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "busy") || strings.Contains(msg, "locked")
}

func TestDeadletterEnsureSchemaMigrationFromMinimal(t *testing.T) {
	db := openEmptyDB(t)
	// Minimal old table (runtime deadletter.EnsureSchema path).
	if _, err := db.Exec(`
		CREATE TABLE dead_letters (
			id TEXT PRIMARY KEY, source TEXT, event_type TEXT, aggregate_id TEXT,
			handler_type TEXT, outbox_id TEXT, remaining_handlers TEXT, blob TEXT,
			error TEXT, retry_count INTEGER, created_at TIMESTAMP, dead_at TIMESTAMP
		)`); err != nil {
		t.Fatal(err)
	}
	if err := deadletter.EnsureSchema(db, "dead_letters"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`SELECT replayed_at, replayed_by, exported_at FROM dead_letters`); err != nil {
		t.Fatal(err)
	}
	// Second call must be idempotent without text-matching duplicate-column errors.
	if err := deadletter.EnsureSchema(db, "dead_letters"); err != nil {
		t.Fatal(err)
	}
}

func schemaVersion(t *testing.T, db *sql.DB) int {
	t.Helper()
	var v int
	if err := db.QueryRow(`PRAGMA schema_version`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func deadLetterColumns(t *testing.T, db *sql.DB) map[string]bool {
	t.Helper()
	rows, err := db.Query(`PRAGMA table_info(dead_letters)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var cid, notnull, pk int
		var name, ctype string
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			t.Fatal(err)
		}
		out[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func openEmptyDB(t *testing.T) *sql.DB {
	t.Helper()
	f, err := os.CreateTemp("", "replay-*.db")
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
	return db
}

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	db := openEmptyDB(t)
	createEventStoreTables(t, db)
	return db
}

func createEventStoreTables(t *testing.T, db *sql.DB) {
	t.Helper()
	// Full-shaped tables as the application would create — not the old replay stubs.
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
			exported_at TIMESTAMP,
			replayed_at TIMESTAMP,
			replayed_by TEXT
		);
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
		CREATE TABLE async_tasks (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			task_uuid TEXT NOT NULL UNIQUE,
			command_type TEXT NOT NULL,
			command_blob TEXT NOT NULL,
			status TEXT NOT NULL,
			retry_count INTEGER NOT NULL DEFAULT 0,
			max_retries INTEGER NOT NULL DEFAULT 5,
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL,
			last_error TEXT,
			locked_by TEXT,
			locked_at TIMESTAMP,
			next_retry_at TIMESTAMP
		);
	`); err != nil {
		t.Fatal(err)
	}
}

func createMinimalOutboxAndAsync(t *testing.T, db *sql.DB) {
	t.Helper()
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
		CREATE TABLE async_tasks (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			task_uuid TEXT NOT NULL UNIQUE,
			command_type TEXT NOT NULL,
			command_blob TEXT NOT NULL,
			status TEXT NOT NULL,
			retry_count INTEGER NOT NULL DEFAULT 0,
			max_retries INTEGER NOT NULL DEFAULT 5,
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL,
			last_error TEXT,
			locked_by TEXT,
			locked_at TIMESTAMP,
			next_retry_at TIMESTAMP
		);
	`); err != nil {
		t.Fatal(err)
	}
}

func seedOutbox(t *testing.T, db *sql.DB, id, handlers, blob string) {
	t.Helper()
	seedOutboxWithRetry(t, db, id, handlers, blob, 0, time.Now())
}

func seedOutboxWithRetry(t *testing.T, db *sql.DB, id, handlers, blob string, retryCount int, availableAt time.Time) {
	t.Helper()
	now := time.Now()
	if _, err := db.Exec(`
		INSERT INTO outbox (id, event_type, aggregate_id, created_at, available_at, handlers, event_blob, retry_count)
		VALUES (?, 'evt', 'agg', ?, ?, ?, ?, ?)
	`, id, now, availableAt, handlers, blob, retryCount); err != nil {
		t.Fatal(err)
	}
}

func seedDLQ(t *testing.T, db *sql.DB, id, source, eventType, agg, handler, outboxID, blob string) {
	t.Helper()
	seedDLQWithRetry(t, db, id, source, eventType, agg, handler, outboxID, blob, 0)
}

func seedDLQWithRetry(t *testing.T, db *sql.DB, id, source, eventType, agg, handler, outboxID, blob string, retryCount int) {
	t.Helper()
	now := time.Now()
	var oid any
	if outboxID != "" {
		oid = outboxID
	}
	if _, err := db.Exec(`
		INSERT INTO dead_letters (id, source, event_type, aggregate_id, handler_type, outbox_id, remaining_handlers, blob, error, retry_count, created_at, dead_at)
		VALUES (?, ?, ?, ?, ?, ?, '[]', ?, 'err', ?, ?, ?)
	`, id, source, eventType, agg, handler, oid, blob, retryCount, now, now); err != nil {
		t.Fatal(err)
	}
}

func assertReplayed(t *testing.T, db *sql.DB, id, actor string) {
	t.Helper()
	var by string
	var at sql.NullTime
	if err := db.QueryRow(`SELECT replayed_by, replayed_at FROM dead_letters WHERE id = ?`, id).Scan(&by, &at); err != nil {
		t.Fatal(err)
	}
	if by != actor || !at.Valid {
		t.Fatalf("replay audit by=%s at=%v", by, at)
	}
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}
