package maintenance

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/vercly/eh-sqlite/internal/deadletter"

	_ "github.com/mattn/go-sqlite3"
)

func TestCleanupAsyncTaskRetention(t *testing.T) {
	db := newMaintenanceTestDB(t)
	createAsyncTasksTable(t, db)
	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	insertAsyncTask(t, db, "completed", now.Add(-48*time.Hour))
	insertAsyncTask(t, db, "completed", now.Add(-time.Hour))
	insertAsyncTask(t, db, "failed_permanent", now.Add(-40*24*time.Hour))
	insertAsyncTask(t, db, "failed_permanent", now.Add(-time.Hour))
	insertAsyncTask(t, db, "failed_retriable", now.Add(-40*24*time.Hour))
	insertAsyncTask(t, db, "processing", now.Add(-40*24*time.Hour))

	result, err := Cleanup(context.Background(), db, WithNow(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	if result.AsyncCompletedDeleted != 1 {
		t.Fatalf("completed deleted = %d, want 1", result.AsyncCompletedDeleted)
	}
	if result.AsyncFailedPermanentDeleted != 1 {
		t.Fatalf("failed permanent deleted = %d, want 1", result.AsyncFailedPermanentDeleted)
	}
	assertAsyncTaskCount(t, db, "completed", 1)
	assertAsyncTaskCount(t, db, "failed_permanent", 1)
	assertAsyncTaskCount(t, db, "failed_retriable", 1)
	assertAsyncTaskCount(t, db, "processing", 1)
}

func TestCleanupDeadLettersRequiresOldExportedAt(t *testing.T) {
	db := newMaintenanceTestDB(t)
	if err := deadletter.EnsureSchema(db, "dead_letters"); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	insertDeadLetter(t, db, "old-exported", "outbox", now.Add(-40*24*time.Hour), sql.NullTime{Time: now.Add(-40 * 24 * time.Hour), Valid: true})
	insertDeadLetter(t, db, "old-unexported", "outbox", now.Add(-40*24*time.Hour), sql.NullTime{})
	insertDeadLetter(t, db, "new-exported", "command", now.Add(-40*24*time.Hour), sql.NullTime{Time: now.Add(-time.Hour), Valid: true})

	result, err := Cleanup(context.Background(), db, WithNow(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	if result.DeadLettersDeleted != 1 {
		t.Fatalf("dead letters deleted = %d, want 1", result.DeadLettersDeleted)
	}
	assertDeadLetterExists(t, db, "old-exported", false)
	assertDeadLetterExists(t, db, "old-unexported", true)
	assertDeadLetterExists(t, db, "new-exported", true)
}

func TestStatsSnapshot(t *testing.T) {
	db := newMaintenanceTestDB(t)
	createOutboxTable(t, db)
	createAsyncTasksTable(t, db)
	if err := deadletter.EnsureSchema(db, "dead_letters"); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	insertOutboxRow(t, db, "pending-future", now.Add(-2*time.Hour), now.Add(time.Hour), 0)
	insertOutboxRow(t, db, "due-retry", now.Add(-3*time.Hour), now.Add(-30*time.Minute), 2)
	insertAsyncTask(t, db, "completed", now)
	insertAsyncTask(t, db, "failed_retriable", now)
	insertDeadLetter(t, db, "dl-outbox", "outbox", now, sql.NullTime{})
	insertDeadLetter(t, db, "dl-command", "command", now, sql.NullTime{})

	snapshot, err := Stats(context.Background(), db, WithNow(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Outbox.PendingRows != 2 {
		t.Fatalf("pending rows = %d, want 2", snapshot.Outbox.PendingRows)
	}
	if snapshot.Outbox.AvailableRows != 1 {
		t.Fatalf("available rows = %d, want 1", snapshot.Outbox.AvailableRows)
	}
	if snapshot.Outbox.RetryRows != 1 || snapshot.Outbox.RetryCountTotal != 2 {
		t.Fatalf("retry stats = (%d, %d), want (1, 2)", snapshot.Outbox.RetryRows, snapshot.Outbox.RetryCountTotal)
	}
	if snapshot.Outbox.OldestPendingAge != 3*time.Hour {
		t.Fatalf("oldest pending age = %s, want 3h", snapshot.Outbox.OldestPendingAge)
	}
	if snapshot.Outbox.OldestAvailableAge != 3*time.Hour {
		t.Fatalf("oldest available age = %s, want 3h", snapshot.Outbox.OldestAvailableAge)
	}
	if snapshot.Outbox.DueLag != 30*time.Minute {
		t.Fatalf("due lag = %s, want 30m", snapshot.Outbox.DueLag)
	}
	if snapshot.DeadLetters.Total != 2 || snapshot.DeadLetters.BySource["outbox"] != 1 || snapshot.DeadLetters.BySource["command"] != 1 {
		t.Fatalf("dead letter stats = %+v, want total 2 by source", snapshot.DeadLetters)
	}
	if snapshot.AsyncTasks["completed"] != 1 || snapshot.AsyncTasks["failed_retriable"] != 1 {
		t.Fatalf("async task stats = %+v, want completed and failed_retriable", snapshot.AsyncTasks)
	}
	for _, severity := range []string{"fatal", "retryable", "unknown"} {
		if _, ok := snapshot.ErrorSeverity[severity]; !ok {
			t.Fatalf("missing error severity %s in %+v", severity, snapshot.ErrorSeverity)
		}
	}
}

func newMaintenanceTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+t.Name()+"?mode=memory&cache=shared&_loc=auto")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	})
	return db
}

func createAsyncTasksTable(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`
		CREATE TABLE async_tasks (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			status TEXT NOT NULL,
			updated_at TIMESTAMP NOT NULL
		)
	`); err != nil {
		t.Fatal(err)
	}
}

func createOutboxTable(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`
		CREATE TABLE outbox (
			id TEXT PRIMARY KEY,
			created_at TIMESTAMP NOT NULL,
			available_at TIMESTAMP NOT NULL,
			retry_count INTEGER NOT NULL DEFAULT 0
		)
	`); err != nil {
		t.Fatal(err)
	}
}

func insertAsyncTask(t *testing.T, db *sql.DB, status string, updatedAt time.Time) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO async_tasks (status, updated_at) VALUES (?, ?)`, status, updatedAt); err != nil {
		t.Fatal(err)
	}
}

func insertOutboxRow(t *testing.T, db *sql.DB, id string, createdAt, availableAt time.Time, retryCount int) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO outbox (id, created_at, available_at, retry_count) VALUES (?, ?, ?, ?)`, id, createdAt, availableAt, retryCount); err != nil {
		t.Fatal(err)
	}
}

func insertDeadLetter(t *testing.T, db *sql.DB, id, source string, deadAt time.Time, exportedAt sql.NullTime) {
	t.Helper()
	if _, err := db.Exec(`
		INSERT INTO dead_letters (id, source, event_type, aggregate_id, handler_type, outbox_id, remaining_handlers, blob, error, retry_count, created_at, dead_at, exported_at)
		VALUES (?, ?, 'event.type', 'aggregate-id', 'handler', NULL, '[]', '{}', 'failed', 0, ?, ?, ?)
	`, id, source, deadAt, deadAt, exportedAt); err != nil {
		t.Fatal(err)
	}
}

func assertAsyncTaskCount(t *testing.T, db *sql.DB, status string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRow(`SELECT COUNT(*) FROM async_tasks WHERE status = ?`, status).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("async_tasks[%s] = %d, want %d", status, got, want)
	}
}

func assertDeadLetterExists(t *testing.T, db *sql.DB, id string, want bool) {
	t.Helper()
	var got int
	if err := db.QueryRow(`SELECT COUNT(*) FROM dead_letters WHERE id = ?`, id).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if (got > 0) != want {
		t.Fatalf("dead letter %s exists = %v, want %v", id, got > 0, want)
	}
}
