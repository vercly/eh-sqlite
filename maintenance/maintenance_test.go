package maintenance

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/vercly/eh-sqlite/internal/deadletter"
	"github.com/vercly/eh-sqlite/schema"

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

func TestStatsSnapshotV2KeepsPublicationAndDeliveryDepthsDistinct(t *testing.T) {
	db := newMaintenanceTestDB(t)
	createOutboxV2Tables(t, db)
	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	insertOutboxV2Publication(t, db, "publication-a", now.Add(-4*time.Hour))
	insertOutboxV2Publication(t, db, "publication-b", now.Add(-2*time.Hour))
	insertOutboxV2Delivery(t, db, "delivery-a1", "publication-a", "handler-a", "handler-a", now.Add(-4*time.Hour), now.Add(-3*time.Hour), sql.NullTime{}, 2, sql.NullTime{})
	insertOutboxV2Delivery(t, db, "delivery-a2", "publication-a", "handler-b", "handler-b", now.Add(-4*time.Hour), now.Add(-time.Hour), sql.NullTime{Time: now.Add(-30 * time.Minute), Valid: true}, 0, sql.NullTime{})
	insertOutboxV2Delivery(t, db, "delivery-b1", "publication-b", "handler-c", "", now.Add(-2*time.Hour), now.Add(-90*time.Minute), sql.NullTime{}, 0, sql.NullTime{Time: now.Add(-time.Hour), Valid: true})
	insertOutboxV2Delivery(t, db, "delivery-b2", "publication-b", "", "", now.Add(-2*time.Hour), now.Add(-30*time.Minute), sql.NullTime{}, 0, sql.NullTime{})

	snapshot, err := Stats(context.Background(), db, WithNow(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	got := snapshot.Outbox
	if got.PendingRows != 2 || got.AvailableRows != 2 || got.RetryRows != 1 || got.RetryCountTotal != 2 {
		t.Fatalf("publication stats = %+v, want pending=2 available=2 retry=1 retry_total=2", got)
	}
	if got.DeliveryRows != 4 || got.DeliveryAvailableRows != 1 || got.DeliveryInFlightRows != 1 || got.DeliveryUnresolved != 1 || got.DeliveryRematch != 1 {
		t.Fatalf("delivery stats = %+v, want rows=4 available=1 inflight=1 unresolved=1 rematch=1", got)
	}
	if got.OldestPendingAge != 4*time.Hour || got.OldestAvailableAge != 4*time.Hour || got.DueLag != 3*time.Hour || got.AdmissionWaitAge != 3*time.Hour {
		t.Fatalf("age stats = %+v", got)
	}
}

func TestStatsPrefersV1BacklogUntilV2MigrationIsApplied(t *testing.T) {
	db := newMaintenanceTestDB(t)
	createOutboxTable(t, db)
	createOutboxV2Tables(t, db)
	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	insertOutboxRow(t, db, "v1-pending", now.Add(-2*time.Hour), now.Add(-time.Hour), 1)

	snapshot, err := Stats(context.Background(), db, WithNow(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Outbox.PendingRows != 1 || snapshot.Outbox.AvailableRows != 1 || snapshot.Outbox.RetryRows != 1 {
		t.Fatalf("outbox stats = %+v, want pending v1 backlog", snapshot.Outbox)
	}
	if snapshot.Outbox.DeliveryRows != 0 {
		t.Fatalf("delivery rows = %d, want v1 stats before migration", snapshot.Outbox.DeliveryRows)
	}
}

func TestCleanupDeletesOnlyOrphanV2Publications(t *testing.T) {
	db := newMaintenanceTestDB(t)
	createOutboxV2Tables(t, db)
	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	insertOutboxV2Publication(t, db, "active", now)
	insertOutboxV2Publication(t, db, "orphan", now)
	insertOutboxV2Delivery(t, db, "active-delivery", "active", "handler", "handler", now, now, sql.NullTime{}, 0, sql.NullTime{})

	result, err := Cleanup(context.Background(), db, WithNow(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	if result.OrphanPublicationsDeleted != 1 {
		t.Fatalf("orphan publications deleted = %d, want 1", result.OrphanPublicationsDeleted)
	}
	var remaining int
	if err := db.QueryRow(`SELECT COUNT(*) FROM outbox_publications WHERE publication_id = 'active'`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 1 {
		t.Fatalf("active publications = %d, want 1", remaining)
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

func createOutboxV2Tables(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`
		CREATE TABLE outbox_publications (
			publication_id TEXT PRIMARY KEY,
			created_at TIMESTAMP NOT NULL
		);
		CREATE TABLE outbox_deliveries (
			id TEXT PRIMARY KEY,
			publication_id TEXT NOT NULL REFERENCES outbox_publications(publication_id),
			handler_type TEXT,
			dispatch_key TEXT,
			created_at TIMESTAMP NOT NULL,
			available_at TIMESTAMP NOT NULL,
			taken_at TIMESTAMP,
			retry_count INTEGER NOT NULL DEFAULT 0,
			unresolved_at TIMESTAMP
		)
	`); err != nil {
		t.Fatal(err)
	}
}

func insertOutboxV2Publication(t *testing.T, db *sql.DB, id string, createdAt time.Time) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO outbox_publications (publication_id, created_at) VALUES (?, ?)`, id, createdAt); err != nil {
		t.Fatal(err)
	}
}

func insertOutboxV2Delivery(t *testing.T, db *sql.DB, id, publicationID, handlerType, dispatchKey string, createdAt, availableAt time.Time, takenAt sql.NullTime, retryCount int, unresolvedAt sql.NullTime) {
	t.Helper()
	var handler, key any = handlerType, dispatchKey
	if handlerType == "" {
		handler = nil
	}
	if dispatchKey == "" {
		key = nil
	}
	if _, err := db.Exec(`
		INSERT INTO outbox_deliveries
			(id, publication_id, handler_type, dispatch_key, created_at, available_at, taken_at, retry_count, unresolved_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, id, publicationID, handler, key, createdAt, availableAt, takenAt, retryCount, unresolvedAt); err != nil {
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

// TestStatsParsesStoredTimestampSpellings checks that Stats reads MIN(...)
// values back through schema.ParseStored: canonical UTC text and the "Z"
// spelling must both resolve to the same instant.
func TestStatsParsesStoredTimestampSpellings(t *testing.T) {
	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	oldest := now.Add(-3 * time.Hour)

	cases := []struct {
		name string
		text string
	}{
		{"canonical", schema.FormatStored(oldest)},
		{"zulu", oldest.Format("2006-01-02 15:04:05.999999999Z")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := newMaintenanceTestDB(t)
			createOutboxTable(t, db)
			createAsyncTasksTable(t, db)
			if err := deadletter.EnsureSchema(db, "dead_letters"); err != nil {
				t.Fatal(err)
			}
			// Raw text insert: the stored spelling is exactly tc.text.
			if _, err := db.Exec(
				`INSERT INTO outbox (id, created_at, available_at, retry_count) VALUES ('raw', ?, ?, 0)`,
				tc.text, tc.text); err != nil {
				t.Fatal(err)
			}

			snapshot, err := Stats(context.Background(), db, WithNow(func() time.Time { return now }))
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.Outbox.OldestPendingAge != 3*time.Hour {
				t.Fatalf("oldest pending age = %s, want 3h", snapshot.Outbox.OldestPendingAge)
			}
			if snapshot.Outbox.DueLag != 3*time.Hour {
				t.Fatalf("due lag = %s, want 3h", snapshot.Outbox.DueLag)
			}
		})
	}
}

func TestParseSQLiteTimeUsesSchemaParseStored(t *testing.T) {
	want := time.Date(2025, 3, 4, 10, 30, 0, 500000000, time.UTC)
	for _, text := range []string{
		"2025-03-04 10:30:00.5+00:00",
		"2025-03-04 10:30:00.5Z",
		"2025-03-04T10:30:00.5Z",
		"2025-03-04 12:30:00.5+02:00",
	} {
		got, err := parseSQLiteTime(text)
		if err != nil {
			t.Fatalf("parseSQLiteTime(%q) error = %v", text, err)
		}
		if !got.Valid || !got.Time.Equal(want) {
			t.Fatalf("parseSQLiteTime(%q) = %v, want %v", text, got.Time, want)
		}
	}
	if _, err := parseSQLiteTime("not a timestamp"); !errors.Is(err, schema.ErrTimestampAmbiguous) {
		t.Fatalf("parseSQLiteTime error = %v, want ErrTimestampAmbiguous", err)
	}
}
