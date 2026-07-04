package durable_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vercly/eh-sqlite/middleware/commandhandler/durable"
	"github.com/vercly/eh-sqlite/tracing"
	eh "github.com/vercly/eventhorizon"
	"github.com/vercly/eventhorizon/codec/json"
	"github.com/vercly/eventhorizon/uuid"
)

const testCommandType eh.CommandType = "durable_test_command"
const testAggregateType eh.AggregateType = "durable_test_aggregate"

func init() {
	eh.RegisterCommand(func() eh.Command { return &testCommand{} })
}

type testCommand struct {
	ID uuid.UUID
}

func (c testCommand) AggregateID() uuid.UUID          { return c.ID }
func (c testCommand) AggregateType() eh.AggregateType { return testAggregateType }
func (c testCommand) CommandType() eh.CommandType     { return testCommandType }

type recordingCommandHandler struct {
	mu       sync.Mutex
	attempts int
	err      error
	panicVal any
}

func (h *recordingCommandHandler) HandleCommand(context.Context, eh.Command) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.attempts++
	if h.panicVal != nil {
		panic(h.panicVal)
	}
	return h.err
}

func (h *recordingCommandHandler) Attempts() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.attempts
}

type fatalTestError struct{}

func (fatalTestError) Error() string {
	return "fatal test error"
}

func (fatalTestError) DurableSeverity() durable.ErrorSeverity {
	return durable.SeverityFatal
}

func TestDurableCommandSuccessCompletesWithoutDeadLetter(t *testing.T) {
	db := newDurableTestDB(t)
	handler := &recordingCommandHandler{}
	wrapped := newWrappedHandler(t, db, handler, durable.WithMaxRetries(2), durable.WithRetryBackoff("FIXED:2:1ms"))

	if err := wrapped.HandleCommand(context.Background(), testCommand{ID: uuid.New()}); err != nil {
		t.Fatal(err)
	}

	assertTaskState(t, db, "completed", 0)
	assertAsyncTaskRows(t, db, 1)
	assertDeadLetters(t, db, 0)
	if handler.Attempts() != 1 {
		t.Fatalf("handler attempts = %d, want 1", handler.Attempts())
	}
}

func TestDurableCommandRetriesUntilPermanentDeadLetter(t *testing.T) {
	db := newDurableTestDB(t)
	ctx := context.Background()
	handler := &recordingCommandHandler{err: errors.New("retry me")}
	wrapped := newWrappedHandler(t, db, handler, durable.WithMaxRetries(2), durable.WithRetryBackoff("FIXED:2:1ms"))

	cmd := testCommand{ID: uuid.New()}
	if err := wrapped.HandleCommand(ctx, cmd); err == nil {
		t.Fatal("HandleCommand error = nil, want handler error")
	}
	assertTaskState(t, db, "failed_retriable", 1)

	time.Sleep(2 * time.Millisecond)
	assertSweepProcessed(t, db, wrapped, 1, durable.WithMaxRetries(2), durable.WithRetryBackoff("FIXED:2:1ms"))
	assertTaskState(t, db, "failed_retriable", 2)

	time.Sleep(2 * time.Millisecond)
	assertSweepProcessed(t, db, wrapped, 1, durable.WithMaxRetries(2), durable.WithRetryBackoff("FIXED:2:1ms"))
	assertTaskState(t, db, "failed_permanent", 2)

	if handler.Attempts() != 3 {
		t.Fatalf("handler attempts = %d, want 3", handler.Attempts())
	}
	assertAsyncTaskRows(t, db, 1)
	assertDeadLetters(t, db, 1)
}

func TestDurableSweepReusesExistingTaskRow(t *testing.T) {
	db := newDurableTestDB(t)
	handler := &recordingCommandHandler{}
	wrapped := newWrappedHandler(t, db, handler, durable.WithMaxRetries(2), durable.WithRetryBackoff("FIXED:2:1ms"))
	seedTask(t, db, "failed_retriable", time.Now().Add(-time.Second), sql.NullTime{}, 1, 2)

	assertSweepProcessed(t, db, wrapped, 1, durable.WithMaxRetries(2), durable.WithRetryBackoff("FIXED:2:1ms"))

	assertTaskState(t, db, "completed", 1)
	assertAsyncTaskRows(t, db, 1)
	assertDeadLetters(t, db, 0)
	if handler.Attempts() != 1 {
		t.Fatalf("handler attempts = %d, want 1", handler.Attempts())
	}
}

func TestDurableResumeReusesExistingTaskRow(t *testing.T) {
	db := newDurableTestDB(t)
	handler := &recordingCommandHandler{}
	wrapped := newWrappedHandler(t, db, handler, durable.WithMaxRetries(2), durable.WithRetryBackoff("FIXED:2:1ms"))
	seedTask(t, db, "new", time.Time{}, sql.NullTime{}, 0, 2)

	if err := durable.Resume(context.Background(), db, wrapped, durable.WithMaxRetries(2), durable.WithRetryBackoff("FIXED:2:1ms")); err != nil {
		t.Fatal(err)
	}

	assertTaskState(t, db, "completed", 0)
	assertAsyncTaskRows(t, db, 1)
	assertDeadLetters(t, db, 0)
	if handler.Attempts() != 1 {
		t.Fatalf("handler attempts = %d, want 1", handler.Attempts())
	}
}

func TestDurableSweepRecoversStuckProcessingTask(t *testing.T) {
	db := newDurableTestDB(t)
	handler := &recordingCommandHandler{}
	wrapped := newWrappedHandler(t, db, handler, durable.WithMaxRetries(2), durable.WithRetryBackoff("FIXED:2:1ms"))
	oldLock := sql.NullTime{Time: time.Now().Add(-time.Hour), Valid: true}
	seedTask(t, db, "processing", time.Time{}, oldLock, 0, 2)

	assertSweepProcessed(t, db, wrapped, 1,
		durable.WithMaxRetries(2),
		durable.WithRetryBackoff("FIXED:2:1ms"),
		durable.WithStuckTimeout(time.Minute),
	)

	assertTaskState(t, db, "completed", 0)
	assertAsyncTaskRows(t, db, 1)
	assertDeadLetters(t, db, 0)
	if handler.Attempts() != 1 {
		t.Fatalf("handler attempts = %d, want 1", handler.Attempts())
	}
}

func TestDurableFatalCategorizedErrorBecomesPermanentDeadLetter(t *testing.T) {
	db := newDurableTestDB(t)
	handler := &recordingCommandHandler{err: fatalTestError{}}
	wrapped := newWrappedHandler(t, db, handler, durable.WithMaxRetries(2), durable.WithRetryBackoff("FIXED:2:1ms"))

	if err := wrapped.HandleCommand(context.Background(), testCommand{ID: uuid.New()}); err == nil {
		t.Fatal("HandleCommand error = nil, want fatal error")
	}

	assertTaskState(t, db, "failed_permanent", 0)
	assertAsyncTaskRows(t, db, 1)
	assertDeadLetters(t, db, 1)
	if handler.Attempts() != 1 {
		t.Fatalf("handler attempts = %d, want 1", handler.Attempts())
	}
}

func TestDurableTracingPanicRecoveryBecomesPermanentDeadLetter(t *testing.T) {
	db := newDurableTestDB(t)
	handler := tracing.NewDurableHandler(&recordingCommandHandler{panicVal: "boom"})
	wrapped := newWrappedHandler(t, db, handler, durable.WithMaxRetries(2), durable.WithRetryBackoff("FIXED:2:1ms"))

	if err := wrapped.HandleCommand(context.Background(), testCommand{ID: uuid.New()}); err == nil {
		t.Fatal("HandleCommand error = nil, want panic recovery error")
	}

	assertTaskState(t, db, "failed_permanent", 0)
	assertAsyncTaskRows(t, db, 1)
	assertDeadLetters(t, db, 1)
}

func newWrappedHandler(t testing.TB, db *sql.DB, handler eh.CommandHandler, options ...durable.Option) eh.CommandHandler {
	t.Helper()

	middleware, err := durable.NewMiddleware(db, options...)
	if err != nil {
		t.Fatal(err)
	}
	return eh.UseCommandHandlerMiddleware(handler, middleware)
}

func seedTask(t testing.TB, db *sql.DB, status string, nextRetryAt time.Time, lockedAt sql.NullTime, retryCount, maxRetries int) int64 {
	t.Helper()

	cmd := testCommand{ID: uuid.New()}
	blob, err := (json.CommandCodec{}).MarshalCommand(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(-time.Minute)
	var nextRetry sql.NullTime
	if !nextRetryAt.IsZero() {
		nextRetry = sql.NullTime{Time: nextRetryAt, Valid: true}
	}
	res, err := db.Exec(`
		INSERT INTO async_tasks (task_uuid, command_type, command_blob, status, retry_count, max_retries, created_at, updated_at, locked_by, locked_at, next_retry_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, uuid.New().String(), cmd.CommandType().String(), string(blob), status, retryCount, maxRetries, now, now, "test", lockedAt, nextRetry)
	if err != nil {
		t.Fatal(err)
	}
	taskID, err := res.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	return taskID
}

func assertSweepProcessed(t testing.TB, db *sql.DB, bus eh.CommandHandler, want int, options ...durable.Option) {
	t.Helper()

	processed, err := durable.Sweep(context.Background(), db, bus, options...)
	if err != nil {
		t.Fatal(err)
	}
	if processed != want {
		t.Fatalf("sweep processed = %d, want %d", processed, want)
	}
}

func assertTaskState(t testing.TB, db *sql.DB, wantStatus string, wantRetryCount int) {
	t.Helper()

	var status string
	var retryCount int
	if err := db.QueryRow(`SELECT status, retry_count FROM async_tasks LIMIT 1`).Scan(&status, &retryCount); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus || retryCount != wantRetryCount {
		t.Fatalf("task state = (%s, %d), want (%s, %d)", status, retryCount, wantStatus, wantRetryCount)
	}
}

func assertAsyncTaskRows(t testing.TB, db *sql.DB, want int) {
	t.Helper()

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM async_tasks`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("async task rows = %d, want %d", count, want)
	}
}

func assertDeadLetters(t testing.TB, db *sql.DB, want int) {
	t.Helper()

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM dead_letters WHERE source = 'command'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("command dead letters = %d, want %d", count, want)
	}
}

func newDurableTestDB(t testing.TB) *sql.DB {
	t.Helper()

	f, err := os.CreateTemp("", "durable-*.db")
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
