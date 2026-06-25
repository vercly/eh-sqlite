package durable

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	eh "github.com/vercly/eventhorizon"
	ehjson "github.com/vercly/eventhorizon/codec/json"
	"github.com/vercly/eventhorizon/uuid"
)

const resumeTestCmdType eh.CommandType = "DurableResumeTestCommand"

type resumeTestCommand struct {
	ID      uuid.UUID `json:"id"`
	Content string    `json:"content"`
}

func (c *resumeTestCommand) AggregateID() uuid.UUID          { return c.ID }
func (c *resumeTestCommand) AggregateType() eh.AggregateType { return "durable-test-agg" }
func (c *resumeTestCommand) CommandType() eh.CommandType     { return resumeTestCmdType }

func init() {
	eh.RegisterCommand(func() eh.Command { return &resumeTestCommand{} })
}

func newDurableTestDB(t testing.TB) *sql.DB {
	t.Helper()

	f, err := os.CreateTemp("", "durable-*.db")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	db, err := sql.Open("sqlite", f.Name()+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(1)&_loc=auto&_inttotime=1")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		db.Close()
		os.Remove(f.Name())
	})

	return db
}

func countTasks(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM async_tasks`).Scan(&n); err != nil {
		t.Fatalf("could not count tasks: %v", err)
	}
	return n
}

// completionHandler mimics tracing.NewDurableHandler: it pulls the completion
// callback from the context and reports the result of the inner handler.
func completionHandler(inner eh.CommandHandler) eh.CommandHandler {
	return eh.CommandHandlerFunc(func(ctx context.Context, cmd eh.Command) error {
		err := inner.HandleCommand(ctx, cmd)
		if f, ok := GetCompletionFunc(ctx); ok {
			if err != nil {
				f("failed_permanent", err)
			} else {
				f("completed", nil)
			}
		}
		return err
	})
}

// TestMiddlewareInsertsOnce verifies the normal path persists exactly one row and
// marks it completed when the handler succeeds.
func TestMiddlewareInsertsOnce(t *testing.T) {
	db := newDurableTestDB(t)

	mwFactory, err := NewMiddleware(db)
	if err != nil {
		t.Fatal(err)
	}

	final := eh.CommandHandlerFunc(func(ctx context.Context, cmd eh.Command) error { return nil })
	bus := eh.UseCommandHandlerMiddleware(completionHandler(final), mwFactory)

	cmd := &resumeTestCommand{ID: uuid.New(), Content: "hello"}
	if err := bus.HandleCommand(context.Background(), cmd); err != nil {
		t.Fatalf("handle command: %v", err)
	}

	if n := countTasks(t, db); n != 1 {
		t.Fatalf("expected exactly 1 task row, got %d", n)
	}

	var status string
	if err := db.QueryRow(`SELECT status FROM async_tasks LIMIT 1`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("expected status 'completed', got %q", status)
	}
}

// TestResumeReusesRow is the core Phase 1 assertion: resuming a persisted task
// must NOT insert a duplicate async_tasks row; it must complete the original one.
func TestResumeReusesRow(t *testing.T) {
	db := newDurableTestDB(t)

	mwFactory, err := NewMiddleware(db)
	if err != nil {
		t.Fatal(err)
	}

	// Track that the resumed command actually reaches the inner handler.
	var dispatched int
	final := eh.CommandHandlerFunc(func(ctx context.Context, cmd eh.Command) error {
		dispatched++
		return nil
	})
	bus := eh.UseCommandHandlerMiddleware(completionHandler(final), mwFactory)

	// Simulate a row left behind by a crash: a 'new' task with a serialized command.
	ctx := context.Background()
	cmd := &resumeTestCommand{ID: uuid.New(), Content: "recover-me"}
	blob, err := (ehjson.CommandCodec{}).MarshalCommand(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	res, err := db.Exec(
		`INSERT INTO async_tasks (task_uuid, command_type, command_blob, status, created_at, updated_at) VALUES (?, ?, ?, 'new', ?, ?)`,
		uuid.New().String(), string(resumeTestCmdType), string(blob), now, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	origID, _ := res.LastInsertId()

	if n := countTasks(t, db); n != 1 {
		t.Fatalf("setup: expected 1 row, got %d", n)
	}

	// Resume through the wrapped bus (which contains the durable middleware).
	if err := Resume(ctx, db, bus); err != nil {
		t.Fatalf("resume: %v", err)
	}

	if dispatched != 1 {
		t.Fatalf("expected resumed command to reach handler once, got %d", dispatched)
	}

	// No duplicate row may be created.
	if n := countTasks(t, db); n != 1 {
		t.Fatalf("resume must not duplicate rows: expected 1, got %d", n)
	}

	// The original row must be the one updated to completed.
	var status string
	if err := db.QueryRow(`SELECT status FROM async_tasks WHERE id = ?`, origID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("expected original row status 'completed', got %q", status)
	}
}

// --- Phase 4 (Path A): durable retry semantics ---

type permanentErr struct{ msg string }

func (e permanentErr) Error() string                 { return e.msg }
func (e permanentErr) DurableSeverity() ErrorSeverity { return SeverityPermanent }

// classifyingHandler mirrors tracing.NewDurableHandler: it classifies the inner
// handler's error and reports the matching durable status via the completion func.
func classifyingHandler(inner eh.CommandHandler) eh.CommandHandler {
	return eh.CommandHandlerFunc(func(ctx context.Context, cmd eh.Command) error {
		err := inner.HandleCommand(ctx, cmd)
		if f, ok := GetCompletionFunc(ctx); ok {
			switch {
			case err == nil:
				f(StatusCompleted, nil)
			case ClassifyError(err) == SeverityPermanent:
				f(StatusFailedPermanent, err)
			default:
				f(StatusFailedRetriable, err)
			}
		}
		return err
	})
}

func failHandler(err error) eh.CommandHandler {
	return eh.CommandHandlerFunc(func(ctx context.Context, cmd eh.Command) error { return err })
}

func getTask(t *testing.T, db *sql.DB, id int64) (status string, retryCount int, nextRetryValid bool) {
	t.Helper()
	var nra sql.NullTime
	if err := db.QueryRow(`SELECT status, retry_count, next_retry_at FROM async_tasks WHERE id = ?`, id).
		Scan(&status, &retryCount, &nra); err != nil {
		t.Fatalf("could not read task %d: %v", id, err)
	}
	return status, retryCount, nra.Valid
}

func onlyTaskID(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(`SELECT id FROM async_tasks LIMIT 1`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func insertRetriableTask(t *testing.T, db *sql.DB, retryCount int, nextRetryAt time.Time) int64 {
	t.Helper()
	cmd := &resumeTestCommand{ID: uuid.New(), Content: "retry-me"}
	blob, err := (ehjson.CommandCodec{}).MarshalCommand(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	res, err := db.Exec(
		`INSERT INTO async_tasks (task_uuid, command_type, command_blob, status, retry_count, max_retries, created_at, updated_at, next_retry_at)
		 VALUES (?, ?, ?, 'failed_retriable', ?, 5, ?, ?, ?)`,
		uuid.New().String(), string(resumeTestCmdType), string(blob), retryCount, now, now, nextRetryAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	return id
}

func TestClassifyError(t *testing.T) {
	if got := ClassifyError(errors.New("plain")); got != SeverityRetriable {
		t.Errorf("plain error should default to retriable, got %v", got)
	}
	if got := ClassifyError(permanentErr{"no"}); got != SeverityPermanent {
		t.Errorf("CategorizedError should be permanent, got %v", got)
	}
	if got := ClassifyError(fmt.Errorf("wrap: %w", permanentErr{"no"})); got != SeverityPermanent {
		t.Errorf("wrapped CategorizedError should be permanent, got %v", got)
	}
}

// TestRetriableFailureSchedulesRetry: a default (unclassified) failure moves the
// task to failed_retriable, bumps retry_count, and sets next_retry_at.
func TestRetriableFailureSchedulesRetry(t *testing.T) {
	db := newDurableTestDB(t)
	mw, err := NewMiddleware(db, WithRetryBackoff(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	bus := eh.UseCommandHandlerMiddleware(classifyingHandler(failHandler(errors.New("transient"))), mw)

	_ = bus.HandleCommand(context.Background(), &resumeTestCommand{ID: uuid.New(), Content: "x"})

	if n := countTasks(t, db); n != 1 {
		t.Fatalf("expected 1 row, got %d", n)
	}
	status, rc, nraValid := getTask(t, db, onlyTaskID(t, db))
	if status != StatusFailedRetriable {
		t.Fatalf("expected status %q, got %q", StatusFailedRetriable, status)
	}
	if rc != 1 {
		t.Fatalf("expected retry_count 1, got %d", rc)
	}
	if !nraValid {
		t.Fatal("expected next_retry_at to be set")
	}
}

// TestPermanentFailureNotRetried: a SeverityPermanent error fails permanently with
// no retry scheduling.
func TestPermanentFailureNotRetried(t *testing.T) {
	db := newDurableTestDB(t)
	mw, err := NewMiddleware(db)
	if err != nil {
		t.Fatal(err)
	}
	bus := eh.UseCommandHandlerMiddleware(classifyingHandler(failHandler(permanentErr{"fatal"})), mw)

	_ = bus.HandleCommand(context.Background(), &resumeTestCommand{ID: uuid.New(), Content: "x"})

	status, rc, nraValid := getTask(t, db, onlyTaskID(t, db))
	if status != StatusFailedPermanent {
		t.Fatalf("expected status %q, got %q", StatusFailedPermanent, status)
	}
	if rc != 0 {
		t.Fatalf("expected retry_count 0, got %d", rc)
	}
	if nraValid {
		t.Fatal("expected next_retry_at to remain unset")
	}
}

// TestResumePicksDueRetriableAndCompletes: a due failed_retriable task is resumed,
// reuses its row, and completes on success.
func TestResumePicksDueRetriableAndCompletes(t *testing.T) {
	db := newDurableTestDB(t)
	mw, err := NewMiddleware(db)
	if err != nil {
		t.Fatal(err)
	}
	bus := eh.UseCommandHandlerMiddleware(classifyingHandler(failHandler(nil)), mw) // nil error => success

	id := insertRetriableTask(t, db, 1, time.Now().Add(-time.Minute)) // due

	if err := Resume(context.Background(), db, bus); err != nil {
		t.Fatal(err)
	}

	if n := countTasks(t, db); n != 1 {
		t.Fatalf("resume must not duplicate rows, got %d", n)
	}
	status, rc, _ := getTask(t, db, id)
	if status != StatusCompleted {
		t.Fatalf("expected status %q, got %q", StatusCompleted, status)
	}
	if rc != 1 {
		t.Fatalf("expected retry_count to stay 1, got %d", rc)
	}
}

// TestRetryExhaustionBecomesPermanent: failing again at retry_count == max-1 flips
// the task to failed_permanent and clears next_retry_at.
func TestRetryExhaustionBecomesPermanent(t *testing.T) {
	db := newDurableTestDB(t)
	mw, err := NewMiddleware(db)
	if err != nil {
		t.Fatal(err)
	}
	bus := eh.UseCommandHandlerMiddleware(classifyingHandler(failHandler(errors.New("still failing"))), mw)

	id := insertRetriableTask(t, db, 4, time.Now().Add(-time.Minute)) // max_retries=5, due

	if err := Resume(context.Background(), db, bus); err != nil {
		t.Fatal(err)
	}

	status, rc, nraValid := getTask(t, db, id)
	if status != StatusFailedPermanent {
		t.Fatalf("expected status %q, got %q", StatusFailedPermanent, status)
	}
	if rc != 5 {
		t.Fatalf("expected retry_count 5, got %d", rc)
	}
	if nraValid {
		t.Fatal("expected next_retry_at cleared on exhaustion")
	}
}

// TestResumeSkipsNotDueRetriable: a retriable task whose next_retry_at is in the
// future is not resumed.
func TestResumeSkipsNotDueRetriable(t *testing.T) {
	db := newDurableTestDB(t)
	mw, err := NewMiddleware(db)
	if err != nil {
		t.Fatal(err)
	}
	called := 0
	counting := eh.CommandHandlerFunc(func(ctx context.Context, cmd eh.Command) error {
		called++
		return nil
	})
	bus := eh.UseCommandHandlerMiddleware(classifyingHandler(counting), mw)

	id := insertRetriableTask(t, db, 1, time.Now().Add(time.Hour)) // not due

	if err := Resume(context.Background(), db, bus); err != nil {
		t.Fatal(err)
	}

	if called != 0 {
		t.Fatalf("a not-due retriable task must not be resumed, handler called %d times", called)
	}
	status, _, _ := getTask(t, db, id)
	if status != StatusFailedRetriable {
		t.Fatalf("expected status to stay %q, got %q", StatusFailedRetriable, status)
	}
}
