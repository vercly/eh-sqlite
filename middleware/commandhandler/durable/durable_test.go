package durable

import (
	"context"
	"database/sql"
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

	db, err := sql.Open("sqlite", f.Name()+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
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
