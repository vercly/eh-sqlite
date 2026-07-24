package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	eh "github.com/vercly/eventhorizon"
	"github.com/vercly/eventhorizon/mocks"
	"github.com/vercly/eventhorizon/uuid"
)

func TestOutboxStartResetsTakenAt(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := mocks.NewEventHandler("reset_handler")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	id := uuid.New().String()
	// Fresh taken_at would block processBatch without Start reset / sweep age.
	seedOutboxEventWithID(t, db, o, id, newTestEvent("reset"), []string{handler.Type}, time.Now(), time.Now(), sql.NullTime{Time: time.Now(), Valid: true})

	// Start must initial-sweep after reset without an external notify.
	if err := o.StartChecked(); err != nil {
		t.Fatal(err)
	}

	if !handler.Wait(3 * time.Second) {
		t.Fatal("handler should run after Start clears taken_at")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if outboxRowCount(t, db) == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := outboxRowCount(t, db); got != 0 {
		t.Fatalf("outbox rows after Start reset = %d, want 0", got)
	}
}

func TestOutboxAddHandlerAfterStartFails(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, mocks.NewEventHandler("before")); err != nil {
		t.Fatal(err)
	}
	if err := o.StartChecked(); err != nil {
		t.Fatal(err)
	}
	err = o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, mocks.NewEventHandler("after"))
	if !errors.Is(err, ErrOutboxAlreadyStarted) {
		t.Fatalf("error = %v, want ErrOutboxAlreadyStarted", err)
	}
}

// TestOutboxStartCheckedFailClosedOnResetAbort proves D1 fail-closed startup:
// reset failure returns error, fetcher never runs, registration stays closed,
// and a later StartChecked after removing the fault succeeds.
func TestOutboxStartCheckedFailClosedOnResetAbort(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := mocks.NewEventHandler("reset_abort_handler")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	id := uuid.New().String()
	seedOutboxEventWithID(t, db, o, id, newTestEvent("reset-abort"), []string{handler.Type},
		time.Now().Add(-time.Minute), time.Now().Add(-time.Minute),
		sql.NullTime{Time: time.Now(), Valid: true})

	if _, err := db.Exec(`
		CREATE TRIGGER abort_taken_at_reset
		BEFORE UPDATE OF taken_at ON outbox
		WHEN NEW.taken_at IS NULL AND OLD.taken_at IS NOT NULL
		BEGIN
			SELECT RAISE(ABORT, 'forced reset abort');
		END
	`); err != nil {
		t.Fatal(err)
	}

	err = o.StartChecked()
	if err == nil {
		t.Fatal("StartChecked error = nil, want reset abort")
	}
	if !strings.Contains(err.Error(), "forced reset abort") && !strings.Contains(err.Error(), "reset outbox taken_at") {
		t.Fatalf("StartChecked error = %v, want reset abort", err)
	}
	if o.processorRunning.Load() {
		t.Fatal("processorRunning = true after failed StartChecked, want false")
	}
	if !o.registrationClosed.Load() {
		t.Fatal("registrationClosed = false after failed StartChecked, want true")
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, mocks.NewEventHandler("late")); !errors.Is(err, ErrOutboxAlreadyStarted) {
		t.Fatalf("AddHandler after failed Start = %v, want ErrOutboxAlreadyStarted", err)
	}
	// Publish must stay blocked until a successful StartChecked (processor up).
	if err := o.HandleEvent(ctx, newTestEvent("before-retry")); !errors.Is(err, ErrOutboxNotStarted) {
		t.Fatalf("HandleEvent after failed Start = %v, want ErrOutboxNotStarted", err)
	}
	if handler.Wait(50 * time.Millisecond) {
		t.Fatal("handler must not run when fetcher did not start")
	}
	if got := outboxRowCount(t, db); got != 1 {
		t.Fatalf("outbox rows = %d, want 1 (unprocessed after failed start)", got)
	}

	if _, err := db.Exec(`DROP TRIGGER abort_taken_at_reset`); err != nil {
		t.Fatal(err)
	}

	if err := o.StartChecked(); err != nil {
		t.Fatalf("retry StartChecked after dropping trigger: %v", err)
	}
	if !o.processorRunning.Load() {
		t.Fatal("processorRunning = false after successful retry")
	}
	// Publish works only after successful retry.
	if err := o.HandleEvent(ctx, newTestEvent("after-retry")); err != nil {
		t.Fatalf("HandleEvent after successful retry: %v", err)
	}
	if !handler.Wait(3 * time.Second) {
		t.Fatal("handler should run after successful StartChecked retry")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if outboxRowCount(t, db) == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := outboxRowCount(t, db); got != 0 {
		t.Fatalf("outbox rows after retry = %d, want 0", got)
	}
}

// TestOutboxAdmissionSkipsActiveAndClaimsIdle ensures SELECT is wide enough to
// skip already-admitted rows and still claim a later idle record (no starvation).
func TestOutboxAdmissionSkipsActiveAndClaimsIdle(t *testing.T) {
	restoreSweepAge := setPeriodicSweepAge(t, 20*time.Millisecond)
	defer restoreSweepAge()

	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithAdmissionLimit(2))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := mocks.NewEventHandler("skip_active_handler")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	activeID := uuid.New().String()
	idleID := uuid.New().String()
	// Active row is older (first in FIFO) and looks reclaimable via taken_at age.
	old := time.Now().Add(-time.Minute)
	seedOutboxEventWithID(t, db, o, activeID, newTestEvent("active"), []string{handler.Type},
		old, old, sql.NullTime{Time: time.Now().Add(-time.Hour), Valid: true})
	seedOutboxEventWithID(t, db, o, idleID, newTestEvent("idle"), []string{handler.Type},
		old.Add(time.Second), old.Add(time.Second), sql.NullTime{})

	// Process-local: active is already admitted (long-running coordinator).
	if !o.admission.tryAdmit(activeID) {
		t.Fatal("pre-admit active id")
	}

	processed, err := o.processBatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("processed = %d, want 1 (idle only)", processed)
	}
	if !handler.Wait(time.Second) {
		t.Fatal("idle event was not dispatched")
	}
	// Active row must remain; idle removed.
	var remaining string
	if err := db.QueryRow(`SELECT id FROM outbox`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != activeID {
		t.Fatalf("remaining outbox id = %s, want active %s", remaining, activeID)
	}
	if got := outboxRowCount(t, db); got != 1 {
		t.Fatalf("outbox rows = %d, want 1", got)
	}
}

func TestOutboxPartialAdmissionClaimsOnlyCapacity(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithAdmissionLimit(1))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	// Blocking handler keeps the single admission slot occupied.
	release := make(chan struct{})
	entered := make(chan string, 1)
	handler := newBlockingHandler("partial_handler", entered, release)
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	ids := []string{uuid.New().String(), uuid.New().String(), uuid.New().String()}
	for _, id := range ids {
		seedOutboxEventWithID(t, db, o, id, newTestEvent(id), []string{handler.Type}, createdAt, createdAt, sql.NullTime{})
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := o.processBatch(ctx); err != nil {
			t.Errorf("processBatch: %v", err)
		}
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first handler entry")
	}

	// While the first record is admitted, a second claim must not take more rows.
	processed, err := o.processBatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 0 {
		t.Fatalf("second processBatch processed = %d, want 0 (admission full)", processed)
	}

	var claimed int
	if err := db.QueryRow(`SELECT COUNT(*) FROM outbox WHERE taken_at IS NOT NULL`).Scan(&claimed); err != nil {
		t.Fatal(err)
	}
	if claimed != 1 {
		t.Fatalf("claimed rows = %d, want 1", claimed)
	}
	var free int
	if err := db.QueryRow(`SELECT COUNT(*) FROM outbox WHERE taken_at IS NULL`).Scan(&free); err != nil {
		t.Fatal(err)
	}
	if free != 2 {
		t.Fatalf("unclaimed rows = %d, want 2", free)
	}

	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("processBatch did not finish")
	}
}

func TestOutboxAdmissionPreventsDuplicateWhileHandlerExceedsSweepAge(t *testing.T) {
	restoreSweepAge := setPeriodicSweepAge(t, 30*time.Millisecond)
	defer restoreSweepAge()

	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithMaxGoroutines(2))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	var invocations atomic.Int64
	release := make(chan struct{})
	handler := &countingBlockHandler{
		Type:        "long_handler",
		entered:     make(chan struct{}, 8),
		release:     release,
		invocations: &invocations,
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	id := uuid.New().String()
	createdAt := time.Now().Add(-time.Minute)
	seedOutboxEventWithID(t, db, o, id, newTestEvent("long"), []string{handler.Type}, createdAt, createdAt, sql.NullTime{})

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := o.processBatch(ctx); err != nil {
			t.Errorf("processBatch: %v", err)
		}
	}()

	select {
	case <-handler.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not enter")
	}

	// Wait past PeriodicSweepAge so SQL would consider taken_at reclaimable.
	time.Sleep(3 * PeriodicSweepAge)

	processed, err := o.processBatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 0 {
		t.Fatalf("concurrent processBatch processed = %d, want 0 (still admitted)", processed)
	}
	if got := invocations.Load(); got != 1 {
		t.Fatalf("handler invocations = %d, want 1 (no duplicate while in-flight)", got)
	}

	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("processBatch did not finish")
	}
	if got := invocations.Load(); got != 1 {
		t.Fatalf("handler invocations after complete = %d, want 1", got)
	}
}

func TestOutboxNewMessageDuringBatchIsProcessedAfter(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithAdmissionLimit(1))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	release := make(chan struct{})
	entered := make(chan string, 2)
	handler := newBlockingHandler("batch_handler", entered, release)
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	firstID := uuid.New().String()
	createdAt := time.Now().Add(-time.Minute)
	seedOutboxEventWithID(t, db, o, firstID, newTestEvent("first"), []string{handler.Type}, createdAt, createdAt, sql.NullTime{})

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := o.processBatch(ctx); err != nil {
			t.Errorf("first processBatch: %v", err)
		}
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first event did not enter handler")
	}

	// Insert a new due message while the batch is in-flight.
	secondID := uuid.New().String()
	seedOutboxEventWithID(t, db, o, secondID, newTestEvent("second"), []string{handler.Type}, createdAt, createdAt, sql.NullTime{})

	// Concurrent claim during batch must not steal capacity incorrectly.
	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 0 {
		t.Fatalf("mid-batch processBatch = %d, want 0 with capacity 1", processed)
	}

	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("first batch did not finish")
	}

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("second processBatch = %d, want 1 for new message", processed)
	}
	if got := outboxRowCount(t, db); got != 0 {
		t.Fatalf("outbox rows = %d, want 0 after draining new message", got)
	}
}

func TestOutboxCloseWaitsForInFlightBatch(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}

	release := make(chan struct{})
	entered := make(chan string, 1)
	handler := newBlockingHandler("shutdown_handler", entered, release)
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	seedOutboxEventWithID(t, db, o, uuid.New().String(), newTestEvent("shutdown"), []string{handler.Type}, createdAt, createdAt, sql.NullTime{})

	// Drive via Start so Close waits on the processor goroutine.
	// Initial sweep after Start must enter the handler without notify.
	if err := o.StartChecked(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start before Close")
	}

	closed := make(chan error, 1)
	go func() {
		closed <- o.Close()
	}()

	// Close must not return while the handler is still running.
	select {
	case err := <-closed:
		t.Fatalf("Close returned early while handler in-flight: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)

	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after handler finished")
	}

	if got := outboxRowCount(t, db); got != 0 {
		t.Fatalf("outbox rows after Close = %d, want 0", got)
	}
}

// TestOutboxCrashRestartStartupReset is a real subprocess E2E: the child admits
// and handles an event, signals readiness, then hangs until SIGKILL. The parent
// reopens the same DB, registers handlers, Start()s (taken_at reset), and
// asserts the event is redelivered exactly once to the new process.
func TestOutboxCrashRestartStartupReset(t *testing.T) {
	if os.Getenv("EH_SQLITE_CRASH_CHILD") == "1" {
		runCrashChild(t)
		return
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "crash.db")
	readyPath := filepath.Join(dir, "ready")

	cmd := exec.Command(os.Args[0], "-test.run=^TestOutboxCrashRestartStartupReset$", "-test.v")
	cmd.Env = append(os.Environ(),
		"EH_SQLITE_CRASH_CHILD=1",
		"EH_SQLITE_CRASH_DB="+dbPath,
		"EH_SQLITE_CRASH_READY="+readyPath,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}

	// Wait for child to claim and enter the handler.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(readyPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			t.Fatal("timed out waiting for child ready signal")
		}
		time.Sleep(20 * time.Millisecond)
	}

	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("SIGKILL child: %v", err)
	}
	_, _ = cmd.Process.Wait()

	// Parent: new process on same DB file — Start must reset taken_at and redeliver.
	db, err := sql.Open("sqlite3", dbPath+"?_journal=wal&_busy_timeout=5000&_synchronous=normal&_fk=1&_loc=auto")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	ctx := context.Background()
	handler := mocks.NewEventHandler("crash_handler")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}
	// Initial sweep after StartChecked must redeliver without external notify.
	if err := o.StartChecked(); err != nil {
		t.Fatal(err)
	}

	if !handler.Wait(5 * time.Second) {
		t.Fatal("event was not redelivered after crash + Start reset")
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if outboxRowCount(t, db) == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := outboxRowCount(t, db); got != 0 {
		t.Fatalf("outbox rows after restart processing = %d, want 0", got)
	}
}

func runCrashChild(t *testing.T) {
	dbPath := os.Getenv("EH_SQLITE_CRASH_DB")
	readyPath := os.Getenv("EH_SQLITE_CRASH_READY")
	if dbPath == "" || readyPath == "" {
		t.Fatal("crash child missing env")
	}

	db, err := sql.Open("sqlite3", dbPath+"?_journal=wal&_busy_timeout=5000&_synchronous=normal&_fk=1&_loc=auto")
	if err != nil {
		fmt.Fprintf(os.Stderr, "child open db: %v\n", err)
		os.Exit(1)
	}
	// Intentionally not deferred close — killed mid-flight.

	o, err := NewOutbox(db)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child new outbox: %v\n", err)
		os.Exit(1)
	}

	entered := make(chan struct{})
	handler := &hangAfterReadyHandler{Type: "crash_handler", readyPath: readyPath, entered: entered}
	if err := o.AddHandler(context.Background(), eh.MatchEvents{mocks.EventType}, handler); err != nil {
		fmt.Fprintf(os.Stderr, "child add handler: %v\n", err)
		os.Exit(1)
	}
	if err := o.StartChecked(); err != nil {
		fmt.Fprintf(os.Stderr, "child start: %v\n", err)
		os.Exit(1)
	}

	if err := o.HandleEvent(context.Background(), newTestEvent("crash-payload")); err != nil {
		fmt.Fprintf(os.Stderr, "child publish: %v\n", err)
		os.Exit(1)
	}

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		fmt.Fprintf(os.Stderr, "child handler never entered\n")
		os.Exit(1)
	}

	// Hang until SIGKILL.
	select {}
}

type hangAfterReadyHandler struct {
	Type      string
	readyPath string
	entered   chan struct{}
	once      sync.Once
}

func (h *hangAfterReadyHandler) HandlerType() eh.EventHandlerType {
	return eh.EventHandlerType(h.Type)
}

func (h *hangAfterReadyHandler) HandleEvent(context.Context, eh.Event) error {
	h.once.Do(func() {
		_ = os.WriteFile(h.readyPath, []byte("ready"), 0o600)
		close(h.entered)
	})
	// Block forever — parent will SIGKILL.
	select {}
}

type countingBlockHandler struct {
	Type        string
	entered     chan struct{}
	release     <-chan struct{}
	invocations *atomic.Int64
}

func (h *countingBlockHandler) HandlerType() eh.EventHandlerType {
	return eh.EventHandlerType(h.Type)
}

func (h *countingBlockHandler) HandleEvent(context.Context, eh.Event) error {
	h.invocations.Add(1)
	select {
	case h.entered <- struct{}{}:
	default:
	}
	<-h.release
	return nil
}
