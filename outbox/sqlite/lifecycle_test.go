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
)

// lcResolveSerial stamps dispatch_key/dispatch_config for a Serial handler on
// one delivery without running the whole startup reconcile (which would also
// clear taken_at on rows that are currently in flight). Used for rows seeded
// while an earlier batch is still executing.
func lcResolveSerial(t testing.TB, o *Outbox, deliveryID, handlerType string) {
	t.Helper()
	if _, err := o.db.Exec(fmt.Sprintf(
		`UPDATE %s SET dispatch_key = ?, dispatch_config = 'mode=serial', unresolved_at = NULL WHERE id = ?`,
		o.deliveriesTable), handlerType, deliveryID); err != nil {
		t.Fatal(err)
	}
}

// lcCountTaken returns (claimed, unclaimed) delivery counts.
func lcCountTaken(t testing.TB, o *Outbox) (claimed, unclaimed int) {
	t.Helper()
	for _, row := range listDeliveries(t, o) {
		if row.TakenAt.Valid {
			claimed++
			continue
		}
		unclaimed++
	}
	return claimed, unclaimed
}

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

	// A fresh taken_at would block the claim without the startup reset.
	createdAt := time.Now().Add(-time.Minute)
	insertDeliveryDirect(t, o, deliverySeed{
		Event:       newTestEvent("reset"),
		HandlerType: handler.Type,
		CreatedAt:   createdAt,
		TakenAt:     time.Now(),
	})

	// Start must reset taken_at and initial-sweep without an external notify.
	if err := o.StartChecked(); err != nil {
		t.Fatal(err)
	}

	if !handler.Wait(3 * time.Second) {
		t.Fatal("handler should run after Start clears taken_at")
	}
	waitUntil(t, 3*time.Second, func() bool { return countDeliveries(t, o) == 0 })
	if got := countPublications(t, o); got != 0 {
		t.Fatalf("publications after delivery completed = %d, want 0", got)
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

// TestOutboxStartCheckedFailClosedOnResetAbort proves the fail-closed startup:
// the single startup transaction (startupReconcile) cannot commit, so
// StartChecked returns an error, the fetcher never runs, registration stays
// closed, publish stays blocked with ErrOutboxNotStarted, and a later
// StartChecked after removing the fault succeeds.
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

	createdAt := time.Now().Add(-time.Minute)
	insertDeliveryDirect(t, o, deliverySeed{
		Event:       newTestEvent("reset-abort"),
		HandlerType: handler.Type,
		CreatedAt:   createdAt,
		TakenAt:     time.Now(),
	})

	// The startup transaction resets taken_at on every delivery; abort it.
	if _, err := db.Exec(fmt.Sprintf(`
		CREATE TRIGGER lc_abort_taken_at_reset
		BEFORE UPDATE OF taken_at ON %s
		WHEN NEW.taken_at IS NULL AND OLD.taken_at IS NOT NULL
		BEGIN
			SELECT RAISE(ABORT, 'forced reset abort');
		END
	`, o.deliveriesTable)); err != nil {
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
	if got := countDeliveries(t, o); got != 1 {
		t.Fatalf("deliveries = %d, want 1 (unprocessed after failed start)", got)
	}
	if got := countPublications(t, o); got != 1 {
		t.Fatalf("publications = %d, want 1 (unprocessed after failed start)", got)
	}

	if _, err := db.Exec(`DROP TRIGGER lc_abort_taken_at_reset`); err != nil {
		t.Fatal(err)
	}

	if err := o.StartChecked(); err != nil {
		t.Fatalf("retry StartChecked after dropping trigger: %v", err)
	}
	if !o.processorRunning.Load() {
		t.Fatal("processorRunning = false after successful retry")
	}
	// Publish works only after the successful retry.
	if err := o.HandleEvent(ctx, newTestEvent("after-retry")); err != nil {
		t.Fatalf("HandleEvent after successful retry: %v", err)
	}
	if !handler.Wait(3 * time.Second) {
		t.Fatal("handler should run after successful StartChecked retry")
	}
	waitUntil(t, 3*time.Second, func() bool { return countDeliveries(t, o) == 0 })
	if got := countPublications(t, o); got != 0 {
		t.Fatalf("publications after retry drain = %d, want 0", got)
	}
}

// TestOutboxAdmissionSkipsActiveAndClaimsIdle: a delivery that is already
// admitted in this process (long-running handler) must not consume the per-key
// LIMIT, so a later idle delivery on the same key is still claimed.
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

	// The active delivery is older (first in FIFO) and its taken_at is stale,
	// so SQL alone would happily re-claim it.
	old := time.Now().Add(-time.Minute)
	_, activeID := insertDeliveryDirect(t, o, deliverySeed{
		Event:       newTestEvent("active"),
		HandlerType: handler.Type,
		CreatedAt:   old,
	})
	_, idleID := insertDeliveryDirect(t, o, deliverySeed{
		Event:       newTestEvent("idle"),
		HandlerType: handler.Type,
		CreatedAt:   old.Add(time.Second),
	})

	reconcileForTest(t, o)
	setTakenAt(t, o, activeID, time.Now().Add(-time.Hour))

	// Process-local: active is already admitted on its dispatch key.
	if !o.admission.tryAdmitKey(activeID, handler.Type) {
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

	// The active delivery must remain untouched; the idle one is gone.
	remaining := listDeliveries(t, o)
	if len(remaining) != 1 {
		t.Fatalf("deliveries = %d, want 1", len(remaining))
	}
	if remaining[0].ID != activeID {
		t.Fatalf("remaining delivery = %s, want active %s (idle %s should be gone)", remaining[0].ID, activeID, idleID)
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
	for i := range 3 {
		insertDeliveryDirect(t, o, deliverySeed{
			Event:       newTestEvent(fmt.Sprintf("partial-%d", i)),
			HandlerType: handler.Type,
			CreatedAt:   createdAt,
		})
	}
	reconcileForTest(t, o)

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

	// While the first delivery is admitted, a second claim must take nothing.
	processed, err := o.processBatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 0 {
		t.Fatalf("second processBatch processed = %d, want 0 (admission full)", processed)
	}

	claimed, unclaimed := lcCountTaken(t, o)
	if claimed != 1 {
		t.Fatalf("claimed deliveries = %d, want 1", claimed)
	}
	if unclaimed != 2 {
		t.Fatalf("unclaimed deliveries = %d, want 2", unclaimed)
	}

	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("processBatch did not finish")
	}
}

// TestOutboxAdmissionPreventsDuplicateWhileHandlerExceedsSweepAge: a delivery
// whose handler runs longer than PeriodicSweepAge is never claimed a second
// time in the same process — the per-key candidate SELECT excludes ids that
// are admitted here, so the stale taken_at is not re-claimed.
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

	createdAt := time.Now().Add(-time.Minute)
	_, deliveryID := insertDeliveryDirect(t, o, deliverySeed{
		Event:       newTestEvent("long"),
		HandlerType: handler.Type,
		CreatedAt:   createdAt,
	})
	reconcileForTest(t, o)

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

	// Backdate taken_at far beyond PeriodicSweepAge: SQL alone would now
	// consider the claim reclaimable. No sleeping, no timing threshold.
	setTakenAt(t, o, deliveryID, time.Now().Add(-time.Hour))

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
	if got := countDeliveries(t, o); got != 0 {
		t.Fatalf("deliveries after complete = %d, want 0", got)
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

	createdAt := time.Now().Add(-time.Minute)
	insertDeliveryDirect(t, o, deliverySeed{
		Event:       newTestEvent("first"),
		HandlerType: handler.Type,
		CreatedAt:   createdAt,
	})
	reconcileForTest(t, o)

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

	// A new due delivery arrives while the batch is in flight. Resolve its key
	// directly so the in-flight row's taken_at is not disturbed.
	_, secondID := insertDeliveryDirect(t, o, deliverySeed{
		Event:       newTestEvent("second"),
		HandlerType: handler.Type,
		CreatedAt:   createdAt,
	})
	lcResolveSerial(t, o, secondID, handler.Type)

	// Concurrent claim during the batch must not exceed admission capacity.
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
	if got := countDeliveries(t, o); got != 0 {
		t.Fatalf("deliveries = %d, want 0 after draining new message", got)
	}
	if got := countPublications(t, o); got != 0 {
		t.Fatalf("publications = %d, want 0 after draining new message", got)
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
	insertDeliveryDirect(t, o, deliverySeed{
		Event:       newTestEvent("shutdown"),
		HandlerType: handler.Type,
		CreatedAt:   createdAt,
	})

	// Drive via Start so Close waits on the processor goroutine. The initial
	// sweep after StartChecked must enter the handler without an external notify.
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

	if got := countDeliveries(t, o); got != 0 {
		t.Fatalf("deliveries after Close = %d, want 0", got)
	}
	if got := countPublications(t, o); got != 0 {
		t.Fatalf("publications after Close = %d, want 0", got)
	}
}

// TestOutboxCrashRestartStartupReset is a real subprocess E2E: the child admits
// and handles an event, signals readiness, then hangs until SIGKILL. The parent
// reopens the same DB, registers handlers, Start()s (startup reset of taken_at),
// and asserts the delivery is redelivered exactly once to the new process.
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

	// Wait for the child to claim and enter the handler.
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

	// Parent: new process on the same DB file — Start must reset taken_at and
	// redeliver the orphaned delivery.
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
	// A claimed-but-orphaned delivery must be present before the restart.
	claimed, _ := lcCountTaken(t, o)
	if claimed != 1 {
		t.Fatalf("claimed deliveries left by the killed child = %d, want 1", claimed)
	}

	// The initial sweep after StartChecked must redeliver without an external notify.
	if err := o.StartChecked(); err != nil {
		t.Fatal(err)
	}

	if !handler.Wait(5 * time.Second) {
		t.Fatal("event was not redelivered after crash + Start reset")
	}
	waitUntil(t, 5*time.Second, func() bool { return countDeliveries(t, o) == 0 })
	if got := countPublications(t, o); got != 0 {
		t.Fatalf("publications after restart processing = %d, want 0", got)
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
	// Block forever — the parent will SIGKILL.
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
