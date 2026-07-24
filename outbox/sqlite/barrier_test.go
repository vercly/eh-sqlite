package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	eh "github.com/vercly/eventhorizon"
	"github.com/vercly/eventhorizon/mocks"
	"github.com/vercly/eventhorizon/uuid"
)

// TestOutboxBarrierIdleEntersBeforeSlowReleased asserts the core 7a contract:
// an idle dispatch key progresses while a slow key is still blocked — no batch
// barrier. Ordering is checked via event sequence, not wall-clock SLOs.
func TestOutboxBarrierIdleEntersBeforeSlowReleased(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithMaxGoroutines(4), WithAdmissionLimit(8))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	var order []string
	var orderMu sync.Mutex
	record := func(name string) {
		orderMu.Lock()
		order = append(order, name)
		orderMu.Unlock()
	}

	slowRelease := make(chan struct{})
	slowEntered := make(chan struct{})
	slow := &namedBlockHandler{
		Type:    "slow_key",
		onEnter: func() { record("slow_enter"); close(slowEntered) },
		release: slowRelease,
	}
	idle := &namedBlockHandler{
		Type:    "idle_key",
		onEnter: func() { record("idle_enter") },
		release: make(chan struct{}), // closed immediately below
	}
	close(idle.release)

	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, slow); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventOtherType}, idle); err != nil {
		t.Fatal(err)
	}
	if err := o.StartChecked(); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	// Distinct event types → distinct Serial keys.
	seedOutboxEventWithID(t, db, o, uuid.New().String(), newTestEvent("slow-body"), []string{slow.Type}, createdAt, createdAt, sql.NullTime{})
	seedOutboxEventWithID(t, db, o, uuid.New().String(),
		eh.NewEvent(mocks.EventOtherType, &mocks.EventData{Content: "idle-body"}, time.Now()),
		[]string{idle.Type}, createdAt.Add(time.Millisecond), createdAt.Add(time.Millisecond), sql.NullTime{})

	o.notify()

	select {
	case <-slowEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("slow handler did not enter")
	}

	// Idle must enter while slow is still held (barrier would prevent this).
	deadline := time.Now().Add(3 * time.Second)
	for {
		orderMu.Lock()
		hasIdle := false
		for _, e := range order {
			if e == "idle_enter" {
				hasIdle = true
			}
		}
		orderMu.Unlock()
		if hasIdle {
			break
		}
		if time.Now().After(deadline) {
			orderMu.Lock()
			t.Fatalf("idle did not enter before slow release; order=%v", order)
			orderMu.Unlock()
		}
		time.Sleep(5 * time.Millisecond)
	}

	orderMu.Lock()
	// Find first idle_enter index relative to slow_enter — idle must appear
	// while slow has entered but before we release slow (still blocked).
	// At this point slow is still held, so any idle_enter proves no barrier.
	sawSlow, sawIdle := false, false
	for _, e := range order {
		if e == "slow_enter" {
			sawSlow = true
		}
		if e == "idle_enter" {
			sawIdle = true
		}
	}
	orderMu.Unlock()
	if !sawSlow || !sawIdle {
		t.Fatalf("order incomplete: %v", order)
	}

	close(slowRelease)
	// Drain remaining work.
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if outboxRowCount(t, db) == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := outboxRowCount(t, db); got != 0 {
		t.Fatalf("outbox rows = %d, want 0", got)
	}
}

// TestOutboxD2RetryLosesPosition: A fails with backoff; B on the same key
// completes before A's retry becomes eligible.
func TestOutboxD2RetryLosesPosition(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithRetryBackoff("FIXED:5:500ms"), WithMaxRetries(5))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	var mu sync.Mutex
	var seen []string
	var failA atomic.Bool
	failA.Store(true)

	handler := &seqHandler{
		Type: "d2_handler",
		fn: func(content string) error {
			mu.Lock()
			seen = append(seen, content)
			mu.Unlock()
			if content == "A" && failA.Load() {
				failA.Store(false)
				return errors.New("A temporary failure")
			}
			return nil
		},
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	// A is due now; B slightly later by created_at but both available now.
	// After A fails, A gets future available_at; B should run next.
	t0 := time.Now().Add(-time.Minute)
	idA := uuid.New().String()
	idB := uuid.New().String()
	seedOutboxEventWithID(t, db, o, idA, newTestEvent("A"), []string{handler.Type}, t0, t0, sql.NullTime{})
	seedOutboxEventWithID(t, db, o, idB, newTestEvent("B"), []string{handler.Type}, t0.Add(time.Millisecond), t0.Add(time.Millisecond), sql.NullTime{})

	// First wave: A fails, B succeeds.
	if _, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	}
	// May need a second wave if only one claimed first — keep processing until B done.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(seen)
		mu.Unlock()
		if n >= 2 {
			break
		}
		_, _ = o.processBatch(ctx)
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	got := append([]string(nil), seen...)
	mu.Unlock()
	// Expect A then B before any A retry (retry is 500ms out).
	if len(got) < 2 {
		t.Fatalf("seen = %v, want at least [A,B]", got)
	}
	if got[0] != "A" {
		t.Fatalf("first = %s, want A", got[0])
	}
	if got[1] != "B" {
		t.Fatalf("second = %s, want B (B completes before A retry)", got[1])
	}
	// A still pending for retry.
	var retryCount int
	var availableAt time.Time
	if err := db.QueryRow(`SELECT retry_count, available_at FROM outbox WHERE id = ?`, idA).Scan(&retryCount, &availableAt); err != nil {
		t.Fatal(err)
	}
	if retryCount != 1 {
		t.Fatalf("A retry_count = %d, want 1", retryCount)
	}
	if !availableAt.After(time.Now().Add(100 * time.Millisecond)) {
		t.Fatalf("A available_at = %s, want future backoff", availableAt)
	}
}

// TestOutboxSaturatedQueueDoesNotBlockOtherKeys: fill one key's queue to capacity;
// another key's record still claims and runs.
func TestOutboxSaturatedQueueDoesNotBlockOtherKeys(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	// Tiny queue depth via constructing registry after NewOutbox — use option.
	// Stage 7a uses defaultQueueDepth; force small depth by setting field before Start.
	o, err := NewOutbox(db, WithMaxGoroutines(2), WithAdmissionLimit(64))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	o.queueDepth = 2
	o.dispatch = newDispatchRegistry(o, 2, o.dispatchStats)

	blockRelease := make(chan struct{})
	saturated := newBlockingHandler("sat_key", make(chan string, 8), blockRelease)
	// Keep first delivery stuck so queue fills.
	idleEntered := make(chan struct{}, 1)
	idle := &namedBlockHandler{
		Type:    "free_key",
		onEnter: func() { idleEntered <- struct{}{} },
		release: make(chan struct{}),
	}
	close(idle.release)

	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, saturated); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventOtherType}, idle); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	// 1 in-flight + 2 queued = depth 2 reserved while first blocks...
	// Seed 4 sat events: first runs (depth 1), next 2 fill queue, 4th cannot reserve.
	for i := range 4 {
		seedOutboxEventWithID(t, db, o, uuid.New().String(), newTestEvent(fmt.Sprintf("sat-%d", i)),
			[]string{saturated.Type}, createdAt, createdAt, sql.NullTime{})
	}
	// Idle record due as well.
	seedOutboxEventWithID(t, db, o, uuid.New().String(),
		eh.NewEvent(mocks.EventOtherType, &mocks.EventData{Content: "free"}, time.Now()),
		[]string{idle.Type}, createdAt, createdAt, sql.NullTime{})

	// Non-waiting fetch waves until idle enters.
	go func() {
		for range 20 {
			_, _ = o.fetchAndDispatch(ctx, false)
			time.Sleep(5 * time.Millisecond)
		}
	}()

	select {
	case <-idleEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("free_key did not run while sat_key queue was saturated")
	}

	close(blockRelease)
	// Clean up remaining.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, _ = o.fetchAndDispatch(ctx, true)
		if outboxRowCount(t, db) == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestOutboxNewMessageWhileSlowKeyRuns: publish/seed a new message for another
// key while a slow key is in-flight; new message is claimed without waiting for slow.
func TestOutboxNewMessageWhileSlowKeyRuns(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithMaxGoroutines(4), WithAdmissionLimit(8))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	slowRelease := make(chan struct{})
	slowEntered := make(chan struct{})
	slow := &namedBlockHandler{
		Type:    "slow_newmsg",
		onEnter: func() { close(slowEntered) },
		release: slowRelease,
	}
	fastEntered := make(chan struct{}, 1)
	fast := &namedBlockHandler{
		Type:    "fast_newmsg",
		onEnter: func() { fastEntered <- struct{}{} },
		release: make(chan struct{}),
	}
	close(fast.release)

	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, slow); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventOtherType}, fast); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	seedOutboxEventWithID(t, db, o, uuid.New().String(), newTestEvent("slow"), []string{slow.Type}, createdAt, createdAt, sql.NullTime{})

	go func() { _, _ = o.fetchAndDispatch(ctx, false) }()

	select {
	case <-slowEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("slow did not enter")
	}

	// New message for a different key while slow is still running.
	seedOutboxEventWithID(t, db, o, uuid.New().String(),
		eh.NewEvent(mocks.EventOtherType, &mocks.EventData{Content: "fast"}, time.Now()),
		[]string{fast.Type}, createdAt, createdAt, sql.NullTime{})
	_, _ = o.fetchAndDispatch(ctx, false)

	select {
	case <-fastEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("new message on free key was not claimed while slow still held")
	}

	close(slowRelease)
	_, _ = o.fetchAndDispatch(ctx, true)
}

// TestOutboxPaginationReachesIdleBehindFullKeyBacklog: more than maxFetchBatch
// due rows on a saturated key must not starve a later idle-key record in the
// same claim transaction wave.
func TestOutboxPaginationReachesIdleBehindFullKeyBacklog(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithMaxGoroutines(2), WithAdmissionLimit(8))
	if err != nil {
		t.Fatal(err)
	}
	// Tiny queue so sat key fills quickly; pagination must walk past the backlog.
	o.queueDepth = 2
	o.dispatch = newDispatchRegistry(o, 2, o.dispatchStats)

	slowRelease := make(chan struct{})
	slowEntered := make(chan struct{})
	slow := &namedBlockHandler{
		Type:    "page_sat_key",
		onEnter: func() { close(slowEntered) },
		release: slowRelease,
	}
	idleEntered := make(chan struct{}, 1)
	idle := &namedBlockHandler{
		Type:    "page_idle_key",
		onEnter: func() { idleEntered <- struct{}{} },
		release: make(chan struct{}),
	}
	close(idle.release)

	// Production Close waits for in-flight handlers; always release before Close.
	// Cleanup runs LIFO: register Close first so release runs first.
	t.Cleanup(func() { _ = o.Close() })
	t.Cleanup(func() { closeOnce(slowRelease) })

	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, slow); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventOtherType}, idle); err != nil {
		t.Fatal(err)
	}

	// ORDER BY available_at, created_at, id — sat backlog fills >1 page
	// (maxFetchBatch), idle is strictly later by available_at (no time.Time
	// cursor comparisons; planAndClaim uses LIMIT/OFFSET in one TX).
	base := time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC)
	backlog := maxFetchBatch + 10
	for i := range backlog {
		ts := base.Add(time.Duration(i) * time.Second)
		seedOutboxEventWithID(t, db, o, uuid.New().String(), newTestEvent(fmt.Sprintf("sat-%d", i)),
			[]string{slow.Type}, ts, ts, sql.NullTime{})
	}
	idleAt := base.Add(24 * time.Hour)
	seedOutboxEventWithID(t, db, o, uuid.New().String(),
		eh.NewEvent(mocks.EventOtherType, &mocks.EventData{Content: "idle"}, idleAt),
		[]string{idle.Type}, idleAt, idleAt, sql.NullTime{})

	// Single claim wave must OFFSET-page past the sat backlog and include idle.
	n, err := o.fetchAndDispatch(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if n < 2 {
		t.Fatalf("claimed = %d, want at least sat+idle (pagination failed to reach idle)", n)
	}

	select {
	case <-slowEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("saturated key did not start")
	}

	// Idle must enter before slow is released.
	select {
	case <-idleEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("idle key behind full-key backlog > maxFetchBatch never claimed")
	}

	closeOnce(slowRelease)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, _ = o.fetchAndDispatch(ctx, true)
		if outboxRowCount(t, db) == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := outboxRowCount(t, db); got != 0 {
		t.Fatalf("leftover outbox rows = %d", got)
	}
}

// TestOutboxMissingHandlerLeavesUnclaimed: stored handler no longer registered
// must not delete the row (no silent loss).
func TestOutboxMissingHandlerLeavesUnclaimed(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	// Register a different handler than the one stored on the row.
	live := mocks.NewEventHandler("live_handler")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, live); err != nil {
		t.Fatal(err)
	}

	id := uuid.New().String()
	createdAt := time.Now().Add(-time.Minute)
	seedOutboxEventWithID(t, db, o, id, newTestEvent("orphan"), []string{"gone_handler"}, createdAt, createdAt, sql.NullTime{})

	// Seed a healthy row that must still progress.
	okID := uuid.New().String()
	seedOutboxEventWithID(t, db, o, okID, newTestEvent("ok"), []string{live.Type}, createdAt, createdAt, sql.NullTime{})

	if _, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	}

	if got := outboxRowCount(t, db); got != 1 {
		t.Fatalf("outbox rows = %d, want 1 (orphan left unclaimed)", got)
	}
	var remaining string
	if err := db.QueryRow(`SELECT id FROM outbox`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != id {
		t.Fatalf("remaining id = %s, want orphan %s", remaining, id)
	}
	var takenAt sql.NullTime
	if err := db.QueryRow(`SELECT taken_at FROM outbox WHERE id = ?`, id).Scan(&takenAt); err != nil {
		t.Fatal(err)
	}
	if takenAt.Valid {
		t.Fatal("orphan row must stay unclaimed (taken_at NULL)")
	}

	// Diagnostic error should be visible.
	found := false
	for range 8 {
		select {
		case err := <-o.Errors():
			if err != nil && strings.Contains(err.Error(), "unresolvable handlers") {
				found = true
			}
		default:
		}
	}
	if !found {
		t.Fatal("expected diagnostic error for unresolvable handlers")
	}
}

// TestOutboxFairHandleEventLimitPreferWaitingKey: with max=1, after the first
// hot delivery an idle key waiting on the permit must run before the next hot
// delivery on the saturated key.
func TestOutboxFairHandleEventLimitPreferWaitingKey(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithMaxGoroutines(1), WithAdmissionLimit(16))
	if err != nil {
		t.Fatal(err)
	}

	var order []string
	var orderMu sync.Mutex
	push := func(s string) {
		orderMu.Lock()
		order = append(order, s)
		orderMu.Unlock()
	}

	hotRelease1 := make(chan struct{})
	hotEntered1 := make(chan struct{})
	hotN := atomic.Int32{}
	hot := &countingGateHandler{
		Type: "fair_hot",
		fn: func(n int) {
			push(fmt.Sprintf("hot-%d", n))
			if n == 1 {
				close(hotEntered1)
				<-hotRelease1
			}
		},
		counter: &hotN,
	}
	idleEntered := make(chan struct{})
	idle := &namedBlockHandler{
		Type: "fair_idle",
		onEnter: func() {
			push("idle")
			close(idleEntered)
		},
		release: make(chan struct{}),
	}
	close(idle.release)

	// Close waits for running handlers; release hot before Close on every path.
	t.Cleanup(func() { _ = o.Close() })
	t.Cleanup(func() { closeOnce(hotRelease1) })

	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, hot); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventOtherType}, idle); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	// Phase 1: only hot-1 so it alone holds the single permit.
	seedOutboxEventWithID(t, db, o, uuid.New().String(), newTestEvent("h1"), []string{hot.Type}, createdAt, createdAt, sql.NullTime{})
	go func() { _, _ = o.fetchAndDispatch(ctx, false) }()

	select {
	case <-hotEntered1:
	case <-time.After(3 * time.Second):
		t.Fatal("hot-1 did not enter")
	}

	// Phase 2: while hot-1 holds the permit, seed idle + hot-2 and dispatch so
	// both queue on the fair semaphore (idle waiter must be registered first).
	seedOutboxEventWithID(t, db, o, uuid.New().String(),
		eh.NewEvent(mocks.EventOtherType, &mocks.EventData{Content: "i1"}, time.Now()),
		[]string{idle.Type}, createdAt.Add(time.Millisecond), createdAt.Add(time.Millisecond), sql.NullTime{})
	// Dispatch idle alone first so its worker is the first fairSem waiter.
	go func() { _, _ = o.fetchAndDispatch(ctx, false) }()
	deadlineWait := time.Now().Add(2 * time.Second)
	for !o.handleSem.hasWaiters() && time.Now().Before(deadlineWait) {
		time.Sleep(2 * time.Millisecond)
	}
	if !o.handleSem.hasWaiters() {
		t.Fatal("idle did not queue on fairSem while hot-1 held the permit")
	}

	seedOutboxEventWithID(t, db, o, uuid.New().String(), newTestEvent("h2"), []string{hot.Type},
		createdAt.Add(2*time.Millisecond), createdAt.Add(2*time.Millisecond), sql.NullTime{})
	go func() { _, _ = o.fetchAndDispatch(ctx, false) }()
	// hot-2 sits in the hot key queue; when hot-1 finishes it will acquire after idle.
	time.Sleep(20 * time.Millisecond)

	closeOnce(hotRelease1)

	select {
	case <-idleEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("idle did not enter after hot-1 released permit")
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, _ = o.fetchAndDispatch(ctx, true)
		if outboxRowCount(t, db) == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	orderMu.Lock()
	got := append([]string(nil), order...)
	orderMu.Unlock()
	idxHot1, idxIdle, idxHot2 := -1, -1, -1
	for i, e := range got {
		switch e {
		case "hot-1":
			if idxHot1 < 0 {
				idxHot1 = i
			}
		case "idle":
			if idxIdle < 0 {
				idxIdle = i
			}
		case "hot-2":
			if idxHot2 < 0 {
				idxHot2 = i
			}
		}
	}
	if idxHot1 < 0 || idxIdle < 0 || idxHot2 < 0 {
		t.Fatalf("incomplete order %v", got)
	}
	if !(idxHot1 < idxIdle && idxIdle < idxHot2) {
		t.Fatalf("fairness order = %v, want hot-1 before idle before hot-2", got)
	}
}

// closeOnce closes ch if still open (safe from multiple success/cleanup paths).
func closeOnce(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

type countingGateHandler struct {
	Type    string
	fn      func(n int)
	counter *atomic.Int32
}

func (h *countingGateHandler) HandlerType() eh.EventHandlerType {
	return eh.EventHandlerType(h.Type)
}

func (h *countingGateHandler) HandleEvent(context.Context, eh.Event) error {
	n := int(h.counter.Add(1))
	h.fn(n)
	return nil
}

type namedBlockHandler struct {
	Type    string
	onEnter func()
	release chan struct{}
	once    sync.Once
}

func (h *namedBlockHandler) HandlerType() eh.EventHandlerType {
	return eh.EventHandlerType(h.Type)
}

func (h *namedBlockHandler) HandleEvent(context.Context, eh.Event) error {
	// Signal enter once so tests can wait for the first in-flight call.
	h.once.Do(func() {
		if h.onEnter != nil {
			h.onEnter()
		}
	})
	if h.release != nil {
		<-h.release
	}
	return nil
}

type seqHandler struct {
	Type string
	fn   func(content string) error
}

func (h *seqHandler) HandlerType() eh.EventHandlerType {
	return eh.EventHandlerType(h.Type)
}

func (h *seqHandler) HandleEvent(_ context.Context, event eh.Event) error {
	return h.fn(eventContent(event))
}
