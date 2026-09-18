package sqlite

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vercly/eh-sqlite/schema"
	eh "github.com/vercly/eventhorizon"
	"github.com/vercly/eventhorizon/mocks"
	"github.com/vercly/eventhorizon/uuid"
)

// brOtherEvent builds an event of mocks.EventOtherType (a second Serial
// dispatch key when matched by its own handler).
func brOtherEvent(content string) eh.Event {
	return eh.NewEvent(mocks.EventOtherType, &mocks.EventData{Content: content}, time.Now(),
		eh.ForAggregate(mocks.AggregateType, uuid.New(), 1),
	)
}

// brInsertSiblingDelivery adds one more delivery to an existing publication
// (the v2 shape: one publication, one row per recipient). Returns the delivery id.
func brInsertSiblingDelivery(t testing.TB, o *Outbox, publicationID string, event eh.Event, handlerType string, createdAt time.Time) string {
	t.Helper()
	deliveryID := uuid.New().String()
	if _, err := o.db.Exec(fmt.Sprintf(`
		INSERT INTO %s (id, publication_id, handler_type, dispatch_key, dispatch_config, event_type, aggregate_id,
		                created_at, available_at, taken_at, retry_count, unresolved_at, legacy_outbox_id)
		VALUES (?, ?, ?, NULL, NULL, ?, ?, ?, ?, NULL, 0, NULL, NULL)`, o.deliveriesTable),
		deliveryID, publicationID, handlerType, event.EventType().String(), event.AggregateID().String(),
		schema.UTC(createdAt), schema.UTC(createdAt)); err != nil {
		t.Fatal(err)
	}
	return deliveryID
}

// brClaimedForKey counts deliveries of one dispatch key that carry a claim.
func brClaimedForKey(t testing.TB, o *Outbox, key string) int {
	t.Helper()
	var n int
	if err := o.db.QueryRow(fmt.Sprintf(
		`SELECT COUNT(*) FROM %s WHERE dispatch_key = ? AND taken_at IS NOT NULL`, o.deliveriesTable), key).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// brUnclaimedForKey counts the remaining backlog of one dispatch key.
func brUnclaimedForKey(t testing.TB, o *Outbox, key string) int {
	t.Helper()
	var n int
	if err := o.db.QueryRow(fmt.Sprintf(
		`SELECT COUNT(*) FROM %s WHERE dispatch_key = ? AND taken_at IS NULL`, o.deliveriesTable), key).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// brMinSeqForKey returns the lowest seq still stored for a dispatch key.
func brMinSeqForKey(t testing.TB, o *Outbox, key string) int64 {
	t.Helper()
	var seq int64
	if err := o.db.QueryRow(fmt.Sprintf(
		`SELECT MIN(seq) FROM %s WHERE dispatch_key = ?`, o.deliveriesTable), key).Scan(&seq); err != nil {
		t.Fatal(err)
	}
	return seq
}

// brDrain runs claim passes until no delivery is left (handlers must be
// released first). Only background progress is polled, never correctness.
func brDrain(t testing.TB, o *Outbox, ctx context.Context) {
	t.Helper()
	// Let every released handler finish its finalize transaction before the
	// next claim transaction starts (single-writer SQLite).
	waitUntil(t, 10*time.Second, func() bool {
		used, _ := o.AdmissionSnapshot()
		return used == 0
	})
	deadline := time.Now().Add(20 * time.Second)
	for {
		if countDeliveries(t, o) == 0 {
			if got := countPublications(t, o); got != 0 {
				t.Fatalf("publications left after draining deliveries = %d, want 0", got)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("deliveries left after drain = %d", countDeliveries(t, o))
		}
		if _, err := o.fetchAndDispatch(ctx, true); err != nil {
			t.Fatal(err)
		}
	}
}

// TestOutboxBarrierIdleEntersBeforeSlowReleased asserts the core contract: an
// idle dispatch key progresses while a slow key is still blocked — no batch
// barrier. Ordering is checked via recorded entries, not wall-clock SLOs.
func TestOutboxBarrierIdleEntersBeforeSlowReleased(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithMaxGoroutines(4), WithAdmissionLimit(8))
	if err != nil {
		t.Fatal(err)
	}

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
	idleEntered := make(chan struct{}, 1)
	idle := &namedBlockHandler{
		Type: "idle_key",
		onEnter: func() {
			record("idle_enter")
			idleEntered <- struct{}{}
		},
		release: make(chan struct{}),
	}
	close(idle.release)

	// Close waits for in-flight handlers; release before Close on every path
	// (cleanup runs LIFO, so register Close first).
	t.Cleanup(func() { _ = o.Close() })
	t.Cleanup(func() { closeOnce(slowRelease) })

	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, slow); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventOtherType}, idle); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	// Distinct event types → distinct Serial dispatch keys.
	insertDeliveryDirect(t, o, deliverySeed{
		Event:       newTestEvent("slow-body"),
		HandlerType: slow.Type,
		CreatedAt:   createdAt,
	})
	insertDeliveryDirect(t, o, deliverySeed{
		Event:       brOtherEvent("idle-body"),
		HandlerType: idle.Type,
		CreatedAt:   createdAt.Add(time.Millisecond),
	})

	// StartChecked reconciles the seeded rows (dispatch keys) and sweeps.
	if err := o.StartChecked(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-slowEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("slow handler did not enter")
	}

	// The idle key must enter while slow is still held — a batch barrier would
	// prevent this. No release has happened yet at this point.
	select {
	case <-idleEntered:
	case <-time.After(3 * time.Second):
		orderMu.Lock()
		got := append([]string(nil), order...)
		orderMu.Unlock()
		t.Fatalf("idle did not enter while slow was still blocked; order=%v", got)
	}

	orderMu.Lock()
	got := append([]string(nil), order...)
	orderMu.Unlock()
	sawSlow, sawIdle := false, false
	for _, e := range got {
		switch e {
		case "slow_enter":
			sawSlow = true
		case "idle_enter":
			sawIdle = true
		}
	}
	if !sawSlow || !sawIdle {
		t.Fatalf("order = %v, want both slow_enter and idle_enter while slow is still blocked", got)
	}

	closeOnce(slowRelease)
	waitUntil(t, 5*time.Second, func() bool { return countDeliveries(t, o) == 0 })
	if got := countPublications(t, o); got != 0 {
		t.Fatalf("publications = %d, want 0", got)
	}
}

// TestOutboxD2RetryLosesPosition: A fails with backoff; B on the same key
// completes before A's retry becomes eligible. Retry is per delivery.
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

	// A and B share one Serial key; both are due now, A first by seq.
	t0 := time.Now().Add(-time.Minute)
	_, idA := insertDeliveryDirect(t, o, deliverySeed{
		Event:       newTestEvent("A"),
		HandlerType: handler.Type,
		CreatedAt:   t0,
	})
	_, idB := insertDeliveryDirect(t, o, deliverySeed{
		Event:       newTestEvent("B"),
		HandlerType: handler.Type,
		CreatedAt:   t0.Add(time.Millisecond),
	})
	reconcileForTest(t, o)

	// One pass claims both (quantum 8) and the key worker runs them in order.
	if _, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	got := append([]string(nil), seen...)
	mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("seen = %v, want exactly [A B] (A's retry is 500ms out)", got)
	}
	if got[0] != "A" || got[1] != "B" {
		t.Fatalf("seen = %v, want [A B] (B completes before A's retry)", got)
	}

	// B is done (row gone); A lost its position and waits for the backoff.
	var bLeft int
	if err := db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE id = ?`, o.deliveriesTable), idB).Scan(&bLeft); err != nil {
		t.Fatal(err)
	}
	if bLeft != 0 {
		t.Fatalf("B delivery rows = %d, want 0 (completed)", bLeft)
	}

	var retryCount int
	var availableAt time.Time
	var takenAt any
	if err := db.QueryRow(fmt.Sprintf(`SELECT retry_count, available_at, taken_at FROM %s WHERE id = ?`, o.deliveriesTable), idA).
		Scan(&retryCount, &availableAt, &takenAt); err != nil {
		t.Fatal(err)
	}
	if retryCount != 1 {
		t.Fatalf("A retry_count = %d, want 1", retryCount)
	}
	if takenAt != nil {
		t.Fatalf("A taken_at = %v, want NULL (claim released for retry)", takenAt)
	}
	if !availableAt.After(time.Now().Add(100 * time.Millisecond)) {
		t.Fatalf("A available_at = %s, want future backoff", availableAt)
	}
	// Canonical UTC storage.
	raw := storedText(t, db, fmt.Sprintf(`SELECT CAST(available_at AS TEXT) FROM %s WHERE id = ?`, o.deliveriesTable), idA)
	if !strings.HasSuffix(raw, "+00:00") {
		t.Fatalf("stored available_at = %q, want canonical UTC text", raw)
	}
}

// TestOutboxSaturatedQueueDoesNotBlockOtherKeys: fill one key's queue to
// capacity; another key's delivery still claims and runs in the same pass.
func TestOutboxSaturatedQueueDoesNotBlockOtherKeys(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithMaxGoroutines(2), WithAdmissionLimit(64), WithQueueDepth(2))
	if err != nil {
		t.Fatal(err)
	}

	blockRelease := make(chan struct{})
	saturated := &namedBlockHandler{Type: "sat_key", release: blockRelease}
	idleEntered := make(chan struct{}, 1)
	idle := &namedBlockHandler{
		Type:    "free_key",
		onEnter: func() { idleEntered <- struct{}{} },
		release: make(chan struct{}),
	}
	close(idle.release)

	t.Cleanup(func() { _ = o.Close() })
	t.Cleanup(func() { closeOnce(blockRelease) })

	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, saturated); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventOtherType}, idle); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	for i := range 6 {
		insertDeliveryDirect(t, o, deliverySeed{
			Event:       newTestEvent(fmt.Sprintf("sat-%d", i)),
			HandlerType: saturated.Type,
			CreatedAt:   createdAt,
		})
	}
	reconcileForTest(t, o)

	// Saturate the sat key: claim passes until the stable fixpoint is reached
	// (one delivery executing in the blocked handler, queueDepth queued, no
	// free queue slot left — the worker cannot pop any more).
	const satDepth = 2
	waitUntil(t, 5*time.Second, func() bool {
		if _, err := o.fetchAndDispatch(ctx, false); err != nil {
			t.Errorf("fetchAndDispatch: %v", err)
			return true
		}
		return brClaimedForKey(t, o, saturated.Type) == satDepth+1 && o.dispatch.freeCapacity(saturated.Type) == 0
	})
	satClaimedBefore := brClaimedForKey(t, o, saturated.Type)
	if satClaimedBefore != satDepth+1 {
		t.Fatalf("saturated key claimed %d, want %d (queue full plus the executing delivery)", satClaimedBefore, satDepth+1)
	}
	if brUnclaimedForKey(t, o, saturated.Type) == 0 {
		t.Fatal("saturated key has no backlog left; test would not prove anything")
	}

	// The other key's delivery arrives while sat is saturated and blocked.
	_, idleDeliveryID := insertDeliveryDirect(t, o, deliverySeed{
		Event:       brOtherEvent("free"),
		HandlerType: idle.Type,
		CreatedAt:   createdAt,
	})
	lcResolveSerial(t, o, idleDeliveryID, idle.Type)

	if _, err := o.fetchAndDispatch(ctx, false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-idleEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("free_key did not run while sat_key queue was saturated")
	}
	// The saturated key must not have taken anything more in that pass.
	if got := brClaimedForKey(t, o, saturated.Type); got != satClaimedBefore {
		t.Fatalf("saturated key claimed %d, want %d (queue was full)", got, satClaimedBefore)
	}

	closeOnce(blockRelease)
	brDrain(t, o, ctx)
}

// TestOutboxNewMessageWhileSlowKeyRuns: a new delivery for another key while a
// slow key is in flight is claimed without waiting for the slow key.
func TestOutboxNewMessageWhileSlowKeyRuns(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithMaxGoroutines(4), WithAdmissionLimit(8))
	if err != nil {
		t.Fatal(err)
	}

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

	t.Cleanup(func() { _ = o.Close() })
	t.Cleanup(func() { closeOnce(slowRelease) })

	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, slow); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventOtherType}, fast); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	insertDeliveryDirect(t, o, deliverySeed{
		Event:       newTestEvent("slow"),
		HandlerType: slow.Type,
		CreatedAt:   createdAt,
	})
	reconcileForTest(t, o)

	if _, err := o.fetchAndDispatch(ctx, false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-slowEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("slow did not enter")
	}

	// New delivery for a different key while slow is still running.
	_, fastDeliveryID := insertDeliveryDirect(t, o, deliverySeed{
		Event:       brOtherEvent("fast"),
		HandlerType: fast.Type,
		CreatedAt:   createdAt,
	})
	lcResolveSerial(t, o, fastDeliveryID, fast.Type)

	if _, err := o.fetchAndDispatch(ctx, false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-fastEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("new message on free key was not claimed while slow still held")
	}

	closeOnce(slowRelease)
	brDrain(t, o, ctx)
}

// TestOutboxPaginationlessBacklogDoesNotHideIdleKey replaces the v1
// OFFSET-pagination test. There is no paging any more: each dispatch key is
// selected separately, so a large backlog on a saturated key can neither hide
// an idle key nor claim more than the key's queue depth in one pass. The idle
// key's delivery must be claimed in the FIRST pass after it is due.
func TestOutboxPaginationlessBacklogDoesNotHideIdleKey(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	const queueDepth = 2
	o, err := NewOutbox(db, WithMaxGoroutines(2), WithAdmissionLimit(8), WithQueueDepth(queueDepth))
	if err != nil {
		t.Fatal(err)
	}

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

	// Close waits for in-flight handlers; always release before Close.
	t.Cleanup(func() { _ = o.Close() })
	t.Cleanup(func() { closeOnce(slowRelease) })

	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, slow); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventOtherType}, idle); err != nil {
		t.Fatal(err)
	}

	// A backlog far larger than any single fetch batch on the saturated key,
	// and one idle-key delivery that is strictly later by available_at.
	base := time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC)
	backlog := maxFetchBatch + 10
	for i := range backlog {
		ts := base.Add(time.Duration(i) * time.Second)
		insertDeliveryDirect(t, o, deliverySeed{
			Event:       newTestEvent(fmt.Sprintf("sat-%d", i)),
			HandlerType: slow.Type,
			CreatedAt:   ts,
		})
	}
	idleAt := base.Add(24 * time.Hour)
	insertDeliveryDirect(t, o, deliverySeed{
		Event:       brOtherEvent("idle"),
		HandlerType: idle.Type,
		CreatedAt:   idleAt,
	})
	reconcileForTest(t, o)

	// FIRST pass after both are due: the idle key must be served although the
	// saturated key sits in front of it with a huge backlog.
	n, err := o.fetchAndDispatch(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if n < 2 {
		t.Fatalf("claimed = %d, want at least sat+idle in the first pass", n)
	}
	satClaimed := brClaimedForKey(t, o, slow.Type)
	if satClaimed > queueDepth {
		t.Fatalf("saturated key claimed %d in one pass, want <= queue depth %d", satClaimed, queueDepth)
	}

	select {
	case <-slowEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("saturated key did not start")
	}
	select {
	case <-idleEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("idle key behind a full-key backlog was not claimed in the first pass")
	}
	// Let the idle delivery's finalize transaction commit before claiming again.
	waitUntil(t, 3*time.Second, func() bool { return countDeliveries(t, o) == backlog })

	// Further passes while the saturated handler is still blocked: the per-pass
	// claim for that key never exceeds its queue depth.
	for pass := range 4 {
		before := brClaimedForKey(t, o, slow.Type)
		if _, err := o.fetchAndDispatch(ctx, false); err != nil {
			t.Fatal(err)
		}
		after := brClaimedForKey(t, o, slow.Type)
		if after-before > queueDepth {
			t.Fatalf("pass %d claimed %d deliveries for the saturated key, want <= %d", pass, after-before, queueDepth)
		}
	}
	if brUnclaimedForKey(t, o, slow.Type) == 0 {
		t.Fatal("whole saturated backlog was claimed although its handler is blocked")
	}

	closeOnce(slowRelease)
	brDrain(t, o, ctx)
}

// TestOutboxMissingHandlerLeavesUnclaimed: a stored handler that is no longer
// registered is flagged unresolved at startup, never claimed and never deleted;
// the sibling delivery of the SAME publication is delivered independently.
func TestOutboxMissingHandlerLeavesUnclaimed(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	// Register a different handler than the one stored on the orphan delivery.
	live := mocks.NewEventHandler("live_handler")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, live); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	event := newTestEvent("orphan")
	publicationID, goneID := insertDeliveryDirect(t, o, deliverySeed{
		Event:       event,
		HandlerType: "gone_handler",
		CreatedAt:   createdAt,
	})
	// Same publication, resolvable recipient: must be delivered independently.
	liveID := brInsertSiblingDelivery(t, o, publicationID, event, live.Type, createdAt)

	// StartChecked reconciles: gone_handler becomes unresolved, live gets a key.
	if err := o.StartChecked(); err != nil {
		t.Fatal(err)
	}

	if !live.Wait(3 * time.Second) {
		t.Fatal("sibling delivery of the same publication was not delivered")
	}
	waitUntil(t, 3*time.Second, func() bool { return countDeliveries(t, o) == 1 })

	// The orphan is never claimed, not even by an explicit claim pass.
	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 0 {
		t.Fatalf("processBatch = %d, want 0 (unresolved delivery is never claimed)", processed)
	}

	rows := listDeliveries(t, o)
	if len(rows) != 1 {
		t.Fatalf("deliveries = %d, want 1 (orphan left in place)", len(rows))
	}
	row := rows[0]
	if row.ID != goneID {
		t.Fatalf("remaining delivery = %s, want orphan %s (live %s should be gone)", row.ID, goneID, liveID)
	}
	if row.TakenAt.Valid {
		t.Fatal("orphan delivery must stay unclaimed (taken_at NULL)")
	}
	if !row.UnresolvedAt.Valid {
		t.Fatal("orphan delivery must carry unresolved_at")
	}
	if row.DispatchKey != "" {
		t.Fatalf("orphan dispatch_key = %q, want NULL", row.DispatchKey)
	}
	// The publication stays alive while any delivery references it.
	if got := countPublications(t, o); got != 1 {
		t.Fatalf("publications = %d, want 1 (orphan delivery still references it)", got)
	}

	// Diagnostic error must be visible on Errors().
	var found error
	deadline := time.After(3 * time.Second)
wait:
	for {
		select {
		case err := <-o.Errors():
			if errors.Is(err, ErrUnresolvedHandler) && strings.Contains(err.Error(), "gone_handler") {
				found = err
				break wait
			}
		case <-deadline:
			break wait
		}
	}
	if found == nil {
		t.Fatal("expected ErrUnresolvedHandler diagnostics for gone_handler")
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
	// Registration closed + dispatch-key ring computed; rows are seeded and
	// resolved individually below so in-flight claims are never reset.
	prepareWithoutFetcher(t, o)

	createdAt := time.Now().Add(-time.Minute)
	// Phase 1: only hot-1, so it alone holds the single permit.
	_, h1 := insertDeliveryDirect(t, o, deliverySeed{
		Event:       newTestEvent("h1"),
		HandlerType: hot.Type,
		CreatedAt:   createdAt,
	})
	lcResolveSerial(t, o, h1, hot.Type)
	if _, err := o.fetchAndDispatch(ctx, false); err != nil {
		t.Fatal(err)
	}

	select {
	case <-hotEntered1:
	case <-time.After(3 * time.Second):
		t.Fatal("hot-1 did not enter")
	}

	// Phase 2: while hot-1 holds the permit, dispatch idle alone so its worker
	// is the first fairSem waiter.
	_, i1 := insertDeliveryDirect(t, o, deliverySeed{
		Event:       brOtherEvent("i1"),
		HandlerType: idle.Type,
		CreatedAt:   createdAt.Add(time.Millisecond),
	})
	lcResolveSerial(t, o, i1, idle.Type)
	if _, err := o.fetchAndDispatch(ctx, false); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool { return o.handleSem.hasWaiters() })

	// hot-2 sits in the hot key queue; when hot-1 finishes, its worker acquires
	// the permit only after the already waiting idle worker.
	_, h2 := insertDeliveryDirect(t, o, deliverySeed{
		Event:       newTestEvent("h2"),
		HandlerType: hot.Type,
		CreatedAt:   createdAt.Add(2 * time.Millisecond),
	})
	lcResolveSerial(t, o, h2, hot.Type)
	if _, err := o.fetchAndDispatch(ctx, false); err != nil {
		t.Fatal(err)
	}

	closeOnce(hotRelease1)

	select {
	case <-idleEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("idle did not enter after hot-1 released the permit")
	}

	brDrain(t, o, ctx)

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

// TestOutboxRoundRobinAcrossKeysWhenAdmissionLimited proves the v2 claim
// fairness: when free admission slots are fewer than the total demand, the
// round-robin pass gives every active dispatch key a quantum before any key
// gets a second delivery, FIFO by seq inside each key. A key that appears only
// after admission is saturated is served as soon as a slot frees — it does not
// wait for the earlier keys' whole backlog.
func TestOutboxRoundRobinAcrossKeysWhenAdmissionLimited(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	// Queue depth 1 bounds each key's quantum to a single delivery per pass,
	// so the pass has to rotate to serve the other keys.
	o, err := NewOutbox(db, WithMaxGoroutines(8), WithAdmissionLimit(3), WithQueueDepth(1))
	if err != nil {
		t.Fatal(err)
	}

	newHandler := func(name string, blocked bool) *brRecordingBlockHandler {
		h := &brRecordingBlockHandler{
			Type:    name,
			entered: make(chan string, 16),
			release: make(chan struct{}),
		}
		if !blocked {
			close(h.release)
		}
		return h
	}
	// Sorted dispatch keys: rr_a < rr_b < rr_c < rr_late.
	hotNames := []string{"rr_a", "rr_b", "rr_c"}
	hot := map[string]*brRecordingBlockHandler{}
	for _, name := range hotNames {
		hot[name] = newHandler(name, true)
	}
	late := newHandler("rr_late", false)

	t.Cleanup(func() { _ = o.Close() })
	t.Cleanup(func() {
		for _, h := range hot {
			closeOnce(h.release)
		}
	})

	for _, name := range hotNames {
		if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, hot[name]); err != nil {
			t.Fatal(err)
		}
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventOtherType}, late); err != nil {
		t.Fatal(err)
	}

	// Four publications, each with one delivery per hot handler (12 deliveries,
	// 4 per key). Insertion order gives per-key FIFO by seq.
	createdAt := time.Now().Add(-time.Minute)
	const publications = 4
	for i := range publications {
		event := newTestEvent(fmt.Sprintf("pub-%d", i))
		publicationID, _ := insertDeliveryDirect(t, o, deliverySeed{
			Event:       event,
			HandlerType: hotNames[0],
			CreatedAt:   createdAt.Add(time.Duration(i) * time.Millisecond),
		})
		for _, name := range hotNames[1:] {
			brInsertSiblingDelivery(t, o, publicationID, event, name, createdAt.Add(time.Duration(i)*time.Millisecond))
		}
	}
	reconcileForTest(t, o)

	// Remember each key's FIFO head before the pass.
	headSeq := map[string]int64{}
	for _, name := range hotNames {
		headSeq[name] = brMinSeqForKey(t, o, name)
	}

	// One pass with free slots (3) < demand (12).
	claimed, err := o.fetchAndDispatch(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if claimed != 3 {
		t.Fatalf("claimed = %d, want 3 (one per active key, admission limit 3)", claimed)
	}

	// Quantum fairness: every active key got exactly one — no key got a second
	// delivery before the others got their first.
	for _, name := range hotNames {
		if got := brClaimedForKey(t, o, name); got != 1 {
			t.Fatalf("key %s claimed %d deliveries in the pass, want exactly 1 before any key gets a second", name, got)
		}
	}
	// FIFO inside each key: the claimed delivery is the key's lowest seq.
	for _, row := range listDeliveries(t, o) {
		if !row.TakenAt.Valid {
			continue
		}
		if row.Seq != headSeq[row.DispatchKey] {
			t.Fatalf("key %s claimed seq %d, want FIFO head %d", row.DispatchKey, row.Seq, headSeq[row.DispatchKey])
		}
	}
	for _, name := range hotNames {
		select {
		case <-hot[name].entered:
		case <-time.After(3 * time.Second):
			t.Fatalf("handler %s did not enter", name)
		}
	}
	if used, limit := o.AdmissionSnapshot(); used != 3 || limit != 3 {
		t.Fatalf("AdmissionSnapshot = (%d, %d), want (3, 3)", used, limit)
	}

	// A new key arrives after admission is saturated.
	_, lateDeliveryID := insertDeliveryDirect(t, o, deliverySeed{
		Event:       brOtherEvent("late"),
		HandlerType: late.Type,
		CreatedAt:   time.Now().Add(-time.Second),
	})
	lcResolveSerial(t, o, lateDeliveryID, late.Type)

	if n, err := o.fetchAndDispatch(ctx, false); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatalf("claimed = %d while admission is full, want 0", n)
	}

	// Free exactly one slot by releasing the first key's handler.
	closeOnce(hot[hotNames[0]].release)
	waitUntil(t, 3*time.Second, func() bool {
		used, _ := o.AdmissionSnapshot()
		return used == 2
	})

	// Backlog of the earlier keys is still there; the late key must not wait for it.
	for _, name := range hotNames {
		if got := brUnclaimedForKey(t, o, name); got < 3 {
			t.Fatalf("key %s backlog = %d, want >= 3 (late key must not wait for it)", name, got)
		}
	}

	if n, err := o.fetchAndDispatch(ctx, false); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("claimed = %d after one slot freed, want 1 (the late key)", n)
	}
	select {
	case <-late.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("late key was not served as soon as a slot freed")
	}

	// Release everything and drain; per-key delivery order must stay FIFO.
	for _, h := range hot {
		closeOnce(h.release)
	}
	brDrain(t, o, ctx)

	want := make([]string, 0, publications)
	for i := range publications {
		want = append(want, fmt.Sprintf("pub-%d", i))
	}
	for _, name := range hotNames {
		got := hot[name].Seen()
		if !slicesEqual(got, want) {
			t.Fatalf("key %s handled %v, want FIFO %v", name, got, want)
		}
	}
	if got := late.Seen(); len(got) != 1 || got[0] != "late" {
		t.Fatalf("late key handled %v, want [late]", got)
	}
}

// brRecordingBlockHandler records the contents it handled (per key FIFO
// assertions) and blocks until its release channel is closed.
type brRecordingBlockHandler struct {
	Type    string
	entered chan string
	release chan struct{}

	mu   sync.Mutex
	seen []string
}

func (h *brRecordingBlockHandler) HandlerType() eh.EventHandlerType {
	return eh.EventHandlerType(h.Type)
}

func (h *brRecordingBlockHandler) HandleEvent(_ context.Context, event eh.Event) error {
	content := eventContent(event)
	h.mu.Lock()
	h.seen = append(h.seen, content)
	h.mu.Unlock()
	select {
	case h.entered <- content:
	default:
	}
	<-h.release
	return nil
}

func (h *brRecordingBlockHandler) Seen() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.seen...)
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
