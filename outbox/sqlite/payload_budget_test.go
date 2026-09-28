package sqlite

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	eh "github.com/vercly/eventhorizon"
)

func TestPayloadBudgetReleasesAfterEachRecipient(t *testing.T) {
	db := newTestDB(t)
	o := v2NewOutbox(t, db, WithPayloadBudget(4096))
	event := newTestEvent(strings.Repeat("x", 2500))
	a := &v2CountingHandler{typ: "a"}
	b := &v2CountingHandler{typ: "b"}
	for _, h := range []*v2CountingHandler{a, b} {
		if err := o.AddHandler(context.Background(), eh.MatchAll{}, h); err != nil {
			t.Fatal(err)
		}
	}
	pub, _ := insertDeliveryDirect(t, o, deliverySeed{Event: event, HandlerType: "a", CreatedAt: time.Now().Add(-time.Minute)})
	brInsertSiblingDelivery(t, o, pub, event, "b", time.Now().Add(-time.Minute))
	reconcileForTest(t, o)
	planned, _, err := o.claimPass(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(planned) != 1 {
		t.Fatalf("claimed=%d want1 (two payloads exceed byte budget)", len(planned))
	}
	if planned[0].doc.Event == nil {
		t.Fatal("event was not decoded for matching")
	}
	o.dispatch.enqueue(planned[0])
	planned[0].wait()
	if planned[0].doc.Event != nil || planned[0].doc.EventBlob != "" {
		t.Fatal("completed delivery retains payload")
	}
	n, err := o.processBatch(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("next=%d %v", n, err)
	}
	if a.calls.Load() != 1 || b.calls.Load() != 1 || countDeliveries(t, o) != 0 {
		t.Fatal("recipient lost")
	}
	if o.payloads.used != 0 {
		t.Fatal("budget leaked")
	}
}

func TestPayloadBudgetOversizePreservedAndRestartRecoverable(t *testing.T) {
	for _, sentinel := range []bool{false, true} {
		t.Run(map[bool]string{false: "delivery", true: "sentinel"}[sentinel], func(t *testing.T) {
			db := newTestDB(t)
			o := v2NewOutbox(t, db, WithPayloadBudget(1024))
			h := &v2CountingHandler{typ: "reader"}
			if err := o.AddHandler(context.Background(), eh.MatchAll{}, h); err != nil {
				t.Fatal(err)
			}
			typ := "reader"
			if sentinel {
				typ = ""
			}
			_, id := insertDeliveryDirect(t, o, deliverySeed{Event: newTestEvent(strings.Repeat("z", 2048)), HandlerType: typ, CreatedAt: time.Now().Add(-time.Minute)})
			reconcileForTest(t, o)
			if _, err := o.processBatch(context.Background()); err != nil {
				t.Fatal(err)
			}
			var blocked int
			if err := db.QueryRow("SELECT count(*) FROM outbox_deliveries WHERE id=? AND unresolved_at IS NOT NULL AND taken_at IS NULL", id).Scan(&blocked); err != nil {
				t.Fatal(err)
			}
			if blocked != 1 || h.calls.Load() != 0 || countPublications(t, o) != 1 {
				t.Fatal("oversize was not preserved unresolved")
			}
			select {
			case err := <-o.Errors():
				if !errors.Is(err, ErrPayloadTooLarge) {
					t.Fatal(err)
				}
			default:
				t.Fatal("missing oversize diagnostic")
			}
			if err := o.Close(); err != nil {
				t.Fatal(err)
			}
			next := v2NewOutbox(t, db, WithPayloadBudget(4096))
			if err := next.AddHandler(context.Background(), eh.MatchAll{}, h); err != nil {
				t.Fatal(err)
			}
			reconcileForTest(t, next)
			v2Drain(t, next)
			if h.calls.Load() != 1 || countDeliveries(t, next) != 0 {
				t.Fatal("preserved delivery not recovered with higher budget")
			}
		})
	}
}

func TestPayloadBudgetRollbackReleasesReservations(t *testing.T) {
	o := v2NewOutbox(t, newTestDB(t), WithPayloadBudget(4096))
	h := &v2CountingHandler{typ: "reader"}
	if err := o.AddHandler(context.Background(), eh.MatchAll{}, h); err != nil {
		t.Fatal(err)
	}
	insertDeliveryDirect(t, o, deliverySeed{Event: newTestEvent("data"), HandlerType: "reader", CreatedAt: time.Now().Add(-time.Minute)})
	reconcileForTest(t, o)
	o.beforeClaimCommit = func(int) error { return errors.New("injected rollback") }
	if _, _, err := o.claimPass(context.Background()); err == nil {
		t.Fatal("expected rollback")
	}
	if o.payloads.used != 0 || len(o.payloads.active) != 0 {
		t.Fatal("rollback leaked budget")
	}
	o.beforeClaimCommit = nil
	if n, err := o.processBatch(context.Background()); err != nil || n != 1 {
		t.Fatalf("retry %d %v", n, err)
	}
}

func TestKeyQueuePopDoesNotRetainPayload(t *testing.T) {
	a, b := &delivery{}, &delivery{}
	backing := []*delivery{a, b}
	q := newKeyQueue("q", "h", "none", 32, nil)
	q.items = backing
	q.depth = 2
	if got, ok := q.pop(); !ok || got != a {
		t.Fatal("wrong first delivery")
	}
	if backing[0] != nil {
		t.Fatal("popped pointer retained")
	}
	q.pop()
	if backing[1] != nil || q.items != nil {
		t.Fatal("empty queue retains backing array")
	}
}

func TestPayloadBlockedSentinelCannotBeOvertaken(t *testing.T) {
	o := v2NewOutbox(t, newTestDB(t), WithPayloadBudget(4096))
	h := &v2CountingHandler{typ: "reader"}
	if err := o.AddHandler(context.Background(), eh.MatchAll{}, h); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(-time.Minute)
	insertDeliveryDirect(t, o, deliverySeed{Event: newTestEvent(strings.Repeat("x", 2500)), CreatedAt: now})
	insertDeliveryDirect(t, o, deliverySeed{Event: newTestEvent("small"), HandlerType: "reader", CreatedAt: now.Add(time.Second)})
	reconcileForTest(t, o)
	if !o.payloads.reserve("in-flight", 2048) {
		t.Fatal("reserve")
	}
	if n, err := o.processBatch(context.Background()); err != nil || n != 0 {
		t.Fatalf("overtook sentinel: %d %v", n, err)
	}
	o.payloads.release("in-flight")
	v2Drain(t, o)
	if h.calls.Load() != 2 || countDeliveries(t, o) != 0 {
		t.Fatal("did not recover after capacity release")
	}
}

func TestPayloadDiagnosticDoesNotRetainLargeEvent(t *testing.T) {
	event := newTestEvent(strings.Repeat("x", 1<<20))
	doc := &deliveryDoc{Event: event, PayloadBytes: 1 << 20}
	got := payloadDiagnostic(doc)
	if got.Data() != nil || got.Metadata() != nil {
		t.Fatal("large diagnostic retained content")
	}
	if got.EventType() != event.EventType() || got.AggregateID() != event.AggregateID() {
		t.Fatal("diagnostic identity lost")
	}
}

func TestPayloadBudgetRejectsInvalidLimit(t *testing.T) {
	for _, n := range []int64{0, -1} {
		if _, err := NewOutbox(newTestDB(t), WithPayloadBudget(n)); err == nil {
			t.Fatalf("accepted %d", n)
		}
	}
}

func TestOversizeDoesNotBlockSmallSibling(t *testing.T) {
	o := v2NewOutbox(t, newTestDB(t), WithPayloadBudget(1024))
	h := &v2CountingHandler{typ: "reader"}
	if err := o.AddHandler(context.Background(), eh.MatchAll{}, h); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(-time.Minute)
	insertDeliveryDirect(t, o, deliverySeed{Event: newTestEvent(strings.Repeat("x", 2048)), HandlerType: "reader", CreatedAt: now})
	insertDeliveryDirect(t, o, deliverySeed{Event: newTestEvent("small"), HandlerType: "reader", CreatedAt: now.Add(time.Second)})
	reconcileForTest(t, o)
	v2Drain(t, o)
	if h.calls.Load() != 1 || countDeliveries(t, o) != 1 {
		t.Fatal("small sibling blocked or oversize lost")
	}
}

func TestQuarantinedSentinelBatchesDoNotDelayNormalClaims(t *testing.T) {
	o := v2NewOutbox(t, newTestDB(t), WithPayloadBudget(1024))
	h := &v2CountingHandler{typ: "reader"}
	if err := o.AddHandler(context.Background(), eh.MatchAll{}, h); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(-time.Minute)
	for range claimQuantum + 2 {
		insertDeliveryDirect(t, o, deliverySeed{Event: newTestEvent(strings.Repeat("x", 2048)), CreatedAt: now})
	}
	insertDeliveryDirect(t, o, deliverySeed{Event: newTestEvent("small"), HandlerType: "reader", CreatedAt: now})
	reconcileForTest(t, o)
	if n, err := o.processBatch(context.Background()); err != nil || n != 1 {
		t.Fatalf("normal claim delayed: %d %v", n, err)
	}
	var unresolved int
	if err := o.db.QueryRow("SELECT count(*) FROM outbox_deliveries WHERE unresolved_at IS NOT NULL").Scan(&unresolved); err != nil {
		t.Fatal(err)
	}
	if unresolved != claimQuantum+2 || h.calls.Load() != 1 {
		t.Fatalf("unresolved=%d calls=%d", unresolved, h.calls.Load())
	}
}

func TestLargePayloadProgressesWhenBudgetDrains(t *testing.T) {
	o := v2NewOutbox(t, newTestDB(t), WithPayloadBudget(4096))
	a, b := &v2CountingHandler{typ: "a-large"}, &v2CountingHandler{typ: "b-small"}
	for _, h := range []*v2CountingHandler{a, b} {
		if err := o.AddHandler(context.Background(), eh.MatchAll{}, h); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().Add(-time.Minute)
	insertDeliveryDirect(t, o, deliverySeed{Event: newTestEvent(strings.Repeat("x", 2500)), HandlerType: "a-large", CreatedAt: now})
	insertDeliveryDirect(t, o, deliverySeed{Event: newTestEvent("small"), HandlerType: "b-small", CreatedAt: now})
	reconcileForTest(t, o)
	if !o.payloads.reserve("other-worker", 2048) {
		t.Fatal("reserve")
	}
	if n, err := o.processBatch(context.Background()); err != nil || n != 0 {
		t.Fatalf("blocked head did not protect capacity: %d %v", n, err)
	}
	if a.calls.Load() != 0 || b.calls.Load() != 0 {
		t.Fatal("budget was exceeded")
	}
	o.payloads.release("other-worker")
	if n, err := o.processBatch(context.Background()); err != nil || n != 2 {
		t.Fatalf("large did not progress: %d %v", n, err)
	}
	if a.calls.Load() != 1 || countDeliveries(t, o) != 0 {
		t.Fatal("large delivery starved after budget drained")
	}
}

func TestPayloadBudgetProtectsLargeHeadFromSmallTraffic(t *testing.T) {
	b := newPayloadBudget(100)
	if !b.reserve("running", 70) {
		t.Fatal("initial reserve")
	}
	if b.reserve("large", 80) {
		t.Fatal("large exceeded budget")
	}
	if b.reserve("small", 10) {
		t.Fatal("small consumed capacity needed by waiting head")
	}
	b.release("running")
	if !b.reserve("small", 20) {
		t.Fatal("spare capacity should be usable")
	}
	if b.reserve("another-small", 1) {
		t.Fatal("waiting head lost its protected capacity")
	}
	if !b.reserve("large", 80) {
		t.Fatal("large starved after existing worker finished")
	}
	b.release("large")
	b.release("small")
	if b.used != 0 || b.waitingID != "" {
		t.Fatal("budget or waiter leaked")
	}
}

func TestPayloadBudgetSentinelPreemptsWaitingNormalHead(t *testing.T) {
	o := v2NewOutbox(t, newTestDB(t), WithPayloadBudget(4096))
	h := &v2CountingHandler{typ: "reader"}
	if err := o.AddHandler(context.Background(), eh.MatchAll{}, h); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(-time.Minute)
	insertDeliveryDirect(t, o, deliverySeed{Event: newTestEvent(strings.Repeat("x", 2500)), CreatedAt: now})
	_, normalID := insertDeliveryDirect(t, o, deliverySeed{Event: newTestEvent(strings.Repeat("y", 2500)), HandlerType: "reader", CreatedAt: now.Add(time.Second)})
	reconcileForTest(t, o)
	if !o.payloads.reserve("running", 2048) {
		t.Fatal("reserve")
	}
	if o.payloads.reserve(normalID, 3000) {
		t.Fatal("should wait")
	}
	o.payloads.release("running")
	// Without sentinel priority, both the sentinel and protected normal head
	// wait forever even though the byte budget is empty.
	v2Drain(t, o)
	if h.calls.Load() != 2 || countDeliveries(t, o) != 0 {
		t.Fatal("sentinel/normal-head budget deadlock")
	}
}
