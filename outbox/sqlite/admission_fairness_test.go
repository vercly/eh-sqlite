package sqlite

import (
	"context"
	"fmt"
	"testing"
	"time"

	eh "github.com/vercly/eventhorizon"
	"github.com/vercly/eventhorizon/mocks"
)

// TestAdmissionMinimumGuaranteeServesIdleHandler: a slow partitioned handler
// with backlog on every shard fills admission. When one of its deliveries
// finishes, the freed slot goes to a handler with nothing admitted, not to the
// slow handler's next shard in ring order.
func TestAdmissionMinimumGuaranteeServesIdleHandler(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o := v2NewOutbox(t, db, WithAdmissionLimit(2), WithQueueDepth(8), WithMaxGoroutines(4))
	gate, _ := v2Gate(t)
	slow := &v2CountingHandler{typ: "slow", gate: gate, entered: make(chan struct{}, 64)}
	fast := &v2CountingHandler{typ: "z_fast", entered: make(chan struct{}, 4)}
	if err := o.AddHandlerWithOptions(ctx, eh.MatchEvents{mocks.EventType}, slow,
		WithDispatchMode(PartitionByAggregate), WithPartitionShards(4)); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventOtherType}, fast); err != nil {
		t.Fatal(err)
	}
	prepareWithoutFetcher(t, o)
	for i := range 40 {
		v2Publish(t, o, fmt.Sprintf("slow-%d", i)) // random aggregates: backlog on every shard
	}

	pass := func() int {
		t.Helper()
		n, err := o.fetchAndDispatch(ctx, false)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := pass(); n != 2 {
		t.Fatalf("first pass claimed %d, want 2 (admission limit)", n)
	}
	for range 2 {
		select {
		case <-slow.entered:
		case <-time.After(3 * time.Second):
			t.Fatal("slow handler did not enter")
		}
	}

	if err := o.HandleEvent(ctx, brOtherEvent("fast")); err != nil {
		t.Fatal(err)
	}
	if n := pass(); n != 0 {
		t.Fatalf("claimed %d while admission is full, want 0", n)
	}

	gate <- struct{}{} // finish exactly one slow delivery
	waitUntil(t, 3*time.Second, func() bool { used, _ := o.AdmissionSnapshot(); return used == 1 })
	if n := pass(); n != 1 {
		t.Fatalf("claimed %d after one slot freed, want 1", n)
	}
	select {
	case <-fast.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("freed slot went to the slow handler; idle handler was not served")
	}
}
