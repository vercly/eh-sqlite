package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	eh "github.com/vercly/eventhorizon"
	"github.com/vercly/eventhorizon/mocks"
	"github.com/vercly/eventhorizon/uuid"
)

func TestWithQueueDepthDefaultIs32(t *testing.T) {
	db := newTestDB(t)
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	if o.queueDepth != DefaultQueueDepth {
		t.Fatalf("queueDepth = %d, want DefaultQueueDepth=%d", o.queueDepth, DefaultQueueDepth)
	}
	if DefaultQueueDepth != 32 {
		t.Fatalf("DefaultQueueDepth = %d, want 32", DefaultQueueDepth)
	}
	if o.dispatch == nil || o.dispatch.queueDepth != 32 {
		t.Fatalf("registry queueDepth = %v, want 32", o.dispatch)
	}
}

func TestWithQueueDepthValid(t *testing.T) {
	db := newTestDB(t)
	o, err := NewOutbox(db, WithQueueDepth(7))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	if o.queueDepth != 7 {
		t.Fatalf("queueDepth = %d, want 7", o.queueDepth)
	}
}

func TestWithQueueDepthRejectsLessThanOne(t *testing.T) {
	db := newTestDB(t)
	for _, depth := range []int{0, -1, -99} {
		_, err := NewOutbox(db, WithQueueDepth(depth))
		if err == nil {
			t.Fatalf("WithQueueDepth(%d) error = nil, want ErrInvalidQueueDepth", depth)
		}
		if !errors.Is(err, ErrInvalidQueueDepth) {
			t.Fatalf("WithQueueDepth(%d) error = %v, want ErrInvalidQueueDepth", depth, err)
		}
	}
}

func TestWithQueueDepthEnforcesCapacity(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	// depth=1 bounds the waiting queue only; a running HandleEvent is in-flight
	// and has already left the queue after pop.
	o, err := NewOutbox(db, WithQueueDepth(1), WithMaxGoroutines(1), WithAdmissionLimit(16))
	if err != nil {
		t.Fatal(err)
	}

	release := make(chan struct{})
	entered := make(chan string, 8)
	handler := newBlockingHandler("cap_handler", entered, release)
	// Close waits for in-flight handlers; always release before Close on every path.
	t.Cleanup(func() { _ = o.Close() })
	t.Cleanup(func() { closeOnce(release) })

	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	for i := range 3 {
		seedOutboxEventWithID(t, db, o, uuid.New().String(), newTestEvent(fmtContent(i)),
			[]string{handler.Type}, createdAt, createdAt, sql.NullTime{})
	}

	// Claim #1: one delivery is popped and enters HandleEvent (queue slot freed).
	n1, err := o.fetchAndDispatch(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if n1 != 1 {
		t.Fatalf("claim #1 = %d, want 1", n1)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not enter (queue slot not yet freed by pop)")
	}

	// Claim #2: fills the single waiting-queue slot while #1 is still in-flight.
	n2, err := o.fetchAndDispatch(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if n2 != 1 {
		t.Fatalf("claim #2 = %d, want 1 (waiting queue)", n2)
	}

	// Claim #3: queue full (1 waiting) + 1 in-flight → cannot admit another.
	n3, err := o.fetchAndDispatch(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if n3 != 0 {
		t.Fatalf("claim #3 = %d, want 0 while waiting queue full", n3)
	}

	closeOnce(release)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, _ = o.fetchAndDispatch(ctx, true)
		if outboxRowCount(t, db) == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("leftover rows = %d", outboxRowCount(t, db))
}

func fmtContent(i int) string {
	return string(rune('a' + i%26))
}

// recordingDispatchStats captures Observe* calls for transition tests.
type recordingDispatchStats struct {
	mu     sync.Mutex
	depths []statSample
	flight []statSample
}

type statSample struct {
	handler string
	shard   string
	n       int
}

func (r *recordingDispatchStats) ObserveQueueDepth(handler, shard string, depth int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.depths = append(r.depths, statSample{handler: handler, shard: shard, n: depth})
}

func (r *recordingDispatchStats) ObserveInFlight(handler, shard string, n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.flight = append(r.flight, statSample{handler: handler, shard: shard, n: n})
}

func (r *recordingDispatchStats) maxDepth(handler string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	max := 0
	for _, s := range r.depths {
		if s.handler == handler && s.n > max {
			max = s.n
		}
	}
	return max
}

func (r *recordingDispatchStats) sawShard(handler, shard string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.depths {
		if s.handler == handler && s.shard == shard {
			return true
		}
	}
	for _, s := range r.flight {
		if s.handler == handler && s.shard == shard {
			return true
		}
	}
	return false
}

func (r *recordingDispatchStats) maxInFlight(handler string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	max := 0
	for _, s := range r.flight {
		if s.handler == handler && s.n > max {
			max = s.n
		}
	}
	return max
}

func TestDispatchStatsEnqueuePopEnterExit(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	stats := &recordingDispatchStats{}
	o, err := NewOutbox(db, WithQueueDepth(4), WithDispatchStats(stats), WithMaxGoroutines(2))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = o.Close() })

	handler := mocks.NewEventHandler("stats_serial")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	for range 3 {
		seedOutboxEventWithID(t, db, o, uuid.New().String(), newTestEvent("x"),
			[]string{handler.Type}, createdAt, createdAt, sql.NullTime{})
	}

	if _, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	}
	if !handler.Wait(2 * time.Second) {
		t.Fatal("handler did not complete")
	}

	if !stats.sawShard(handler.Type, serialShardLabel) {
		t.Fatalf("expected serial shard label %q", serialShardLabel)
	}
	if got := stats.maxDepth(handler.Type); got < 1 {
		t.Fatalf("max queue depth samples = %d, want >= 1", got)
	}
	if got := stats.maxInFlight(handler.Type); got < 1 {
		t.Fatalf("max in-flight = %d, want >= 1", got)
	}
	// After drain, last samples for this handler should return to zero.
	stats.mu.Lock()
	lastFlight, lastDepth := -1, -1
	for i := len(stats.flight) - 1; i >= 0; i-- {
		if stats.flight[i].handler == handler.Type {
			lastFlight = stats.flight[i].n
			break
		}
	}
	for i := len(stats.depths) - 1; i >= 0; i-- {
		if stats.depths[i].handler == handler.Type {
			lastDepth = stats.depths[i].n
			break
		}
	}
	stats.mu.Unlock()
	if lastFlight != 0 {
		t.Fatalf("final in-flight = %d, want 0 after exit", lastFlight)
	}
	if lastDepth != 0 {
		t.Fatalf("final queue depth = %d, want 0 after drain", lastDepth)
	}
}

// TestDispatchStatsShutdownAbandonsQueued clears gauges and leaves abandoned
// claimed rows for startup redelivery (not marked completed).
func TestDispatchStatsShutdownAbandonsQueued(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	stats := &recordingDispatchStats{}
	o, err := NewOutbox(db, WithQueueDepth(4), WithDispatchStats(stats), WithMaxGoroutines(1), WithAdmissionLimit(8))
	if err != nil {
		t.Fatal(err)
	}

	release := make(chan struct{})
	entered := make(chan string, 4)
	handler := newBlockingHandler("shutdown_stats", entered, release)
	t.Cleanup(func() { closeOnce(release) })

	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	idRun := uuid.New().String()
	idQueued := uuid.New().String()
	seedOutboxEventWithID(t, db, o, idRun, newTestEvent("run"), []string{handler.Type}, createdAt, createdAt, sql.NullTime{})
	seedOutboxEventWithID(t, db, o, idQueued, newTestEvent("queued"), []string{handler.Type}, createdAt.Add(time.Millisecond), createdAt.Add(time.Millisecond), sql.NullTime{})

	// Claim both: first runs, second waits in queue.
	if n, err := o.fetchAndDispatch(ctx, false); err != nil || n != 2 {
		t.Fatalf("claim both: n=%d err=%v, want 2", n, err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("running handler did not enter")
	}

	// Close while one is running and one is queued; release running so Close can finish.
	doneClose := make(chan error, 1)
	go func() { doneClose <- o.Close() }()
	// Wait until Close has marked shutdown so the queued delivery is abandoned,
	// not executed as a normal success.
	deadlineSD := time.Now().Add(2 * time.Second)
	for !o.shuttingDown.Load() && time.Now().Before(deadlineSD) {
		time.Sleep(1 * time.Millisecond)
	}
	if !o.shuttingDown.Load() {
		t.Fatal("Close did not set shuttingDown")
	}
	closeOnce(release)

	select {
	case err := <-doneClose:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return")
	}

	// Queued delivery abandoned: not finalized as success; row remains claimed.
	if got := outboxRowCount(t, db); got != 1 {
		t.Fatalf("outbox rows after Close = %d, want 1 (abandoned queued)", got)
	}
	var remaining string
	var takenAt sql.NullTime
	if err := db.QueryRow(`SELECT id, taken_at FROM outbox`).Scan(&remaining, &takenAt); err != nil {
		t.Fatal(err)
	}
	if remaining != idQueued {
		t.Fatalf("remaining id = %s, want queued %s", remaining, idQueued)
	}
	if !takenAt.Valid {
		t.Fatal("abandoned queued row should keep taken_at for startup-reset redelivery")
	}

	stats.mu.Lock()
	lastDepth, lastFlight := -1, -1
	for i := len(stats.depths) - 1; i >= 0; i-- {
		if stats.depths[i].handler == handler.Type {
			lastDepth = stats.depths[i].n
			break
		}
	}
	for i := len(stats.flight) - 1; i >= 0; i-- {
		if stats.flight[i].handler == handler.Type {
			lastFlight = stats.flight[i].n
			break
		}
	}
	stats.mu.Unlock()
	if lastDepth != 0 {
		t.Fatalf("final queue depth = %d, want 0 after shutdown drain", lastDepth)
	}
	if lastFlight != 0 {
		t.Fatalf("final in-flight = %d, want 0 after shutdown", lastFlight)
	}
}

// TestDispatchStatsPanicDoesNotBreakDelivery ensures Observe* panics are
// recovered and do not prevent finalization.
func TestDispatchStatsPanicDoesNotBreakDelivery(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithDispatchStats(panickingDispatchStats{}), WithMaxGoroutines(2))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = o.Close() })

	handler := mocks.NewEventHandler("panic_stats_handler")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	seedOutboxEventWithID(t, db, o, uuid.New().String(), newTestEvent("ok"),
		[]string{handler.Type}, createdAt, createdAt, sql.NullTime{})

	if _, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	}
	if !handler.Wait(2 * time.Second) {
		t.Fatal("handler did not run despite panicking collector")
	}
	if got := outboxRowCount(t, db); got != 0 {
		t.Fatalf("outbox rows = %d, want 0 (finalized despite stats panic)", got)
	}
}

type panickingDispatchStats struct{}

func (panickingDispatchStats) ObserveQueueDepth(string, string, int) {
	panic("queue depth observer boom")
}

func (panickingDispatchStats) ObserveInFlight(string, string, int) {
	panic("in-flight observer boom")
}

func TestDispatchStatsPartitionUsesNumericShardLabel(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	stats := &recordingDispatchStats{}
	o, err := NewOutbox(db, WithDispatchStats(stats), WithMaxGoroutines(2))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = o.Close() })

	handler := mocks.NewEventHandler("stats_part")
	if err := o.AddHandlerWithOptions(ctx, eh.MatchEvents{mocks.EventType}, handler,
		WithDispatchMode(PartitionByAggregate), WithPartitionShards(8)); err != nil {
		t.Fatal(err)
	}

	agg := uuid.New()
	createdAt := time.Now().Add(-time.Minute)
	seedOutboxEventWithID(t, db, o, uuid.New().String(), newTestEventForAggregate("p", agg),
		[]string{handler.Type}, createdAt, createdAt, sql.NullTime{})

	if _, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	}
	if !handler.Wait(2 * time.Second) {
		t.Fatal("handler wait")
	}

	stats.mu.Lock()
	defer stats.mu.Unlock()
	foundNumeric := false
	for _, s := range stats.depths {
		if s.handler != handler.Type {
			continue
		}
		if s.shard == serialShardLabel {
			t.Fatalf("partition queue used serial shard label")
		}
		// Must be a small integer string, not a UUID/correlation id.
		if len(s.shard) > 2 {
			t.Fatalf("shard label %q looks like an id, want shard index", s.shard)
		}
		foundNumeric = true
	}
	if !foundNumeric {
		t.Fatal("no partition depth samples")
	}
}
