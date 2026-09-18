package sqlite

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	eh "github.com/vercly/eventhorizon"
	"github.com/vercly/eventhorizon/mocks"
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
	if o.dispatch.queueDepth != 7 {
		t.Fatalf("registry queueDepth = %d, want 7", o.dispatch.queueDepth)
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

// TestWithQueueDepthEnforcesCapacity pins the v2 per-key bound: a claim pass
// never reserves more than the queue's free capacity, so with depth N and a
// blocking handler the key ends up holding exactly N queued deliveries plus the
// one that already left the queue into HandleEvent. Everything else stays
// unclaimed (taken_at NULL) and is claimed only after capacity frees up.
func TestWithQueueDepthEnforcesCapacity(t *testing.T) {
	const depth = 2
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithQueueDepth(depth), WithMaxGoroutines(1), WithAdmissionLimit(16))
	if err != nil {
		t.Fatal(err)
	}

	gate := qdNewGate()
	entered := make(chan string, 8)
	handler := newBlockingHandler("cap_handler", entered, gate.ch)
	// Close waits for in-flight handlers; cleanups run LIFO, so the gate is
	// opened before Close on every path.
	t.Cleanup(func() { _ = o.Close() })
	t.Cleanup(gate.open)

	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}
	qdSeed(t, o, handler.Type, 5)
	qdReconcile(t, o)

	// Claim #1 fills the whole queue (depth 2); the worker pops one delivery
	// into HandleEvent, which frees exactly one slot.
	if n, err := o.fetchAndDispatch(ctx, false); err != nil || n != depth {
		t.Fatalf("claim #1 = %d, err = %v, want %d", n, err, depth)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not enter (queue slot not freed by pop)")
	}

	// Claim #2 may only take the single freed slot.
	if n, err := o.fetchAndDispatch(ctx, false); err != nil || n != 1 {
		t.Fatalf("claim #2 = %d, err = %v, want 1", n, err)
	}

	// Claim #3: queue full again → nothing may be claimed.
	if n, err := o.fetchAndDispatch(ctx, false); err != nil || n != 0 {
		t.Fatalf("claim #3 = %d, err = %v, want 0 while the queue is full", n, err)
	}

	q := o.dispatch.getExisting(handler.Type)
	if q == nil {
		t.Fatal("no queue created for the serial dispatch key")
	}
	if free := q.freeCapacity(); free != 0 {
		t.Fatalf("queue free capacity = %d, want 0 (enqueued+reserved == depth)", free)
	}
	// depth queued + 1 executing are admitted for this key; nothing more.
	if got := len(o.admission.idsForKey(handler.Type)); got != depth+1 {
		t.Fatalf("admitted deliveries for key = %d, want %d (%d queued + 1 executing)", got, depth+1, depth)
	}
	if used, _ := o.AdmissionSnapshot(); used != depth+1 {
		t.Fatalf("admission used = %d, want %d", used, depth+1)
	}

	unclaimed := 0
	for _, row := range listDeliveries(t, o) {
		if !row.TakenAt.Valid {
			unclaimed++
		}
	}
	if want := 5 - (depth + 1); unclaimed != want {
		t.Fatalf("unclaimed deliveries (taken_at NULL) = %d, want %d", unclaimed, want)
	}

	// Releasing lets the backlog drain completely through repeated passes.
	gate.open()
	waitUntil(t, 10*time.Second, func() bool {
		if _, err := o.fetchAndDispatch(ctx, false); err != nil {
			t.Fatal(err)
		}
		qdDrain(entered)
		return countDeliveries(t, o) == 0
	})
	if got := countPublications(t, o); got != 0 {
		t.Fatalf("publications left = %d, want 0", got)
	}
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

// lastFor returns the last depth / in-flight sample recorded for handler, or -1.
func (r *recordingDispatchStats) lastFor(handler string) (depth, flight int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	depth, flight = -1, -1
	for i := len(r.depths) - 1; i >= 0; i-- {
		if r.depths[i].handler == handler {
			depth = r.depths[i].n
			break
		}
	}
	for i := len(r.flight) - 1; i >= 0; i-- {
		if r.flight[i].handler == handler {
			flight = r.flight[i].n
			break
		}
	}
	return depth, flight
}

// assertBoundedLabels fails when any observed label carries something other
// than the handler type and a bounded shard label (never a delivery /
// publication / aggregate id).
func (r *recordingDispatchStats) assertBoundedLabels(t testing.TB, handler string) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, group := range [][]statSample{r.depths, r.flight} {
		for _, s := range group {
			if s.handler != handler {
				t.Fatalf("unexpected handler label %q, want %q", s.handler, handler)
			}
			if s.shard == serialShardLabel {
				continue
			}
			if _, err := strconv.Atoi(s.shard); err != nil {
				t.Fatalf("shard label %q is neither %q nor a shard index", s.shard, serialShardLabel)
			}
		}
	}
}

// TestDispatchStatsEnqueuePopEnterExit follows one key through enqueue, pop,
// HandleEvent enter and exit, and asserts both gauges are back at zero once the
// queue is drained.
func TestDispatchStatsEnqueuePopEnterExit(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	stats := &recordingDispatchStats{}
	o, err := NewOutbox(db, WithQueueDepth(4), WithDispatchStats(stats), WithMaxGoroutines(1))
	if err != nil {
		t.Fatal(err)
	}

	gate := qdNewGate()
	entered := make(chan string, 8)
	handler := newBlockingHandler("stats_serial", entered, gate.ch)
	t.Cleanup(func() { _ = o.Close() })
	t.Cleanup(gate.open)

	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}
	qdSeed(t, o, handler.Type, 3)
	qdReconcile(t, o)

	// All three are claimed and enqueued in one pass; the worker pops the first
	// one and blocks inside HandleEvent, so the queue is observably non-empty.
	if n, err := o.fetchAndDispatch(ctx, false); err != nil || n != 3 {
		t.Fatalf("claim = %d, err = %v, want 3", n, err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first handler did not enter")
	}

	if !stats.sawShard(handler.Type, serialShardLabel) {
		t.Fatalf("expected serial shard label %q", serialShardLabel)
	}
	if got := stats.maxDepth(handler.Type); got < 2 {
		t.Fatalf("max queue depth = %d, want >= 2 (two deliveries still waiting)", got)
	}
	if got := stats.maxInFlight(handler.Type); got < 1 {
		t.Fatalf("max in-flight = %d, want >= 1", got)
	}

	// Let the key drain: every remaining pop and HandleEvent exit is emitted by
	// the single key worker, so the final samples are deterministic.
	gate.open()
	waitUntil(t, 10*time.Second, func() bool {
		qdDrain(entered)
		return countDeliveries(t, o) == 0
	})

	lastDepth, lastFlight := stats.lastFor(handler.Type)
	if lastDepth != 0 {
		t.Fatalf("final queue depth = %d, want 0 after drain", lastDepth)
	}
	if lastFlight != 0 {
		t.Fatalf("final in-flight = %d, want 0 after exit", lastFlight)
	}
	stats.assertBoundedLabels(t, handler.Type)
}

// TestDispatchStatsShutdownAbandonsQueued: Close abandons queued deliveries.
// Their rows keep taken_at (redelivered after the startup reset), their handler
// never runs, admission drops back to zero and both gauges are cleared.
func TestDispatchStatsShutdownAbandonsQueued(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	stats := &recordingDispatchStats{}
	o, err := NewOutbox(db, WithQueueDepth(4), WithDispatchStats(stats), WithMaxGoroutines(1), WithAdmissionLimit(8))
	if err != nil {
		t.Fatal(err)
	}

	gate := qdNewGate()
	entered := make(chan string, 4)
	handler := newBlockingHandler("shutdown_stats", entered, gate.ch)
	t.Cleanup(gate.open)

	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}
	ids := qdSeed(t, o, handler.Type, 2)
	idRun, idQueued := ids[0], ids[1]
	qdReconcile(t, o)

	// Claim both: the first runs, the second waits in the queue.
	if n, err := o.fetchAndDispatch(ctx, false); err != nil || n != 2 {
		t.Fatalf("claim both: n=%d err=%v, want 2", n, err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("running handler did not enter")
	}

	doneClose := make(chan error, 1)
	go func() { doneClose <- o.Close() }()
	// Wait until Close marked shutdown so the queued delivery is abandoned
	// instead of being executed as a normal success.
	waitUntil(t, 5*time.Second, func() bool { return o.shuttingDown.Load() })
	gate.open()

	select {
	case err := <-doneClose:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return")
	}

	// The queued delivery was never executed.
	select {
	case content := <-entered:
		t.Fatalf("queued handler ran during shutdown (content %q)", content)
	default:
	}

	rows := listDeliveries(t, o)
	if len(rows) != 1 {
		t.Fatalf("deliveries after Close = %d, want 1 (abandoned queued)", len(rows))
	}
	if rows[0].ID != idQueued {
		t.Fatalf("remaining delivery = %s, want queued %s (running one was %s)", rows[0].ID, idQueued, idRun)
	}
	if !rows[0].TakenAt.Valid {
		t.Fatal("abandoned queued row should keep taken_at for startup-reset redelivery")
	}
	if countPublications(t, o) != 1 {
		t.Fatalf("publications = %d, want 1 (the abandoned delivery keeps its publication)", countPublications(t, o))
	}
	if used, _ := o.AdmissionSnapshot(); used != 0 {
		t.Fatalf("admission used after Close = %d, want 0", used)
	}

	lastDepth, lastFlight := stats.lastFor(handler.Type)
	if lastDepth != 0 {
		t.Fatalf("final queue depth = %d, want 0 after shutdown drain", lastDepth)
	}
	if lastFlight != 0 {
		t.Fatalf("final in-flight = %d, want 0 after shutdown", lastFlight)
	}
	stats.assertBoundedLabels(t, handler.Type)
}

// TestDispatchStatsPanicDoesNotBreakDelivery ensures Observe* panics (dispatch
// and admission observers alike) are recovered and never prevent finalization.
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
	if o.admissionStats == nil {
		t.Fatal("panicking collector should have been detected as AdmissionStats")
	}
	qdSeed(t, o, handler.Type, 1)
	qdReconcile(t, o)

	if _, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	}
	if !handler.Wait(5 * time.Second) {
		t.Fatal("handler did not run despite panicking collector")
	}
	if got := countDeliveries(t, o); got != 0 {
		t.Fatalf("deliveries = %d, want 0 (finalized despite stats panic)", got)
	}
	if got := countPublications(t, o); got != 0 {
		t.Fatalf("publications = %d, want 0", got)
	}
}

// panickingDispatchStats panics from every observer, dispatch and admission.
type panickingDispatchStats struct{}

func (panickingDispatchStats) ObserveQueueDepth(string, string, int) {
	panic("queue depth observer boom")
}

func (panickingDispatchStats) ObserveInFlight(string, string, int) {
	panic("in-flight observer boom")
}

func (panickingDispatchStats) ObserveAdmission(int, int) {
	panic("admission observer boom")
}

func (panickingDispatchStats) ObserveClaimSkip(ClaimSkipReason) {
	panic("claim skip observer boom")
}

func (panickingDispatchStats) ObserveFinalize(FinalizeOutcome, time.Duration, bool, error) {
	panic("finalize observer boom")
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

	qdSeed(t, o, handler.Type, 1)
	qdReconcile(t, o)

	if _, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	}
	if !handler.Wait(5 * time.Second) {
		t.Fatal("handler wait")
	}

	stats.mu.Lock()
	defer stats.mu.Unlock()
	foundNumeric := false
	for _, s := range stats.depths {
		if s.handler != handler.Type {
			t.Fatalf("unexpected handler label %q", s.handler)
		}
		if s.shard == serialShardLabel {
			t.Fatalf("partition queue used serial shard label")
		}
		// Must be a small integer string, not a UUID/correlation id.
		shard, err := strconv.Atoi(s.shard)
		if err != nil {
			t.Fatalf("shard label %q is not a shard index: %v", s.shard, err)
		}
		if shard < 0 || shard >= 8 {
			t.Fatalf("shard label %d out of range [0,8)", shard)
		}
		foundNumeric = true
	}
	if !foundNumeric {
		t.Fatal("no partition depth samples")
	}
}

// qdAdmissionRecorder implements DispatchStats and the additive AdmissionStats,
// so WithDispatchStats detects both by type assertion.
type qdAdmissionRecorder struct {
	recordingDispatchStats

	amu        sync.Mutex
	admissions []qdAdmissionSample
	skips      []ClaimSkipReason
	finalizes  []qdFinalizeSample
}

type qdAdmissionSample struct {
	used  int
	limit int
}

type qdFinalizeSample struct {
	outcome  FinalizeOutcome
	duration time.Duration
	retried  bool
	err      error
}

func (r *qdAdmissionRecorder) ObserveAdmission(used, limit int) {
	r.amu.Lock()
	defer r.amu.Unlock()
	r.admissions = append(r.admissions, qdAdmissionSample{used: used, limit: limit})
}

func (r *qdAdmissionRecorder) ObserveClaimSkip(reason ClaimSkipReason) {
	r.amu.Lock()
	defer r.amu.Unlock()
	r.skips = append(r.skips, reason)
}

func (r *qdAdmissionRecorder) ObserveFinalize(outcome FinalizeOutcome, d time.Duration, retried bool, err error) {
	r.amu.Lock()
	defer r.amu.Unlock()
	r.finalizes = append(r.finalizes, qdFinalizeSample{outcome: outcome, duration: d, retried: retried, err: err})
}

func (r *qdAdmissionRecorder) admissionSamples() []qdAdmissionSample {
	r.amu.Lock()
	defer r.amu.Unlock()
	return append([]qdAdmissionSample(nil), r.admissions...)
}

func (r *qdAdmissionRecorder) sawSkip(reason ClaimSkipReason) bool {
	r.amu.Lock()
	defer r.amu.Unlock()
	for _, s := range r.skips {
		if s == reason {
			return true
		}
	}
	return false
}

func (r *qdAdmissionRecorder) finalizeSamples() []qdFinalizeSample {
	r.amu.Lock()
	defer r.amu.Unlock()
	return append([]qdFinalizeSample(nil), r.finalizes...)
}

// TestAdmissionStatsObserved covers the additive AdmissionStats observer end to
// end: admission gauges, both bounded claim-skip reasons and every finalize
// outcome the happy/retry/dead-letter/release paths can produce.
func TestAdmissionStatsObserved(t *testing.T) {
	t.Run("AdmissionAndCompleted", func(t *testing.T) {
		db := newTestDB(t)
		ctx := context.Background()
		stats := &qdAdmissionRecorder{}
		o, err := NewOutbox(db, WithDispatchStats(stats), WithAdmissionLimit(4))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = o.Close() })
		if o.admissionStats == nil {
			t.Fatal("collector implementing AdmissionStats was not detected")
		}

		handler := mocks.NewEventHandler("adm_ok")
		if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
			t.Fatal(err)
		}
		qdSeed(t, o, handler.Type, 1)
		qdReconcile(t, o)

		if _, err := o.processBatch(ctx); err != nil {
			t.Fatal(err)
		}
		if countDeliveries(t, o) != 0 {
			t.Fatalf("deliveries = %d, want 0", countDeliveries(t, o))
		}

		samples := stats.admissionSamples()
		if len(samples) == 0 {
			t.Fatal("no ObserveAdmission samples")
		}
		sawUsed := false
		for _, s := range samples {
			if s.limit != 4 {
				t.Fatalf("ObserveAdmission limit = %d, want 4", s.limit)
			}
			if s.used < 0 || s.used > s.limit {
				t.Fatalf("ObserveAdmission used = %d, want 0 <= used <= %d", s.used, s.limit)
			}
			if s.used > 0 {
				sawUsed = true
			}
		}
		if !sawUsed {
			t.Fatal("expected at least one ObserveAdmission with used > 0")
		}
		if last := samples[len(samples)-1]; last.used != 0 {
			t.Fatalf("final ObserveAdmission used = %d, want 0", last.used)
		}
		if used, _ := o.AdmissionSnapshot(); used != 0 {
			t.Fatalf("AdmissionSnapshot used = %d, want 0", used)
		}

		fins := stats.finalizeSamples()
		if len(fins) != 1 {
			t.Fatalf("finalize observations = %d, want 1", len(fins))
		}
		if fins[0].outcome != FinalizeCompleted || fins[0].retried || fins[0].err != nil {
			t.Fatalf("finalize = %+v, want {completed, retried=false, err=nil}", fins[0])
		}
		stats.assertBoundedLabels(t, handler.Type)
	})

	t.Run("SkipQueueFull", func(t *testing.T) {
		db := newTestDB(t)
		ctx := context.Background()
		stats := &qdAdmissionRecorder{}
		o, err := NewOutbox(db, WithDispatchStats(stats), WithQueueDepth(1), WithMaxGoroutines(1), WithAdmissionLimit(16))
		if err != nil {
			t.Fatal(err)
		}
		gate := qdNewGate()
		entered := make(chan string, 8)
		handler := newBlockingHandler("adm_queue_full", entered, gate.ch)
		t.Cleanup(func() { _ = o.Close() })
		t.Cleanup(gate.open)

		if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
			t.Fatal(err)
		}
		qdSeed(t, o, handler.Type, 3)
		qdReconcile(t, o)

		// Pass 1 claims one (queue depth 1); the worker pops it into the
		// blocking handler, freeing the slot.
		if n, err := o.fetchAndDispatch(ctx, false); err != nil || n != 1 {
			t.Fatalf("claim #1 = %d, err = %v, want 1", n, err)
		}
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("handler did not enter")
		}
		// Pass 2 fills the only queue slot.
		if n, err := o.fetchAndDispatch(ctx, false); err != nil || n != 1 {
			t.Fatalf("claim #2 = %d, err = %v, want 1", n, err)
		}
		// Pass 3 finds the key saturated.
		if n, err := o.fetchAndDispatch(ctx, false); err != nil || n != 0 {
			t.Fatalf("claim #3 = %d, err = %v, want 0", n, err)
		}
		if !stats.sawSkip(SkipQueueFull) {
			t.Fatal("expected ObserveClaimSkip(SkipQueueFull) for the saturated key")
		}
		if stats.sawSkip(SkipAdmissionFull) {
			t.Fatal("admission had free slots; SkipAdmissionFull must not be reported")
		}

		gate.open()
		waitUntil(t, 10*time.Second, func() bool {
			if _, err := o.fetchAndDispatch(ctx, false); err != nil {
				t.Fatal(err)
			}
			qdDrain(entered)
			return countDeliveries(t, o) == 0
		})
	})

	t.Run("SkipAdmissionFull", func(t *testing.T) {
		db := newTestDB(t)
		ctx := context.Background()
		stats := &qdAdmissionRecorder{}
		o, err := NewOutbox(db, WithDispatchStats(stats), WithAdmissionLimit(1), WithMaxGoroutines(1))
		if err != nil {
			t.Fatal(err)
		}
		gate := qdNewGate()
		entered := make(chan string, 8)
		handler := newBlockingHandler("adm_full", entered, gate.ch)
		t.Cleanup(func() { _ = o.Close() })
		t.Cleanup(gate.open)

		if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
			t.Fatal(err)
		}
		qdSeed(t, o, handler.Type, 2)
		qdReconcile(t, o)

		// The single admission slot is taken by the blocking delivery.
		if n, err := o.fetchAndDispatch(ctx, false); err != nil || n != 1 {
			t.Fatalf("claim #1 = %d, err = %v, want 1", n, err)
		}
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("handler did not enter")
		}
		if used, limit := o.AdmissionSnapshot(); used != 1 || limit != 1 {
			t.Fatalf("AdmissionSnapshot = (%d, %d), want (1, 1)", used, limit)
		}
		// The second delivery is due but cannot be admitted.
		if n, err := o.fetchAndDispatch(ctx, false); err != nil || n != 0 {
			t.Fatalf("claim #2 = %d, err = %v, want 0 at the admission limit", n, err)
		}
		if !stats.sawSkip(SkipAdmissionFull) {
			t.Fatal("expected ObserveClaimSkip(SkipAdmissionFull) at the admission limit")
		}
		for _, s := range stats.admissionSamples() {
			if s.used > s.limit {
				t.Fatalf("ObserveAdmission used %d > limit %d", s.used, s.limit)
			}
		}

		gate.open()
		waitUntil(t, 10*time.Second, func() bool {
			if _, err := o.fetchAndDispatch(ctx, false); err != nil {
				t.Fatal(err)
			}
			qdDrain(entered)
			return countDeliveries(t, o) == 0
		})
		if used, _ := o.AdmissionSnapshot(); used != 0 {
			t.Fatalf("admission used after drain = %d, want 0", used)
		}
	})

	t.Run("FinalizeRetry", func(t *testing.T) {
		db := newTestDB(t)
		ctx := context.Background()
		stats := &qdAdmissionRecorder{}
		o, err := NewOutbox(db, WithDispatchStats(stats), WithMaxRetries(5), WithRetryBackoff("FIXED:5:1m"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = o.Close() })

		handler := &qdFailingHandler{typ: "adm_retry", err: errors.New("transient")}
		if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
			t.Fatal(err)
		}
		qdSeed(t, o, handler.typ, 1)
		qdReconcile(t, o)

		if _, err := o.processBatch(ctx); err != nil {
			t.Fatal(err)
		}
		fins := stats.finalizeSamples()
		if len(fins) != 1 {
			t.Fatalf("finalize observations = %d, want 1", len(fins))
		}
		if fins[0].outcome != FinalizeRetry || fins[0].retried || fins[0].err != nil {
			t.Fatalf("finalize = %+v, want {retry, retried=false, err=nil}", fins[0])
		}
		rows := listDeliveries(t, o)
		if len(rows) != 1 || rows[0].RetryCount != 1 || rows[0].TakenAt.Valid {
			t.Fatalf("delivery after retry = %+v, want retry_count 1 and taken_at NULL", rows)
		}
	})

	t.Run("FinalizeDeadLetter", func(t *testing.T) {
		db := newTestDB(t)
		ctx := context.Background()
		stats := &qdAdmissionRecorder{}
		o, err := NewOutbox(db, WithDispatchStats(stats))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = o.Close() })

		handler := &qdFailingHandler{typ: "adm_fatal", err: qdFatalError{errors.New("fatal")}}
		if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
			t.Fatal(err)
		}
		qdSeed(t, o, handler.typ, 1)
		qdReconcile(t, o)

		if _, err := o.processBatch(ctx); err != nil {
			t.Fatal(err)
		}
		fins := stats.finalizeSamples()
		if len(fins) != 1 {
			t.Fatalf("finalize observations = %d, want 1", len(fins))
		}
		if fins[0].outcome != FinalizeDeadLetter || fins[0].retried || fins[0].err != nil {
			t.Fatalf("finalize = %+v, want {dead_letter, retried=false, err=nil}", fins[0])
		}
		if got := countDeadLetters(t, o); got != 1 {
			t.Fatalf("dead letters = %d, want 1", got)
		}
		if got := countDeliveries(t, o); got != 0 {
			t.Fatalf("deliveries = %d, want 0", got)
		}
	})

	t.Run("FinalizeReleased", func(t *testing.T) {
		db := newTestDB(t)
		ctx := context.Background()
		stats := &qdAdmissionRecorder{}
		o, err := NewOutbox(db, WithDispatchStats(stats))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = o.Close() })

		var attempts []int
		var attemptsMu sync.Mutex
		commitErr := errors.New("finalize commit unavailable")
		o.beforeFinalizeCommit = func(_ string, attempt int) error {
			attemptsMu.Lock()
			attempts = append(attempts, attempt)
			attemptsMu.Unlock()
			return commitErr
		}
		o.finalizeSleep = func(time.Duration) {}

		handler := mocks.NewEventHandler("adm_released")
		if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
			t.Fatal(err)
		}
		qdSeed(t, o, handler.Type, 1)
		qdReconcile(t, o)

		before := time.Now()
		if _, err := o.processBatch(ctx); err != nil {
			t.Fatal(err)
		}

		attemptsMu.Lock()
		gotAttempts := append([]int(nil), attempts...)
		attemptsMu.Unlock()
		if len(gotAttempts) != 1+len(finalizeRetryDelays) {
			t.Fatalf("finalize attempts = %v, want %d", gotAttempts, 1+len(finalizeRetryDelays))
		}

		fins := stats.finalizeSamples()
		if len(fins) != 1 {
			t.Fatalf("finalize observations = %d, want 1", len(fins))
		}
		if fins[0].outcome != FinalizeReleased {
			t.Fatalf("finalize outcome = %q, want %q", fins[0].outcome, FinalizeReleased)
		}
		if !fins[0].retried {
			t.Fatal("finalize retried = false, want true after exhausting the finalize retries")
		}
		if fins[0].err == nil {
			t.Fatal("finalize err = nil, want the finalize failure")
		}
		if !errors.Is(fins[0].err, commitErr) {
			t.Fatalf("finalize err = %v, want %v", fins[0].err, commitErr)
		}

		// The claim was released: eligible again after PeriodicSweepAge, with
		// retry_count untouched (the handler already ran successfully).
		rows := listDeliveries(t, o)
		if len(rows) != 1 {
			t.Fatalf("deliveries = %d, want 1 (released, not deleted)", len(rows))
		}
		if rows[0].TakenAt.Valid {
			t.Fatal("released delivery should have taken_at NULL")
		}
		if !rows[0].AvailableAt.After(before) {
			t.Fatalf("available_at = %v, want a future time (>= now + PeriodicSweepAge)", rows[0].AvailableAt)
		}
		if rows[0].RetryCount != 0 {
			t.Fatalf("retry_count = %d, want 0 (unchanged by release)", rows[0].RetryCount)
		}
		if used, _ := o.AdmissionSnapshot(); used != 0 {
			t.Fatalf("admission used = %d, want 0 after release", used)
		}
		if samples := stats.admissionSamples(); len(samples) == 0 || samples[len(samples)-1].used != 0 {
			t.Fatalf("final ObserveAdmission = %+v, want used 0", samples)
		}
	})
}

// --- file-local helpers (qd* prefix) ------------------------------------

// qdGate is a one-shot release channel that is safe to close twice.
type qdGate struct {
	ch   chan struct{}
	once sync.Once
}

func qdNewGate() *qdGate {
	return &qdGate{ch: make(chan struct{})}
}

func (g *qdGate) open() {
	g.once.Do(func() { close(g.ch) })
}

// qdDrain empties a handler "entered" channel so blocking handlers never stall
// on an unread notification.
func qdDrain(entered <-chan string) {
	for {
		select {
		case <-entered:
		default:
			return
		}
	}
}

// qdReconcile runs the shared startup reconcile and additionally primes the
// claim key ring, which the live path builds in StartChecked. Tests here drive
// claimPass directly without starting the processor.
func qdReconcile(t testing.TB, o *Outbox) {
	t.Helper()
	reconcileForTest(t, o)
	o.handlersMu.Lock()
	o.claimKeys = o.registeredDispatchKeys()
	o.handlersMu.Unlock()
}

// qdSeed writes n due deliveries for handlerType (strictly increasing
// available_at, so claim order is stable) and returns their delivery ids in
// that order. Callers run qdReconcile afterwards to compute dispatch keys.
func qdSeed(t testing.TB, o *Outbox, handlerType string, n int) []string {
	t.Helper()
	base := time.Now().Add(-time.Minute)
	ids := make([]string, 0, n)
	for i := range n {
		at := base.Add(time.Duration(i) * time.Millisecond)
		_, id := insertDeliveryDirect(t, o, deliverySeed{
			Event:       newTestEvent(fmt.Sprintf("%s-%d", handlerType, i)),
			HandlerType: handlerType,
			CreatedAt:   at,
			AvailableAt: at,
		})
		ids = append(ids, id)
	}
	return ids
}

// qdFailingHandler always fails with a fixed error (retryable or fatal).
type qdFailingHandler struct {
	typ string
	err error
}

func (h *qdFailingHandler) HandlerType() eh.EventHandlerType {
	return eh.EventHandlerType(h.typ)
}

func (h *qdFailingHandler) HandleEvent(context.Context, eh.Event) error {
	return h.err
}

// qdFatalError marks an error as SeverityFatal (straight to the dead letters).
type qdFatalError struct {
	err error
}

func (e qdFatalError) Error() string { return e.err.Error() }

func (e qdFatalError) Unwrap() error { return e.err }

func (e qdFatalError) OutboxSeverity() ErrorSeverity { return SeverityFatal }
