package sqlite

import (
	"context"
	"log"
	"sync"
	"sync/atomic"

	eh "github.com/vercly/eventhorizon"
)

// DefaultQueueDepth is the default per-dispatch-key buffer size (enqueued +
// reserved). Overridable via WithQueueDepth.
const DefaultQueueDepth = 32

// defaultQueueDepth keeps the historical unexported name used inside the package.
const defaultQueueDepth = DefaultQueueDepth

// serialShardLabel is the stable Prometheus/shard dimension for Serial queues
// (no partition). Never a correlation id.
const serialShardLabel = "none"

// DispatchStats is a transport-neutral observer for dispatch queues and
// HandleEvent in-flight. Implementations must be safe for concurrent use.
// Observe* methods must not block and must not call back into the outbox.
// Labels are only handler type and shard (never correlation/aggregate ids).
type DispatchStats interface {
	ObserveQueueDepth(handler, shard string, depth int)
	ObserveInFlight(handler, shard string, n int)
}

// delivery is one (record, handler) unit of work on a long-lived dispatch queue.
type delivery struct {
	event    *outboxDoc
	handler  *matcherHandler
	eventCtx map[string]any
	coord    *recordCoordinator
	queueKey string
	// Explicit bounded labels for stats (not derived by parsing queueKey).
	handlerLabel string
	shardLabel   string
}

// keyQueue is a bounded, long-lived FIFO for one dispatch key (handler or
// handler+shard). Reservations allow atomic multi-queue admit before claim.
type keyQueue struct {
	key          string
	handlerLabel string
	shardLabel   string
	capacity     int
	stats        DispatchStats

	mu       sync.Mutex
	depth    int // enqueued + reserved
	reserved int
	items    []*delivery
	closed   bool

	wake chan struct{} // signal worker that work may be available
}

func newKeyQueue(key, handlerLabel, shardLabel string, capacity int, stats DispatchStats) *keyQueue {
	if capacity < 1 {
		capacity = DefaultQueueDepth
	}
	if shardLabel == "" {
		shardLabel = serialShardLabel
	}
	return &keyQueue{
		key:          key,
		handlerLabel: handlerLabel,
		shardLabel:   shardLabel,
		capacity:     capacity,
		stats:        stats,
		wake:         make(chan struct{}, 1),
	}
}

func (q *keyQueue) reportDepthUnlocked() (handler, shard string, depth int) {
	return q.handlerLabel, q.shardLabel, q.depth
}

func (q *keyQueue) emitDepth(handler, shard string, depth int) {
	if q.stats == nil {
		return
	}
	// Called only after releasing q.mu. Recover so a panicking collector
	// cannot abort fetch/workers or change delivery semantics.
	safeDispatchObserve(func() {
		q.stats.ObserveQueueDepth(handler, shard, depth)
	})
}

// safeDispatchObserve runs an external DispatchStats callback and recovers
// panics with a bounded log. Does not send on Errors() (avoids re-entrancy).
func safeDispatchObserve(fn func()) {
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("eventhorizon: dispatch stats observer panicked: %v", rec)
		}
	}()
	fn()
}

func (q *keyQueue) tryReserve(n int) bool {
	if n < 1 {
		return true
	}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return false
	}
	if q.depth+n > q.capacity {
		q.mu.Unlock()
		return false
	}
	q.depth += n
	q.reserved += n
	handler, shard, depth := q.reportDepthUnlocked()
	q.mu.Unlock()
	q.emitDepth(handler, shard, depth)
	return true
}

func (q *keyQueue) releaseReserve(n int) {
	if n < 1 {
		return
	}
	q.mu.Lock()
	q.reserved -= n
	q.depth -= n
	if q.reserved < 0 {
		q.reserved = 0
	}
	if q.depth < 0 {
		q.depth = 0
	}
	handler, shard, depth := q.reportDepthUnlocked()
	q.mu.Unlock()
	q.emitDepth(handler, shard, depth)
}

// enqueueCommitted pushes a delivery that already holds a reservation.
// Must not block: capacity was reserved before claim.
func (q *keyQueue) enqueueCommitted(d *delivery) {
	q.mu.Lock()
	if q.reserved > 0 {
		q.reserved--
	}
	// depth already includes the reservation; convert to enqueued item.
	q.items = append(q.items, d)
	handler, shard, depth := q.reportDepthUnlocked()
	q.mu.Unlock()
	q.emitDepth(handler, shard, depth)
	q.signal()
}

func (q *keyQueue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *keyQueue) pop() (*delivery, bool) {
	q.mu.Lock()
	if len(q.items) == 0 {
		q.mu.Unlock()
		return nil, false
	}
	d := q.items[0]
	q.items = q.items[1:]
	q.depth--
	if q.depth < 0 {
		q.depth = 0
	}
	handler, shard, depth := q.reportDepthUnlocked()
	q.mu.Unlock()
	q.emitDepth(handler, shard, depth)
	return d, true
}

func (q *keyQueue) closeAndDrain() []*delivery {
	q.mu.Lock()
	q.closed = true
	// Drop reservations that never became enqueued items.
	q.depth -= q.reserved
	q.reserved = 0
	if q.depth < 0 {
		q.depth = 0
	}
	left := q.items
	q.items = nil
	q.depth = 0
	handler, shard, depth := q.reportDepthUnlocked()
	q.mu.Unlock()
	q.emitDepth(handler, shard, depth)
	return left
}

// dispatchRegistry owns long-lived per-key queues and their workers.
type dispatchRegistry struct {
	mu     sync.Mutex
	queues map[string]*keyQueue
	// labels[key] stores handler/shard for getOrCreate without re-parsing key.
	labels     map[string]dispatchKeyLabels
	started    map[string]bool
	queueDepth int
	outbox     *Outbox
	stats      DispatchStats
	stop       <-chan struct{}
	workersWg  sync.WaitGroup

	// inFlight tracks HandleEvent concurrency per (handler, shard).
	inFlightMu sync.Mutex
	inFlight   map[string]int
}

type dispatchKeyLabels struct {
	handler string
	shard   string
}

func newDispatchRegistry(o *Outbox, queueDepth int, stats DispatchStats) *dispatchRegistry {
	if queueDepth < 1 {
		queueDepth = DefaultQueueDepth
	}
	return &dispatchRegistry{
		queues:     make(map[string]*keyQueue),
		labels:     make(map[string]dispatchKeyLabels),
		started:    make(map[string]bool),
		queueDepth: queueDepth,
		outbox:     o,
		stats:      stats,
		stop:       o.done,
		inFlight:   make(map[string]int),
	}
}

func (r *dispatchRegistry) rememberLabels(key, handler, shard string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.labels[key]; !ok {
		if shard == "" {
			shard = serialShardLabel
		}
		r.labels[key] = dispatchKeyLabels{handler: handler, shard: shard}
	}
}

func (r *dispatchRegistry) getOrCreate(key string) *keyQueue {
	r.mu.Lock()
	defer r.mu.Unlock()
	q, ok := r.queues[key]
	if !ok {
		lbl := r.labels[key]
		if lbl.handler == "" {
			// Fallback: key may be bare handler type (Serial).
			lbl = dispatchKeyLabels{handler: key, shard: serialShardLabel}
		}
		q = newKeyQueue(key, lbl.handler, lbl.shard, r.queueDepth, r.stats)
		r.queues[key] = q
	}
	if !r.started[key] {
		r.started[key] = true
		r.workersWg.Add(1)
		go r.runWorker(q)
	}
	return q
}

func (r *dispatchRegistry) inFlightKey(handler, shard string) string {
	return handler + "\x00" + shard
}

func (r *dispatchRegistry) addInFlight(handler, shard string, delta int) {
	if r.stats == nil {
		return
	}
	if shard == "" {
		shard = serialShardLabel
	}
	r.inFlightMu.Lock()
	k := r.inFlightKey(handler, shard)
	r.inFlight[k] += delta
	if r.inFlight[k] < 0 {
		r.inFlight[k] = 0
	}
	n := r.inFlight[k]
	r.inFlightMu.Unlock()
	safeDispatchObserve(func() {
		r.stats.ObserveInFlight(handler, shard, n)
	})
}

// tryReserveKeys reserves slots for each key (counted). On failure rolls back all.
// deliveries supply explicit handler/shard labels so queues are not created by
// parsing composite keys.
func (r *dispatchRegistry) tryReserveKeys(keys []string, deliveries []*delivery) bool {
	if len(keys) == 0 {
		return true
	}
	counts := make(map[string]int, len(keys))
	for _, k := range keys {
		counts[k]++
	}
	for _, d := range deliveries {
		r.rememberLabels(d.queueKey, d.handlerLabel, d.shardLabel)
	}
	// Deterministic order for reserve to reduce lock-order races between fetchers.
	ordered := make([]string, 0, len(counts))
	for k := range counts {
		ordered = append(ordered, k)
	}
	// tiny stable sort without importing sort package churn — insertion sort
	for i := 1; i < len(ordered); i++ {
		for j := i; j > 0 && ordered[j] < ordered[j-1]; j-- {
			ordered[j], ordered[j-1] = ordered[j-1], ordered[j]
		}
	}

	reservedKeys := make([]string, 0, len(ordered))
	for _, key := range ordered {
		q := r.getOrCreate(key)
		if !q.tryReserve(counts[key]) {
			r.releaseKeyCounts(reservedKeys, counts)
			return false
		}
		reservedKeys = append(reservedKeys, key)
	}
	return true
}

func (r *dispatchRegistry) getExisting(key string) *keyQueue {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.queues[key]
}

func (r *dispatchRegistry) releaseKeys(keys []string) {
	counts := make(map[string]int, len(keys))
	for _, k := range keys {
		counts[k]++
	}
	ordered := make([]string, 0, len(counts))
	for k := range counts {
		ordered = append(ordered, k)
	}
	r.releaseKeyCounts(ordered, counts)
}

func (r *dispatchRegistry) releaseKeyCounts(keys []string, counts map[string]int) {
	for _, key := range keys {
		if q := r.getExisting(key); q != nil {
			q.releaseReserve(counts[key])
		}
	}
}

func (r *dispatchRegistry) enqueue(d *delivery) {
	q := r.getOrCreate(d.queueKey)
	q.enqueueCommitted(d)
}

func (r *dispatchRegistry) runWorker(q *keyQueue) {
	defer r.workersWg.Done()
	for {
		if d, ok := q.pop(); ok {
			r.outbox.executeDelivery(d)
			continue
		}
		select {
		case <-r.stop:
			// Drain abandoned items without executing; coordinators stay incomplete
			// so records are redelivered after startup reset (at-least-once).
			abandoned := q.closeAndDrain()
			for _, d := range abandoned {
				if d.coord != nil {
					d.coord.abandon()
				}
			}
			return
		case <-q.wake:
		}
	}
}

func (r *dispatchRegistry) waitWorkers() {
	r.workersWg.Wait()
}

func (r *dispatchRegistry) wakeAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, q := range r.queues {
		q.signal()
	}
}

// recordCoordinator collects all delivery outcomes for one claimed outbox row
// and runs atomic finalization exactly once.
type recordCoordinator struct {
	outbox   *Outbox
	record   *outboxDoc
	eventCtx map[string]any
	result   *processedResult

	remaining atomic.Int32
	doneOnce  sync.Once
	doneCh    chan struct{}
	abandoned atomic.Bool
	mu        sync.Mutex
}

func newRecordCoordinator(o *Outbox, record *outboxDoc, eventCtx map[string]any, deliveryCount int) *recordCoordinator {
	c := &recordCoordinator{
		outbox:   o,
		record:   record,
		eventCtx: eventCtx,
		result: &processedResult{
			r:              record,
			failedHandlers: map[string]handlerFailure{},
		},
		doneCh: make(chan struct{}),
	}
	c.remaining.Store(int32(deliveryCount))
	return c
}

func (c *recordCoordinator) report(res handlerDispatchResult) {
	if c.abandoned.Load() {
		return
	}
	c.mu.Lock()
	if res.err != nil {
		recordHandlerFailure(c.result, res)
	} else {
		c.result.successfulHandlers = append(c.result.successfulHandlers, res.handlerType)
	}
	c.mu.Unlock()

	if c.remaining.Add(-1) == 0 {
		c.finish()
	}
}

func (c *recordCoordinator) abandon() {
	if c.abandoned.Swap(true) {
		return
	}
	// Incomplete records keep taken_at; next Start resets and redelivers.
	c.outbox.admission.release(c.record.ID.String())
	c.doneOnce.Do(func() { close(c.doneCh) })
}

func (c *recordCoordinator) finish() {
	c.doneOnce.Do(func() {
		if !c.abandoned.Load() {
			c.outbox.updateEventDB(context.Background(), *c.result)
			// Free admission so the fetcher can claim more records.
			c.outbox.admission.release(c.record.ID.String())
			// Wake fetcher after completion (decoupled from claim).
			c.outbox.notify()
		}
		close(c.doneCh)
	})
}

func (c *recordCoordinator) wait() {
	<-c.doneCh
}

func (o *Outbox) executeDelivery(d *delivery) {
	if d.coord != nil && d.coord.abandoned.Load() {
		return
	}
	// Global HandleEvent permit (FIFO fair across dispatch keys).
	if err := o.handleSem.acquire(o.runContext()); err != nil {
		if d.coord != nil {
			d.coord.abandon()
		}
		return
	}
	defer o.handleSem.release()

	if o.shuttingDown.Load() {
		if d.coord != nil {
			d.coord.abandon()
		}
		return
	}

	handlerLabel := d.handlerLabel
	shardLabel := d.shardLabel
	if shardLabel == "" {
		shardLabel = serialShardLabel
	}
	if o.dispatch != nil {
		o.dispatch.addInFlight(handlerLabel, shardLabel, 1)
		defer o.dispatch.addInFlight(handlerLabel, shardLabel, -1)
	}

	eventCtx := eh.UnmarshalContext(context.Background(), d.eventCtx)
	res := o.dispatchHandler(eventCtx, dispatchItem{event: d.event, handler: d.handler})
	if d.coord != nil {
		d.coord.report(res)
	}
}

// planDeliveries resolves handlers for a record into dispatch keys and deliveries.
// missing lists stored handler types that are not registered or no longer match.
func (o *Outbox) planDeliveries(record *outboxDoc, eventCtx map[string]any) (keys []string, deliveries []*delivery, missing []string) {
	handlersByType := o.snapshotHandlersByType()
	for _, handlerType := range record.Handlers {
		handler := handlersByType[handlerType]
		if handler == nil || !handler.Match(record.Event) {
			missing = append(missing, handlerType)
			continue
		}
		key, handlerLabel, shardLabel := dispatchQueueIdentity(handler, record.Event)
		keys = append(keys, key)
		deliveries = append(deliveries, &delivery{
			event:        record,
			handler:      handler,
			eventCtx:     eventCtx,
			queueKey:     key,
			handlerLabel: handlerLabel,
			shardLabel:   shardLabel,
		})
	}
	return keys, deliveries, missing
}

// planRematchDeliveries matches the event against every currently registered
// handler (empty stored handlers list). Used after no_match DLQ replay.
//
// Callers must assign the matched handler type names to record.Handlers and
// persist that list in the same claim transaction before dispatch. Finalize
// computes remaining work from record.Handlers; leaving it empty causes silent
// delete even when a handler failed retryably.
func (o *Outbox) planRematchDeliveries(record *outboxDoc, eventCtx map[string]any) (keys []string, deliveries []*delivery) {
	o.handlersMu.RLock()
	handlers := append([]*matcherHandler(nil), o.handlers...)
	o.handlersMu.RUnlock()

	for _, mh := range handlers {
		if mh == nil || !mh.Match(record.Event) {
			continue
		}
		key, handlerLabel, shardLabel := dispatchQueueIdentity(mh, record.Event)
		keys = append(keys, key)
		deliveries = append(deliveries, &delivery{
			event:        record,
			handler:      mh,
			eventCtx:     eventCtx,
			queueKey:     key,
			handlerLabel: handlerLabel,
			shardLabel:   shardLabel,
		})
	}
	return keys, deliveries
}
