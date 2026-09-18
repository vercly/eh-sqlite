package sqlite

import (
	"log"
	"sync"
	"sync/atomic"
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

// delivery is one claimed outbox_deliveries row on its long-lived dispatch
// queue. It owns its full lifecycle: execute, finalize, release admission.
type delivery struct {
	doc      *deliveryDoc
	handler  *matcherHandler
	queueKey string
	// Explicit bounded labels for stats (not derived by parsing queueKey).
	handlerLabel string
	shardLabel   string

	// done is closed after finalize or abandon (test/wait helper path).
	done      chan struct{}
	doneOnce  sync.Once
	abandoned atomic.Bool
}

func newDelivery(doc *deliveryDoc, handler *matcherHandler, key, handlerLabel, shardLabel string) *delivery {
	if shardLabel == "" {
		shardLabel = serialShardLabel
	}
	return &delivery{
		doc:          doc,
		handler:      handler,
		queueKey:     key,
		handlerLabel: handlerLabel,
		shardLabel:   shardLabel,
		done:         make(chan struct{}),
	}
}

func (d *delivery) finish() {
	d.doneOnce.Do(func() { close(d.done) })
}

func (d *delivery) wait() {
	<-d.done
}

// keyQueue is a bounded, long-lived FIFO for one dispatch key (handler or
// handler+shard). Reservations allow admit-before-claim.
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

// safeDispatchObserve runs an external stats callback and recovers panics with
// a bounded log. Does not send on Errors() (avoids re-entrancy).
func safeDispatchObserve(fn func()) {
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("eventhorizon: dispatch stats observer panicked: %v", rec)
		}
	}()
	fn()
}

// freeCapacity reports how many more deliveries the queue can accept.
func (q *keyQueue) freeCapacity() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return 0
	}
	free := q.capacity - q.depth
	if free < 0 {
		return 0
	}
	return free
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
	q.reserved = 0
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

func (r *dispatchRegistry) getExisting(key string) *keyQueue {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.queues[key]
}

// freeCapacity reports free slots of key's queue; a queue that does not exist
// yet has full capacity.
func (r *dispatchRegistry) freeCapacity(key string) int {
	if q := r.getExisting(key); q != nil {
		return q.freeCapacity()
	}
	return r.queueDepth
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

// tryReserve reserves one slot on key's queue for d (labels remembered first
// so the queue is created with explicit handler/shard labels).
func (r *dispatchRegistry) tryReserve(d *delivery) bool {
	r.rememberLabels(d.queueKey, d.handlerLabel, d.shardLabel)
	return r.getOrCreate(d.queueKey).tryReserve(1)
}

func (r *dispatchRegistry) releaseReserve(key string) {
	if q := r.getExisting(key); q != nil {
		q.releaseReserve(1)
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
			// Drain abandoned items without executing; rows keep taken_at and
			// are redelivered after startup reset (at-least-once).
			for _, d := range q.closeAndDrain() {
				r.outbox.abandonDelivery(d)
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
