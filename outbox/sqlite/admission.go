package sqlite

import "sync"

// recordAdmission is the process-local set of delivery IDs currently admitted
// (claimed, queued or executing, not yet finalized). It is not durable truth:
// after a crash the set is empty and StartChecked resets taken_at so rows may
// be redelivered (at-least-once).
//
// Capacity bounds in-flight deliveries in this process (memory bound), not
// per-key queue depth. tryAdmit never blocks; callers admit before claiming.
type recordAdmission struct {
	mu        sync.Mutex
	active    map[string]admittedDelivery // delivery id -> owner
	byHandler map[string]int              // handler type -> admitted count
	limit     int
}

type admittedDelivery struct {
	key     string // dispatch key ("" when unknown)
	handler string // handler type ("" when unknown)
}

func newRecordAdmission(limit int) *recordAdmission {
	return &recordAdmission{
		active:    make(map[string]admittedDelivery),
		byHandler: make(map[string]int),
		limit:     normalizeAdmissionLimit(limit),
	}
}

func normalizeAdmissionLimit(limit int) int {
	if limit < 1 {
		return 1
	}
	return limit
}

func (a *recordAdmission) setLimit(limit int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.limit = normalizeAdmissionLimit(limit)
}

func (a *recordAdmission) freeSlots() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	free := a.limit - len(a.active)
	if free < 0 {
		return 0
	}
	return free
}

// snapshot returns used and limit for stats observers.
func (a *recordAdmission) snapshot() (used, limit int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.active), a.limit
}

// tryAdmit reserves id if it is not already admitted and capacity remains.
func (a *recordAdmission) tryAdmit(id string) bool {
	return a.tryAdmitKey(id, "")
}

// tryAdmitKey is tryAdmit remembering the dispatch key the delivery belongs
// to, so claim queries can exclude active rows per key.
func (a *recordAdmission) tryAdmitKey(id, key string) bool {
	return a.tryAdmitDelivery(id, key, key)
}

// tryAdmitDelivery is tryAdmitKey also counting the delivery against its
// handler type (see handlerActive).
func (a *recordAdmission) tryAdmitDelivery(id, key, handler string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, exists := a.active[id]; exists {
		return false
	}
	if len(a.active) >= a.limit {
		return false
	}
	a.active[id] = admittedDelivery{key: key, handler: handler}
	a.byHandler[handler]++
	return true
}

func (a *recordAdmission) release(id string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	d, ok := a.active[id]
	if !ok {
		return
	}
	delete(a.active, id)
	if a.byHandler[d.handler]--; a.byHandler[d.handler] <= 0 {
		delete(a.byHandler, d.handler)
	}
}

// handlerActive is the number of admitted deliveries of handler type.
func (a *recordAdmission) handlerActive(handler string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.byHandler[handler]
}

func (a *recordAdmission) contains(id string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.active[id]
	return ok
}

// idsForKey lists admitted delivery IDs belonging to key. Bounded by the
// per-key queue depth plus one executing delivery.
func (a *recordAdmission) idsForKey(key string) []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var ids []string
	for id, d := range a.active {
		if d.key == key {
			ids = append(ids, id)
		}
	}
	return ids
}
