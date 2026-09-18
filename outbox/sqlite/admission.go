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
	mu     sync.Mutex
	active map[string]string // delivery id -> dispatch key ("" when unknown)
	limit  int
}

func newRecordAdmission(limit int) *recordAdmission {
	return &recordAdmission{
		active: make(map[string]string),
		limit:  normalizeAdmissionLimit(limit),
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
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, exists := a.active[id]; exists {
		return false
	}
	if len(a.active) >= a.limit {
		return false
	}
	a.active[id] = key
	return true
}

func (a *recordAdmission) release(id string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.active, id)
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
	for id, k := range a.active {
		if k == key {
			ids = append(ids, id)
		}
	}
	return ids
}
