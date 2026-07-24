package sqlite

import "sync"

// recordAdmission is a process-local set of outbox record IDs currently admitted
// for dispatch. It is not durable truth: after a crash the set is empty and
// Start resets taken_at so rows may be redelivered (at-least-once).
//
// Capacity bounds the number of in-flight record coordinators, not channel depth.
// tryAdmit never blocks; callers must admit before claiming a row.
type recordAdmission struct {
	mu     sync.Mutex
	active map[string]struct{}
	limit  int
}

func newRecordAdmission(limit int) *recordAdmission {
	return &recordAdmission{
		active: make(map[string]struct{}),
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

// tryAdmit reserves id if it is not already admitted and capacity remains.
// Returns false without blocking when the record is already active or the set is full.
func (a *recordAdmission) tryAdmit(id string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, exists := a.active[id]; exists {
		return false
	}
	if len(a.active) >= a.limit {
		return false
	}
	a.active[id] = struct{}{}
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
