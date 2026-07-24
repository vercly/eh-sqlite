package sqlite

import (
	"context"
	"sync"
)

// fairSem is a FIFO semaphore for global HandleEvent permits. Release always
// prefers queued waiters over the releaser re-entering, so a saturated key
// cannot monopolize permits when other keys are waiting.
type fairSem struct {
	mu        sync.Mutex
	available int
	capacity  int
	waiters   []chan struct{}
}

func newFairSem(capacity int) *fairSem {
	if capacity < 1 {
		capacity = 1
	}
	return &fairSem{available: capacity, capacity: capacity}
}

func (s *fairSem) acquire(ctx context.Context) error {
	s.mu.Lock()
	// Fast path only when nobody is already waiting (preserve FIFO).
	if s.available > 0 && len(s.waiters) == 0 {
		s.available--
		s.mu.Unlock()
		return nil
	}
	ch := make(chan struct{}, 1)
	s.waiters = append(s.waiters, ch)
	s.mu.Unlock()

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		s.cancelWaiter(ch)
		return ctx.Err()
	}
}

func (s *fairSem) cancelWaiter(ch chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, w := range s.waiters {
		if w == ch {
			s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
			// If a permit was already handed off into ch, put it back.
			select {
			case <-ch:
				s.releaseLocked()
			default:
			}
			return
		}
	}
	// Acquired between cancel and remove: restore permit.
	select {
	case <-ch:
		s.releaseLocked()
	default:
	}
}

func (s *fairSem) release() {
	s.mu.Lock()
	s.releaseLocked()
	s.mu.Unlock()
}

func (s *fairSem) releaseLocked() {
	if len(s.waiters) > 0 {
		w := s.waiters[0]
		s.waiters = s.waiters[1:]
		w <- struct{}{}
		return
	}
	if s.available < s.capacity {
		s.available++
	}
}

// hasWaiters reports whether at least one goroutine is queued (test/observability).
func (s *fairSem) hasWaiters() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.waiters) > 0
}
