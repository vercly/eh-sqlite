package sqlite

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	eh "github.com/vercly/eventhorizon"
	"github.com/vercly/eventhorizon/mocks"
)

// TestFinalizeGroupIsolatesFailedOutcome: concurrent finalizations queued
// behind a leader commit as one group; an outcome failing inside the group
// rolls back alone (savepoint) and succeeds on its regular retry, while the
// rest of the group commits exactly once.
func TestFinalizeGroupIsolatesFailedOutcome(t *testing.T) {
	const recipients = 8
	db := newTestDB(t)
	o := v2NewOutbox(t, db, WithMaxGoroutines(recipients))
	handlers := make([]*v2CountingHandler, recipients)
	for i := range handlers {
		handlers[i] = &v2CountingHandler{typ: eh.EventHandlerType(fmt.Sprintf("h%d", i))}
		if err := o.AddHandler(context.Background(), eh.MatchEvents{mocks.EventType}, handlers[i]); err != nil {
			t.Fatal(err)
		}
	}
	prepareWithoutFetcher(t, o)
	v2Publish(t, o, "e")

	var (
		mu       sync.Mutex
		attempts = map[string][]int{}
		first    string
		failed   string
	)
	injected := errors.New("injected finalize failure")
	o.beforeFinalizeCommit = func(id string, attempt int) error {
		mu.Lock()
		attempts[id] = append(attempts[id], attempt)
		if first == "" {
			// The first finalize leads alone; hold it until every other
			// worker has queued behind it, so they form one group.
			first = id
			mu.Unlock()
			waitUntil(t, 5*time.Second, func() bool {
				o.finalizer.mu.Lock()
				defer o.finalizer.mu.Unlock()
				return len(o.finalizer.queue) == recipients-1
			})
			return nil
		}
		defer mu.Unlock()
		if failed == "" && id != first {
			failed = id
			return injected
		}
		return nil
	}
	v2Drain(t, o)

	for _, h := range handlers {
		if h.calls.Load() != 1 {
			t.Fatalf("handler %s calls=%d, want 1", h.typ, h.calls.Load())
		}
	}
	if countDeliveries(t, o) != 0 || countPublications(t, o) != 0 || countDeadLetters(t, o) != 0 {
		t.Fatalf("deliveries=%d publications=%d dead_letters=%d, want all 0",
			countDeliveries(t, o), countPublications(t, o), countDeadLetters(t, o))
	}
	mu.Lock()
	defer mu.Unlock()
	if len(attempts) != recipients {
		t.Fatalf("finalized %d deliveries, want %d", len(attempts), recipients)
	}
	for id, got := range attempts {
		want := []int{1}
		if id == failed {
			want = []int{1, 2}
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("delivery %s attempts=%v, want %v", id, got, want)
		}
	}
	if failed == "" {
		t.Fatal("no grouped finalize was failed")
	}
}
