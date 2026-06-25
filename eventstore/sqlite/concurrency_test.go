package sqlite

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	eh "github.com/vercly/eventhorizon"
	"github.com/vercly/eventhorizon/mocks"
	"github.com/vercly/eventhorizon/uuid"
)

func queryAllPositions(t *testing.T, store *EventStore) []int {
	t.Helper()
	rows, err := store.db.Query(`SELECT position FROM ` + store.eventsTable + ` ORDER BY position ASC`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var positions []int
	for rows.Next() {
		var p int
		if err := rows.Scan(&p); err != nil {
			t.Fatal(err)
		}
		positions = append(positions, p)
	}
	return positions
}

// TestConcurrentSavesDifferentAggregates is the core Phase 3 assertion: concurrent
// saves for distinct aggregates must produce unique, contiguous global positions
// with no primary-key collisions.
func TestConcurrentSavesDifferentAggregates(t *testing.T) {
	store, err := newTestEventStore(t)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	const n = 60

	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := uuid.New()
			ev := eh.NewEventForAggregate(mocks.EventType, &mocks.EventData{Content: "c"},
				time.Now(), mocks.AggregateType, id, 1)
			if err := store.Save(ctx, []eh.Event{ev}, 0); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent save failed: %v", err)
	}

	positions := queryAllPositions(t, store)
	if len(positions) != n {
		t.Fatalf("expected %d events, got %d", n, len(positions))
	}
	for i, p := range positions {
		want := i + 1
		if p != want {
			t.Fatalf("positions not unique/contiguous: at index %d got position %d, want %d", i, p, want)
		}
	}
}

// TestMultiEventContiguousPositions verifies a multi-event save reserves a
// contiguous block of positions.
func TestMultiEventContiguousPositions(t *testing.T) {
	store, err := newTestEventStore(t)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	id := uuid.New()
	evs := []eh.Event{
		eh.NewEventForAggregate(mocks.EventType, &mocks.EventData{Content: "1"}, time.Now(), mocks.AggregateType, id, 1),
		eh.NewEventForAggregate(mocks.EventType, &mocks.EventData{Content: "2"}, time.Now(), mocks.AggregateType, id, 2),
		eh.NewEventForAggregate(mocks.EventType, &mocks.EventData{Content: "3"}, time.Now(), mocks.AggregateType, id, 3),
	}
	if err := store.Save(ctx, evs, 0); err != nil {
		t.Fatal(err)
	}

	positions := queryAllPositions(t, store)
	sort.Ints(positions)
	want := []int{1, 2, 3}
	if len(positions) != len(want) {
		t.Fatalf("expected %v, got %v", want, positions)
	}
	for i := range want {
		if positions[i] != want[i] {
			t.Fatalf("expected contiguous %v, got %v", want, positions)
		}
	}
}

// TestConcurrentSaveSameAggregateConflict verifies that with two concurrent saves
// for the same aggregate version, exactly one succeeds and the other gets a clean
// event-store conflict (not a primary-key error), with no leaked global position.
func TestConcurrentSaveSameAggregateConflict(t *testing.T) {
	store, err := newTestEventStore(t)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	id := uuid.New()

	// Seed version 1.
	v1 := eh.NewEventForAggregate(mocks.EventType, &mocks.EventData{Content: "v1"},
		time.Now(), mocks.AggregateType, id, 1)
	if err := store.Save(ctx, []eh.Event{v1}, 0); err != nil {
		t.Fatal(err)
	}

	mkV2 := func(content string) []eh.Event {
		return []eh.Event{eh.NewEventForAggregate(mocks.EventType, &mocks.EventData{Content: content},
			time.Now(), mocks.AggregateType, id, 2)}
	}

	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, content := range []string{"a", "b"} {
		wg.Add(1)
		go func(c string) {
			defer wg.Done()
			results <- store.Save(ctx, mkV2(c), 1)
		}(content)
	}
	wg.Wait()
	close(results)

	var success, conflict int
	for err := range results {
		switch {
		case err == nil:
			success++
		case errors.Is(err, eh.ErrEventConflictFromOtherSave):
			conflict++
		default:
			t.Fatalf("unexpected error (want conflict): %v", err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("expected exactly 1 success and 1 conflict, got success=%d conflict=%d", success, conflict)
	}

	// Exactly two events committed (v1 + the winning v2): positions 1 and 2, no leak.
	positions := queryAllPositions(t, store)
	if len(positions) != 2 || positions[0] != 1 || positions[1] != 2 {
		t.Fatalf("expected positions [1 2] with no leaked range, got %v", positions)
	}
}
