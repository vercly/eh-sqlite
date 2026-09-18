package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vercly/eventhorizon"
	"github.com/vercly/eventhorizon/mocks"
	"github.com/vercly/eventhorizon/uuid"
)

// BenchmarkDeliveryBaseline records the pre-refactor delivery-isolation
// baseline. It keeps a 64-recipient fanout backlog active while independent
// fast events are published and completed. Run with -benchmem; the benchmark
// reports latency percentiles for the fast stream and Go reports allocations.
//
// The backlog is intentionally larger than a normal short benchmark run. This
// makes the result describe the contended state instead of the drained tail.
func BenchmarkDeliveryBaseline(b *testing.B) {
	const (
		fanoutRecipients  = 64
		backlogEvents     = 256
		slowDuration      = 5 * time.Millisecond
		maxLatencySamples = 100_000
	)

	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_txlock=immediate&_journal_mode=WAL&_busy_timeout=5000&_synchronous=NORMAL&_fk=1&_loc=auto", filepath.Join(b.TempDir(), "baseline.db")))
	if err != nil {
		b.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	b.Cleanup(func() { _ = db.Close() })
	var journalMode string
	if err := db.QueryRow(`PRAGMA journal_mode`).Scan(&journalMode); err != nil {
		b.Fatal(err)
	}
	if journalMode != "wal" {
		b.Fatalf("journal_mode = %q, want wal", journalMode)
	}
	ctx := context.Background()
	fast := &baselineFastHandler{samples: make([]time.Duration, 0, min(b.N, maxLatencySamples)), signal: make(chan struct{}, 1)}
	slowEntered := make(chan struct{})
	slow := &baselineSlowHandler{duration: slowDuration, entered: slowEntered}

	o, err := NewOutbox(db)
	if err != nil {
		b.Fatal(err)
	}
	defer o.Close()

	if err := o.AddHandler(ctx, eventhorizon.MatchEvents{mocks.EventType}, slow); err != nil {
		b.Fatal(err)
	}
	for i := 1; i < fanoutRecipients; i++ {
		h := &baselineNoopHandler{typ: eventhorizon.EventHandlerType("baseline_fanout_" + strconv.Itoa(i))}
		if err := o.AddHandler(ctx, eventhorizon.MatchEvents{mocks.EventType}, h); err != nil {
			b.Fatal(err)
		}
	}
	if err := o.AddHandler(ctx, eventhorizon.MatchEvents{mocks.EventOtherType}, fast); err != nil {
		b.Fatal(err)
	}

	if err := o.StartChecked(); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < backlogEvents; i++ {
		event := eventhorizon.NewEvent(mocks.EventType, &mocks.EventData{Content: "baseline-backlog"}, time.Now(),
			eventhorizon.ForAggregate(mocks.AggregateType, uuid.New(), 1))
		if err := o.HandleEvent(ctx, event); err != nil {
			b.Fatal(err)
		}
	}
	select {
	case <-slowEntered:
	case <-time.After(5 * time.Second):
		b.Fatal("slow handler did not enter before benchmark")
	}
	processingErrors := make(chan error, 16)
	stopErrors := make(chan struct{})
	var errorWG sync.WaitGroup
	errorWG.Add(1)
	go func() {
		defer errorWG.Done()
		for {
			select {
			case err := <-o.Errors():
				if err != nil {
					select {
					case processingErrors <- err:
					default:
					}
				}
			case <-stopErrors:
				return
			}
		}
	}()

	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		started := time.Now()
		event := eventhorizon.NewEvent(eventhorizon.EventType(mocks.EventOtherType),
			&mocks.EventData{Content: "baseline-fast"}, started,
			eventhorizon.ForAggregate(mocks.AggregateType, uuid.New(), 1),
			eventhorizon.WithMetadata(map[string]any{"baseline.started": started}))
		if err := o.HandleEvent(ctx, event); err != nil {
			b.Fatal(err)
		}
	}
	if !fast.waitFor(b.N, 30*time.Second) {
		b.Fatal("fast events did not complete before safety timeout")
	}
	b.StopTimer()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	close(stopErrors)
	errorWG.Wait()
	select {
	case err := <-processingErrors:
		b.Fatalf("unexpected outbox processing error: %v", err)
	default:
	}

	fast.report(b)
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "fast-events/s")
	if after.TotalAlloc > before.TotalAlloc {
		b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc)/float64(b.N), "heap-alloc-bytes/op")
	}
}

type baselineSlowHandler struct {
	duration time.Duration
	entered  chan struct{}
	once     sync.Once
}

func (h *baselineSlowHandler) HandlerType() eventhorizon.EventHandlerType { return "baseline_slow" }
func (h *baselineSlowHandler) HandleEvent(context.Context, eventhorizon.Event) error {
	h.once.Do(func() { close(h.entered) })
	time.Sleep(h.duration)
	return nil
}

type baselineNoopHandler struct{ typ eventhorizon.EventHandlerType }

func (h *baselineNoopHandler) HandlerType() eventhorizon.EventHandlerType            { return h.typ }
func (h *baselineNoopHandler) HandleEvent(context.Context, eventhorizon.Event) error { return nil }

type baselineFastHandler struct {
	mu        sync.Mutex
	samples   []time.Duration
	signal    chan struct{}
	completed int
}

func (h *baselineFastHandler) HandlerType() eventhorizon.EventHandlerType { return "baseline_fast" }
func (h *baselineFastHandler) HandleEvent(_ context.Context, event eventhorizon.Event) error {
	started := event.Timestamp()
	h.mu.Lock()
	if len(h.samples) < 100_000 {
		h.samples = append(h.samples, time.Since(started))
	}
	h.completed++
	h.mu.Unlock()
	select {
	case h.signal <- struct{}{}:
	default:
	}
	return nil
}

func (h *baselineFastHandler) waitFor(n int, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		h.mu.Lock()
		completed := h.completed
		h.mu.Unlock()
		if completed >= n {
			return true
		}
		select {
		case <-h.signal:
		case <-timer.C:
			return false
		}
	}
}

func (h *baselineFastHandler) report(b *testing.B) {
	h.mu.Lock()
	samples := append([]time.Duration(nil), h.samples...)
	h.mu.Unlock()
	if len(samples) == 0 {
		return
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	percentile := func(p float64) time.Duration {
		index := int(float64(len(samples)-1) * p)
		return samples[index]
	}
	b.ReportMetric(float64(percentile(.50).Nanoseconds()), "fast-p50-ns")
	b.ReportMetric(float64(percentile(.95).Nanoseconds()), "fast-p95-ns")
	b.ReportMetric(float64(percentile(.99).Nanoseconds()), "fast-p99-ns")
}
