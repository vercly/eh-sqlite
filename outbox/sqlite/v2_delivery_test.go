package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vercly/eh-sqlite/schema"
	eh "github.com/vercly/eventhorizon"
	"github.com/vercly/eventhorizon/mocks"
	"github.com/vercly/eventhorizon/uuid"
)

// Tests for the v2 per-delivery contract. Helpers here are prefixed v2 and
// live only in this file.

type v2CountingHandler struct {
	typ   eh.EventHandlerType
	calls atomic.Int32
	err   error
	// gate, when non-nil, blocks HandleEvent until released (barrier).
	gate    chan struct{}
	entered chan struct{}
}

func (h *v2CountingHandler) HandlerType() eh.EventHandlerType { return h.typ }
func (h *v2CountingHandler) HandleEvent(context.Context, eh.Event) error {
	h.calls.Add(1)
	if h.entered != nil {
		h.entered <- struct{}{}
	}
	if h.gate != nil {
		<-h.gate
	}
	return h.err
}

type v2FatalError struct{ error }

func (v2FatalError) OutboxSeverity() ErrorSeverity { return SeverityFatal }

// v2Gate returns a gate channel whose cleanup (registered AFTER the outbox
// cleanup, so it runs BEFORE Close) unblocks any handler still waiting, even
// when the test failed early. Tests may close it themselves.
func v2Gate(t *testing.T) (chan struct{}, func()) {
	t.Helper()
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	return gate, release
}

func v2NewOutbox(t *testing.T, db *sql.DB, options ...Option) *Outbox {
	t.Helper()
	o, err := NewOutbox(db, options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = o.Close() })
	o.finalizeSleep = func(time.Duration) {}
	return o
}

func v2Publish(t *testing.T, o *Outbox, content string) eh.Event {
	t.Helper()
	event := newTestEvent(content)
	if err := o.HandleEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	return event
}

// v2Drain runs claim passes until nothing is claimed twice in a row and all
// claimed deliveries finalized (synchronous progress for assertions).
func v2Drain(t *testing.T, o *Outbox) {
	t.Helper()
	for range 50 {
		n, err := o.processBatch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return
		}
	}
	t.Fatal("outbox did not drain")
}

func TestV2SlowSiblingDoesNotHoldFastSiblingCompletion(t *testing.T) {
	db := newTestDB(t)
	o := v2NewOutbox(t, db)
	gate, release := v2Gate(t)
	slow := &v2CountingHandler{typ: "slow", gate: gate, entered: make(chan struct{}, 1)}
	fast := &v2CountingHandler{typ: "fast"}
	for _, h := range []eh.EventHandler{slow, fast} {
		if err := o.AddHandler(context.Background(), eh.MatchEvents{mocks.EventType}, h); err != nil {
			t.Fatal(err)
		}
	}
	if err := o.StartChecked(); err != nil {
		t.Fatal(err)
	}
	v2Publish(t, o, "one")
	<-slow.entered

	// The fast sibling completes and is durably removed while slow still runs.
	waitUntil(t, 5*time.Second, func() bool {
		for _, d := range listDeliveries(t, o) {
			if d.HandlerType == "fast" {
				return false
			}
		}
		return true
	})
	rows := listDeliveries(t, o)
	if len(rows) != 1 || rows[0].HandlerType != "slow" || !rows[0].TakenAt.Valid {
		t.Fatalf("expected only the in-flight slow delivery, got %+v", rows)
	}
	if countPublications(t, o) != 1 {
		t.Fatal("publication must survive while a delivery still references it")
	}
	release()
	waitUntil(t, 5*time.Second, func() bool { return countDeliveries(t, o) == 0 })
	if countPublications(t, o) != 0 {
		t.Fatal("publication must be garbage collected with its last delivery")
	}
	if used, _ := o.AdmissionSnapshot(); used != 0 {
		t.Fatalf("admission leaked: used=%d", used)
	}
}

func TestV2FinalizeRetriesSQLWithSavedOutcomeWithoutRerunningHandler(t *testing.T) {
	db := newTestDB(t)
	o := v2NewOutbox(t, db)
	h := &v2CountingHandler{typ: "h"}
	if err := o.AddHandler(context.Background(), eh.MatchEvents{mocks.EventType}, h); err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	boom := errors.New("commit failed")
	o.beforeFinalizeCommit = func(_ string, attempt int) error {
		attempts.Store(int32(attempt))
		if attempt < 3 {
			return boom
		}
		return nil
	}
	if err := o.StartChecked(); err != nil {
		t.Fatal(err)
	}
	v2Publish(t, o, "x")
	waitUntil(t, 5*time.Second, func() bool { return countDeliveries(t, o) == 0 })
	if h.calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", h.calls.Load())
	}
	if attempts.Load() != 3 {
		t.Fatalf("finalize attempts = %d, want 3", attempts.Load())
	}
	if used, _ := o.AdmissionSnapshot(); used != 0 {
		t.Fatalf("admission leaked: used=%d", used)
	}
}

func TestV2FinalizeExhaustedReleasesClaimAndKeepsRetryCount(t *testing.T) {
	db := newTestDB(t)
	o := v2NewOutbox(t, db, WithMaxRetries(5))
	h := &v2CountingHandler{typ: "h", err: errors.New("retryable")}
	if err := o.AddHandler(context.Background(), eh.MatchEvents{mocks.EventType}, h); err != nil {
		t.Fatal(err)
	}
	o.beforeFinalizeCommit = func(string, int) error { return errors.New("db down") }
	// No background fetcher: hooks and handler state are mutated between
	// manual passes, which must not race a live processor.
	// Seed a delivery that already has retry_count 2 so we can prove the
	// release path leaves it untouched (a retry finalize would set 3).
	_, id := insertDeliveryDirect(t, o, deliverySeed{Event: newTestEvent("x"), HandlerType: "h", CreatedAt: time.Now().Add(-time.Minute), RetryCount: 2})
	prepareWithoutFetcher(t, o)
	before := schema.UTC(time.Now())
	if _, err := o.processBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	rows := listDeliveries(t, o)
	if len(rows) != 1 || rows[0].ID != id {
		t.Fatalf("delivery must remain, got %+v", rows)
	}
	if rows[0].TakenAt.Valid {
		t.Fatal("claim was not released")
	}
	if rows[0].RetryCount != 2 {
		t.Fatalf("retry_count changed on release: %d", rows[0].RetryCount)
	}
	if !rows[0].AvailableAt.After(before) {
		t.Fatalf("available_at %s must move into the future on release (now %s)", rows[0].AvailableAt, before)
	}
	if h.calls.Load() != 1 {
		t.Fatalf("handler ran %d times", h.calls.Load())
	}
	if used, _ := o.AdmissionSnapshot(); used != 0 {
		t.Fatalf("admission leaked: used=%d", used)
	}

	// Release failing too: FinalizeStuck, row keeps taken_at, admission freed;
	// after the DB "recovers" the taken_at timeout reclaims it.
	o.beforeReleaseClaim = func(string) error { return errors.New("db still down") }
	setTakenAt(t, o, id, time.Time{})
	if _, err := o.db.Exec(fmt.Sprintf(`UPDATE %s SET available_at = ? WHERE id = ?`, o.deliveriesTable), before, id); err != nil {
		t.Fatal(err)
	}
	if _, err := o.processBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	rows = listDeliveries(t, o)
	if !rows[0].TakenAt.Valid {
		t.Fatal("stuck delivery must keep taken_at until the timeout")
	}
	if used, _ := o.AdmissionSnapshot(); used != 0 {
		t.Fatalf("admission leaked after stuck: used=%d", used)
	}
	o.beforeFinalizeCommit = nil
	o.beforeReleaseClaim = nil
	h.err = nil
	setTakenAt(t, o, id, time.Now().Add(-2*PeriodicSweepAge))
	if _, err := o.processBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if countDeliveries(t, o) != 0 {
		t.Fatal("stale stuck claim was not reclaimed and completed")
	}
}

func TestV2ClaimFailureReleasesEveryReservation(t *testing.T) {
	db := newTestDB(t)
	stats := &v2AdmissionRecorder{}
	o := v2NewOutbox(t, db, WithQueueDepth(4), WithDispatchStats(stats))
	a := &v2CountingHandler{typ: "a"}
	b := &v2CountingHandler{typ: "b"}
	for _, h := range []eh.EventHandler{a, b} {
		if err := o.AddHandler(context.Background(), eh.MatchEvents{mocks.EventType}, h); err != nil {
			t.Fatal(err)
		}
	}
	prepareWithoutFetcher(t, o) // manual passes only: no live fetcher to race
	for i := range 3 {
		v2Publish(t, o, fmt.Sprintf("e%d", i))
	}
	injected := errors.New("commit failed")
	var reservedAtCommit int
	o.beforeClaimCommit = func(reserved int) error {
		reservedAtCommit = reserved
		return injected
	}
	_, _, err := o.claimPass(context.Background())
	if !errors.Is(err, injected) {
		t.Fatalf("claimPass err = %v", err)
	}
	if reservedAtCommit < 1 {
		t.Fatalf("test must inject after at least one reservation, got %d", reservedAtCommit)
	}
	if used, _ := o.AdmissionSnapshot(); used != 0 {
		t.Fatalf("admission leaked: used=%d", used)
	}
	for _, key := range []string{"a", "b"} {
		if free := o.dispatch.freeCapacity(key); free != 4 {
			t.Fatalf("queue %s free=%d, want 4 (reservation leaked)", key, free)
		}
	}
	for _, d := range listDeliveries(t, o) {
		if d.TakenAt.Valid {
			t.Fatalf("claim was persisted despite failed commit: %+v", d)
		}
	}
	if a.calls.Load() != 0 || b.calls.Load() != 0 {
		t.Fatal("handlers ran for a rolled-back claim")
	}
	// Recovery: the next pass claims and delivers everything.
	o.beforeClaimCommit = nil
	v2Drain(t, o)
	if countDeliveries(t, o) != 0 || a.calls.Load() != 3 || b.calls.Load() != 3 {
		t.Fatalf("after recovery: deliveries=%d a=%d b=%d", countDeliveries(t, o), a.calls.Load(), b.calls.Load())
	}
}

type v2AdmissionRecorder struct {
	mu        sync.Mutex
	admission []int
	skips     map[ClaimSkipReason]int
	finalizes map[FinalizeOutcome]int
}

func (r *v2AdmissionRecorder) ObserveQueueDepth(string, string, int) {}
func (r *v2AdmissionRecorder) ObserveInFlight(string, string, int)   {}
func (r *v2AdmissionRecorder) ObserveAdmission(used, _ int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.admission = append(r.admission, used)
}
func (r *v2AdmissionRecorder) ObserveClaimSkip(reason ClaimSkipReason) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.skips == nil {
		r.skips = map[ClaimSkipReason]int{}
	}
	r.skips[reason]++
}
func (r *v2AdmissionRecorder) ObserveFinalize(outcome FinalizeOutcome, _ time.Duration, _ bool, _ error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finalizes == nil {
		r.finalizes = map[FinalizeOutcome]int{}
	}
	r.finalizes[outcome]++
}

func TestV2MigrationSplitsV1RowsPreservesOrderAndIsIdempotent(t *testing.T) {
	db := newTestDB(t)
	createV1Table(t, db, "outbox")
	base := time.Date(2026, 1, 25, 0, 0, 0, 0, time.UTC) // in the past, so migrated rows are due
	warsaw, _ := time.LoadLocation("Europe/Warsaw")
	// Three rows whose raw text order disagrees with instant order (local
	// offsets) so the migration must sort on parsed
	// instants: created 00:10Z, 00:20Z, 00:30Z.
	idA := insertV1Row(t, db, "outbox", v1Seed{Event: newTestEvent("a"), Handlers: []string{"h1", "h2"},
		CreatedAt: base, RawCreatedAt: base.Add(10 * time.Minute).In(warsaw).Format(schema.StoredTimeLayout)})
	idB := insertV1Row(t, db, "outbox", v1Seed{Event: newTestEvent("b"), Handlers: nil, // sentinel
		CreatedAt: base, RawCreatedAt: base.Add(20 * time.Minute).Format("2006-01-02T15:04:05Z07:00")})
	idC := insertV1Row(t, db, "outbox", v1Seed{Event: newTestEvent("c"), Handlers: []string{"h1"},
		CreatedAt: base.Add(30 * time.Minute), RetryCount: 3, TakenAt: base.Add(31 * time.Minute)})

	ctx := context.Background()
	report, err := Migrate(ctx, db, "outbox")
	if err != nil {
		t.Fatal(err)
	}
	if !report.Applied || report.Publications != 3 || report.Deliveries != 3 || report.Sentinels != 1 || report.V1Table != "outbox" {
		t.Fatalf("report = %+v", report)
	}
	if exists, _ := schema.TableExists(ctx, db, "outbox"); exists {
		t.Fatal("v1 table must be renamed")
	}
	if exists, _ := schema.TableExists(ctx, db, "outbox_v1_migrated"); !exists {
		t.Fatal("v1 backup table missing")
	}
	o := v2NewOutbox(t, db)
	rows := listDeliveries(t, o)
	wantLegacy := []string{idA, idA, idB, idC}
	if len(rows) != 4 {
		t.Fatalf("deliveries = %d", len(rows))
	}
	for i, r := range rows {
		if r.LegacyOutboxID != wantLegacy[i] {
			t.Fatalf("seq order lost: row %d legacy=%s want %s", i, r.LegacyOutboxID, wantLegacy[i])
		}
		if r.TakenAt.Valid {
			t.Fatalf("migrated row %d must not be claimed", i)
		}
		raw := storedText(t, db, fmt.Sprintf(`SELECT CAST(available_at AS TEXT) FROM %s WHERE id = ?`, o.deliveriesTable), r.ID)
		if parsed, err := schema.ParseStored(raw); err != nil || raw != schema.FormatStored(parsed) {
			t.Fatalf("available_at not canonical UTC: %q", raw)
		}
	}
	if rows[2].HandlerType != "" {
		t.Fatal("handlers=[] row must become a sentinel")
	}
	if rows[3].RetryCount != 3 {
		t.Fatalf("retry_count not carried: %d", rows[3].RetryCount)
	}
	var origin, ref string
	if err := db.QueryRow(fmt.Sprintf(`SELECT origin, origin_ref FROM %s WHERE publication_id = ?`, o.publicationsTable), rows[0].PublicationID).Scan(&origin, &ref); err != nil {
		t.Fatal(err)
	}
	if origin != originMigrated || ref != idA {
		t.Fatalf("provenance origin=%s ref=%s", origin, ref)
	}

	// Idempotent: a second Migrate and StartChecked touch nothing.
	again, err := Migrate(ctx, db, "outbox")
	if err != nil || again.Applied {
		t.Fatalf("second migrate applied=%v err=%v", again.Applied, err)
	}
	h1 := &v2CountingHandler{typ: "h1"}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, h1); err != nil {
		t.Fatal(err)
	}
	if err := o.StartChecked(); err != nil {
		t.Fatal(err)
	}
	// h2 is not registered: its delivery is flagged unresolved, never deleted;
	// the sentinel rematches to h1 only; h1 rows deliver.
	waitUntil(t, 5*time.Second, func() bool { return h1.calls.Load() == 3 })
	waitUntil(t, 5*time.Second, func() bool {
		rows := listDeliveries(t, o)
		return len(rows) == 1 && rows[0].HandlerType == "h2"
	})
	left := listDeliveries(t, o)[0]
	if !left.UnresolvedAt.Valid || left.DispatchKey != "" || left.TakenAt.Valid {
		t.Fatalf("unresolved h2 delivery state: %+v", left)
	}
	if countPublications(t, o) != 1 {
		t.Fatalf("publications = %d, want 1 (only the one still referenced)", countPublications(t, o))
	}
	var sawUnresolved bool
	for range 10 {
		select {
		case err := <-o.Errors():
			if errors.Is(err, ErrUnresolvedHandler) {
				sawUnresolved = true
			}
		default:
		}
	}
	if !sawUnresolved {
		t.Fatal("ErrUnresolvedHandler diagnostics missing")
	}
}

func TestV2StartupRecomputesDispatchKeysAfterShardChangeAndResetsClaims(t *testing.T) {
	db := newTestDB(t)
	aggregate := uuid.New()
	first := v2NewOutbox(t, db)
	gate, release := v2Gate(t)
	h := &v2CountingHandler{typ: "p", gate: gate, entered: make(chan struct{}, 4)}
	if err := first.AddHandlerWithOptions(context.Background(), eh.MatchEvents{mocks.EventType}, h,
		WithDispatchMode(PartitionByAggregate), WithPartitionShards(4)); err != nil {
		t.Fatal(err)
	}
	if err := first.StartChecked(); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		if err := first.HandleEvent(context.Background(), newTestEventForAggregate(fmt.Sprintf("e%d", i), aggregate)); err != nil {
			t.Fatal(err)
		}
	}
	<-h.entered // first delivery claimed and executing (taken_at set)
	rows := listDeliveries(t, first)
	shard4 := fmt.Sprintf("p:%d", hashPartition(aggregate.String(), 4))
	for _, r := range rows {
		if r.DispatchKey != shard4 {
			t.Fatalf("dispatch_key=%s want %s", r.DispatchKey, shard4)
		}
	}
	// "Crash": stop the fetcher while the delivery still executes (taken_at
	// stays set), then let the handler return so Close can finish; its
	// finalize commits after Close returns from waitWorkers.
	first.shuttingDown.Store(true)
	first.cancel()
	first.wg.Wait()
	claimedBefore := listDeliveries(t, first)
	release()
	_ = first.Close()
	_ = claimedBefore

	second := v2NewOutbox(t, db)
	h2 := &v2CountingHandler{typ: "p"}
	if err := second.AddHandlerWithOptions(context.Background(), eh.MatchEvents{mocks.EventType}, h2,
		WithDispatchMode(PartitionByAggregate), WithPartitionShards(16)); err != nil {
		t.Fatal(err)
	}
	// Reconcile only (no fetcher) so the state is observable.
	reconcileForTest(t, second)
	shard16 := fmt.Sprintf("p:%d", hashPartition(aggregate.String(), 16))
	for _, r := range listDeliveries(t, second) {
		if r.DispatchKey != shard16 {
			t.Fatalf("dispatch_key not recomputed: %s want %s", r.DispatchKey, shard16)
		}
		if r.TakenAt.Valid {
			t.Fatal("taken_at not reset at startup")
		}
		if r.UnresolvedAt.Valid {
			t.Fatal("registered handler flagged unresolved")
		}
	}
	var cfg string
	if err := db.QueryRow(fmt.Sprintf(`SELECT dispatch_config FROM %s LIMIT 1`, second.deliveriesTable)).Scan(&cfg); err != nil {
		t.Fatal(err)
	}
	if cfg != "mode=partition;shards=16" {
		t.Fatalf("dispatch_config=%s", cfg)
	}
}

func TestV2SentinelIsExpandedBeforeNewerNormalDeliveryOnSameKey(t *testing.T) {
	db := newTestDB(t)
	o := v2NewOutbox(t, db)
	var order []string
	var mu sync.Mutex
	h := &v2OrderHandler{typ: "h", record: func(c string) { mu.Lock(); order = append(order, c); mu.Unlock() }}
	if err := o.AddHandler(context.Background(), eh.MatchEvents{mocks.EventType}, h); err != nil {
		t.Fatal(err)
	}
	older := time.Now().Add(-time.Minute)
	// Older sentinel (replayed no_match) and a newer normal delivery on key "h".
	insertDeliveryDirect(t, o, deliverySeed{Event: newTestEvent("sentinel-older"), HandlerType: "", CreatedAt: older, AvailableAt: older})
	insertDeliveryDirect(t, o, deliverySeed{Event: newTestEvent("normal-newer"), HandlerType: "h", CreatedAt: time.Now()})
	// Plus more sentinels than one quantum to prove all due sentinels expand first.
	for i := range claimQuantum + 2 {
		insertDeliveryDirect(t, o, deliverySeed{Event: newTestEvent(fmt.Sprintf("sentinel-%02d", i)), HandlerType: "", CreatedAt: older.Add(time.Duration(i+1) * time.Second)})
	}
	prepareWithoutFetcher(t, o)
	v2Drain(t, o)
	mu.Lock()
	defer mu.Unlock()
	if len(order) != claimQuantum+4 {
		t.Fatalf("delivered %d, want %d: %v", len(order), claimQuantum+4, order)
	}
	if order[0] != "sentinel-older" {
		t.Fatalf("older sentinel overtaken: %v", order)
	}
	if order[len(order)-1] != "normal-newer" {
		t.Fatalf("newer normal delivery must come after every older sentinel: %v", order)
	}
	for i := 1; i < len(order)-1; i++ {
		if !strings.HasPrefix(order[i], "sentinel-") {
			t.Fatalf("FIFO broken at %d: %v", i, order)
		}
	}
}

type v2OrderHandler struct {
	typ    eh.EventHandlerType
	record func(content string)
}

func (h *v2OrderHandler) HandlerType() eh.EventHandlerType { return h.typ }
func (h *v2OrderHandler) HandleEvent(_ context.Context, e eh.Event) error {
	if data, ok := e.Data().(*mocks.EventData); ok {
		h.record(data.Content)
	}
	return nil
}

func TestV2LateKeyIsServedWhileHeavyKeysHaveBacklog(t *testing.T) {
	db := newTestDB(t)
	o := v2NewOutbox(t, db, WithAdmissionLimit(6), WithQueueDepth(8), WithMaxGoroutines(4))
	gateA, releaseA := v2Gate(t)
	gateB, releaseB := v2Gate(t)
	heavyA := &v2CountingHandler{typ: "heavy-a", gate: gateA}
	heavyB := &v2CountingHandler{typ: "heavy-b", gate: gateB}
	late := &v2CountingHandler{typ: "late", entered: make(chan struct{}, 1)}
	for _, h := range []eh.EventHandler{heavyA, heavyB} {
		if err := o.AddHandler(context.Background(), eh.MatchEvents{mocks.EventType}, h); err != nil {
			t.Fatal(err)
		}
	}
	if err := o.AddHandler(context.Background(), eh.MatchEvents{mocks.EventOtherType}, late); err != nil {
		t.Fatal(err)
	}
	prepareWithoutFetcher(t, o) // manual passes only
	for i := range 20 {
		v2Publish(t, o, fmt.Sprintf("heavy-%d", i))
	}
	// Fill admission entirely with heavy deliveries (both keys blocked, nothing
	// finalizes). Bounded number of passes: each pass either fills or claims 0.
	for range 4 {
		if used, limit := o.AdmissionSnapshot(); used == limit {
			break
		}
		if _, err := o.fetchAndDispatch(context.Background(), false); err != nil {
			t.Fatal(err)
		}
	}
	if used, limit := o.AdmissionSnapshot(); used != limit {
		t.Fatalf("admission should be saturated: %d/%d", used, limit)
	}
	// Late key arrives now.
	event := eh.NewEvent(mocks.EventOtherType, &mocks.EventData{Content: "late"}, time.Now(), eh.ForAggregate(mocks.AggregateType, uuid.New(), 1))
	if err := o.HandleEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	// Free exactly one slot by letting one heavy-a delivery finish. The
	// guarantee is bounded rotation: the late key is served within one full
	// rotation of the ring (3 keys), not necessarily with the very first slot
	// (the cursor may have last visited an empty late key).
	gateA <- struct{}{}
	waitUntil(t, 5*time.Second, func() bool { used, limit := o.AdmissionSnapshot(); return used < limit })
	served := false
	for pass := 0; pass < len(o.claimKeys)+1 && !served; pass++ {
		if _, err := o.fetchAndDispatch(context.Background(), false); err != nil {
			t.Fatal(err)
		}
		select {
		case <-late.entered:
			served = true
		case <-time.After(200 * time.Millisecond):
			// Slot went to a heavy key this pass; free another and rotate.
			gateA <- struct{}{}
			waitUntil(t, 5*time.Second, func() bool { used, limit := o.AdmissionSnapshot(); return used < limit })
		}
	}
	if !served {
		t.Fatal("late key was not served within one rotation of freed slots")
	}
	releaseA()
	releaseB()
	waitUntil(t, 10*time.Second, func() bool {
		if _, err := o.fetchAndDispatch(context.Background(), false); err != nil {
			t.Fatal(err)
		}
		return countDeliveries(t, o) == 0
	})
	if heavyA.calls.Load() != 20 || heavyB.calls.Load() != 20 || late.calls.Load() != 1 {
		t.Fatalf("calls a=%d b=%d late=%d", heavyA.calls.Load(), heavyB.calls.Load(), late.calls.Load())
	}
}

func TestV2TimestampsAreCanonicalUTCText(t *testing.T) {
	db := newTestDB(t)
	o := v2NewOutbox(t, db, WithMaxRetries(3))
	h := &v2CountingHandler{typ: "h", err: errors.New("retry me")}
	if err := o.AddHandler(context.Background(), eh.MatchEvents{mocks.EventType}, h); err != nil {
		t.Fatal(err)
	}
	if err := o.StartChecked(); err != nil {
		t.Fatal(err)
	}
	warsaw, _ := time.LoadLocation("Europe/Warsaw")
	ctx := WithAvailableAt(context.Background(), time.Now().Add(time.Hour).In(warsaw))
	if err := o.HandleEvent(ctx, newTestEvent("delayed")); err != nil {
		t.Fatal(err)
	}
	for _, col := range []string{"created_at", "available_at"} {
		raw := storedText(t, db, fmt.Sprintf(`SELECT CAST(%s AS TEXT) FROM %s`, col, o.deliveriesTable))
		parsed, err := schema.ParseStored(raw)
		if err != nil || raw != schema.FormatStored(parsed) || !strings.HasSuffix(raw, "+00:00") {
			t.Fatalf("%s not canonical UTC: %q", col, raw)
		}
	}
	// Make it due, deliver (retryable failure) and check the retry timestamp.
	if _, err := db.Exec(fmt.Sprintf(`UPDATE %s SET available_at = ? WHERE 1=1`, o.deliveriesTable), schema.UTC(time.Now().Add(-time.Second))); err != nil {
		t.Fatal(err)
	}
	if _, err := o.processBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	rows := listDeliveries(t, o)
	if len(rows) != 1 || rows[0].RetryCount != 1 || rows[0].TakenAt.Valid {
		t.Fatalf("retry state: %+v", rows)
	}
	raw := storedText(t, db, fmt.Sprintf(`SELECT CAST(available_at AS TEXT) FROM %s`, o.deliveriesTable))
	if parsed, err := schema.ParseStored(raw); err != nil || raw != schema.FormatStored(parsed) {
		t.Fatalf("retry available_at not canonical: %q", raw)
	}
}

func TestV2ConcurrentStartCheckedRunsStartupOnce(t *testing.T) {
	db := newTestDB(t)
	o := v2NewOutbox(t, db)
	gate, release := v2Gate(t)
	h := &v2CountingHandler{typ: "h", gate: gate, entered: make(chan struct{}, 1)}
	if err := o.AddHandler(context.Background(), eh.MatchEvents{mocks.EventType}, h); err != nil {
		t.Fatal(err)
	}
	// A pre-claimed delivery: exactly one startup reset must happen, and it
	// must happen before the fetcher claims it (no second reset after claim).
	_, id := insertDeliveryDirect(t, o, deliverySeed{Event: newTestEvent("x"), HandlerType: "h", CreatedAt: time.Now().Add(-time.Minute), TakenAt: time.Now()})
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- o.StartChecked()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	<-h.entered
	rows := listDeliveries(t, o)
	if len(rows) != 1 || rows[0].ID != id || !rows[0].TakenAt.Valid {
		t.Fatalf("claimed delivery lost its claim to a late startup reset: %+v", rows)
	}
	release()
	waitUntil(t, 5*time.Second, func() bool { return countDeliveries(t, o) == 0 })
	if h.calls.Load() != 1 {
		t.Fatalf("handler ran %d times", h.calls.Load())
	}
}

func TestV2FatalDeliveryDeadLettersWithProvenanceAndSiblingContinues(t *testing.T) {
	db := newTestDB(t)
	o := v2NewOutbox(t, db)
	bad := &v2CountingHandler{typ: "bad", err: v2FatalError{errors.New("fatal")}}
	good := &v2CountingHandler{typ: "good"}
	for _, h := range []eh.EventHandler{bad, good} {
		if err := o.AddHandler(context.Background(), eh.MatchEvents{mocks.EventType}, h); err != nil {
			t.Fatal(err)
		}
	}
	if err := o.StartChecked(); err != nil {
		t.Fatal(err)
	}
	v2Publish(t, o, "x")
	waitUntil(t, 5*time.Second, func() bool { return countDeliveries(t, o) == 0 })
	if good.calls.Load() != 1 {
		t.Fatal("sibling did not run")
	}
	var outboxID, pubID, remaining string
	if err := db.QueryRow(`SELECT outbox_id, publication_id, remaining_handlers FROM dead_letters WHERE handler_type = 'bad'`).Scan(&outboxID, &pubID, &remaining); err != nil {
		t.Fatal(err)
	}
	if outboxID == "" || pubID == "" || remaining != "[]" {
		t.Fatalf("dead letter provenance: outbox_id=%q publication_id=%q remaining=%q", outboxID, pubID, remaining)
	}
	if countPublications(t, o) != 0 {
		t.Fatal("publication must be GC'd after the last delivery (dead letter keeps its own blob)")
	}
}
