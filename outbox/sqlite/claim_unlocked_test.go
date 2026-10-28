package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	eh "github.com/vercly/eventhorizon"
	"github.com/vercly/eventhorizon/mocks"
)

// Tests for loading, decoding and matching claimed payloads after the claim
// transaction commits (outside the SQLite write lock).

// claimProbeMatcher matches mocks.EventType. From the second call on (the
// first is the publish-time match) it runs onClaim and returns its verdict.
type claimProbeMatcher struct {
	calls   atomic.Int32
	onClaim func() bool
}

func (m *claimProbeMatcher) Match(event eh.Event) bool {
	if event == nil || event.EventType() != mocks.EventType {
		return false
	}
	if m.calls.Add(1) == 1 || m.onClaim == nil {
		return true
	}
	return m.onClaim()
}

func assertClaimReleased(t *testing.T, o *Outbox, key string, depth int) {
	t.Helper()
	if used, _ := o.AdmissionSnapshot(); used != 0 {
		t.Fatalf("admission leaked: used=%d", used)
	}
	if free := o.dispatch.freeCapacity(key); free != depth {
		t.Fatalf("queue %s free=%d, want %d (reservation leaked)", key, free, depth)
	}
}

func TestClaimMatchRunsOutsideWriteLock(t *testing.T) {
	db := newTestDB(t)
	o := v2NewOutbox(t, db)
	// A second handle that never waits for a lock: any write it attempts while
	// a claim transaction holds the lock fails with SQLITE_BUSY at once.
	var path string
	if err := db.QueryRow(`SELECT file FROM pragma_database_list WHERE name = 'main'`).Scan(&path); err != nil {
		t.Fatal(err)
	}
	probe, err := sql.Open("sqlite3", path+"?_journal=wal&_busy_timeout=0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = probe.Close() })
	if _, err := probe.Exec(`CREATE TABLE claim_probe (n INTEGER)`); err != nil {
		t.Fatal(err)
	}

	var probeErr error
	m := &claimProbeMatcher{onClaim: func() bool {
		_, probeErr = probe.Exec(`INSERT INTO claim_probe (n) VALUES (1)`)
		return true
	}}
	h := &v2CountingHandler{typ: "h"}
	if err := o.AddHandler(context.Background(), m, h); err != nil {
		t.Fatal(err)
	}
	prepareWithoutFetcher(t, o)
	v2Publish(t, o, "e")
	v2Drain(t, o)

	if m.calls.Load() < 2 {
		t.Fatalf("matcher was not called at claim (calls=%d)", m.calls.Load())
	}
	if probeErr != nil {
		t.Fatalf("write during claim-time match failed (write lock still held): %v", probeErr)
	}
	if h.calls.Load() != 1 || countDeliveries(t, o) != 0 {
		t.Fatalf("calls=%d deliveries=%d", h.calls.Load(), countDeliveries(t, o))
	}
}

func TestClaimMatchRejectFlagsUnresolvedAndReleases(t *testing.T) {
	db := newTestDB(t)
	o := v2NewOutbox(t, db, WithQueueDepth(4))
	m := &claimProbeMatcher{onClaim: func() bool { return false }}
	rejected := &v2CountingHandler{typ: "rejected"}
	other := &v2CountingHandler{typ: "other"}
	if err := o.AddHandler(context.Background(), m, rejected); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(context.Background(), eh.MatchEvents{mocks.EventType}, other); err != nil {
		t.Fatal(err)
	}
	prepareWithoutFetcher(t, o)
	v2Publish(t, o, "e")
	v2Drain(t, o)

	if rejected.calls.Load() != 0 || other.calls.Load() != 1 {
		t.Fatalf("rejected=%d other=%d", rejected.calls.Load(), other.calls.Load())
	}
	rows := listDeliveries(t, o)
	if len(rows) != 1 || rows[0].HandlerType != "rejected" || !rows[0].UnresolvedAt.Valid || rows[0].TakenAt.Valid || rows[0].DispatchKey != "" {
		t.Fatalf("deliveries = %+v, want one unresolved unclaimed row for rejected", rows)
	}
	assertClaimReleased(t, o, "rejected", 4)
	errs := drainOutboxErrors(o)
	if len(errs) != 1 || !errors.Is(errs[0], ErrUnresolvedHandler) {
		t.Fatalf("errors = %v, want one ErrUnresolvedHandler", errs)
	}
}

func TestClaimDecodeFailureFlagsUnresolvedAndReleases(t *testing.T) {
	db := newTestDB(t)
	o := v2NewOutbox(t, db, WithQueueDepth(4))
	h := &v2CountingHandler{typ: "h"}
	if err := o.AddHandler(context.Background(), eh.MatchEvents{mocks.EventType}, h); err != nil {
		t.Fatal(err)
	}
	prepareWithoutFetcher(t, o)
	v2Publish(t, o, "bad")
	if _, err := db.Exec(fmt.Sprintf(`UPDATE %s SET event_blob = '{not json'`, o.publicationsTable)); err != nil {
		t.Fatal(err)
	}
	v2Publish(t, o, "good")
	v2Drain(t, o)

	if h.calls.Load() != 1 {
		t.Fatalf("calls=%d, want 1 (only the decodable event)", h.calls.Load())
	}
	rows := listDeliveries(t, o)
	if len(rows) != 1 || !rows[0].UnresolvedAt.Valid || rows[0].TakenAt.Valid {
		t.Fatalf("deliveries = %+v, want one unresolved unclaimed row", rows)
	}
	assertClaimReleased(t, o, "h", 4)
	errs := drainOutboxErrors(o)
	if len(errs) != 1 || !errors.Is(errs[0], ErrUnresolvedHandler) {
		t.Fatalf("errors = %v, want one ErrUnresolvedHandler", errs)
	}
}

func TestClaimReleasesRawBlobAndDeadLetterReloadsIt(t *testing.T) {
	db := newTestDB(t)
	o := v2NewOutbox(t, db)
	h := &v2CountingHandler{typ: "h", err: v2FatalError{errors.New("fatal")}}
	if err := o.AddHandler(context.Background(), eh.MatchEvents{mocks.EventType}, h); err != nil {
		t.Fatal(err)
	}
	prepareWithoutFetcher(t, o)
	v2Publish(t, o, "e")
	want := storedText(t, db, fmt.Sprintf(`SELECT event_blob FROM %s`, o.publicationsTable))

	planned, _, err := o.claimPass(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(planned) != 1 {
		t.Fatalf("planned %d deliveries, want 1", len(planned))
	}
	if planned[0].doc.Event == nil || planned[0].doc.EventBlob != "" {
		t.Fatalf("claimed delivery: event=%v raw blob bytes=%d, want decoded event and no raw blob",
			planned[0].doc.Event, len(planned[0].doc.EventBlob))
	}
	o.dispatch.enqueue(planned[0])
	planned[0].wait()

	if got := storedText(t, db, fmt.Sprintf(`SELECT blob FROM %s`, o.deadLetterTable)); got != want {
		t.Fatalf("dead letter blob = %q, want publication blob %q", got, want)
	}
}
