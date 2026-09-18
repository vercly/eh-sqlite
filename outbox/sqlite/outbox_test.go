package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	dl "github.com/vercly/eh-sqlite/deadletter"
	"github.com/vercly/eh-sqlite/schema"

	txctx "github.com/vercly/eh-sqlite/context/sqlite"
	eh "github.com/vercly/eventhorizon"
	"github.com/vercly/eventhorizon/mocks"
	"github.com/vercly/eventhorizon/outbox"
	"github.com/vercly/eventhorizon/uuid"
)

func init() {
	eh.RegisterEventData(mocks.EventOtherType, func() eh.EventData { return &mocks.EventData{} })
}

func TestOutboxAddHandler(t *testing.T) {
	db := newTestDB(t)
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}

	outbox.TestAddHandler(t, o, context.Background())
}

// TestOutboxIntegrationStaticRegistration covers the former AcceptanceTest
// surface under the static-registration contract (all handlers before Start):
// context marshal/unmarshal into handlers, multi-handler fan-out, and async
// handler errors on Errors().
func TestOutboxIntegrationStaticRegistration(t *testing.T) {
	restoreSweepInterval := setPeriodicSweepInterval(t, 2*time.Second)
	defer restoreSweepInterval()
	restoreSweepAge := setPeriodicSweepAge(t, 2*time.Second)
	defer restoreSweepAge()

	db := newTestDB(t)
	ctx := mocks.WithContextOne(context.Background(), "testval")

	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := mocks.NewEventHandler("static_handler")
	anotherHandler := mocks.NewEventHandler("static_another_handler")
	otherHandler := mocks.NewEventHandler("static_other_handler")
	errorHandler := mocks.NewEventHandler("static_error_handler")
	errorHandler.Err = errors.New("handler error")

	// All registrations before Start.
	for _, h := range []eh.EventHandler{handler, anotherHandler} {
		if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, h); err != nil {
			t.Fatal(err)
		}
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventOtherType}, otherHandler); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchAll{}, errorHandler); err != nil {
		t.Fatal(err)
	}

	if err := o.StartChecked(); err != nil {
		t.Fatal(err)
	}

	id := uuid.New()
	timestamp := time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
	event2 := eh.NewEvent(mocks.EventType, &mocks.EventData{Content: "event2"}, timestamp,
		eh.ForAggregate(mocks.AggregateType, id, 2),
		eh.WithMetadata(map[string]any{"meta": "data", "num": 42.0}),
	)
	if err := o.HandleEvent(ctx, event2); err != nil {
		t.Fatal(err)
	}

	if !handler.Wait(3 * time.Second) {
		t.Fatal("handler did not receive event")
	}
	handler.Lock()
	if len(handler.Events) != 1 {
		t.Fatalf("handler events = %d, want 1", len(handler.Events))
	}
	if val, ok := mocks.ContextOne(handler.Context); !ok || val != "testval" {
		t.Fatalf("handler context = %v, want testval", handler.Context)
	}
	handler.Unlock()

	if !anotherHandler.Wait(3 * time.Second) {
		t.Fatal("another handler did not receive event")
	}
	anotherHandler.Lock()
	if val, ok := mocks.ContextOne(anotherHandler.Context); !ok || val != "testval" {
		t.Fatalf("another handler context = %v, want testval", anotherHandler.Context)
	}
	anotherHandler.Unlock()

	// MatchAll error handler also sees the event; async error must appear.
	// Drain until we see the handler error (other Errors may exist).
	deadline := time.After(3 * time.Second)
	found := false
	for !found {
		select {
		case err := <-o.Errors():
			if err != nil && strings.Contains(err.Error(), "handler error") {
				found = true
			}
		case <-deadline:
			t.Fatal("expected async handler error on Errors()")
		}
	}

	// Event without data to the other-type handler.
	eventOther := eh.NewEvent(mocks.EventOtherType, nil, timestamp,
		eh.ForAggregate(mocks.AggregateType, uuid.New(), 1))
	if err := o.HandleEvent(ctx, eventOther); err != nil {
		t.Fatal(err)
	}
	if !otherHandler.Wait(3 * time.Second) {
		t.Fatal("other handler did not receive event")
	}
}

// TestWithTableNameIntegration asserts the prefix contract: the v2 tables are
// <prefix>_publications and <prefix>_deliveries.
func TestWithTableNameIntegration(t *testing.T) {
	db := newTestDB(t)

	o, err := NewOutbox(db, WithTableName("foo_outbox"))
	if err != nil {
		t.Fatal(err)
	}

	defer o.Close()

	if o == nil {
		t.Fatal("there should be a store")
	}

	if o.outboxTable != "foo_outbox" {
		t.Fatal("table name should use custom table name")
	}
	if o.publicationsTable != "foo_outbox_publications" {
		t.Fatalf("publications table = %s, want foo_outbox_publications", o.publicationsTable)
	}
	if o.deliveriesTable != "foo_outbox_deliveries" {
		t.Fatalf("deliveries table = %s, want foo_outbox_deliveries", o.deliveriesTable)
	}
	for _, table := range []string{"foo_outbox_publications", "foo_outbox_deliveries"} {
		var name string
		if err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&name); err != nil {
			t.Fatalf("table %s was not created: %v", table, err)
		}
	}
	// The default-prefix tables must not be created by a prefixed outbox.
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'outbox_deliveries'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("default prefix tables = %d, want 0", count)
	}
}

func TestOutboxCloseDoesNotCloseSharedDB(t *testing.T) {
	db := newTestDB(t)

	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}

	if err := o.Close(); err != nil {
		t.Fatal(err)
	}

	if err := db.Ping(); err != nil {
		t.Fatalf("shared db should remain open after outbox close: %v", err)
	}
}

func TestOutboxHandleEventRequiresStart(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, mocks.NewEventHandler("start_guard_handler")); err != nil {
		t.Fatal(err)
	}

	err = o.HandleEvent(ctx, newTestEvent("before-start"))
	if !errors.Is(err, ErrOutboxNotStarted) {
		t.Fatalf("HandleEvent before Start error = %v, want ErrOutboxNotStarted", err)
	}
	if got := outboxRowCount(t, db); got != 0 {
		t.Fatalf("delivery rows before Start = %d, want 0", got)
	}
	if got := countPublications(t, o); got != 0 {
		t.Fatalf("publication rows before Start = %d, want 0", got)
	}
	if got := deadLetterRowCount(t, db); got != 0 {
		t.Fatalf("dead letter rows before Start = %d, want 0", got)
	}

	if err := o.StartChecked(); err != nil {
		t.Fatal(err)
	}
	if err := o.StartChecked(); err != nil {
		t.Fatal(err)
	}

	if err := o.HandleEvent(WithDelay(ctx, time.Hour), newTestEvent("after-start")); err != nil {
		t.Fatal(err)
	}
	if got := outboxRowCount(t, db); got != 1 {
		t.Fatalf("delivery rows after Start = %d, want 1", got)
	}
	if got := countPublications(t, o); got != 1 {
		t.Fatalf("publication rows after Start = %d, want 1", got)
	}

	// Registration is closed after Start.
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, mocks.NewEventHandler("late_handler")); !errors.Is(err, ErrOutboxAlreadyStarted) {
		t.Fatalf("AddHandler after Start error = %v, want ErrOutboxAlreadyStarted", err)
	}
}

func TestOutboxNotifySignalsScheduleWhenWatchChannelFull(t *testing.T) {
	db := newTestDB(t)
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	for range cap(o.watchCh) {
		o.watchCh <- struct{}{}
	}

	o.notify()

	select {
	case <-o.scheduleCh:
	default:
		t.Fatal("schedule signal was not queued when watchCh was full")
	}
}

func TestOutboxProcessesStaleTakenAtAfterRestart(t *testing.T) {
	restoreSweepAge := setPeriodicSweepAge(t, 25*time.Millisecond)
	defer restoreSweepAge()

	db := newTestDB(t)
	ctx := context.Background()

	firstOutbox, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := firstOutbox.Close(); err != nil {
		t.Fatal(err)
	}

	restartedOutbox, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer restartedOutbox.Close()

	restartedHandler := mocks.NewEventHandler("stale_handler")
	if err := restartedOutbox.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, restartedHandler); err != nil {
		t.Fatal(err)
	}

	// Delivery left claimed by the previous process, older than the sweep age.
	_, deliveryID := insertDeliveryDirect(t, restartedOutbox, deliverySeed{
		Event:       newTestEvent("stale"),
		HandlerType: restartedHandler.Type,
		CreatedAt:   time.Now(),
	})
	reconcileForTest(t, restartedOutbox)
	otReadyForClaim(t, restartedOutbox)
	setTakenAt(t, restartedOutbox, deliveryID, time.Now().Add(-2*PeriodicSweepAge))

	processed, err := restartedOutbox.processBatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("processed count = %d, want 1", processed)
	}
	if !restartedHandler.Wait(time.Second) {
		t.Fatal("stale outbox delivery was not dispatched")
	}
	if got := countDeliveries(t, restartedOutbox); got != 0 {
		t.Fatalf("delivery rows after processing = %d, want 0", got)
	}
	if got := countPublications(t, restartedOutbox); got != 0 {
		t.Fatalf("publication rows after processing = %d, want 0", got)
	}
}

func TestOutboxDoesNotProcessFreshTakenAtBeforeSweepAge(t *testing.T) {
	restoreSweepAge := setPeriodicSweepAge(t, time.Hour)
	defer restoreSweepAge()

	db := newTestDB(t)
	ctx := context.Background()

	firstOutbox, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := firstOutbox.Close(); err != nil {
		t.Fatal(err)
	}

	restartedOutbox, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer restartedOutbox.Close()

	restartedHandler := mocks.NewEventHandler("fresh_handler")
	if err := restartedOutbox.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, restartedHandler); err != nil {
		t.Fatal(err)
	}

	_, deliveryID := insertDeliveryDirect(t, restartedOutbox, deliverySeed{
		Event:       newTestEvent("fresh"),
		HandlerType: restartedHandler.Type,
		CreatedAt:   time.Now(),
	})
	reconcileForTest(t, restartedOutbox)
	otReadyForClaim(t, restartedOutbox)
	setTakenAt(t, restartedOutbox, deliveryID, time.Now())

	processed, err := restartedOutbox.processBatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 0 {
		t.Fatalf("processed count = %d, want 0", processed)
	}
	if restartedHandler.Wait(50 * time.Millisecond) {
		t.Fatal("freshly taken delivery should not be dispatched before PeriodicSweepAge")
	}
	if got := countDeliveries(t, restartedOutbox); got != 1 {
		t.Fatalf("delivery rows after skipped processing = %d, want 1", got)
	}
}

func TestOutboxConcurrentPublishDoesNotLoseRows(t *testing.T) {
	db := newTestDB(t)
	db.SetMaxOpenConns(8)
	ctx := context.Background()

	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, mocks.NewEventHandler("concurrent_handler")); err != nil {
		t.Fatal(err)
	}
	o.Start()

	const events = 100
	var wg sync.WaitGroup
	errCh := make(chan error, events)
	for i := range events {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := o.HandleEvent(WithDelay(ctx, time.Hour), newTestEvent(fmt.Sprintf("event-%d", i))); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		t.FailNow()
	}

	if got := outboxRowCount(t, db); got != events {
		t.Fatalf("delivery rows after concurrent publish = %d, want %d", got, events)
	}
	if got := countPublications(t, o); got != events {
		t.Fatalf("publication rows after concurrent publish = %d, want %d", got, events)
	}
}

// TestOutboxV1MigrationSplitsRowsIntoPublicationsAndDeliveries replaces the v1
// available_at backfill test: StartChecked migrates the v1 `outbox` table into
// publications plus one delivery per handler (or a rematch sentinel for
// handlers='[]'), preserves the v1 (available_at, created_at, id) order in the
// delivery sequence, keeps the v1 id as provenance, backfills a NULL
// available_at from created_at, archives the v1 table and is idempotent.
func TestOutboxV1MigrationSplitsRowsIntoPublicationsAndDeliveries(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	createV1Table(t, db, "outbox")

	// Everything is seeded in the future so the started processor cannot claim
	// (and delete) the migrated rows while the assertions run.
	base := time.Now().Add(time.Hour).UTC().Truncate(time.Millisecond)
	nullAvailableID := insertV1Row(t, db, "outbox", v1Seed{
		Event:     newTestEvent("v1-null-available"),
		Handlers:  []string{"mig_handler"},
		CreatedAt: base,
	})
	// NULL available_at must be backfilled from created_at by the migration.
	if _, err := db.Exec(`UPDATE outbox SET available_at = NULL WHERE id = ?`, nullAvailableID); err != nil {
		t.Fatal(err)
	}
	sentinelID := insertV1Row(t, db, "outbox", v1Seed{
		Event:       newTestEvent("v1-rematch"),
		Handlers:    []string{},
		CreatedAt:   base,
		AvailableAt: base.Add(time.Hour),
	})
	twoHandlerID := insertV1Row(t, db, "outbox", v1Seed{
		Event:       newTestEvent("v1-two-handlers"),
		Handlers:    []string{"mig_handler", "mig_second"},
		CreatedAt:   base,
		AvailableAt: base.Add(2 * time.Hour),
		RetryCount:  3,
	})

	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	for _, handlerType := range []string{"mig_handler", "mig_second"} {
		if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, mocks.NewEventHandler(handlerType)); err != nil {
			t.Fatal(err)
		}
	}
	if err := o.StartChecked(); err != nil {
		t.Fatal(err)
	}

	if got := countPublications(t, o); got != 3 {
		t.Fatalf("publications = %d, want 3 (one per v1 row)", got)
	}
	deliveries := listDeliveries(t, o)
	if len(deliveries) != 4 {
		t.Fatalf("deliveries = %d, want 4 (1 + sentinel + 2)", len(deliveries))
	}

	// v1 order (available_at, created_at, id) is preserved by the sequence.
	wantHandlers := []string{"mig_handler", "", "mig_handler", "mig_second"}
	wantLegacy := []string{nullAvailableID, sentinelID, twoHandlerID, twoHandlerID}
	for i, d := range deliveries {
		if d.HandlerType != wantHandlers[i] {
			t.Fatalf("delivery[%d] handler_type = %q, want %q", i, d.HandlerType, wantHandlers[i])
		}
		if d.LegacyOutboxID != wantLegacy[i] {
			t.Fatalf("delivery[%d] legacy_outbox_id = %s, want %s", i, d.LegacyOutboxID, wantLegacy[i])
		}
		if d.TakenAt.Valid {
			t.Fatalf("delivery[%d] taken_at must be NULL after migration", i)
		}
		if i > 0 && d.Seq <= deliveries[i-1].Seq {
			t.Fatalf("delivery[%d] seq %d is not increasing", i, d.Seq)
		}
	}
	// Exactly one rematch sentinel, and it has no dispatch key.
	if deliveries[1].DispatchKey != "" {
		t.Fatalf("sentinel dispatch_key = %q, want empty", deliveries[1].DispatchKey)
	}
	// retry_count is carried over per delivery.
	for _, i := range []int{2, 3} {
		if deliveries[i].RetryCount != 3 {
			t.Fatalf("delivery[%d] retry_count = %d, want 3", i, deliveries[i].RetryCount)
		}
	}
	// NULL available_at was backfilled from created_at.
	if !deliveries[0].AvailableAt.Equal(base) {
		t.Fatalf("backfilled available_at = %s, want created_at %s", deliveries[0].AvailableAt, base)
	}
	// Timestamps are stored as canonical UTC text.
	raw := storedText(t, db, `SELECT CAST(available_at AS TEXT) FROM outbox_deliveries WHERE id = ?`, deliveries[0].ID)
	if !strings.HasSuffix(raw, "+00:00") {
		t.Fatalf("stored available_at = %q, want canonical UTC text", raw)
	}

	// The v1 table is archived, never read again.
	if otTableExists(t, db, "outbox") {
		t.Fatal("v1 table outbox must be renamed after migration")
	}
	if !otTableExists(t, db, "outbox_v1_migrated") {
		t.Fatal("v1 table must be archived as outbox_v1_migrated")
	}
	applied, err := schema.IsApplied(ctx, db, "outbox", "v2_deliveries")
	if err != nil {
		t.Fatal(err)
	}
	if !applied {
		t.Fatal("migration marker outbox/v2_deliveries must be recorded")
	}

	// Re-running the migration (and StartChecked) is a no-op.
	report, err := Migrate(ctx, db, "outbox")
	if err != nil {
		t.Fatal(err)
	}
	if report.Applied {
		t.Fatalf("second Migrate applied = true, want false")
	}
	if err := o.StartChecked(); err != nil {
		t.Fatal(err)
	}
	if got := countPublications(t, o); got != 3 {
		t.Fatalf("publications after re-run = %d, want 3", got)
	}
	if got := countDeliveries(t, o); got != 4 {
		t.Fatalf("deliveries after re-run = %d, want 4", got)
	}
}

func TestOutboxDelayedEventWaitsForAvailableAtTimer(t *testing.T) {
	restoreSweepInterval := setPeriodicSweepInterval(t, time.Hour)
	defer restoreSweepInterval()

	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := mocks.NewEventHandler("delayed_handler")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	o.Start()

	if err := o.HandleEvent(WithDelay(ctx, 300*time.Millisecond), newTestEvent("delayed")); err != nil {
		t.Fatal(err)
	}
	if handler.Wait(100 * time.Millisecond) {
		t.Fatal("delayed event was dispatched before available_at")
	}
	if !handler.Wait(2 * time.Second) {
		t.Fatal("delayed event was not dispatched by the available_at timer")
	}
}

func TestOutboxMetadataAvailableAtOverridesContextDelay(t *testing.T) {
	restoreSweepInterval := setPeriodicSweepInterval(t, time.Hour)
	defer restoreSweepInterval()

	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := mocks.NewEventHandler("metadata_delay_handler")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	o.Start()

	availableAt := time.Now().Add(300 * time.Millisecond)
	event := eh.NewEvent(mocks.EventType, &mocks.EventData{Content: "metadata-delay"}, time.Now(),
		eh.ForAggregate(mocks.AggregateType, uuid.New(), 1),
		eh.WithMetadata(map[string]any{metadataAvailableAtKey: availableAt.Format(time.RFC3339Nano)}),
	)
	if err := o.HandleEvent(WithDelay(ctx, time.Hour), event); err != nil {
		t.Fatal(err)
	}
	if handler.Wait(100 * time.Millisecond) {
		t.Fatal("metadata-delayed event was dispatched before available_at")
	}
	if !handler.Wait(2 * time.Second) {
		t.Fatal("metadata available_at did not override context delay")
	}
}

func TestOutboxNoMatchWritesDeadLetter(t *testing.T) {
	db := newTestDB(t)
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	o.Start()

	if err := o.HandleEvent(context.Background(), newTestEvent("no-match")); err != nil {
		t.Fatal(err)
	}

	if got := outboxRowCount(t, db); got != 0 {
		t.Fatalf("delivery rows = %d, want 0", got)
	}
	if got := countPublications(t, o); got != 0 {
		t.Fatalf("publication rows = %d, want 0 (nothing stored for a no-match)", got)
	}
	if got := deadLetterRowCount(t, db); got != 1 {
		t.Fatalf("dead letter rows = %d, want 1", got)
	}

	var handlerType, source, errMsg string
	var outboxID, publicationID sql.NullString
	if err := db.QueryRow(`SELECT handler_type, source, error, outbox_id, publication_id FROM dead_letters LIMIT 1`).
		Scan(&handlerType, &source, &errMsg, &outboxID, &publicationID); err != nil {
		t.Fatal(err)
	}
	if handlerType != "no_match" || source != "outbox" || errMsg != "no matching handlers" {
		t.Fatalf("dead letter = (%s, %s, %s), want no_match/outbox/no matching handlers", handlerType, source, errMsg)
	}
	if outboxID.Valid || publicationID.Valid {
		t.Fatalf("no_match dead letter must keep outbox_id/publication_id NULL, got %v/%v", outboxID, publicationID)
	}
}

// TestOutboxEmptyHandlersRematch expands rematch sentinels (handler_type NULL)
// against the current registration. Sentinels that still match nothing stay
// unclaimed and are never deleted (they are flagged unresolved instead).
func TestOutboxEmptyHandlersRematch(t *testing.T) {
	db := newTestDB(t)
	_ = db
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := mocks.NewEventHandler("rematch_handler")
	if err := o.AddHandler(context.Background(), eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	now := time.Now().Add(-time.Minute)
	matchedPub, matchedSentinel := insertDeliveryDirect(t, o, deliverySeed{
		Event:     newTestEvent("rematch-hit"),
		CreatedAt: now,
	})
	// EventOtherType has no matching handler in this outbox.
	other := eh.NewEvent(mocks.EventOtherType, &mocks.EventData{Content: "nope"}, now,
		eh.ForAggregate(mocks.AggregateType, uuid.New(), 1))
	_, unmatchedSentinel := insertDeliveryDirect(t, o, deliverySeed{
		Event:     other,
		CreatedAt: now,
	})

	processAllBatches(t, o, context.Background())
	if !handler.Wait(3 * time.Second) {
		t.Fatal("rematch handler did not receive event")
	}

	// Matched sentinel was replaced by a real delivery, which completed; its
	// publication is garbage collected with the last delivery.
	if otDeliveryExists(t, o, matchedSentinel) {
		t.Fatal("matched rematch sentinel still present")
	}
	if otPublicationExists(t, o, matchedPub) {
		t.Fatal("publication of the completed rematch delivery was not garbage collected")
	}

	// Unmatched sentinel stays unclaimed and visible (never deleted).
	remaining := listDeliveries(t, o)
	if len(remaining) != 1 || remaining[0].ID != unmatchedSentinel {
		t.Fatalf("remaining deliveries = %+v, want only the unmatched sentinel", remaining)
	}
	if remaining[0].HandlerType != "" {
		t.Fatalf("unmatched sentinel handler_type = %q, want NULL", remaining[0].HandlerType)
	}
	if remaining[0].TakenAt.Valid {
		t.Fatal("unmatched sentinel must stay unclaimed")
	}
	if !remaining[0].UnresolvedAt.Valid {
		t.Fatal("unmatched sentinel must be flagged unresolved (visible, not silent)")
	}
}

// TestOutboxRematchPartialSuccessRetryable keeps the failed handler's delivery
// after a sentinel expansion; the successful sibling is deleted independently.
func TestOutboxRematchPartialSuccessRetryable(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithMaxRetries(2), WithRetryBackoff("FIXED:2:200ms"))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	successHandler := mocks.NewEventHandler("rematch_ok_handler")
	retryHandler := mocks.NewEventHandler("rematch_retry_handler")
	retryHandler.Err = errors.New("temporary rematch failure")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, successHandler); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, retryHandler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	insertDeliveryDirect(t, o, deliverySeed{Event: newTestEvent("rematch-partial-retry"), CreatedAt: createdAt})
	otReadyForClaim(t, o)

	// One sentinel expanded (+1) and its two deliveries claimed (+2).
	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 3 {
		t.Fatalf("processed = %d, want 3 (1 sentinel expansion + 2 deliveries)", processed)
	}

	if !successHandler.Wait(2 * time.Second) {
		t.Fatal("success handler did not run")
	}
	// Only the retryable delivery survives (no silent drop of retryable work).
	deliveries := listDeliveries(t, o)
	if len(deliveries) != 1 {
		t.Fatalf("deliveries = %d, want 1 (retryable work kept)", len(deliveries))
	}
	d := deliveries[0]
	if d.HandlerType != retryHandler.Type {
		t.Fatalf("remaining handler_type = %s, want %s", d.HandlerType, retryHandler.Type)
	}
	if d.RetryCount != 1 {
		t.Fatalf("retry_count = %d, want 1", d.RetryCount)
	}
	if !d.AvailableAt.After(time.Now().Add(100 * time.Millisecond)) {
		t.Fatalf("available_at = %s, want future retry backoff", d.AvailableAt)
	}
	if d.TakenAt.Valid {
		t.Fatal("taken_at should be cleared for retry")
	}
	if got := countPublications(t, o); got != 1 {
		t.Fatalf("publications = %d, want 1 (kept while a delivery remains)", got)
	}
	if got := deadLetterRowCount(t, db); got != 0 {
		t.Fatalf("dead letter rows = %d, want 0 for retryable failure", got)
	}
}

// TestOutboxRematchPartialSuccessFatal dead-letters only the fatal delivery
// after a sentinel expansion and removes the publication once the last
// delivery is gone.
func TestOutboxRematchPartialSuccessFatal(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	successHandler := mocks.NewEventHandler("rematch_fatal_ok_handler")
	fatalHandler := mocks.NewEventHandler("rematch_fatal_bad_handler")
	fatalHandler.Err = fatalOutboxError{err: errors.New("fatal rematch failure")}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, successHandler); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, fatalHandler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	publicationID, _ := insertDeliveryDirect(t, o, deliverySeed{Event: newTestEvent("rematch-partial-fatal"), CreatedAt: createdAt})
	otReadyForClaim(t, o)

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 3 {
		t.Fatalf("processed = %d, want 3", processed)
	}

	if !successHandler.Wait(2 * time.Second) {
		t.Fatal("success handler did not run")
	}
	if got := countDeliveries(t, o); got != 0 {
		t.Fatalf("deliveries = %d, want 0 after success+terminal", got)
	}
	if got := countPublications(t, o); got != 0 {
		t.Fatalf("publications = %d, want 0 after the last delivery finalized", got)
	}
	assertDeadLetterHandlers(t, db, []string{fatalHandler.Type})
	record := deadLetterRecordByHandler(t, db, fatalHandler.Type)
	if record.RemainingHandlers != "[]" {
		t.Fatalf("remaining_handlers = %s, want [] (no sibling coupling in v2)", record.RemainingHandlers)
	}
	if record.PublicationID != publicationID {
		t.Fatalf("publication_id = %s, want %s", record.PublicationID, publicationID)
	}
	if record.OutboxID == "" {
		t.Fatal("outbox_id must carry the delivery id")
	}
}

// TestOutboxRematchFatalLeavesRetryable mirrors mixed terminal+retryable
// finalize after a sentinel expansion: siblings are independent.
func TestOutboxRematchFatalLeavesRetryable(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithRetryBackoff("FIXED:2:200ms"))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	fatalHandler := mocks.NewEventHandler("rematch_mixed_fatal")
	fatalHandler.Err = fatalOutboxError{err: errors.New("fatal rematch")}
	retryableHandler := mocks.NewEventHandler("rematch_mixed_retry")
	retryableHandler.Err = errors.New("temporary rematch")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, fatalHandler); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, retryableHandler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	insertDeliveryDirect(t, o, deliverySeed{Event: newTestEvent("rematch-fatal-retry"), CreatedAt: createdAt})
	otReadyForClaim(t, o)

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 3 {
		t.Fatalf("processed = %d, want 3", processed)
	}

	assertDeadLetterHandlers(t, db, []string{fatalHandler.Type})
	otAssertDeliveryHandlers(t, o, []string{retryableHandler.Type})
	d := listDeliveries(t, o)[0]
	if d.RetryCount != 1 {
		t.Fatalf("retry_count = %d, want 1", d.RetryCount)
	}
	if d.TakenAt.Valid {
		t.Fatal("taken_at should be cleared for retry")
	}
	if !d.AvailableAt.After(time.Now().Add(100 * time.Millisecond)) {
		t.Fatalf("available_at = %s, want future retry", d.AvailableAt)
	}
	record := deadLetterRecordByHandler(t, db, fatalHandler.Type)
	if record.RemainingHandlers != "[]" {
		t.Fatalf("remaining_handlers = %s, want [] in v2", record.RemainingHandlers)
	}
}

func TestOutboxNoMatchDeadLetterRollsBackWithTransaction(t *testing.T) {
	db := newTestDB(t)
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	o.Start()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := o.HandleEvent(txctx.NewContextWithTx(context.Background(), tx), newTestEvent("rollback-no-match")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if got := deadLetterRowCount(t, db); got != 0 {
		t.Fatalf("dead letter rows after rollback = %d, want 0", got)
	}
	if got := outboxRowCount(t, db); got != 0 {
		t.Fatalf("delivery rows after rollback = %d, want 0", got)
	}
}

func TestOutboxRetryableErrorSchedulesBackoff(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	o, err := NewOutbox(db, WithMaxRetries(2), WithRetryBackoff("FIXED:2:200ms"))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	handler := mocks.NewEventHandler("retry_handler")
	handler.Err = errors.New("temporary failure")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}
	// Allow HandleEvent without starting the processor (unit path uses processBatch).
	o.processorRunning.Store(true)

	if err := o.HandleEvent(ctx, newTestEvent("retry")); err != nil {
		t.Fatal(err)
	}
	otReadyForClaim(t, o)
	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed count = %d, want 1", processed)
	}

	deliveries := listDeliveries(t, o)
	if len(deliveries) != 1 {
		t.Fatalf("deliveries = %d, want 1", len(deliveries))
	}
	d := deliveries[0]
	if d.RetryCount != 1 {
		t.Fatalf("retry_count = %d, want 1", d.RetryCount)
	}
	if !d.AvailableAt.After(time.Now().Add(100 * time.Millisecond)) {
		t.Fatalf("available_at = %s, want retry backoff in the future", d.AvailableAt)
	}
	if d.TakenAt.Valid {
		t.Fatalf("taken_at valid = true, want retry row released")
	}
	if got := countPublications(t, o); got != 1 {
		t.Fatalf("publications = %d, want 1", got)
	}
	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 0 {
		t.Fatalf("immediate processed count = %d, want 0", processed)
	}
}

func TestOutboxTerminalFailureWritesDeadLetter(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	o, err := NewOutbox(db, WithRetryBackoff("FIXED:1:1ms"), WithMaxRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	handler := mocks.NewEventHandler("terminal_handler")
	handler.Err = errors.New("permanent failure")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}
	// Allow HandleEvent without starting the processor (unit path uses processBatch).
	o.processorRunning.Store(true)

	if err := o.HandleEvent(ctx, newTestEvent("terminal")); err != nil {
		t.Fatal(err)
	}
	otReadyForClaim(t, o)
	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed count = %d, want 1", processed)
	}

	if got := outboxRowCount(t, db); got != 0 {
		t.Fatalf("delivery rows = %d, want 0", got)
	}
	if got := countPublications(t, o); got != 0 {
		t.Fatalf("publication rows = %d, want 0", got)
	}
	if got := deadLetterRowCount(t, db); got != 1 {
		t.Fatalf("dead letter rows = %d, want 1", got)
	}
	var source, handlerType string
	if err := db.QueryRow(`SELECT source, handler_type FROM dead_letters LIMIT 1`).Scan(&source, &handlerType); err != nil {
		t.Fatal(err)
	}
	if source != "outbox" || handlerType != "terminal_handler" {
		t.Fatalf("dead letter = (%s, %s), want outbox/terminal_handler", source, handlerType)
	}
}

func TestOutboxFatalHandlerDeadLetterDoesNotDropSuccessfulHandler(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	successHandler := mocks.NewEventHandler("fatal_success_handler")
	fatalHandler := mocks.NewEventHandler("fatal_failed_handler")
	fatalHandler.Err = fatalOutboxError{err: errors.New("fatal handler failure")}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, successHandler); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, fatalHandler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	ids := otSeedDeliveries(t, o, newTestEvent("fatal-success"), []string{successHandler.Type, fatalHandler.Type}, createdAt, createdAt, sql.NullTime{})

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 2 {
		t.Fatalf("processed = %d, want 2 (one delivery per handler)", processed)
	}

	if got := countDeliveries(t, o); got != 0 {
		t.Fatalf("deliveries = %d, want 0", got)
	}
	if got := countPublications(t, o); got != 0 {
		t.Fatalf("publications = %d, want 0", got)
	}
	if !successHandler.Wait(2 * time.Second) {
		t.Fatal("successful sibling must still run")
	}
	assertDeadLetterHandlers(t, db, []string{fatalHandler.Type})
	record := deadLetterRecordByHandler(t, db, fatalHandler.Type)
	if record.OutboxID != ids[fatalHandler.Type] {
		t.Fatalf("outbox_id = %s, want fatal delivery id %s", record.OutboxID, ids[fatalHandler.Type])
	}
	if record.RemainingHandlers != "[]" {
		t.Fatalf("remaining_handlers = %s, want []", record.RemainingHandlers)
	}
}

func TestOutboxFatalHandlerDeadLetterLeavesRetryableHandler(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithRetryBackoff("FIXED:2:200ms"))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	fatalHandler := mocks.NewEventHandler("mixed_fatal_handler")
	fatalHandler.Err = fatalOutboxError{err: errors.New("fatal handler failure")}
	retryableHandler := mocks.NewEventHandler("mixed_retry_handler")
	retryableHandler.Err = errors.New("temporary handler failure")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, fatalHandler); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, retryableHandler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	ids := otSeedDeliveries(t, o, newTestEvent("fatal-retry"), []string{fatalHandler.Type, retryableHandler.Type}, createdAt, createdAt, sql.NullTime{})

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 2 {
		t.Fatalf("processed = %d, want 2", processed)
	}

	assertDeadLetterHandlers(t, db, []string{fatalHandler.Type})
	otAssertDeliveryHandlers(t, o, []string{retryableHandler.Type})
	d := listDeliveries(t, o)[0]
	if d.ID != ids[retryableHandler.Type] {
		t.Fatalf("remaining delivery id = %s, want %s", d.ID, ids[retryableHandler.Type])
	}
	if d.RetryCount != 1 {
		t.Fatalf("retry_count = %d, want 1", d.RetryCount)
	}
	if !d.AvailableAt.After(time.Now().Add(100 * time.Millisecond)) {
		t.Fatalf("available_at = %s, want future retry", d.AvailableAt)
	}
	if d.TakenAt.Valid {
		t.Fatal("taken_at should be cleared for retry")
	}
	record := deadLetterRecordByHandler(t, db, fatalHandler.Type)
	if record.RemainingHandlers != "[]" {
		t.Fatalf("remaining_handlers = %s, want [] in v2 (siblings are independent rows)", record.RemainingHandlers)
	}
	if record.OutboxID != ids[fatalHandler.Type] {
		t.Fatalf("outbox_id = %s, want fatal delivery id %s", record.OutboxID, ids[fatalHandler.Type])
	}
}

func TestOutboxExhaustedRetriesWritesDeadLetterPerFailedHandler(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithMaxRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	first := mocks.NewEventHandler("exhausted_first_handler")
	first.Err = errors.New("first failure")
	second := mocks.NewEventHandler("exhausted_second_handler")
	second.Err = errors.New("second failure")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, first); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, second); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	otSeedDeliveries(t, o, newTestEvent("exhausted"), []string{first.Type, second.Type}, createdAt, createdAt, sql.NullTime{})

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 2 {
		t.Fatalf("processed = %d, want 2", processed)
	}

	if got := countDeliveries(t, o); got != 0 {
		t.Fatalf("deliveries = %d, want 0", got)
	}
	if got := countPublications(t, o); got != 0 {
		t.Fatalf("publications = %d, want 0", got)
	}
	assertDeadLetterHandlers(t, db, []string{first.Type, second.Type})
	if got := deadLetterRowCount(t, db); got != 2 {
		t.Fatalf("dead letter rows = %d, want 2", got)
	}
}

func TestOutboxPermanentFailureDoesNotBlockRemainingReceiver(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	fatalHandler := mocks.NewEventHandler("first_permanent_handler")
	fatalHandler.Err = fatalOutboxError{err: errors.New("fatal handler failure")}
	remainingHandler := mocks.NewEventHandler("remaining_handler")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, fatalHandler); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, remainingHandler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	otSeedDeliveries(t, o, newTestEvent("remaining"), []string{fatalHandler.Type}, createdAt, createdAt, sql.NullTime{})
	otSeedDeliveries(t, o, newTestEvent("next"), []string{remainingHandler.Type}, createdAt, createdAt, sql.NullTime{})

	processAllBatches(t, o, ctx)

	if got := countDeliveries(t, o); got != 0 {
		t.Fatalf("deliveries = %d, want 0", got)
	}
	if got := countPublications(t, o); got != 0 {
		t.Fatalf("publications = %d, want 0", got)
	}
	if !remainingHandler.Wait(time.Second) {
		t.Fatal("remaining receiver did not process its event")
	}
	assertDeadLetterHandlers(t, db, []string{fatalHandler.Type})
}

func TestOutboxDeadLetterExporterBestEffort(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	exporter := &recordingDeadLetterExporter{err: errors.New("disk full")}
	o, err := NewOutbox(db, WithMaxRetries(0), WithDeadLetterExporter(exporter))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	first := mocks.NewEventHandler("export_first_handler")
	first.Err = errors.New("first failure")
	second := mocks.NewEventHandler("export_second_handler")
	second.Err = errors.New("second failure")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, first); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, second); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	otSeedDeliveries(t, o, newTestEvent("export"), []string{first.Type, second.Type}, createdAt, createdAt, sql.NullTime{})

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 2 {
		t.Fatalf("processed = %d, want 2", processed)
	}

	if got := deadLetterRowCount(t, db); got != 2 {
		t.Fatalf("dead letter rows = %d, want 2", got)
	}
	if got := len(exporter.Records()); got != 2 {
		t.Fatalf("exported records = %d, want 2", got)
	}
	if got := exportedDeadLetterCount(t, db); got != 0 {
		t.Fatalf("exported dead letters = %d, want 0 after exporter error", got)
	}
}

func TestOutboxDeadLetterExporterMarksExportedAt(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	exporter := &recordingDeadLetterExporter{}
	o, err := NewOutbox(db, WithMaxRetries(0), WithDeadLetterExporter(exporter))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := mocks.NewEventHandler("export_success_handler")
	handler.Err = errors.New("handler failure")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	otSeedDeliveries(t, o, newTestEvent("export-success"), []string{handler.Type}, createdAt, createdAt, sql.NullTime{})

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}

	if got := deadLetterRowCount(t, db); got != 1 {
		t.Fatalf("dead letter rows = %d, want 1", got)
	}
	if got := exportedDeadLetterCount(t, db); got != 1 {
		t.Fatalf("exported dead letters = %d, want 1", got)
	}
}

// TestOutboxDeadLetterExportAfterFinalize asserts that file export runs only
// after the atomic finalize transaction has committed (DLQ insert + delivery
// delete + publication GC). The exporter observes the post-finalize DB state:
// the delivery row is gone and the dead_letters row is visible.
func TestOutboxDeadLetterExportAfterFinalize(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	var deliveryMissing bool
	var deliveriesLeft int
	var deadLetterPresent bool
	exporter := dl.ExportFunc(func(_ context.Context, record dl.Record) error {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM outbox_deliveries WHERE id = ?`, record.OutboxID).Scan(&count); err != nil {
			t.Errorf("exporter could not read outbox_deliveries: %v", err)
			return nil
		}
		deliveryMissing = count == 0
		deliveriesLeft = count
		if err := db.QueryRow(`
			SELECT COUNT(*) FROM dead_letters
			WHERE source = ? AND outbox_id = ? AND handler_type = ?
		`, record.Source, record.OutboxID, record.HandlerType).Scan(&count); err != nil {
			t.Errorf("exporter could not read dead_letters: %v", err)
			return nil
		}
		deadLetterPresent = count == 1
		return nil
	})

	o, err := NewOutbox(db, WithMaxRetries(0), WithDeadLetterExporter(exporter))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := mocks.NewEventHandler("export_after_finalize_handler")
	handler.Err = errors.New("terminal failure")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	otSeedDeliveries(t, o, newTestEvent("export-after-finalize"), []string{handler.Type}, createdAt, createdAt, sql.NullTime{})

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}

	if !deadLetterPresent {
		t.Fatal("exporter must observe the committed dead_letters row")
	}
	if !deliveryMissing {
		t.Fatalf("exporter observed %d delivery rows; want the row deleted after finalize commit", deliveriesLeft)
	}
}

// TestOutboxDeadLetterUniqueSourceOutboxHandler enforces uniqueness of
// (source, outbox_id, handler_type) so a crash-replay cannot create duplicate DLQs.
func TestOutboxDeadLetterUniqueSourceOutboxHandler(t *testing.T) {
	db := newTestDB(t)
	// EnsureSchema runs via NewOutbox.
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	now := time.Now()
	if _, err := db.Exec(`
		INSERT INTO dead_letters (id, source, event_type, aggregate_id, handler_type, outbox_id, remaining_handlers, blob, error, retry_count, created_at, dead_at)
		VALUES (?, 'outbox', 'Event', 'agg', 'handler_a', 'delivery-1', '[]', '{}', 'err', 0, ?, ?)
	`, uuid.New().String(), now, now); err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		INSERT INTO dead_letters (id, source, event_type, aggregate_id, handler_type, outbox_id, remaining_handlers, blob, error, retry_count, created_at, dead_at)
		VALUES (?, 'outbox', 'Event', 'agg', 'handler_a', 'delivery-1', '[]', '{}', 'err-dup', 0, ?, ?)
	`, uuid.New().String(), now, now)
	if err == nil {
		t.Fatal("expected unique constraint violation for duplicate (source, outbox_id, handler_type)")
	}

	// Distinct handler_type for the same delivery id must still be allowed.
	if _, err := db.Exec(`
		INSERT INTO dead_letters (id, source, event_type, aggregate_id, handler_type, outbox_id, remaining_handlers, blob, error, retry_count, created_at, dead_at)
		VALUES (?, 'outbox', 'Event', 'agg', 'handler_b', 'delivery-1', '[]', '{}', 'err', 0, ?, ?)
	`, uuid.New().String(), now, now); err != nil {
		t.Fatalf("distinct handler_type insert failed: %v", err)
	}
}

// TestOutboxFinalizeIdempotentOnReplay simulates a crash after the DLQ insert
// but before the delivery was deleted: the DLQ row already exists. Re-processing
// must not create a second dead letter, must still remove the terminal delivery
// and must leave the retryable sibling scheduled.
func TestOutboxFinalizeIdempotentOnReplay(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithRetryBackoff("FIXED:2:200ms"))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	fatalHandler := mocks.NewEventHandler("replay_fatal_handler")
	fatalHandler.Err = fatalOutboxError{err: errors.New("fatal again")}
	retryHandler := mocks.NewEventHandler("replay_retry_handler")
	retryHandler.Err = errors.New("retryable again")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, fatalHandler); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, retryHandler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	event := newTestEvent("replay-finalize")
	ids := otSeedDeliveries(t, o, event, []string{fatalHandler.Type, retryHandler.Type}, createdAt, createdAt, sql.NullTime{})

	// Pre-seed a dead letter as if the previous attempt wrote DLQ then crashed.
	eventBlob, err := o.codec.MarshalEvent(ctx, event)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if _, err := db.Exec(`
		INSERT INTO dead_letters (id, source, event_type, aggregate_id, handler_type, outbox_id, remaining_handlers, blob, error, retry_count, created_at, dead_at)
		VALUES (?, 'outbox', ?, ?, ?, ?, '[]', ?, 'previous fatal', 0, ?, ?)
	`, uuid.New().String(), event.EventType().String(), event.AggregateID().String(), fatalHandler.Type,
		ids[fatalHandler.Type], string(eventBlob), createdAt, now); err != nil {
		t.Fatal(err)
	}

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 2 {
		t.Fatalf("processed = %d, want 2", processed)
	}

	if got := deadLetterRowCount(t, db); got != 1 {
		t.Fatalf("dead letter rows = %d, want 1 (no duplicate on replay)", got)
	}
	otAssertDeliveryHandlers(t, o, []string{retryHandler.Type})
	d := listDeliveries(t, o)[0]
	if d.RetryCount != 1 {
		t.Fatalf("retry_count = %d, want 1", d.RetryCount)
	}
	if !d.AvailableAt.After(time.Now().Add(100 * time.Millisecond)) {
		t.Fatalf("available_at = %s, want future backoff", d.AvailableAt)
	}
	if d.TakenAt.Valid {
		t.Fatal("taken_at should be cleared after atomic finalize with retry")
	}
}

// TestOutboxFinalizeAtomicMixedTerminalAndRetry checks that the per-delivery
// finalize transactions leave DLQ, surviving deliveries and retry fields
// consistent for a mixed fatal/retryable/success fan-out.
func TestOutboxFinalizeAtomicMixedTerminalAndRetry(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithRetryBackoff("FIXED:2:300ms"))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	fatalHandler := mocks.NewEventHandler("atomic_fatal_handler")
	fatalHandler.Err = fatalOutboxError{err: errors.New("fatal")}
	retryHandler := mocks.NewEventHandler("atomic_retry_handler")
	retryHandler.Err = errors.New("retryable")
	successHandler := mocks.NewEventHandler("atomic_success_handler")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, fatalHandler); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, retryHandler); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, successHandler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	publicationID, ids := otSeedPublication(t, o, newTestEvent("atomic-mixed"), []string{
		fatalHandler.Type, retryHandler.Type, successHandler.Type,
	}, createdAt, createdAt, sql.NullTime{})

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 3 {
		t.Fatalf("processed = %d, want 3", processed)
	}

	// Atomic outcome: exactly one DLQ for the fatal delivery, the retryable one
	// rescheduled, the successful one deleted, publication kept.
	if got := deadLetterRowCount(t, db); got != 1 {
		t.Fatalf("dead letter rows = %d, want 1", got)
	}
	assertDeadLetterHandlers(t, db, []string{fatalHandler.Type})
	otAssertDeliveryHandlers(t, o, []string{retryHandler.Type})
	d := listDeliveries(t, o)[0]
	if d.PublicationID != publicationID {
		t.Fatalf("publication_id = %s, want %s", d.PublicationID, publicationID)
	}
	if d.RetryCount != 1 {
		t.Fatalf("retry_count = %d, want 1", d.RetryCount)
	}
	if !d.AvailableAt.After(time.Now().Add(100 * time.Millisecond)) {
		t.Fatalf("available_at = %s, want future backoff", d.AvailableAt)
	}
	if d.TakenAt.Valid {
		t.Fatal("taken_at should be cleared")
	}
	if got := countPublications(t, o); got != 1 {
		t.Fatalf("publications = %d, want 1 while a delivery remains", got)
	}
	record := deadLetterRecordByHandler(t, db, fatalHandler.Type)
	if record.OutboxID != ids[fatalHandler.Type] {
		t.Fatalf("outbox_id = %s, want %s", record.OutboxID, ids[fatalHandler.Type])
	}
	if record.PublicationID != publicationID {
		t.Fatalf("dead letter publication_id = %s, want %s", record.PublicationID, publicationID)
	}
	if record.RemainingHandlers != "[]" {
		t.Fatalf("remaining_handlers = %s, want [] in v2", record.RemainingHandlers)
	}
}

// TestOutboxFinalizeAtomicAllTerminalDeletesOutbox ensures a multi-handler
// permanent failure writes all DLQ rows and removes every delivery plus the
// publication.
func TestOutboxFinalizeAtomicAllTerminalDeletesOutbox(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	exporter := &recordingDeadLetterExporter{}
	o, err := NewOutbox(db, WithMaxRetries(0), WithDeadLetterExporter(exporter))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	first := mocks.NewEventHandler("all_terminal_a")
	first.Err = errors.New("a failed")
	second := mocks.NewEventHandler("all_terminal_b")
	second.Err = errors.New("b failed")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, first); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, second); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	otSeedDeliveries(t, o, newTestEvent("all-terminal"), []string{first.Type, second.Type}, createdAt, createdAt, sql.NullTime{})

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 2 {
		t.Fatalf("processed = %d, want 2", processed)
	}

	if got := countDeliveries(t, o); got != 0 {
		t.Fatalf("deliveries = %d, want 0", got)
	}
	if got := countPublications(t, o); got != 0 {
		t.Fatalf("publications = %d, want 0", got)
	}
	if got := deadLetterRowCount(t, db); got != 2 {
		t.Fatalf("dead letter rows = %d, want 2", got)
	}
	if got := len(exporter.Records()); got != 2 {
		t.Fatalf("exported records = %d, want 2 (after commit)", got)
	}
	if got := exportedDeadLetterCount(t, db); got != 2 {
		t.Fatalf("exported_at set = %d, want 2", got)
	}
}

// TestOutboxFinalizeCommitFailureRetriesSavedOutcome replaces the v1
// "rollback on handlers update abort" test (there is no handlers column any
// more). A finalize commit that fails is retried with the SAVED outcome: the
// handler is never re-run and the failed attempt leaves nothing behind (no
// partial or duplicate DLQ row).
func TestOutboxFinalizeCommitFailureRetriesSavedOutcome(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	exporter := &recordingDeadLetterExporter{}
	o, err := NewOutbox(db, WithRetryBackoff("FIXED:2:500ms"), WithDeadLetterExporter(exporter))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	o.finalizeSleep = func(time.Duration) {}

	var attempts atomic.Int64
	o.beforeFinalizeCommit = func(_ string, attempt int) error {
		attempts.Add(1)
		if attempt == 1 {
			return errors.New("forced finalize abort")
		}
		return nil
	}

	fatalHandler := &otCountingHandler{Type: "rollback_fatal_handler", err: fatalOutboxError{err: errors.New("fatal during finalize")}}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, fatalHandler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute).UTC().Truncate(time.Millisecond)
	ids := otSeedDeliveries(t, o, newTestEvent("finalize-retry"), []string{fatalHandler.Type}, createdAt, createdAt, sql.NullTime{})

	drainOutboxErrors(o)
	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}

	if got := attempts.Load(); got != 2 {
		t.Fatalf("finalize commit attempts = %d, want 2", got)
	}
	// The handler ran exactly once: finalize retries never re-run it.
	if handlerCalls := fatalHandler.Calls(); handlerCalls != 1 {
		t.Fatalf("handler calls = %d, want 1 (finalize retry must not re-run the handler)", handlerCalls)
	}
	// Finalized exactly once with the saved outcome: one DLQ row, no delivery.
	if got := deadLetterRowCount(t, db); got != 1 {
		t.Fatalf("dead letter rows = %d, want exactly 1 (no duplicate from the retried commit)", got)
	}
	record := deadLetterRecordByHandler(t, db, fatalHandler.Type)
	if record.OutboxID != ids[fatalHandler.Type] {
		t.Fatalf("outbox_id = %s, want %s", record.OutboxID, ids[fatalHandler.Type])
	}
	if got := countDeliveries(t, o); got != 0 {
		t.Fatalf("deliveries = %d, want 0", got)
	}
	if got := countPublications(t, o); got != 0 {
		t.Fatalf("publications = %d, want 0", got)
	}
	if got := len(exporter.Records()); got != 1 {
		t.Fatalf("exporter calls = %d, want 1 (export runs once, after commit)", got)
	}
}

// TestOutboxFinalizeCommitFailureReleasesClaim covers the exhausted-finalize
// path: every commit attempt fails, so the claim is released (taken_at NULL,
// available_at moved forward) without touching retry_count, and the admission
// slot is given back.
func TestOutboxFinalizeCommitFailureReleasesClaim(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	exporter := &recordingDeadLetterExporter{}
	o, err := NewOutbox(db, WithRetryBackoff("FIXED:2:500ms"), WithDeadLetterExporter(exporter))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	o.finalizeSleep = func(time.Duration) {}
	o.beforeFinalizeCommit = func(string, int) error { return errors.New("forced finalize abort") }
	o.beforeReleaseClaim = nil

	handler := mocks.NewEventHandler("release_claim_handler")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute).UTC().Truncate(time.Millisecond)
	availableAt := createdAt
	ids := otSeedDeliveries(t, o, newTestEvent("finalize-release"), []string{handler.Type}, createdAt, availableAt, sql.NullTime{})

	drainOutboxErrors(o)
	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}

	deliveries := listDeliveries(t, o)
	if len(deliveries) != 1 {
		t.Fatalf("deliveries = %d, want 1 (nothing was committed)", len(deliveries))
	}
	d := deliveries[0]
	if d.ID != ids[handler.Type] {
		t.Fatalf("delivery id = %s, want %s", d.ID, ids[handler.Type])
	}
	if d.TakenAt.Valid {
		t.Fatal("taken_at must be NULL after the claim was released")
	}
	if !d.AvailableAt.After(availableAt) {
		t.Fatalf("available_at = %s, want moved forward from %s", d.AvailableAt, availableAt)
	}
	if d.RetryCount != 0 {
		t.Fatalf("retry_count = %d, want 0 (release must not consume a retry)", d.RetryCount)
	}
	if got := deadLetterRowCount(t, db); got != 0 {
		t.Fatalf("dead letter rows = %d, want 0 after finalize rollback", got)
	}
	if got := len(exporter.Records()); got != 0 {
		t.Fatalf("exporter calls = %d, want 0 after finalize rollback", got)
	}
	if used, _ := o.AdmissionSnapshot(); used != 0 {
		t.Fatalf("admission used = %d, want 0 (slot released)", used)
	}

	finalizeErrs := drainOutboxErrors(o)
	if !containsErrorSubstring(finalizeErrs, "forced finalize abort") {
		t.Fatalf("expected finalize abort on Errors(), got: %v", finalizeErrs)
	}
}

// TestOutboxExportUsesDurableDeadLetterOnReplay ensures that after ON CONFLICT the
// exporter receives the durable dead_letters row (blob/error/id), not a freshly
// synthesized candidate, and marks exported_at on that row's id.
func TestOutboxExportUsesDurableDeadLetterOnReplay(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	exporter := &recordingDeadLetterExporter{}
	o, err := NewOutbox(db, WithMaxRetries(0), WithDeadLetterExporter(exporter))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := mocks.NewEventHandler("export_durable_handler")
	handler.Err = errors.New("new terminal failure from this attempt")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	event := newTestEvent("export-durable")
	ids := otSeedDeliveries(t, o, event, []string{handler.Type}, createdAt, createdAt, sql.NullTime{})
	deliveryID := ids[handler.Type]

	existingID := uuid.New().String()
	existingBlob := `{"durable":"previous-blob-not-from-this-attempt"}`
	existingError := "previous durable dead letter error"
	existingRemaining := `[]`
	existingCreatedAt := createdAt.Add(-time.Hour)
	existingDeadAt := createdAt.Add(-30 * time.Minute)
	if _, err := db.Exec(`
		INSERT INTO dead_letters (id, source, event_type, aggregate_id, handler_type, outbox_id, remaining_handlers, blob, error, retry_count, created_at, dead_at)
		VALUES (?, 'outbox', ?, ?, ?, ?, ?, ?, ?, 3, ?, ?)
	`, existingID, event.EventType().String(), event.AggregateID().String(), handler.Type, deliveryID, existingRemaining, existingBlob, existingError, existingCreatedAt, existingDeadAt); err != nil {
		t.Fatal(err)
	}

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}

	if got := deadLetterRowCount(t, db); got != 1 {
		t.Fatalf("dead letter rows = %d, want 1", got)
	}
	records := exporter.Records()
	if len(records) != 1 {
		t.Fatalf("exported records = %d, want 1", len(records))
	}
	got := records[0]
	if got.ID != existingID {
		t.Fatalf("exported id = %s, want durable id %s", got.ID, existingID)
	}
	if got.Blob != existingBlob {
		t.Fatalf("exported blob = %q, want durable blob %q", got.Blob, existingBlob)
	}
	if got.Error != existingError {
		t.Fatalf("exported error = %q, want durable error %q", got.Error, existingError)
	}
	if got.RetryCount != 3 {
		t.Fatalf("exported retry_count = %d, want 3 from durable row", got.RetryCount)
	}
	if got.RemainingHandlers != existingRemaining {
		t.Fatalf("exported remaining_handlers = %q, want %q", got.RemainingHandlers, existingRemaining)
	}

	var exportedID string
	var exportedAt sql.NullTime
	if err := db.QueryRow(`SELECT id, exported_at FROM dead_letters WHERE source = 'outbox' AND outbox_id = ? AND handler_type = ?`, deliveryID, handler.Type).Scan(&exportedID, &exportedAt); err != nil {
		t.Fatal(err)
	}
	if exportedID != existingID {
		t.Fatalf("stored id = %s, want %s", exportedID, existingID)
	}
	if !exportedAt.Valid {
		t.Fatal("exported_at should be set on the durable row id")
	}
}

// TestOutboxSuccessPathFinalizeDeletesWithoutDeadLetter covers pure success.
func TestOutboxSuccessPathFinalizeDeletesWithoutDeadLetter(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := mocks.NewEventHandler("success_only_handler")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	otSeedDeliveries(t, o, newTestEvent("success-only"), []string{handler.Type}, createdAt, createdAt, sql.NullTime{})

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}
	if got := countDeliveries(t, o); got != 0 {
		t.Fatalf("deliveries = %d, want 0", got)
	}
	if got := countPublications(t, o); got != 0 {
		t.Fatalf("publications = %d, want 0 (garbage collected with the last delivery)", got)
	}
	if got := deadLetterRowCount(t, db); got != 0 {
		t.Fatalf("dead letter rows = %d, want 0", got)
	}
}

// TestOutboxSerialDispatchPreservesSeqOrder is the v2 form of the former
// created_at/id ordering test: a Serial handler sees its deliveries in
// (available_at, seq) order, which for equal availability is insertion order.
func TestOutboxSerialDispatchPreservesSeqOrder(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := newOrderingHandler("serial_order_handler")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	var expected []string
	for i := range 100 {
		content := fmt.Sprintf("event-%03d", i)
		otSeedDeliveries(t, o, newTestEvent(content), []string{handler.Type}, createdAt, createdAt, sql.NullTime{})
		expected = append(expected, content)
	}
	otAssertDeliveryContentOrder(t, o, expected)

	processAllBatches(t, o, ctx)

	if got := handler.Contents(); !slicesEqual(got, expected) {
		t.Fatalf("serial order mismatch\n got: %v\nwant: %v", got, expected)
	}
}

func TestOutboxSerialDispatchDoesNotOverlapHandler(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := newConcurrencyHandler("serial_no_overlap_handler", time.Millisecond)
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	for i := range 20 {
		otSeedDeliveries(t, o, newTestEvent(fmt.Sprintf("event-%d", i)), []string{handler.Type}, createdAt, createdAt, sql.NullTime{})
	}

	processAllBatches(t, o, ctx)

	if got := handler.MaxInFlight(); got != 1 {
		t.Fatalf("max in-flight = %d, want 1", got)
	}
}

func TestOutboxPartitionByAggregatePreservesSameAggregateOrder(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := newOrderingHandler("partition_same_aggregate_handler")
	if err := o.AddHandlerWithOptions(ctx, eh.MatchEvents{mocks.EventType}, handler, WithDispatchMode(PartitionByAggregate), WithPartitionShards(8)); err != nil {
		t.Fatal(err)
	}

	aggregateID := uuid.New()
	createdAt := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	var expected []string
	for i := range 50 {
		content := fmt.Sprintf("event-%03d", i)
		otSeedDeliveries(t, o, newTestEventForAggregate(content, aggregateID), []string{handler.Type}, createdAt, createdAt, sql.NullTime{})
		expected = append(expected, content)
	}
	otAssertDeliveryContentOrder(t, o, expected)

	processAllBatches(t, o, ctx)

	if got := handler.Contents(); !slicesEqual(got, expected) {
		t.Fatalf("partition order mismatch\n got: %v\nwant: %v", got, expected)
	}
}

func TestOutboxPartitionByAggregateAllowsDifferentAggregatesInParallel(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithMaxGoroutines(2))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	entered := make(chan string, 2)
	release := make(chan struct{})
	handler := newBlockingHandler("partition_parallel_handler", entered, release)
	if err := o.AddHandlerWithOptions(ctx, eh.MatchEvents{mocks.EventType}, handler, WithDispatchMode(PartitionByAggregate), WithPartitionShards(16)); err != nil {
		t.Fatal(err)
	}

	aggregates := aggregateIDsForDistinctShards(t, 2, 16)
	createdAt := time.Now().Add(-time.Minute)
	for i, aggregateID := range aggregates {
		otSeedDeliveries(t, o, newTestEventForAggregate(fmt.Sprintf("event-%d", i), aggregateID), []string{handler.Type}, createdAt, createdAt, sql.NullTime{})
	}

	done := make(chan error, 1)
	go func() {
		_, err := o.processBatch(ctx)
		done <- err
	}()

	waitForEntries(t, entered, 2)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := handler.MaxInFlight(); got < 2 {
		t.Fatalf("max in-flight = %d, want at least 2", got)
	}
}

func TestOutboxPartitionByAggregateUsesCorrelationIDFallback(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithMaxGoroutines(2))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	entered := make(chan string, 2)
	release := make(chan struct{})
	handler := newBlockingHandler("partition_correlation_handler", entered, release)
	if err := o.AddHandlerWithOptions(ctx, eh.MatchEvents{mocks.EventType}, handler, WithDispatchMode(PartitionByAggregate), WithPartitionShards(16)); err != nil {
		t.Fatal(err)
	}

	correlationIDs := correlationIDsForDistinctShards(t, 2, 16)
	createdAt := time.Now().Add(-time.Minute)
	for i, correlationID := range correlationIDs {
		event := eh.NewEvent(mocks.EventType, &mocks.EventData{Content: fmt.Sprintf("event-%d", i)}, time.Now(),
			eh.WithMetadata(map[string]any{"correlation_id": correlationID}),
		)
		otSeedDeliveries(t, o, event, []string{handler.Type}, createdAt, createdAt, sql.NullTime{})
	}

	// The stored partition key comes from the correlation metadata, so the two
	// deliveries land on distinct dispatch keys.
	keys := map[string]bool{}
	for _, d := range listDeliveries(t, o) {
		keys[d.DispatchKey] = true
	}
	if len(keys) != 2 {
		t.Fatalf("distinct dispatch keys = %d, want 2 (correlation fallback)", len(keys))
	}

	done := make(chan error, 1)
	go func() {
		_, err := o.processBatch(ctx)
		done <- err
	}()

	waitForEntries(t, entered, 2)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := handler.MaxInFlight(); got < 2 {
		t.Fatalf("max in-flight = %d, want at least 2", got)
	}
}

func TestOutboxDispatchRespectsGlobalMaxGoroutines(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithMaxGoroutines(2))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	handler := newConcurrencyHandler("partition_cap_handler", 10*time.Millisecond)
	if err := o.AddHandlerWithOptions(ctx, eh.MatchEvents{mocks.EventType}, handler, WithDispatchMode(PartitionByAggregate), WithPartitionShards(16)); err != nil {
		t.Fatal(err)
	}

	aggregates := aggregateIDsForDistinctShards(t, 8, 16)
	createdAt := time.Now().Add(-time.Minute)
	for i, aggregateID := range aggregates {
		otSeedDeliveries(t, o, newTestEventForAggregate(fmt.Sprintf("event-%d", i), aggregateID), []string{handler.Type}, createdAt, createdAt, sql.NullTime{})
	}

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != len(aggregates) {
		t.Fatalf("processed = %d, want %d", processed, len(aggregates))
	}
	if got := handler.MaxInFlight(); got > 2 {
		t.Fatalf("max in-flight = %d, want <= 2", got)
	}
}

func TestOutboxPartialSuccessKeepsOnlyFailedHandlerForRetry(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	o, err := NewOutbox(db, WithRetryBackoff("FIXED:2:200ms"))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	successHandler := mocks.NewEventHandler("partial_success_handler")
	failingHandler := mocks.NewEventHandler("partial_failing_handler")
	failingHandler.Err = errors.New("temporary failure")
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, successHandler); err != nil {
		t.Fatal(err)
	}
	if err := o.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, failingHandler); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().Add(-time.Minute)
	otSeedDeliveries(t, o, newTestEvent("partial"), []string{successHandler.Type, failingHandler.Type}, createdAt, createdAt, sql.NullTime{})

	if processed, err := o.processBatch(ctx); err != nil {
		t.Fatal(err)
	} else if processed != 2 {
		t.Fatalf("processed = %d, want 2", processed)
	}

	otAssertDeliveryHandlers(t, o, []string{failingHandler.Type})
	d := listDeliveries(t, o)[0]
	if d.RetryCount != 1 {
		t.Fatalf("retry_count = %d, want 1", d.RetryCount)
	}
	if d.TakenAt.Valid {
		t.Fatal("taken_at should be cleared for retry")
	}
	if got := deadLetterRowCount(t, db); got != 0 {
		t.Fatalf("dead letter rows = %d, want 0", got)
	}
}

func newTestDB(t testing.TB) *sql.DB {
	t.Helper()

	f, err := os.CreateTemp("", "outbox-*.db")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Get a new SQLite database.
	db, err := sql.Open("sqlite3", f.Name()+"?_journal=wal&_busy_timeout=5000&_synchronous=normal&_fk=1&_loc=auto")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		db.Close()
		os.Remove(f.Name())
	})

	return db
}

func setPeriodicSweepInterval(t testing.TB, interval time.Duration) func() {
	t.Helper()

	previous := PeriodicSweepInterval
	PeriodicSweepInterval = interval
	return func() {
		PeriodicSweepInterval = previous
	}
}

func setPeriodicSweepAge(t testing.TB, age time.Duration) func() {
	t.Helper()

	previous := PeriodicSweepAge
	PeriodicSweepAge = age
	return func() {
		PeriodicSweepAge = previous
	}
}

func newTestEvent(content string) eh.Event {
	return eh.NewEvent(mocks.EventType, &mocks.EventData{Content: content}, time.Now(),
		eh.ForAggregate(mocks.AggregateType, uuid.New(), 1),
	)
}

func newTestEventForAggregate(content string, aggregateID uuid.UUID) eh.Event {
	return eh.NewEvent(mocks.EventType, &mocks.EventData{Content: content}, time.Now(),
		eh.ForAggregate(mocks.AggregateType, aggregateID, 1),
	)
}

// otSeedPublication writes one v2 publication plus one delivery per handler
// name (an empty list seeds a rematch sentinel), computing dispatch_key and
// dispatch_config from the current registration so the rows are claimable
// without a startup reconcile. It returns the publication id and a map of
// handler type ("" for the sentinel) to delivery id.
func otSeedPublication(t testing.TB, o *Outbox, event eh.Event, handlers []string, createdAt, availableAt time.Time, takenAt sql.NullTime) (string, map[string]string) {
	t.Helper()

	if availableAt.IsZero() {
		availableAt = createdAt
	}
	eventBlob, err := o.codec.MarshalEvent(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	publicationID := uuid.New().String()
	partitionKey := eventPartitionKey(event)
	if _, err := o.db.Exec(fmt.Sprintf(`
		INSERT INTO %s (publication_id, event_type, aggregate_id, partition_key, event_blob, created_at, origin, origin_ref)
		VALUES (?, ?, ?, ?, ?, ?, 'publish', NULL)`, o.publicationsTable),
		publicationID, event.EventType().String(), event.AggregateID().String(), partitionKey,
		string(eventBlob), schema.UTC(createdAt)); err != nil {
		t.Fatal(err)
	}

	otReadyForClaim(t, o)
	registered := o.snapshotHandlersByType()
	insert := func(handlerType any, key, config any) string {
		deliveryID := uuid.New().String()
		var taken any
		if takenAt.Valid {
			taken = schema.UTC(takenAt.Time)
		}
		if _, err := o.db.Exec(fmt.Sprintf(`
			INSERT INTO %s (id, publication_id, handler_type, dispatch_key, dispatch_config, event_type, aggregate_id,
			                created_at, available_at, taken_at, retry_count, unresolved_at, legacy_outbox_id)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL)`, o.deliveriesTable),
			deliveryID, publicationID, handlerType, key, config,
			event.EventType().String(), event.AggregateID().String(),
			schema.UTC(createdAt), schema.UTC(availableAt), taken); err != nil {
			t.Fatal(err)
		}
		return deliveryID
	}

	ids := map[string]string{}
	if len(handlers) == 0 {
		ids[""] = insert(nil, nil, nil)
		return publicationID, ids
	}
	for _, handlerType := range handlers {
		var key, config any
		if mh := registered[handlerType]; mh != nil {
			queueKey, _, _ := dispatchKeyFor(mh, partitionKey)
			key = queueKey
			config = dispatchConfigFor(mh)
		}
		ids[handlerType] = insert(handlerType, key, config)
	}
	return publicationID, ids
}

// otSeedDeliveries is otSeedPublication without the publication id.
func otSeedDeliveries(t testing.TB, o *Outbox, event eh.Event, handlers []string, createdAt, availableAt time.Time, takenAt sql.NullTime) map[string]string {
	t.Helper()

	_, ids := otSeedPublication(t, o, event, handlers, createdAt, availableAt, takenAt)
	return ids
}

// seedOutboxEvent seeds one publication with a single handler delivery.
func seedOutboxEvent(t testing.TB, db *sql.DB, o *Outbox, event eh.Event, handlerType string, createdAt, availableAt time.Time, takenAt sql.NullTime) {
	t.Helper()

	_ = db
	otSeedDeliveries(t, o, event, []string{handlerType}, createdAt, availableAt, takenAt)
}

// seedOutboxEventWithID keeps the historical signature; id is ignored for the
// v2 row identities (deliveries own their own uuid) and kept only so existing
// callers compile. Use otSeedDeliveries when the delivery ids are needed.
func seedOutboxEventWithID(t testing.TB, db *sql.DB, o *Outbox, id string, event eh.Event, handlers []string, createdAt, availableAt time.Time, takenAt sql.NullTime) {
	t.Helper()

	_, _ = db, id
	otSeedDeliveries(t, o, event, handlers, createdAt, availableAt, takenAt)
}

func processAllBatches(t testing.TB, o *Outbox, ctx context.Context) {
	t.Helper()

	otReadyForClaim(t, o)
	for {
		processed, err := o.processBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if processed == 0 {
			return
		}
	}
}

// otReadyForClaim publishes the registered dispatch key ring that the fetcher
// rotates over. StartChecked does this; tests that drive processBatch directly
// after seeding rows must do it themselves.
func otReadyForClaim(t testing.TB, o *Outbox) {
	t.Helper()

	o.handlersMu.Lock()
	o.claimKeys = o.registeredDispatchKeys()
	o.handlersMu.Unlock()
}

// otAssertDeliveryHandlers asserts the handler types of the surviving
// deliveries (seq order), replacing the v1 `handlers` column assertion.
func otAssertDeliveryHandlers(t testing.TB, o *Outbox, want []string) {
	t.Helper()

	var got []string
	for _, d := range listDeliveries(t, o) {
		got = append(got, d.HandlerType)
	}
	if !slicesEqual(got, want) {
		t.Fatalf("delivery handler types = %v, want %v", got, want)
	}
}

// otAssertDeliveryContentOrder asserts the stored deliveries are in the same
// (available_at, seq) order as the expected event contents.
func otAssertDeliveryContentOrder(t testing.TB, o *Outbox, want []string) {
	t.Helper()

	rows, err := o.db.Query(fmt.Sprintf(`
		SELECT p.event_blob FROM %s d JOIN %s p ON p.publication_id = d.publication_id
		ORDER BY d.available_at ASC, d.seq ASC`, o.deliveriesTable, o.publicationsTable))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var got []string
	for rows.Next() {
		var blob string
		if err := rows.Scan(&blob); err != nil {
			t.Fatal(err)
		}
		event, _, err := o.codec.UnmarshalEvent(context.Background(), []byte(blob))
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, eventContent(event))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !slicesEqual(got, want) {
		t.Fatalf("stored delivery order = %v, want %v", got, want)
	}
}

func otDeliveryExists(t testing.TB, o *Outbox, deliveryID string) bool {
	t.Helper()

	var count int
	if err := o.db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE id = ?`, o.deliveriesTable), deliveryID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count > 0
}

func otPublicationExists(t testing.TB, o *Outbox, publicationID string) bool {
	t.Helper()

	var count int
	if err := o.db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE publication_id = ?`, o.publicationsTable), publicationID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count > 0
}

func otTableExists(t testing.TB, db *sql.DB, table string) bool {
	t.Helper()

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count > 0
}

func aggregateIDsForDistinctShards(t testing.TB, count, shards int) []uuid.UUID {
	t.Helper()

	seen := map[uint64]bool{}
	var ids []uuid.UUID
	for attempts := 0; len(ids) < count && attempts < 10000; attempts++ {
		id := uuid.New()
		shard := hashPartition(id.String(), shards)
		if seen[shard] {
			continue
		}
		seen[shard] = true
		ids = append(ids, id)
	}
	if len(ids) != count {
		t.Fatalf("generated %d distinct shard aggregate IDs, want %d", len(ids), count)
	}
	return ids
}

func correlationIDsForDistinctShards(t testing.TB, count, shards int) []string {
	t.Helper()

	seen := map[uint64]bool{}
	var ids []string
	for i := 0; len(ids) < count && i < 10000; i++ {
		id := fmt.Sprintf("correlation-%d", i)
		shard := hashPartition(id, shards)
		if seen[shard] {
			continue
		}
		seen[shard] = true
		ids = append(ids, id)
	}
	if len(ids) != count {
		t.Fatalf("generated %d distinct shard correlation IDs, want %d", len(ids), count)
	}
	return ids
}

type orderingHandler struct {
	Type string
	mu   sync.Mutex
	seen []string
}

func newOrderingHandler(handlerType string) *orderingHandler {
	return &orderingHandler{Type: handlerType}
}

func (h *orderingHandler) HandlerType() eh.EventHandlerType {
	return eh.EventHandlerType(h.Type)
}

func (h *orderingHandler) HandleEvent(_ context.Context, event eh.Event) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.seen = append(h.seen, eventContent(event))
	return nil
}

func (h *orderingHandler) Contents() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	return append([]string(nil), h.seen...)
}

// otCountingHandler counts HandleEvent calls and always returns err (the mocks
// handler does not record a call when it fails).
type otCountingHandler struct {
	Type  string
	err   error
	calls atomic.Int64
}

func (h *otCountingHandler) HandlerType() eh.EventHandlerType {
	return eh.EventHandlerType(h.Type)
}

func (h *otCountingHandler) HandleEvent(context.Context, eh.Event) error {
	h.calls.Add(1)
	return h.err
}

func (h *otCountingHandler) Calls() int64 {
	return h.calls.Load()
}

type concurrencyHandler struct {
	Type     string
	delay    time.Duration
	inFlight atomic.Int64
	max      atomic.Int64
}

func newConcurrencyHandler(handlerType string, delay time.Duration) *concurrencyHandler {
	return &concurrencyHandler{Type: handlerType, delay: delay}
}

func (h *concurrencyHandler) HandlerType() eh.EventHandlerType {
	return eh.EventHandlerType(h.Type)
}

func (h *concurrencyHandler) HandleEvent(context.Context, eh.Event) error {
	current := h.inFlight.Add(1)
	updateMax(&h.max, current)
	if h.delay > 0 {
		time.Sleep(h.delay)
	}
	h.inFlight.Add(-1)
	return nil
}

func (h *concurrencyHandler) MaxInFlight() int64 {
	return h.max.Load()
}

type blockingHandler struct {
	Type     string
	entered  chan<- string
	release  <-chan struct{}
	inFlight atomic.Int64
	max      atomic.Int64
}

func newBlockingHandler(handlerType string, entered chan<- string, release <-chan struct{}) *blockingHandler {
	return &blockingHandler{Type: handlerType, entered: entered, release: release}
}

func (h *blockingHandler) HandlerType() eh.EventHandlerType {
	return eh.EventHandlerType(h.Type)
}

func (h *blockingHandler) HandleEvent(_ context.Context, event eh.Event) error {
	current := h.inFlight.Add(1)
	updateMax(&h.max, current)
	h.entered <- eventContent(event)
	<-h.release
	h.inFlight.Add(-1)
	return nil
}

func (h *blockingHandler) MaxInFlight() int64 {
	return h.max.Load()
}

func updateMax(max *atomic.Int64, value int64) {
	for {
		current := max.Load()
		if value <= current {
			return
		}
		if max.CompareAndSwap(current, value) {
			return
		}
	}
}

func waitForEntries(t testing.TB, entered <-chan string, count int) {
	t.Helper()

	deadline := time.After(2 * time.Second)
	for i := 0; i < count; i++ {
		select {
		case <-entered:
		case <-deadline:
			t.Fatalf("timed out waiting for %d parallel entries, got %d", count, i)
		}
	}
}

func eventContent(event eh.Event) string {
	if data, ok := event.Data().(*mocks.EventData); ok {
		return data.Content
	}
	return fmt.Sprint(event.Data())
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// outboxRowCount counts pending delivery rows (the v2 unit of work) for the
// default table prefix.
func outboxRowCount(t testing.TB, db *sql.DB) int {
	t.Helper()

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM outbox_deliveries`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func deadLetterRowCount(t testing.TB, db *sql.DB) int {
	t.Helper()

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM dead_letters`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func drainOutboxErrors(o *Outbox) []error {
	var errs []error
	for {
		select {
		case err := <-o.Errors():
			if err != nil {
				errs = append(errs, err)
			}
		default:
			return errs
		}
	}
}

func containsErrorSubstring(errs []error, substr string) bool {
	for _, err := range errs {
		if err != nil && strings.Contains(err.Error(), substr) {
			return true
		}
	}
	return false
}

func exportedDeadLetterCount(t testing.TB, db *sql.DB) int {
	t.Helper()

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM dead_letters WHERE exported_at IS NOT NULL`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

type fatalOutboxError struct {
	err error
}

func (e fatalOutboxError) Error() string {
	return e.err.Error()
}

func (e fatalOutboxError) Unwrap() error {
	return e.err
}

func (e fatalOutboxError) OutboxSeverity() ErrorSeverity {
	return SeverityFatal
}

type recordingDeadLetterExporter struct {
	mu      sync.Mutex
	records []dl.Record
	err     error
}

func (e *recordingDeadLetterExporter) ExportDeadLetter(_ context.Context, record dl.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.records = append(e.records, record)
	return e.err
}

func (e *recordingDeadLetterExporter) Records() []dl.Record {
	e.mu.Lock()
	defer e.mu.Unlock()

	return append([]dl.Record(nil), e.records...)
}

func assertDeadLetterHandlers(t testing.TB, db *sql.DB, want []string) {
	t.Helper()

	rows, err := db.Query(`SELECT handler_type FROM dead_letters WHERE source = 'outbox' ORDER BY handler_type ASC`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var got []string
	for rows.Next() {
		var handlerType string
		if err := rows.Scan(&handlerType); err != nil {
			t.Fatal(err)
		}
		got = append(got, handlerType)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	want = append([]string(nil), want...)
	sortStrings(want)
	if !slicesEqual(got, want) {
		t.Fatalf("dead letter handlers = %v, want %v", got, want)
	}
}

type deadLetterRow struct {
	OutboxID          string
	RemainingHandlers string
	PublicationID     string
	LegacyOutboxID    string
}

func deadLetterRecordByHandler(t testing.TB, db *sql.DB, handlerType string) deadLetterRow {
	t.Helper()

	var record deadLetterRow
	var outboxID, publicationID, legacyOutboxID sql.NullString
	if err := db.QueryRow(`
		SELECT outbox_id, remaining_handlers, publication_id, legacy_outbox_id
		FROM dead_letters
		WHERE source = 'outbox' AND handler_type = ?
	`, handlerType).Scan(&outboxID, &record.RemainingHandlers, &publicationID, &legacyOutboxID); err != nil {
		t.Fatal(err)
	}
	record.OutboxID = outboxID.String
	record.PublicationID = publicationID.String
	record.LegacyOutboxID = legacyOutboxID.String
	return record
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}
