package sqlite

import (
	"context"
	"fmt"
	"testing"
	"time"

	eh "github.com/vercly/eventhorizon"
	"github.com/vercly/eventhorizon/mocks"
)

// TestStartupReconcileKeepsKeyOfUndecodableEnvelopes: envelopes the
// partition pass cannot read keep their stored partition_key and never fail
// startup; a readable sibling is still re-sharded.
func TestStartupReconcileKeepsKeyOfUndecodableEnvelopes(t *testing.T) {
	db := newTestDB(t)
	o := v2NewOutbox(t, db)
	h := &v2CountingHandler{typ: "h"}
	if err := o.AddHandler(context.Background(), eh.MatchEvents{mocks.EventType}, h); err != nil {
		t.Fatal(err)
	}
	blobs := map[string]string{
		"malformed":       `{not json`,
		"array":           `[1,2,3]`,
		"metadata-string": `{"aggregate_id":"x","metadata":"oops"}`,
	}
	ids := map[string]string{}
	for name, blob := range blobs {
		pubID, _ := insertDeliveryDirect(t, o, deliverySeed{Event: newTestEvent(name), HandlerType: "h", CreatedAt: time.Now()})
		if _, err := db.Exec(fmt.Sprintf(`UPDATE %s SET event_blob = ?, partition_key = 'stored' WHERE publication_id = ?`, o.publicationsTable), blob, pubID); err != nil {
			t.Fatal(err)
		}
		ids[name] = pubID
	}
	readable := newTestEvent("readable")
	readableID, _ := insertDeliveryDirect(t, o, deliverySeed{Event: readable, HandlerType: "h", CreatedAt: time.Now()})
	if _, err := db.Exec(fmt.Sprintf(`UPDATE %s SET partition_key = 'stale' WHERE publication_id = ?`, o.publicationsTable), readableID); err != nil {
		t.Fatal(err)
	}

	reconcileForTest(t, o)

	keyOf := func(id string) string {
		return storedText(t, db, fmt.Sprintf(`SELECT partition_key FROM %s WHERE publication_id = ?`, o.publicationsTable), id)
	}
	for name, id := range ids {
		if got := keyOf(id); got != "stored" {
			t.Fatalf("%s envelope: partition_key = %q, want stored key kept", name, got)
		}
	}
	if got, want := keyOf(readableID), readable.AggregateID().String(); got != want {
		t.Fatalf("readable envelope: partition_key = %q, want re-sharded %q", got, want)
	}
}
