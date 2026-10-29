package sqlite

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	eh "github.com/vercly/eventhorizon"
	"github.com/vercly/eventhorizon/mocks"
)

// TestStartupReconcileExplicitKeyMatchesFullDecode: the explicit-key fast
// path re-derives exactly the key a full envelope decode yields
// (StoredEventPartitionKeyWith, the replay path), for every JSON shape of
// MetadataPartitionKey, duplicate keys and numbers encoding/json rejects, and
// never consults the resolver when a text or null explicit key decides the
// partition.
func TestStartupReconcileExplicitKeyMatchesFullDecode(t *testing.T) {
	cases := []struct {
		name string
		// metadata is the raw JSON of the envelope's metadata ("" = absent).
		metadata string
		// extra is raw JSON appended to the envelope after metadata.
		extra string
		// fast: a text/null explicit key decides; the resolver must not run.
		fast bool
		// reconcileWant overrides the full-decode key where reconcile, since
		// it reads metadata through SQLite, sees only the first of duplicate
		// metadata objects while encoding/json merges them.
		reconcileWant string
	}{
		{name: "text", metadata: `{"case":"text","outbox.partition_key":"  k1  ","resolve":"r"}`, fast: true},
		{name: "empty text", metadata: `{"case":"empty text","outbox.partition_key":"","resolve":"r"}`, fast: true},
		{name: "escaped text", metadata: `{"case":"escaped text","outbox.partition_key":"\u00e9\u0041\t","resolve":"r"}`, fast: true},
		{name: "lone surrogate", metadata: `{"case":"lone surrogate","outbox.partition_key":"\ud800x","resolve":"r"}`, fast: true},
		{name: "nul", metadata: `{"case":"nul","outbox.partition_key":"a\u0000b","resolve":"r"}`, fast: true},
		{name: "null", metadata: `{"case":"null","outbox.partition_key":null,"resolve":"r"}`, fast: true},
		{name: "integer", metadata: `{"case":"integer","outbox.partition_key":12345678901234567890}`},
		{name: "real", metadata: `{"case":"real","outbox.partition_key":1.5}`},
		{name: "bool", metadata: `{"case":"bool","outbox.partition_key":true}`},
		{name: "object", metadata: `{"case":"object","outbox.partition_key":{"a":1}}`},
		{name: "array", metadata: `{"case":"array","outbox.partition_key":["a"]}`},
		{name: "missing, resolver", metadata: `{"case":"missing, resolver","resolve":"r1"}`},
		{name: "missing, correlation", metadata: `{"case":"missing, correlation","correlation_id":"c1"}`},
		{name: "nested look-alike", metadata: `{"case":"nested look-alike","outbox":{"partition_key":"wrong"}}`},
		{name: "no metadata"},
		{name: "null metadata", metadata: `null`},
		{name: "metadata string", metadata: `"oops"`},
		// encoding/json keeps the last duplicate; the fast path must agree.
		{name: "duplicate key", metadata: `{"case":"duplicate key","outbox.partition_key":"first","outbox.partition_key":"last"}`},
		{name: "duplicate key, other type", metadata: `{"case":"duplicate key, other type","outbox.partition_key":"first","outbox.partition_key":7}`},
		{name: "duplicate key, null first", metadata: `{"case":"duplicate key, null first","outbox.partition_key":null,"outbox.partition_key":"last"}`},
		{name: "duplicate metadata", metadata: `{"case":"duplicate metadata","outbox.partition_key":"first"}`,
			extra: `"metadata":{"case":"duplicate metadata","outbox.partition_key":"last"}`, reconcileWant: "first"},
		{name: "duplicate metadata, key dropped", metadata: `{"case":"duplicate metadata, key dropped","outbox.partition_key":"first"}`,
			extra: `"metadata":{"case":"duplicate metadata, key dropped","resolve":"r2"}`},
		{name: "duplicate correlation", metadata: `{"case":"duplicate correlation","resolve":"first","resolve":"last"}`},
		// SQLite accepts numbers beyond float64 that encoding/json rejects;
		// the rejection keeps the stored key even beside an explicit key.
		{name: "overflow sibling", metadata: `{"case":"overflow sibling","outbox.partition_key":"k","n":1e999}`},
		{name: "negative overflow nested", metadata: `{"case":"negative overflow nested","outbox.partition_key":"k","x":{"y":[-1e999]}}`},
		{name: "overflow at float64 edge", metadata: `{"case":"overflow at float64 edge","outbox.partition_key":"k","n":1.7976931348623159e308}`},
		{name: "overflow integer", metadata: `{"case":"overflow integer","outbox.partition_key":"k","n":1` + strings.Repeat("0", 400) + `}`},
		{name: "large but finite", metadata: `{"case":"large but finite","outbox.partition_key":"k","n":1.5e308}`},
		{name: "int64 min", metadata: `{"case":"int64 min","outbox.partition_key":"k","n":-9223372036854775808}`, fast: true},
	}
	resolverCalls := map[string]int{}
	counting := false
	resolver := func(metadata map[string]any, _ string) (string, bool) {
		if tag, ok := metadata["case"].(string); ok && counting {
			resolverCalls[tag]++
		}
		key, ok := metadata["resolve"].(string)
		return key, ok
	}

	db := newTestDB(t)
	o := v2NewOutbox(t, db, WithPartitionKeyResolver(resolver))
	h := &v2CountingHandler{typ: "h"}
	if err := o.AddHandler(context.Background(), eh.MatchEvents{mocks.EventType}, h); err != nil {
		t.Fatal(err)
	}
	type seeded struct {
		pubID, want string
		fast        bool
	}
	seeds := map[string]seeded{}
	for _, tc := range cases {
		event := newTestEvent(tc.name)
		blob := fmt.Sprintf(`{"event_type":%q,"aggregate_id":%q`, mocks.EventType, event.AggregateID())
		if tc.metadata != "" {
			blob += `,"metadata":` + tc.metadata
		}
		if tc.extra != "" {
			blob += `,` + tc.extra
		}
		blob += `}`
		want, err := StoredEventPartitionKeyWith([]byte(blob), resolver)
		if err != nil {
			want = "stale" // undecodable metadata keeps the stored key
		}
		if tc.reconcileWant != "" {
			want = tc.reconcileWant
		}
		pubID, _ := insertDeliveryDirect(t, o, deliverySeed{Event: event, HandlerType: "h", CreatedAt: time.Now()})
		if _, err := db.Exec(fmt.Sprintf(`UPDATE %s SET event_blob = ?, partition_key = 'stale' WHERE publication_id = ?`, o.publicationsTable), blob, pubID); err != nil {
			t.Fatal(err)
		}
		seeds[tc.name] = seeded{pubID: pubID, want: want, fast: tc.fast}
	}

	counting = true
	reconcileForTest(t, o)
	counting = false

	for name, s := range seeds {
		got := storedText(t, db, fmt.Sprintf(`SELECT partition_key FROM %s WHERE publication_id = ?`, o.publicationsTable), s.pubID)
		if got != s.want {
			t.Errorf("%s: partition_key = %q, want %q", name, got, s.want)
		}
		if s.fast && resolverCalls[name] != 0 {
			t.Errorf("%s: resolver called %d times, want none for a decisive explicit key", name, resolverCalls[name])
		}
	}
	if resolverCalls["missing, resolver"] == 0 {
		t.Error("missing, resolver: resolver not consulted without an explicit key")
	}
}

// TestStartupReconcileKeepsKeyOfOversizedExplicitEnvelope: an envelope over
// the payload budget keeps its stored key even when it carries an explicit
// key, as before the fast path.
func TestStartupReconcileKeepsKeyOfOversizedExplicitEnvelope(t *testing.T) {
	db := newTestDB(t)
	o := v2NewOutbox(t, db, WithPayloadBudget(1024))
	h := &v2CountingHandler{typ: "h"}
	if err := o.AddHandler(context.Background(), eh.MatchEvents{mocks.EventType}, h); err != nil {
		t.Fatal(err)
	}
	event := eh.NewEvent(mocks.EventType, &mocks.EventData{Content: strings.Repeat("x", 2048)}, time.Now(),
		eh.ForAggregate(mocks.AggregateType, newTestEvent("").AggregateID(), 1),
		eh.WithMetadata(map[string]any{MetadataPartitionKey: "explicit"}))
	pubID, _ := insertDeliveryDirect(t, o, deliverySeed{Event: event, HandlerType: "h", CreatedAt: time.Now()})
	if _, err := db.Exec(fmt.Sprintf(`UPDATE %s SET partition_key = 'stored' WHERE publication_id = ?`, o.publicationsTable), pubID); err != nil {
		t.Fatal(err)
	}

	reconcileForTest(t, o)

	if got := storedText(t, db, fmt.Sprintf(`SELECT partition_key FROM %s WHERE publication_id = ?`, o.publicationsTable), pubID); got != "stored" {
		t.Fatalf("partition_key = %q, want stored key kept for an oversized envelope", got)
	}
}

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
