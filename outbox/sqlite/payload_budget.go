package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	eh "github.com/vercly/eventhorizon"
	"strings"
	"sync"
	"time"
)

// DefaultPayloadBudget bounds serialized bytes retained by queued and running
// deliveries. Decoding, SQLite and handlers use additional memory; this is not
// an RSS limit. One publication is charged separately for each recipient.
const DefaultPayloadBudget int64 = 256 << 20

// ErrPayloadTooLarge leaves a delivery unresolved and its publication intact.
// After raising the budget or replacing the payload offline, restart to retry.
var ErrPayloadTooLarge = errors.New("outbox: payload exceeds byte budget")

// WithPayloadBudget limits serialized in-flight bytes and individual payload
// size. It must be positive. Oversized persisted records are never auto-deleted.
func WithPayloadBudget(bytes int64) Option {
	return func(o *Outbox) error {
		if bytes < 1 {
			return errors.New("payload budget must be positive")
		}
		o.payloads = newPayloadBudget(bytes)
		return nil
	}
}

type payloadBudget struct {
	mu           sync.Mutex
	limit, used  int64
	active       map[string]int64
	waitingID    string
	waitingBytes int64
}

func newPayloadBudget(limit int64) *payloadBudget {
	return &payloadBudget{limit: limit, active: make(map[string]int64)}
}
func (b *payloadBudget) reserve(id string, size int64) bool {
	return b.reserveWithPriority(id, size, false)
}

// Rematch sentinels precede normal claims. Let their budget reservation replace
// a normal waiting head, or the sentinel barrier and that head could deadlock.
func (b *payloadBudget) reserveSentinel(id string, size int64) bool {
	return b.reserveWithPriority(id, size, true)
}
func (b *payloadBudget) reserveWithPriority(id string, size int64, priority bool) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.active[id]; ok || size < 0 || size > b.limit {
		return false
	}
	if priority {
		b.waitingID, b.waitingBytes = id, size
	}
	// Protect the first blocked head from a continuous stream of smaller
	// deliveries. They may use spare capacity, but cannot consume its share.
	if b.waitingID != "" && b.waitingID != id && size > b.limit-b.used-b.waitingBytes {
		return false
	}
	if size > b.limit-b.used {
		if b.waitingID == "" {
			b.waitingID, b.waitingBytes = id, size
		}
		return false
	}
	if b.waitingID == id {
		b.waitingID, b.waitingBytes = "", 0
	}
	b.active[id] = size
	b.used += size
	return true
}
func (b *payloadBudget) release(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.used -= b.active[id]
	delete(b.active, id)
	if b.waitingID == id {
		b.waitingID, b.waitingBytes = "", 0
	}
}
func (o *Outbox) releasePayload(doc *deliveryDoc) {
	// Only the claim owner (rollback), queue drain (not executing), or the
	// executing worker owns doc here; these lifecycle paths are exclusive.
	// Clear references before admitting a replacement payload.
	doc.EventBlob = ""
	doc.Event = nil
	doc.EventCtx = nil
	o.payloads.release(doc.ID)
}
func (o *Outbox) loadPayload(ctx context.Context, tx *sql.Tx, doc *deliveryDoc) error {
	return tx.QueryRowContext(ctx, fmt.Sprintf("SELECT event_blob FROM %s WHERE publication_id = ?", o.publicationsTable), doc.PublicationID).Scan(&doc.EventBlob)
}
func (o *Outbox) quarantinePayload(ctx context.Context, doc *deliveryDoc, now time.Time, stmt *sql.Stmt) error {
	if _, err := stmt.ExecContext(ctx, now, doc.ID); err != nil {
		return err
	}
	o.observeSkip(SkipPayloadTooLarge, 1)
	o.sendError(ctx, fmt.Errorf("%w: delivery %s publication %s bytes=%d budget=%d (retained unresolved)", ErrPayloadTooLarge, doc.ID, doc.PublicationID, doc.PayloadBytes, o.payloads.limit), nil)
	return nil
}
func (o *Outbox) prepareCandidate(ctx context.Context, tx *sql.Tx, doc *deliveryDoc, handlers map[string]*matcherHandler, now time.Time, unresolved *sql.Stmt) (_ *delivery, blocked bool, err error) {
	if doc.PayloadBytes > o.payloads.limit {
		return nil, false, o.quarantinePayload(ctx, doc, now, unresolved)
	}
	if !o.payloads.reserve(doc.ID, doc.PayloadBytes) {
		o.observeSkip(SkipPayloadBudget, 1)
		return nil, true, nil
	}
	keep := false
	defer func() {
		if !keep {
			o.releasePayload(doc)
		}
	}()
	if err = o.loadPayload(ctx, tx, doc); err != nil {
		return nil, false, err
	}
	if err = o.decodeDelivery(doc); err != nil {
		if _, e := unresolved.ExecContext(ctx, now, doc.ID); e != nil {
			return nil, false, e
		}
		o.observeSkip(SkipDecodeFailed, 1)
		o.sendError(ctx, fmt.Errorf("%w: delivery %s: %v", ErrUnresolvedHandler, doc.ID, err), nil)
		return nil, false, nil
	}
	mh := handlers[doc.HandlerType]
	if mh == nil || !mh.Match(doc.Event) {
		if _, e := unresolved.ExecContext(ctx, now, doc.ID); e != nil {
			return nil, false, e
		}
		o.observeSkip(SkipUnresolved, 1)
		o.sendError(ctx, fmt.Errorf("%w: delivery %s handler %q", ErrUnresolvedHandler, doc.ID, doc.HandlerType), payloadDiagnostic(doc))
		return nil, false, nil
	}
	key, handlerLabel, shardLabel := dispatchKeyFor(mh, doc.PartitionKey)
	d := newDelivery(doc, mh, key, handlerLabel, shardLabel)
	if !o.dispatch.tryReserve(d) {
		o.observeSkip(SkipQueueFull, 1)
		return nil, true, nil
	}
	if !o.admission.tryAdmitKey(doc.ID, key) {
		o.dispatch.releaseReserve(key)
		o.observeSkip(SkipAdmissionFull, 1)
		return nil, true, nil
	}
	keep = true
	return d, false, nil
}

// Buffered diagnostics must not retain another unbudgeted copy of a large
// event after finalize. The full record remains in the publication or DLQ.
func payloadDiagnostic(doc *deliveryDoc) eh.Event {
	event := doc.Event
	if event == nil || doc.PayloadBytes <= 64<<10 {
		return event
	}
	// Keep bounded scalar context such as correlation/routing, but no nested
	// objects or large strings. Clone strings so slices cannot pin a big buffer.
	var metadata map[string]any
	for key, value := range event.Metadata() {
		if len(metadata) >= 16 {
			break
		}
		if len(key) > 128 {
			continue
		}
		text, ok := value.(string)
		if !ok || len(text) > 256 {
			continue
		}
		if metadata == nil {
			metadata = make(map[string]any)
		}
		metadata[strings.Clone(key)] = strings.Clone(text)
	}
	return eh.NewEvent(event.EventType(), nil, event.Timestamp(), eh.ForAggregate(event.AggregateType(), event.AggregateID(), event.Version()), eh.WithMetadata(metadata))
}
