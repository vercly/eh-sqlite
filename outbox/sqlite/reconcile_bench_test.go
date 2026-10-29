package sqlite

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	eh "github.com/vercly/eventhorizon"
	"github.com/vercly/eventhorizon/mocks"
	"github.com/vercly/eventhorizon/uuid"
)

// BenchmarkReconcilePartitionKeys measures the startup partition-key pass over
// a backlog of publications with sizable payloads.
func BenchmarkReconcilePartitionKeys(b *testing.B) {
	const (
		backlog     = 2000
		payloadSize = 32 << 10
	)
	db := newTestDB(b)
	o, err := NewOutbox(db)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = o.Close() })
	content := strings.Repeat("x", payloadSize)
	for range backlog {
		event := eh.NewEvent(mocks.EventType, &mocks.EventData{Content: content}, time.Now(),
			eh.ForAggregate(mocks.AggregateType, uuid.New(), 1),
			eh.WithMetadata(map[string]any{"correlation_id": uuid.New().String()}))
		insertDeliveryDirect(b, o, deliverySeed{Event: event, HandlerType: "h", CreatedAt: time.Now()})
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		tx, err := o.beginWriteTx(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if err := o.reconcilePartitionKeys(ctx, tx); err != nil {
			b.Fatal(err)
		}
		rollbackTx(tx)
	}
}

// legacyPartitionKey mirrors the vercly legacy Rabbit resolver: legacy
// envelopes partition by their raw correlation, everything else defers.
func legacyPartitionKey(metadata map[string]any, _ string) (string, bool) {
	if _, ok := metadata["legacy.body_base64"]; !ok {
		return "", false
	}
	if raw, ok := metadata["legacy.correlation_id"].(string); ok {
		return strings.TrimSpace(raw), true
	}
	return "", true
}

// BenchmarkReconcilePartitionKeysLegacy measures the startup partition-key
// pass over legacy Rabbit envelopes: no event data, the message body carried
// as base64 metadata. "explicit" rows carry MetadataPartitionKey as the
// current bridge writes them; "resolver" rows predate it.
func BenchmarkReconcilePartitionKeysLegacy(b *testing.B) {
	const (
		backlog  = 2000
		bodySize = 32 << 10
	)
	body := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", bodySize)))
	for _, explicit := range []bool{true, false} {
		name := "resolver"
		if explicit {
			name = "explicit"
		}
		b.Run(name, func(b *testing.B) {
			db := newTestDB(b)
			o, err := NewOutbox(db, WithPartitionKeyResolver(legacyPartitionKey))
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = o.Close() })
			for range backlog {
				correlation := uuid.New().String()
				metadata := map[string]any{
					"legacy.type":           "legacy.Message",
					"legacy.body_base64":    body,
					"legacy.correlation_id": correlation,
				}
				if explicit {
					metadata[MetadataPartitionKey] = correlation
				}
				event := eh.NewEvent("legacy.Message", nil, time.Now(),
					eh.ForAggregate("legacy-message", uuid.New(), 1), eh.WithMetadata(metadata))
				insertDeliveryDirect(b, o, deliverySeed{Event: event, HandlerType: "h", CreatedAt: time.Now()})
			}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				tx, err := o.beginWriteTx(ctx)
				if err != nil {
					b.Fatal(err)
				}
				if err := o.reconcilePartitionKeys(ctx, tx); err != nil {
					b.Fatal(err)
				}
				rollbackTx(tx)
			}
		})
	}
}
