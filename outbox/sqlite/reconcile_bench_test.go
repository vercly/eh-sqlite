package sqlite

import (
	"context"
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
