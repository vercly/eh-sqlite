package durable_test

import (
	"context"
	"testing"

	"github.com/vercly/eventhorizon/uuid"
)

// BenchmarkDurableCommandSuccess measures the persistence overhead of one
// successful command (task insert and status updates).
func BenchmarkDurableCommandSuccess(b *testing.B) {
	db := newDurableTestDB(b)
	wrapped := newWrappedHandler(b, db, &recordingCommandHandler{})
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := wrapped.HandleCommand(ctx, testCommand{ID: uuid.New()}); err != nil {
			b.Fatal(err)
		}
	}
}
