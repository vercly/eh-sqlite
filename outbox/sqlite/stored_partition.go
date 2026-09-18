package sqlite

import (
	"encoding/json"
	"fmt"

	"github.com/vercly/eventhorizon/uuid"
)

// StoredEventPartitionKey reads only the routing envelope from EventCodec JSON.
// Offline migration and replay must not require the application's event-data
// factories to be registered merely to preserve a delivery's partition.
func StoredEventPartitionKey(blob []byte) (string, error) {
	var envelope struct {
		AggregateID string         `json:"aggregate_id"`
		Metadata    map[string]any `json:"metadata"`
	}
	if err := json.Unmarshal(blob, &envelope); err != nil {
		return "", fmt.Errorf("decode stored event partition: %w", err)
	}
	if id, err := uuid.Parse(envelope.AggregateID); err == nil && id != uuid.Nil {
		return id.String(), nil
	}
	for _, key := range []string{"correlation_id", "CorrelationId", "correlationId", "x-correlation-id"} {
		if value, ok := envelope.Metadata[key]; ok {
			if partition := metadataPartitionKey(value); partition != "" {
				return partition, nil
			}
		}
	}
	return "", nil
}
