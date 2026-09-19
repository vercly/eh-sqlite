package sqlite

import (
	"encoding/json"
	"fmt"
)

// StoredEventPartitionKey reads only the routing envelope from EventCodec JSON.
// Offline migration and replay must not require the application's event-data
// factories to be registered merely to preserve a delivery's partition.
func StoredEventPartitionKey(blob []byte) (string, error) {
	return StoredEventPartitionKeyWith(blob, nil)
}

// StoredEventPartitionKeyWith is StoredEventPartitionKey with the same
// resolver an Outbox would use (WithPartitionKeyResolver), so offline tools
// and the live path agree on the shard.
func StoredEventPartitionKeyWith(blob []byte, resolver PartitionKeyResolver) (string, error) {
	var envelope storedEnvelope
	if err := json.Unmarshal(blob, &envelope); err != nil {
		return "", fmt.Errorf("decode stored event partition: %w", err)
	}
	return (&Outbox{partitionKey: resolver}).partitionKeyFor(envelope.Metadata, envelope.AggregateID), nil
}

type storedEnvelope struct {
	AggregateID string         `json:"aggregate_id"`
	Metadata    map[string]any `json:"metadata"`
}

// storedEnvelopeMetadata reads only the metadata of a stored event blob.
func storedEnvelopeMetadata(blob []byte) (map[string]any, error) {
	var envelope storedEnvelope
	if err := json.Unmarshal(blob, &envelope); err != nil {
		return nil, fmt.Errorf("decode stored event envelope: %w", err)
	}
	return envelope.Metadata, nil
}
