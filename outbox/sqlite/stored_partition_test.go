package sqlite

import (
	"fmt"
	"github.com/vercly/eventhorizon/uuid"
	"testing"
)

func TestStoredEventPartitionKeyDoesNotRequireDataRegistration(t *testing.T) {
	aggregate := uuid.New().String()
	key, err := StoredEventPartitionKey([]byte(fmt.Sprintf(`{
		"event_type":"unregistered.native.type", "data":{"domain":"unavailable"},
		"aggregate_id":%q,
		"metadata":{"correlation_id":"fallback"}
	}`, aggregate)))
	if err != nil || key != aggregate {
		t.Fatalf("partition = %q, err = %v; want aggregate %q", key, err, aggregate)
	}
}

func TestStoredEventPartitionKeyMetadataFallback(t *testing.T) {
	for _, tc := range []struct {
		blob string
		want string
	}{
		{`{"aggregate_id":"00000000-0000-0000-0000-000000000000","metadata":{"correlation_id":" first ","CorrelationId":"second"}}`, "first"},
		{`{"aggregate_id":"invalid","metadata":{"correlation_id":" ","x-correlation-id":"last"}}`, "last"},
		{`{"metadata":{"CorrelationId":123}}`, "123"},
		{`{"metadata":{}}`, ""},
	} {
		key, err := StoredEventPartitionKey([]byte(tc.blob))
		if err != nil || key != tc.want {
			t.Fatalf("partition for %s = %q, %v; want %q", tc.blob, key, err, tc.want)
		}
	}
	if _, err := StoredEventPartitionKey([]byte(`{"broken"`)); err == nil {
		t.Fatal("malformed envelope accepted")
	}
}
