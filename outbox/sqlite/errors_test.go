package sqlite

import "testing"

func TestErrorSnapshotCountsSeverity(t *testing.T) {
	before := ErrorSnapshot()

	recordErrorSeverity(SeverityFatal)
	recordErrorSeverity(SeverityRetryable)
	recordErrorSeverity(SeverityUnknown)

	after := ErrorSnapshot()
	if after.Fatal-before.Fatal != 1 {
		t.Fatalf("fatal errors delta = %d, want 1", after.Fatal-before.Fatal)
	}
	if after.Retryable-before.Retryable != 1 {
		t.Fatalf("retryable errors delta = %d, want 1", after.Retryable-before.Retryable)
	}
	if after.Unknown-before.Unknown != 1 {
		t.Fatalf("unknown errors delta = %d, want 1", after.Unknown-before.Unknown)
	}
}
