package sqlite

import "testing"

func TestRecordAdmissionCapacityAndIdempotency(t *testing.T) {
	a := newRecordAdmission(2)

	if !a.tryAdmit("a") || !a.tryAdmit("b") {
		t.Fatal("first two admits should succeed")
	}
	if a.tryAdmit("c") {
		t.Fatal("admit beyond capacity should fail without blocking")
	}
	if a.tryAdmit("a") {
		t.Fatal("re-admit of active id should fail")
	}
	if a.freeSlots() != 0 {
		t.Fatalf("freeSlots = %d, want 0", a.freeSlots())
	}

	a.release("a")
	if a.freeSlots() != 1 {
		t.Fatalf("freeSlots after release = %d, want 1", a.freeSlots())
	}
	if !a.tryAdmit("c") {
		t.Fatal("admit after release should succeed")
	}
	if a.contains("a") {
		t.Fatal("released id should not remain in the set")
	}
	if !a.contains("b") || !a.contains("c") {
		t.Fatal("active ids should remain admitted")
	}
}

func TestRecordAdmissionNormalizeLimit(t *testing.T) {
	a := newRecordAdmission(0)
	if a.limit != 1 {
		t.Fatalf("limit = %d, want 1", a.limit)
	}
	if !a.tryAdmit("x") {
		t.Fatal("single-slot admit should work")
	}
	if a.tryAdmit("y") {
		t.Fatal("second admit should fail at limit 1")
	}
}
