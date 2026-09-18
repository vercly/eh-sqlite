package sqlite

import (
	"sort"
	"testing"
)

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

// TestRecordAdmissionTracksKeys covers the v2 addition: admission remembers the
// dispatch key of every admitted delivery so the claim query can exclude rows
// that are already active on that key.
func TestRecordAdmissionTracksKeys(t *testing.T) {
	a := newRecordAdmission(3)

	if !a.tryAdmitKey("d1", "k1") || !a.tryAdmitKey("d2", "k1") || !a.tryAdmitKey("d3", "k2") {
		t.Fatal("admits within capacity should succeed")
	}
	if used, limit := a.snapshot(); used != 3 || limit != 3 {
		t.Fatalf("snapshot = (%d, %d), want (3, 3)", used, limit)
	}
	if got, want := adSortedIDs(a.idsForKey("k1")), []string{"d1", "d2"}; !adEqual(got, want) {
		t.Fatalf("idsForKey(k1) = %v, want %v", got, want)
	}
	if got, want := adSortedIDs(a.idsForKey("k2")), []string{"d3"}; !adEqual(got, want) {
		t.Fatalf("idsForKey(k2) = %v, want %v", got, want)
	}
	if got := a.idsForKey("unknown"); len(got) != 0 {
		t.Fatalf("idsForKey(unknown) = %v, want empty", got)
	}
	if a.tryAdmitKey("d4", "k3") {
		t.Fatal("admit beyond capacity should fail even with a fresh key")
	}

	// release drops the id from its key bucket and frees a slot.
	a.release("d1")
	if got, want := adSortedIDs(a.idsForKey("k1")), []string{"d2"}; !adEqual(got, want) {
		t.Fatalf("idsForKey(k1) after release = %v, want %v", got, want)
	}
	if used, _ := a.snapshot(); used != 2 {
		t.Fatalf("used after release = %d, want 2", used)
	}

	// tryAdmit keeps the historical "unknown key" behaviour (empty key bucket).
	if !a.tryAdmit("d5") {
		t.Fatal("admit after release should succeed")
	}
	if got, want := adSortedIDs(a.idsForKey("")), []string{"d5"}; !adEqual(got, want) {
		t.Fatalf("idsForKey(\"\") = %v, want %v", got, want)
	}
	if got := adSortedIDs(a.idsForKey("k1")); !adEqual(got, []string{"d2"}) {
		t.Fatalf("keyless admit leaked into k1: %v", got)
	}

	a.release("d2")
	a.release("d3")
	a.release("d5")
	if used, limit := a.snapshot(); used != 0 || limit != 3 {
		t.Fatalf("snapshot after full release = (%d, %d), want (0, 3)", used, limit)
	}
	for _, key := range []string{"k1", "k2", ""} {
		if got := a.idsForKey(key); len(got) != 0 {
			t.Fatalf("idsForKey(%q) after full release = %v, want empty", key, got)
		}
	}
}

// adSortedIDs makes idsForKey (map iteration order) comparable.
func adSortedIDs(ids []string) []string {
	out := append([]string(nil), ids...)
	sort.Strings(out)
	return out
}

func adEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
