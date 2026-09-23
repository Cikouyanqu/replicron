package registry

import "testing"

func TestAcquireRelease(t *testing.T) {
	r := New()

	tok, ok := r.Acquire("a")
	if !ok || tok == nil {
		t.Fatal("first acquire should succeed")
	}
	if _, ok := r.Acquire("a"); ok {
		t.Fatal("second acquire of same name should fail")
	}
	r.Release(tok)
	if _, ok := r.Acquire("a"); !ok {
		t.Fatal("acquire after release should succeed")
	}
}

// TestStaleTokenDoesNotRelease is the core safety property: a token from an
// earlier lease must never release the current holder's lease. Releasing an
// unheld lease used to let concurrent runs of the same task double-write.
func TestStaleTokenDoesNotRelease(t *testing.T) {
	r := New()
	first, _ := r.Acquire("a")
	r.Release(first)

	second, _ := r.Acquire("a")
	r.Release(first) // stale token: must be a no-op

	if _, ok := r.Acquire("a"); ok {
		t.Fatal("stale token released the current lease")
	}
	r.Release(second)
}

func TestReleaseNil(t *testing.T) {
	New().Release(nil) // must not panic
}

func TestIndependentNames(t *testing.T) {
	r := New()
	if _, ok := r.Acquire("a"); !ok {
		t.Fatal("acquire a failed")
	}
	if _, ok := r.Acquire("b"); !ok {
		t.Fatal("acquire b should be independent of a")
	}
}
