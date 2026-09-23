package lease

import (
	"path/filepath"
	"testing"
	"time"
)

func newPair(t *testing.T, ttl time.Duration) (*Lease, *Lease) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runs.db")
	a, err := New(path, ttl)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	b, err := New(path, ttl)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return a, b
}

func TestMutualExclusionAcrossInstances(t *testing.T) {
	a, b := newPair(t, time.Minute)

	tok, ok := a.Acquire("x")
	if !ok {
		t.Fatal("first acquire should succeed")
	}
	if _, ok := b.Acquire("x"); ok {
		t.Fatal("second instance should not acquire a held lease")
	}
	a.Release(tok)
	if _, ok := b.Acquire("x"); !ok {
		t.Fatal("acquire after release should succeed")
	}
}

func TestStaleTokenDoesNotRelease(t *testing.T) {
	a, b := newPair(t, time.Minute)

	first, _ := a.Acquire("x")
	a.Release(first)

	second, _ := b.Acquire("x")
	a.Release(first) // stale: must be a no-op for the other owner
	if _, ok := a.Acquire("x"); ok {
		t.Fatal("stale token released another instance's lease")
	}
	b.Release(second)
}

func TestLeaseExpiry(t *testing.T) {
	a, b := newPair(t, 100*time.Millisecond)

	if _, ok := a.Acquire("x"); !ok {
		t.Fatal("acquire failed")
	}
	time.Sleep(300 * time.Millisecond) // well past the TTL
	if _, ok := b.Acquire("x"); !ok {
		t.Fatal("expired lease should be evicted and re-acquirable")
	}
}

func TestRenewExtendsLease(t *testing.T) {
	a, b := newPair(t, 300*time.Millisecond)

	tok, ok := a.Acquire("x")
	if !ok {
		t.Fatal("acquire failed")
	}
	time.Sleep(150 * time.Millisecond)
	a.Renew(tok) // expiry moves to ~450ms
	time.Sleep(225 * time.Millisecond)
	if _, ok := b.Acquire("x"); ok {
		t.Fatal("renewed lease was lost before its extended expiry")
	}
	a.Release(tok)
	if _, ok := b.Acquire("x"); !ok {
		t.Fatal("release should free the lease")
	}
}

func TestReleaseNilToken(t *testing.T) {
	a, _ := newPair(t, time.Minute)
	a.Release(nil) // must not panic
	a.Renew(nil)   // must not panic
}
