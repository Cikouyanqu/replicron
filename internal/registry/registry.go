// Package registry provides per-task mutual exclusion for concurrent runs.
package registry

import "sync"

// Token identifies a lease acquired on a task name. Only the lease holder
// can release it; a stale token from an earlier run must never unlock the
// current holder.
type Token struct {
	Name string
	Gen  uint64
}

// Registry is an in-process, single-instance mutex map keyed by task name.
// Multi-instance locking (e.g. via a database lease table) is on the roadmap.
type Registry struct {
	mu      sync.Mutex
	running map[string]*Token
	gen     uint64
}

func New() *Registry {
	return &Registry{running: make(map[string]*Token)}
}

// Acquire returns a lease token, or false when the task already has an
// active run.
func (r *Registry) Acquire(name string) (*Token, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, held := r.running[name]; held {
		return nil, false
	}
	r.gen++
	t := &Token{Name: name, Gen: r.gen}
	r.running[name] = t
	return t, true
}

// Release drops the lease only if token still owns it.
func (r *Registry) Release(t *Token) {
	if t == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if cur := r.running[t.Name]; cur == t {
		delete(r.running, t.Name)
	}
}
