package containers

import "sync"

// AtomicMap is a mutex-protected map safe for concurrent use. It serves
// as a lighter alternative to [sync.Map] for workloads where the extra
// per-element allocation that sync.Map requires is undesirable.
//
// The zero value is ready to use; the underlying map is allocated lazily
// on the first call to [AtomicMap.LoadOrStore].
type AtomicMap[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]V
}

// LoadOrStore returns the existing value for key if present.
// Otherwise, it stores and returns the given value.
// The boolean result is true if the value was loaded, false if stored.
func (m *AtomicMap[K, V]) LoadOrStore(key K, value V) (V, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.m == nil {
		m.m = make(map[K]V)
	}

	v, ok := m.m[key]
	if !ok {
		m.m[key] = value
		return value, ok
	}
	return v, ok
}

// Clear removes all elements from the map.
func (m *AtomicMap[K, V]) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.m = nil
}
