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

// Store writes value at key within the internal map. If the key already
// exists, its value is overwritten with the new value. If the key does
// not exist, it is created and value is assigned.
func (m *AtomicMap[K, V]) Store(key K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.m == nil {
		m.m = make(map[K]V)
	}

	m.m[key] = value
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

// Unwrap returns the internal map that backs the AtomicMap.
// A nil value may be returned if the AtomicMap has not yet
// been initialized, or Unwrap is called after a Clear call.
//
// Once the internal map has been unwrapped, methods on the
// AtomicMap instance are no longer safe.
func (m *AtomicMap[K, V]) Unwrap() map[K]V {
	return m.m
}
