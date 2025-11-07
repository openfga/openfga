package utils

import (
	"sync"
)

// TypedSyncMap is a generic wrapper for sync.Map to reduce type assertions in code.
type TypedSyncMap[K comparable, V any] struct {
	m sync.Map
}

// Store sets the value for a key.
func (tm *TypedSyncMap[K, V]) Store(key K, value V) {
	tm.m.Store(key, value)
}

// Load returns the value stored in the map for a key, or the zero value of V if not present.
// The ok result indicates whether value was found.
func (tm *TypedSyncMap[K, V]) Load(key K) (value V, ok bool) {
	v, loaded := tm.m.Load(key)
	if !loaded {
		return value, false // return zero value of V and false
	}

	// Perform the type assertion internally
	actualValue, ok := v.(V)
	if !ok {
		// Handle unexpected type (though this shouldn't happen if usage is correct)
		return value, false
	}
	return actualValue, true
}

// Delete deletes the value for a key.
func (tm *TypedSyncMap[K, V]) Delete(key K) {
	tm.m.Delete(key)
}
