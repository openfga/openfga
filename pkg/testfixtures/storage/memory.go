package storage

import (
	"testing"
)

type memoryTest[T any] struct{}

// NewMemoryTestEngine constructs an implementation of the DatastoreTestEngine interface
// for the in memory datastore engine.
func NewMemoryTestEngine[T any]() *memoryTest[T] {
	return &memoryTest[T]{}
}

func (m *memoryTest[T]) NewDatabase(t testing.TB) string {
	return ""
}

func (m *memoryTest[T]) NewDatastore(t testing.TB, init DatastoreInitFunc[T]) T {
	return init("memory", "")
}

// RunMemoryTestEngine returns a bootstrapped datastore test engine for the in memory driver.
func RunMemoryTestEngine[T any](t testing.TB) DatastoreTestEngine[T] {
	return &memoryTest[T]{}
}
