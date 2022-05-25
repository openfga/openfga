package storage

import (
	"testing"
)

type memoryTest[T any] struct{}

func NewMemoryTester[T any]() *memoryTest[T] {
	return &memoryTest[T]{}
}

func (m *memoryTest[T]) NewDatabase(t testing.TB) string {
	return ""
}

func (m *memoryTest[T]) NewDatastore(t testing.TB, initFunc InitFunc[T]) T {
	return initFunc("memory", "")
}

// RunMemoryForTesting returns a RunningEngineForTest for the in-memory driver.
func RunMemoryForTesting[T any](t testing.TB) RunningEngineForTest[T] {
	return &memoryTest[T]{}
}
