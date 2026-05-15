package containers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAtomicMap_Store(t *testing.T) {
	var m AtomicMap[string, int]

	m.Store("a", 1)

	v, ok := m.LoadOrStore("a", 0)
	require.True(t, ok)
	assert.Equal(t, 1, v)
}

func TestAtomicMap_Store_OverwritesExisting(t *testing.T) {
	var m AtomicMap[string, int]

	m.Store("a", 1)
	m.Store("a", 2)

	v, ok := m.LoadOrStore("a", 0)
	require.True(t, ok)
	assert.Equal(t, 2, v)
}

func TestAtomicMap_LoadOrStore_KeyAbsent(t *testing.T) {
	var m AtomicMap[string, int]

	v, loaded := m.LoadOrStore("x", 42)

	assert.False(t, loaded)
	assert.Equal(t, 42, v)

	// Subsequent load should find the stored value.
	v, loaded = m.LoadOrStore("x", 99)
	assert.True(t, loaded)
	assert.Equal(t, 42, v)
}

func TestAtomicMap_LoadOrStore_KeyPresent(t *testing.T) {
	var m AtomicMap[string, int]

	m.Store("k", 10)

	v, loaded := m.LoadOrStore("k", 20)

	assert.True(t, loaded)
	assert.Equal(t, 10, v)
}

func TestAtomicMap_Clear(t *testing.T) {
	var m AtomicMap[string, int]

	m.Store("a", 1)
	m.Store("b", 2)
	m.Clear()

	assert.Nil(t, m.Unwrap())
}

func TestAtomicMap_Clear_ThenReuse(t *testing.T) {
	var m AtomicMap[string, int]

	m.Store("a", 1)
	m.Clear()

	// Map should be usable again after Clear.
	m.Store("b", 2)
	v, loaded := m.LoadOrStore("b", 0)
	assert.True(t, loaded)
	assert.Equal(t, 2, v)
}

func TestAtomicMap_Unwrap_BeforeUse(t *testing.T) {
	var m AtomicMap[string, int]

	assert.Nil(t, m.Unwrap())
}

func TestAtomicMap_Unwrap_AfterStore(t *testing.T) {
	var m AtomicMap[string, int]

	m.Store("a", 1)
	m.Store("b", 2)

	raw := m.Unwrap()
	assert.Equal(t, map[string]int{"a": 1, "b": 2}, raw)
}
