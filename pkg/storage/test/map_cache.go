package test

import (
	"strings"
	"sync"
	"time"

	"github.com/openfga/openfga/pkg/storage/cache/keys"
)

type MapCache struct {
	mu    sync.Mutex
	m     map[keys.Key]any
	calls int
	hits  int
	size  int
}

func NewMapCache() *MapCache {
	return &MapCache{
		m: make(map[keys.Key]any),
	}
}

func (m *MapCache) Get(key keys.Key) any {
	m.mu.Lock()
	defer m.mu.Unlock()

	value, ok := m.m[key]
	if ok {
		m.hits++
	}
	m.calls++
	return value
}

func (m *MapCache) Calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.calls
}

func (m *MapCache) Hits() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.hits
}

func (m *MapCache) Set(key keys.Key, value any, _ time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.m[key]
	m.m[key] = value

	if !ok {
		m.size++
	}
}

func (m *MapCache) Delete(key keys.Key) {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.m[key]
	delete(m.m, key)

	if ok {
		m.size--
	}
}

func (m *MapCache) Stop() {}

// Size returns the number of entries in the cache.
func (m *MapCache) Size() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.size
}

// KeysWithPrefix returns all keys whose hex-encoded representation starts
// with the given prefix string.
func (m *MapCache) KeysWithPrefix(prefix string) []keys.Key {
	var kb keys.Builder
	kb.EncodeString(prefix)
	prefix = kb.Key().String()

	m.mu.Lock()
	defer m.mu.Unlock()
	var result []keys.Key
	for k := range m.m {
		if strings.HasPrefix(k.String(), prefix) {
			result = append(result, k)
		}
	}
	return result
}
