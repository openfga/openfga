package test

import (
	"sync"
	"time"
)

type MapCache struct {
	mu    sync.Mutex
	m     map[string]any
	calls int
	hits  int
	size  int
}

func NewMapCache() *MapCache {
	return &MapCache{
		m: make(map[string]any),
	}
}

func (m *MapCache) Get(key string) any {
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

func (m *MapCache) Set(key string, value any, _ time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.m[key]
	m.m[key] = value

	if !ok {
		m.size++
	}
}

func (m *MapCache) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.m[key]
	delete(m.m, key)

	if ok {
		m.size--
	}
}

func (m *MapCache) Stop() {}
