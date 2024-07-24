// Code generated by MockGen. DO NOT EDIT.
// Source: cache.go
//
// Generated by this command:
//
//	mockgen -source cache.go -destination ../../internal/mocks/mock_cache.go -package mocks cache
//

// Package mocks is a generated GoMock package.
package mocks

import (
	reflect "reflect"
	time "time"

	storage "github.com/openfga/openfga/pkg/storage"
	gomock "go.uber.org/mock/gomock"
)

// MockInMemoryCache is a mock of InMemoryCache interface.
type MockInMemoryCache[T any] struct {
	ctrl     *gomock.Controller
	recorder *MockInMemoryCacheMockRecorder[T]
}

// MockInMemoryCacheMockRecorder is the mock recorder for MockInMemoryCache.
type MockInMemoryCacheMockRecorder[T any] struct {
	mock *MockInMemoryCache[T]
}

// NewMockInMemoryCache creates a new mock instance.
func NewMockInMemoryCache[T any](ctrl *gomock.Controller) *MockInMemoryCache[T] {
	mock := &MockInMemoryCache[T]{ctrl: ctrl}
	mock.recorder = &MockInMemoryCacheMockRecorder[T]{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockInMemoryCache[T]) EXPECT() *MockInMemoryCacheMockRecorder[T] {
	return m.recorder
}

// Get mocks base method.
func (m *MockInMemoryCache[T]) Get(key string) (*storage.CachedResult[T], bool) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Get", key)
	ret0, _ := ret[0].(*storage.CachedResult[T])
	ret1, _ := ret[1].(bool)
	return ret0, ret1
}

// Get indicates an expected call of Get.
func (mr *MockInMemoryCacheMockRecorder[T]) Get(key any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Get", reflect.TypeOf((*MockInMemoryCache[T])(nil).Get), key)
}

// Set mocks base method.
func (m *MockInMemoryCache[T]) Set(key string, value T, ttl time.Duration) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Set", key, value, ttl)
}

// Set indicates an expected call of Set.
func (mr *MockInMemoryCacheMockRecorder[T]) Set(key, value, ttl any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Set", reflect.TypeOf((*MockInMemoryCache[T])(nil).Set), key, value, ttl)
}

// Stop mocks base method.
func (m *MockInMemoryCache[T]) Stop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Stop")
}

// Stop indicates an expected call of Stop.
func (mr *MockInMemoryCacheMockRecorder[T]) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockInMemoryCache[T])(nil).Stop))
}
