package utils

import (
	"sync"
)

// ResolutionMetadata stores the number of times database relationship are resolved. It also returns number of times
// the database is read from and written to.
type ResolutionMetadata struct {
	mu           sync.Mutex
	resolveCalls uint32
}

// NewResolutionMetadata will return a new resolution metadata
func NewResolutionMetadata() *ResolutionMetadata {
	return &ResolutionMetadata{
		resolveCalls: 0,
	}
}

// AddResolve increments the number of times database are resolved for relationship
func (r *ResolutionMetadata) AddResolve() uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resolveCalls++
	return r.resolveCalls
}

// GetResolve returns the number of times database relationship are resolved
func (r *ResolutionMetadata) GetResolve() uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.resolveCalls
}

// Fork will create a new set of resolveCalls but share the same dbCall
func (r *ResolutionMetadata) Fork() *ResolutionMetadata {
	r.mu.Lock()
	defer r.mu.Unlock()

	return &ResolutionMetadata{resolveCalls: r.resolveCalls}
}
