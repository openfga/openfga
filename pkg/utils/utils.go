package utils

import (
	"context"
	"sync"

	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/encrypter"
	"github.com/openfga/openfga/pkg/logger"
)

// DBCallCounter allows counting the numbers of times database are read / write
type DBCallCounter interface {
	// AddReadCall increments number of times database are read
	AddReadCall()
	// AddWriteCall increments number of times database are written
	AddWriteCall()
	// GetReadCalls returns the number of times database are read
	GetReadCalls() uint32
	// GetWriteCalls returns the number of times database are written
	GetWriteCalls() uint32
}

// NewDBCallCounter return new database calls counter
func NewDBCallCounter() *dbCallCounter {
	return &dbCallCounter{}
}

type dbCallCounter struct {
	mu         sync.Mutex
	readCalls  uint32
	writeCalls uint32
}

// AddReadCall increments the number of times database is read from
func (r *dbCallCounter) AddReadCall() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.readCalls++
}

// AddWriteCall increments the number of times the database is written to
func (r *dbCallCounter) AddWriteCall() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writeCalls++
}

// GetReadCalls returns the number of times database has been read
func (r *dbCallCounter) GetReadCalls() uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.readCalls
}

// GetWriteCalls returns the number of times database has been written to
func (r *dbCallCounter) GetWriteCalls() uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.writeCalls
}

// ResolutionMetadata stores the number of times database relationship are resolved. It also returns number of times
// the database is read from and written to.
type ResolutionMetadata struct {
	mu             sync.Mutex
	resolveCalls   uint32
	dbCallsCounter DBCallCounter
}

// NewResolutionMetadata will return a new resolution metadata
func NewResolutionMetadata() *ResolutionMetadata {
	return &ResolutionMetadata{
		resolveCalls:   0,
		dbCallsCounter: NewDBCallCounter(),
	}
}

// AddResolve increments the number of times database are resolved for relationship
func (r *ResolutionMetadata) AddResolve() uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resolveCalls++
	return r.resolveCalls
}

// AddReadCall increments number of times database are read
func (r *ResolutionMetadata) AddReadCall() {
	r.dbCallsCounter.AddReadCall()
}

// AddWriteCall increments number of times database are written
func (r *ResolutionMetadata) AddWriteCall() {
	r.dbCallsCounter.AddWriteCall()
}

// GetResolve returns the number of times database relationship are resolved
func (r *ResolutionMetadata) GetResolve() uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.resolveCalls
}

// GetReadCalls returns the number of times database are read
func (r *ResolutionMetadata) GetReadCalls() uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dbCallsCounter.GetReadCalls()
}

// GetWriteCalls returns the number of times database are written
func (r *ResolutionMetadata) GetWriteCalls() uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dbCallsCounter.GetWriteCalls()
}

// Fork will create a new set of resolveCalls but share the same dbCall
func (r *ResolutionMetadata) Fork() *ResolutionMetadata {
	r.mu.Lock()
	defer r.mu.Unlock()

	return &ResolutionMetadata{resolveCalls: r.resolveCalls, dbCallsCounter: r.dbCallsCounter}
}

// LogDBStats will call the logger to log the number of reads and writes for the feature
func LogDBStats(ctx context.Context, log logger.Logger, method string, reads uint32, writes uint32) {
	log.InfoWithContext(ctx, "db_stats", logger.String("method", method), logger.Uint32("reads", reads), logger.Uint32("writes", writes))
}

func DecodeAndDecrypt(encrypter encrypter.Encrypter, encoder encoder.Encoder, s string) ([]byte, error) {
	decoded, err := encoder.Decode(s)
	if err != nil {
		return nil, err
	}

	decrypted, err := encrypter.Decrypt(decoded)
	if err != nil {
		return nil, err
	}

	return decrypted, nil
}

func EncryptAndEncode(encrypter encrypter.Encrypter, encoder encoder.Encoder, data []byte) (string, error) {
	encrypted, err := encrypter.Encrypt(data)
	if err != nil {
		return "", err
	}

	encoded, err := encoder.Encode(encrypted)
	if err != nil {
		return "", err
	}

	return encoded, nil
}
