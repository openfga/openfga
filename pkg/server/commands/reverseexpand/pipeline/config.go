package pipeline

import (
	"time"

	"github.com/openfga/openfga/internal/pipe"
)

type Option func(*Config)

// WithBufferCapacity sets the capacity of pipes between workers.
// Must be a power of two. Larger buffers reduce blocking but increase memory.
func WithBufferCapacity(size int) Option {
	return func(config *Config) {
		config.BufferConfig.Capacity = size
	}
}

// WithChunkSize sets how many items are batched before sending between workers.
// Larger chunks improve throughput but increase latency to first result.
func WithChunkSize(size int) Option {
	return func(config *Config) {
		config.ChunkSize = size
	}
}

// WithNumProcs sets goroutines per worker for parallel message processing.
// Higher values improve throughput but increase scheduling overhead.
func WithNumProcs(num int) Option {
	return func(config *Config) {
		config.NumProcs = num
	}
}

// WithPipeExtension enables dynamic buffer growth when pipes block.
// Each extension doubles capacity. Use -1 for maxExtensions to allow unbounded growth.
// Disabled by default; enable when workloads have unpredictable burst sizes.
func WithPipeExtension(extendAfter time.Duration, maxExtensions int) Option {
	return func(config *Config) {
		config.BufferConfig.ExtendAfter = extendAfter
		config.BufferConfig.MaxExtensions = maxExtensions
	}
}

// WithConfig replaces the entire configuration.
func WithConfig(c Config) Option {
	return func(config *Config) {
		*config = c
	}
}

// Config contains pipeline tuning parameters.
type Config struct {
	BufferConfig pipe.Config
	ChunkSize    int
	NumProcs     int
}

// DefaultConfig returns a balanced configuration suitable for most workloads.
func DefaultConfig() Config {
	var config Config
	config.BufferConfig = pipe.DefaultConfig()
	config.BufferConfig.Capacity = defaultBufferSize
	config.ChunkSize = defaultChunkSize
	config.NumProcs = defaultNumProcs
	return config
}

func (config *Config) Validate() error {
	if err := config.BufferConfig.Validate(); err != nil {
		return err
	}

	if config.ChunkSize < 1 {
		return ErrInvalidChunkSize
	}

	if config.NumProcs < 1 {
		return ErrInvalidNumProcs
	}
	return nil
}
