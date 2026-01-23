package pipeline

import (
	"time"

	"github.com/openfga/openfga/internal/pipe"
)

type Option func(*Config)

// WithBufferSize is a function that sets the value for a pipeline's workers' internal
// pipe buffer size. The value must be a power of two. (e.g. 1, 2, 4, 8, 16, 32...)
// The default value of the buffer size is 128. If an invalid value is provided as size
// then the default value will be applied.
func WithBufferSize(size int) Option {
	return func(config *Config) {
		config.BufferConfig.Capacity = size
	}
}

// WithChunkSize configures the batch size for tuple processing.
// Larger chunks reduce message overhead but increase latency and memory usage.
// The default of 100 balances throughput and responsiveness for most workloads.
func WithChunkSize(size int) Option {
	return func(config *Config) {
		config.ChunkSize = size
	}
}

// WithNumProcs sets the number of goroutines spawned per worker to process messages.
// Higher values increase parallelism within each worker but consume more system resources.
// The default of 4 provides good parallelism without excessive goroutine overhead.
func WithNumProcs(num int) Option {
	return func(config *Config) {
		config.NumProcs = num
	}
}

// WithPipeExtension enables functionality to dynamically extend the size of pipes
// between workers as needed up to a defined number of times. Each extension doubles
// the size of a pipe's internal buffer. A pipe is only extended if a call to send on
// the pipe blocks for longer than the extendAfter duration.
//
// This functionality is disabled by default. To enable, set extendAfter to a duration
// greater than 0 and set maxExtensions to a value greater than 0, or to -1 to
// make the number of extensions unbounded.
func WithPipeExtension(extendAfter time.Duration, maxExtensions int) Option {
	return func(config *Config) {
		config.BufferConfig.ExtendAfter = extendAfter
		config.BufferConfig.MaxExtensions = maxExtensions
	}
}

// WithConfig replaces the entire configuration.
// Use this when you have a pre-configured Config struct from another source.
// Prefer the specific With* functions for clearer intent.
func WithConfig(c Config) Option {
	return func(config *Config) {
		*config = c
	}
}

// Config contains pipeline tuning parameters.
type Config struct {
	// BufferConfig is a value that contains configuration values for the pipes
	// that exist between pipeline workers.
	BufferConfig pipe.Config

	// ChunkSize is a value that indicates the maximum size of tuples
	// accumulated from a data store query before sending the tuples
	// as a message to the next node in the pipeline.
	//
	// As an example, if the chunkSize is set to 100, then a new message
	// is sent for every 100 tuples returned from a data store query.
	ChunkSize int

	// NumProcs is a value that indicates the maximum number of goroutines
	// that will be allocated to processing each subscription for a pipeline
	// worker.
	NumProcs int
}

// DefaultConfig returns a configuration tuned for general-purpose workloads.
// Buffer size of 128 provides good throughput without excessive memory per connection.
// Chunk size of 100 balances message overhead against processing latency.
// NumProcs of 4 exploits common CPU core counts without goroutine proliferation.
func DefaultConfig() Config {
	var config Config
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
