package pipeline

type Option func(*Config)

// WithBufferCapacity sets the capacity of pipes between workers.
// A value of 0 creates unbuffered channels. Larger buffers reduce
// blocking but increase memory.
func WithBufferCapacity(size int) Option {
	return func(config *Config) {
		config.BufferCapacity = size
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

// WithConfig replaces the entire configuration.
func WithConfig(c Config) Option {
	return func(config *Config) {
		*config = c
	}
}

// Config contains pipeline tuning parameters.
type Config struct {
	BufferCapacity int
	ChunkSize      int
	NumProcs       int
}

// DefaultConfig returns a balanced configuration suitable for most workloads.
func DefaultConfig() Config {
	var config Config
	config.BufferCapacity = defaultBufferSize
	config.ChunkSize = defaultChunkSize
	config.NumProcs = defaultNumProcs
	return config
}

func (config *Config) Validate() error {
	if config.BufferCapacity < 0 {
		return ErrInvalidBufferCapacity
	}

	if config.ChunkSize < 1 {
		return ErrInvalidChunkSize
	}

	if config.NumProcs < 1 {
		return ErrInvalidNumProcs
	}
	return nil
}
