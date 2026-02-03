package pipeline

import (
	"time"

	serverconfig "github.com/openfga/openfga/pkg/server/config"
)

type Option func(*serverconfig.PipelineConfig)

// WithBufferCapacity sets the capacity of pipes between workers.
// Must be a power of two. Larger buffers reduce blocking but increase memory.
func WithBufferCapacity(size int) Option {
	return func(config *serverconfig.PipelineConfig) {
		config.Buffer.Capacity = size
	}
}

// WithChunkSize sets how many items are batched before sending between workers.
// Larger chunks improve throughput but increase latency to first result.
func WithChunkSize(size int) Option {
	return func(config *serverconfig.PipelineConfig) {
		config.ChunkSize = size
	}
}

// WithNumProcs sets goroutines per worker for parallel message processing.
// Higher values improve throughput but increase scheduling overhead.
func WithNumProcs(num int) Option {
	return func(config *serverconfig.PipelineConfig) {
		config.NumProcs = num
	}
}

// WithPipeExtension enables dynamic buffer growth when pipes block.
// Each extension doubles capacity. Use -1 for maxExtensions to allow unbounded growth.
// Disabled by default; enable when workloads have unpredictable burst sizes.
func WithPipeExtension(extendAfter time.Duration, maxExtensions int) Option {
	return func(config *serverconfig.PipelineConfig) {
		config.Buffer.ExtendAfter = extendAfter
		config.Buffer.MaxExtensions = maxExtensions
	}
}

// WithConfig replaces the entire configuration.
func WithConfig(c serverconfig.PipelineConfig) Option {
	return func(config *serverconfig.PipelineConfig) {
		*config = c
	}
}
