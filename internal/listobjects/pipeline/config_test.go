package pipeline_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/internal/listobjects/pipeline"
)

func TestDefaultConfig(t *testing.T) {
	config := pipeline.DefaultConfig()

	assert.Positive(t, config.BufferCapacity, "BufferCapacity should be positive")
	assert.Positive(t, config.ChunkSize, "ChunkSize should be positive")
	assert.Positive(t, config.NumProcs, "NumProcs should be positive")
}

func TestDefaultConfig_IsValid(t *testing.T) {
	config := pipeline.DefaultConfig()
	require.NoError(t, config.Validate())
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  pipeline.Config
		wantErr error
	}{
		{
			name:    "valid config",
			config:  pipeline.Config{BufferCapacity: 1, ChunkSize: 1, NumProcs: 1},
			wantErr: nil,
		},
		{
			name:    "zero buffer capacity is valid",
			config:  pipeline.Config{BufferCapacity: 0, ChunkSize: 1, NumProcs: 1},
			wantErr: nil,
		},
		{
			name:    "negative buffer capacity",
			config:  pipeline.Config{BufferCapacity: -1, ChunkSize: 1, NumProcs: 1},
			wantErr: pipeline.ErrInvalidBufferCapacity,
		},
		{
			name:    "zero chunk size",
			config:  pipeline.Config{BufferCapacity: 0, ChunkSize: 0, NumProcs: 1},
			wantErr: pipeline.ErrInvalidChunkSize,
		},
		{
			name:    "negative chunk size",
			config:  pipeline.Config{BufferCapacity: 0, ChunkSize: -1, NumProcs: 1},
			wantErr: pipeline.ErrInvalidChunkSize,
		},
		{
			name:    "zero num procs",
			config:  pipeline.Config{BufferCapacity: 0, ChunkSize: 1, NumProcs: 0},
			wantErr: pipeline.ErrInvalidNumProcs,
		},
		{
			name:    "negative num procs",
			config:  pipeline.Config{BufferCapacity: 0, ChunkSize: 1, NumProcs: -1},
			wantErr: pipeline.ErrInvalidNumProcs,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.config.Validate()
			if tc.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.wantErr)
			}
		})
	}
}

func TestWithBufferCapacity(t *testing.T) {
	config := pipeline.DefaultConfig()
	pipeline.WithBufferCapacity(42)(&config)
	assert.Equal(t, 42, config.BufferCapacity)
}

func TestWithChunkSize(t *testing.T) {
	config := pipeline.DefaultConfig()
	pipeline.WithChunkSize(50)(&config)
	assert.Equal(t, 50, config.ChunkSize)
}

func TestWithNumProcs(t *testing.T) {
	config := pipeline.DefaultConfig()
	pipeline.WithNumProcs(8)(&config)
	assert.Equal(t, 8, config.NumProcs)
}

func TestWithConfig(t *testing.T) {
	config := pipeline.DefaultConfig()
	custom := pipeline.Config{BufferCapacity: 10, ChunkSize: 5, NumProcs: 2}
	pipeline.WithConfig(custom)(&config)
	assert.Equal(t, custom, config)
}

func TestWithConfig_InvalidConfig_ReturnsError(t *testing.T) {
	invalid := pipeline.Config{BufferCapacity: -1, ChunkSize: 0, NumProcs: 0}
	pl := pipeline.New(nil, nil, pipeline.WithConfig(invalid))
	_, err := pl.Expand(context.Background(), pipeline.Spec{ObjectType: "x", Relation: "y", User: "u:1"})
	// Validate runs before resolveObjectNode, so we get config error first.
	require.ErrorIs(t, err, pipeline.ErrInvalidBufferCapacity)
}
