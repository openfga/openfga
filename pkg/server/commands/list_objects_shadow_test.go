package commands

import (
	"context"
	"errors"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/storage"
	"testing"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock implementations
type mockTupleReader struct {
	storage.RelationshipTupleReader
}
type mockCheckResolver struct{ graph.CheckResolver }

func TestNewShadowedListObjectsQuery(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		noopLogger := logger.NewNoopLogger()
		result, err := newShadowedListObjectsQuery(&mockTupleReader{}, &mockCheckResolver{}, NewShadowListObjectsQueryConfig(
			WithShadowListObjectsQuerySamplePercentage(13),
		), WithListObjectsOptimizationEnabled(true))
		require.NoError(t, err)
		require.NotNil(t, result)
		query := result.(*shadowedListObjectsQuery)
		assert.False(t, query.standard.(*listObjectsQuery).listObjectsOptimizationEnabled)
		assert.True(t, query.optimized.(*listObjectsQuery).listObjectsOptimizationEnabled)
		assert.Equal(t, noopLogger, query.logger)
	})

	t.Run("ds error", func(t *testing.T) {
		result, err := newShadowedListObjectsQuery(nil, &mockCheckResolver{}, NewShadowListObjectsQueryConfig())
		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("check resolver error", func(t *testing.T) {
		result, err := newShadowedListObjectsQuery(&mockTupleReader{}, nil, NewShadowListObjectsQueryConfig())
		require.Error(t, err)
		require.Nil(t, result)
	})
}

type mockListObjectsQuery struct {
	executeFunc         func(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error)
	executeStreamedFunc func(ctx context.Context, req *openfgav1.StreamedListObjectsRequest, srv openfgav1.OpenFGAService_StreamedListObjectsServer) (*ListObjectsResolutionMetadata, error)
}

func (m *mockListObjectsQuery) Execute(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error) {
	return m.executeFunc(ctx, req)
}
func (m *mockListObjectsQuery) ExecuteStreamed(ctx context.Context, req *openfgav1.StreamedListObjectsRequest, srv openfgav1.OpenFGAService_StreamedListObjectsServer) (*ListObjectsResolutionMetadata, error) {
	return m.executeStreamedFunc(ctx, req, srv)
}

func TestShadowedListObjectsQuery_Execute(t *testing.T) {
	ctx := context.WithValue(context.Background(), "list-objects-optimization", true)
	req := &openfgav1.ListObjectsRequest{}
	expected := &ListObjectsResponse{Objects: []string{"foo"}}
	expectedOpt := &ListObjectsResponse{Objects: []string{"foo"}}

	tests := []struct {
		name            string
		standardErr     error
		optimizedErr    error
		standardResult  *ListObjectsResponse
		optimizedResult *ListObjectsResponse
		expectErr       bool
		expectResult    *ListObjectsResponse
	}{
		{
			name:            "both succeed, equal results",
			standardResult:  expected,
			optimizedResult: expectedOpt,
			expectResult:    expected,
		},
		{
			name:            "standard fails",
			standardErr:     errors.New("fail"),
			optimizedResult: expectedOpt,
			expectErr:       true,
		},
		{
			name:           "optimized fails",
			standardResult: expected,
			optimizedErr:   errors.New("fail"),
			expectResult:   expected,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &shadowedListObjectsQuery{
				standard: &mockListObjectsQuery{
					executeFunc: func(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error) {
						return tt.standardResult, tt.standardErr
					},
				},
				optimized: &mockListObjectsQuery{
					executeFunc: func(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error) {
						return tt.optimizedResult, tt.optimizedErr
					},
				},
				logger: logger.NewNoopLogger(),
				config: NewShadowListObjectsQueryConfig(
					WithShadowListObjectsQueryEnabled(true),
					WithShadowListObjectsQuerySamplePercentage(100),
				),
			}
			result, err := q.Execute(ctx, req)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectResult, result)
			}
		})
	}
}

func TestShadowedListObjectsQuery_ExecuteStreamed(t *testing.T) {
	ctx := context.WithValue(context.Background(), "list-objects-optimization", true)
	req := &openfgav1.StreamedListObjectsRequest{}
	expected := &ListObjectsResolutionMetadata{}
	expectedOpt := &ListObjectsResolutionMetadata{}

	tests := []struct {
		name            string
		standardErr     error
		optimizedErr    error
		standardResult  *ListObjectsResolutionMetadata
		optimizedResult *ListObjectsResolutionMetadata
		expectErr       bool
		expectResult    *ListObjectsResolutionMetadata
	}{
		{
			name:            "both succeed, equal results",
			standardResult:  expected,
			optimizedResult: expectedOpt,
			expectResult:    expected,
		},
		{
			name:            "standard fails",
			standardErr:     errors.New("fail"),
			optimizedResult: expectedOpt,
			expectErr:       true,
		},
		{
			name:           "optimized fails",
			standardResult: expected,
			optimizedErr:   errors.New("fail"),
			expectResult:   expected,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &shadowedListObjectsQuery{
				standard: &mockListObjectsQuery{
					executeStreamedFunc: func(ctx context.Context, req *openfgav1.StreamedListObjectsRequest, srv openfgav1.OpenFGAService_StreamedListObjectsServer) (*ListObjectsResolutionMetadata, error) {
						return tt.standardResult, tt.standardErr
					},
				},
				optimized: &mockListObjectsQuery{
					executeStreamedFunc: func(ctx context.Context, req *openfgav1.StreamedListObjectsRequest, srv openfgav1.OpenFGAService_StreamedListObjectsServer) (*ListObjectsResolutionMetadata, error) {
						return tt.optimizedResult, tt.optimizedErr
					},
				},
				logger: logger.NewNoopLogger(),
				config: NewShadowListObjectsQueryConfig(
					WithShadowListObjectsQueryEnabled(true),
					WithShadowListObjectsQuerySamplePercentage(100),
				),
			}
			result, err := q.ExecuteStreamed(ctx, req, nil)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectResult, result)
			}
		})
	}
}

func TestShadowedListObjectsQuery_isShadowModeEnabled(t *testing.T) {
	q, _ := newShadowedListObjectsQuery(&mockTupleReader{}, &mockCheckResolver{}, NewShadowListObjectsQueryConfig(WithShadowListObjectsQueryEnabled(true), WithShadowListObjectsQuerySamplePercentage(100)))
	sq := q.(*shadowedListObjectsQuery)
	assert.True(t, sq.checkShadowModeSampleRate())

	q, _ = newShadowedListObjectsQuery(&mockTupleReader{}, &mockCheckResolver{}, NewShadowListObjectsQueryConfig(WithShadowListObjectsQueryEnabled(true), WithShadowListObjectsQuerySamplePercentage(0)))
	sq = q.(*shadowedListObjectsQuery)
	assert.False(t, sq.checkShadowModeSampleRate())
}

func TestRunInParallel(t *testing.T) {
	fn1 := func() (int, error) {
		time.Sleep(10 * time.Microsecond)
		return 1, nil
	}
	fn2 := func() (int, error) {
		time.Sleep(20 * time.Microsecond)
		return 2, nil
	}
	lat1, lat2, res1, res2, err1, err2 := runInParallel(fn1, fn2)
	assert.Equal(t, 1, res1)
	assert.Equal(t, 2, res2)
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.True(t, lat1 >= 10*time.Microsecond)
	assert.True(t, lat2 >= 20*time.Microsecond)
}
