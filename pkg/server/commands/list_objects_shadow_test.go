package commands

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
)

// Mock implementations.
type mockTupleReader struct {
	storage.RelationshipTupleReader
}
type mockCheckResolver struct{ graph.CheckResolver }

func TestNewShadowedListObjectsQuery(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		noopLogger := logger.NewNoopLogger()
		result, err := newShadowedListObjectsQuery(&mockTupleReader{}, &mockCheckResolver{}, NewShadowListObjectsQueryConfig(
			WithShadowListObjectsQuerySamplePercentage(13),
			WithShadowListObjectsQueryMaxDeltaItems(99),
			WithShadowListObjectsQueryTimeout(66*time.Millisecond),
		), WithListObjectsOptimizationsEnabled(true))
		require.NoError(t, err)
		require.NotNil(t, result)
		query := result.(*shadowedListObjectsQuery)
		assert.False(t, query.main.(*ListObjectsQuery).optimizationsEnabled)
		assert.False(t, query.main.(*ListObjectsQuery).useShadowCache)
		assert.True(t, query.shadow.(*ListObjectsQuery).optimizationsEnabled)
		assert.True(t, query.shadow.(*ListObjectsQuery).useShadowCache)
		assert.Equal(t, noopLogger, query.logger)
		assert.Equal(t, 13, query.shadowPct)
		assert.Equal(t, 99, query.maxDeltaItems)
		assert.Equal(t, 66*time.Millisecond, query.shadowTimeout)
	})

	t.Run("ds_error", func(t *testing.T) {
		result, err := newShadowedListObjectsQuery(nil, &mockCheckResolver{}, NewShadowListObjectsQueryConfig())
		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("check_resolver_error", func(t *testing.T) {
		result, err := newShadowedListObjectsQuery(&mockTupleReader{}, nil, NewShadowListObjectsQueryConfig())
		require.Error(t, err)
		require.Nil(t, result)
	})
}

type mockListObjectsQuery struct {
	executeFunc         func(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error)
	executeStreamedFunc func(ctx context.Context, req *openfgav1.StreamedListObjectsRequest, srv openfgav1.OpenFGAService_StreamedListObjectsServer) (*ListObjectsResolutionMetadata, error)
}

// check that mockListObjectsQuery implements ListObjectsResolver interface.
var _ ListObjectsResolver = &mockListObjectsQuery{}

func (m *mockListObjectsQuery) Execute(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error) {
	return m.executeFunc(ctx, req)
}
func (m *mockListObjectsQuery) ExecuteStreamed(ctx context.Context, req *openfgav1.StreamedListObjectsRequest, srv openfgav1.OpenFGAService_StreamedListObjectsServer) (*ListObjectsResolutionMetadata, error) {
	return m.executeStreamedFunc(ctx, req, srv)
}

func TestShadowedListObjectsQuery_Execute(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	req := &openfgav1.ListObjectsRequest{}
	expected := &ListObjectsResponse{Objects: []string{"foo"}}
	expectedOpt := &ListObjectsResponse{Objects: []string{"foo"}}

	tests := []struct {
		name         string
		percentage   int
		mainErr      error
		shadowErr    error
		mainResult   *ListObjectsResponse
		shadowResult *ListObjectsResponse
		expectErr    bool
		expectResult *ListObjectsResponse
	}{
		{
			name:         "both_succeed_with_equal_results",
			mainResult:   expected,
			shadowResult: expectedOpt,
			expectResult: expected,
			percentage:   100,
		},
		{
			name:         "main_fails",
			mainErr:      errors.New("fail"),
			shadowResult: expectedOpt,
			expectErr:    true,
			percentage:   100,
		},
		{
			name:         "shadow_fails",
			mainResult:   expected,
			shadowErr:    errors.New("fail"),
			expectResult: expected,
			percentage:   100,
		},
		{
			name:         "turned_off_shadow_mode",
			mainResult:   expected,
			shadowErr:    errors.New("ignored"),
			expectResult: expected,
			percentage:   0, // never run shadow
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			q := &shadowedListObjectsQuery{
				main: &mockListObjectsQuery{
					executeFunc: func(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error) {
						defer cancel() // ensure context is cancelled after execution
						return tt.mainResult, tt.mainErr
					},
				},
				shadow: &mockListObjectsQuery{
					executeFunc: func(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error) {
						require.NoError(t, ctx.Err())
						return tt.shadowResult, tt.shadowErr
					},
				},
				logger:        logger.NewNoopLogger(),
				shadowPct:     tt.percentage,
				shadowTimeout: 1 * time.Second, // set a long timeout for testing
				wg:            &sync.WaitGroup{},
			}
			result, err := q.Execute(ctx, req)

			q.wg.Wait()

			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectResult, result)
			}
		})
	}
}

func TestShadowedListObjectsQuery_Panics(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	req := &openfgav1.ListObjectsRequest{}

	tests := []struct {
		name        string
		percentage  int
		mainFunc    func(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error)
		shadowFunc  func(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error)
		expectedErr error
		loggerFn    func(t *testing.T, ctrl *gomock.Controller) logger.Logger
	}{
		{
			name:       "stardard_panics",
			percentage: 0,
			mainFunc: func(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error) {
				panic("this is a panic in main query")
			},
			loggerFn: func(t *testing.T, ctrl *gomock.Controller) logger.Logger {
				return mocks.NewMockLogger(ctrl)
			},
		},
		{
			name:       "shadow_panics",
			percentage: 100,
			mainFunc: func(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error) {
				return &ListObjectsResponse{
					Objects: []string{"a", "b", "c"},
				}, nil
			},
			shadowFunc: func(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error) {
				panic("this is a panic in shadow query")
			},
			loggerFn: func(t *testing.T, ctrl *gomock.Controller) logger.Logger {
				mockLogger := mocks.NewMockLogger(ctrl)
				mockLogger.EXPECT().ErrorWithContext(
					gomock.Any(),
					gomock.Eq("panic recovered"),
					gomock.Eq(zap.String("func", ListObjectsShadowExecute)),
					gomock.Eq(zap.Any("request", req)),
					gomock.Eq(zap.String("store_id", "")),
					gomock.Eq(zap.String("model_id", "")),
					gomock.Any(), // main_latency - can't be determined here
					gomock.Any(), // shadow_latency - can't be determined here
					gomock.Eq(zap.Int("main_result_count", 3)),
					gomock.Eq(zap.String("error", "this is a panic in shadow query")),
				).Times(1)
				return mockLogger
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			mockCtl := gomock.NewController(t)
			q := &shadowedListObjectsQuery{
				main:      &mockListObjectsQuery{executeFunc: tt.mainFunc},
				shadow:    &mockListObjectsQuery{executeFunc: tt.shadowFunc},
				logger:    tt.loggerFn(t, mockCtl),
				shadowPct: tt.percentage,
				wg:        &sync.WaitGroup{},
			}
			defer func() {
				if r := recover(); r != nil {
					// add extra wait to ensure the goroutine has finished
					defer q.wg.Wait()
					// should only panic in main mode
					panicMsg := fmt.Sprintf("%v", r)
					assert.Contains(t, panicMsg, "this is a panic in main query")
				}
			}()

			_, err := q.Execute(ctx, req)

			// this will never be reached if the main query panics
			q.wg.Wait()
			assert.NoError(t, err)
		})
	}
}

func TestShadowedListObjectsQuery_ExecuteStreamed(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	req := &openfgav1.StreamedListObjectsRequest{}
	expected := &ListObjectsResolutionMetadata{}
	expectedOpt := &ListObjectsResolutionMetadata{}

	tests := []struct {
		name         string
		mainErr      error
		shadowErr    error
		mainResult   *ListObjectsResolutionMetadata
		shadowResult *ListObjectsResolutionMetadata
		expectErr    bool
		expectResult *ListObjectsResolutionMetadata
	}{
		{
			name:         "both_succeed_with_equal_results",
			mainResult:   expected,
			shadowResult: expectedOpt,
			expectResult: expected,
		},
		{
			name:         "main_fails",
			mainErr:      errors.New("fail"),
			shadowResult: expectedOpt,
			expectErr:    true,
		},
		{
			name:         "shadow_fails",
			mainResult:   expected,
			shadowErr:    errors.New("fail"),
			expectResult: expected,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			q := &shadowedListObjectsQuery{
				main: &mockListObjectsQuery{
					executeStreamedFunc: func(ctx context.Context, req *openfgav1.StreamedListObjectsRequest, srv openfgav1.OpenFGAService_StreamedListObjectsServer) (*ListObjectsResolutionMetadata, error) {
						return tt.mainResult, tt.mainErr
					},
				},
				shadow: &mockListObjectsQuery{
					executeStreamedFunc: func(ctx context.Context, req *openfgav1.StreamedListObjectsRequest, srv openfgav1.OpenFGAService_StreamedListObjectsServer) (*ListObjectsResolutionMetadata, error) {
						return tt.shadowResult, tt.shadowErr
					},
				},
				logger:    logger.NewNoopLogger(),
				shadowPct: 100, // Always run in shadow mode for testing

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
	sq, ok := q.(*shadowedListObjectsQuery)
	require.True(t, ok)
	assert.True(t, sq.checkShadowModeSampleRate())

	q, _ = newShadowedListObjectsQuery(&mockTupleReader{}, &mockCheckResolver{}, NewShadowListObjectsQueryConfig(WithShadowListObjectsQueryEnabled(true), WithShadowListObjectsQuerySamplePercentage(0)))
	sq, ok = q.(*shadowedListObjectsQuery)
	require.True(t, ok)
	assert.False(t, sq.checkShadowModeSampleRate())
}

func TestShadowedListObjectsQuery_nilConfig(t *testing.T) {
	_, err := newShadowedListObjectsQuery(&mockTupleReader{}, &mockCheckResolver{}, nil)
	require.Error(t, err)
}

func Test_calculateDelta(t *testing.T) {
	type args struct {
		inputMain   []string
		inputShadow []string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "no_difference",
			args: args{
				inputMain:   []string{"a", "b", "c"},
				inputShadow: []string{"a", "b", "c"},
			},
			want: []string{},
		},
		{
			name: "main_has_extra",
			args: args{
				inputMain:   []string{"a", "b", "c", "d"},
				inputShadow: []string{"a", "b", "c"},
			},
			want: []string{"-d"},
		},
		{
			name: "shadow_has_extra",
			args: args{
				inputMain:   []string{"a", "b", "c"},
				inputShadow: []string{"a", "b", "c", "d"},
			},
			want: []string{"+d"},
		},
		{
			name: "both_have_different_elements",
			args: args{
				inputMain:   []string{"a", "b", "c"},
				inputShadow: []string{"b", "c", "d"},
			},
			want: []string{"+d", "-a"},
		},
		{
			name: "only_different",
			args: args{
				inputMain:   []string{"a", "b", "c"},
				inputShadow: []string{"x", "y", "z"},
			},
			want: []string{"+x", "+y", "+z", "-a", "-b", "-c"},
		},
		{
			name: "mixed_order",
			args: args{
				inputMain:   []string{"3", "2", "1"},
				inputShadow: []string{"1", "2", "3"},
			},
			want: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, calculateDelta(keyMapFromSlice(tt.args.inputMain), keyMapFromSlice(tt.args.inputShadow)), "calculateDelta(%v, %v)", tt.args.inputMain, tt.args.inputShadow)
		})
	}
}

func Test_shadowedListObjectsQuery_executeShadowModeAndCompareResults(t *testing.T) {
	type fields struct {
		main          ListObjectsResolver
		shadow        ListObjectsResolver
		shadowPct     int
		shadowTimeout time.Duration
		maxDeltaItems int
		loggerFn      func(t *testing.T, ctrl *gomock.Controller) logger.Logger
	}
	type args struct {
		req     *openfgav1.ListObjectsRequest
		result  []string
		latency time.Duration
	}
	commonMetadata := NewListObjectsResolutionMetadata()
	commonMetadata.ShouldRunShadowQuery.Store(true)
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "equal_results",
			fields: fields{
				shadow: &mockListObjectsQuery{
					executeFunc: func(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error) {
						return &ListObjectsResponse{Objects: []string{"a", "b", "c"}, ResolutionMetadata: commonMetadata}, nil
					},
				},
				shadowPct:     100,
				shadowTimeout: 1 * time.Minute,
				maxDeltaItems: 100,
				loggerFn: func(t *testing.T, ctrl *gomock.Controller) logger.Logger {
					mockLogger := mocks.NewMockLogger(ctrl)
					mockLogger.EXPECT().InfoWithContext(
						gomock.Any(),
						gomock.Eq("shadowed list objects result matches"),
						gomock.Eq(zap.String("func", ListObjectsShadowExecute)),
						gomock.Eq(zap.Any("request", &openfgav1.ListObjectsRequest{
							StoreId:              "req.GetStoreId()",
							AuthorizationModelId: "req.GetAuthorizationModelId()",
						})),
						gomock.Eq(zap.String("store_id", "req.GetStoreId()")),
						gomock.Eq(zap.String("model_id", "req.GetAuthorizationModelId()")),
						gomock.Eq(zap.Bool("is_match", true)),
						gomock.Eq(zap.Duration("main_latency", 77*time.Millisecond)),
						gomock.Any(),
						zap.Int("main_result_count", 3),
						gomock.Eq(zap.Uint32("datastore_query_count", uint32(0))),
					)
					return mockLogger
				},
			},
			args: args{
				req: &openfgav1.ListObjectsRequest{
					StoreId:              "req.GetStoreId()",
					AuthorizationModelId: "req.GetAuthorizationModelId()",
				},
				result:  []string{"a", "b", "c"},
				latency: 77 * time.Millisecond,
			},
		},
		{
			name: "has_delta",
			fields: fields{
				shadow: &mockListObjectsQuery{
					executeFunc: func(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error) {
						return &ListObjectsResponse{Objects: []string{"c", "d"}, ResolutionMetadata: commonMetadata}, nil
					},
				},
				shadowPct:     100,
				shadowTimeout: 1 * time.Minute,
				maxDeltaItems: 100,
				loggerFn: func(t *testing.T, ctrl *gomock.Controller) logger.Logger {
					mockLogger := mocks.NewMockLogger(ctrl)
					mockLogger.EXPECT().WarnWithContext(
						gomock.Any(),
						gomock.Eq("shadowed list objects result difference"),
						gomock.Eq(zap.String("func", ListObjectsShadowExecute)),
						gomock.Eq(zap.Any("request", &openfgav1.ListObjectsRequest{})),
						gomock.Eq(zap.String("store_id", "")),
						gomock.Eq(zap.String("model_id", "")),
						gomock.Eq(zap.Bool("is_match", false)),
						gomock.Eq(zap.Duration("main_latency", 77*time.Millisecond)),
						gomock.Any(),
						gomock.Eq(zap.Int("main_result_count", 3)),
						gomock.Eq(zap.Int("shadow_result_count", 2)),
						gomock.Eq(zap.Int("total_delta", 3)),
						gomock.Eq(zap.Any("delta", []string{"+d", "-a", "-b"})),
						gomock.Eq(zap.Uint32("datastore_query_count", uint32(0))),
					)
					return mockLogger
				},
			},
			args: args{
				req:     &openfgav1.ListObjectsRequest{},
				result:  []string{"a", "b", "c"},
				latency: 77 * time.Millisecond,
			},
		},
		{
			name: "delta_exceeds_max_items",
			fields: fields{
				shadow: &mockListObjectsQuery{
					executeFunc: func(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error) {
						return &ListObjectsResponse{Objects: []string{"c", "d", "x", "y", "z"}, ResolutionMetadata: commonMetadata}, nil
					},
				},
				shadowPct:     100,
				shadowTimeout: 1 * time.Minute,
				maxDeltaItems: 3,
				loggerFn: func(t *testing.T, ctrl *gomock.Controller) logger.Logger {
					mockLogger := mocks.NewMockLogger(ctrl)
					mockLogger.EXPECT().WarnWithContext(
						gomock.Any(),
						gomock.Eq("shadowed list objects result difference"),
						gomock.Eq(zap.String("func", ListObjectsShadowExecute)),
						gomock.Eq(zap.Any("request", &openfgav1.ListObjectsRequest{})),
						gomock.Eq(zap.String("store_id", "")),
						gomock.Eq(zap.String("model_id", "")),
						gomock.Eq(zap.Bool("is_match", false)),
						gomock.Eq(zap.Duration("main_latency", 77*time.Millisecond)),
						gomock.Any(),
						gomock.Eq(zap.Int("main_result_count", 10)),
						gomock.Eq(zap.Int("shadow_result_count", 5)),
						gomock.Eq(zap.Int("total_delta", 11)),
						gomock.Eq(zap.Any("delta", []string{"+x", "+y", "+z"})),
						gomock.Eq(zap.Uint32("datastore_query_count", uint32(0))),
					)
					return mockLogger
				},
			},
			args: args{
				req:     &openfgav1.ListObjectsRequest{},
				result:  []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
				latency: 77 * time.Millisecond,
			},
		},
		{
			name: "shadow_timeout",
			fields: fields{
				shadow: &mockListObjectsQuery{
					executeFunc: func(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error) {
						<-ctx.Done() // wait for context to be cancelled
						return nil, ctx.Err()
					},
				},
				shadowPct:     100,
				shadowTimeout: 1 * time.Nanosecond,
				maxDeltaItems: 100,
				loggerFn: func(t *testing.T, ctrl *gomock.Controller) logger.Logger {
					mockLogger := mocks.NewMockLogger(ctrl)
					mockLogger.EXPECT().WarnWithContext(
						gomock.Any(),
						gomock.Eq("shadowed list objects error"),
						gomock.Eq(zap.String("func", ListObjectsShadowExecute)),
						gomock.Eq(zap.Any("request", &openfgav1.ListObjectsRequest{})),
						gomock.Eq(zap.String("store_id", "")),
						gomock.Eq(zap.String("model_id", "")),
						gomock.Eq(zap.Duration("main_latency", 77*time.Millisecond)),
						gomock.Any(),
						gomock.Eq(zap.Int("main_result_count", 3)),
						gomock.Eq(zap.Any("error", context.DeadlineExceeded)),
					)
					return mockLogger
				},
			},
			args: args{
				req:     &openfgav1.ListObjectsRequest{},
				result:  []string{"a", "b", "c"},
				latency: 77 * time.Millisecond,
			},
		},
		{
			name: "shadow_error",
			fields: fields{
				shadow: &mockListObjectsQuery{
					executeFunc: func(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error) {
						return nil, errors.New("shadow query error")
					},
				},
				shadowPct:     100,
				shadowTimeout: 1 * time.Nanosecond,
				maxDeltaItems: 100,
				loggerFn: func(t *testing.T, ctrl *gomock.Controller) logger.Logger {
					mockLogger := mocks.NewMockLogger(ctrl)
					mockLogger.EXPECT().WarnWithContext(
						gomock.Any(),
						gomock.Eq("shadowed list objects error"),
						gomock.Eq(zap.String("func", ListObjectsShadowExecute)),
						gomock.Eq(zap.Any("request", &openfgav1.ListObjectsRequest{})),
						gomock.Eq(zap.String("store_id", "")),
						gomock.Eq(zap.String("model_id", "")),
						gomock.Eq(zap.Duration("main_latency", 77*time.Millisecond)),
						gomock.Any(),
						gomock.Eq(zap.Int("main_result_count", 3)),
						gomock.Eq(zap.Any("error", errors.New("shadow query error"))),
					)
					return mockLogger
				},
			},
			args: args{
				req:     &openfgav1.ListObjectsRequest{},
				result:  []string{"a", "b", "c"},
				latency: 77 * time.Millisecond,
			},
		},
		{
			name: "verify_context_cancelled",
			fields: fields{
				main: &mockListObjectsQuery{
					executeFunc: func(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error) {
						require.Error(t, ctx.Err()) // ensure main context is cancelled
						return &ListObjectsResponse{}, nil
					},
				},
				shadow: &mockListObjectsQuery{
					executeFunc: func(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error) {
						require.NoError(t, ctx.Err()) // context must not be cancelled
						return &ListObjectsResponse{ResolutionMetadata: commonMetadata}, nil
					},
				},
				shadowPct:     100,
				shadowTimeout: 1 * time.Second,
				maxDeltaItems: 0,
				loggerFn: func(t *testing.T, ctrl *gomock.Controller) logger.Logger {
					mockLogger := mocks.NewMockLogger(ctrl)
					mockLogger.EXPECT().InfoWithContext(gomock.Any(), gomock.Any(), gomock.Any())
					return mockLogger
				},
			},
			args: args{
				req:     nil,
				result:  nil,
				latency: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel) // ensure context is cancelled after test

			mockCtrl := gomock.NewController(t)
			q := &shadowedListObjectsQuery{
				main:          tt.fields.main,
				shadow:        tt.fields.shadow,
				shadowPct:     tt.fields.shadowPct,
				shadowTimeout: tt.fields.shadowTimeout,
				maxDeltaItems: tt.fields.maxDeltaItems,
				logger:        tt.fields.loggerFn(t, mockCtrl),
			}
			q.executeShadowModeAndCompareResults(ctx, tt.args.req, tt.args.result, tt.args.latency)
		})
	}
}

func TestShadowedListObjectsQuery_checkShadowModePreconditions(t *testing.T) {
	type args struct {
		mainResultFunc func() *ListObjectsResponse
		latency        time.Duration
		pct            int
		maxResults     uint32
		deadline       time.Duration
	}
	tests := []struct {
		name           string
		args           args
		expectedReturn bool
		loggerFn       func(t *testing.T, ctrl *gomock.Controller) logger.Logger
		wg             *sync.WaitGroup
	}{
		{
			name: "main result reaches max result size",
			args: args{
				mainResultFunc: func() *ListObjectsResponse {
					return &ListObjectsResponse{Objects: []string{"a", "b", "c"}}
				},
				latency:    10 * time.Millisecond,
				pct:        100,
				maxResults: 3,
				deadline:   1 * time.Second,
			},
			expectedReturn: false,
			loggerFn: func(t *testing.T, ctrl *gomock.Controller) logger.Logger {
				mockLogger := mocks.NewMockLogger(ctrl)
				mockLogger.EXPECT().DebugWithContext(
					gomock.Any(),
					gomock.Eq("shadowed list objects query skipped due to max results reached"),
					gomock.Eq(zap.String("func", ListObjectsShadowExecute)),
					gomock.Eq(zap.Any("request", &openfgav1.ListObjectsRequest{})),
					gomock.Eq(zap.String("store_id", "")),
					gomock.Eq(zap.String("model_id", "")),
				)
				return mockLogger
			},
		},
		{
			name: "main query latency too high",
			args: args{
				mainResultFunc: func() *ListObjectsResponse {
					meta := NewListObjectsResolutionMetadata()
					meta.ShouldRunShadowQuery.Store(true)
					return &ListObjectsResponse{Objects: []string{"a"}, ResolutionMetadata: meta}
				},
				latency:    950 * time.Millisecond,
				pct:        100,
				maxResults: 10,
				deadline:   1 * time.Second,
			},
			expectedReturn: false,
			loggerFn: func(t *testing.T, ctrl *gomock.Controller) logger.Logger {
				mockLogger := mocks.NewMockLogger(ctrl)
				mockLogger.EXPECT().DebugWithContext(
					gomock.Any(),
					gomock.Eq("shadowed list objects query skipped due to high latency of the main query"),
					gomock.Eq(zap.String("func", ListObjectsShadowExecute)),
					gomock.Eq(zap.Any("request", &openfgav1.ListObjectsRequest{})),
					gomock.Eq(zap.String("store_id", "")),
					gomock.Eq(zap.String("model_id", "")),
					gomock.Eq(zap.Duration("latency", 950*time.Millisecond)),
				)
				return mockLogger
			},
		},
		{
			name: "sample rate not met",
			args: args{
				mainResultFunc: func() *ListObjectsResponse {
					meta := NewListObjectsResolutionMetadata()
					meta.ShouldRunShadowQuery.Store(true)
					return &ListObjectsResponse{Objects: []string{"a"}, ResolutionMetadata: meta}
				},
				latency:    10 * time.Millisecond,
				pct:        0,
				maxResults: 10,
				deadline:   1 * time.Second,
			},
			expectedReturn: false,
			loggerFn: func(t *testing.T, ctrl *gomock.Controller) logger.Logger {
				return mocks.NewMockLogger(ctrl)
			},
		},
		{
			name: "metadata_should_not_run_shadow",
			args: args{
				mainResultFunc: func() *ListObjectsResponse {
					meta := NewListObjectsResolutionMetadata()
					meta.ShouldRunShadowQuery.Store(false)
					return &ListObjectsResponse{ResolutionMetadata: meta}
				},
				latency:    10 * time.Millisecond,
				pct:        0,
				maxResults: 10,
				deadline:   1 * time.Second,
			},
			expectedReturn: false,
			loggerFn: func(t *testing.T, ctrl *gomock.Controller) logger.Logger {
				mockLogger := mocks.NewMockLogger(ctrl)
				mockLogger.EXPECT().DebugWithContext(
					gomock.Any(),
					gomock.Eq("shadowed list objects query skipped due to infinite weight query"),
					gomock.Eq(zap.String("func", ListObjectsShadowExecute)),
					gomock.Eq(zap.Any("request", &openfgav1.ListObjectsRequest{})),
					gomock.Eq(zap.String("store_id", "")),
					gomock.Eq(zap.String("model_id", "")),
				)
				return mockLogger
			},
		},
		{
			name: "all preconditions met",
			args: args{
				mainResultFunc: func() *ListObjectsResponse {
					meta := NewListObjectsResolutionMetadata()
					meta.ShouldRunShadowQuery.Store(true)
					return &ListObjectsResponse{ResolutionMetadata: meta}
				},
				latency:    10 * time.Millisecond,
				pct:        100,
				maxResults: 10,
				deadline:   1 * time.Second,
			},
			expectedReturn: true,
			loggerFn: func(t *testing.T, ctrl *gomock.Controller) logger.Logger {
				return mocks.NewMockLogger(ctrl)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockLogger := tt.loggerFn(t, ctrl)
			mainQuery := &ListObjectsQuery{
				listObjectsMaxResults: tt.args.maxResults,
				listObjectsDeadline:   tt.args.deadline,
			}
			q := &shadowedListObjectsQuery{
				main:      mainQuery,
				shadowPct: tt.args.pct,
				logger:    mockLogger,
			}

			ret := q.checkShadowModePreconditions(context.TODO(), &openfgav1.ListObjectsRequest{}, tt.args.mainResultFunc(), tt.args.latency)
			assert.Equal(t, tt.expectedReturn, ret)
		})
	}
}
