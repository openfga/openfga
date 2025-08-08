package graph

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func collectMessagesFromChannel(dispatchChan chan dispatchMsg) []dispatchMsg {
	var receivedDispatches []dispatchMsg
	for msg := range dispatchChan {
		receivedDispatches = append(receivedDispatches, msg)
	}
	return receivedDispatches
}

// helperReceivedOutcome is a helper function that listen to chan checkOutcome and return
// all the checkOutcomes when channel is closed.
func helperReceivedOutcome(outcomes <-chan checkOutcome) []checkOutcome {
	var checkOutcome []checkOutcome
	for outcome := range outcomes {
		checkOutcome = append(checkOutcome, outcome)
	}
	return checkOutcome
}

func TestDefaultUserset(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	filter := func(tupleKey *openfgav1.TupleKey) (bool, error) {
		if tupleKey.GetCondition().GetName() == "condition1" {
			return true, nil
		}
		return false, fmt.Errorf("condition not found")
	}

	// model does not matter for this unit test.  All we care about is schema 1.1+.
	model := parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user
				type group
					relations
						define member: [user with condX, user:* with condX, group#member]
				type document
					relations
						define viewer: [group#member]

				condition condX(x: int) {
					x < 100
				}`)
	ts, err := typesystem.New(model)
	require.NoError(t, err)
	ctx := typesystem.ContextWithTypesystem(context.Background(), ts)

	tests := []struct {
		name          string
		tuples        []*openfgav1.TupleKey
		expected      *ResolveCheckResponse
		expectedError error
	}{
		{
			name: "shortcut",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("group:1", "member", "user:*", "condition1", nil),
				tuple.NewTupleKeyWithCondition("group:1", "member", "group:2#member", "condition1", nil),
			},
			expected: &ResolveCheckResponse{
				Allowed: true,
			},
			expectedError: nil,
		},
		{
			name: "error",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("group:1", "member", "user:*", "badCondition", nil),
			},
			expected:      nil,
			expectedError: fmt.Errorf("condition not found"),
		},
		{
			name:   "notFound",
			tuples: []*openfgav1.TupleKey{},
			expected: &ResolveCheckResponse{
				Allowed: false,
			},
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			iter := storage.NewConditionsFilteredTupleKeyIterator(storage.NewStaticTupleKeyIterator(tt.tuples), filter)
			checker := NewLocalChecker()
			defer checker.Close()

			req := &ResolveCheckRequest{
				TupleKey:        tuple.NewTupleKey("group:1", "member", "user:maria"),
				RequestMetadata: NewCheckRequestMetadata(),
			}
			resp, err := checker.defaultUserset(ctx, req, iter)
			require.Equal(t, tt.expectedError, err)
			require.Equal(t, tt.expected, resp)
		})
	}

	t.Run("should_error_if_produceUsersetDispatches_panics", func(t *testing.T) {
		iter := &mockPanicIterator[*openfgav1.TupleKey]{}
		checker := NewLocalChecker()
		defer checker.Close()

		req := &ResolveCheckRequest{
			TupleKey:        tuple.NewTupleKey("group:1", "member", "user:maria"),
			RequestMetadata: NewCheckRequestMetadata(),
		}
		resp, err := checker.defaultUserset(ctx, req, iter)
		require.ErrorContains(t, err, panicErr)
		require.ErrorIs(t, err, ErrPanic)
		require.Equal(t, (*ResolveCheckResponse)(nil), resp)
	})
}

func TestDefaultTTU(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	filter := func(tupleKey *openfgav1.TupleKey) (bool, error) {
		if tupleKey.GetCondition().GetName() == "condition1" {
			return true, nil
		}
		return false, fmt.Errorf("condition not found")
	}

	// model does not matter for this unit test.  All we care about is schema 1.1+ and computedRelation is defined for type
	model := parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user
				type group
					relations
						define member: [user with condX]
				type team
					relations
						define teammate: [user with condX]
				type document
					relations
						define viewer: member from owner
						define owner: [group, team]

				condition condX(x: int) {
					x < 100
				}`)

	ts, err := typesystem.New(model)
	require.NoError(t, err)
	ctx := typesystem.ContextWithTypesystem(context.Background(), ts)

	tests := []struct {
		name             string
		rewrite          *openfgav1.Userset
		tuples           []*openfgav1.TupleKey
		dispatchResponse *ResolveCheckResponse
		expected         *ResolveCheckResponse
		expectedError    error
	}{
		{
			name:    "no_tuple",
			rewrite: typesystem.TupleToUserset("owner", "member"),
			tuples:  []*openfgav1.TupleKey{},
			expected: &ResolveCheckResponse{
				Allowed: false,
			},
			expectedError: nil,
		},
		{
			name:    "error",
			rewrite: typesystem.TupleToUserset("owner", "member"),
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "owner", "group:1", "badCondition", nil),
			},
			expected:      nil,
			expectedError: fmt.Errorf("condition not found"),
		},
		{
			name:    "dispatcher_found",
			rewrite: typesystem.TupleToUserset("owner", "member"),
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "owner", "group:1", "condition1", nil),
			},
			dispatchResponse: &ResolveCheckResponse{
				Allowed: true,
			},
			expected: &ResolveCheckResponse{
				Allowed: true,
			},
			expectedError: nil,
		},
		{
			name:    "dispatcher_not_found",
			rewrite: typesystem.TupleToUserset("owner", "member"),
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "owner", "group:1", "condition1", nil),
			},
			dispatchResponse: &ResolveCheckResponse{
				Allowed: false,
			},
			expected: &ResolveCheckResponse{
				Allowed: false,
			},
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			iter := storage.NewConditionsFilteredTupleKeyIterator(storage.NewStaticTupleKeyIterator(tt.tuples), filter)
			checker := NewLocalChecker()
			defer checker.Close()
			mockResolver := NewMockCheckResolver(ctrl)
			checker.SetDelegate(mockResolver)

			if tt.dispatchResponse != nil {
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Return(tt.dispatchResponse, nil)
			}

			req := &ResolveCheckRequest{
				TupleKey:        tuple.NewTupleKey("group:1", "member", "user:maria"),
				RequestMetadata: NewCheckRequestMetadata(),
			}
			resp, err := checker.defaultTTU(ctx, req, tt.rewrite, iter)
			require.Equal(t, tt.expectedError, err)
			require.Equal(t, tt.expected, resp)
		})
	}
}

func TestProduceUsersetDispatches(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	filter := func(tupleKey *openfgav1.TupleKey) (bool, error) {
		if tupleKey.GetCondition().GetName() == "condition1" {
			return true, nil
		}
		return false, fmt.Errorf("condition not found")
	}

	// model does not matter for this unit test.  All we care about is schema 1.1+.
	model := parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user
				type group
					relations
						define member: [user with condX, group#member]
				type document
					relations
						define viewer: [group#member]

				condition condX(x: int) {
					x < 100
				}`)
	ts, err := typesystem.New(model)
	require.NoError(t, err)
	ctx := typesystem.ContextWithTypesystem(context.Background(), ts)
	req := &ResolveCheckRequest{
		TupleKey:        tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "user:maria", "condition1", nil),
		RequestMetadata: NewCheckRequestMetadata(),
	}

	tests := []struct {
		name               string
		tuples             []*openfgav1.TupleKey
		expectedDispatches []dispatchMsg
	}{
		{
			name:               "empty_iterator",
			tuples:             nil,
			expectedDispatches: nil,
		},
		{
			name: "iterator_error_first_tuple",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("group:1", "member", "user:maria", "badCondition", nil),
			},
			expectedDispatches: []dispatchMsg{
				{
					err:            fmt.Errorf("condition not found"),
					shortCircuit:   false,
					dispatchParams: nil,
				},
			},
		},
		{
			name: "good_condition_wildcard",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("group:1", "member", "user:*", "condition1", nil),
			},
			expectedDispatches: []dispatchMsg{
				{
					err:            nil,
					shortCircuit:   true,
					dispatchParams: nil,
				},
			},
		},
		{
			name: "good_condition_non_wildcard",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("group:1", "member", "group:2#member", "condition1", nil),
			},
			expectedDispatches: []dispatchMsg{
				{
					err:          nil,
					shortCircuit: false,
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:2", "member", "user:maria"),
					},
				},
			},
		},
		{
			name: "multiple_tuples",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("group:1", "member", "group:2#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("group:1", "member", "group:3#member", "condition1", nil),
			},
			expectedDispatches: []dispatchMsg{
				{
					err:          nil,
					shortCircuit: false,
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:2", "member", "user:maria"),
					},
				},
				{
					err:          nil,
					shortCircuit: false,
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:3", "member", "user:maria"),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			iter := storage.NewConditionsFilteredTupleKeyIterator(storage.NewStaticTupleKeyIterator(tt.tuples), filter)
			checker := NewLocalChecker()
			defer checker.Close()
			mockResolver := NewMockCheckResolver(ctrl)
			checker.SetDelegate(mockResolver)

			pool := concurrency.NewPool(ctx, 1)

			dispatchChan := make(chan dispatchMsg, 1)

			pool.Go(func(ctx context.Context) error {
				checker.produceUsersetDispatches(ctx, req, dispatchChan, iter)
				return nil
			})

			receivedMsgs := collectMessagesFromChannel(dispatchChan)
			_ = pool.Wait()
			require.Equal(t, tt.expectedDispatches, receivedMsgs)
		})
	}
}

func TestProduceTTUDispatches(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	filter := func(tupleKey *openfgav1.TupleKey) (bool, error) {
		if tupleKey.GetCondition().GetName() == "condition1" {
			return true, nil
		}
		return false, fmt.Errorf("condition not found")
	}

	// model does not matter for this unit test.  All we care about is schema 1.1+ and computedRelation is defined for type
	model := parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user
				type group
					relations
						define member: [user with condX]
				type team
					relations
						define teammate: [user with condX]
				type document
					relations
						define viewer: member from owner
						define owner: [group, team]

				condition condX(x: int) {
					x < 100
				}`)

	ts, err := typesystem.New(model)
	require.NoError(t, err)
	ctx := typesystem.ContextWithTypesystem(context.Background(), ts)
	req := &ResolveCheckRequest{
		TupleKey:        tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "user:maria", "condition1", nil),
		RequestMetadata: NewCheckRequestMetadata(),
	}

	tests := []struct {
		name               string
		computedRelation   string
		tuples             []*openfgav1.TupleKey
		expectedDispatches []dispatchMsg
	}{
		{
			name:               "empty_iterator",
			computedRelation:   "member",
			tuples:             nil,
			expectedDispatches: nil,
		},
		{
			name:             "iterator_error_first_tuple",
			computedRelation: "member",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "owner", "group:1", "badCondition", nil),
			},
			expectedDispatches: []dispatchMsg{
				{
					err:            fmt.Errorf("condition not found"),
					shortCircuit:   false,
					dispatchParams: nil,
				},
			},
		},
		{
			name:             "relation_not_found",
			computedRelation: "member",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "owner", "team:1", "condition1", nil),
			},
			expectedDispatches: nil,
		},
		{
			name:             "single_match",
			computedRelation: "member",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "owner", "group:1", "condition1", nil),
			},
			expectedDispatches: []dispatchMsg{
				{
					err:          nil,
					shortCircuit: false,
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:1", "member", "user:maria"),
					},
				},
			},
		},
		{
			name:             "multiple_matches",
			computedRelation: "member",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "owner", "group:1", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "owner", "group:2", "condition1", nil),
			},
			expectedDispatches: []dispatchMsg{
				{
					err:          nil,
					shortCircuit: false,
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:1", "member", "user:maria"),
					},
				},
				{
					err:          nil,
					shortCircuit: false,
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:2", "member", "user:maria"),
					},
				},
			},
		},
		{
			name:             "mix_relation_found_not_found",
			computedRelation: "member",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "owner", "team:1", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "owner", "group:1", "condition1", nil),
			},
			expectedDispatches: []dispatchMsg{
				{
					err:          nil,
					shortCircuit: false,
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:1", "member", "user:maria"),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			iter := storage.NewConditionsFilteredTupleKeyIterator(storage.NewStaticTupleKeyIterator(tt.tuples), filter)
			checker := NewLocalChecker()
			defer checker.Close()
			mockResolver := NewMockCheckResolver(ctrl)
			checker.SetDelegate(mockResolver)

			pool := concurrency.NewPool(ctx, 1)

			dispatchChan := make(chan dispatchMsg, 1)

			pool.Go(func(ctx context.Context) error {
				checker.produceTTUDispatches(ctx, tt.computedRelation, req, dispatchChan, iter)
				return nil
			})

			receivedMsgs := collectMessagesFromChannel(dispatchChan)
			_ = pool.Wait()
			require.Equal(t, tt.expectedDispatches, receivedMsgs)
		})
	}
}

func TestProcessDispatch(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	req := &ResolveCheckRequest{
		TupleKey:        tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "user:maria", "condition1", nil),
		RequestMetadata: NewCheckRequestMetadata(),
	}

	tests := []struct {
		name                   string
		poolSize               int
		ctxCancelled           bool
		dispatchMsgs           []dispatchMsg
		mockedDispatchResponse []*ResolveCheckResponse
		expectedOutcomes       []checkOutcome
	}{
		{
			name:             "ctx_cancelled",
			poolSize:         1,
			ctxCancelled:     true,
			dispatchMsgs:     []dispatchMsg{},
			expectedOutcomes: nil,
		},
		{
			name:         "two_error",
			poolSize:     1,
			ctxCancelled: false,
			dispatchMsgs: []dispatchMsg{
				{
					err: fmt.Errorf("error_1"),
				},
				{
					err: fmt.Errorf("error_2"),
				},
			},
			expectedOutcomes: []checkOutcome{
				{
					err: fmt.Errorf("error_1"),
				},
				{
					err: fmt.Errorf("error_2"),
				},
			},
		},
		{
			name:         "shortcut_with_only",
			poolSize:     1,
			ctxCancelled: false,
			dispatchMsgs: []dispatchMsg{
				{
					shortCircuit: true,
				},
			},
			expectedOutcomes: []checkOutcome{
				{
					resp: &ResolveCheckResponse{
						Allowed: true,
					},
				},
			},
		},
		{
			name:         "shortcut_with_error_at_end",
			poolSize:     1,
			ctxCancelled: false,
			dispatchMsgs: []dispatchMsg{
				{
					shortCircuit: true,
				},
				{
					err: fmt.Errorf("should_not_process_this"),
				},
			},
			expectedOutcomes: []checkOutcome{
				{
					resp: &ResolveCheckResponse{
						Allowed: true,
					},
				},
			},
		},
		{
			name:         "multiple_dispatches",
			poolSize:     1,
			ctxCancelled: false,
			dispatchMsgs: []dispatchMsg{
				{
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:1", "member", "user:maria"),
					},
				},
				{
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:2", "member", "user:maria"),
					},
				},
			},
			mockedDispatchResponse: []*ResolveCheckResponse{
				{
					Allowed: true,
				},
				{
					Allowed: false,
				},
			},
			expectedOutcomes: []checkOutcome{
				{
					resp: &ResolveCheckResponse{
						Allowed: true,
					},
				},
				{
					resp: &ResolveCheckResponse{
						Allowed: false,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ctx := context.Background()
			var cancel context.CancelFunc
			if tt.ctxCancelled {
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			checker := NewLocalChecker()
			defer checker.Close()
			mockResolver := NewMockCheckResolver(ctrl)
			checker.SetDelegate(mockResolver)

			for _, mockedDispatchResponse := range tt.mockedDispatchResponse {
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Times(1).Return(mockedDispatchResponse, nil)
			}

			dispatchMsgChan := make(chan dispatchMsg, 100)
			for _, dispatchMsg := range tt.dispatchMsgs {
				dispatchMsgChan <- dispatchMsg
			}

			outcomeChan := checker.processDispatches(ctx, tt.poolSize, dispatchMsgChan)

			// now, close the channel to simulate everything is sent
			close(dispatchMsgChan)
			outcomes := helperReceivedOutcome(outcomeChan)

			require.Equal(t, tt.expectedOutcomes, outcomes)
		})
	}

	t.Run("should_error_if_dispatch_panics", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := context.Background()

		checker := NewLocalChecker()
		defer checker.Close()
		mockResolver := NewMockCheckResolver(ctrl)
		checker.SetDelegate(mockResolver)

		dispatchChan := make(chan dispatchMsg, 1)
		outcomeChan := checker.processDispatches(ctx, 1, dispatchChan)
		dispatchChan <- dispatchMsg{
			dispatchParams: &dispatchParams{
				parentReq: nil, // This will cause a panic when accessed in `dispatch`
				tk:        nil, // Invalid TupleKey to trigger a panic
			},
		}
		close(dispatchChan)

		outcome := <-outcomeChan
		require.ErrorContains(t, outcome.err, "invalid memory address or nil pointer")
		require.ErrorIs(t, outcome.err, ErrPanic)
	})
}

func TestConsumeDispatch(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	req := &ResolveCheckRequest{
		TupleKey:        tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "user:maria", "condition1", nil),
		RequestMetadata: NewCheckRequestMetadata(),
	}
	tests := []struct {
		name                   string
		limit                  int
		ctxCancelled           bool
		dispatchMsgs           []dispatchMsg
		mockedDispatchResponse []*ResolveCheckResponse
		expected               *ResolveCheckResponse
		expectedError          error
	}{
		{
			name:          "ctx_cancelled",
			limit:         1,
			ctxCancelled:  true,
			dispatchMsgs:  []dispatchMsg{},
			expected:      nil,
			expectedError: context.Canceled,
		},
		{
			name:         "single_error",
			limit:        1,
			ctxCancelled: false,
			dispatchMsgs: []dispatchMsg{
				{
					err: fmt.Errorf("error_1"),
				},
			},
			expected:      nil,
			expectedError: fmt.Errorf("error_1"),
		},
		{
			name:         "false_cycle_detected",
			limit:        1,
			ctxCancelled: false,
			dispatchMsgs: []dispatchMsg{
				{
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:1", "member", "user:maria"),
					},
				},
			},
			mockedDispatchResponse: []*ResolveCheckResponse{
				{
					Allowed: false,
					ResolutionMetadata: ResolveCheckResponseMetadata{
						CycleDetected: true,
					},
				},
			},
			expected: &ResolveCheckResponse{
				Allowed: false,
				ResolutionMetadata: ResolveCheckResponseMetadata{
					CycleDetected: true,
				},
			},
			expectedError: nil,
		},
		{
			name:         "two_false_no_cycle",
			limit:        1,
			ctxCancelled: false,
			dispatchMsgs: []dispatchMsg{
				{
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:1", "member", "user:maria"),
					},
				},
				{
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:2", "member", "user:maria"),
					},
				},
			},
			mockedDispatchResponse: []*ResolveCheckResponse{
				{
					Allowed: false,
				},
				{
					Allowed: false,
				},
			},
			expected: &ResolveCheckResponse{
				Allowed: false,
			},
			expectedError: nil,
		},
		{
			name:         "false_true",
			limit:        1,
			ctxCancelled: false,
			dispatchMsgs: []dispatchMsg{
				{
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:1", "member", "user:maria"),
					},
				},
				{
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:1", "member", "user:maria"),
					},
				},
			},
			mockedDispatchResponse: []*ResolveCheckResponse{
				{
					Allowed: false,
				},
				{
					Allowed: true,
				},
			},
			expected: &ResolveCheckResponse{
				Allowed: true,
			},
			expectedError: nil,
		},
		{
			name:         "single_true",
			limit:        1,
			ctxCancelled: false,
			dispatchMsgs: []dispatchMsg{
				{
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:1", "member", "user:maria"),
					},
				},
			},
			mockedDispatchResponse: []*ResolveCheckResponse{
				{
					Allowed: true,
				},
			},
			expected: &ResolveCheckResponse{
				Allowed: true,
			},
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ctx := context.Background()
			var cancel context.CancelFunc
			if tt.ctxCancelled {
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			checker := NewLocalChecker()
			defer checker.Close()
			mockResolver := NewMockCheckResolver(ctrl)
			checker.SetDelegate(mockResolver)
			for _, mockedDispatchResponse := range tt.mockedDispatchResponse {
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Times(1).Return(mockedDispatchResponse, nil)
			}

			dispatchMsgChan := make(chan dispatchMsg, 100)
			for _, dispatchMsg := range tt.dispatchMsgs {
				dispatchMsgChan <- dispatchMsg
			}
			close(dispatchMsgChan)

			resp, err := checker.consumeDispatches(ctx, tt.limit, dispatchMsgChan)
			require.Equal(t, tt.expectedError, err)
			require.Equal(t, tt.expected, resp)
		})
	}

	t.Run("should_error_if_panic_occurs", func(t *testing.T) {
		ctx := context.Background()
		checker := NewLocalChecker()
		defer checker.Close()

		dispatchChan := make(chan dispatchMsg, 1)
		dispatchChan <- dispatchMsg{
			dispatchParams: &dispatchParams{
				parentReq: nil, // This will cause a panic when accessed in `dispatch`
				tk:        nil, // Invalid TupleKey to trigger a panic
			},
		}
		close(dispatchChan)

		_, err := checker.consumeDispatches(ctx, 1, dispatchChan)

		require.ErrorContains(t, err, "invalid memory address or nil pointer")
		require.ErrorIs(t, err, ErrPanic)
	})
}
