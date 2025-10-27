package strategies

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/internal/check"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestDefaultUserset(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storeID := ulid.Make().String()

	// model does not matter for this unit test.  All we care about is schema 1.1+.
	model := testutils.MustTransformDSLToProtoWithID(`
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

	mg, err := check.NewAuthorizationModelGraph(model)
	require.NoError(t, err)

	edges, ok := mg.GetEdgesFromNodeId("group#member")
	require.True(t, ok)

	tests := []struct {
		name          string
		tuples        []*openfgav1.TupleKey
		setupMock     func(*graph.MockCheckResolver)
		expected      *check.Response
		expectedError error
	}{
		{
			name: "shortcut",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:1", "member", "user:1#member"),
				tuple.NewTupleKey("group:1", "member", "user:2#member"),
			},
			expected: &check.Response{
				Allowed: true,
			},
			expectedError: nil,
			setupMock: func(mockResolver *graph.MockCheckResolver) {
				mockResolver.EXPECT().
					ResolveCheck(gomock.Any(), gomock.Any()).
					Return(&check.Response{Allowed: true}, nil).
					Times(2)
			},
		},
		/*
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

		*/
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockResolver := graph.NewMockCheckResolver(ctrl)
			tt.setupMock(mockResolver)

			iter := storage.NewStaticTupleKeyIterator(tt.tuples)
			strategy := NewDefault(mockResolver, 2)

			req, err := check.NewRequest(check.RequestParams{
				StoreID:              storeID,
				AuthorizationModelID: mg.GetModelID(),
				TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
			})
			require.NoError(t, err)

			resp, err := strategy.Userset(context.Background(), req, edges[0], iter)

			if tt.expectedError != nil {
				require.Error(t, err)
				require.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.expected, resp)
		})
	}
}

/*
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
			resp, err := checker.defaultTTU(ctx, req, tt.rewrite, iter)(ctx)
			require.Equal(t, tt.expectedError, err)
			require.Equal(t, tt.expected, resp)
		})
	}
}


*/
