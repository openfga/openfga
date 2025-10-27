package strategies

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	authzGraph "github.com/openfga/language/pkg/go/graph"
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
			name: "found",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:1", "member", "user:1#member"),
				tuple.NewTupleKey("group:1", "member", "user:2#member"),
			},
			expected: &check.Response{
				Allowed: true,
			},
			expectedError: nil,
			setupMock: func(mockResolver *graph.MockCheckResolver) {
				mockResolver.EXPECT().GetDelegate().Return(mockResolver)
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Cond(func(req *check.Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "user:1" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
				})).Return(&check.Response{Allowed: false}, nil)
				mockResolver.EXPECT().GetDelegate().Return(mockResolver)
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Cond(func(req *check.Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "user:2" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
				})).Return(&check.Response{Allowed: true}, nil)

			},
		},
		{
			name: "not_found",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:1", "member", "user:1#member"),
				tuple.NewTupleKey("group:1", "member", "user:2#member"),
			},
			expected: &check.Response{
				Allowed: false,
			},
			expectedError: nil,
			setupMock: func(mockResolver *graph.MockCheckResolver) {
				mockResolver.EXPECT().GetDelegate().Return(mockResolver).Times(2)
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Cond(func(req *check.Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "user:1" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
				})).Return(&check.Response{Allowed: false}, nil)
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Cond(func(req *check.Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "user:2" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
				})).Return(&check.Response{Allowed: false}, nil)
			},
		},
		{
			name:   "no_tuples",
			tuples: []*openfgav1.TupleKey{},
			expected: &check.Response{
				Allowed: false,
			},
			expectedError: nil,
			setupMock: func(mockResolver *graph.MockCheckResolver) {
				// No calls expected
			},
		},
		{
			name: "error_during_resolution_but_eventually_succeeds",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:1", "member", "user:1#member"),
				tuple.NewTupleKey("group:1", "member", "user:2#member"),
				tuple.NewTupleKey("group:1", "member", "user:3#member"),
			},
			expected: &check.Response{
				Allowed: true,
			},
			expectedError: nil,
			setupMock: func(mockResolver *graph.MockCheckResolver) {
				mockResolver.EXPECT().GetDelegate().Return(mockResolver).AnyTimes()
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).
					DoAndReturn(func(ctx context.Context, req *check.Request) (*check.Response, error) {
						tk := req.GetTupleKey()
						// First tuple returns error
						if tk.GetObject() == "user:1" {
							return nil, fmt.Errorf("temporary error")
						}
						// Second tuple succeeds
						if tk.GetObject() == "user:2" {
							return &check.Response{Allowed: true}, nil
						}
						// Third tuple not reached due to shortcircuit
						return &check.Response{Allowed: false}, nil
					}).AnyTimes()
			},
		},
		{
			name: "all_tuples_error",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:1", "member", "user:1#member"),
				tuple.NewTupleKey("group:1", "member", "user:2#member"),
			},
			expected: &check.Response{
				Allowed: false,
			},
			expectedError: fmt.Errorf("temporary error"),
			setupMock: func(mockResolver *graph.MockCheckResolver) {
				mockResolver.EXPECT().GetDelegate().Return(mockResolver).Times(2)
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).
					Return(nil, fmt.Errorf("temporary error")).
					Times(2)
			},
		},
		{
			name: "mixed_errors_and_false_results",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:1", "member", "user:1#member"),
				tuple.NewTupleKey("group:1", "member", "user:2#member"),
				tuple.NewTupleKey("group:1", "member", "user:3#member"),
			},
			expected: &check.Response{
				Allowed: false,
			},
			expectedError: fmt.Errorf("resolver error"),
			setupMock: func(mockResolver *graph.MockCheckResolver) {
				mockResolver.EXPECT().GetDelegate().Return(mockResolver).Times(3)
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Cond(func(req *check.Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "user:1"
				})).Return(&check.Response{Allowed: false}, nil)
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Cond(func(req *check.Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "user:2"
				})).Return(nil, fmt.Errorf("resolver error"))
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Cond(func(req *check.Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "user:3"
				})).Return(&check.Response{Allowed: false}, nil)
			},
		},
		{
			name: "single_tuple_error",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:1", "member", "user:1#member"),
			},
			expected: &check.Response{
				Allowed: false,
			},
			expectedError: fmt.Errorf("database error"),
			setupMock: func(mockResolver *graph.MockCheckResolver) {
				mockResolver.EXPECT().GetDelegate().Return(mockResolver).Times(1)
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Cond(func(req *check.Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "user:1" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
				})).Return(nil, fmt.Errorf("database error"))
			},
		},
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

func TestDefaultTTU(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	storeID := ulid.Make().String()

	model := testutils.MustTransformDSLToProtoWithID(`
		  model
		   schema 1.1
		
		  type user
		  type group
		   relations
			define member: [user]
		  type document
		   relations
			define viewer: member from owner
			define owner: [group]
	`)

	mg, err := check.NewAuthorizationModelGraph(model)
	require.NoError(t, err)

	edges, ok := mg.GetEdgesFromNodeId("document#viewer")
	require.True(t, ok)

	// Find the TTU edge
	var ttuEdge *authzGraph.WeightedAuthorizationModelEdge
	for _, edge := range edges {
		if edge.GetEdgeType() == authzGraph.TTUEdge {
			ttuEdge = edge
			break
		}
	}
	require.NotNil(t, ttuEdge)

	tests := []struct {
		name          string
		tuples        []*openfgav1.TupleKey
		setupMock     func(*graph.MockCheckResolver)
		expected      *check.Response
		expectedError error
	}{
		{
			name: "found",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:doc1", "owner", "group:1"),
				tuple.NewTupleKey("document:doc1", "owner", "group:2"),
			},
			expected: &check.Response{
				Allowed: true,
			},
			expectedError: nil,
			setupMock: func(mockResolver *graph.MockCheckResolver) {
				mockResolver.EXPECT().GetDelegate().Return(mockResolver)
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Cond(func(req *check.Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "group:1" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
				})).Return(&check.Response{Allowed: false}, nil)
				mockResolver.EXPECT().GetDelegate().Return(mockResolver)
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Cond(func(req *check.Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "group:2" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
				})).Return(&check.Response{Allowed: true}, nil)
			},
		},
		{
			name: "not_found",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:doc1", "owner", "group:1"),
				tuple.NewTupleKey("document:doc1", "owner", "group:2"),
			},
			expected: &check.Response{
				Allowed: false,
			},
			expectedError: nil,
			setupMock: func(mockResolver *graph.MockCheckResolver) {
				mockResolver.EXPECT().GetDelegate().Return(mockResolver).Times(2)
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Cond(func(req *check.Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "group:1" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
				})).Return(&check.Response{Allowed: false}, nil)
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Cond(func(req *check.Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "group:2" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
				})).Return(&check.Response{Allowed: false}, nil)
			},
		},
		{
			name:   "no_tuples",
			tuples: []*openfgav1.TupleKey{},
			expected: &check.Response{
				Allowed: false,
			},
			expectedError: nil,
			setupMock: func(mockResolver *graph.MockCheckResolver) {
				// No calls expected
			},
		},
		{
			name: "error_during_resolution_but_eventually_succeeds",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:doc1", "owner", "group:1"),
				tuple.NewTupleKey("document:doc1", "owner", "team:1"),
				tuple.NewTupleKey("document:doc1", "owner", "group:2"),
			},
			expected: &check.Response{
				Allowed: true,
			},
			expectedError: nil,
			setupMock: func(mockResolver *graph.MockCheckResolver) {
				mockResolver.EXPECT().GetDelegate().Return(mockResolver).AnyTimes()
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).
					DoAndReturn(func(ctx context.Context, req *check.Request) (*check.Response, error) {
						tk := req.GetTupleKey()
						// First tuple returns error
						if tk.GetObject() == "group:1" {
							return nil, fmt.Errorf("temporary error")
						}
						// Second tuple succeeds
						if tk.GetObject() == "team:1" {
							return &check.Response{Allowed: true}, nil
						}
						// Third tuple not reached due to shortcircuit
						return &check.Response{Allowed: false}, nil
					}).AnyTimes()
			},
		},
		{
			name: "all_tuples_error",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:doc1", "owner", "group:1"),
				tuple.NewTupleKey("document:doc1", "owner", "group:2"),
			},
			expected: &check.Response{
				Allowed: false,
			},
			expectedError: fmt.Errorf("temporary error"),
			setupMock: func(mockResolver *graph.MockCheckResolver) {
				mockResolver.EXPECT().GetDelegate().Return(mockResolver).Times(2)
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).
					Return(nil, fmt.Errorf("temporary error")).
					Times(2)
			},
		},
		{
			name: "mixed_errors_and_false_results",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:doc1", "owner", "group:1"),
				tuple.NewTupleKey("document:doc1", "owner", "team:1"),
				tuple.NewTupleKey("document:doc1", "owner", "group:2"),
			},
			expected: &check.Response{
				Allowed: false,
			},
			expectedError: fmt.Errorf("resolver error"),
			setupMock: func(mockResolver *graph.MockCheckResolver) {
				mockResolver.EXPECT().GetDelegate().Return(mockResolver).Times(3)
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Cond(func(req *check.Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "group:1"
				})).Return(&check.Response{Allowed: false}, nil)
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Cond(func(req *check.Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "team:1"
				})).Return(nil, fmt.Errorf("resolver error"))
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Cond(func(req *check.Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "group:2"
				})).Return(&check.Response{Allowed: false}, nil)
			},
		},
		{
			name: "single_tuple_error",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:doc1", "owner", "group:1"),
			},
			expected: &check.Response{
				Allowed: false,
			},
			expectedError: fmt.Errorf("database error"),
			setupMock: func(mockResolver *graph.MockCheckResolver) {
				mockResolver.EXPECT().GetDelegate().Return(mockResolver).Times(1)
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Cond(func(req *check.Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "group:1" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
				})).Return(nil, fmt.Errorf("database error"))
			},
		},
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
				TupleKey:             tuple.NewTupleKey("document:doc1", "viewer", "user:maria"),
			})
			require.NoError(t, err)

			resp, err := strategy.TTU(context.Background(), req, ttuEdge, iter)

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
