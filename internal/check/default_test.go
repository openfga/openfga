package check

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	authzGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestDefaultUserset(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storeID := ulid.Make().String()

	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1
	
		type user
		type group
			relations
				define member: [user with condX, user:* with condX, group#member]
		condition condX(x: int) {
			x < 100
		}`)

	mg, err := modelgraph.New(model)
	require.NoError(t, err)

	edges, ok := mg.GetEdgesFromNodeId("group#member")
	require.True(t, ok)

	tests := []struct {
		name          string
		tuples        []*openfgav1.TupleKey
		setupMock     func(*MockCheckResolver)
		expected      *Response
		expectedError error
	}{
		{
			name: "found",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:1", "member", "group:1#member"),
				tuple.NewTupleKey("group:1", "member", "group:2#member"),
			},
			expected: &Response{
				Allowed: true,
			},
			expectedError: nil,
			setupMock: func(mockResolver *MockCheckResolver) {
				mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Cond(
					func(req *Request) bool {
						tk := req.GetTupleKey()
						return tk.GetObject() == "group:1" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
					}), gomock.Any(), nil).Return(&Response{Allowed: false}, nil).MaxTimes(1)
				mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Cond(func(req *Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "group:2" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
				}), gomock.Any(), nil).Return(&Response{Allowed: true}, nil)
			},
		},
		{
			name: "not_found",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:1", "member", "group:1#member"),
				tuple.NewTupleKey("group:1", "member", "group:2#member"),
			},
			expected: &Response{
				Allowed: false,
			},
			expectedError: nil,
			setupMock: func(mockResolver *MockCheckResolver) {
				mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Cond(
					func(req *Request) bool {
						tk := req.GetTupleKey()
						return tk.GetObject() == "group:1" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
					}), gomock.Any(), nil).Return(&Response{Allowed: false}, nil)
				mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Cond(
					func(req *Request) bool {
						tk := req.GetTupleKey()
						return tk.GetObject() == "group:2" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
					}), gomock.Any(), nil).Return(&Response{Allowed: false}, nil)
			},
		},
		{
			name:   "no_tuples",
			tuples: []*openfgav1.TupleKey{},
			expected: &Response{
				Allowed: false,
			},
			expectedError: nil,
			setupMock: func(mockResolver *MockCheckResolver) {
			},
		},
		{
			name: "error_during_resolution_but_eventually_succeeds",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:1", "member", "group:1#member"),
				tuple.NewTupleKey("group:1", "member", "group:2#member"),
				tuple.NewTupleKey("group:1", "member", "group:3#member"),
			},
			expected: &Response{
				Allowed: true,
			},
			expectedError: nil,
			setupMock: func(mockResolver *MockCheckResolver) {
				mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Cond(
					func(req *Request) bool {
						tk := req.GetTupleKey()
						return tk.GetObject() == "group:1" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
					}), gomock.Any(), nil).Return(nil, fmt.Errorf("boom")).MaxTimes(1)
				mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Cond(
					func(req *Request) bool {
						tk := req.GetTupleKey()
						return tk.GetObject() == "group:2" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
					}), gomock.Any(), nil).Return(&Response{Allowed: true}, nil).MaxTimes(1)
				mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Cond(
					func(req *Request) bool {
						tk := req.GetTupleKey()
						return tk.GetObject() == "group:3" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
					}), gomock.Any(), nil).Return(&Response{Allowed: false}, nil).MaxTimes(1)
			},
		},
		{
			name: "all_tuples_error",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:1", "member", "group:1#member"),
				tuple.NewTupleKey("group:1", "member", "group:2#member"),
			},
			expected: &Response{
				Allowed: false,
			},
			expectedError: fmt.Errorf("boom"),
			setupMock: func(mockResolver *MockCheckResolver) {
				mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Any(), gomock.Any(), nil).Return(nil, fmt.Errorf("boom")).Times(2)
			},
		},
		{
			name: "mixed_errors_and_false_results",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:1", "member", "group:1#member"),
				tuple.NewTupleKey("group:1", "member", "group:2#member"),
				tuple.NewTupleKey("group:1", "member", "group:3#member"),
			},
			expected: &Response{
				Allowed: false,
			},
			expectedError: fmt.Errorf("boom"),
			setupMock: func(mockResolver *MockCheckResolver) {
				mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Cond(
					func(req *Request) bool {
						tk := req.GetTupleKey()
						return tk.GetObject() == "group:1" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
					}), gomock.Any(), nil).Return(&Response{Allowed: false}, nil)
				mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Cond(
					func(req *Request) bool {
						tk := req.GetTupleKey()
						return tk.GetObject() == "group:2" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
					}), gomock.Any(), nil).Return(nil, fmt.Errorf("boom"))
				mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Cond(
					func(req *Request) bool {
						tk := req.GetTupleKey()
						return tk.GetObject() == "group:3" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
					}), gomock.Any(), nil).Return(&Response{Allowed: false}, nil)
			},
		},
		{
			name: "single_tuple_error",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:1", "member", "group:1#member"),
			},
			expected: &Response{
				Allowed: false,
			},
			expectedError: fmt.Errorf("boom"),
			setupMock: func(mockResolver *MockCheckResolver) {
				mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Cond(
					func(req *Request) bool {
						tk := req.GetTupleKey()
						return tk.GetObject() == "group:1" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
					}), gomock.Any(), nil).Return(nil, fmt.Errorf("boom"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockResolver := NewMockCheckResolver(ctrl)
			tt.setupMock(mockResolver)

			iter := storage.NewStaticTupleKeyIterator(tt.tuples)
			strategy := NewDefault(mg, mockResolver, 2)

			req, err := NewRequest(RequestParams{
				StoreID:              storeID,
				AuthorizationModelID: mg.GetModelID(),
				TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
			})
			require.NoError(t, err)

			resp, err := strategy.Userset(context.Background(), req, edges[0], iter, nil)

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
		  type document
		   relations
			define member: [user]
			define viewer: member from owner
			define owner: [document]
	`)

	mg, err := modelgraph.New(model)
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
		setupMock     func(*MockCheckResolver)
		expected      *Response
		expectedError error
	}{
		{
			name: "found",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "owner", "document:3"),
				tuple.NewTupleKey("document:1", "owner", "document:4"),
			},
			expected: &Response{
				Allowed: true,
			},
			expectedError: nil,
			setupMock: func(mockResolver *MockCheckResolver) {
				mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Cond(func(req *Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "document:3" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
				}), gomock.Any(), nil).Return(&Response{Allowed: false}, nil).MaxTimes(1)
				mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Cond(func(req *Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "document:4" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
				}), gomock.Any(), nil).Return(&Response{Allowed: true}, nil)
			},
		},
		{
			name: "not_found",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "owner", "document:3"),
				tuple.NewTupleKey("document:1", "owner", "document:4"),
			},
			expected: &Response{
				Allowed: false,
			},
			expectedError: nil,
			setupMock: func(mockResolver *MockCheckResolver) {
				mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Cond(func(req *Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "document:3" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
				}), gomock.Any(), nil).Return(&Response{Allowed: false}, nil)
				mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Cond(func(req *Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "document:4" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
				}), gomock.Any(), nil).Return(&Response{Allowed: false}, nil)
			},
		},
		{
			name:   "no_tuples",
			tuples: []*openfgav1.TupleKey{},
			expected: &Response{
				Allowed: false,
			},
			expectedError: nil,
			setupMock: func(mockResolver *MockCheckResolver) {
			},
		},
		{
			name: "error_during_resolution_but_eventually_succeeds",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "owner", "document:3"),
				tuple.NewTupleKey("document:1", "owner", "document:4"),
				tuple.NewTupleKey("document:1", "owner", "document:5"),
			},
			expected: &Response{
				Allowed: true,
			},
			expectedError: nil,
			setupMock: func(mockResolver *MockCheckResolver) {
				mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Cond(func(req *Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "document:3" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
				}), gomock.Any(), nil).Return(&Response{Allowed: false}, nil).MaxTimes(1)
				mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Cond(func(req *Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "document:4" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
				}), gomock.Any(), nil).Return(nil, fmt.Errorf("error during resolution")).MaxTimes(1)
				mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Cond(func(req *Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "document:5" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
				}), gomock.Any(), nil).Return(&Response{Allowed: true}, nil)
			},
		},
		{
			name: "all_tuples_error",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "owner", "document:3"),
				tuple.NewTupleKey("document:1", "owner", "document:4"),
			},
			expected: &Response{
				Allowed: false,
			},
			expectedError: fmt.Errorf("boom"),
			setupMock: func(mockResolver *MockCheckResolver) {
				mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Any(), gomock.Any(), nil).Return(nil, fmt.Errorf("boom")).Times(2)
			},
		},
		{
			name: "mixed_errors_and_false_results",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "owner", "document:3"),
				tuple.NewTupleKey("document:1", "owner", "document:4"),
				tuple.NewTupleKey("document:1", "owner", "document:5"),
			},
			expected: &Response{
				Allowed: false,
			},
			expectedError: fmt.Errorf("resolver error"),
			setupMock: func(mockResolver *MockCheckResolver) {
				mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Cond(func(req *Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "document:3" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
				}), gomock.Any(), nil).Return(&Response{Allowed: false}, nil)
				mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Cond(func(req *Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "document:4" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
				}), gomock.Any(), nil).Return(nil, fmt.Errorf("resolver error"))
				mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Cond(func(req *Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "document:5" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
				}), gomock.Any(), nil).Return(&Response{Allowed: false}, nil)
			},
		},
		{
			name: "single_tuple_error",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "owner", "document:3"),
			},
			expected: &Response{
				Allowed: false,
			},
			expectedError: fmt.Errorf("boom"),
			setupMock: func(mockResolver *MockCheckResolver) {
				mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Cond(func(req *Request) bool {
					tk := req.GetTupleKey()
					return tk.GetObject() == "document:3" && tk.GetRelation() == "member" && tk.GetUser() == "user:maria"
				}), gomock.Any(), nil).Return(nil, fmt.Errorf("boom"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockResolver := NewMockCheckResolver(ctrl)
			tt.setupMock(mockResolver)

			iter := storage.NewStaticTupleKeyIterator(tt.tuples)
			strategy := NewDefault(mg, mockResolver, 2)

			req, err := NewRequest(RequestParams{
				StoreID:              storeID,
				AuthorizationModelID: mg.GetModelID(),
				TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			})
			require.NoError(t, err)

			resp, err := strategy.TTU(context.Background(), req, ttuEdge, iter, nil)

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
