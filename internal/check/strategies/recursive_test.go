package strategies

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	authzGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/check"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestRecursiveTTU(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name                            string
		readStartingWithUserTuples      []*openfgav1.Tuple
		readStartingWithUserTuplesError error
		readTuples                      [][]*openfgav1.Tuple
		readTuplesError                 error
		expected                        *check.Response
		expectedError                   error
	}{
		{
			name:                       "no_user_assigned_to_group",
			readStartingWithUserTuples: []*openfgav1.Tuple{},
			readTuples: [][]*openfgav1.Tuple{
				{
					{
						Key: tuple.NewTupleKey("group:1", "parent", "group:3"),
					},
				},
				{},
			},
			expected: &check.Response{
				Allowed: false,
			},
		},
		{
			name: "user_assigned_to_first_level_sub_group",
			readStartingWithUserTuples: []*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:3", "member", "user:maria"),
				},
				{
					Key: tuple.NewTupleKey("group:4", "member", "user:maria"),
				},
			},
			readTuples: [][]*openfgav1.Tuple{
				{
					{
						Key: tuple.NewTupleKey("group:1", "parent", "group:3"),
					},
				},
			},
			expected: &check.Response{
				Allowed: true,
			},
		},
		{
			name: "user_assigned_to_second_level_sub_group",
			readStartingWithUserTuples: []*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:3", "member", "user:maria"),
				},
				{
					Key: tuple.NewTupleKey("group:4", "member", "user:maria"),
				},
			},
			readTuples: [][]*openfgav1.Tuple{
				{
					{
						Key: tuple.NewTupleKey("group:1", "parent", "group:5"),
					},
				},
				{
					{
						Key: tuple.NewTupleKey("group:5", "parent", "group:3"),
					},
				},
			},
			expected: &check.Response{
				Allowed: true,
			},
		},
		{
			name: "user_not_assigned_to_sub_group",
			readStartingWithUserTuples: []*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:3", "member", "user:maria"),
				},
				{
					Key: tuple.NewTupleKey("group:4", "member", "user:maria"),
				},
			},
			readTuples: [][]*openfgav1.Tuple{
				{
					{
						Key: tuple.NewTupleKey("group:1", "parent", "group:2"),
					},
				},
				{},
			},
			expected: &check.Response{
				Allowed: false,
			},
		},
		{
			name:                            "error_getting_tuple",
			readStartingWithUserTuples:      []*openfgav1.Tuple{},
			readStartingWithUserTuplesError: fmt.Errorf("mock error"),
			readTuples: [][]*openfgav1.Tuple{
				{
					{
						Key: tuple.NewTupleKey("group:1", "parent", "group:2"),
					},
				},
				{},
			},
			expected:      nil,
			expectedError: fmt.Errorf("mock error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user] or member from parent
						define parent: [group]
				`)

			mg, err := check.NewAuthorizationModelGraph(model)
			require.NoError(t, err)

			edges, ok := mg.GetEdgesFromNodeId("group#member")
			require.True(t, ok)

			ttuEdge := edges[0]

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			storeID := ulid.Make().String()

			mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
			mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
				ObjectType: "group",
				Relation:   "member",
				UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
				ObjectIDs:  nil,
			}, storage.ReadStartingWithUserOptions{
				Consistency:                storage.ConsistencyOptions{Preference: openfgav1.ConsistencyPreference_UNSPECIFIED},
				WithResultsSortedAscending: true},
			).MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuples), tt.readStartingWithUserTuplesError)

			for _, tuples := range tt.readTuples[1:] {
				mockDatastore.EXPECT().Read(gomock.Any(), storeID, gomock.Any(), gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tuples), tt.readTuplesError)
			}

			ctx := context.Background()
			req := &check.Request{
				StoreID:              storeID,
				AuthorizationModelID: mg.GetModelID(),
				TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
			}

			tupleKeys := make([]*openfgav1.TupleKey, 0, len(tt.readTuples[0]))
			for _, t := range tt.readTuples[0] {
				k := t.GetKey()
				tupleKeys = append(tupleKeys, &openfgav1.TupleKey{
					User:     k.GetUser(),
					Relation: k.GetRelation(),
					Object:   k.GetObject(),
				})
			}

			strategy := NewRecursive(mg, mockDatastore, 5)

			result, err := strategy.TTU(ctx, req, ttuEdge, storage.NewStaticTupleKeyIterator(tupleKeys))
			require.Equal(t, tt.expectedError, err)
			require.Equal(t, tt.expected.GetAllowed(), result.GetAllowed())
			require.Equal(t, tt.expected.GetResolutionMetadata(), result.GetResolutionMetadata())
		})
	}

	t.Run("complex_model", func(t *testing.T) {
		model := testutils.MustTransformDSLToProtoWithID(`
model
	schema 1.1

type user
type group
	relations
		define member: member from parent or (rel2 but not rel6)
		define parent: [group]
		define rel2: (rel4 or (rel7 and rel8)) but not rel5
		define rel4: [user]
		define rel5: [user]
		define rel6: [user]
		define rel7: [user]
		define rel8: [user]
		`)

		mg, err := check.NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("group#member")
		require.True(t, ok)

		var ttuEdge *authzGraph.WeightedAuthorizationModelEdge
		for _, edge := range edges {
			if edge.GetEdgeType() == authzGraph.TTUEdge {
				ttuEdge = edge
				break
			}
		}
		require.NotNil(t, ttuEdge)

		tests := []struct {
			name                             string
			readStartingWithUserTuplesMember []*openfgav1.Tuple
			readStartingWithUserTuplesRel4   []*openfgav1.Tuple
			readStartingWithUserTuplesRel5   []*openfgav1.Tuple
			readStartingWithUserTuplesRel6   []*openfgav1.Tuple
			readStartingWithUserTuplesRel7   []*openfgav1.Tuple
			readStartingWithUserTuplesRel8   []*openfgav1.Tuple
			readStartingWithUserTuplesError  error
			readTuples                       [][]*openfgav1.Tuple
			readTuplesError                  error
			expected                         *check.Response
			expectedError                    error
		}{
			{
				name: "no_user_assigned_to_group",
				readTuples: [][]*openfgav1.Tuple{
					{
						{
							Key: tuple.NewTupleKey("group:1", "parent", "group:3"),
						},
					},
					{},
				},
				expected: &check.Response{
					Allowed: false,
				},
			},
			{
				name: "user_assigned_to_group_recursively",
				readStartingWithUserTuplesRel4: []*openfgav1.Tuple{
					{
						Key: tuple.NewTupleKey("group:3", "rel4", "user:maria"),
					},
				},
				readTuples: [][]*openfgav1.Tuple{
					{
						{
							Key: tuple.NewTupleKey("group:6", "parent", "group:5"),
						},
					}, {
						{
							Key: tuple.NewTupleKey("group:5", "parent", "group:3"),
						},
					},
				},
				expected: &check.Response{
					Allowed: true,
				},
			},
			{
				name: "user_assigned_via_rel4",
				readStartingWithUserTuplesRel4: []*openfgav1.Tuple{
					{
						Key: tuple.NewTupleKey("group:3", "rel4", "user:maria"),
					},
				},
				readTuples: [][]*openfgav1.Tuple{
					{
						{
							Key: tuple.NewTupleKey("group:1", "parent", "group:3"),
						},
					},
				},
				expected: &check.Response{
					Allowed: true,
				},
			},
			{
				name: "user_assigned_via_rel4_but_denied_rel5",
				readStartingWithUserTuplesRel4: []*openfgav1.Tuple{
					{
						Key: tuple.NewTupleKey("group:3", "rel4", "user:maria"),
					},
				},
				readStartingWithUserTuplesRel5: []*openfgav1.Tuple{
					{
						Key: tuple.NewTupleKey("group:3", "rel5", "user:maria"),
					},
				},
				readTuples: [][]*openfgav1.Tuple{
					{
						{
							Key: tuple.NewTupleKey("group:5", "parent", "group:5"),
						},
					},
				},
				expected: &check.Response{
					Allowed: false,
				},
			},
			{
				name: "user_assigned_via_rel4_but_denied_rel6",
				readStartingWithUserTuplesRel4: []*openfgav1.Tuple{
					{
						Key: tuple.NewTupleKey("group:3", "rel4", "user:maria"),
					},
				},
				readStartingWithUserTuplesRel6: []*openfgav1.Tuple{
					{
						Key: tuple.NewTupleKey("group:3", "rel6", "user:maria"),
					},
				},
				readTuples: [][]*openfgav1.Tuple{
					{
						{
							Key: tuple.NewTupleKey("group:1", "parent", "group:3"),
						},
					},
				},
				expected: &check.Response{
					Allowed: false,
				},
			},
			{
				name: "user_assigned_via_rel7_and_rel8",
				readStartingWithUserTuplesRel7: []*openfgav1.Tuple{
					{
						Key: tuple.NewTupleKey("group:3", "rel7", "user:maria"),
					},
				},
				readStartingWithUserTuplesRel8: []*openfgav1.Tuple{
					{
						Key: tuple.NewTupleKey("group:3", "rel8", "user:maria"),
					},
				},
				readTuples: [][]*openfgav1.Tuple{
					{
						{
							Key: tuple.NewTupleKey("group:1", "parent", "group:3"),
						},
					},
				},
				expected: &check.Response{
					Allowed: true,
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				storeID := ulid.Make().String()
				mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)

				mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
					ObjectType: "group",
					Relation:   "rel4",
					UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
					ObjectIDs:  nil,
				}, gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuplesRel4), tt.readStartingWithUserTuplesError)

				mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
					ObjectType: "group",
					Relation:   "rel7",
					UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
					ObjectIDs:  nil,
				}, gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuplesRel7), tt.readStartingWithUserTuplesError)

				mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
					ObjectType: "group",
					Relation:   "rel8",
					UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
					ObjectIDs:  nil,
				}, gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuplesRel8), tt.readStartingWithUserTuplesError)

				mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
					ObjectType: "group",
					Relation:   "rel5",
					UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
					ObjectIDs:  nil,
				}, gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuplesRel5), tt.readStartingWithUserTuplesError)

				mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
					ObjectType: "group",
					Relation:   "rel6",
					UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
					ObjectIDs:  nil,
				}, gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuplesRel6), tt.readStartingWithUserTuplesError)

				mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
					ObjectType: "group",
					Relation:   "member",
					UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
					ObjectIDs:  nil,
				}, gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuplesMember), tt.readStartingWithUserTuplesError)

				for _, tuples := range tt.readTuples[1:] {
					mockDatastore.EXPECT().Read(gomock.Any(), storeID, gomock.Any(), gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tuples), tt.readTuplesError)
				}

				ctx := context.Background()
				req := &check.Request{
					StoreID:              storeID,
					AuthorizationModelID: mg.GetModelID(),
					TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
				}

				tupleKeys := make([]*openfgav1.TupleKey, 0, len(tt.readTuples[0]))
				for _, t := range tt.readTuples[0] {
					k := t.GetKey()
					tupleKeys = append(tupleKeys, &openfgav1.TupleKey{
						User:     k.GetUser(),
						Relation: k.GetRelation(),
						Object:   k.GetObject(),
					})
				}

				strategy := NewRecursive(mg, mockDatastore, 5)
				result, err := strategy.TTU(ctx, req, ttuEdge, storage.NewStaticTupleKeyIterator(tupleKeys))
				require.Equal(t, tt.expectedError, err)
				require.Equal(t, tt.expected.GetAllowed(), result.GetAllowed())
				require.Equal(t, tt.expected.GetResolutionMetadata(), result.GetResolutionMetadata())
			})
		}
	})
}

func TestRecursiveUserset(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	tests := []struct {
		name                            string
		readStartingWithUserTuples      []*openfgav1.Tuple
		readStartingWithUserTuplesError error
		readUsersetTuples               [][]*openfgav1.Tuple
		readUsersetTuplesError          error
		expected                        *check.Response
		expectedError                   error
	}{
		{
			name:                       "no_user_assigned_to_group",
			readStartingWithUserTuples: []*openfgav1.Tuple{},
			readUsersetTuples: [][]*openfgav1.Tuple{
				{
					{
						Key: tuple.NewTupleKey("group:1", "member", "group:3#member"),
					},
				},
				{},
			},
			expected: &check.Response{
				Allowed: false,
			},
		},
		{
			name: "user_assigned_to_first_level_sub_group",
			readStartingWithUserTuples: []*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:3", "member", "user:maria"),
				},
				{
					Key: tuple.NewTupleKey("group:4", "member", "user:maria"),
				},
			},
			readUsersetTuples: [][]*openfgav1.Tuple{
				{
					{
						Key: tuple.NewTupleKey("group:1", "member", "group:3#member"),
					},
				},
			},
			expected: &check.Response{
				Allowed: true,
			},
		},
		{
			name: "user_assigned_to_second_level_sub_group",
			readStartingWithUserTuples: []*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:3", "member", "user:maria"),
				},
				{
					Key: tuple.NewTupleKey("group:4", "member", "user:maria"),
				},
			},
			readUsersetTuples: [][]*openfgav1.Tuple{
				{
					{
						Key: tuple.NewTupleKey("group:6", "member", "group:5#member"),
					},
				},
				{
					{
						Key: tuple.NewTupleKey("group:5", "member", "group:3#member"),
					},
				},
			},
			expected: &check.Response{
				Allowed: true,
			},
		},
		{
			name: "user_not_assigned_to_sub_group",
			readStartingWithUserTuples: []*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:3", "member", "user:maria"),
				},
				{
					Key: tuple.NewTupleKey("group:4", "member", "user:maria"),
				},
			},
			readUsersetTuples: [][]*openfgav1.Tuple{
				{
					{
						Key: tuple.NewTupleKey("group:1", "member", "group:2#member"),
					},
				},
				{},
			},
			expected: &check.Response{
				Allowed: false,
			},
		},
		{
			name:                            "error_getting_tuple",
			readStartingWithUserTuples:      []*openfgav1.Tuple{},
			readStartingWithUserTuplesError: fmt.Errorf("mock error"),
			readUsersetTuples: [][]*openfgav1.Tuple{
				{
					{
						Key: tuple.NewTupleKey("group:1", "member", "group:2#member"),
					},
				},
				{},
			},
			expected:      nil,
			expectedError: fmt.Errorf("mock error"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			storeID := ulid.Make().String()
			mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)

			mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
				ObjectType: "group",
				Relation:   "member",
				UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
				ObjectIDs:  nil,
			}, gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuples), tt.readStartingWithUserTuplesError)

			for _, tuples := range tt.readUsersetTuples[1:] {
				mockDatastore.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, gomock.Any(), gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tuples), tt.readUsersetTuplesError)
			}
			model := testutils.MustTransformDSLToProtoWithID(`
						model
							schema 1.1

						type user
						type group
							relations
								define member: [user, group#member]
`)

			mg, err := check.NewAuthorizationModelGraph(model)
			require.NoError(t, err)

			edges, ok := mg.GetEdgesFromNodeId("group#member")
			require.True(t, ok)

			ctx := context.Background()

			req := &check.Request{
				StoreID:              storeID,
				AuthorizationModelID: mg.GetModelID(),
				TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
			}

			tupleKeys := make([]*openfgav1.TupleKey, 0, len(tt.readUsersetTuples[0]))
			for _, t := range tt.readUsersetTuples[0] {
				k := t.GetKey()
				tupleKeys = append(tupleKeys, &openfgav1.TupleKey{
					User:     k.GetUser(),
					Relation: k.GetRelation(),
					Object:   k.GetObject(),
				})
			}

			strategy := NewRecursive(mg, mockDatastore, 5)
			result, err := strategy.Userset(ctx, req, edges[0], storage.NewStaticTupleKeyIterator(tupleKeys))
			require.Equal(t, tt.expectedError, err)
			require.Equal(t, tt.expected.GetAllowed(), result.GetAllowed())
			require.Equal(t, tt.expected.GetResolutionMetadata(), result.GetResolutionMetadata())
		})
	}

	t.Run("complex_model", func(t *testing.T) {
		tests := []struct {
			name                             string
			readStartingWithUserTuplesMember []*openfgav1.Tuple
			readStartingWithUserTuplesRel4   []*openfgav1.Tuple
			readStartingWithUserTuplesRel5   []*openfgav1.Tuple
			readStartingWithUserTuplesRel6   []*openfgav1.Tuple
			readStartingWithUserTuplesRel7   []*openfgav1.Tuple
			readStartingWithUserTuplesRel8   []*openfgav1.Tuple
			readStartingWithUserTuplesError  error
			readUsersetTuples                [][]*openfgav1.Tuple
			readUsersetTuplesError           error
			expected                         *check.Response
			expectedError                    error
		}{
			{
				name: "no_user_assigned_to_group",
				readUsersetTuples: [][]*openfgav1.Tuple{
					{
						{
							Key: tuple.NewTupleKey("group:1", "member", "group:3#member"),
						},
					},
					{},
				},
				expected: &check.Response{
					Allowed: false,
				},
			},
			{
				name: "user_assigned_to_group_recursively",
				readStartingWithUserTuplesRel4: []*openfgav1.Tuple{
					{
						Key: tuple.NewTupleKey("group:3", "rel4", "user:maria"),
					},
				},
				readUsersetTuples: [][]*openfgav1.Tuple{
					{
						{
							Key: tuple.NewTupleKey("group:6", "member", "group:5#member"),
						},
					}, {
						{
							Key: tuple.NewTupleKey("group:5", "member", "group:3#member"),
						},
					},
				},
				expected: &check.Response{
					Allowed: true,
				},
			},
			{
				name: "user_assigned_via_rel4",
				readStartingWithUserTuplesRel4: []*openfgav1.Tuple{
					{
						Key: tuple.NewTupleKey("group:3", "rel4", "user:maria"),
					},
				},
				readUsersetTuples: [][]*openfgav1.Tuple{
					{
						{
							Key: tuple.NewTupleKey("group:1", "member", "group:3#member"),
						},
					},
				},
				expected: &check.Response{
					Allowed: true,
				},
			},
			{
				name: "user_assigned_via_rel4_but_denied_rel5",
				readStartingWithUserTuplesRel4: []*openfgav1.Tuple{
					{
						Key: tuple.NewTupleKey("group:3", "rel4", "user:maria"),
					},
				},
				readStartingWithUserTuplesRel5: []*openfgav1.Tuple{
					{
						Key: tuple.NewTupleKey("group:3", "rel5", "user:maria"),
					},
				},
				readUsersetTuples: [][]*openfgav1.Tuple{
					{
						{
							Key: tuple.NewTupleKey("group:5", "member", "group:3#member"),
						},
					},
				},
				expected: &check.Response{
					Allowed: false,
				},
			},
			{
				name: "user_assigned_via_rel4_but_denied_rel6",
				readStartingWithUserTuplesRel4: []*openfgav1.Tuple{
					{
						Key: tuple.NewTupleKey("group:3", "rel4", "user:maria"),
					},
				},
				readStartingWithUserTuplesRel6: []*openfgav1.Tuple{
					{
						Key: tuple.NewTupleKey("group:3", "rel6", "user:maria"),
					},
				},
				readUsersetTuples: [][]*openfgav1.Tuple{
					{
						{
							Key: tuple.NewTupleKey("group:1", "member", "group:3#member"),
						},
					},
				},
				expected: &check.Response{
					Allowed: false,
				},
			},
			{
				name: "user_assigned_via_rel7_and_rel8",
				readStartingWithUserTuplesRel7: []*openfgav1.Tuple{
					{
						Key: tuple.NewTupleKey("group:3", "rel7", "user:maria"),
					},
				},
				readStartingWithUserTuplesRel8: []*openfgav1.Tuple{
					{
						Key: tuple.NewTupleKey("group:3", "rel8", "user:maria"),
					},
				},
				readUsersetTuples: [][]*openfgav1.Tuple{
					{
						{
							Key: tuple.NewTupleKey("group:1", "member", "group:3#member"),
						},
					},
				},
				expected: &check.Response{
					Allowed: true,
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				storeID := ulid.Make().String()
				mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
				mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
					ObjectType: "group",
					Relation:   "member",
					UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
					ObjectIDs:  nil,
				}, gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuplesMember), tt.readStartingWithUserTuplesError)

				mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
					ObjectType: "group",
					Relation:   "rel4",
					UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
					ObjectIDs:  nil,
				}, gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuplesRel4), tt.readStartingWithUserTuplesError)

				mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
					ObjectType: "group",
					Relation:   "rel7",
					UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
					ObjectIDs:  nil,
				}, gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuplesRel7), tt.readStartingWithUserTuplesError)

				mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
					ObjectType: "group",
					Relation:   "rel8",
					UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
					ObjectIDs:  nil,
				}, gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuplesRel8), tt.readStartingWithUserTuplesError)

				mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
					ObjectType: "group",
					Relation:   "rel5",
					UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
					ObjectIDs:  nil,
				}, gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuplesRel5), tt.readStartingWithUserTuplesError)

				mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
					ObjectType: "group",
					Relation:   "rel6",
					UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
					ObjectIDs:  nil,
				}, gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuplesRel6), tt.readStartingWithUserTuplesError)

				for _, tuples := range tt.readUsersetTuples[1:] {
					mockDatastore.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, gomock.Any(), gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tuples), tt.readUsersetTuplesError)
				}
				model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1

		type user
		type employee
		type group
			relations
				define member: [group#member, user, employee, group#rel3] or (rel2 but not rel6)
				define rel2: (rel4 or (rel7 and rel8)) but not rel5
				define rel3: [employee]
				define rel4: [user]
				define rel5: [user]
				define rel6: [user]
				define rel7: [user]
				define rel8: [user]
		`)

				mg, err := check.NewAuthorizationModelGraph(model)
				require.NoError(t, err)

				edges, ok := mg.GetEdgesFromNodeId("group#member")
				require.True(t, ok)

				ctx := context.Background()

				req := &check.Request{
					StoreID:              storeID,
					AuthorizationModelID: mg.GetModelID(),
					TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
				}

				tupleKeys := make([]*openfgav1.TupleKey, 0, len(tt.readUsersetTuples[0]))
				for _, t := range tt.readUsersetTuples[0] {
					k := t.GetKey()
					tupleKeys = append(tupleKeys, &openfgav1.TupleKey{
						User:     k.GetUser(),
						Relation: k.GetRelation(),
						Object:   k.GetObject(),
					})
				}

				strategy := NewRecursive(mg, mockDatastore, 5)
				result, err := strategy.Userset(ctx, req, edges[0], storage.NewStaticTupleKeyIterator(tupleKeys))
				require.Equal(t, tt.expectedError, err)
				require.Equal(t, tt.expected.GetAllowed(), result.GetAllowed())
				require.Equal(t, tt.expected.GetResolutionMetadata(), result.GetResolutionMetadata())
			})
		}
	})
}

func TestBreadthFirstRecursiveMatch(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name                 string
		currentLevelUsersets map[string]struct{}
		usersetFromUser      map[string]struct{}
		readMocks            [][]*openfgav1.Tuple
		expected             bool
	}{
		{
			name:                 "empty_userset",
			currentLevelUsersets: map[string]struct{}{},
			usersetFromUser:      map[string]struct{}{},
		},
		{
			name:                 "duplicates_no_match_no_recursion",
			currentLevelUsersets: map[string]struct{}{"group:1": {}, "group:2": {}, "group:3": {}},
			usersetFromUser:      map[string]struct{}{},
			readMocks: [][]*openfgav1.Tuple{
				{{}},
				{{}},
				{{}},
			},
		},
		{
			name:                 "duplicates_no_match_with_recursion",
			currentLevelUsersets: map[string]struct{}{"group:1": {}, "group:2": {}, "group:3": {}},
			usersetFromUser:      map[string]struct{}{},
			readMocks: [][]*openfgav1.Tuple{
				{{Key: tuple.NewTupleKey("group:1", "parent", "group:3")}},
				{{Key: tuple.NewTupleKey("group:3", "parent", "group:2")}},
				{{Key: tuple.NewTupleKey("group:2", "parent", "group:1")}},
			},
		},
		{
			name:                 "duplicates_match_with_recursion",
			currentLevelUsersets: map[string]struct{}{"group:1": {}, "group:2": {}, "group:3": {}},
			usersetFromUser:      map[string]struct{}{"group:4": {}},
			readMocks: [][]*openfgav1.Tuple{
				{{Key: tuple.NewTupleKey("group:1", "parent", "group:3")}},
				{{Key: tuple.NewTupleKey("group:2", "parent", "group:1")}},
				{{Key: tuple.NewTupleKey("group:3", "parent", "group:4")}},
			},
			expected: true,
		},
		{
			name:                 "no_duplicates_no_match_counts",
			currentLevelUsersets: map[string]struct{}{"group:1": {}, "group:2": {}, "group:3": {}},
			usersetFromUser:      map[string]struct{}{},
			readMocks: [][]*openfgav1.Tuple{
				{{Key: tuple.NewTupleKey("group:1", "parent", "group:4")}},
				{{Key: tuple.NewTupleKey("group:2", "parent", "group:5")}},
				{{Key: tuple.NewTupleKey("group:3", "parent", "group:6")}},
				{{Key: tuple.NewTupleKey("group:6", "parent", "group:9")}},
				{{Key: tuple.NewTupleKey("group:7", "parent", "group:10")}},
				{{Key: tuple.NewTupleKey("group:8", "parent", "group:11")}},
				{{}},
				{{}},
				{{}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			storeID := ulid.Make().String()

			mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
			for _, mock := range tt.readMocks {
				mockDatastore.EXPECT().Read(gomock.Any(), storeID, gomock.Any(), gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(mock), nil)
			}

			model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user] or member from parent
						define parent: [group]
				`)

			mg, err := check.NewAuthorizationModelGraph(model)
			require.NoError(t, err)

			edges, ok := mg.GetEdgesFromNodeId("group#member")
			require.True(t, ok)

			ctx := context.Background()

			req := &check.Request{
				StoreID:              storeID,
				AuthorizationModelID: ulid.Make().String(),
				TupleKey:             tuple.NewTupleKey("group:3", "member", "user:maria"),
			}

			checkOutcomeChan := make(chan check.ResponseMsg, 100) // large buffer since there is no need to concurrently evaluate partial results
			strategy := NewRecursive(mg, mockDatastore, 10)
			strategy.breadthFirstRecursiveMatch(ctx, req, edges[0], &sync.Map{}, tt.currentLevelUsersets, tt.usersetFromUser, checkOutcomeChan)

			result := false
			for outcome := range checkOutcomeChan {
				if outcome.Res.Allowed {
					result = true
					break
				}
			}
			require.Equal(t, tt.expected, result)
		})
	}
	t.Run("context_cancelled", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()

		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		ctx := context.Background()
		ctx, cancel := context.WithCancel(ctx)
		// Stop is called under race conditions thus is not guaranteed these observers may see a call to it
		iter1 := mocks.NewMockIterator[*openfgav1.Tuple](ctrl)
		iter1.EXPECT().Stop().MaxTimes(1)
		iter1.EXPECT().Next(gomock.Any()).MaxTimes(1).Return(nil, storage.ErrIteratorDone)
		iter2 := mocks.NewMockIterator[*openfgav1.Tuple](ctrl)
		iter2.EXPECT().Stop().MaxTimes(1)
		iter2.EXPECT().Next(gomock.Any()).MaxTimes(1).Return(nil, storage.ErrIteratorDone)
		iter3 := mocks.NewMockIterator[*openfgav1.Tuple](ctrl)
		iter3.EXPECT().Stop().MaxTimes(1)
		iter3.EXPECT().Next(gomock.Any()).MaxTimes(1).Return(nil, storage.ErrIteratorDone)
		// currentUsersetLevel.Values() doesn't return results in order, thus there is no guarantee that `Times` will be consistent as it can return err due to context being cancelled
		mockDatastore.EXPECT().Read(gomock.Any(), storeID, gomock.Any(), gomock.Any()).MaxTimes(1).Return(iter1, nil)
		mockDatastore.EXPECT().Read(gomock.Any(), storeID, gomock.Any(), gomock.Any()).MaxTimes(1).DoAndReturn(func(ctx context.Context, store string, filter storage.ReadFilter, options storage.ReadOptions) (storage.TupleIterator, error) {
			cancel()
			return iter2, nil
		})
		mockDatastore.EXPECT().Read(gomock.Any(), storeID, gomock.Any(), gomock.Any()).MaxTimes(1).Return(iter3, nil)

		model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user] or member from parent
						define parent: [group]
				`)

		mg, err := check.NewAuthorizationModelGraph(model)
		require.NoError(t, err)
		edges, ok := mg.GetEdgesFromNodeId("group#member")
		require.True(t, ok)

		req := &check.Request{
			StoreID:              storeID,
			AuthorizationModelID: ulid.Make().String(),
			TupleKey:             tuple.NewTupleKey("group:3", "member", "user:maria"),
		}

		strategy := NewRecursive(mg, mockDatastore, 10)
		checkOutcomeChan := make(chan check.ResponseMsg, 100)
		strategy.breadthFirstRecursiveMatch(ctx, req, edges[0], &sync.Map{}, map[string]struct{}{"group:1": {}, "group:2": {}, "group:3": {}}, make(map[string]struct{}), checkOutcomeChan)
	})
}
