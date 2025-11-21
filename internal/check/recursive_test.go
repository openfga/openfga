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
	"github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/internal/modelgraph"
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
		expected                        *Response
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
			expected: &Response{
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
			expected: &Response{
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
			expected: &Response{
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
			expected: &Response{
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

			mg, err := modelgraph.New(model)
			require.NoError(t, err)

			node, ok := mg.GetNodeByID("group#member")
			require.True(t, ok)
			recursiveEdge, ok := mg.CanApplyRecursion(node, "user", true)
			require.True(t, ok)
			require.NotNil(t, recursiveEdge)
			require.Equal(t, graph.TTUEdge, recursiveEdge.GetEdgeType())
			require.Equal(t, "group#member", recursiveEdge.GetRecursiveRelation())
			require.Equal(t, recursiveEdge.GetTo(), node)

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			storeID := ulid.Make().String()

			mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
			mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
				ObjectType: "group",
				Relation:   "member",
				UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
				Conditions: []string{""},
			}, storage.ReadStartingWithUserOptions{
				Consistency: storage.ConsistencyOptions{Preference: openfgav1.ConsistencyPreference_UNSPECIFIED},
			},
			).MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuples), tt.readStartingWithUserTuplesError)

			for _, tuples := range tt.readTuples[1:] {
				mockDatastore.EXPECT().Read(gomock.Any(), storeID, gomock.Any(), gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tuples), tt.readTuplesError)
			}

			ctx := context.Background()
			req, err := NewRequest(RequestParams{
				StoreID:              storeID,
				Model: mg,
				TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
			})
			require.NoError(t, err)

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

			result, err := strategy.TTU(ctx, req, recursiveEdge, storage.NewStaticTupleKeyIterator(tupleKeys), nil)
			require.Equal(t, tt.expectedError, err)
			if tt.expected != nil {
				require.Equal(t, tt.expected.GetAllowed(), result.GetAllowed())
			}
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

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		node, ok := mg.GetNodeByID("group#member")
		require.True(t, ok)
		recursiveEdge, ok := mg.CanApplyRecursion(node, "user", true)

		require.True(t, ok)
		require.NotNil(t, recursiveEdge)
		require.Equal(t, graph.TTUEdge, recursiveEdge.GetEdgeType())
		require.Equal(t, "group#member", recursiveEdge.GetRecursiveRelation())
		require.Equal(t, recursiveEdge.GetTo(), node)

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
			expected                         *Response
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
				expected: &Response{
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
				expected: &Response{
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
				expected: &Response{
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
				expected: &Response{
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
				expected: &Response{
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
				expected: &Response{
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
					Conditions: []string{""},
				}, gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuplesRel4), tt.readStartingWithUserTuplesError)

				mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
					ObjectType: "group",
					Relation:   "rel7",
					UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
					Conditions: []string{""},
				}, gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuplesRel7), tt.readStartingWithUserTuplesError)

				mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
					ObjectType: "group",
					Relation:   "rel8",
					UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
					Conditions: []string{""},
				}, gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuplesRel8), tt.readStartingWithUserTuplesError)

				mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
					ObjectType: "group",
					Relation:   "rel5",
					UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
					Conditions: []string{""},
				}, gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuplesRel5), tt.readStartingWithUserTuplesError)

				mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
					ObjectType: "group",
					Relation:   "rel6",
					UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
					Conditions: []string{""},
				}, gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuplesRel6), tt.readStartingWithUserTuplesError)

				mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
					ObjectType: "group",
					Relation:   "member",
					UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
					Conditions: []string{""},
				}, gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuplesMember), tt.readStartingWithUserTuplesError)

				for _, tuples := range tt.readTuples[1:] {
					mockDatastore.EXPECT().Read(gomock.Any(), storeID, gomock.Any(), gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tuples), tt.readTuplesError)
				}

				ctx := context.Background()
				req, err := NewRequest(RequestParams{
					StoreID:              storeID,
					Model: mg,
					TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
				})
				require.NoError(t, err)

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
				result, err := strategy.TTU(ctx, req, recursiveEdge, storage.NewStaticTupleKeyIterator(tupleKeys), nil)
				require.Equal(t, tt.expectedError, err)
				require.Equal(t, tt.expected.GetAllowed(), result.GetAllowed())
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
		expected                        *Response
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
			expected: &Response{
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
			expected: &Response{
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
			expected: &Response{
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
			expected: &Response{
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
				Conditions: []string{""},
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

			mg, err := modelgraph.New(model)
			require.NoError(t, err)

			node, ok := mg.GetNodeByID("group#member")
			require.True(t, ok)
			recursiveEdge, ok := mg.CanApplyRecursion(node, "user", true)
			require.True(t, ok)
			require.NotNil(t, recursiveEdge)

			ctx := context.Background()

			req, err := NewRequest(RequestParams{
				StoreID:              storeID,
				Model: mg,
				TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
			})
			require.NoError(t, err)

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
			result, err := strategy.Userset(ctx, req, recursiveEdge, storage.NewStaticTupleKeyIterator(tupleKeys), nil)
			require.Equal(t, tt.expectedError, err)
			if tt.expected != nil {
				require.Equal(t, tt.expected.GetAllowed(), result.GetAllowed())
			}
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
			expected                         *Response
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
				expected: &Response{
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
				expected: &Response{
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
				expected: &Response{
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
				expected: &Response{
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
				expected: &Response{
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
				expected: &Response{
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

				mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
					MaxTimes(6). // Allow any number of calls
					DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadStartingWithUserFilter, opts storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
						// Manually check the relation and return the right data
						switch filter.Relation {
						case "rel4":
							return storage.NewStaticTupleIterator(tt.readStartingWithUserTuplesRel4), tt.readStartingWithUserTuplesError
						case "rel7":
							return storage.NewStaticTupleIterator(tt.readStartingWithUserTuplesRel7), tt.readStartingWithUserTuplesError
						case "rel8":
							return storage.NewStaticTupleIterator(tt.readStartingWithUserTuplesRel8), tt.readStartingWithUserTuplesError
						case "rel5":
							return storage.NewStaticTupleIterator(tt.readStartingWithUserTuplesRel5), tt.readStartingWithUserTuplesError
						case "rel6":
							return storage.NewStaticTupleIterator(tt.readStartingWithUserTuplesRel6), tt.readStartingWithUserTuplesError
						case "member":
							return storage.NewStaticTupleIterator(tt.readStartingWithUserTuplesMember), tt.readStartingWithUserTuplesError
						default:
							return nil, fmt.Errorf("unexpected relation %s", filter.Relation)
						}
					})

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

				mg, err := modelgraph.New(model)
				require.NoError(t, err)

				node, ok := mg.GetNodeByID("group#member")
				require.True(t, ok)
				recursiveEdge, ok := mg.CanApplyRecursion(node, "user", true)
				require.True(t, ok)
				require.NotNil(t, recursiveEdge)

				ctx := context.Background()

				req, err := NewRequest(RequestParams{
					StoreID:              storeID,
					Model: mg,
					TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
				})
				require.NoError(t, err)

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
				result, err := strategy.Userset(ctx, req, recursiveEdge, storage.NewStaticTupleKeyIterator(tupleKeys), nil)
				require.Equal(t, tt.expectedError, err)
				require.Equal(t, tt.expected.GetAllowed(), result.GetAllowed())
			})
		}
	})
}

func TestRecursiveMatch(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name          string
		idsFromObject map[string]struct{}
		idsFromUser   map[string]struct{}
		readMocks     [][]*openfgav1.Tuple
		expected      bool
	}{
		{
			name:          "empty",
			idsFromObject: map[string]struct{}{},
			idsFromUser:   map[string]struct{}{},
		},
		{
			name:          "duplicates_no_match_no_recursion",
			idsFromObject: map[string]struct{}{"group:1": {}, "group:2": {}, "group:3": {}},
			idsFromUser:   map[string]struct{}{"group:4": {}},
			readMocks: [][]*openfgav1.Tuple{
				{},
				{},
				{},
			},
		},
		{
			name:          "duplicates_no_match_with_recursion",
			idsFromObject: map[string]struct{}{"group:1": {}, "group:2": {}, "group:3": {}},
			idsFromUser:   map[string]struct{}{"group:4": {}},
			readMocks: [][]*openfgav1.Tuple{
				{{Key: tuple.NewTupleKey("group:1", "parent", "group:3")}},
				{{Key: tuple.NewTupleKey("group:3", "parent", "group:2")}},
				{{Key: tuple.NewTupleKey("group:2", "parent", "group:1")}},
			},
		},
		{
			name:          "duplicates_match_with_recursion",
			idsFromObject: map[string]struct{}{"group:1": {}, "group:2": {}, "group:3": {}},
			idsFromUser:   map[string]struct{}{"group:4": {}},
			readMocks: [][]*openfgav1.Tuple{
				{{Key: tuple.NewTupleKey("group:1", "parent", "group:3")}},
				{{Key: tuple.NewTupleKey("group:2", "parent", "group:1")}},
				{{Key: tuple.NewTupleKey("group:3", "parent", "group:4")}},
			},
			expected: true,
		},
		{
			name:          "no_duplicates_no_match_counts",
			idsFromObject: map[string]struct{}{"group:1": {}, "group:2": {}, "group:3": {}},
			idsFromUser:   map[string]struct{}{"group:nope": {}},
			readMocks: [][]*openfgav1.Tuple{
				{{Key: tuple.NewTupleKey("group:1", "parent", "group:4")}},
				{{Key: tuple.NewTupleKey("group:2", "parent", "group:5")}},
				{{Key: tuple.NewTupleKey("group:3", "parent", "group:6")}},
				{{Key: tuple.NewTupleKey("group:6", "parent", "group:9")}},
				{{Key: tuple.NewTupleKey("group:7", "parent", "group:10")}},
				{{Key: tuple.NewTupleKey("group:8", "parent", "group:11")}},
				{},
				{},
				{},
			},
		},
		{
			name:          "deep_recursion_multiple_paths",
			idsFromObject: map[string]struct{}{"group:1": {}},
			idsFromUser:   map[string]struct{}{"group:10": {}},
			readMocks: [][]*openfgav1.Tuple{
				{
					{Key: tuple.NewTupleKey("group:1", "parent", "group:2")},
					{Key: tuple.NewTupleKey("group:1", "parent", "group:3")},
				},
				{{Key: tuple.NewTupleKey("group:2", "parent", "group:4")}},
				{{Key: tuple.NewTupleKey("group:3", "parent", "group:5")}},
				{{Key: tuple.NewTupleKey("group:4", "parent", "group:10")}},
				{}, // for group:5
			},
			expected: true,
		},
		{
			name:          "diamond_pattern_match",
			idsFromObject: map[string]struct{}{"group:1": {}},
			idsFromUser:   map[string]struct{}{"group:4": {}},
			readMocks: [][]*openfgav1.Tuple{
				{
					{Key: tuple.NewTupleKey("group:1", "parent", "group:2")},
					{Key: tuple.NewTupleKey("group:1", "parent", "group:3")},
				},
				{{Key: tuple.NewTupleKey("group:2", "parent", "group:4")}},
				{{Key: tuple.NewTupleKey("group:3", "parent", "group:4")}},
			},
			expected: true,
		},
		{
			name:          "multiple_users_one_matches",
			idsFromObject: map[string]struct{}{"group:1": {}},
			idsFromUser:   map[string]struct{}{"group:target1": {}, "group:target2": {}, "group:target3": {}},
			readMocks: [][]*openfgav1.Tuple{
				{{Key: tuple.NewTupleKey("group:1", "parent", "group:2")}},
				{{Key: tuple.NewTupleKey("group:2", "parent", "group:target2")}},
			},
			expected: true,
		},
		{
			name:          "wide_tree_match",
			idsFromObject: map[string]struct{}{"group:1": {}},
			idsFromUser:   map[string]struct{}{"group:target": {}},
			readMocks: [][]*openfgav1.Tuple{
				{
					{Key: tuple.NewTupleKey("group:1", "parent", "group:2")},
					{Key: tuple.NewTupleKey("group:1", "parent", "group:3")},
					{Key: tuple.NewTupleKey("group:1", "parent", "group:4")},
					{Key: tuple.NewTupleKey("group:1", "parent", "group:5")},
					{Key: tuple.NewTupleKey("group:1", "parent", "group:6")},
					{Key: tuple.NewTupleKey("group:1", "parent", "group:7")},
					{Key: tuple.NewTupleKey("group:1", "parent", "group:8")},
					{Key: tuple.NewTupleKey("group:1", "parent", "group:9")},
					{Key: tuple.NewTupleKey("group:1", "parent", "group:10")},
					{Key: tuple.NewTupleKey("group:1", "parent", "group:11")},
					{Key: tuple.NewTupleKey("group:1", "parent", "group:12")},
					{Key: tuple.NewTupleKey("group:1", "parent", "group:13")},
				},
				{{Key: tuple.NewTupleKey("group:2", "parent", "group:14")}},
				{{Key: tuple.NewTupleKey("group:3", "parent", "group:15")}},
				{{Key: tuple.NewTupleKey("group:4", "parent", "group:16")}},
				{{Key: tuple.NewTupleKey("group:5", "parent", "group:17")}},
				{{Key: tuple.NewTupleKey("group:6", "parent", "group:18")}},
				{{Key: tuple.NewTupleKey("group:7", "parent", "group:19")}},
				{{Key: tuple.NewTupleKey("group:8", "parent", "group:20")}},
				{{Key: tuple.NewTupleKey("group:9", "parent", "group:21")}},
				{{Key: tuple.NewTupleKey("group:10", "parent", "group:19")}},
				{{Key: tuple.NewTupleKey("group:11", "parent", "group:20")}},
				{{Key: tuple.NewTupleKey("group:12", "parent", "group:21")}},
				{{Key: tuple.NewTupleKey("group:13", "parent", "group:22")}},
				{},
				{},
				{},
				{},
				{},
				{},
				{{Key: tuple.NewTupleKey("group:22", "parent", "group:target")}},
				{},
				{},
				{},
				{},
				{},
			},
			expected: true,
		},
		{
			name:          "long_chain_match_at_end",
			idsFromObject: map[string]struct{}{"group:1": {}},
			idsFromUser:   map[string]struct{}{"group:final": {}},
			readMocks: [][]*openfgav1.Tuple{
				{{Key: tuple.NewTupleKey("group:1", "parent", "group:2")}},
				{{Key: tuple.NewTupleKey("group:2", "parent", "group:3")}},
				{{Key: tuple.NewTupleKey("group:3", "parent", "group:4")}},
				{{Key: tuple.NewTupleKey("group:4", "parent", "group:5")}},
				{{Key: tuple.NewTupleKey("group:5", "parent", "group:final")}},
			},
			expected: true,
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

			mg, err := modelgraph.New(model)
			require.NoError(t, err)

			node, ok := mg.GetNodeByID("group#member")
			require.True(t, ok)
			recursiveEdge, ok := mg.CanApplyRecursion(node, "user", true)
			require.True(t, ok)
			require.NotNil(t, recursiveEdge)

			ctx := context.Background()

			req, err := NewRequest(RequestParams{
				StoreID:              storeID,
				Model: mg,
				TupleKey:             tuple.NewTupleKey("group:3", "member", "user:maria"),
			})
			require.NoError(t, err)

			strategy := NewRecursive(mg, mockDatastore, 10)
			res, err := strategy.recursiveMatch(ctx, req, recursiveEdge, RecursiveTypeTTU, tt.idsFromUser, tt.idsFromObject)
			require.NoError(t, err)
			require.Equal(t, tt.expected, res.Allowed)
		})
	}
	t.Run("context_cancelled", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()

		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		ctx, cancel := context.WithCancel(context.Background())

		// Cancel context after first read to trigger cancellation during recursion
		mockDatastore.EXPECT().Read(gomock.Any(), storeID, gomock.Any(), gomock.Any()).MaxTimes(1).DoAndReturn(
			func(ctx context.Context, store string, filter storage.ReadFilter, options storage.ReadOptions) (storage.TupleIterator, error) {
				cancel()
				return storage.NewStaticTupleIterator([]*openfgav1.Tuple{
					{Key: tuple.NewTupleKey("group:1", "parent", "group:2")},
				}), nil
			},
		)

		model := testutils.MustTransformDSLToProtoWithID(`
        model
         schema 1.1
        type user
        type group
         relations
          define member: [user] or member from parent
          define parent: [group]
    `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		node, ok := mg.GetNodeByID("group#member")
		require.True(t, ok)
		recursiveEdge, ok := mg.CanApplyRecursion(node, "user", true)
		require.True(t, ok)
		require.NotNil(t, recursiveEdge)

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			Model: mg,
			TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
		})
		require.NoError(t, err)

		strategy := NewRecursive(mg, mockDatastore, 10)
		res, err := strategy.recursiveMatch(ctx, req, recursiveEdge, RecursiveTypeTTU,
			map[string]struct{}{"group:target": {}},
			map[string]struct{}{"group:1": {}})

		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, res)
	})
}

func TestRecursiveTTUWithTupleCycles(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	t.Run("complex_model", func(t *testing.T) {
		model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1
			type user
			type employee
			type group
				relations
					define activator: [user]
					define public_restricted: [user:*] but not restricted
					define restricted: [user with xcond]
					define viewer: (activator and public_restricted) or guest
					define guest: [employee]
					define admin: [user]
					define member: member from parent or (viewer but not admin)
					define parent: [group]
			condition xcond(tcond: string) {
  				tcond == 'restricted'
			}
		`)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		readEmpty := []*openfgav1.Tuple{}
		initialTks := []*openfgav1.TupleKey{
			tuple.NewTupleKey("group:1", "parent", "group:4"),
			tuple.NewTupleKey("group:1", "parent", "group:3"),
		}
		readGroup1Parent := []*openfgav1.Tuple{
			{
				Key: tuple.NewTupleKey("group:1", "parent", "group:3"),
			},
			{
				Key: tuple.NewTupleKey("group:1", "parent", "group:4"),
			},
		}
		readGroup3Parent := []*openfgav1.Tuple{
			{
				Key: tuple.NewTupleKey("group:3", "parent", "group:5"),
			},
		}
		readGroup4Parent := []*openfgav1.Tuple{
			{
				Key: tuple.NewTupleKey("group:4", "parent", "group:1"),
			},
		}
		readGroup5Parent := []*openfgav1.Tuple{
			{
				Key: tuple.NewTupleKey("group:5", "parent", "group:2"),
			},
		}
		readStartingWithUserActivator := []*openfgav1.Tuple{
			{
				Key: tuple.NewTupleKey("group:2", "activator", "user:1"),
			},
			{
				Key: tuple.NewTupleKey("group:3", "activator", "user:1"),
			},
		}
		readStartingWithUserAdmin := []*openfgav1.Tuple{
			{
				Key: tuple.NewTupleKey("group:3", "admin", "user:1"),
			},
		}
		readStartingWithUserPublic := []*openfgav1.Tuple{
			{
				Key: tuple.NewTupleKey("group:1", "public_restricted", "user:*"),
			},
			{
				Key: tuple.NewTupleKey("group:2", "public_restricted", "user:*"),
			},
		}
		contextStruct := testutils.MustNewStruct(t, map[string]interface{}{"tcond": "restricted"})
		readStartingWithUserRestricted := []*openfgav1.Tuple{
			{
				Key: &openfgav1.TupleKey{
					Object:   "group:1",
					Relation: "restricted",
					User:     "user:1",
					Condition: &openfgav1.RelationshipCondition{
						Name:    "xcond",
						Context: contextStruct,
					},
				},
			},
			{
				Key: &openfgav1.TupleKey{
					Object:   "group:3",
					Relation: "restricted",
					User:     "user:1",
					Condition: &openfgav1.RelationshipCondition{
						Name:    "xcond",
						Context: contextStruct,
					},
				},
			},
		}

		mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			MaxTimes(4). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadStartingWithUserFilter, opts storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
				// Manually check the relation and return the right data
				switch filter.Relation {
				case "activator":
					return storage.NewStaticTupleIterator(readStartingWithUserActivator), nil
				case "admin":
					return storage.NewStaticTupleIterator(readStartingWithUserAdmin), nil
				case "public_restricted":
					return storage.NewStaticTupleIterator(readStartingWithUserPublic), nil
				case "restricted":
					return storage.NewStaticTupleIterator(readStartingWithUserRestricted), nil
				default:
					return nil, fmt.Errorf("unexpected relation %s", filter.Relation)
				}
			})

		mockDatastore.EXPECT().Read(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			MaxTimes(4). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadFilter, opts storage.ReadOptions) (storage.TupleIterator, error) {
				// Manually check the relation and return the right data
				switch filter.Object {
				case "group:1":
					return storage.NewStaticTupleIterator(readGroup1Parent), nil
				case "group:3":
					return storage.NewStaticTupleIterator(readGroup3Parent), nil
				case "group:5":
					return storage.NewStaticTupleIterator(readGroup5Parent), nil
				case "group:4":
					return storage.NewStaticTupleIterator(readGroup4Parent), nil
				default:
					return storage.NewStaticTupleIterator(readEmpty), nil
				}
			})
		ctx := context.Background()
		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		node, ok := mg.GetNodeByID("group#member")
		require.True(t, ok)
		recursiveEdge, ok := mg.CanApplyRecursion(node, "user", true)
		require.True(t, ok)
		require.NotNil(t, recursiveEdge)
		require.Equal(t, graph.TTUEdge, recursiveEdge.GetEdgeType())
		require.Equal(t, "group#member", recursiveEdge.GetRecursiveRelation())
		require.Equal(t, recursiveEdge.GetTo(), node)

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			Model: mg,
			TupleKey:             tuple.NewTupleKey("group:1", "member", "user:1"),
		})
		require.NoError(t, err)

		strategy := NewRecursive(mg, mockDatastore, 5)
		result, err := strategy.TTU(ctx, req, recursiveEdge, storage.NewStaticTupleKeyIterator(initialTks), nil)
		require.NoError(t, err)
		require.True(t, result.GetAllowed())
	})
}

func TestRecursiveUsersetWithTupleCycles(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	t.Run("complex_model", func(t *testing.T) {
		model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1
			type user
			type employee
			type group
				relations
					define activator: [user]
					define public_restricted: [user:*] but not restricted
					define restricted: [user with xcond]
					define viewer: (activator and public_restricted) or guest
					define guest: [employee]
					define admin: [user]
					define member: [group#member] or (viewer but not admin)
			condition xcond(tcond: string) {
  				tcond == 'restricted'
			}
		`)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		readEmpty := []*openfgav1.Tuple{}
		initialTks := []*openfgav1.TupleKey{
			tuple.NewTupleKey("group:1", "member", "group:4#member"),
			tuple.NewTupleKey("group:1", "member", "group:3#member"),
		}
		readGroup1Parent := []*openfgav1.Tuple{
			{
				Key: tuple.NewTupleKey("group:1", "member", "group:3#member"),
			},
			{
				Key: tuple.NewTupleKey("group:1", "member", "group:4#member"),
			},
		}
		readGroup3Parent := []*openfgav1.Tuple{
			{
				Key: tuple.NewTupleKey("group:3", "member", "group:5#member"),
			},
		}
		readGroup4Parent := []*openfgav1.Tuple{
			{
				Key: tuple.NewTupleKey("group:4", "member", "group:1#member"),
			},
		}
		readGroup5Parent := []*openfgav1.Tuple{
			{
				Key: tuple.NewTupleKey("group:5", "member", "group:2#member"),
			},
		}
		readStartingWithUserActivator := []*openfgav1.Tuple{
			{
				Key: tuple.NewTupleKey("group:2", "activator", "user:1"),
			},
			{
				Key: tuple.NewTupleKey("group:3", "activator", "user:1"),
			},
		}
		readStartingWithUserAdmin := []*openfgav1.Tuple{
			{
				Key: tuple.NewTupleKey("group:3", "admin", "user:1"),
			},
		}
		readStartingWithUserPublic := []*openfgav1.Tuple{
			{
				Key: tuple.NewTupleKey("group:1", "public_restricted", "user:*"),
			},
			{
				Key: tuple.NewTupleKey("group:2", "public_restricted", "user:*"),
			},
		}
		contextStruct := testutils.MustNewStruct(t, map[string]interface{}{"tcond": "restricted"})
		readStartingWithUserRestricted := []*openfgav1.Tuple{
			{
				Key: &openfgav1.TupleKey{
					Object:   "group:1",
					Relation: "restricted",
					User:     "user:1",
					Condition: &openfgav1.RelationshipCondition{
						Name:    "xcond",
						Context: contextStruct,
					},
				},
			},
			{
				Key: &openfgav1.TupleKey{
					Object:   "group:3",
					Relation: "restricted",
					User:     "user:1",
					Condition: &openfgav1.RelationshipCondition{
						Name:    "xcond",
						Context: contextStruct,
					},
				},
			},
		}

		mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			MaxTimes(4). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadStartingWithUserFilter, opts storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
				// Manually check the relation and return the right data
				switch filter.Relation {
				case "activator":
					return storage.NewStaticTupleIterator(readStartingWithUserActivator), nil
				case "admin":
					return storage.NewStaticTupleIterator(readStartingWithUserAdmin), nil
				case "public_restricted":
					return storage.NewStaticTupleIterator(readStartingWithUserPublic), nil
				case "restricted":
					return storage.NewStaticTupleIterator(readStartingWithUserRestricted), nil
				default:
					return nil, fmt.Errorf("unexpected relation %s", filter.Relation)
				}
			})

		mockDatastore.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			MaxTimes(4). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadUsersetTuplesFilter, opts storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
				// Manually check the relation and return the right data
				switch filter.Object {
				case "group:1":
					return storage.NewStaticTupleIterator(readGroup1Parent), nil
				case "group:3":
					return storage.NewStaticTupleIterator(readGroup3Parent), nil
				case "group:5":
					return storage.NewStaticTupleIterator(readGroup5Parent), nil
				case "group:4":
					return storage.NewStaticTupleIterator(readGroup4Parent), nil
				default:
					return storage.NewStaticTupleIterator(readEmpty), nil
				}
			})
		ctx := context.Background()
		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		node, ok := mg.GetNodeByID("group#member")
		require.True(t, ok)
		recursiveEdge, ok := mg.CanApplyRecursion(node, "user", true)
		require.True(t, ok)
		require.NotNil(t, recursiveEdge)
		require.Equal(t, graph.DirectEdge, recursiveEdge.GetEdgeType())
		require.Equal(t, "group#member", recursiveEdge.GetRecursiveRelation())
		require.Equal(t, recursiveEdge.GetTo(), node)

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			Model: mg,
			TupleKey:             tuple.NewTupleKey("group:1", "member", "user:1"),
		})
		require.NoError(t, err)

		strategy := NewRecursive(mg, mockDatastore, 5)
		result, err := strategy.Userset(ctx, req, recursiveEdge, storage.NewStaticTupleKeyIterator(initialTks), nil)
		require.NoError(t, err)
		require.True(t, result.GetAllowed())
	})
}

func TestRecursiveTTUWithContextualTuples(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name                       string
		contextualTuples           []*openfgav1.TupleKey
		readStartingWithUserTuples []*openfgav1.Tuple
		readTuples                 [][]*openfgav1.Tuple
		expected                   *Response
	}{
		{
			name: "contextual_tuple_provides_direct_match",
			contextualTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:3", "member", "user:maria"),
			},
			readStartingWithUserTuples: []*openfgav1.Tuple{},
			readTuples: [][]*openfgav1.Tuple{
				{
					{
						Key: tuple.NewTupleKey("group:1", "parent", "group:3"),
					},
				},
			},
			expected: &Response{
				Allowed: true,
			},
		},
		{
			name: "contextual_tuple_combined_with_stored_tuples",
			contextualTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:2", "parent", "group:3"),
			},
			readStartingWithUserTuples: []*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:3", "member", "user:maria"),
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
			expected: &Response{
				Allowed: true,
			},
		},
		{
			name: "contextual_tuple_does_not_match",
			contextualTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:1", "parent", "group:5"),
			},
			readStartingWithUserTuples: []*openfgav1.Tuple{},
			readTuples: [][]*openfgav1.Tuple{
				{},
			},
			expected: &Response{
				Allowed: false,
			},
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

			mg, err := modelgraph.New(model)
			require.NoError(t, err)

			node, ok := mg.GetNodeByID("group#member")
			require.True(t, ok)
			recursiveEdge, ok := mg.CanApplyRecursion(node, "user", true)
			require.True(t, ok)
			require.NotNil(t, recursiveEdge)

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			storeID := ulid.Make().String()

			mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
			mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
				MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuples), nil)

			for _, tuples := range tt.readTuples[1:] {
				mockDatastore.EXPECT().Read(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
					MaxTimes(1).Return(storage.NewStaticTupleIterator(tuples), nil)
			}

			ctx := context.Background()
			req, err := NewRequest(RequestParams{
				StoreID:              storeID,
				Model: mg,
				TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
				ContextualTuples:     tt.contextualTuples,
			})
			require.NoError(t, err)

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
			result, err := strategy.TTU(ctx, req, recursiveEdge, storage.NewStaticTupleKeyIterator(tupleKeys), nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected.GetAllowed(), result.GetAllowed())
		})
	}
}

func TestRecursiveUsersetWithContextualTuples(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name                       string
		contextualTuples           []*openfgav1.TupleKey
		readStartingWithUserTuples []*openfgav1.Tuple
		readUsersetTuples          [][]*openfgav1.Tuple
		expected                   *Response
	}{
		{
			name: "contextual_tuple_provides_direct_match",
			contextualTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:3", "member", "user:maria"),
			},
			readStartingWithUserTuples: []*openfgav1.Tuple{},
			readUsersetTuples: [][]*openfgav1.Tuple{
				{
					{
						Key: tuple.NewTupleKey("group:1", "member", "group:3#member"),
					},
				},
			},
			expected: &Response{
				Allowed: true,
			},
		},
		{
			name: "contextual_tuple_combined_with_stored_tuples",
			contextualTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:2", "member", "group:3#member"),
			},
			readStartingWithUserTuples: []*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:3", "member", "user:maria"),
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
			expected: &Response{
				Allowed: true,
			},
		},
		{
			name: "contextual_tuple_does_not_match",
			contextualTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:1", "member", "group:5#member"),
			},
			readStartingWithUserTuples: []*openfgav1.Tuple{},
			readUsersetTuples: [][]*openfgav1.Tuple{
				{},
			},
			expected: &Response{
				Allowed: false,
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

			mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
				MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuples), nil)

			for _, tuples := range tt.readUsersetTuples[1:] {
				mockDatastore.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
					MaxTimes(1).Return(storage.NewStaticTupleIterator(tuples), nil)
			}

			model := testutils.MustTransformDSLToProtoWithID(`
				model
				 schema 1.1
				type user
				type group
				 relations
				  define member: [user, group#member]
			`)

			mg, err := modelgraph.New(model)
			require.NoError(t, err)

			node, ok := mg.GetNodeByID("group#member")
			require.True(t, ok)
			recursiveEdge, ok := mg.CanApplyRecursion(node, "user", true)
			require.True(t, ok)
			require.NotNil(t, recursiveEdge)

			ctx := context.Background()

			req, err := NewRequest(RequestParams{
				StoreID:              storeID,
				Model: mg,
				TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
				ContextualTuples:     tt.contextualTuples,
			})
			require.NoError(t, err)

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
			result, err := strategy.Userset(ctx, req, recursiveEdge, storage.NewStaticTupleKeyIterator(tupleKeys), nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected.GetAllowed(), result.GetAllowed())
		})
	}
}
