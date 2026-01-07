package graph

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/emirpasic/gods/sets/hashset"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestRecursiveTTU(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	model := parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user] or member from parent
						define parent: [group]
				`)

	ts, err := typesystem.New(model)
	require.NoError(t, err)

	tests := []struct {
		name                            string
		readStartingWithUserTuples      []*openfgav1.Tuple
		readStartingWithUserTuplesError error
		readTuples                      [][]*openfgav1.Tuple
		readTuplesError                 error
		expected                        *ResolveCheckResponse
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
			expected: &ResolveCheckResponse{
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
			expected: &ResolveCheckResponse{
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
			expected: &ResolveCheckResponse{
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
			expected: &ResolveCheckResponse{
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

			req := &ResolveCheckRequest{
				StoreID:              storeID,
				AuthorizationModelID: ulid.Make().String(),
				TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
				RequestMetadata:      NewCheckRequestMetadata(),
			}
			ctx := context.Background()
			ctx = setRequestContext(ctx, ts, mockDatastore, nil)
			checker := NewLocalChecker()

			tupleKeys := make([]*openfgav1.TupleKey, 0, len(tt.readTuples[0]))
			for _, t := range tt.readTuples[0] {
				k := t.GetKey()
				tupleKeys = append(tupleKeys, &openfgav1.TupleKey{
					User:     k.GetUser(),
					Relation: k.GetRelation(),
					Object:   k.GetObject(),
				})
			}

			result, err := checker.recursiveTTU(ctx, req, typesystem.TupleToUserset("parent", "member"), storage.NewStaticTupleKeyIterator(tupleKeys))(ctx)
			require.Equal(t, tt.expectedError, err)
			require.Equal(t, tt.expected.GetAllowed(), result.GetAllowed())
			require.Equal(t, tt.expected.GetResolutionMetadata(), result.GetResolutionMetadata())
		})
	}

	t.Run("complex_model", func(t *testing.T) {
		model := parser.MustTransformDSLToProto(`
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

		ts, err := typesystem.New(model)
		require.NoError(t, err)

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
			expected                         *ResolveCheckResponse
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
				expected: &ResolveCheckResponse{
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
				expected: &ResolveCheckResponse{
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
				expected: &ResolveCheckResponse{
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
				expected: &ResolveCheckResponse{
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
				expected: &ResolveCheckResponse{
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
				expected: &ResolveCheckResponse{
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

				ctx := setRequestContext(context.Background(), ts, mockDatastore, nil)

				req := &ResolveCheckRequest{
					StoreID:              storeID,
					AuthorizationModelID: ulid.Make().String(),
					TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
					RequestMetadata:      NewCheckRequestMetadata(),
				}

				checker := NewLocalChecker()
				tupleKeys := make([]*openfgav1.TupleKey, 0, len(tt.readTuples[0]))
				for _, t := range tt.readTuples[0] {
					k := t.GetKey()
					tupleKeys = append(tupleKeys, &openfgav1.TupleKey{
						User:     k.GetUser(),
						Relation: k.GetRelation(),
						Object:   k.GetObject(),
					})
				}

				result, err := checker.recursiveTTU(ctx, req, typesystem.TupleToUserset("parent", "member"), storage.NewStaticTupleKeyIterator(tupleKeys))(ctx)
				require.Equal(t, tt.expectedError, err)
				require.Equal(t, tt.expected.GetAllowed(), result.GetAllowed())
				require.Equal(t, tt.expected.GetResolutionMetadata(), result.GetResolutionMetadata())
			})
		}
	})

	t.Run("resolution_depth_exceeded", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).MaxTimes(1).Return(
			storage.NewStaticTupleIterator([]*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:30", "member", "user:maria"),
				},
			}), nil)

		for i := 1; i < 26; i++ {
			mockDatastore.EXPECT().Read(gomock.Any(), storeID, gomock.Any(), gomock.Any()).MaxTimes(1).Return(
				storage.NewStaticTupleIterator([]*openfgav1.Tuple{
					{
						Key: tuple.NewTupleKey("group:"+strconv.Itoa(i), "parent", "group:"+strconv.Itoa(i+1)),
					},
				}), nil)
		}
		model := parser.MustTransformDSLToProto(`
model
	schema 1.1

type user
type group
	relations
		define member: [user] or member from parent
		define parent: [group]
			`)

		ts, err := typesystem.New(model)
		require.NoError(t, err)
		ctx := setRequestContext(context.Background(), ts, mockDatastore, nil)

		req := &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: ulid.Make().String(),
			TupleKey:             tuple.NewTupleKey("group:0", "member", "user:maria"),
			RequestMetadata:      NewCheckRequestMetadata(),
		}

		checker := NewLocalChecker()
		tupleKeys := []*openfgav1.TupleKey{{Object: "group:0", Relation: "parent", User: "group:1"}}

		result, err := checker.recursiveTTU(ctx, req, typesystem.TupleToUserset("parent", "member"), storage.NewStaticTupleKeyIterator(tupleKeys))(ctx)
		require.Nil(t, result)
		require.Equal(t, ErrResolutionDepthExceeded, err)
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
		expected                        *ResolveCheckResponse
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
			expected: &ResolveCheckResponse{
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
			expected: &ResolveCheckResponse{
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
			expected: &ResolveCheckResponse{
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
			expected: &ResolveCheckResponse{
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
			model := parser.MustTransformDSLToProto(`
						model
							schema 1.1

						type user
						type group
							relations
								define member: [user, group#member]
`)

			ts, err := typesystem.New(model)
			require.NoError(t, err)
			ctx := setRequestContext(context.Background(), ts, mockDatastore, nil)

			req := &ResolveCheckRequest{
				StoreID:              storeID,
				AuthorizationModelID: ulid.Make().String(),
				TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
				RequestMetadata:      NewCheckRequestMetadata(),
			}

			checker := NewLocalChecker()
			tupleKeys := make([]*openfgav1.TupleKey, 0, len(tt.readUsersetTuples[0]))
			for _, t := range tt.readUsersetTuples[0] {
				k := t.GetKey()
				tupleKeys = append(tupleKeys, &openfgav1.TupleKey{
					User:     k.GetUser(),
					Relation: k.GetRelation(),
					Object:   k.GetObject(),
				})
			}

			result, err := checker.recursiveUserset(ctx, req, nil, storage.NewStaticTupleKeyIterator(tupleKeys))(ctx)
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
			expected                         *ResolveCheckResponse
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
				expected: &ResolveCheckResponse{
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
				expected: &ResolveCheckResponse{
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
				expected: &ResolveCheckResponse{
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
				expected: &ResolveCheckResponse{
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
				expected: &ResolveCheckResponse{
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
				expected: &ResolveCheckResponse{
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
				model := parser.MustTransformDSLToProto(`
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

				ts, err := typesystem.New(model)
				require.NoError(t, err)
				ctx := setRequestContext(context.Background(), ts, mockDatastore, nil)

				req := &ResolveCheckRequest{
					StoreID:              storeID,
					AuthorizationModelID: ulid.Make().String(),
					TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
					RequestMetadata:      NewCheckRequestMetadata(),
				}

				checker := NewLocalChecker()
				tupleKeys := make([]*openfgav1.TupleKey, 0, len(tt.readUsersetTuples[0]))
				for _, t := range tt.readUsersetTuples[0] {
					k := t.GetKey()
					tupleKeys = append(tupleKeys, &openfgav1.TupleKey{
						User:     k.GetUser(),
						Relation: k.GetRelation(),
						Object:   k.GetObject(),
					})
				}

				result, err := checker.recursiveUserset(ctx, req, nil, storage.NewStaticTupleKeyIterator(tupleKeys))(ctx)
				require.Equal(t, tt.expectedError, err)
				require.Equal(t, tt.expected.GetAllowed(), result.GetAllowed())
				require.Equal(t, tt.expected.GetResolutionMetadata(), result.GetResolutionMetadata())
			})
		}
	})

	t.Run("resolution_depth_exceeded", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).MaxTimes(1).Return(
			storage.NewStaticTupleIterator([]*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:bad", "member", "user:maria"),
				},
			}), nil)

		for i := 1; i < 26; i++ {
			mockDatastore.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, gomock.Any(), gomock.Any()).MaxTimes(1).Return(
				storage.NewStaticTupleIterator([]*openfgav1.Tuple{
					{
						Key: tuple.NewTupleKey("group:"+strconv.Itoa(i+1), "member", "group:"+strconv.Itoa(i)+"#member"),
					},
				}), nil)
		}
		model := parser.MustTransformDSLToProto(`
							model
								schema 1.1

							type user
							type group
								relations
									define member: [user, group#member]

			`)

		ts, err := typesystem.New(model)
		require.NoError(t, err)
		ctx := setRequestContext(context.Background(), ts, mockDatastore, nil)

		req := &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: ulid.Make().String(),
			TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
			RequestMetadata:      NewCheckRequestMetadata(),
		}

		checker := NewLocalChecker()
		tupleKeys := []*openfgav1.TupleKey{{Object: "group:1", Relation: "member", User: "group:0#member"}}

		result, err := checker.recursiveUserset(ctx, req, nil, storage.NewStaticTupleKeyIterator(tupleKeys))(ctx)
		require.Nil(t, result)
		require.Equal(t, ErrResolutionDepthExceeded, err)
	})
}

func TestBreadthFirstRecursiveMatch(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name                 string
		currentLevelUsersets *hashset.Set
		usersetFromUser      *hashset.Set
		readMocks            [][]*openfgav1.Tuple
		expected             bool
	}{
		{
			name:                 "empty_userset",
			currentLevelUsersets: hashset.New(),
			usersetFromUser:      hashset.New(),
		},
		{
			name:                 "duplicates_no_match_no_recursion",
			currentLevelUsersets: hashset.New("group:1", "group:2", "group:3", "group:1"),
			usersetFromUser:      hashset.New(),
			readMocks: [][]*openfgav1.Tuple{
				{{}},
				{{}},
				{{}},
			},
		},
		{
			name:                 "duplicates_no_match_with_recursion",
			currentLevelUsersets: hashset.New("group:1", "group:2", "group:3"),
			usersetFromUser:      hashset.New(),
			readMocks: [][]*openfgav1.Tuple{
				{{Key: tuple.NewTupleKey("group:1", "parent", "group:3")}},
				{{Key: tuple.NewTupleKey("group:3", "parent", "group:2")}},
				{{Key: tuple.NewTupleKey("group:2", "parent", "group:1")}},
			},
		},
		{
			name:                 "duplicates_match_with_recursion",
			currentLevelUsersets: hashset.New("group:1", "group:2", "group:3"),
			usersetFromUser:      hashset.New("group:4"),
			readMocks: [][]*openfgav1.Tuple{
				{{Key: tuple.NewTupleKey("group:1", "parent", "group:3")}},
				{{Key: tuple.NewTupleKey("group:2", "parent", "group:1")}},
				{{Key: tuple.NewTupleKey("group:3", "parent", "group:4")}},
			},
			expected: true,
		},
		{
			name:                 "no_duplicates_no_match_counts",
			currentLevelUsersets: hashset.New("group:1", "group:2", "group:3"),
			usersetFromUser:      hashset.New(),
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

			model := parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user] or member from parent
						define parent: [group]
				`)

			req := &ResolveCheckRequest{
				StoreID:              storeID,
				AuthorizationModelID: ulid.Make().String(),
				TupleKey:             tuple.NewTupleKey("group:3", "member", "user:maria"),
				RequestMetadata:      NewCheckRequestMetadata(),
			}

			ts, err := typesystem.New(model)
			require.NoError(t, err)
			ctx := context.Background()
			ctx = setRequestContext(ctx, ts, mockDatastore, nil)

			checker := NewLocalChecker()
			mapping := &recursiveMapping{
				kind:             storage.TTUKind,
				tuplesetRelation: "parent",
			}
			checkOutcomeChan := make(chan checkOutcome, 100) // large buffer since there is no need to concurrently evaluate partial results
			checker.breadthFirstRecursiveMatch(ctx, req, mapping, &sync.Map{}, tt.currentLevelUsersets, tt.usersetFromUser, checkOutcomeChan)

			result := false
			for outcome := range checkOutcomeChan {
				if outcome.resp.Allowed {
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

		model := parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user] or member from parent
						define parent: [group]
				`)

		req := &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: ulid.Make().String(),
			TupleKey:             tuple.NewTupleKey("group:3", "member", "user:maria"),
			RequestMetadata:      NewCheckRequestMetadata(),
		}

		ts, err := typesystem.New(model)
		require.NoError(t, err)

		ctx = setRequestContext(ctx, ts, mockDatastore, nil)

		checker := NewLocalChecker()
		mapping := &recursiveMapping{
			kind:             storage.TTUKind,
			tuplesetRelation: "parent",
		}
		checkOutcomeChan := make(chan checkOutcome, 100) // large buffer since there is no need to concurrently evaluate partial results
		checker.breadthFirstRecursiveMatch(ctx, req, mapping, &sync.Map{}, hashset.New("group:1", "group:2", "group:3"), hashset.New(), checkOutcomeChan)
	})
}

func TestBuildRecursiveMapper(t *testing.T) {
	storeID := ulid.Make().String()

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]`)
	ts, err := typesystem.New(model)
	require.NoError(t, err)

	mockDatastore := mocks.NewMockRelationshipTupleReader(mockController)
	ctx := setRequestContext(context.Background(), ts, mockDatastore, nil)

	t.Run("recursive_userset", func(t *testing.T) {
		mockDatastore.EXPECT().ReadUsersetTuples(ctx, storeID, storage.ReadUsersetTuplesFilter{
			Object:   "document:1",
			Relation: "viewer",
			AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
				typesystem.DirectRelationReference("group", "member"),
			},
		}, storage.ReadUsersetTuplesOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
			},
		}).Times(1)

		mapping := &recursiveMapping{
			kind: storage.UsersetKind,
			allowedUserTypeRestrictions: []*openfgav1.RelationReference{
				typesystem.DirectRelationReference("group", "member"),
			},
		}
		res, err := buildRecursiveMapper(ctx, &ResolveCheckRequest{
			StoreID:     storeID,
			TupleKey:    tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			Context:     testutils.MustNewStruct(t, map[string]interface{}{"x": "2"}),
			Consistency: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
		}, mapping)
		require.NoError(t, err)
		_, ok := res.(*storage.UsersetMapper)
		require.True(t, ok)
	})

	t.Run("recursive_ttu", func(t *testing.T) {
		mockDatastore.EXPECT().Read(ctx, storeID, storage.ReadFilter{Object: "document:1", Relation: "parent", User: ""}, storage.ReadOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
			},
		}).Times(1)

		mapping := &recursiveMapping{
			tuplesetRelation: "parent",
			kind:             storage.TTUKind,
		}
		res, err := buildRecursiveMapper(ctx, &ResolveCheckRequest{
			StoreID:     storeID,
			TupleKey:    tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			Context:     testutils.MustNewStruct(t, map[string]interface{}{"x": "2"}),
			Consistency: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
		}, mapping)
		require.NoError(t, err)
		_, ok := res.(*storage.TTUMapper)
		require.True(t, ok)
	})
}
