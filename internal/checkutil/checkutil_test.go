package checkutil

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestBuildTupleKeyConditionFilter(t *testing.T) {
	tests := []struct {
		name         string
		tupleKey     *openfgav1.TupleKey
		model        *openfgav1.AuthorizationModel
		context      map[string]interface{}
		conditionMet bool
		expectedErr  error
	}{
		{
			name:     "no_condition",
			tupleKey: tuple.NewTupleKey("document:1", "can_view", "user:maria"),
			model: parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user

				type document
					relations
						define can_view: [user]`),
			context:      map[string]interface{}{},
			conditionMet: true,
			expectedErr:  nil,
		},
		{
			name:     "condition_not_met",
			tupleKey: tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:maria", "correct_ip", nil),
			model: parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user

				type document
					relations
						define can_view: [user]`),
			context:      map[string]interface{}{},
			conditionMet: false,
			expectedErr:  condition.NewEvaluationError("correct_ip", fmt.Errorf("condition was not found")),
		},
		{
			name:     "condition_met",
			tupleKey: tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:maria", "x", nil),
			model: parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user

				type document
					relations
						define can_view: [user with x]

				condition x(x: int) {
					x == 1
				}
`),
			context:      map[string]interface{}{"x": 1},
			conditionMet: true,
			expectedErr:  nil,
		},
		{
			name:     "condition_false",
			tupleKey: tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:maria", "x", nil),
			model: parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user

				type document
					relations
						define can_view: [user with x]

				condition x(x: int) {
					x == 1
				}
`),
			context:      map[string]interface{}{"x": 15},
			conditionMet: false,
			expectedErr:  nil,
		},
		{
			name:     "condition_missing_parameter",
			tupleKey: tuple.NewTupleKeyWithCondition("document:1", "can_view", "user:maria", "x_y", nil),
			model: parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user

				type document
					relations
						define can_view: [user with x_y]

				condition x_y(x: int, y: int) {
					x == 1 && y == 0
				}
`),
			context:      map[string]interface{}{"x": 5},
			conditionMet: false,
			expectedErr: condition.NewEvaluationError("x_y",
				fmt.Errorf("tuple 'document:1#can_view@user:maria' is missing context parameters '[y]'")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ts, err := typesystem.NewAndValidate(context.Background(), tt.model)
			require.NoError(t, err)

			contextStruct, err := structpb.NewStruct(tt.context)
			require.NoError(t, err)

			iterFunc := BuildTupleKeyConditionFilter(context.Background(), contextStruct, ts)
			result, err := iterFunc(tt.tupleKey)
			if tt.expectedErr != nil {
				require.Equal(t, tt.expectedErr.Error(), err.Error())
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.conditionMet, result)
		})
	}
}

func TestObjectIDInSortedSet(t *testing.T) {
	filter := func(tupleKey *openfgav1.TupleKey) (bool, error) {
		if tupleKey.GetCondition().GetName() == "condition1" {
			return true, nil
		}
		return false, fmt.Errorf("condition not found")
	}

	tests := []struct {
		name          string
		tuples        []*openfgav1.TupleKey
		objectIDs     []string
		expectedError bool
		expected      bool
	}{
		{
			name: "no_match",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:2#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc2", "viewer", "group:2#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc3", "viewer", "group:2#member", "condition1", nil),
			},
			objectIDs:     []string{"doc0", "doc5", "doc6"},
			expected:      false,
			expectedError: false,
		},
		{
			name: "match",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:2#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc2", "viewer", "group:2#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc3", "viewer", "group:2#member", "condition1", nil),
			},
			objectIDs:     []string{"doc0", "doc2", "doc6"},
			expected:      true,
			expectedError: false,
		},
		{
			name: "error",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:2#member", "badCondition", nil),
			},
			objectIDs:     []string{"doc0", "doc2", "doc6"},
			expected:      false,
			expectedError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter := storage.NewConditionsFilteredTupleKeyIterator(storage.NewStaticTupleKeyIterator(tt.tuples), filter)
			objectIDs := storage.NewSortedSet()
			for _, item := range tt.objectIDs {
				objectIDs.Add(item)
			}
			result, err := ObjectIDInSortedSet(context.Background(), iter, objectIDs)
			if tt.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestUserFilter(t *testing.T) {
	tests := []struct {
		name                    string
		hasPubliclyAssignedType bool
		user                    string
		userType                string
		expected                []*openfgav1.ObjectRelation
	}{
		{
			name:                    "non_public",
			hasPubliclyAssignedType: false,
			user:                    "user:1",
			userType:                "user",
			expected: []*openfgav1.ObjectRelation{{
				Object: "user:1",
			}},
		},
		{
			name:                    "public",
			hasPubliclyAssignedType: true,
			user:                    "user:1",
			userType:                "user",
			expected: []*openfgav1.ObjectRelation{
				{Object: "user:1"},
				{Object: "user:*"},
			},
		},
		{
			name:                    "user_wildcard",
			hasPubliclyAssignedType: true,
			user:                    "user:*",
			userType:                "user",
			expected: []*openfgav1.ObjectRelation{
				{Object: "user:*"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := userFilter(tt.hasPubliclyAssignedType, tt.user, tt.userType)
			require.Equal(t, tt.expected, result)
		})
	}
}

type mockResolveCheckRequest struct {
	StoreID     string
	TupleKey    *openfgav1.TupleKey
	Consistency openfgav1.ConsistencyPreference
	Context     *structpb.Struct
}

func (m *mockResolveCheckRequest) GetStoreID() string {
	return m.StoreID
}

func (m *mockResolveCheckRequest) GetTupleKey() *openfgav1.TupleKey {
	return m.TupleKey
}

func (m *mockResolveCheckRequest) GetConsistency() openfgav1.ConsistencyPreference {
	return m.Consistency
}

func (m *mockResolveCheckRequest) GetContext() *structpb.Struct {
	return m.Context
}

func TestIteratorReadStartingFromUser(t *testing.T) {
	tests := []struct {
		name             string
		model            string
		reqConsistency   openfgav1.ConsistencyPreference
		expectedIsPublic bool
	}{
		{
			name: "non_public",
			model: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
			type folder
				relations
					define owner: [group]
					define viewer: member from owner
`,
			reqConsistency:   openfgav1.ConsistencyPreference_MINIMIZE_LATENCY,
			expectedIsPublic: false,
		},
		{
			name: "public",
			model: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user, user:*]
			type folder
				relations
					define owner: [group]
					define viewer: member from owner
`,
			reqConsistency:   openfgav1.ConsistencyPreference_MINIMIZE_LATENCY,
			expectedIsPublic: true,
		},
		{
			name: "higher_consistency",
			model: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
			type folder
				relations
					define owner: [group]
					define viewer: member from owner
`,
			reqConsistency:   openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
			expectedIsPublic: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			storeID := ulid.Make().String()

			req := mockResolveCheckRequest{
				StoreID:     storeID,
				TupleKey:    tuple.NewTupleKey("document:1", "viewer", "user:maria"),
				Consistency: tt.reqConsistency,
			}
			objectIDs := storage.NewSortedSet()

			expectedFilter := storage.ReadStartingWithUserFilter{
				ObjectType: "group",
				Relation:   "member",
				UserFilter: userFilter(tt.expectedIsPublic, "user:maria", "user"),
				ObjectIDs:  objectIDs,
			}
			expectedOpts := storage.ReadStartingWithUserOptions{
				WithResultsSortedAscending: true,
				Consistency: storage.ConsistencyOptions{
					Preference: req.GetConsistency(),
				},
			}
			ds := mocks.NewMockRelationshipTupleReader(ctrl)
			ds.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, expectedFilter, expectedOpts).Times(1).Return(nil, nil)
			ts, err := typesystem.New(testutils.MustTransformDSLToProtoWithID(tt.model))
			require.NoError(t, err)
			_, _ = IteratorReadStartingFromUser(context.Background(), ts, ds, &req, "group#member", objectIDs, true)
		})
	}
}

func TestBuildUsersetDetailsUserset(t *testing.T) {
	tests := []struct {
		name             string
		model            string
		tuple            *openfgav1.TupleKey
		expectedHasError bool
		expectedRelation string
		expectedObjectID string
	}{
		{
			name: "userset_direct_assignment",
			model: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
			type folder
				relations
					define viewer: [group#member]
`,
			tuple:            tuple.NewTupleKey("folder:1", "viewer", "group:2#member"),
			expectedHasError: false,
			expectedRelation: "group#member",
			expectedObjectID: "2",
		},
		{
			name: "userset_computed_userset",
			model: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
					define computed_member: member
			type folder
				relations
					define viewer: [group#computed_member]
`,
			tuple:            tuple.NewTupleKey("folder:1", "viewer", "group:2#computed_member"),
			expectedHasError: false,
			expectedRelation: "group#member",
			expectedObjectID: "2",
		},
		{
			name: "relation_not_found",
			model: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
			type folder
				relations
					define viewer: [group#computed_member]
`,
			tuple:            tuple.NewTupleKey("folder:1", "viewer", "group:2#computed_member"),
			expectedHasError: true,
			expectedRelation: "",
			expectedObjectID: "",
		},
		{
			name: "nonuserset_model",
			model: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
					define owner: [user]
					define viewer: member or owner
			type folder
				relations
					define viewer: [group#viewer]
`,
			tuple:            tuple.NewTupleKey("folder:1", "viewer", "group:2#viewer"),
			expectedHasError: true,
			expectedRelation: "",
			expectedObjectID: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ts, err := typesystem.New(testutils.MustTransformDSLToProtoWithID(tt.model))
			require.NoError(t, err)
			usersetFunc := BuildUsersetDetailsUserset(ts)
			rel, obj, err := usersetFunc(tt.tuple)
			if tt.expectedHasError {
				// details of the error doesn't really matter
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.expectedRelation, rel)
			require.Equal(t, tt.expectedObjectID, obj)
		})
	}
}

func TestBuildUsersetDetailsTTU(t *testing.T) {
	tests := []struct {
		name             string
		model            string
		tuple            *openfgav1.TupleKey
		computedRelation string
		expectedHasError bool
		expectedRelation string
		expectedObjectID string
	}{
		{
			name: "ttu_direct_assignment",
			model: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
			type folder
				relations
					define owner: [group]
					define viewer: member from owner
`,
			tuple:            tuple.NewTupleKey("folder:1", "owner", "group:2"),
			computedRelation: "member",
			expectedHasError: false,
			expectedRelation: "group#member",
			expectedObjectID: "2",
		},
		{
			name: "ttu_computed_userset",
			model: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
					define viewable_member: member
			type folder
				relations
					define owner: [group]
					define viewer: viewable_member from owner
`,
			tuple:            tuple.NewTupleKey("folder:1", "owner", "group:2"),
			computedRelation: "viewable_member",
			expectedHasError: false,
			expectedRelation: "group#member",
			expectedObjectID: "2",
		},
		{
			name: "ttu_not_found",
			model: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
					define viewable_member: member
			type folder
				relations
					define owner: [group]
					define viewer: viewable_member from owner
`,
			tuple:            tuple.NewTupleKey("folder:1", "owner", "group:2"),
			computedRelation: "not_found",
			expectedHasError: true,
			expectedRelation: "",
			expectedObjectID: "",
		},
		{
			name: "ttu_not_assignable",
			model: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
					define viewable_member: [user] or member
			type folder
				relations
					define owner: [group]
					define viewer: viewable_member from owner
`,
			tuple:            tuple.NewTupleKey("folder:1", "owner", "group:2"),
			computedRelation: "viewable_member",
			expectedHasError: true,
			expectedRelation: "",
			expectedObjectID: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ts, err := typesystem.New(testutils.MustTransformDSLToProtoWithID(tt.model))
			require.NoError(t, err)
			usersetFunc := BuildUsersetDetailsTTU(ts, tt.computedRelation)
			rel, obj, err := usersetFunc(tt.tuple)
			if tt.expectedHasError {
				// details of the error doesn't really matter
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.expectedRelation, rel)
			require.Equal(t, tt.expectedObjectID, obj)
		})
	}
}
