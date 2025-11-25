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

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestWeight2Userset(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	t.Run("non_public_wildcard_union", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()

		mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "members",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			Conditions: []string{""},
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("group:1", "members", "user:1")},
		}), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "public",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			Conditions: []string{""},
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator(nil), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "public",
			UserFilter: []*openfgav1.ObjectRelation{{Object: tuple.TypedPublicWildcard("user")}},
			Conditions: []string{""},
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator(nil), nil)

		ctx := context.Background()

		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type group
				relations
					define members: [user]
					define public: [user, user:*]
					define all: members or public
			type document
				relations
					define viewer: [group#all]
			`)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("group#all")
		require.True(t, ok)

		iter := storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			User:     "group:1#all",
			Relation: "viewer",
			Object:   "document:1",
		}})

		strategy := NewWeight2(mg, mockDatastore)
		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:1"),
		})
		require.NoError(t, err)

		res, err := strategy.Userset(ctx, req, edges[0], iter, nil)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.True(t, res.GetAllowed())
	})

	t.Run("non_public_wildcard_union_not_match", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()

		mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "members",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			Conditions: []string{""},
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("group:1", "members", "user:1")},
		}), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "public",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			Conditions: []string{""},
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator(nil), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "public",
			UserFilter: []*openfgav1.ObjectRelation{{Object: tuple.TypedPublicWildcard("user")}},
			Conditions: []string{""},
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator(nil), nil)

		ctx := context.Background()
		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type group
				relations
					define members: [user]
					define public: [user, user:*]
					define all: members or public
			type document
				relations
					define viewer: [group#all]
			`)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("group#all")
		require.True(t, ok)

		iter := storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			User:     "group:1#all",
			Relation: "viewer",
			Object:   "document:1",
		}})
		strategy := NewWeight2(mg, mockDatastore)
		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:1"),
		})
		require.NoError(t, err)

		res, err := strategy.Userset(ctx, req, edges[0], iter, nil)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.True(t, res.GetAllowed())
	})

	t.Run("public_wildcard_union", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()

		mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "members",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			Conditions: []string{""},
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator(nil), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "public",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			Conditions: []string{""},
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator(nil), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "public",
			UserFilter: []*openfgav1.ObjectRelation{{Object: tuple.TypedPublicWildcard("user")}},
			Conditions: []string{""},
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("group:1", "public", "user:*")},
		}), nil)

		ctx := context.Background()

		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type group
				relations
					define members: [user]
					define public: [user, user:*]
					define all: members or public
			type document
				relations
					define viewer: [group#all]
			`)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("group#all")
		require.True(t, ok)

		iter := storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			User:     "group:1#all",
			Relation: "viewer",
			Object:   "document:1",
		}})
		strategy := NewWeight2(mg, mockDatastore)
		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:1"),
		})
		require.NoError(t, err)

		res, err := strategy.Userset(ctx, req, edges[0], iter, nil)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.True(t, res.GetAllowed())
	})
	t.Run("public_wildcard_union_not_match", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()

		mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "members",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			Conditions: []string{""},
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator(nil), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "public",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			Conditions: []string{""},
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator(nil), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "public",
			UserFilter: []*openfgav1.ObjectRelation{{Object: tuple.TypedPublicWildcard("user")}},
			Conditions: []string{""},
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("group:1", "public", "user:*")},
		}), nil)

		ctx := context.Background()

		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type group
				relations
					define members: [user]
					define public: [user, user:*]
					define all: members or public
			type document
				relations
					define viewer: [group#all]
			`)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("group#all")
		require.True(t, ok)

		iter := storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			User:     "group:2#all",
			Relation: "viewer",
			Object:   "document:1",
		}})

		strategy := NewWeight2(mg, mockDatastore)
		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:1"),
		})
		require.NoError(t, err)

		res, err := strategy.Userset(ctx, req, edges[0], iter, nil)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.False(t, res.GetAllowed())
	})

	t.Run("combined_with_match_and_condition", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()

		readStartingWithUserMembers := []*openfgav1.Tuple{
			{
				Key: tuple.NewTupleKey("group:2", "members", "user:1"),
			},
			{
				Key: tuple.NewTupleKey("group:3", "members", "user:1"),
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
			MaxTimes(3). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadStartingWithUserFilter, opts storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
				// Manually check the relation and return the right data
				switch filter.Relation {
				case "members":
					return storage.NewStaticTupleIterator(readStartingWithUserMembers), nil
				case "public_restricted":
					return storage.NewStaticTupleIterator(readStartingWithUserPublic), nil
				case "restricted":
					return storage.NewStaticTupleIterator(readStartingWithUserRestricted), nil
				default:
					return nil, fmt.Errorf("unexpected relation %s", filter.Relation)
				}
			})

		ctx := context.Background()
		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type employee
			type group
				relations
					define members: [user]
					define public_restricted: [user:*] but not restricted
					define restricted: [user with xcond]
					define all: (members and public_restricted) or guest
					define guest: [employee]
			type document
				relations
					define viewer: [user, group#all]
			condition xcond(tcond: string) {
  				tcond == 'restricted'
			}`)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		iter := storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			User:     "group:2#all",
			Relation: "viewer",
			Object:   "document:1",
		}})

		strategy := NewWeight2(mg, mockDatastore)
		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:1"),
		})
		require.NoError(t, err)

		res, err := strategy.Userset(ctx, req, edges[1], iter, nil)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.True(t, res.GetAllowed())
	})
}

func TestWeight2TTU(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("non_public_wildcard_union", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()

		mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "members",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			Conditions: []string{""},
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("group:1", "members", "user:1")},
		}), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "public",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			Conditions: []string{""},
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator(nil), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "public",
			UserFilter: []*openfgav1.ObjectRelation{{Object: tuple.TypedPublicWildcard("user")}},
			Conditions: []string{""},
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator(nil), nil)

		ctx := context.Background()

		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type group
				relations
					define members: [user]
					define public: [user, user:*]
					define all: members or public
			type document
				relations
					define parent: [group]
					define viewer: all from parent
			`)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		iter := storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			User:     "group:1",
			Relation: "parent",
			Object:   "document:1",
		}})
		strategy := NewWeight2(mg, mockDatastore)
		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:1"),
		})
		require.NoError(t, err)

		res, err := strategy.TTU(ctx, req, edges[0], iter, nil)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.True(t, res.GetAllowed())
	})

	t.Run("public_wildcard_union", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()

		mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "members",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			Conditions: []string{""},
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator(nil), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "public",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			Conditions: []string{""},
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator(nil), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "public",
			UserFilter: []*openfgav1.ObjectRelation{{Object: tuple.TypedPublicWildcard("user")}},
			Conditions: []string{""},
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("group:1", "public", "user:*")},
		}), nil)

		ctx := context.Background()

		model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type group
					relations
						define members: [user]
						define public: [user, user:*]
						define all: members or public
				type document
					relations
						define parent: [group]
						define viewer: all from parent
				`)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		iter := storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			User:     "group:1",
			Relation: "parent",
			Object:   "document:1",
		}})
		strategy := NewWeight2(mg, mockDatastore)
		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:1"),
		})
		require.NoError(t, err)

		res, err := strategy.TTU(ctx, req, edges[0], iter, nil)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.True(t, res.GetAllowed())
	})

	t.Run("non_public_wildcard_union_not_match", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()

		mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "members",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			Conditions: []string{""},
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("group:1", "members", "user:1")},
		}), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "public",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			Conditions: []string{""},
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator(nil), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "public",
			UserFilter: []*openfgav1.ObjectRelation{{Object: tuple.TypedPublicWildcard("user")}},
			Conditions: []string{""},
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator(nil), nil)

		ctx := context.Background()

		model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type group
					relations
						define members: [user]
						define public: [user, user:*]
						define all: members or public
				type document
					relations
						define parent: [group]
						define viewer: all from parent
				`)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		iter := storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			User:     "group:2",
			Relation: "parent",
			Object:   "document:1",
		}})
		strategy := NewWeight2(mg, mockDatastore)
		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:1"),
		})
		require.NoError(t, err)

		res, err := strategy.TTU(ctx, req, edges[0], iter, nil)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.False(t, res.GetAllowed())
	})

	t.Run("public_wildcard_union_not_match", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()

		mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "members",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			Conditions: []string{""},
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator(nil), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "public",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			Conditions: []string{""},
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator(nil), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "public",
			UserFilter: []*openfgav1.ObjectRelation{{Object: tuple.TypedPublicWildcard("user")}},
			Conditions: []string{""},
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("group:1", "public", "user:*")},
		}), nil)

		ctx := context.Background()

		model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type group
					relations
						define members: [user]
						define public: [user, user:*]
						define all: members or public
				type document
					relations
						define parent: [group]
						define viewer: all from parent
				`)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		iter := storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			User:     "group:2",
			Relation: "parent",
			Object:   "document:1",
		}})
		strategy := NewWeight2(mg, mockDatastore)
		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:1"),
		})
		require.NoError(t, err)

		res, err := strategy.TTU(ctx, req, edges[0], iter, nil)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.False(t, res.GetAllowed())
	})

	t.Run("combined_with_match_and_condition", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()

		readStartingWithUserMembers := []*openfgav1.Tuple{
			{
				Key: tuple.NewTupleKey("group:2", "members", "user:1"),
			},
			{
				Key: tuple.NewTupleKey("group:3", "members", "user:1"),
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
			MaxTimes(3). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadStartingWithUserFilter, opts storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
				// Manually check the relation and return the right data
				switch filter.Relation {
				case "members":
					return storage.NewStaticTupleIterator(readStartingWithUserMembers), nil
				case "public_restricted":
					return storage.NewStaticTupleIterator(readStartingWithUserPublic), nil
				case "restricted":
					return storage.NewStaticTupleIterator(readStartingWithUserRestricted), nil
				default:
					return nil, fmt.Errorf("unexpected relation %s", filter.Relation)
				}
			})

		ctx := context.Background()
		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type employee
			type group
				relations
					define members: [user]
					define public_restricted: [user:*] but not restricted
					define restricted: [user with xcond]
					define all: (members and public_restricted) or guest
					define guest: [employee]
			type document
				relations
					define viewer: all from group
					define group: [group]
			condition xcond(tcond: string) {
  				tcond == 'restricted'
			}`)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		iter := storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			User:     "group:2",
			Relation: "viewer",
			Object:   "document:1",
		}})

		strategy := NewWeight2(mg, mockDatastore)
		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:1"),
		})
		require.NoError(t, err)

		res, err := strategy.TTU(ctx, req, edges[0], iter, nil)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.True(t, res.GetAllowed())
	})
}
