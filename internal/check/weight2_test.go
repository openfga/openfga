package check

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

	"github.com/openfga/openfga/internal/iterator"
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

		edges, ok := mg.GetEdgesFromNodeID("group#all")
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

		edges, ok := mg.GetEdgesFromNodeID("group#all")
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

		edges, ok := mg.GetEdgesFromNodeID("group#all")
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

		edges, ok := mg.GetEdgesFromNodeID("group#all")
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

		edges, ok := mg.GetEdgesFromNodeID("document#viewer")
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

		edges, ok := mg.GetEdgesFromNodeID("document#viewer")
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

		edges, ok := mg.GetEdgesFromNodeID("document#viewer")
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

		edges, ok := mg.GetEdgesFromNodeID("document#viewer")
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

		edges, ok := mg.GetEdgesFromNodeID("document#viewer")
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

		edges, ok := mg.GetEdgesFromNodeID("document#viewer")
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

// TestWeight2ExecuteCancelledContextRace is a probabilistic regression test for the race where
// Go's select non-deterministically picks a closed leftChan/rightChan over ctx.Done() when both
// are ready simultaneously. Without the ctx.Err() guard this would occasionally return
// {Allowed:false, nil} instead of propagating the cancellation error.
func TestWeight2ExecuteCancelledContextRace(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storeID := ulid.Make().String()

	mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
	// Non-empty iterator so leftChan carries a value before closing,
	// widening the window where ctx.Done() and the channel close are both ready.
	// group#all is a union of "members" and "public" so bottomUp calls ReadStartingWithUser
	// for each operand (members, public-direct, public-wildcard); allow up to 3 calls.
	mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
		MaxTimes(3).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
		{Key: tuple.NewTupleKey("group:1", "members", "user:1")},
	}), nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel so ctx.Done() and the ToChannel close race immediately

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

	edges, ok := mg.GetEdgesFromNodeID("group#all")
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

	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, res)
}

// gatedIterator blocks the first Next() call until a gate is closed, and signals
// a blocked channel when it first hits the gate. This lets tests synchronize on
// the exact moment the iterator is parked, eliminating time.Sleep-based races.
type gatedIterator[T any] struct {
	inner   storage.Iterator[T]
	gate    chan struct{}
	blocked chan struct{}
	once    sync.Once
	used    bool
}

func newGatedIterator[T any](inner storage.Iterator[T]) (*gatedIterator[T], chan struct{}, chan struct{}) {
	gate := make(chan struct{})
	blocked := make(chan struct{})
	return &gatedIterator[T]{inner: inner, gate: gate, blocked: blocked}, gate, blocked
}

func (g *gatedIterator[T]) Next(ctx context.Context) (T, error) {
	if !g.used {
		g.used = true
		g.once.Do(func() { close(g.blocked) })
		select {
		case <-g.gate:
		case <-ctx.Done():
			var zero T
			return zero, ctx.Err()
		}
	}
	return g.inner.Next(ctx)
}

func (g *gatedIterator[T]) Head(ctx context.Context) (T, error) { return g.inner.Head(ctx) }

func (g *gatedIterator[T]) Stop() { g.inner.Stop() }

func (g *gatedIterator[T]) IsOrdered() bool { return g.inner.IsOrdered() }

// TestWeight2ExecuteConcatOrderingBug demonstrates that when contextual tuples are
// prepended via iterator.Concat, the weight2 pruning optimization previously would
// incorrectly discard valid intersection matches.
//
// The bug required this specific interleaving:
//  1. Right processes contextual value "group:9" → sets lastRightVal = "group:9"
//  2. Left processes ["group:1", "group:2"] → "group:1" stored, "group:2" PRUNED
//     because "group:2" > "group:9" is false
//  3. Right processes datastore value "group:2" → checks leftSeen["group:2"] → NOT FOUND
//  4. Result: Allowed=false (incorrect; "group:2" is in both sets)
//
// The fix: Concat returns IsOrdered()=false, disabling pruning when sources are combined.
// We still force the adversarial interleaving to confirm the old code would have failed.
func TestWeight2ExecuteConcatOrderingBug(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })
	ctx := context.Background()

	// Right: Concat of contextual tuple (high value) then gated datastore (low value).
	// Gate ensures right produces "group:9" and parks before left sends anything,
	// forcing the exact interleaving that triggered the bug.
	// UsersetKind mapper extracts the object portion from the User field:
	//   "group:9#members" → "group:9",  "group:2#members" → "group:2"
	datastoreGated, gate, blocked := newGatedIterator[*openfgav1.TupleKey](
		storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{
			{Object: "document:1", Relation: "viewer", User: "group:2#members"},
		}),
	)
	rightIter := storage.WrapIterator(storage.UsersetKind, iterator.Concat[*openfgav1.TupleKey](
		storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{
			{Object: "document:1", Relation: "viewer", User: "group:9#members"},
		}),
		datastoreGated,
	))

	leftChan := make(chan *iterator.Msg, 1)
	go func() {
		// Wait until the ToChannel goroutine for rightIter is parked at the gate,
		// meaning "group:9" is already in the right channel buffer. Only then send
		// left so that execute processes right's contextual tuple first.
		<-blocked
		leftChan <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"group:1", "group:2"})}
		close(leftChan)
		// Ungate the datastore source; execute will process left entirely before
		// returning to the outer select, so timing here doesn't affect correctness.
		close(gate)
	}()

	w2 := &Weight2{}
	res, err := w2.execute(ctx, leftChan, rightIter)
	require.NoError(t, err)
	require.NotNil(t, res)
	// "group:2" exists in both left and right; the intersection must be non-empty.
	require.True(t, res.GetAllowed(), "expected Allowed=true because group:2 exists in both channels")
}
