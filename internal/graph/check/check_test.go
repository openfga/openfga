package check_test

import (
	"context"
	"fmt"
	"log"
	"testing"

	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/cmd/util"
	"github.com/openfga/openfga/internal/dispatch"
	"github.com/openfga/openfga/internal/dispatch/local"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/graph/check"

	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	storagetest "github.com/openfga/openfga/pkg/storage/test"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"

	dispatchv1 "github.com/openfga/openfga/internal/proto/dispatch/v1alpha1"
)

var cmpOpts = cmpopts.IgnoreUnexported(
	dispatchv1.DispatchCheckResponse{},
	dispatchv1.DispatchCheckResolutionMetadata{},
	dispatchv1.DispatchCheckResult{},
)

func TestCheck(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()

	t.Run("reflexive_subjects_return_promptly", func(t *testing.T) {
		mockController := gomock.NewController(t)
		t.Cleanup(mockController.Finish)

		mockDispatcher := mocks.NewMockDispatcher(mockController)
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

		checkResolver := check.NewCheckResolver(mockDatastore).WithDispatcher(mockDispatcher)

		model := parser.MustTransformDSLToProto(`
		model
		  schema 1.1

		type user

		type group
		  relations
		    define member: [user]
		`)

		typesys, err := typesystem.NewAndValidate(ctx, model)
		require.NoError(t, err)

		checkResp, err := checkResolver.Check(ctx, &check.CheckRequest{
			StoreID:         ulid.Make().String(),
			Typesystem:      typesys,
			ObjectType:      "group",
			ObjectIDs:       []string{"x", "x"},
			Relation:        "member",
			SubjectType:     "group",
			SubjectID:       "x",
			SubjectRelation: "member",
		})
		require.NoError(t, err)

		expected := &dispatchv1.DispatchCheckResponse{
			ResultsForeachObjectId: map[string]*dispatchv1.DispatchCheckResult{
				"x": {Allowed: true},
			},
			ResolutionMetadata: &dispatchv1.DispatchCheckResolutionMetadata{
				DatastoreQueryCount: 0,
				DispatchCount:       0,
				CycleDetected:       false,
			},
		}

		if diff := cmp.Diff(expected, checkResp, cmpOpts); diff != "" {
			require.FailNow(t, "mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("non_terminal_related_subject_returns_promptly", func(t *testing.T) {
		mockController := gomock.NewController(t)
		t.Cleanup(mockController.Finish)

		mockDispatcher := mocks.NewMockDispatcher(mockController)
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

		checkResolver := check.NewCheckResolver(mockDatastore).WithDispatcher(mockDispatcher)

		model := parser.MustTransformDSLToProto(`
		model
		  schema 1.1

		type employee
		type user

		type group
		  relations
		    define member: [employee]

		type org
		  relations
		    define member: [user]
		`)

		typesys, err := typesystem.NewAndValidate(ctx, model)
		require.NoError(t, err)

		t.Run("subject_is_object", func(t *testing.T) {
			checkResp, err := checkResolver.Check(ctx, &check.CheckRequest{
				StoreID:         ulid.Make().String(),
				Typesystem:      typesys,
				ObjectType:      "group",
				ObjectIDs:       []string{"x"},
				Relation:        "member",
				SubjectType:     "org",
				SubjectID:       "acme",
				SubjectRelation: "member",
			})
			require.NoError(t, err)

			expected := &dispatchv1.DispatchCheckResponse{
				ResultsForeachObjectId: map[string]*dispatchv1.DispatchCheckResult{
					"x": {Allowed: false},
				},
				ResolutionMetadata: &dispatchv1.DispatchCheckResolutionMetadata{
					DatastoreQueryCount: 0,
					DispatchCount:       0,
					CycleDetected:       false,
				},
			}

			if diff := cmp.Diff(expected, checkResp, cmpOpts); diff != "" {
				require.FailNow(t, "mismatch (-want +got):\n%s", diff)
			}
		})

		t.Run("subject_is_userset", func(t *testing.T) {
			checkResp, err := checkResolver.Check(ctx, &check.CheckRequest{
				StoreID:         ulid.Make().String(),
				Typesystem:      typesys,
				ObjectType:      "group",
				ObjectIDs:       []string{"x"},
				Relation:        "member",
				SubjectType:     "user",
				SubjectID:       "jon",
				SubjectRelation: "",
			})
			require.NoError(t, err)

			expected := &dispatchv1.DispatchCheckResponse{
				ResultsForeachObjectId: map[string]*dispatchv1.DispatchCheckResult{
					"x": {Allowed: false},
				},
				ResolutionMetadata: &dispatchv1.DispatchCheckResolutionMetadata{
					DatastoreQueryCount: 0,
					DispatchCount:       0,
					CycleDetected:       false,
				},
			}

			if diff := cmp.Diff(expected, checkResp, cmpOpts); diff != "" {
				require.FailNow(t, "mismatch (-want +got):\n%s", diff)
			}
		})
	})

	t.Run("terminal_relationships", func(t *testing.T) {
		mockController := gomock.NewController(t)
		t.Cleanup(mockController.Finish)

		mockDispatcher := mocks.NewMockDispatcher(mockController)
		mockDispatcher.EXPECT().DispatchCheck(gomock.Any(), gomock.Any()).Times(0)

		ds := memory.New()
		t.Cleanup(ds.Close)

		checkResolver := check.NewCheckResolver(ds).WithDispatcher(mockDispatcher)

		modelStr := `
		model
		  schema 1.1

		type user

		type group
		  relations
		    define member: [user, group#member]

		type document
		  relations
		    define editor: [group#member]
		    define viewer: [user]
		`

		storeID, model := storagetest.BootstrapFGAStore(t, ds, modelStr, []string{
			"document:1#viewer@user:jon",
			"document:2#viewer@user:jon",
			"document:3#viewer@user:will",
			"document:1#editor@group:eng#member",
			"document:2#editor@group:eng#member",
			"document:3#editor@group:other#member",
		})

		typesys, err := typesystem.NewAndValidate(ctx, model)
		require.NoError(t, err)

		t.Run("subject_is_object", func(t *testing.T) {
			checkResp, err := checkResolver.Check(ctx, &check.CheckRequest{
				StoreID:         storeID,
				Typesystem:      typesys,
				ObjectType:      "document",
				ObjectIDs:       []string{"1", "2", "3"},
				Relation:        "viewer",
				SubjectType:     "user",
				SubjectID:       "jon",
				SubjectRelation: "",
			})
			require.NoError(t, err)

			expected := &dispatchv1.DispatchCheckResponse{
				ResultsForeachObjectId: map[string]*dispatchv1.DispatchCheckResult{
					"1": {Allowed: true},
					"2": {Allowed: true},
					"3": {Allowed: false},
				},
				ResolutionMetadata: &dispatchv1.DispatchCheckResolutionMetadata{
					DatastoreQueryCount: 1,
					DispatchCount:       0,
					CycleDetected:       false,
				},
			}

			if diff := cmp.Diff(expected, checkResp, cmpOpts); diff != "" {
				require.FailNow(t, "mismatch (-want +got):\n%s", diff)
			}
		})

		t.Run("subject_is_userset", func(t *testing.T) {
			checkResp, err := checkResolver.Check(ctx, &check.CheckRequest{
				StoreID:         storeID,
				Typesystem:      typesys,
				ObjectType:      "document",
				ObjectIDs:       []string{"1", "2", "3"},
				Relation:        "editor",
				SubjectType:     "group",
				SubjectID:       "eng",
				SubjectRelation: "member",
			})
			require.NoError(t, err)

			expected := &dispatchv1.DispatchCheckResponse{
				ResultsForeachObjectId: map[string]*dispatchv1.DispatchCheckResult{
					"1": {Allowed: true},
					"2": {Allowed: true},
					"3": {Allowed: false},
				},
				ResolutionMetadata: &dispatchv1.DispatchCheckResolutionMetadata{
					DatastoreQueryCount: 1,
					DispatchCount:       0,
					CycleDetected:       false,
				},
			}

			if diff := cmp.Diff(expected, checkResp, cmpOpts); diff != "" {
				require.FailNow(t, "mismatch (-want +got):\n%s", diff)
			}
		})
	})

	t.Run("resolve_any_not_all_nested_dispatches", func(t *testing.T) {
		mockController := gomock.NewController(t)
		t.Cleanup(mockController.Finish)

		mockDispatcher := mocks.NewMockDispatcher(mockController)
		mockDispatcher.EXPECT().
			DispatchCheck(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, req *dispatchv1.DispatchCheckRequest) (*dispatchv1.DispatchCheckResponse, error) {
				if req.GetObjectIds()[0] == "1" {
					return &dispatchv1.DispatchCheckResponse{
						ResultsForeachObjectId: map[string]*dispatchv1.DispatchCheckResult{
							"1": {Allowed: true},
						},
						ResolutionMetadata: &dispatchv1.DispatchCheckResolutionMetadata{
							DatastoreQueryCount: 2,
							DispatchCount:       1,
							CycleDetected:       false,
						},
					}, nil
				}

				return nil, fmt.Errorf("some error occurred")
			}).
			AnyTimes()

		ds := memory.New()
		t.Cleanup(ds.Close)

		checkResolver := check.NewCheckResolver(ds, check.WithMaxConcurrentDispatches(1), check.WithMaxDispatchBatchSize(1)).WithDispatcher(mockDispatcher)

		modelStr := `
		model
		  schema 1.1

		type user

		type group
		  relations
		    define member: [user, group#member]
		`

		storeID, model := storagetest.BootstrapFGAStore(t, ds, modelStr, []string{
			"group:1#member@user:jon",
			"group:x#member@group:1#member",
			"group:x#member@group:2#member",
		})

		typesys, err := typesystem.NewAndValidate(ctx, model)
		require.NoError(t, err)

		checkResp, err := checkResolver.Check(ctx, &check.CheckRequest{
			StoreID:         storeID,
			Typesystem:      typesys,
			ObjectType:      "group",
			ObjectIDs:       []string{"x"},
			Relation:        "member",
			SubjectType:     "user",
			SubjectID:       "jon",
			SubjectRelation: "",
		})
		require.NoError(t, err)

		expected := &dispatchv1.DispatchCheckResponse{
			ResultsForeachObjectId: map[string]*dispatchv1.DispatchCheckResult{
				"x": {Allowed: true},
			},
			ResolutionMetadata: &dispatchv1.DispatchCheckResolutionMetadata{
				DatastoreQueryCount: 4, // 1 (group:x#member@user:jon), 1 find usersets [group:1#member, group:2#member], 1 (group:1#member@user:jon), potentially 1 more (group:2#member@user:jon)
				DispatchCount:       2, // 2 dispatches - 1 for group:1#member@user:jon, 1 for group:2#member@user:jon
				CycleDetected:       false,
			},
		}

		if diff := cmp.Diff(expected, checkResp, cmpOpts); diff != "" {
			require.FailNow(t, "mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("nested_groups_of_the_same_type_multiple_objects", func(t *testing.T) {
		mockController := gomock.NewController(t)
		t.Cleanup(mockController.Finish)

		ds := memory.New()
		t.Cleanup(ds.Close)

		modelStr := `
		model
		  schema 1.1

		type user

		type group
		  relations
		    define member: [user, group#member]
		`

		storeID, model := storagetest.BootstrapFGAStore(t, ds, modelStr, []string{
			"group:x#member@group:1#member",
			"group:x#member@group:2#member",
			"group:x#member@group:3#member",
		})

		mockDispatcher := mocks.NewMockDispatcher(mockController)
		mockDispatcher.EXPECT().DispatchCheck(gomock.Any(), &dispatchv1.DispatchCheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: model.GetId(),
			ObjectType:           "group",
			ObjectIds:            []string{"1", "2", "3"},
			SubjectType:          "user",
			SubjectId:            "jon",
			SubjectRelation:      "",
		}).Return(&dispatchv1.DispatchCheckResponse{
			ResultsForeachObjectId: map[string]*dispatchv1.DispatchCheckResult{
				"1": {Allowed: false},
				"2": {Allowed: false},
				"3": {Allowed: false},
			},
			ResolutionMetadata: &dispatchv1.DispatchCheckResolutionMetadata{
				DatastoreQueryCount: 2,
				DispatchCount:       0,
				CycleDetected:       false,
			},
		}, nil)

		checkResolver := check.NewCheckResolver(ds).WithDispatcher(mockDispatcher)

		typesys, err := typesystem.NewAndValidate(ctx, model)
		require.NoError(t, err)

		checkResp, err := checkResolver.Check(ctx, &check.CheckRequest{
			StoreID:         storeID,
			Typesystem:      typesys,
			ObjectType:      "group",
			ObjectIDs:       []string{"x"},
			Relation:        "member",
			SubjectType:     "user",
			SubjectID:       "jon",
			SubjectRelation: "",
		})
		require.NoError(t, err)

		expected := &dispatchv1.DispatchCheckResponse{
			ResultsForeachObjectId: map[string]*dispatchv1.DispatchCheckResult{
				"x": {Allowed: false},
			},
			ResolutionMetadata: &dispatchv1.DispatchCheckResolutionMetadata{
				DatastoreQueryCount: 4, // 1 (terminal lookup) + 1 (userset lookup) + 2 (from dispatched result)
				DispatchCount:       1,
				CycleDetected:       false,
			},
		}

		if diff := cmp.Diff(expected, checkResp); diff != "" {
			require.FailNow(t, "mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("ttu_with_multiple_tupleset_relationships", func(t *testing.T) {
		mockController := gomock.NewController(t)
		t.Cleanup(mockController.Finish)

		ds := memory.New()
		t.Cleanup(ds.Close)

		modelStr := `
		model
		  schema 1.1

		type user

		type folder
		  relations
		    define viewer: [user]

		type document
		  relations
		    define parent: [folder]
		    define viewer: viewer from parent
		`

		storeID, model := storagetest.BootstrapFGAStore(t, ds, modelStr, []string{
			"document:x#parent@folder:1",
			"document:x#parent@folder:2",
			"document:x#parent@folder:3",
		})

		mockDispatcher := mocks.NewMockDispatcher(mockController)
		mockDispatcher.EXPECT().DispatchCheck(gomock.Any(), &dispatchv1.DispatchCheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: model.GetId(),
			ObjectType:           "folder",
			ObjectIds:            []string{"1", "2", "3"},
			Relation:             "viewer",
			SubjectType:          "user",
			SubjectId:            "jon",
			SubjectRelation:      "",
		}).Return(&dispatchv1.DispatchCheckResponse{
			ResultsForeachObjectId: map[string]*dispatchv1.DispatchCheckResult{
				"1": {Allowed: false},
				"2": {Allowed: false},
				"3": {Allowed: false},
			},
			ResolutionMetadata: &dispatchv1.DispatchCheckResolutionMetadata{
				DatastoreQueryCount: 2, // 1 (for terminal lookup)
				DispatchCount:       0,
				CycleDetected:       false,
			},
		}, nil)

		checkResolver := check.NewCheckResolver(ds).WithDispatcher(mockDispatcher)

		typesys, err := typesystem.NewAndValidate(ctx, model)
		require.NoError(t, err)

		checkResp, err := checkResolver.Check(ctx, &check.CheckRequest{
			StoreID:         storeID,
			Typesystem:      typesys,
			ObjectType:      "document",
			ObjectIDs:       []string{"x"},
			Relation:        "viewer",
			SubjectType:     "user",
			SubjectID:       "jon",
			SubjectRelation: "",
		})
		require.NoError(t, err)

		expected := &dispatchv1.DispatchCheckResponse{
			ResultsForeachObjectId: map[string]*dispatchv1.DispatchCheckResult{
				"x": {Allowed: false},
			},
			ResolutionMetadata: &dispatchv1.DispatchCheckResolutionMetadata{
				DatastoreQueryCount: 3, // 1 (TTU tupleset read) + 2 (from dispatched request)
				DispatchCount:       1,
				CycleDetected:       false,
			},
		}

		if diff := cmp.Diff(expected, checkResp); diff != "" {
			require.FailNow(t, "mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestCheck_DatastoreQueryCount(t *testing.T) {

}

func TestCheck_DispatchCount(t *testing.T) {

}

// BenchmarkCheck runs Check benchmarks for the different Check implementations.
func BenchmarkCheck(b *testing.B) {
	ctx := context.Background()

	// LocalChecker is the implementation which doesn't have internal batch dispatching.
	b.Run("LocalChecker", func(b *testing.B) {
		_, ds, _ := util.MustBootstrapDatastore(b, "postgres")
		b.Cleanup(ds.Close)

		modelStr := `
		model
		  schema 1.1

		type user

		type group
		  relations
		    define member: [user, group#member]
		`

		var tuples []string
		for i := 0; i < 1000; i++ {
			tuples = append(tuples, fmt.Sprintf("group:x#member@group:%d#member", i))
		}

		storeID, model := storagetest.BootstrapFGAStore(b, ds, modelStr, tuples)

		checkResolver := graph.NewLocalChecker(
			graph.WithResolveNodeBreadthLimit(100),
		)

		ctx := typesystem.ContextWithTypesystem(
			context.Background(),
			typesystem.New(model),
		)

		ctx = storage.ContextWithRelationshipTupleReader(ctx, storagewrappers.NewBoundedConcurrencyTupleReader(ds, 75))

		datastoreQueryCount := 0

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			checkResp, err := checkResolver.ResolveCheck(ctx, &graph.ResolveCheckRequest{
				StoreID:              storeID,
				AuthorizationModelID: model.GetId(),
				TupleKey:             tuple.NewTupleKey("group:x", "member", "user:jon"),
				RequestMetadata:      graph.NewCheckRequestMetadata(25),
			})
			require.NoError(b, err)
			datastoreQueryCount = int(checkResp.GetResolutionMetadata().DatastoreQueryCount)
		}

		log.Printf("LocalChecker datastoreQueryCount: '%d'", datastoreQueryCount)
	})

	// CheckResolver is the implementation with internal batch dispatching.
	b.Run("CheckResolver", func(b *testing.B) {
		_, ds, _ := util.MustBootstrapDatastore(b, "postgres")
		b.Cleanup(ds.Close)

		modelStr := `
		model
		  schema 1.1

		type user

		type group
		  relations
		    define member: [user, group#member]
		`

		var tuples []string
		for i := 0; i < 1000; i++ {
			tuples = append(tuples, fmt.Sprintf("group:x#member@group:%d#member", i))
		}

		storeID, model := storagetest.BootstrapFGAStore(b, ds, modelStr, tuples)

		typesystemResolver, typesystemResolverStop := typesystem.MemoizedTypesystemResolverFunc(ds)
		b.Cleanup(typesystemResolverStop)

		dispatcher := local.NewLocalDispatcher(
			ds,
			typesystemResolver,
			local.WithMaxConcurrentDispatches(100),
			local.WithMaxDispatchBatchSize(100),
			local.WithMaxReadQueriesInflight(75),
		)

		datastoreQueryCount := 0

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dispatchResp, err := dispatcher.DispatchCheck(ctx, &dispatchv1.DispatchCheckRequest{
				StoreId:              storeID,
				AuthorizationModelId: model.GetId(),
				ObjectType:           "group",
				ObjectIds:            []string{"x"},
				Relation:             "member",
				SubjectType:          "user",
				SubjectId:            "jon",
				SubjectRelation:      "",
				RequestMetadata:      dispatch.NewDispatchCheckRequestMetadata(dispatch.DefaultCheckResolutionDepth),
			})
			require.NoError(b, err)

			datastoreQueryCount = int(dispatchResp.GetResolutionMetadata().GetDatastoreQueryCount())
		}

		log.Printf("CheckResolver datastoreQueryCount: '%d'", datastoreQueryCount)
	})
}
