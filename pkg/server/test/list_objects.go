package test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/karlseguin/ccache/v3"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

type mockStreamServer struct {
	grpc.ServerStream
	channel chan string
}

func (x *mockStreamServer) Send(m *openfgav1.StreamedListObjectsResponse) error {
	x.channel <- m.Object
	return nil
}

type listObjectsTestCase struct {
	name                   string
	schema                 string
	tuples                 []*openfgav1.TupleKey
	model                  string
	authorizationModel     *openfgav1.AuthorizationModel
	objectType             string
	user                   string
	relation               string
	contextualTuples       *openfgav1.ContextualTupleKeys
	context                *structpb.Struct
	allResults             []string // all the results. the server may return less
	maxResults             uint32
	minimumResultsExpected uint32
	listObjectsDeadline    time.Duration // 10 seconds if not set
	readTuplesDelay        time.Duration // if set, purposely use a slow storage to slow down read and simulate timeout
	useCheckCache          bool
}

func TestListObjects(t *testing.T, ds storage.OpenFGADatastore) {
	testCases := []listObjectsTestCase{
		{
			name:   "max_results_equal_0_with_simple_model",
			schema: typesystem.SchemaVersion1_1,
			model: `model
	schema 1.1
type user
type repo
  relations
	define admin: [user]`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("repo:1", "admin", "user:alice"),
				tuple.NewTupleKey("repo:2", "admin", "user:alice"),
			},
			user:       "user:alice",
			objectType: "repo",
			relation:   "admin",
			contextualTuples: &openfgav1.ContextualTupleKeys{
				TupleKeys: []*openfgav1.TupleKey{tuple.NewTupleKey("repo:3", "admin", "user:alice")},
			},
			maxResults:             0,
			minimumResultsExpected: 3,
			allResults:             []string{"repo:1", "repo:2", "repo:3"},
			useCheckCache:          false,
		},
		{
			name:   "max_results_equal_2_with_simple_model",
			schema: typesystem.SchemaVersion1_1,
			model: `model
	schema 1.1
type user
type repo
  relations
	define admin: [user]`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("repo:1", "admin", "user:alice"),
				tuple.NewTupleKey("repo:2", "admin", "user:alice"),
			},
			user:       "user:alice",
			objectType: "repo",
			relation:   "admin",
			contextualTuples: &openfgav1.ContextualTupleKeys{
				TupleKeys: []*openfgav1.TupleKey{tuple.NewTupleKey("repo:3", "admin", "user:alice")},
			},
			maxResults:             2,
			minimumResultsExpected: 2,
			allResults:             []string{"repo:1", "repo:2", "repo:3"},
			useCheckCache:          false,
		},
		{
			name:   "max_results_with_model_that_uses_exclusion",
			schema: typesystem.SchemaVersion1_1,
			model: `model
	schema 1.1
type user
type org
  relations
	define blocked: [user]
	define admin: [user] but not blocked`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("org:1", "admin", "user:charlie"),
				tuple.NewTupleKey("org:2", "admin", "user:charlie"),
			},
			user:       "user:charlie",
			objectType: "org",
			relation:   "admin",
			contextualTuples: &openfgav1.ContextualTupleKeys{
				TupleKeys: []*openfgav1.TupleKey{tuple.NewTupleKey("org:3", "admin", "user:charlie")},
			},
			maxResults:             2,
			minimumResultsExpected: 2,
			allResults:             []string{"org:1", "org:2", "org:3"},
			useCheckCache:          false,
		},
		{
			name:   "max_results_with_model_that_uses_exclusion_and_one_object_is_a_false_candidate",
			schema: typesystem.SchemaVersion1_1,
			model: `model
	schema 1.1
type user
type org
  relations
	define blocked: [user]
	define admin: [user] but not blocked`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("org:2", "blocked", "user:charlie"),
				tuple.NewTupleKey("org:1", "admin", "user:charlie"),
				tuple.NewTupleKey("org:2", "admin", "user:charlie"),
				tuple.NewTupleKey("org:3", "admin", "user:charlie"),
			},
			user:                   "user:charlie",
			objectType:             "org",
			relation:               "admin",
			contextualTuples:       &openfgav1.ContextualTupleKeys{},
			maxResults:             2,
			minimumResultsExpected: 2,
			allResults:             []string{"org:1", "org:3"},
			useCheckCache:          false,
		},
		{
			name:   "respects_when_schema_1_1_and_maxresults_is_higher_than_actual_result_length",
			schema: typesystem.SchemaVersion1_1,
			model: `model
	schema 1.1
type user

type team
  relations
	define admin: [user]`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("team:1", "admin", "user:bob"),
			},
			user:                   "user:bob",
			objectType:             "team",
			relation:               "admin",
			maxResults:             2,
			minimumResultsExpected: 1,
			allResults:             []string{"team:1"},
			useCheckCache:          false,
		},
		{
			name:   "respects_max_results_when_deadline_timeout_and_returns_no_error_and_no_results",
			schema: typesystem.SchemaVersion1_1,
			model: `model
	schema 1.1
type user
type repo
  relations
	define admin: [user]`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("repo:1", "admin", "user:alice"),
				tuple.NewTupleKey("repo:2", "admin", "user:alice"),
			},
			user:                   "user:alice",
			objectType:             "repo",
			relation:               "admin",
			maxResults:             2,
			minimumResultsExpected: 0,
			// We expect empty array to be returned as list object will timeout due to readTuplesDelay > listObjectsDeadline
			allResults:          []string{},
			listObjectsDeadline: 1 * time.Second,
			readTuplesDelay:     2 * time.Second, // We are mocking the ds to slow down the read call and simulate timeout
			useCheckCache:       false,
		},
		{
			name:   "list_object_use_check_cache",
			schema: typesystem.SchemaVersion1_1,
			model: `model
	schema 1.1
type user
type repo
  relations
	define admin: [user]`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("repo:1", "admin", "user:alice"),
				tuple.NewTupleKey("repo:2", "admin", "user:alice"),
			},
			user:       "user:alice",
			objectType: "repo",
			relation:   "admin",
			contextualTuples: &openfgav1.ContextualTupleKeys{
				TupleKeys: []*openfgav1.TupleKey{tuple.NewTupleKey("repo:3", "admin", "user:alice")},
			},
			maxResults:             2,
			minimumResultsExpected: 2,
			allResults:             []string{"repo:1", "repo:2", "repo:3"},
			// when we use cache, the regular_endpoint should pick up the cached value from the streaming_endpoint run
			useCheckCache: true,
		},
		{
			name: "condition_with_tuples",
			authorizationModel: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										typesystem.ConditionedRelationReference(typesystem.DirectRelationReference("user", ""), "condition1"),
									},
								},
							},
						},
					},
				},
				Conditions: map[string]*openfgav1.Condition{
					"condition1": {
						Name:       "condition1",
						Expression: "param1 == 'ok'",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"param1": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
							},
						},
					},
				},
			},
			tuples: []*openfgav1.TupleKey{
				{
					User:     "user:anne",
					Relation: "viewer",
					Object:   "document:1",
					Condition: &openfgav1.RelationshipCondition{
						Name:    "condition1",
						Context: testutils.MustNewStruct(t, map[string]interface{}{"param1": "ok"}),
					},
				},
				{
					User:     "user:anne",
					Relation: "viewer",
					Object:   "document:2",
					Condition: &openfgav1.RelationshipCondition{
						Name:    "condition1",
						Context: testutils.MustNewStruct(t, map[string]interface{}{"param1": "notok"}),
					},
				},
			},
			user:                   "user:anne",
			objectType:             "document",
			relation:               "viewer",
			contextualTuples:       nil,
			maxResults:             1,
			minimumResultsExpected: 1,
			allResults:             []string{"document:1"},
			useCheckCache:          false,
		},
		{
			name: "condition_with_contextual_tuples",
			authorizationModel: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										typesystem.ConditionedRelationReference(typesystem.DirectRelationReference("user", ""), "condition1"),
									},
								},
							},
						},
					},
				},
				Conditions: map[string]*openfgav1.Condition{
					"condition1": {
						Name:       "condition1",
						Expression: "param1 == 'ok'",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"param1": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
							},
						},
					},
				},
			},
			tuples:     nil,
			user:       "user:anne",
			objectType: "document",
			relation:   "viewer",
			contextualTuples: &openfgav1.ContextualTupleKeys{
				TupleKeys: []*openfgav1.TupleKey{
					{
						User:     "user:anne",
						Relation: "viewer",
						Object:   "document:1",
						Condition: &openfgav1.RelationshipCondition{
							Name:    "condition1",
							Context: testutils.MustNewStruct(t, map[string]interface{}{"param1": "ok"}),
						},
					},
					{
						User:     "user:anne",
						Relation: "viewer",
						Object:   "document:2",
						Condition: &openfgav1.RelationshipCondition{
							Name:    "condition1",
							Context: testutils.MustNewStruct(t, map[string]interface{}{"param1": "notok"}),
						},
					},
				},
			},
			maxResults:             1,
			minimumResultsExpected: 1,
			allResults:             []string{"document:1"},
			useCheckCache:          false,
		},
		{
			name: "condition_with_tuples_and_contextual_tuples",
			authorizationModel: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										typesystem.ConditionedRelationReference(typesystem.DirectRelationReference("user", ""), "condition1"),
									},
								},
							},
						},
					},
				},
				Conditions: map[string]*openfgav1.Condition{
					"condition1": {
						Name:       "condition1",
						Expression: "param1 == 'ok'",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"param1": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
							},
						},
					},
				},
			},
			tuples: []*openfgav1.TupleKey{
				{
					User:     "user:anne",
					Relation: "viewer",
					Object:   "document:1",
					Condition: &openfgav1.RelationshipCondition{
						Name:    "condition1",
						Context: testutils.MustNewStruct(t, map[string]interface{}{"param1": "notok"}),
					},
				},
				{
					User:     "user:anne",
					Relation: "viewer",
					Object:   "document:2",
					Condition: &openfgav1.RelationshipCondition{
						Name:    "condition1",
						Context: testutils.MustNewStruct(t, map[string]interface{}{"param1": "notok"}),
					},
				},
			},
			user:       "user:anne",
			objectType: "document",
			relation:   "viewer",
			contextualTuples: &openfgav1.ContextualTupleKeys{
				TupleKeys: []*openfgav1.TupleKey{
					{
						User:     "user:anne",
						Relation: "viewer",
						Object:   "document:1",
						Condition: &openfgav1.RelationshipCondition{
							Name:    "condition1",
							Context: testutils.MustNewStruct(t, map[string]interface{}{"param1": "ok"}),
						},
					},
					{
						User:     "user:anne",
						Relation: "viewer",
						Object:   "document:2",
						Condition: &openfgav1.RelationshipCondition{
							Name:    "condition1",
							Context: testutils.MustNewStruct(t, map[string]interface{}{"param1": "ok"}),
						},
					},
				},
			},
			maxResults:             2,
			minimumResultsExpected: 2,
			allResults:             []string{"document:1", "document:2"},
			useCheckCache:          false,
		},
		{
			name: "condition_with_tuples_and_contextual_tuples_and_context",
			authorizationModel: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										typesystem.ConditionedRelationReference(typesystem.DirectRelationReference("user", ""), "condition1"),
									},
								},
							},
						},
					},
				},
				Conditions: map[string]*openfgav1.Condition{
					"condition1": {
						Name:       "condition1",
						Expression: "param1 == 'ok' && param2 == 'ok'",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"param1": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
							},
							"param2": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
							},
						},
					},
				},
			},
			tuples: []*openfgav1.TupleKey{
				{
					User:     "user:anne",
					Relation: "viewer",
					Object:   "document:1",
					Condition: &openfgav1.RelationshipCondition{
						Name:    "condition1",
						Context: testutils.MustNewStruct(t, map[string]interface{}{"param1": "ok"}),
					},
				},
				{
					User:     "user:anne",
					Relation: "viewer",
					Object:   "document:2",
					Condition: &openfgav1.RelationshipCondition{
						Name:    "condition1",
						Context: testutils.MustNewStruct(t, map[string]interface{}{"param1": "ok"}),
					},
				},
			},
			user:       "user:anne",
			objectType: "document",
			relation:   "viewer",
			context:    testutils.MustNewStruct(t, map[string]interface{}{"param2": "ok"}),
			contextualTuples: &openfgav1.ContextualTupleKeys{
				TupleKeys: []*openfgav1.TupleKey{
					{
						User:     "user:anne",
						Relation: "viewer",
						Object:   "document:3",
						Condition: &openfgav1.RelationshipCondition{
							Name:    "condition1",
							Context: testutils.MustNewStruct(t, map[string]interface{}{"param1": "ok"}),
						},
					},
					{
						User:     "user:anne",
						Relation: "viewer",
						Object:   "document:4",
						Condition: &openfgav1.RelationshipCondition{
							Name:    "condition1",
							Context: testutils.MustNewStruct(t, map[string]interface{}{"param1": "ok"}),
						},
					},
				},
			},
			maxResults:             4,
			minimumResultsExpected: 4,
			allResults:             []string{"document:1", "document:2", "document:3", "document:4"},
			useCheckCache:          false,
		},
		{
			name: "condition_in_ttu_relationships",
			authorizationModel: parser.MustTransformDSLToProto(`model
  schema 1.1

type user

type folder
  relations
    define viewer: [user]

type document
  relations
    define parent: [folder with condition1]
	define viewer: viewer from parent

condition condition1(x: int) {
  x < 100
}`),
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:1", "parent", "folder:x", "condition1", nil),
				tuple.NewTupleKey("folder:x", "viewer", "user:jon"),
			},
			user:                   "user:jon",
			objectType:             "document",
			relation:               "viewer",
			context:                testutils.MustNewStruct(t, map[string]interface{}{"x": 50}),
			minimumResultsExpected: 1,
			allResults:             []string{"document:1"},
			useCheckCache:          false,
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			storeID := ulid.Make().String()

			// arrange: write model
			model := test.authorizationModel

			if model == nil {
				model = &openfgav1.AuthorizationModel{
					Id:              ulid.Make().String(),
					SchemaVersion:   test.schema,
					TypeDefinitions: parser.MustTransformDSLToProto(test.model).TypeDefinitions,
				}
			}

			if model.Id == "" {
				model.Id = ulid.Make().String()
			}

			err := ds.WriteAuthorizationModel(ctx, storeID, model)
			require.NoError(t, err)

			// arrange: write tuples
			err = ds.Write(context.Background(), storeID, nil, test.tuples)
			require.NoError(t, err)

			// act: run ListObjects

			datastore := ds
			if test.readTuplesDelay > 0 {
				datastore = mocks.NewMockSlowDataStorage(ds, test.readTuplesDelay)
			}

			ctx = typesystem.ContextWithTypesystem(ctx, typesystem.New(model))

			opts := []commands.ListObjectsQueryOption{
				commands.WithListObjectsMaxResults(test.maxResults),
				commands.WithListObjectsDeadline(10 * time.Second),
			}

			if test.listObjectsDeadline != 0 {
				opts = append(opts, commands.WithListObjectsDeadline(test.listObjectsDeadline))
			}

			checkOptions := []graph.LocalCheckerOption{
				graph.WithResolveNodeBreadthLimit(100),
				graph.WithMaxConcurrentReads(30),
			}

			if test.useCheckCache {
				checkCache := ccache.New(
					ccache.Configure[*graph.CachedResolveCheckResponse]().MaxSize(100),
				)
				defer checkCache.Stop()

				checkOptions = append(checkOptions, graph.WithCachedResolver(
					graph.WithExistingCache(checkCache),
					graph.WithCacheTTL(10*time.Second),
				))
			}

			opts = append(opts, commands.WithCheckOptions(checkOptions))
			listObjectsQuery := commands.NewListObjectsQuery(datastore, opts...)

			// assertions
			t.Run("streaming_endpoint", func(t *testing.T) {
				server := &mockStreamServer{
					channel: make(chan string, len(test.allResults)),
				}

				done := make(chan struct{})
				var streamedObjectIds []string
				go func() {
					for x := range server.channel {
						streamedObjectIds = append(streamedObjectIds, x)
					}

					done <- struct{}{}
				}()

				_, err := listObjectsQuery.ExecuteStreamed(ctx, &openfgav1.StreamedListObjectsRequest{
					StoreId:          storeID,
					Type:             test.objectType,
					Relation:         test.relation,
					User:             test.user,
					ContextualTuples: test.contextualTuples,
					Context:          test.context,
				}, server)
				close(server.channel)
				<-done

				require.NoError(t, err)
				// there is no upper bound of the number of results for the streamed version
				require.GreaterOrEqual(t, len(streamedObjectIds), int(test.minimumResultsExpected))
				require.ElementsMatch(t, test.allResults, streamedObjectIds)
			})

			t.Run("regular_endpoint", func(t *testing.T) {
				res, err := listObjectsQuery.Execute(ctx, &openfgav1.ListObjectsRequest{
					StoreId:          storeID,
					Type:             test.objectType,
					Relation:         test.relation,
					User:             test.user,
					ContextualTuples: test.contextualTuples,
					Context:          test.context,
				})

				require.NotNil(t, res)
				require.NoError(t, err)
				if test.maxResults != 0 { // don't get all results
					require.LessOrEqual(t, len(res.Objects), int(test.maxResults))
				}
				require.GreaterOrEqual(t, len(res.Objects), int(test.minimumResultsExpected))
				require.Subset(t, test.allResults, res.Objects)
			})
		})
	}
}

// Used to avoid compiler optimizations (see https://dave.cheney.net/2013/06/30/how-to-write-benchmarks-in-go)
var listObjectsResponse *commands.ListObjectsResponse //nolint

// setupListObjectsBenchmark writes the model and lots of tuples
func setupListObjectsBenchmark(b *testing.B, ds storage.OpenFGADatastore, storeID string) (*openfgav1.AuthorizationModel, string, int) {
	b.Helper()
	modelID := ulid.Make().String()
	model := &openfgav1.AuthorizationModel{
		Id:            modelID,
		SchemaVersion: typesystem.SchemaVersion1_1,
		TypeDefinitions: parser.MustTransformDSLToProto(`model
	schema 1.1
type user

type document
  relations
	define viewer: [user]`).TypeDefinitions,
	}
	err := ds.WriteAuthorizationModel(context.Background(), storeID, model)
	require.NoError(b, err)

	numberObjectsAccesible := 0
	for i := 0; i < 100; i++ {
		var tuples []*openfgav1.TupleKey

		for j := 0; j < ds.MaxTuplesPerWrite(); j++ {
			obj := fmt.Sprintf("document:%s", strconv.Itoa(numberObjectsAccesible))

			tuples = append(tuples, tuple.NewTupleKey(obj, "viewer", "user:maria"))

			numberObjectsAccesible += 1
		}

		err := ds.Write(context.Background(), storeID, nil, tuples)
		require.NoError(b, err)
	}

	return model, modelID, numberObjectsAccesible
}

func BenchmarkListObjects(b *testing.B, ds storage.OpenFGADatastore) {
	ctx := context.Background()
	store := ulid.Make().String()

	model, modelID, numberObjectsAccessible := setupListObjectsBenchmark(b, ds, store)
	ctx = typesystem.ContextWithTypesystem(ctx, typesystem.New(model))

	req := &openfgav1.ListObjectsRequest{
		StoreId:              store,
		AuthorizationModelId: modelID,
		Type:                 "document",
		Relation:             "viewer",
		User:                 "user:maria",
	}

	var r *commands.ListObjectsResponse

	var oneResultIterations, allResultsIterations int

	b.Run("oneResult", func(b *testing.B) {
		listObjectsQuery := commands.NewListObjectsQuery(ds,
			commands.WithListObjectsMaxResults(1),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			r, _ := listObjectsQuery.Execute(ctx, req)
			require.Len(b, r.Objects, 1)
		}

		listObjectsResponse = r
		oneResultIterations = b.N
	})
	b.Run("allResults", func(b *testing.B) {
		listObjectsQuery := commands.NewListObjectsQuery(ds,
			commands.WithListObjectsMaxResults(0),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			r, _ := listObjectsQuery.Execute(ctx, req)
			require.Len(b, r.Objects, numberObjectsAccessible)
		}

		listObjectsResponse = r
		allResultsIterations = b.N
	})

	require.Greater(b, oneResultIterations, allResultsIterations)
}
