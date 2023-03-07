package test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc"
)

const (
	checkConcurrencyLimit = 100
)

type mockStreamServer struct {
	grpc.ServerStream
	channel chan string
}

func (x *mockStreamServer) Send(m *openfgapb.StreamedListObjectsResponse) error {
	x.channel <- m.Object
	return nil
}

type listObjectsTestCase struct {
	name                   string
	schema                 string
	tuples                 []*openfgapb.TupleKey
	model                  string
	objectType             string
	user                   string
	relation               string
	contextualTuples       *openfgapb.ContextualTupleKeys
	allResults             []string //all the results. the server may return less
	maxResults             uint32
	minimumResultsExpected uint32
}

func TestListObjectsRespectsMaxResults(t *testing.T, ds storage.OpenFGADatastore) {
	testCases := []listObjectsTestCase{
		{
			name:   "respects_when_schema_1_1_and_reverse_expansion_implementation",
			schema: typesystem.SchemaVersion1_1,
			model: `
			type user
			type repo
			  relations
				define admin: [user] as self
			`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:1", "admin", "user:alice"),
				tuple.NewTupleKey("repo:2", "admin", "user:alice"),
			},
			user:       "user:alice",
			objectType: "repo",
			relation:   "admin",
			contextualTuples: &openfgapb.ContextualTupleKeys{
				TupleKeys: []*openfgapb.TupleKey{tuple.NewTupleKey("repo:3", "admin", "user:alice")},
			},
			maxResults:             2,
			minimumResultsExpected: 2,
			allResults:             []string{"repo:1", "repo:2", "repo:3"},
		},
		{
			name:   "respects_when_schema_1_1_and_concurrent_checks_implementation",
			schema: typesystem.SchemaVersion1_1,
			model: `
			type user
			type org
			  relations
				define blocked: [user] as self
				define admin: [user] as self but not blocked
			`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("org:1", "admin", "user:charlie"),
				tuple.NewTupleKey("org:2", "admin", "user:charlie"),
			},
			user:       "user:charlie",
			objectType: "org",
			relation:   "admin",
			contextualTuples: &openfgapb.ContextualTupleKeys{
				TupleKeys: []*openfgapb.TupleKey{tuple.NewTupleKey("org:3", "admin", "user:charlie")},
			},
			maxResults:             2,
			minimumResultsExpected: 2,
			allResults:             []string{"org:1", "org:2", "org:3"},
		},
		{
			name:   "respects_when_schema_1_0",
			schema: typesystem.SchemaVersion1_0,
			model: `
			type document
			  relations
			    define admin as self
			`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:1", "admin", "bob"),
				tuple.NewTupleKey("document:2", "admin", "bob"),
			},
			user:       "bob",
			objectType: "document",
			relation:   "admin",
			contextualTuples: &openfgapb.ContextualTupleKeys{
				TupleKeys: []*openfgapb.TupleKey{tuple.NewTupleKey("document:3", "admin", "bob")},
			},
			maxResults:             2,
			minimumResultsExpected: 2,
			allResults:             []string{"document:1", "document:2", "document:3"},
		},
		{
			name:   "respects_when_schema_1_0_and_maxresults_is_higher_than_actual_result_length",
			schema: typesystem.SchemaVersion1_0,
			model: `
			type team
			  relations
			    define admin as self
			`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("team:1", "admin", "bob"),
			},
			user:                   "bob",
			objectType:             "team",
			relation:               "admin",
			maxResults:             2,
			minimumResultsExpected: 1,
			allResults:             []string{"team:1"},
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			storeID := ulid.Make().String()

			// arrange: write model
			model := &openfgapb.AuthorizationModel{
				Id:              ulid.Make().String(),
				SchemaVersion:   test.schema,
				TypeDefinitions: parser.MustParse(test.model),
			}
			err := ds.WriteAuthorizationModel(ctx, storeID, model)
			require.NoError(t, err)

			// arrange: write tuples
			err = ds.Write(context.Background(), storeID, nil, test.tuples)
			require.NoError(t, err)

			// act: run ListObjects
			checker := graph.NewLocalChecker(storage.NewContextualTupleDatastore(ds), checkConcurrencyLimit)
			listObjectsQuery := &commands.ListObjectsQuery{
				Datastore:             ds,
				Logger:                logger.NewNoopLogger(),
				ListObjectsDeadline:   time.Minute,
				ListObjectsMaxResults: test.maxResults,
				ResolveNodeLimit:      defaultResolveNodeLimit,
				CheckResolver:         checker,
			}
			typesys := typesystem.New(model)
			ctx = typesystem.ContextWithTypesystem(ctx, typesys)
			ctx = storage.ContextWithContextualTuples(ctx, test.contextualTuples.GetTupleKeys())

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

				err := listObjectsQuery.ExecuteStreamed(ctx, &openfgapb.StreamedListObjectsRequest{
					StoreId:          storeID,
					Type:             test.objectType,
					Relation:         test.relation,
					User:             test.user,
					ContextualTuples: test.contextualTuples,
				}, server)
				close(server.channel)
				<-done

				require.NoError(t, err)
				require.LessOrEqual(t, len(streamedObjectIds), int(test.maxResults))
				require.GreaterOrEqual(t, len(streamedObjectIds), int(test.minimumResultsExpected))
				require.Subset(t, test.allResults, streamedObjectIds)
			})

			t.Run("regular_endpoint", func(t *testing.T) {
				res, err := listObjectsQuery.Execute(ctx, &openfgapb.ListObjectsRequest{
					StoreId:          storeID,
					Type:             test.objectType,
					Relation:         test.relation,
					User:             test.user,
					ContextualTuples: test.contextualTuples,
				})

				require.NotNil(t, res)
				require.NoError(t, err)
				require.LessOrEqual(t, len(res.Objects), int(test.maxResults))
				require.GreaterOrEqual(t, len(res.Objects), int(test.minimumResultsExpected))
				require.Subset(t, test.allResults, res.Objects)
			})
		})
	}
}

// Used to avoid compiler optimizations (see https://dave.cheney.net/2013/06/30/how-to-write-benchmarks-in-go)
var listObjectsResponse *openfgapb.ListObjectsResponse //nolint

func BenchmarkListObjectsWithReverseExpand(b *testing.B, ds storage.OpenFGADatastore) {

	ctx := context.Background()
	store := ulid.Make().String()

	model := &openfgapb.AuthorizationModel{
		Id:            ulid.Make().String(),
		SchemaVersion: typesystem.SchemaVersion1_1,
		TypeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "user",
			},
			{
				Type: "document",
				Relations: map[string]*openfgapb.Userset{
					"viewer": typesystem.This(),
				},
				Metadata: &openfgapb.Metadata{
					Relations: map[string]*openfgapb.RelationMetadata{
						"viewer": {
							DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
								typesystem.DirectRelationReference("user", ""),
							},
						},
					},
				},
			},
		},
	}
	err := ds.WriteAuthorizationModel(ctx, store, model)
	require.NoError(b, err)

	n := 0
	for i := 0; i < 100; i++ {
		var tuples []*openfgapb.TupleKey

		for j := 0; j < ds.MaxTuplesPerWrite(); j++ {
			obj := fmt.Sprintf("document:%s", strconv.Itoa(n))
			user := fmt.Sprintf("user:%s", strconv.Itoa(n))

			tuples = append(tuples, tuple.NewTupleKey(obj, "viewer", user))

			n += 1
		}

		err = ds.Write(ctx, store, nil, tuples)
		require.NoError(b, err)
	}

	connectedObjCmd := commands.ConnectedObjectsCommand{
		Datastore:        ds,
		Typesystem:       typesystem.New(model),
		ResolveNodeLimit: defaultResolveNodeLimit,
	}

	listObjectsQuery := commands.ListObjectsQuery{
		Datastore:        ds,
		Logger:           logger.NewNoopLogger(),
		ResolveNodeLimit: defaultResolveNodeLimit,
		ConnectedObjects: connectedObjCmd.StreamedConnectedObjects,
	}

	var r *openfgapb.ListObjectsResponse

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ = listObjectsQuery.Execute(ctx, &openfgapb.ListObjectsRequest{
			StoreId:              store,
			AuthorizationModelId: model.Id,
			Type:                 "document",
			Relation:             "viewer",
			User:                 "user:999",
		})
	}

	listObjectsResponse = r
}

func BenchmarkListObjectsWithConcurrentChecks(b *testing.B, ds storage.OpenFGADatastore) {
	ctx := context.Background()
	store := ulid.Make().String()

	model := &openfgapb.AuthorizationModel{
		Id:            ulid.Make().String(),
		SchemaVersion: typesystem.SchemaVersion1_0,
		TypeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "document",
				Relations: map[string]*openfgapb.Userset{
					"viewer": typesystem.This(),
				},
			},
		},
	}
	err := ds.WriteAuthorizationModel(ctx, store, model)
	require.NoError(b, err)

	n := 0
	for i := 0; i < 100; i++ {
		var tuples []*openfgapb.TupleKey

		for j := 0; j < ds.MaxTuplesPerWrite(); j++ {
			obj := fmt.Sprintf("document:%s", strconv.Itoa(n))
			user := fmt.Sprintf("user:%s", strconv.Itoa(n))

			tuples = append(tuples, tuple.NewTupleKey(obj, "viewer", user))

			n += 1
		}

		err = ds.Write(ctx, store, nil, tuples)
		require.NoError(b, err)
	}

	listObjectsQuery := commands.ListObjectsQuery{
		Datastore:        ds,
		Logger:           logger.NewNoopLogger(),
		ResolveNodeLimit: defaultResolveNodeLimit,
		CheckResolver:    graph.NewLocalChecker(storage.NewContextualTupleDatastore(ds), checkConcurrencyLimit),
	}

	var r *openfgapb.ListObjectsResponse

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ = listObjectsQuery.Execute(ctx, &openfgapb.ListObjectsRequest{
			StoreId:              store,
			AuthorizationModelId: model.Id,
			Type:                 "document",
			Relation:             "viewer",
			User:                 "user:999",
		})
	}

	listObjectsResponse = r
}
