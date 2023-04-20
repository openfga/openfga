package test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc"
)

type mockStreamServer struct {
	grpc.ServerStream
	channel chan string
}

func (x *mockStreamServer) Send(m *openfgapb.StreamedListObjectsResponse) error {
	x.channel <- m.Object
	return nil
}

// mockSlowDataStorage is a proxy to the actual ds except the Reads are slow time by the sleepTime
// This allows simulating list objection condition that times out
type mockSlowDataStorage struct {
	sleepTime time.Duration
	ds        storage.OpenFGADatastore
}

func NewMockSlowDataStorage(ds storage.OpenFGADatastore, sleepTime time.Duration) storage.OpenFGADatastore {
	return &mockSlowDataStorage{
		sleepTime: sleepTime,
		ds:        ds,
	}
}

func (m *mockSlowDataStorage) Close() {}

func (m *mockSlowDataStorage) ListObjectsByType(ctx context.Context, store string, objectType string) (storage.ObjectIterator, error) {
	return m.ds.ListObjectsByType(ctx, store, objectType)
}

func (m *mockSlowDataStorage) Read(ctx context.Context, store string, key *openfgapb.TupleKey) (storage.TupleIterator, error) {
	time.Sleep(m.sleepTime)
	return m.ds.Read(ctx, store, key)
}

func (m *mockSlowDataStorage) ReadPage(ctx context.Context, store string, key *openfgapb.TupleKey, paginationOptions storage.PaginationOptions) ([]*openfgapb.Tuple, []byte, error) {
	time.Sleep(m.sleepTime)
	return m.ds.ReadPage(ctx, store, key, paginationOptions)
}

func (m *mockSlowDataStorage) ReadChanges(ctx context.Context, store, objectType string, paginationOptions storage.PaginationOptions, horizonOffset time.Duration) ([]*openfgapb.TupleChange, []byte, error) {
	return m.ds.ReadChanges(ctx, store, objectType, paginationOptions, horizonOffset)
}

func (m *mockSlowDataStorage) Write(ctx context.Context, store string, deletes storage.Deletes, writes storage.Writes) error {
	return m.ds.Write(ctx, store, deletes, writes)
}

func (m *mockSlowDataStorage) ReadUserTuple(ctx context.Context, store string, key *openfgapb.TupleKey) (*openfgapb.Tuple, error) {
	time.Sleep(m.sleepTime)
	return m.ds.ReadUserTuple(ctx, store, key)
}

func (m *mockSlowDataStorage) ReadUsersetTuples(ctx context.Context, store string, filter storage.ReadUsersetTuplesFilter) (storage.TupleIterator, error) {
	time.Sleep(m.sleepTime)
	return m.ds.ReadUsersetTuples(ctx, store, filter)
}

func (m *mockSlowDataStorage) ReadStartingWithUser(
	ctx context.Context,
	store string,
	filter storage.ReadStartingWithUserFilter,
) (storage.TupleIterator, error) {
	time.Sleep(m.sleepTime)
	return m.ds.ReadStartingWithUser(ctx, store, filter)
}

func (m *mockSlowDataStorage) ReadAuthorizationModel(ctx context.Context, store string, id string) (*openfgapb.AuthorizationModel, error) {
	return m.ds.ReadAuthorizationModel(ctx, store, id)
}

func (m *mockSlowDataStorage) ReadAuthorizationModels(ctx context.Context, store string, options storage.PaginationOptions) ([]*openfgapb.AuthorizationModel, []byte, error) {
	return m.ds.ReadAuthorizationModels(ctx, store, options)
}

func (m *mockSlowDataStorage) FindLatestAuthorizationModelID(ctx context.Context, store string) (string, error) {
	return m.ds.FindLatestAuthorizationModelID(ctx, store)
}

func (m *mockSlowDataStorage) ReadTypeDefinition(ctx context.Context, store, id, objectType string) (*openfgapb.TypeDefinition, error) {
	return m.ds.ReadTypeDefinition(ctx, store, id, objectType)
}

func (m *mockSlowDataStorage) WriteAuthorizationModel(ctx context.Context, store string, model *openfgapb.AuthorizationModel) error {
	return m.ds.WriteAuthorizationModel(ctx, store, model)
}

func (m *mockSlowDataStorage) CreateStore(ctx context.Context, newStore *openfgapb.Store) (*openfgapb.Store, error) {
	return m.ds.CreateStore(ctx, newStore)
}

func (m *mockSlowDataStorage) DeleteStore(ctx context.Context, id string) error {
	return m.ds.DeleteStore(ctx, id)
}

func (m *mockSlowDataStorage) WriteAssertions(ctx context.Context, store, modelID string, assertions []*openfgapb.Assertion) error {
	return m.ds.WriteAssertions(ctx, store, modelID, assertions)
}

func (m *mockSlowDataStorage) ReadAssertions(ctx context.Context, store, modelID string) ([]*openfgapb.Assertion, error) {
	return m.ds.ReadAssertions(ctx, store, modelID)
}

func (m *mockSlowDataStorage) MaxTuplesPerWrite() int {
	return m.ds.MaxTuplesPerWrite()
}

func (m *mockSlowDataStorage) MaxTypesPerAuthorizationModel() int {
	return m.ds.MaxTypesPerAuthorizationModel()
}

func (m *mockSlowDataStorage) GetStore(ctx context.Context, storeID string) (*openfgapb.Store, error) {
	return m.ds.GetStore(ctx, storeID)
}

func (m *mockSlowDataStorage) ListStores(ctx context.Context, paginationOptions storage.PaginationOptions) ([]*openfgapb.Store, []byte, error) {
	return m.ds.ListStores(ctx, paginationOptions)
}

func (m *mockSlowDataStorage) IsReady(ctx context.Context) (bool, error) {
	return m.ds.IsReady(ctx)
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
	listObjectsDeadline    time.Duration // 1 minute if not set
	dsSlowTime             time.Duration // if set, purposely use a slow storage to slow down read and simulate timeout
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
			name:   "respects_when_schema_1_1_and_ttu_in_model_and_reverse_expansion_implementation",
			schema: typesystem.SchemaVersion1_1,
			model: `
			type user
			type folder
			  relations
			    define viewer: [user] as self
			type document
			  relations
			    define parent: [folder] as self
			    define viewer as viewer from parent
			`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("folder:x", "viewer", "user:alice"),
				tuple.NewTupleKey("document:1", "parent", "folder:x"),
				tuple.NewTupleKey("document:2", "parent", "folder:x"),
				tuple.NewTupleKey("document:3", "parent", "folder:x"),
			},
			user:                   "user:alice",
			objectType:             "document",
			relation:               "viewer",
			maxResults:             2,
			minimumResultsExpected: 2,
			allResults:             []string{"document:1", "document:2", "document:3"},
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
		{
			// should not return any useful data as it times out
			// however, there should not be any error
			name:   "connected objects timeout",
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
			minimumResultsExpected: 0,
			allResults:             []string{},
			listObjectsDeadline:    1 * time.Second,
			dsSlowTime:             2 * time.Second, // We are mocking the ds to slow down the read call and simulate timeout
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

			listObjectsDeadline := time.Minute
			if test.listObjectsDeadline > 0 {
				listObjectsDeadline = test.listObjectsDeadline
			}

			datastore := ds
			if test.dsSlowTime > 0 {
				datastore = NewMockSlowDataStorage(ds, test.dsSlowTime)
			}

			listObjectsQuery := &commands.ListObjectsQuery{
				Datastore:             datastore,
				Logger:                logger.NewNoopLogger(),
				ListObjectsDeadline:   listObjectsDeadline,
				ListObjectsMaxResults: test.maxResults,
				ResolveNodeLimit:      defaultResolveNodeLimit,
			}
			typesys := typesystem.New(model)
			ctx = typesystem.ContextWithTypesystem(ctx, typesys)

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

	listObjectsQuery := commands.ListObjectsQuery{
		Datastore:        ds,
		Logger:           logger.NewNoopLogger(),
		ResolveNodeLimit: defaultResolveNodeLimit,
	}

	var r *openfgapb.ListObjectsResponse

	ctx = typesystem.ContextWithTypesystem(ctx, typesystem.New(model))

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
	}

	var r *openfgapb.ListObjectsResponse

	ctx = typesystem.ContextWithTypesystem(ctx, typesystem.New(model))

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
