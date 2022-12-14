package test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/server/commands"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	defaultListObjectsDeadline   = 5 * time.Second
	defaultListObjectsMaxResults = 5
)

type listObjectsTestCase struct {
	name           string
	request        *openfgapb.ListObjectsRequest
	expectedError  error
	expectedResult []string //all the results. the server may return less
}

var tKAllAdminsRepo6 = tuple.NewTupleKey("repo:6", "admin", "*")
var tkAnnaRepo1 = tuple.NewTupleKey("repo:1", "admin", "anna")
var tkAnnaRepo2 = tuple.NewTupleKey("repo:2", "admin", "anna")
var tkAnnaRepo3 = tuple.NewTupleKey("repo:3", "admin", "anna")
var tkAnnaRepo4 = tuple.NewTupleKey("repo:4", "admin", "anna")
var tkBobRepo2 = tuple.NewTupleKey("repo:2", "admin", "bob")

func newListObjectsRequest(store, objectType, relation, user, modelID string, contextualTuples *openfgapb.ContextualTupleKeys) *openfgapb.ListObjectsRequest {
	return &openfgapb.ListObjectsRequest{
		StoreId:              store,
		Type:                 objectType,
		Relation:             relation,
		User:                 user,
		AuthorizationModelId: modelID,
		ContextualTuples:     contextualTuples,
	}
}

func ListObjectsTest(t *testing.T, ds storage.OpenFGADatastore) {

	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()

	t.Run("Github without TypeInfo", func(t *testing.T) {
		store := ulid.Make().String()

		data, err := os.ReadFile(gitHubTestDataFile)
		require.NoError(t, err)

		var gitHubTypeDefinitions openfgapb.WriteAuthorizationModelRequest
		err = protojson.Unmarshal(data, &gitHubTypeDefinitions)
		require.NoError(t, err)

		model := &openfgapb.AuthorizationModel{
			Id:              ulid.Make().String(),
			SchemaVersion:   typesystem.SchemaVersion1_0,
			TypeDefinitions: gitHubTypeDefinitions.GetTypeDefinitions(),
		}
		err = ds.WriteAuthorizationModel(ctx, store, model)
		require.NoError(t, err)

		writes := []*openfgapb.TupleKey{tKAllAdminsRepo6, tkAnnaRepo1, tkAnnaRepo2, tkAnnaRepo3, tkAnnaRepo4, tkBobRepo2}
		err = ds.Write(ctx, store, nil, writes)
		require.NoError(t, err)

		testCases := []listObjectsTestCase{
			{
				name:           "does not return duplicates",
				request:        newListObjectsRequest(store, "repo", "admin", "anna", model.Id, nil),
				expectedResult: []string{"repo:1", "repo:2", "repo:3", "repo:4", "repo:6"},
				expectedError:  nil,
			},

			{
				name: "respects max results",
				request: newListObjectsRequest(store, "repo", "admin", "anna", model.Id, &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{{
						User:     "anna",
						Relation: "admin",
						Object:   "repo:7",
					}}}),
				expectedResult: []string{"repo:1", "repo:2", "repo:3", "repo:4", "repo:6", "repo:7"},
				expectedError:  nil,
			},
			{
				name:           "performs correct checks",
				request:        newListObjectsRequest(store, "repo", "admin", "bob", model.Id, nil),
				expectedResult: []string{"repo:2", "repo:6"},
				expectedError:  nil,
			},
			{
				name: "includes contextual tuples in the checks",
				request: newListObjectsRequest(store, "repo", "admin", "bob", model.Id, &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{{
						User:     "bob",
						Relation: "admin",
						Object:   "repo:5",
					}, {
						User:     "bob",
						Relation: "admin",
						Object:   "repo:7",
					}}}),
				expectedResult: []string{"repo:2", "repo:5", "repo:6", "repo:7"},
				expectedError:  nil,
			},
			{
				name: "ignores irrelevant contextual tuples in the checks",
				request: newListObjectsRequest(store, "repo", "admin", "bob", model.Id, &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{{
						User:     "bob",
						Relation: "member",
						Object:   "team:abc",
					}}}),
				expectedResult: []string{"repo:2", "repo:6"},
				expectedError:  nil,
			},
			{
				name: "ignores irrelevant contextual tuples in the checks because they are not of the same type",
				request: newListObjectsRequest(store, "repo", "owner", "bob", model.Id, &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{{
						User:     "bob",
						Relation: "owner",
						Object:   "org:abc",
					}}}),
				expectedResult: []string{},
				expectedError:  nil,
			},
			{
				name: "returns_error_if_duplicate_contextual_tuples",
				request: newListObjectsRequest(store, "repo", "owner", "bob", model.Id, &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{{
						User:     "bob",
						Relation: "admin",
						Object:   "repo:5",
					}, {
						User:     "bob",
						Relation: "admin",
						Object:   "repo:5",
					}}}),
				expectedResult: nil,
				expectedError:  serverErrors.DuplicateContextualTuple(tuple.NewTupleKey("repo:5", "admin", "bob")),
			},
			{
				name:           "returns error if unknown type",
				request:        newListObjectsRequest(store, "unknown", "admin", "anna", model.Id, nil),
				expectedResult: nil,
				expectedError:  serverErrors.TypeNotFound("unknown"),
			},
			{
				name:           "returns error if unknown relation",
				request:        newListObjectsRequest(store, "repo", "unknown", "anna", model.Id, nil),
				expectedResult: nil,
				expectedError:  serverErrors.RelationNotFound("unknown", "repo", nil),
			},
		}

		listObjectsQuery := &commands.ListObjectsQuery{
			Datastore:             ds,
			Logger:                logger.NewNoopLogger(),
			Tracer:                tracer,
			Meter:                 telemetry.NewNoopMeter(),
			ListObjectsDeadline:   defaultListObjectsDeadline,
			ListObjectsMaxResults: defaultListObjectsMaxResults,
			ResolveNodeLimit:      defaultResolveNodeLimit,
		}

		runListObjectsTests(t, ctx, testCases, listObjectsQuery)
	})

	t.Run("Github with TypeInfo", func(t *testing.T) {
		store := ulid.Make().String()

		data, err := os.ReadFile("testdata/github/typedefs.json")
		require.NoError(t, err)

		var gitHubTypeDefinitions openfgapb.WriteAuthorizationModelRequest
		err = protojson.Unmarshal(data, &gitHubTypeDefinitions)
		require.NoError(t, err)

		model := &openfgapb.AuthorizationModel{
			Id:              ulid.Make().String(),
			SchemaVersion:   typesystem.SchemaVersion1_1,
			TypeDefinitions: gitHubTypeDefinitions.GetTypeDefinitions(),
		}

		err = ds.WriteAuthorizationModel(ctx, store, model)
		require.NoError(t, err)

		data, err = os.ReadFile("testdata/github/tuples.json")
		require.NoError(t, err)

		var writes []*openfgapb.TupleKey
		err = json.Unmarshal(data, &writes)
		require.NoError(t, err)

		err = ds.Write(ctx, store, nil, writes)
		require.NoError(t, err)

		testCases := []listObjectsTestCase{
			{
				name:           "does not return duplicates",
				request:        newListObjectsRequest(store, "repo", "admin", "user:anna", model.Id, nil),
				expectedResult: []string{"repo:1", "repo:2", "repo:3", "repo:4", "repo:6"},
				expectedError:  nil,
			},

			{
				name: "respects max results",
				request: newListObjectsRequest(store, "repo", "admin", "user:anna", model.Id, &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{{
						User:     "user:anna",
						Relation: "admin",
						Object:   "repo:7",
					}}}),
				expectedResult: []string{"repo:1", "repo:2", "repo:3", "repo:4", "repo:6", "repo:7"},
				expectedError:  nil,
			},
			{
				name:           "performs correct checks",
				request:        newListObjectsRequest(store, "repo", "admin", "user:bob", model.Id, nil),
				expectedResult: []string{"repo:2", "repo:6"},
				expectedError:  nil,
			},
			{
				name: "includes contextual tuples in the checks",
				request: newListObjectsRequest(store, "repo", "admin", "user:bob", model.Id, &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{{
						User:     "user:bob",
						Relation: "admin",
						Object:   "repo:5",
					}, {
						User:     "user:bob",
						Relation: "admin",
						Object:   "repo:7",
					}}}),
				expectedResult: []string{"repo:2", "repo:5", "repo:6", "repo:7"},
				expectedError:  nil,
			},
			{
				name: "ignores irrelevant contextual tuples in the checks",
				request: newListObjectsRequest(store, "repo", "admin", "user:bob", model.Id, &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{{
						User:     "user:bob",
						Relation: "member",
						Object:   "team:abc",
					}}}),
				expectedResult: []string{"repo:2", "repo:6"},
				expectedError:  nil,
			},
			{
				name: "ignores irrelevant contextual tuples in the checks because they are not of the same type",
				request: newListObjectsRequest(store, "repo", "owner", "user:bob", model.Id, &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{{
						User:     "user:bob",
						Relation: "owner",
						Object:   "organization:abc",
					}}}),
				expectedResult: []string{},
				expectedError:  nil,
			},
			{
				name: "returns_error_if_contextual_tuples_do_not_follow_type_restrictions",
				request: newListObjectsRequest(store, "repo", "owner", "user:bob", model.Id, &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{{
						User:     "user:*",
						Relation: "member",
						Object:   "team:fga",
					}}}),
				expectedResult: nil,

				expectedError: serverErrors.InvalidTuple("the typed wildcard 'user:*' is not an allowed type restriction for 'team#member'",
					tuple.NewTupleKey("team:fga", "member", "user:*"),
				),
			},
			{
				name: "returns_error_if_duplicate_contextual_tuples",
				request: newListObjectsRequest(store, "repo", "admin", "user:bob", model.Id, &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{{
						User:     "user:bob",
						Relation: "admin",
						Object:   "repo:5",
					}, {
						User:     "user:bob",
						Relation: "admin",
						Object:   "repo:5",
					}}}),
				expectedResult: nil,
				expectedError:  serverErrors.DuplicateContextualTuple(tuple.NewTupleKey("repo:5", "admin", "user:bob")),
			},
			{
				name:           "returns error if unknown type",
				request:        newListObjectsRequest(store, "unknown", "admin", "user:anna", model.Id, nil),
				expectedResult: nil,
				expectedError:  serverErrors.TypeNotFound("unknown"),
			},
			{
				name:           "returns error if unknown relation",
				request:        newListObjectsRequest(store, "repo", "unknown", "user:anna", model.Id, nil),
				expectedResult: nil,
				expectedError:  serverErrors.RelationNotFound("unknown", "repo", nil),
			},
		}

		connectedObjectsCmd := commands.ConnectedObjectsCommand{
			Datastore:        ds,
			Typesystem:       typesystem.New(model),
			ResolveNodeLimit: defaultResolveNodeLimit,
			Limit:            defaultListObjectsMaxResults,
		}

		listObjectsQuery := &commands.ListObjectsQuery{
			Datastore:             ds,
			Logger:                logger.NewNoopLogger(),
			Tracer:                tracer,
			Meter:                 telemetry.NewNoopMeter(),
			ListObjectsDeadline:   defaultListObjectsDeadline,
			ListObjectsMaxResults: defaultListObjectsMaxResults,
			ResolveNodeLimit:      defaultResolveNodeLimit,
			ConnectedObjects:      connectedObjectsCmd.StreamedConnectedObjects,
		}

		runListObjectsTests(t, ctx, testCases, listObjectsQuery)
	})
}

type mockStreamServer struct {
	grpc.ServerStream
	channel chan string
}

func NewMockStreamServer(size int) *mockStreamServer {
	return &mockStreamServer{
		channel: make(chan string, size),
	}
}

func (x *mockStreamServer) Send(m *openfgapb.StreamedListObjectsResponse) error {
	x.channel <- m.Object
	return nil
}

func runListObjectsTests(t *testing.T, ctx context.Context, testCases []listObjectsTestCase, listObjectsQuery *commands.ListObjectsQuery) {

	for _, test := range testCases {
		t.Run(test.name+"/streaming", func(t *testing.T) {
			server := NewMockStreamServer(len(test.expectedResult))

			done := make(chan struct{})
			var streamedObjectIds []string
			go func() {
				for x := range server.channel {
					streamedObjectIds = append(streamedObjectIds, x)
				}

				done <- struct{}{}
			}()

			err := listObjectsQuery.ExecuteStreamed(ctx, &openfgapb.StreamedListObjectsRequest{
				StoreId:              test.request.StoreId,
				AuthorizationModelId: test.request.AuthorizationModelId,
				Type:                 test.request.Type,
				Relation:             test.request.Relation,
				User:                 test.request.User,
				ContextualTuples:     test.request.ContextualTuples,
			}, server)

			close(server.channel)
			<-done

			require.ErrorIs(t, err, test.expectedError)
			require.LessOrEqual(t, len(streamedObjectIds), defaultListObjectsMaxResults)
			require.Subset(t, test.expectedResult, streamedObjectIds)
		})

		t.Run(test.name, func(t *testing.T) {
			res, err := listObjectsQuery.Execute(ctx, test.request)

			if res == nil && err == nil {
				t.Error("Expected an error or a response, got neither")
			}

			require.ErrorIs(t, err, test.expectedError)

			if res != nil {
				require.LessOrEqual(t, len(res.Objects), defaultListObjectsMaxResults)
				require.Subset(t, test.expectedResult, res.Objects)
			}
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
		Tracer:           telemetry.NewNoopTracer(),
		Meter:            telemetry.NewNoopMeter(),
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
		Tracer:           telemetry.NewNoopTracer(),
		Meter:            telemetry.NewNoopMeter(),
		ResolveNodeLimit: defaultResolveNodeLimit,
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
