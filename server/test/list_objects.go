package test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
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

func TestListObjects(t *testing.T, datastore storage.OpenFGADatastore) {
	store := testutils.CreateRandomString(10)
	tracer := telemetry.NewNoopTracer()
	ctx, backend, modelID, err := setupTestListObjects(store, datastore)
	require.NoError(t, err)

	t.Run("list objects", func(t *testing.T) {
		testCases := []listObjectsTestCase{
			{
				name:           "does not return duplicates",
				request:        newListObjectsRequest(store, "repo", "admin", "anna", modelID, nil),
				expectedResult: []string{"1", "2", "3", "4", "6"},
				expectedError:  nil,
			},

			{
				name: "respects max results",
				request: newListObjectsRequest(store, "repo", "admin", "anna", modelID, &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{{
						User:     "anna",
						Relation: "admin",
						Object:   "repo:7",
					}}}),
				expectedResult: []string{"1", "2", "3", "4", "6", "7"},
				expectedError:  nil,
			},
			{
				name:           "performs correct checks",
				request:        newListObjectsRequest(store, "repo", "admin", "bob", modelID, nil),
				expectedResult: []string{"2", "6"},
				expectedError:  nil,
			},
			{
				name: "includes contextual tuples in the checks",
				request: newListObjectsRequest(store, "repo", "admin", "bob", modelID, &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{{
						User:     "bob",
						Relation: "admin",
						Object:   "repo:5",
					}, {
						User:     "bob",
						Relation: "admin",
						Object:   "repo:7",
					}}}),
				expectedResult: []string{"2", "5", "6", "7"},
				expectedError:  nil,
			},
			{
				name: "ignores irrelevant contextual tuples in the checks",
				request: newListObjectsRequest(store, "repo", "admin", "bob", modelID, &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{{
						User:     "bob",
						Relation: "member",
						Object:   "team:abc",
					}}}),
				expectedResult: []string{"2", "6"},
				expectedError:  nil,
			},
			{
				name:           "returns error if unknown type",
				request:        newListObjectsRequest(store, "unknown", "admin", "anna", modelID, nil),
				expectedResult: nil,
				expectedError:  serverErrors.TypeNotFound("unknown"),
			},
			{
				name:           "returns error if unknown relation",
				request:        newListObjectsRequest(store, "repo", "unknown", "anna", modelID, nil),
				expectedResult: nil,
				expectedError:  serverErrors.RelationNotFound("unknown", "repo", nil),
			},
		}

		listObjectsQuery := &commands.ListObjectsQuery{
			Datastore:             backend,
			Logger:                logger.NewNoopLogger(),
			Tracer:                tracer,
			Meter:                 telemetry.NewNoopMeter(),
			ListObjectsDeadline:   defaultListObjectsDeadline,
			ListObjectsMaxResults: defaultListObjectsMaxResults,
			ResolveNodeLimit:      defaultResolveNodeLimit,
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
	x.channel <- m.ObjectId
	return nil
}

func runListObjectsTests(t *testing.T, ctx context.Context, testCases []listObjectsTestCase, listObjectsQuery *commands.ListObjectsQuery) {
	sortFn := func(a, b string) bool { return a < b }

	for _, test := range testCases {
		t.Run(test.name+"/streaming", func(t *testing.T) {
			server := NewMockStreamServer(len(test.expectedResult))
			err := listObjectsQuery.ExecuteStreamed(ctx, &openfgapb.StreamedListObjectsRequest{
				StoreId:              test.request.StoreId,
				AuthorizationModelId: test.request.AuthorizationModelId,
				Type:                 test.request.Type,
				Relation:             test.request.Relation,
				User:                 test.request.User,
				ContextualTuples:     test.request.ContextualTuples,
			}, server)
			close(server.channel)
			require.ErrorIs(t, err, test.expectedError)
			streamedObjectIds := make([]string, 0, len(test.expectedResult))
			for x := range server.channel {
				streamedObjectIds = append(streamedObjectIds, x)
			}
			if len(streamedObjectIds) > defaultListObjectsMaxResults {
				t.Errorf("expected a maximum of %d results but got %d:", defaultListObjectsMaxResults, len(streamedObjectIds))
			}
			if !subset(streamedObjectIds, test.expectedResult) {
				if diff := cmp.Diff(streamedObjectIds, test.expectedResult, cmpopts.EquateEmpty(), cmpopts.SortSlices(sortFn)); diff != "" {
					t.Errorf("object ID mismatch (-got +want):\n%s", diff)
				}
			}
		})
		t.Run(test.name, func(t *testing.T) {
			res, err := listObjectsQuery.Execute(ctx, test.request)

			if res == nil && err == nil {
				t.Error("Expected an error or a response, got neither")
			}

			require.ErrorIs(t, err, test.expectedError)

			if res != nil {
				if len(res.ObjectIds) > defaultListObjectsMaxResults {
					t.Errorf("expected a maximum of %d results but got %d:", defaultListObjectsMaxResults, len(res.ObjectIds))
				}
				if !subset(res.ObjectIds, test.expectedResult) {
					if diff := cmp.Diff(res.ObjectIds, test.expectedResult, cmpopts.EquateEmpty(), cmpopts.SortSlices(sortFn)); diff != "" {
						t.Errorf("object ID mismatch (-got +want):\n%s", diff)
					}
				}

			}
		})
	}
}

func setupTestListObjects(store string, datastore storage.OpenFGADatastore) (context.Context, storage.OpenFGADatastore, string, error) {
	ctx := context.Background()
	data, err := os.ReadFile(gitHubTestDataFile)
	if err != nil {
		return nil, nil, "", err
	}
	var gitHubTypeDefinitions openfgapb.TypeDefinitions
	if err := protojson.Unmarshal(data, &gitHubTypeDefinitions); err != nil {
		return nil, nil, "", err
	}
	modelID, err := id.NewString()
	if err != nil {
		return nil, nil, "", err
	}
	err = datastore.WriteAuthorizationModel(ctx, store, modelID, gitHubTypeDefinitions.GetTypeDefinitions())
	if err != nil {
		return nil, nil, "", err
	}

	writes := []*openfgapb.TupleKey{tKAllAdminsRepo6, tkAnnaRepo1, tkAnnaRepo2, tkAnnaRepo3, tkAnnaRepo4, tkBobRepo2}
	err = datastore.Write(ctx, store, []*openfgapb.TupleKey{}, writes)
	if err != nil {
		return nil, nil, "", err
	}

	return ctx, datastore, modelID, nil
}

// subset returns true if the first slice is a subset of second
func subset(first, second []string) bool {
	set := make(map[string]bool)
	for _, value := range second {
		set[value] = true
	}

	for _, value := range first {
		if _, found := set[value]; !found {
			return false
		}
	}

	return true
}
