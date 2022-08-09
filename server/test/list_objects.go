package test

import (
	"context"
	"io/ioutil"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/server/commands"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	defaultListObjectsDeadline   = 5 * time.Second
	defaultListObjectsMaxResults = 5
)

type listObjectsTestCase struct {
	_name          string
	request        *openfgapb.ListObjectsRequest
	expectedError  error
	expectedResult []string //all the results. the server may return less
}

var tKAllAdminsRepo6 = &openfgapb.TupleKey{
	Object:   "repo:6",
	Relation: "admin",
	User:     "*",
}
var tkAnnaRepo1 = &openfgapb.TupleKey{
	Object:   "repo:1",
	Relation: "admin",
	User:     "anna",
}
var tkAnnaRepo2 = &openfgapb.TupleKey{
	Object:   "repo:2",
	Relation: "admin",
	User:     "anna",
}
var tkAnnaRepo3 = &openfgapb.TupleKey{
	Object:   "repo:3",
	Relation: "admin",
	User:     "anna",
}
var tkAnnaRepo4 = &openfgapb.TupleKey{
	Object:   "repo:4",
	Relation: "admin",
	User:     "anna",
}
var tkBobRepo2 = &openfgapb.TupleKey{
	Object:   "repo:2",
	Relation: "admin",
	User:     "bob",
}

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
				_name:          "does not return duplicates and respects maximum length allowed",
				request:        newListObjectsRequest(store, "repo", "admin", "anna", modelID, nil),
				expectedResult: []string{"1", "2", "3", "4", "6"},
				expectedError:  nil,
			},
			{
				_name:          "performs correct checks",
				request:        newListObjectsRequest(store, "repo", "admin", "bob", modelID, nil),
				expectedResult: []string{"2", "6"},
				expectedError:  nil,
			},
			{
				_name: "includes contextual tuples in the checks",
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
				_name: "ignores irrelevant contextual tuples in the checks",
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
				_name:          "returns error if unknown type",
				request:        newListObjectsRequest(store, "unknown", "admin", "anna", modelID, nil),
				expectedResult: nil,
				expectedError:  serverErrors.TypeNotFound("unknown"),
			},
			{
				_name:          "returns error if unknown relation",
				request:        newListObjectsRequest(store, "repo", "unknown", "anna", modelID, nil),
				expectedResult: nil,
				expectedError:  serverErrors.UnknownRelationWhenListingObjects("unknown", "repo"),
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

func runListObjectsTests(t *testing.T, ctx context.Context, testCases []listObjectsTestCase, listObjectsQuery *commands.ListObjectsQuery) {
	var res *openfgapb.ListObjectsResponse
	var err error
	for _, test := range testCases {
		t.Run(test._name, func(t *testing.T) {
			res, err = listObjectsQuery.Execute(ctx, test.request)

			if res == nil && err == nil {
				t.Error("Expected an error or a response, got neither")
			}

			require.ErrorIs(t, err, test.expectedError)

			if res != nil {
				if len(res.ObjectIds) > defaultListObjectsMaxResults {
					t.Errorf("expected a maximum of %d results but got %d:", defaultListObjectsMaxResults, len(res.ObjectIds))
				}
				less := func(a, b string) bool { return a < b }
				if diff := cmp.Diff(res.ObjectIds, test.expectedResult, cmpopts.EquateEmpty(), cmpopts.SortSlices(less)); diff != "" {
					t.Errorf("object ID mismatch (-got +want):\n%s", diff)
				}
			}
		})
	}
}

func setupTestListObjects(store string, datastore storage.OpenFGADatastore) (context.Context, storage.OpenFGADatastore, string, error) {
	ctx := context.Background()
	data, err := ioutil.ReadFile(gitHubTestDataFile)
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
	err = datastore.WriteAuthorizationModel(ctx, store, modelID, &gitHubTypeDefinitions)
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
