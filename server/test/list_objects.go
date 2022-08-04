package test

import (
	"context"
	"io/ioutil"
	"strings"
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
	teststorage "github.com/openfga/openfga/storage/test"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	defaultListObjectsDeadline   = 5 * time.Second
	defaultListObjectsMaxResults = 3
)

type listObjectsTestCase struct {
	_name          string
	request        *openfgapb.ListObjectsRequest
	expectedError  error
	expectedResult []string //all the results. the server may return less
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
var tkBobRepo4 = &openfgapb.TupleKey{
	Object:   "repo:2",
	Relation: "admin",
	User:     "bob",
}

func newListObjectsRequest(store, objectType, relation, user, modelID string) *openfgapb.ListObjectsRequest {
	return &openfgapb.ListObjectsRequest{
		StoreId:              store,
		Type:                 objectType,
		Relation:             relation,
		User:                 user,
		AuthorizationModelId: modelID,
	}
}

func TestListObjects(t *testing.T, dbTester teststorage.DatastoreTester[storage.OpenFGADatastore]) {
	store := testutils.CreateRandomString(10)
	tracer := telemetry.NewNoopTracer()
	ctx, backend, modelID, err := setupTestListObjects(store, dbTester)
	require.NoError(t, err)

	t.Run("list objects", func(t *testing.T) {
		testCases := []listObjectsTestCase{
			{
				_name:          "does not return duplicates and respects maximum length allowed",
				request:        newListObjectsRequest(store, "repo", "admin", "anna", modelID),
				expectedResult: []string{"1", "2", "3", "4"},
				expectedError:  nil,
			},
			{
				_name:          "performs correct checks",
				request:        newListObjectsRequest(store, "repo", "admin", "bob", modelID),
				expectedResult: []string{"2"},
				expectedError:  nil,
			},
			{
				_name:          "returns error if unknown type",
				request:        newListObjectsRequest(store, "unknown", "admin", "anna", modelID),
				expectedResult: nil,
				expectedError:  serverErrors.TypeNotFound("unknown"),
			},
			{
				_name:          "returns error if unknown relation",
				request:        newListObjectsRequest(store, "repo", "unknown", "anna", modelID),
				expectedResult: nil,
				expectedError:  serverErrors.UnknownRelationWhenListingObjects("unknown", "repo"),
			},
		}

		listObjectsQuery := commands.NewListObjectsQuery(backend, tracer, logger.NewNoopLogger(), telemetry.NewNoopMeter(), defaultListObjectsDeadline, defaultListObjectsMaxResults, defaultResolveNodeLimit)
		runListObjectsTests(t, ctx, testCases, listObjectsQuery)
	})
}

func runListObjectsTests(t *testing.T, ctx context.Context, testCases []listObjectsTestCase, listObjectsQuery *commands.ListObjectsQuery) {
	var res *openfgapb.ListObjectsResponse
	var err error
	for _, test := range testCases {
		res, err = listObjectsQuery.Execute(ctx, test.request)

		if res == nil && err == nil {
			t.Fatalf("[%s] Expected an error or a response, got neither", test._name)
		}

		if test.expectedError == nil && err != nil {
			t.Fatalf("[%s] Expected no error but got '%s'", test._name, err)
		}

		if test.expectedError != nil && err == nil {
			t.Fatalf("[%s] Expected an error '%s' but got nothing", test._name, test.expectedError)
		}

		if test.expectedError != nil && err != nil && !strings.Contains(test.expectedError.Error(), err.Error()) {
			t.Fatalf("[%s] Expected error '%s', actual '%s'", test._name, test.expectedError, err)
		}

		if res != nil {
			if !subset(res.ObjectIds, test.expectedResult) {
				diff := cmp.Diff(res.ObjectIds, test.expectedResult, cmpopts.EquateEmpty())
				t.Fatalf("[%s] object ID mismatch (-got +want):\n%s", test._name, diff)
			}

			if len(res.ObjectIds) > defaultListObjectsMaxResults {
				t.Fatalf("[%s] expected a maximum of %d results but got %d:", test._name, defaultListObjectsMaxResults, len(res.ObjectIds))
			}
		}
	}
}

func setupTestListObjects(store string, dbTester teststorage.DatastoreTester[storage.OpenFGADatastore]) (context.Context, storage.OpenFGADatastore, string, error) {
	ctx := context.Background()
	datastore, err := dbTester.New()
	if err != nil {
		return nil, nil, "", err
	}

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

	writes := []*openfgapb.TupleKey{tkAnnaRepo1, tkAnnaRepo2, tkAnnaRepo3, tkAnnaRepo4, tkBobRepo4}
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
