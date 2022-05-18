package test

import (
	"context"
	"testing"

	"github.com/go-errors/errors"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/testutils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/server/queries"
	teststorage "github.com/openfga/openfga/storage/test"
	"github.com/stretchr/testify/require"
	"go.buf.build/openfga/go/openfga/api/openfga"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestReadTuplesQuery(t *testing.T, dbTester teststorage.DatastoreTester) {
	require := require.New(t)
	ctx := context.Background()
	logger := logger.NewNoopLogger()

	datastore, err := dbTester.New()
	require.NoError(err)

	encoder := encoder.NewNoopEncoder()

	store := testutils.CreateRandomString(10)
	modelID, err := id.NewString()
	if err != nil {
		t.Fatal(err)
	}

	backendState := &openfgav1pb.TypeDefinitions{
		TypeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "ns1",
			},
		},
	}

	err = datastore.WriteAuthorizationModel(ctx, store, modelID, backendState)
	if err != nil {
		t.Fatalf("First WriteAuthorizationModel err = %v, want nil", err)
	}

	cmd := queries.NewReadTuplesQuery(datastore, encoder, logger)

	writes := []*openfga.TupleKey{
		{
			Object:   "repo:auth0/foo",
			Relation: "admin",
			User:     "github|jon.allie@auth0.com",
		},
		{
			Object:   "repo:auth0/bar",
			Relation: "admin",
			User:     "github|jon.allie@auth0.com",
		},
		{
			Object:   "repo:auth0/baz",
			Relation: "admin",
			User:     "github|jon.allie@auth0.com",
		},
	}
	if err := datastore.Write(ctx, store, []*openfga.TupleKey{}, writes); err != nil {
		return
	}
	firstRequest := &openfgav1pb.ReadTuplesRequest{
		StoreId:           store,
		PageSize:          wrapperspb.Int32(1),
		ContinuationToken: "",
	}
	firstResponse, err := cmd.Execute(ctx, firstRequest)
	if err != nil {
		t.Errorf("Query.Execute(), err = %v, want nil", err)
	}
	if len(firstResponse.Tuples) != 1 {
		t.Errorf("Expected 1 tuple got %d", len(firstResponse.Tuples))
	}
	if diff := cmp.Diff(firstResponse.Tuples[0].Key, writes[0], cmpopts.IgnoreUnexported(openfga.TupleKey{})); diff != "" {
		t.Errorf("Tuple mismatch (-got +want):\n%s", diff)
	}
	if firstResponse.ContinuationToken == "" {
		t.Error("Expected continuation token")
	}

	secondRequest := &openfgav1pb.ReadTuplesRequest{StoreId: store, ContinuationToken: firstResponse.ContinuationToken}
	secondResponse, err := cmd.Execute(ctx, secondRequest)
	if err != nil {
		t.Fatalf("Query.Execute(), err = %v, want nil", err)
	}
	if len(secondResponse.Tuples) != 2 {
		t.Fatal("Expected 2 tuples")
	}
	if diff := cmp.Diff(secondResponse.Tuples[0].Key, writes[1], cmpopts.IgnoreUnexported(openfga.TupleKey{})); diff != "" {
		t.Errorf("Tuple mismatch (-got +want):\n%s", diff)
	}
	if diff := cmp.Diff(secondResponse.Tuples[1].Key, writes[2], cmpopts.IgnoreUnexported(openfga.TupleKey{})); diff != "" {
		t.Errorf("Tuple mismatch (-got +want):\n%s", diff)
	}
	// no token <=> no more results
	if secondResponse.ContinuationToken != "" {
		t.Fatal("Expected empty continuation token")
	}
}

func TestReadTuplesQueryInvalidContinuationToken(t *testing.T, dbTester teststorage.DatastoreTester) {
	require := require.New(t)
	ctx := context.Background()
	logger := logger.NewNoopLogger()

	datastore, err := dbTester.New()
	require.NoError(err)

	encoder, err := encoder.NewTokenEncrypter("key")
	if err != nil {
		t.Fatalf("Error creating encoder: %v", err)
	}

	store := testutils.CreateRandomString(10)
	modelID, err := id.NewString()
	if err != nil {
		t.Fatal(err)
	}
	state := &openfgav1pb.TypeDefinitions{
		TypeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repo",
			},
		},
	}

	if err := datastore.WriteAuthorizationModel(ctx, store, modelID, state); err != nil {
		t.Fatalf("First WriteAuthorizationModel err = %v, want nil", err)
	}

	q := queries.NewReadTuplesQuery(datastore, encoder, logger)
	if _, err := q.Execute(ctx, &openfgav1pb.ReadTuplesRequest{
		StoreId:           store,
		ContinuationToken: "foo",
	}); !errors.Is(err, serverErrors.InvalidContinuationToken) {
		t.Errorf("expected '%v', got '%v'", serverErrors.InvalidContinuationToken, err)
	}
}
