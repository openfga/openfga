package queries

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/testutils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/go-errors/errors"
	"go.buf.build/openfga/go/openfga/api/openfga"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	testStore = "auth0"
)

func TestReadTuplesQuery(t *testing.T) {
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	backend, err := testutils.BuildAllBackends(tracer)
	if err != nil {
		t.Fatalf("Error building backend: %v", err)
	}

	encoder := encoder.NewNoopEncoder()

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

	if err := backend.AuthorizationModelBackend.WriteAuthorizationModel(ctx, testStore, modelID, backendState); err != nil {
		t.Fatalf("First WriteAuthorizationModel err = %v, want nil", err)
	}

	readQuery := NewReadTuplesQuery(backend.TupleBackend, encoder, logger.NewNoopLogger())

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
	if err := backend.TupleBackend.Write(ctx, testStore, []*openfga.TupleKey{}, writes); err != nil {
		return
	}
	firstRequest := &openfgav1pb.ReadTuplesRequest{
		StoreId: testStore,
		Params:  &openfgav1pb.ReadTuplesRequestParams{PageSize: wrapperspb.Int32(1), ContinuationToken: ""},
	}
	firstResponse, err := readQuery.Execute(ctx, firstRequest)
	if err != nil {
		t.Errorf("Query.Execute(), err = %v, want nil", err)
	}
	if len(firstResponse.Tuples) != 1 {
		t.Errorf("Expected 1 tuple got %d", len(firstResponse.Tuples))
	}
	if firstResponse.ContinuationToken == "" {
		t.Error("Expected continuation token")
	}

	secondRequest := &openfgav1pb.ReadTuplesRequest{StoreId: testStore, Params: &openfgav1pb.ReadTuplesRequestParams{ContinuationToken: firstResponse.ContinuationToken}}
	secondResponse, err := readQuery.Execute(ctx, secondRequest)
	if err != nil {
		t.Fatalf("Query.Execute(), err = %v, want nil", err)
	}
	if len(secondResponse.Tuples) != 2 {
		t.Fatal("Expected 2 tuples")
	}
	if secondResponse.ContinuationToken != "" {
		t.Fatal("Expected empty continuation token")
	}
}

func TestInvalidContinuationToken(t *testing.T) {
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	backend, err := testutils.BuildAllBackends(tracer)
	if err != nil {
		t.Fatalf("Error building backend: %v", err)
	}
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

	if err := backend.AuthorizationModelBackend.WriteAuthorizationModel(ctx, store, modelID, state); err != nil {
		t.Fatalf("First WriteAuthorizationModel err = %v, want nil", err)
	}

	q := NewReadTuplesQuery(backend.TupleBackend, encoder, logger.NewNoopLogger())
	if _, err := q.Execute(ctx, &openfgav1pb.ReadTuplesRequest{
		StoreId: store,
		Params:  &openfgav1pb.ReadTuplesRequestParams{ContinuationToken: "foo"},
	}); !errors.Is(err, serverErrors.InvalidContinuationToken) {
		t.Errorf("expected '%v', got '%v'", serverErrors.InvalidContinuationToken, err)
	}
}
