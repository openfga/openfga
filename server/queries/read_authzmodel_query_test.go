package queries

import (
	"context"
	"errors"
	"testing"

	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/testutils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type readAuthorizationModelQueryTest struct {
	_name         string
	request       *openfgav1pb.ReadAuthorizationModelRequest
	expectedError error
}

var readAuthorizationModelQueriesTest = []readAuthorizationModelQueryTest{
	{
		_name: "ReturnsAuthorizationModelNotFoundIfAuthorizationModelNotInDatabase",
		request: &openfgav1pb.ReadAuthorizationModelRequest{
			StoreId: "auth0",
			Id:      "123",
		},
		expectedError: serverErrors.AuthorizationModelNotFound("123"),
	},
}

func TestReadAuthorizationModelQueryErrors(t *testing.T) {
	tracer := telemetry.NewNoopTracer()
	for _, test := range readAuthorizationModelQueriesTest {
		backend, err := testutils.BuildAllBackends(tracer)
		if err != nil {
			t.Fatalf("Error building backend: %s", err)
		}
		ctx := context.Background()
		query := NewReadAuthorizationModelQuery(backend.AuthorizationModelBackend, logger.NewNoopLogger())
		if _, err := query.Execute(ctx, test.request); !errors.Is(test.expectedError, err) {
			t.Errorf("[%s] Expected error '%s', actual '%s'", test._name, test.expectedError, err)
			continue
		}
	}
}

func TestReadAuthorizationModelByIdAndOneTypeDefinition_ReturnsAuthorizationModel(t *testing.T) {
	storeId := "auth0"
	tracer := telemetry.NewNoopTracer()
	ctx := context.Background()
	backend, err := testutils.BuildAllBackends(tracer)
	if err != nil {
		t.Fatalf("Error building backend: %s", err)
	}
	state := &openfgav1pb.TypeDefinitions{
		TypeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgav1pb.Userset{
					"viewer": {
						Userset: &openfgav1pb.Userset_This{},
					},
				},
			},
		},
	}
	modelID, err := id.NewString()
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.AuthorizationModelBackend.WriteAuthorizationModel(ctx, storeId, modelID, state); err != nil {
		t.Fatalf("WriteAuthorizationModel err = %v, want nil", err)
	}
	query := NewReadAuthorizationModelQuery(backend.AuthorizationModelBackend, logger.NewNoopLogger())
	actualResponse, actualError := query.Execute(ctx, &openfgav1pb.ReadAuthorizationModelRequest{
		StoreId: storeId,
		Id:      modelID,
	})

	if actualResponse == nil {
		t.Fatal("ReadAuthorizationModelRequest response was nil")
	}
	if actualResponse.AuthorizationModel.Id != modelID {
		t.Fatalf("ReadAuthorizationModelRequest id = %v, want %v", actualResponse.AuthorizationModel.Id, modelID)
	}

	if actualError != nil {
		t.Fatalf("ReadAuthorizationModelRequest err = %v, want nil", err)
	}
}

func TestReadAuthorizationModelByIdAndTypeDefinitions_ReturnsError(t *testing.T) {
	storeId := "auth0"
	tracer := telemetry.NewNoopTracer()
	ctx := context.Background()
	backend, err := testutils.BuildAllBackends(tracer)
	if err != nil {
		t.Fatalf("Error building backend: %s", err)
	}
	emptyState := &openfgav1pb.TypeDefinitions{
		TypeDefinitions: []*openfgav1pb.TypeDefinition{},
	}
	modelID, err := id.NewString()
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.AuthorizationModelBackend.WriteAuthorizationModel(ctx, storeId, modelID, emptyState); err != nil {
		t.Fatalf("WriteAuthorizationModel err = %v, want nil", err)
	}
	query := NewReadAuthorizationModelQuery(backend.AuthorizationModelBackend, logger.NewNoopLogger())
	_, actualError := query.Execute(ctx, &openfgav1pb.ReadAuthorizationModelRequest{
		StoreId: storeId,
		Id:      modelID,
	})
	if actualError == nil {
		t.Fatalf("WriteAuthorizationModel err = nil, wanted %v", storage.NotFound)
	}
}
