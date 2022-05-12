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
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestReadAuthorizationModelQueryErrors(t *testing.T) {
	type readAuthorizationModelQueryTest struct {
		_name         string
		request       *openfgav1pb.ReadAuthorizationModelRequest
		expectedError error
	}

	var tests = []readAuthorizationModelQueryTest{
		{
			_name: "ReturnsAuthorizationModelNotFoundIfAuthorizationModelNotInDatabase",
			request: &openfgav1pb.ReadAuthorizationModelRequest{
				StoreId: testutils.CreateRandomString(10),
				Id:      "123",
			},
			expectedError: serverErrors.AuthorizationModelNotFound("123"),
		},
	}

	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()

	backends, err := testutils.BuildAllBackends(ctx, tracer, logger)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range tests {
		query := NewReadAuthorizationModelQuery(backends.AuthorizationModelBackend, logger)
		if _, err := query.Execute(ctx, test.request); !errors.Is(test.expectedError, err) {
			t.Errorf("[%s] Expected error '%s', actual '%s'", test._name, test.expectedError, err)
			continue
		}
	}
}

func TestReadAuthorizationModelByIdAndOneTypeDefinitionReturnsAuthorizationModel(t *testing.T) {
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()

	backend, err := testutils.BuildAllBackends(ctx, tracer, logger)
	if err != nil {
		t.Fatal(err)
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

	store := testutils.CreateRandomString(10)
	modelID, err := id.NewString()
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.AuthorizationModelBackend.WriteAuthorizationModel(ctx, store, modelID, state); err != nil {
		t.Fatalf("WriteAuthorizationModel err = %v, want nil", err)
	}
	query := NewReadAuthorizationModelQuery(backend.AuthorizationModelBackend, logger)
	actualResponse, actualError := query.Execute(ctx, &openfgav1pb.ReadAuthorizationModelRequest{
		StoreId: store,
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

func TestReadAuthorizationModelByIdAndTypeDefinitionsReturnsError(t *testing.T) {
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()

	backend, err := testutils.BuildAllBackends(ctx, tracer, logger)
	if err != nil {
		t.Fatal(err)
	}

	emptyState := &openfgav1pb.TypeDefinitions{
		TypeDefinitions: []*openfgav1pb.TypeDefinition{},
	}

	store := testutils.CreateRandomString(10)
	modelID, err := id.NewString()
	if err != nil {
		t.Fatal(err)
	}

	if err := backend.AuthorizationModelBackend.WriteAuthorizationModel(ctx, store, modelID, emptyState); err != nil {
		t.Fatalf("WriteAuthorizationModel err = %v, want nil", err)
	}

	query := NewReadAuthorizationModelQuery(backend.AuthorizationModelBackend, logger)
	_, err = query.Execute(ctx, &openfgav1pb.ReadAuthorizationModelRequest{
		StoreId: store,
		Id:      modelID,
	})
	if err.Error() != serverErrors.AuthorizationModelNotFound(modelID).Error() {
		t.Fatalf("got '%v', wanted '%v'", err, serverErrors.AuthorizationModelNotFound(modelID))
	}
}
