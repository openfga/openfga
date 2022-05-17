package server

import (
	"context"
	"errors"
	"testing"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/testutils"
	serverErrors "github.com/openfga/openfga/server/errors"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestResolveAuthorizationModel(t *testing.T) {
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()
	backends, err := testutils.BuildAllBackends(ctx, tracer, logger)
	if err != nil {
		t.Fatal(err)
	}

	s := Server{
		authorizationModelBackend: backends.AuthorizationModelBackend,
		tracer:                    tracer,
		logger:                    logger,
		config:                    &Config{},
	}

	t.Run("no latest authorization model id found", func(t *testing.T) {
		store := testutils.CreateRandomString(10)
		expectedError := serverErrors.LatestAuthorizationModelNotFound(store)

		if _, err = s.resolveAuthorizationModelID(ctx, store, ""); !errors.Is(err, expectedError) {
			t.Errorf("Expected '%v' but got %v", expectedError, err)
		}
	})

	t.Run("authorization model id not found", func(t *testing.T) {
		store := testutils.CreateRandomString(10)
		wrongAzmID := "wrong-azmID"
		expectedError := serverErrors.AuthorizationModelNotFound(wrongAzmID)

		if _, err := s.WriteAuthorizationModel(ctx, &openfgav1pb.WriteAuthorizationModelRequest{
			StoreId: store,
		}); err != nil {
			t.Fatal(err)
		}

		if _, err = s.resolveAuthorizationModelID(ctx, store, wrongAzmID); !errors.Is(err, expectedError) {
			t.Errorf("Expected '%v' but got %v", expectedError, err)
		}
	})

	t.Run("authorization model id found", func(t *testing.T) {
		store := testutils.CreateRandomString(10)
		typeDefinitions := &openfgav1pb.TypeDefinitions{TypeDefinitions: []*openfgav1pb.TypeDefinition{{Type: "type"}}}

		resp, err := s.WriteAuthorizationModel(ctx, &openfgav1pb.WriteAuthorizationModelRequest{
			StoreId:         store,
			TypeDefinitions: typeDefinitions,
		})
		if err != nil {
			t.Fatal(err)
		}

		got, err := s.resolveAuthorizationModelID(ctx, store, resp.GetAuthorizationModelId())
		if err != nil {
			t.Fatal(err)
		}
		if got != resp.GetAuthorizationModelId() {
			t.Errorf("wanted '%v', but got %v", resp.GetAuthorizationModelId(), got)

		}
	})
}
