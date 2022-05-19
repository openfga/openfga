package server

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/testutils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/server/test"
	"github.com/openfga/openfga/storage"
	"github.com/openfga/openfga/storage/memory"
	mockstorage "github.com/openfga/openfga/storage/mocks"
	"github.com/openfga/openfga/storage/postgres"
	teststorage "github.com/openfga/openfga/storage/test"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestOpenFGAServer(t *testing.T) {

	t.Run("TestPostgresDatstore", func(t *testing.T) {
		testEngine := storagefixtures.RunDatastoreEngine(t, "postgres")

		test.TestAll(t, teststorage.DatastoreTesterFunc(func() (storage.OpenFGADatastore, error) {
			ds := testEngine.NewDatastore(t, func(engine, uri string) storage.OpenFGADatastore {
				ds, err := postgres.NewPostgresDatastore(uri)
				require.NoError(t, err)

				return ds
			})

			return ds, nil
		}))
	})

	t.Run("TestMemoryDatastore", func(t *testing.T) {
		testEngine := storagefixtures.RunDatastoreEngine(t, "memory")

		test.TestAll(t, teststorage.DatastoreTesterFunc(func() (storage.OpenFGADatastore, error) {
			ds := testEngine.NewDatastore(t, func(engine, uri string) storage.OpenFGADatastore {
				return memory.New(telemetry.NewNoopTracer(), 10, 24)
			})

			return ds, nil
		}))
	})
}

func BenchmarkOpenFGAServer(b *testing.B) {

	b.Run("TestPostgresDatstore", func(b *testing.B) {
		testEngine := storagefixtures.RunDatastoreEngine(b, "postgres")

		test.BenchmarkAll(b, teststorage.DatastoreTesterFunc(func() (storage.OpenFGADatastore, error) {
			ds := testEngine.NewDatastore(b, func(engine, uri string) storage.OpenFGADatastore {
				ds, err := postgres.NewPostgresDatastore(uri)
				require.NoError(b, err)

				return ds
			})

			return ds, nil
		}))
	})

	b.Run("TestMemoryDatastore", func(b *testing.B) {
		testEngine := storagefixtures.RunDatastoreEngine(b, "memory")

		test.BenchmarkAll(b, teststorage.DatastoreTesterFunc(func() (storage.OpenFGADatastore, error) {
			ds := testEngine.NewDatastore(b, func(engine, uri string) storage.OpenFGADatastore {
				return memory.New(telemetry.NewNoopTracer(), 10, 24)
			})

			return ds, nil
		}))
	})
}

func TestResolveAuthorizationModel(t *testing.T) {
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()

	t.Run("no latest authorization model id found", func(t *testing.T) {

		store := testutils.CreateRandomString(10)

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
		mockDatastore.EXPECT().FindLatestAuthorizationModelID(gomock.Any(), store).Return("", storage.ErrNotFound)

		s := Server{
			authorizationModelBackend: mockDatastore,
			tracer:                    tracer,
			logger:                    logger,
		}

		expectedError := serverErrors.LatestAuthorizationModelNotFound(store)

		if _, err := s.resolveAuthorizationModelID(ctx, store, ""); !errors.Is(err, expectedError) {
			t.Errorf("Expected '%v' but got %v", expectedError, err)
		}
	})

	t.Run("authorization model id not found", func(t *testing.T) {
		store := testutils.CreateRandomString(10)
		missingModelID := "missing-modelID"

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
		mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), store, missingModelID).Return(nil, storage.ErrNotFound)

		s := Server{
			authorizationModelBackend: mockDatastore,
			tracer:                    tracer,
			logger:                    logger,
		}

		expectedError := serverErrors.AuthorizationModelNotFound(missingModelID)

		if _, err := s.resolveAuthorizationModelID(ctx, store, missingModelID); !errors.Is(err, expectedError) {
			t.Errorf("Expected '%v' but got %v", expectedError, err)
		}
	})

	t.Run("read existing authorization model", func(t *testing.T) {
		store := testutils.CreateRandomString(10)

		modelID, err := id.NewString()
		if err != nil {
			t.Fatal(err)
		}

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
		mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), store, modelID).
			Return(&openfgapb.AuthorizationModel{Id: modelID}, nil)

		s := Server{
			authorizationModelBackend: mockDatastore,
			tracer:                    tracer,
			logger:                    logger,
		}

		got, err := s.resolveAuthorizationModelID(ctx, store, modelID)
		if err != nil {
			t.Fatal(err)
		}
		if got != modelID {
			t.Errorf("wanted '%v', but got %v", modelID, got)
		}
	})
}
