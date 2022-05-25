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
	"github.com/openfga/openfga/server/gateway"
	"github.com/openfga/openfga/server/test"
	"github.com/openfga/openfga/storage"
	"github.com/openfga/openfga/storage/memory"
	mockstorage "github.com/openfga/openfga/storage/mocks"
	"github.com/openfga/openfga/storage/postgres"
	teststorage "github.com/openfga/openfga/storage/test"
	"github.com/stretchr/testify/require"
)

func TestOpenFGAServer(t *testing.T) {

	t.Run("TestPostgresDatastore", func(t *testing.T) {
		testEngine := storagefixtures.RunOpenFGADatastoreTestEngine(t, "postgres")

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
		testEngine := storagefixtures.RunOpenFGADatastoreTestEngine(t, "memory")

		test.TestAll(t, teststorage.DatastoreTesterFunc(func() (storage.OpenFGADatastore, error) {
			ds := testEngine.NewDatastore(t, func(engine, uri string) storage.OpenFGADatastore {
				return memory.New(telemetry.NewNoopTracer(), 10, 24)
			})

			return ds, nil
		}))
	})
}

func BenchmarkOpenFGAServer(b *testing.B) {

	b.Run("TestPostgresDatastore", func(b *testing.B) {
		testEngine := storagefixtures.RunOpenFGADatastoreTestEngine(b, "postgres")

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
		testEngine := storagefixtures.RunOpenFGADatastoreTestEngine(b, "memory")

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
	transport := gateway.NewNoopTransport()

	t.Run("no latest authorization model id found", func(t *testing.T) {

		store := testutils.CreateRandomString(10)

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
		mockDatastore.EXPECT().FindLatestAuthorizationModelID(gomock.Any(), store).Return("", storage.ErrNotFound)

		s := Server{
			authorizationModelBackend: mockDatastore,
			tracer:                    tracer,
			transport:                 transport,
			logger:                    logger,
		}

		expectedError := serverErrors.LatestAuthorizationModelNotFound(store)

		if _, err := s.resolveAuthorizationModelID(ctx, store, ""); !errors.Is(err, expectedError) {
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
		mockDatastore.EXPECT().FindLatestAuthorizationModelID(gomock.Any(), store).Return(modelID, nil)

		s := Server{
			authorizationModelBackend: mockDatastore,
			tracer:                    tracer,
			transport:                 transport,
			logger:                    logger,
		}

		got, err := s.resolveAuthorizationModelID(ctx, store, "")
		if err != nil {
			t.Fatal(err)
		}
		if got != modelID {
			t.Errorf("wanted '%v', but got %v", modelID, got)
		}
	})

	t.Run("non-valid modelID returns error", func(t *testing.T) {
		store := testutils.CreateRandomString(10)
		modelID := "foo"
		want := serverErrors.AuthorizationModelNotFound(modelID)

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

		s := Server{
			authorizationModelBackend: mockDatastore,
			tracer:                    tracer,
			transport:                 transport,
			logger:                    logger,
		}

		if _, err := s.resolveAuthorizationModelID(ctx, store, modelID); err.Error() != want.Error() {
			t.Fatalf("got '%v', want '%v'", err, want)
		}
	})
}
