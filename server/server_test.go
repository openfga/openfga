package server

import (
	"context"
	"errors"
	"os"
	"path"
	"runtime"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/server/gateway"
	"github.com/openfga/openfga/server/test"
	"github.com/openfga/openfga/storage"
	"github.com/openfga/openfga/storage/memory"
	mockstorage "github.com/openfga/openfga/storage/mocks"
	"github.com/openfga/openfga/storage/mysql"
	"github.com/openfga/openfga/storage/postgres"
	"github.com/stretchr/testify/require"
)

func init() {
	_, filename, _, _ := runtime.Caller(0)
	dir := path.Join(path.Dir(filename), "..")
	err := os.Chdir(dir)
	if err != nil {
		panic(err)
	}
}

func TestOpenFGAServer(t *testing.T) {

	t.Run("TestPostgresDatastore", func(t *testing.T) {
		testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

		uri := testDatastore.GetConnectionURI()
		ds, err := postgres.NewPostgresDatastore(uri)
		require.NoError(t, err)

		test.RunAllTests(t, ds)
	})

	t.Run("TestMemoryDatastore", func(t *testing.T) {
		ds := memory.New(telemetry.NewNoopTracer(), 10, 24)
		test.RunAllTests(t, ds)
	})

	t.Run("TestMySQLDatastore", func(t *testing.T) {
		testDatastore := storagefixtures.RunDatastoreTestContainer(t, "mysql")

		uri := testDatastore.GetConnectionURI()
		ds, err := mysql.NewMySQLDatastore(uri)
		require.NoError(t, err)

		test.RunAllTests(t, ds)
	})
}

func BenchmarkOpenFGAServer(b *testing.B) {

	b.Run("BenchmarkPostgresDatastore", func(b *testing.B) {
		testDatastore := storagefixtures.RunDatastoreTestContainer(b, "postgres")

		uri := testDatastore.GetConnectionURI()
		ds, err := postgres.NewPostgresDatastore(uri)
		require.NoError(b, err)

		test.RunAllBenchmarks(b, ds)
	})

	b.Run("BenchmarkMemoryDatastore", func(b *testing.B) {
		ds := memory.New(telemetry.NewNoopTracer(), 10, 24)
		test.RunAllBenchmarks(b, ds)
	})
}

func TestResolveAuthorizationModel(t *testing.T) {
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()
	transport := gateway.NewNoopTransport()

	t.Run("no latest authorization model id found", func(t *testing.T) {

		store := id.Must(id.New()).String()

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
		mockDatastore.EXPECT().FindLatestAuthorizationModelID(gomock.Any(), store).Return("", storage.ErrNotFound)

		s := Server{
			datastore: mockDatastore,
			tracer:    tracer,
			transport: transport,
			logger:    logger,
		}

		expectedError := serverErrors.LatestAuthorizationModelNotFound(store)

		if _, err := s.resolveAuthorizationModelID(ctx, store, ""); !errors.Is(err, expectedError) {
			t.Errorf("Expected '%v' but got %v", expectedError, err)
		}
	})

	t.Run("read existing authorization model", func(t *testing.T) {
		store := id.Must(id.New()).String()
		modelID := id.Must(id.New()).String()

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
		mockDatastore.EXPECT().FindLatestAuthorizationModelID(gomock.Any(), store).Return(modelID, nil)

		s := Server{
			datastore: mockDatastore,
			tracer:    tracer,
			transport: transport,
			logger:    logger,
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
		store := id.Must(id.New()).String()
		modelID := "foo"
		want := serverErrors.AuthorizationModelNotFound(modelID)

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

		s := Server{
			datastore: mockDatastore,
			tracer:    tracer,
			transport: transport,
			logger:    logger,
		}

		if _, err := s.resolveAuthorizationModelID(ctx, store, modelID); err.Error() != want.Error() {
			t.Fatalf("got '%v', want '%v'", err, want)
		}
	})
}
