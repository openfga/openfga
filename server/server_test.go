package server

import (
	"context"
	"errors"
	"os"
	"path"
	"runtime"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/typesystem"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/server/gateway"
	"github.com/openfga/openfga/server/test"
	"github.com/openfga/openfga/storage"
	"github.com/openfga/openfga/storage/memory"
	mockstorage "github.com/openfga/openfga/storage/mocks"
	"github.com/openfga/openfga/storage/mysql"
	"github.com/openfga/openfga/storage/postgres"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/encoding/protojson"
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

		store := ulid.Make().String()

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
		store := ulid.Make().String()
		modelID := ulid.Make().String()

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
		store := ulid.Make().String()
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

func TestListObjects_Unoptimized_UnhappyPaths(t *testing.T) {
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()
	transport := gateway.NewNoopTransport()
	meter := telemetry.NewNoopMeter()
	store := ulid.Make().String()
	modelID := ulid.Make().String()

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	data, err := os.ReadFile("testdata/github.json")
	require.NoError(t, err)

	var gitHubTypeDefinitions openfgapb.WriteAuthorizationModelRequest
	err = protojson.Unmarshal(data, &gitHubTypeDefinitions)
	require.NoError(t, err)

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), store, modelID).AnyTimes().Return(&openfgapb.AuthorizationModel{
		SchemaVersion:   typesystem.SchemaVersion1_0,
		TypeDefinitions: gitHubTypeDefinitions.GetTypeDefinitions(),
	}, nil)
	mockDatastore.EXPECT().ListObjectsByType(gomock.Any(), store, "repo").AnyTimes().Return(nil, errors.New("error reading from storage"))

	s := Server{
		datastore: mockDatastore,
		tracer:    tracer,
		transport: transport,
		logger:    logger,
		meter:     meter,
		config: &Config{
			ResolveNodeLimit:      25,
			ListObjectsDeadline:   5 * time.Second,
			ListObjectsMaxResults: 1000,
		},
	}

	t.Run("error listing objects from storage in non-streaming version", func(t *testing.T) {
		res, err := s.ListObjects(ctx, &openfgapb.ListObjectsRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			Type:                 "repo",
			Relation:             "owner",
			User:                 "bob",
		})

		require.Nil(t, res)
		require.ErrorIs(t, err, serverErrors.NewInternalError("", errors.New("error reading from storage")))
	})

	t.Run("error listing objects from storage in streaming version", func(t *testing.T) {
		err = s.StreamedListObjects(&openfgapb.StreamedListObjectsRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			Type:                 "repo",
			Relation:             "owner",
			User:                 "bob",
		}, nil)

		require.ErrorIs(t, err, serverErrors.NewInternalError("", errors.New("error reading from storage")))
	})
}

func TestListObjects_Optimized_UnhappyPaths(t *testing.T) {
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()
	transport := gateway.NewNoopTransport()
	meter := telemetry.NewNoopMeter()
	store := ulid.Make().String()
	modelID := ulid.Make().String()

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), store, modelID).AnyTimes().Return(&openfgapb.AuthorizationModel{
		SchemaVersion: typesystem.SchemaVersion1_1,
		TypeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "user",
			},
			{
				Type: "document",
				Relations: map[string]*openfgapb.Userset{
					"viewer": typesystem.This(),
				},
				Metadata: &openfgapb.Metadata{
					Relations: map[string]*openfgapb.RelationMetadata{
						"viewer": {
							DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
								{Type: "user"},
							},
						},
					},
				},
			},
		},
	}, nil)
	mockDatastore.EXPECT().ListObjectsByType(gomock.Any(), store, "document").AnyTimes().Return(nil, errors.New("error reading from storage"))

	s := Server{
		datastore: mockDatastore,
		tracer:    tracer,
		transport: transport,
		logger:    logger,
		meter:     meter,
		config: &Config{
			ResolveNodeLimit:      25,
			ListObjectsDeadline:   5 * time.Second,
			ListObjectsMaxResults: 1000,
		},
	}

	t.Run("error listing objects from storage in non-streaming version", func(t *testing.T) {
		res, err := s.ListObjects(ctx, &openfgapb.ListObjectsRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			Type:                 "document",
			Relation:             "viewer",
			User:                 "user:bob",
		})

		require.Nil(t, res)
		require.ErrorIs(t, err, serverErrors.NewInternalError("", errors.New("error reading from storage")))
	})

	t.Run("error listing objects from storage in streaming version", func(t *testing.T) {
		err := s.StreamedListObjects(&openfgapb.StreamedListObjectsRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			Type:                 "document",
			Relation:             "viewer",
			User:                 "user:bob",
		}, nil)

		require.ErrorIs(t, err, serverErrors.NewInternalError("", errors.New("error reading from storage")))
	})
}
