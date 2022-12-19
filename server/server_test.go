package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"runtime"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/internal/gateway"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/server/test"
	"github.com/openfga/openfga/storage"
	"github.com/openfga/openfga/storage/memory"
	mockstorage "github.com/openfga/openfga/storage/mocks"
	"github.com/openfga/openfga/storage/mysql"
	"github.com/openfga/openfga/storage/postgres"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc"
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
		defer ds.Close()

		test.RunAllTests(t, ds)
	})

	t.Run("TestMemoryDatastore", func(t *testing.T) {
		ds := memory.New(telemetry.NewNoopTracer(), 10, 24)
		defer ds.Close()
		test.RunAllTests(t, ds)
	})

	t.Run("TestMySQLDatastore", func(t *testing.T) {
		testDatastore := storagefixtures.RunDatastoreTestContainer(t, "mysql")

		uri := testDatastore.GetConnectionURI()
		ds, err := mysql.NewMySQLDatastore(uri)
		require.NoError(t, err)
		defer ds.Close()

		test.RunAllTests(t, ds)
	})
}

func BenchmarkOpenFGAServer(b *testing.B) {

	b.Run("BenchmarkPostgresDatastore", func(b *testing.B) {
		testDatastore := storagefixtures.RunDatastoreTestContainer(b, "postgres")

		uri := testDatastore.GetConnectionURI()
		ds, err := postgres.NewPostgresDatastore(uri)
		require.NoError(b, err)
		defer ds.Close()
		test.RunAllBenchmarks(b, ds)
	})

	b.Run("BenchmarkMemoryDatastore", func(b *testing.B) {
		ds := memory.New(telemetry.NewNoopTracer(), 10, 24)
		defer ds.Close()
		test.RunAllBenchmarks(b, ds)
	})

	b.Run("BenchmarkMySQLDatastore", func(b *testing.B) {
		testDatastore := storagefixtures.RunDatastoreTestContainer(b, "mysql")

		uri := testDatastore.GetConnectionURI()
		ds, err := mysql.NewMySQLDatastore(uri)
		require.NoError(b, err)
		defer ds.Close()
		test.RunAllBenchmarks(b, ds)
	})
}

func TestCheckDoesNotThrowBecauseDirectTupleWasFound(t *testing.T) {
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()
	meter := telemetry.NewNoopMeter()
	transport := gateway.NewNoopTransport()
	store := ulid.Make().String()
	modelID := ulid.Make().String()

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), store, modelID).AnyTimes().Return(&openfgapb.AuthorizationModel{
		SchemaVersion: typesystem.SchemaVersion1_0,
		TypeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"reader": typesystem.This(),
				},
			},
		},
	}, nil)

	tupleKey := &openfgapb.TupleKey{
		Object:   "repo:openfga",
		Relation: "reader",
		User:     "anne",
	}
	tuple := &openfgapb.Tuple{Key: tupleKey}
	mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), store, gomock.Any()).Return(tuple, nil)
	mockDatastore.EXPECT().ReadUsersetTuples(gomock.Any(), store, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ *openfgapb.TupleKey) (storage.TupleIterator, error) {
			time.Sleep(50 * time.Millisecond)
			return nil, errors.New("some error")
		})
	s := Server{
		datastore: mockDatastore,
		tracer:    tracer,
		meter:     meter,
		transport: transport,
		logger:    logger,
		config: &Config{
			ResolveNodeLimit: 25,
		},
	}

	checkResponse, err := s.Check(ctx, &openfgapb.CheckRequest{
		StoreId:              store,
		TupleKey:             tupleKey,
		AuthorizationModelId: modelID,
	})
	require.NoError(t, err)
	require.Equal(t, true, checkResponse.Allowed)
}

func TestShortestPathToSolutionWins(t *testing.T) {
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()
	meter := telemetry.NewNoopMeter()
	transport := gateway.NewNoopTransport()
	store := ulid.Make().String()
	modelID := ulid.Make().String()

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), store, modelID).AnyTimes().Return(&openfgapb.AuthorizationModel{
		SchemaVersion: typesystem.SchemaVersion1_0,
		TypeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"reader": typesystem.This(),
				},
			},
		},
	}, nil)

	tupleKey := &openfgapb.TupleKey{
		Object:   "repo:openfga",
		Relation: "reader",
		User:     "*",
	}
	tuple := &openfgapb.Tuple{Key: tupleKey}
	mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), store, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ *openfgapb.TupleKey) (storage.TupleIterator, error) {
			time.Sleep(500 * time.Millisecond)
			return nil, storage.ErrNotFound
		})
	mockDatastore.EXPECT().ReadUsersetTuples(gomock.Any(), store, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ *openfgapb.TupleKey) (storage.TupleIterator, error) {
			time.Sleep(100 * time.Millisecond)
			return storage.NewStaticTupleIterator([]*openfgapb.Tuple{tuple}), nil
		})
	s := Server{
		datastore: mockDatastore,
		tracer:    tracer,
		meter:     meter,
		transport: transport,
		logger:    logger,
		config: &Config{
			ResolveNodeLimit: 25,
		},
	}

	start := time.Now()
	checkResponse, err := s.Check(ctx, &openfgapb.CheckRequest{
		StoreId:              store,
		TupleKey:             tupleKey,
		AuthorizationModelId: modelID,
	})
	end := time.Since(start)
	require.Truef(t, end < 200*time.Millisecond, fmt.Sprintf("end was %s", end))
	require.NoError(t, err)
	require.Equal(t, true, checkResponse.Allowed)
}

func TestResolveAuthorizationModel(t *testing.T) {
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()
	transport := gateway.NewNoopTransport()

	t.Run("no_latest_authorization_model_id_found", func(t *testing.T) {

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

	t.Run("read_existing_authorization_model", func(t *testing.T) {
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

	t.Run("non-valid_modelID_returns_error", func(t *testing.T) {
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

type mockStreamServer struct {
	grpc.ServerStream
}

func NewMockStreamServer() *mockStreamServer {
	return &mockStreamServer{}
}

func (m *mockStreamServer) Context() context.Context {
	return context.Background()
}

func (m *mockStreamServer) Send(*openfgapb.StreamedListObjectsResponse) error {
	return nil
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

	t.Run("error_listing_objects_from_storage_in_non-streaming_version", func(t *testing.T) {
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

	t.Run("error_listing_objects_from_storage_in_streaming_version", func(t *testing.T) {
		err = s.StreamedListObjects(&openfgapb.StreamedListObjectsRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			Type:                 "repo",
			Relation:             "owner",
			User:                 "bob",
		}, NewMockStreamServer())

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
	mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), store, storage.ReadStartingWithUserFilter{
		ObjectType: "document",
		Relation:   "viewer",
		UserFilter: []*openfgapb.ObjectRelation{
			{Object: "user:bob"},
			{Object: tuple.Wildcard},
		}}).AnyTimes().Return(nil, errors.New("error reading from storage"))

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
			Experimentals:         []ExperimentalFeatureFlag{ListObjectsOptimized},
		},
	}

	t.Run("error_listing_objects_from_storage_in_non-streaming_version", func(t *testing.T) {
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

	t.Run("error_listing_objects_from_storage_in_streaming_version", func(t *testing.T) {
		err := s.StreamedListObjects(&openfgapb.StreamedListObjectsRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			Type:                 "document",
			Relation:             "viewer",
			User:                 "user:bob",
		}, NewMockStreamServer())

		require.ErrorIs(t, err, serverErrors.NewInternalError("", errors.New("error reading from storage")))
	})
}
