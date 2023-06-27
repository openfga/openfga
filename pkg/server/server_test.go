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

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/golang/mock/gomock"
	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/internal/gateway"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/server/test"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	mockstorage "github.com/openfga/openfga/pkg/storage/mocks"
	"github.com/openfga/openfga/pkg/storage/mysql"
	"github.com/openfga/openfga/pkg/storage/postgres"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func init() {
	_, filename, _, _ := runtime.Caller(0)
	dir := path.Join(path.Dir(filename), "..", "..")
	err := os.Chdir(dir)
	if err != nil {
		panic(err)
	}
}

func TestServerWithPostgresDatastore(t *testing.T) {
	ds := MustBootstrapDatastore(t, "postgres")
	defer ds.Close()

	test.RunAllTests(t, ds)
}

func TestServerWithPostgresDatastoreAndExplicitCredentials(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI(false)
	ds, err := postgres.New(
		uri,
		sqlcommon.NewConfig(
			sqlcommon.WithUsername(testDatastore.GetUsername()),
			sqlcommon.WithPassword(testDatastore.GetPassword()),
		),
	)
	require.NoError(t, err)
	defer ds.Close()

	test.RunAllTests(t, ds)
}

func TestServerWithMemoryDatastore(t *testing.T) {
	ds := MustBootstrapDatastore(t, "memory")
	defer ds.Close()

	test.RunAllTests(t, ds)
}

func TestServerWithMySQLDatastore(t *testing.T) {
	ds := MustBootstrapDatastore(t, "mysql")
	defer ds.Close()

	test.RunAllTests(t, ds)
}

func TestServerWithMySQLDatastoreAndExplicitCredentials(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "mysql")

	uri := testDatastore.GetConnectionURI(false)
	ds, err := mysql.New(
		uri,
		sqlcommon.NewConfig(
			sqlcommon.WithUsername(testDatastore.GetUsername()),
			sqlcommon.WithPassword(testDatastore.GetPassword()),
		),
	)
	require.NoError(t, err)
	defer ds.Close()

	test.RunAllTests(t, ds)
}

func BenchmarkOpenFGAServer(b *testing.B) {

	b.Run("BenchmarkPostgresDatastore", func(b *testing.B) {
		testDatastore := storagefixtures.RunDatastoreTestContainer(b, "postgres")

		uri := testDatastore.GetConnectionURI(true)
		ds, err := postgres.New(uri, sqlcommon.NewConfig())
		require.NoError(b, err)
		defer ds.Close()
		test.RunAllBenchmarks(b, ds)
	})

	b.Run("BenchmarkMemoryDatastore", func(b *testing.B) {
		ds := memory.New()
		defer ds.Close()
		test.RunAllBenchmarks(b, ds)
	})

	b.Run("BenchmarkMySQLDatastore", func(b *testing.B) {
		testDatastore := storagefixtures.RunDatastoreTestContainer(b, "mysql")

		uri := testDatastore.GetConnectionURI(true)
		ds, err := mysql.New(uri, sqlcommon.NewConfig())
		require.NoError(b, err)
		defer ds.Close()
		test.RunAllBenchmarks(b, ds)
	})
}

func TestCheckDoesNotThrowBecauseDirectTupleWasFound(t *testing.T) {
	ctx := context.Background()
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	typedefs := parser.MustParse(`
	type user

	type repo
	  relations
	    define reader: [user] as self
	`)

	tk := tuple.NewTupleKey("repo:openfga", "reader", "user:anne")
	tuple := &openfgapb.Tuple{Key: tk}

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	mockDatastore.EXPECT().
		ReadAuthorizationModel(gomock.Any(), storeID, modelID).
		AnyTimes().
		Return(&openfgapb.AuthorizationModel{
			SchemaVersion:   typesystem.SchemaVersion1_1,
			TypeDefinitions: typedefs,
		}, nil)

	// it could happen that one of the following two mocks won't be necessary because the goroutine will be short-circuited
	mockDatastore.EXPECT().
		ReadUserTuple(gomock.Any(), storeID, gomock.Any()).
		AnyTimes().
		Return(tuple, nil)

	mockDatastore.EXPECT().
		ReadUsersetTuples(gomock.Any(), storeID, gomock.Any()).
		AnyTimes().
		DoAndReturn(
			func(_ context.Context, _ string, _ storage.ReadUsersetTuplesFilter) (storage.TupleIterator, error) {
				time.Sleep(50 * time.Millisecond)
				return nil, errors.New("some error")
			})

	s := New(&Dependencies{
		Datastore: mockDatastore,
		Logger:    logger.NewNoopLogger(),
		Transport: gateway.NewNoopTransport(),
	}, &Config{
		ResolveNodeLimit: test.DefaultResolveNodeLimit,
	})

	checkResponse, err := s.Check(ctx, &openfgapb.CheckRequest{
		StoreId:              storeID,
		TupleKey:             tk,
		AuthorizationModelId: modelID,
	})
	require.NoError(t, err)
	require.Equal(t, true, checkResponse.Allowed)
}

func TestOperationsWithInvalidModel(t *testing.T) {
	ctx := context.Background()
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	// The model is invalid
	typedefs := parser.MustParse(`
	type user

	type repo
	  relations
        define admin: [user] as self
	    define r1: [user] as self and r2 and r3
	    define r2: [user] as self and r1 and r3
	    define r3: [user] as self and r1 and r2
	`)

	tk := tuple.NewTupleKey("repo:openfga", "r1", "user:anne")
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	mockDatastore.EXPECT().
		ReadAuthorizationModel(gomock.Any(), storeID, modelID).
		AnyTimes().
		Return(&openfgapb.AuthorizationModel{
			SchemaVersion:   typesystem.SchemaVersion1_1,
			TypeDefinitions: typedefs,
		}, nil)

	// the model is error and err should return

	s := New(&Dependencies{
		Datastore: mockDatastore,
		Logger:    logger.NewNoopLogger(),
		Transport: gateway.NewNoopTransport(),
	}, &Config{
		ResolveNodeLimit: test.DefaultResolveNodeLimit,
	})

	_, err := s.Check(ctx, &openfgapb.CheckRequest{
		StoreId:              storeID,
		TupleKey:             tk,
		AuthorizationModelId: modelID,
	})
	require.Error(t, err)
	e, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Code(openfgapb.ErrorCode_validation_error), e.Code())

	_, err = s.ListObjects(ctx, &openfgapb.ListObjectsRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Type:                 "repo",
		Relation:             "r1",
		User:                 "user:anne",
	})
	require.Error(t, err)
	e, ok = status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Code(openfgapb.ErrorCode_validation_error), e.Code())

	err = s.StreamedListObjects(&openfgapb.StreamedListObjectsRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Type:                 "repo",
		Relation:             "r1",
		User:                 "user:anne",
	}, NewMockStreamServer())
	require.Error(t, err)
	e, ok = status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Code(openfgapb.ErrorCode_validation_error), e.Code())

	_, err = s.Expand(ctx, &openfgapb.ExpandRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		TupleKey:             tk,
	})
	require.Error(t, err)
	e, ok = status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Code(openfgapb.ErrorCode_validation_error), e.Code())

}

func TestShortestPathToSolutionWins(t *testing.T) {
	ctx := context.Background()

	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	typedefs := parser.MustParse(`
	type user

	type repo
	  relations
	    define reader: [user:*] as self
	`)

	tk := tuple.NewTupleKey("repo:openfga", "reader", "user:*")
	tuple := &openfgapb.Tuple{Key: tk}

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	mockDatastore.EXPECT().
		ReadAuthorizationModel(gomock.Any(), storeID, modelID).
		AnyTimes().
		Return(&openfgapb.AuthorizationModel{
			SchemaVersion:   typesystem.SchemaVersion1_1,
			TypeDefinitions: typedefs,
		}, nil)

	// it could happen that one of the following two mocks won't be necessary because the goroutine will be short-circuited
	mockDatastore.EXPECT().
		ReadUserTuple(gomock.Any(), storeID, gomock.Any()).
		AnyTimes().
		DoAndReturn(
			func(ctx context.Context, _ string, _ *openfgapb.TupleKey) (storage.TupleIterator, error) {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(500 * time.Millisecond):
					return nil, storage.ErrNotFound
				}
			})

	mockDatastore.EXPECT().
		ReadUsersetTuples(gomock.Any(), storeID, gomock.Any()).
		AnyTimes().
		DoAndReturn(
			func(_ context.Context, _ string, _ storage.ReadUsersetTuplesFilter) (storage.TupleIterator, error) {
				time.Sleep(100 * time.Millisecond)
				return storage.NewStaticTupleIterator([]*openfgapb.Tuple{tuple}), nil
			})

	s := New(&Dependencies{
		Datastore: mockDatastore,
		Logger:    logger.NewNoopLogger(),
		Transport: gateway.NewNoopTransport(),
	}, &Config{
		ResolveNodeLimit: test.DefaultResolveNodeLimit,
	})

	start := time.Now()
	checkResponse, err := s.Check(ctx, &openfgapb.CheckRequest{
		StoreId:              storeID,
		TupleKey:             tk,
		AuthorizationModelId: modelID,
	})
	end := time.Since(start)

	// we expect the Check call to be short-circuited after ReadUsersetTuples runs
	require.Truef(t, end < 200*time.Millisecond, fmt.Sprintf("end was %s", end))
	require.NoError(t, err)
	require.Equal(t, true, checkResponse.Allowed)
}

func TestResolveAuthorizationModel(t *testing.T) {
	ctx := context.Background()
	logger := logger.NewNoopLogger()
	transport := gateway.NewNoopTransport()

	t.Run("no_latest_authorization_model_id_found", func(t *testing.T) {

		store := ulid.Make().String()

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
		mockDatastore.EXPECT().FindLatestAuthorizationModelID(gomock.Any(), store).Return("", storage.ErrNotFound)

		s := New(&Dependencies{
			Datastore: mockDatastore,
			Transport: transport,
			Logger:    logger,
		}, &Config{})

		expectedError := serverErrors.LatestAuthorizationModelNotFound(store)

		_, err := s.resolveAuthorizationModelID(ctx, store, "")
		require.ErrorIs(t, err, expectedError)
	})

	t.Run("read_existing_authorization_model", func(t *testing.T) {
		store := ulid.Make().String()
		modelID := ulid.Make().String()

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
		mockDatastore.EXPECT().FindLatestAuthorizationModelID(gomock.Any(), store).Return(modelID, nil)

		s := New(&Dependencies{
			Datastore: mockDatastore,
			Transport: transport,
			Logger:    logger,
		}, &Config{})

		got, err := s.resolveAuthorizationModelID(ctx, store, "")
		require.NoError(t, err)
		require.Equal(t, modelID, got)
	})

	t.Run("non-valid_modelID_returns_error", func(t *testing.T) {
		store := ulid.Make().String()
		modelID := "foo"
		want := serverErrors.AuthorizationModelNotFound(modelID)

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

		s := New(&Dependencies{
			Datastore: mockDatastore,
			Transport: transport,
			Logger:    logger,
		}, &Config{})

		_, err := s.resolveAuthorizationModelID(ctx, store, modelID)
		require.Equal(t, want, err)
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

// This runs TestListObjects_Unoptimized_UnhappyPaths many times over to ensure no race conditions (see https://github.com/openfga/openfga/pull/762)
func BenchmarkListObjectsNoRaceCondition(b *testing.B) {
	ctx := context.Background()
	logger := logger.NewNoopLogger()
	transport := gateway.NewNoopTransport()
	store := ulid.Make().String()
	modelID := ulid.Make().String()

	mockController := gomock.NewController(b)
	defer mockController.Finish()

	typedefs := parser.MustParse(`
	type user

	type repo
	  relations
	    define allowed: [user] as self
	    define viewer: [user] as self and allowed
    `)

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), store, modelID).AnyTimes().Return(&openfgapb.AuthorizationModel{
		SchemaVersion:   typesystem.SchemaVersion1_1,
		TypeDefinitions: typedefs,
	}, nil)
	mockDatastore.EXPECT().ListObjectsByType(gomock.Any(), store, "repo").AnyTimes().Return(nil, errors.New("error reading from storage"))

	s := New(&Dependencies{
		Datastore: mockDatastore,
		Transport: transport,
		Logger:    logger,
	}, &Config{
		ResolveNodeLimit:      test.DefaultResolveNodeLimit,
		ListObjectsDeadline:   5 * time.Second,
		ListObjectsMaxResults: 1000,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := s.ListObjects(ctx, &openfgapb.ListObjectsRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			Type:                 "repo",
			Relation:             "viewer",
			User:                 "user:bob",
		})

		require.ErrorIs(b, err, serverErrors.NewInternalError("", errors.New("error reading from storage")))

		err = s.StreamedListObjects(&openfgapb.StreamedListObjectsRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			Type:                 "repo",
			Relation:             "viewer",
			User:                 "user:bob",
		}, NewMockStreamServer())

		require.ErrorIs(b, err, serverErrors.NewInternalError("", errors.New("error reading from storage")))
	}
}

// This test ensures that when the data storage fails, ListObjects v0 throws an error
func TestListObjects_Unoptimized_UnhappyPaths(t *testing.T) {
	ctx := context.Background()
	logger := logger.NewNoopLogger()
	transport := gateway.NewNoopTransport()
	store := ulid.Make().String()
	modelID := ulid.Make().String()

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), store, modelID).AnyTimes().Return(&openfgapb.AuthorizationModel{
		SchemaVersion: typesystem.SchemaVersion1_1,
		TypeDefinitions: parser.MustParse(`
		type user

		type repo
		  relations
		    define allowed: [user] as self
		    define viewer: [user] as self and allowed
		`),
	}, nil)
	mockDatastore.EXPECT().ListObjectsByType(gomock.Any(), store, "repo").AnyTimes().Return(nil, errors.New("error reading from storage"))

	s := New(&Dependencies{
		Datastore: mockDatastore,
		Transport: transport,
		Logger:    logger,
	}, &Config{
		ResolveNodeLimit:      test.DefaultResolveNodeLimit,
		ListObjectsDeadline:   5 * time.Second,
		ListObjectsMaxResults: 1000,
	})

	t.Run("error_listing_objects_from_storage_in_non-streaming_version", func(t *testing.T) {
		res, err := s.ListObjects(ctx, &openfgapb.ListObjectsRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			Type:                 "repo",
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
			Type:                 "repo",
			Relation:             "viewer",
			User:                 "user:bob",
		}, NewMockStreamServer())

		require.ErrorIs(t, err, serverErrors.NewInternalError("", errors.New("error reading from storage")))
	})
}

// This test ensures that when the data storage fails, ListObjects v1 throws an error
func TestListObjects_UnhappyPaths(t *testing.T) {
	ctx := context.Background()
	logger := logger.NewNoopLogger()
	transport := gateway.NewNoopTransport()
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
								typesystem.DirectRelationReference("user", ""),
								typesystem.WildcardRelationReference("user"),
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
			{Object: "user:*"},
			{Object: "user:bob"},
		}}).AnyTimes().Return(nil, errors.New("error reading from storage"))

	s := New(&Dependencies{
		Datastore: mockDatastore,
		Transport: transport,
		Logger:    logger,
	}, &Config{
		ResolveNodeLimit:      test.DefaultResolveNodeLimit,
		ListObjectsDeadline:   5 * time.Second,
		ListObjectsMaxResults: 1000,
	})

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

func TestAuthorizationModelInvalidSchemaVersion(t *testing.T) {
	ctx := context.Background()
	logger := logger.NewNoopLogger()
	transport := gateway.NewNoopTransport()
	store := ulid.Make().String()
	modelID := ulid.Make().String()

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	mockDatastore.EXPECT().MaxTypesPerAuthorizationModel().Return(100)

	mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), store, modelID).AnyTimes().Return(&openfgapb.AuthorizationModel{
		SchemaVersion: typesystem.SchemaVersion1_0,
		TypeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "user",
			},
			{
				Type: "team",
				Relations: map[string]*openfgapb.Userset{
					"member": typesystem.This(),
				},
			},
		},
	}, nil)
	mockDatastore.EXPECT().ListObjectsByType(gomock.Any(), store, "repo").AnyTimes().Return(nil, errors.New("error reading from storage"))

	s := Server{
		datastore: mockDatastore,
		transport: transport,
		logger:    logger,
		config: &Config{
			ResolveNodeLimit:      test.DefaultResolveNodeLimit,
			ListObjectsDeadline:   5 * time.Second,
			ListObjectsMaxResults: 1000,
		},
	}

	t.Run("invalid_schema_error_in_check", func(t *testing.T) {
		_, err := s.Check(ctx, &openfgapb.CheckRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			TupleKey: tuple.NewTupleKey(
				"team:abc",
				"member",
				"user:anne"),
		})
		require.Error(t, err)
		e, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(openfgapb.ErrorCode_validation_error), e.Code())
	})

	t.Run("invalid_schema_error_in_list_objects", func(t *testing.T) {
		_, err := s.ListObjects(ctx, &openfgapb.ListObjectsRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			Type:                 "team",
			Relation:             "member",
			User:                 "user:anne",
		})
		require.Error(t, err)
		e, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(openfgapb.ErrorCode_validation_error), e.Code())
	})

	t.Run("invalid_schema_error_in_streamed_list_objects", func(t *testing.T) {
		err := s.StreamedListObjects(&openfgapb.StreamedListObjectsRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			Type:                 "team",
			Relation:             "member",
			User:                 "user:anne",
		}, NewMockStreamServer())
		require.Error(t, err)
		e, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(openfgapb.ErrorCode_validation_error), e.Code())
	})

	t.Run("invalid_schema_error_in_expand", func(t *testing.T) {
		_, err := s.Expand(ctx, &openfgapb.ExpandRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			TupleKey: tuple.NewTupleKey("repo:openfga",
				"reader",
				"user:anne"),
		})
		require.Error(t, err)
		e, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(openfgapb.ErrorCode_validation_error), e.Code())
	})

	t.Run("invalid_schema_error_in_write", func(t *testing.T) {
		_, err := s.Write(ctx, &openfgapb.WriteRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			Writes: &openfgapb.TupleKeys{TupleKeys: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga/openfga",
					"reader",
					"user:anne"),
			}},
		})
		require.Error(t, err)
		e, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(openfgapb.ErrorCode_validation_error), e.Code())
	})

	t.Run("invalid_schema_error_in_write_model", func(t *testing.T) {
		_, err := s.WriteAuthorizationModel(ctx, &openfgapb.WriteAuthorizationModelRequest{
			StoreId:         store,
			SchemaVersion:   typesystem.SchemaVersion1_0,
			TypeDefinitions: parser.MustParse(`type repo`),
		})
		require.Error(t, err)
		e, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(openfgapb.ErrorCode_invalid_authorization_model), e.Code(), err)
	})

	t.Run("invalid_schema_error_in_write_assertion", func(t *testing.T) {
		_, err := s.WriteAssertions(ctx, &openfgapb.WriteAssertionsRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			Assertions: []*openfgapb.Assertion{{
				TupleKey:    tuple.NewTupleKey("repo:test", "reader", "user:elbuo"),
				Expectation: false,
			}},
		})
		require.Error(t, err)
		e, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(openfgapb.ErrorCode_validation_error), e.Code())
	})
}

func MustBootstrapDatastore(t testing.TB, engine string) storage.OpenFGADatastore {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, engine)

	uri := testDatastore.GetConnectionURI(true)

	var ds storage.OpenFGADatastore
	var err error

	switch engine {
	case "memory":
		ds = memory.New()
	case "postgres":
		ds, err = postgres.New(uri, sqlcommon.NewConfig())
	case "mysql":
		ds, err = mysql.New(uri, sqlcommon.NewConfig())
	default:
		t.Fatalf("'%s' is not a supported datastore engine", engine)
	}
	require.NoError(t, err)

	return ds
}
