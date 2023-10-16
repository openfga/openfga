package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"runtime"
	"testing"
	"time"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/golang/mock/gomock"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	mockstorage "github.com/openfga/openfga/internal/mocks"
	serverconfig "github.com/openfga/openfga/internal/server/config"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/server/test"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/mysql"
	"github.com/openfga/openfga/pkg/storage/postgres"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
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

func TestServerPanicIfNoDatastore(t *testing.T) {
	require.PanicsWithError(t, "failed to construct the OpenFGA server: a datastore option must be provided", func() {
		_ = MustNewServerWithOpts()
	})
}

func TestServerPanicIfEmptyRequestDurationDatastoreCountBuckets(t *testing.T) {
	require.PanicsWithError(t, "failed to construct the OpenFGA server: request duration datastore count buckets must not be empty", func() {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
		_ = MustNewServerWithOpts(
			WithDatastore(mockDatastore),
			WithRequestDurationByQueryHistogramBuckets([]uint{}),
		)
	})
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
	tuple := &openfgav1.Tuple{Key: tk}

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	mockDatastore.EXPECT().
		ReadAuthorizationModel(gomock.Any(), storeID, modelID).
		AnyTimes().
		Return(&openfgav1.AuthorizationModel{
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

	s := MustNewServerWithOpts(
		WithDatastore(mockDatastore),
	)

	checkResponse, err := s.Check(ctx, &openfgav1.CheckRequest{
		StoreId:              storeID,
		TupleKey:             tk,
		AuthorizationModelId: modelID,
	})
	require.NoError(t, err)
	require.Equal(t, true, checkResponse.Allowed)
}

func TestListObjectsReleasesConnections(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI(true)
	ds, err := postgres.New(uri, sqlcommon.NewConfig(
		sqlcommon.WithMaxOpenConns(1),
		sqlcommon.WithMaxTuplesPerWrite(2000),
	))
	require.NoError(t, err)
	defer ds.Close()

	s := MustNewServerWithOpts(
		WithDatastore(storagewrappers.NewContextWrapper(ds)),
		WithMaxConcurrentReadsForListObjects(1),
	)

	storeID := ulid.Make().String()

	writeAuthzModelResp, err := s.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId: storeID,
		TypeDefinitions: parser.MustParse(`
		type user

		type document
		  relations
		    define editor: [user] as self
		`),
		SchemaVersion: typesystem.SchemaVersion1_1,
	})
	require.NoError(t, err)

	modelID := writeAuthzModelResp.GetAuthorizationModelId()

	numTuples := 2000
	tuples := make([]*openfgav1.TupleKey, 0, numTuples)
	for i := 0; i < numTuples; i++ {
		tk := tuple.NewTupleKey(fmt.Sprintf("document:%d", i), "editor", "user:jon")

		tuples = append(tuples, tk)
	}

	_, err = s.Write(context.Background(), &openfgav1.WriteRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Writes: &openfgav1.TupleKeys{
			TupleKeys: tuples,
		},
	})
	require.NoError(t, err)

	_, err = s.ListObjects(context.Background(), &openfgav1.ListObjectsRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Type:                 "document",
		Relation:             "editor",
		User:                 "user:jon",
	})
	require.NoError(t, err)

	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer timeoutCancel()

	// If ListObjects is still hogging the database connection pool even after responding, then this fails.
	// If ListObjects is closing up its connections effectively then this will not fail.
	ready, err := ds.IsReady(timeoutCtx)
	require.NoError(t, err)
	require.True(t, ready)
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
		Return(&openfgav1.AuthorizationModel{
			Id:              modelID,
			SchemaVersion:   typesystem.SchemaVersion1_1,
			TypeDefinitions: typedefs,
		}, nil)

	// the model is error and err should return

	s := MustNewServerWithOpts(
		WithDatastore(mockDatastore),
	)

	_, err := s.Check(ctx, &openfgav1.CheckRequest{
		StoreId:              storeID,
		TupleKey:             tk,
		AuthorizationModelId: modelID,
	})
	require.Error(t, err)
	e, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Code(openfgav1.ErrorCode_validation_error), e.Code())

	_, err = s.ListObjects(ctx, &openfgav1.ListObjectsRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Type:                 "repo",
		Relation:             "r1",
		User:                 "user:anne",
	})
	require.Error(t, err)
	e, ok = status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Code(openfgav1.ErrorCode_validation_error), e.Code())

	err = s.StreamedListObjects(&openfgav1.StreamedListObjectsRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Type:                 "repo",
		Relation:             "r1",
		User:                 "user:anne",
	}, NewMockStreamServer())
	require.Error(t, err)
	e, ok = status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Code(openfgav1.ErrorCode_validation_error), e.Code())

	_, err = s.Expand(ctx, &openfgav1.ExpandRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		TupleKey:             tk,
	})
	require.Error(t, err)
	e, ok = status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Code(openfgav1.ErrorCode_validation_error), e.Code())
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
	tuple := &openfgav1.Tuple{Key: tk}

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	mockDatastore.EXPECT().
		ReadAuthorizationModel(gomock.Any(), storeID, modelID).
		AnyTimes().
		Return(&openfgav1.AuthorizationModel{
			SchemaVersion:   typesystem.SchemaVersion1_1,
			TypeDefinitions: typedefs,
		}, nil)

	// it could happen that one of the following two mocks won't be necessary because the goroutine will be short-circuited
	mockDatastore.EXPECT().
		ReadUserTuple(gomock.Any(), storeID, gomock.Any()).
		AnyTimes().
		DoAndReturn(
			func(ctx context.Context, _ string, _ *openfgav1.TupleKey) (storage.TupleIterator, error) {
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
				return storage.NewStaticTupleIterator([]*openfgav1.Tuple{tuple}), nil
			})

	s := MustNewServerWithOpts(
		WithDatastore(mockDatastore),
	)

	start := time.Now()
	checkResponse, err := s.Check(ctx, &openfgav1.CheckRequest{
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

func TestCheckWithCachedResolution(t *testing.T) {
	ctx := context.Background()

	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	typedefs := parser.MustParse(`
	type user

	type repo
	  relations
	    define reader: [user] as self
	`)

	tk := tuple.NewTupleKey("repo:openfga", "reader", "user:mike")
	tuple := &openfgav1.Tuple{Key: tk}

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	mockDatastore.EXPECT().
		ReadAuthorizationModel(gomock.Any(), storeID, modelID).
		AnyTimes().
		Return(&openfgav1.AuthorizationModel{
			SchemaVersion:   typesystem.SchemaVersion1_1,
			TypeDefinitions: typedefs,
		}, nil)

	mockDatastore.EXPECT().
		ReadUserTuple(gomock.Any(), storeID, gomock.Any()).
		Times(1).
		Return(tuple, nil)

	s := MustNewServerWithOpts(
		WithDatastore(mockDatastore),
		WithCheckQueryCacheEnabled(true),
		WithCheckQueryCacheLimit(10),
		WithCheckQueryCacheTTL(1*time.Minute),
		WithExperimentals(ExperimentalCheckQueryCache),
	)

	checkResponse, err := s.Check(ctx, &openfgav1.CheckRequest{
		StoreId:              storeID,
		TupleKey:             tk,
		AuthorizationModelId: modelID,
	})

	require.NoError(t, err)
	require.True(t, checkResponse.Allowed)

	// If we check for the same request, data should come from cache and number of ReadUserTuple should still be 1
	checkResponse, err = s.Check(ctx, &openfgav1.CheckRequest{
		StoreId:              storeID,
		TupleKey:             tk,
		AuthorizationModelId: modelID,
	})

	require.NoError(t, err)
	require.True(t, checkResponse.Allowed)
}

func TestResolveAuthorizationModel(t *testing.T) {
	ctx := context.Background()

	t.Run("no_latest_authorization_model_id_found", func(t *testing.T) {
		store := ulid.Make().String()

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
		mockDatastore.EXPECT().FindLatestAuthorizationModelID(gomock.Any(), store).Return("", storage.ErrNotFound)

		s := MustNewServerWithOpts(
			WithDatastore(mockDatastore),
		)

		expectedError := serverErrors.LatestAuthorizationModelNotFound(store)

		_, err := s.resolveTypesystem(ctx, store, "")
		require.ErrorIs(t, err, expectedError)
	})

	t.Run("read_existing_authorization_model", func(t *testing.T) {
		store := ulid.Make().String()
		modelID := ulid.Make().String()

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
		mockDatastore.EXPECT().FindLatestAuthorizationModelID(gomock.Any(), store).Return(modelID, nil)
		mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), store, modelID).Return(
			&openfgav1.AuthorizationModel{
				Id:            modelID,
				SchemaVersion: typesystem.SchemaVersion1_1,
			},
			nil,
		)

		s := MustNewServerWithOpts(
			WithDatastore(mockDatastore),
		)

		typesys, err := s.resolveTypesystem(ctx, store, "")
		require.NoError(t, err)
		require.Equal(t, modelID, typesys.GetAuthorizationModelID())
	})

	t.Run("non-valid_modelID_returns_error", func(t *testing.T) {
		store := ulid.Make().String()
		modelID := "foo"
		want := serverErrors.AuthorizationModelNotFound(modelID)

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

		s := MustNewServerWithOpts(
			WithDatastore(mockDatastore),
		)

		_, err := s.resolveTypesystem(ctx, store, modelID)
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

func (m *mockStreamServer) Send(*openfgav1.StreamedListObjectsResponse) error {
	return nil
}

// This runs ListObjects and StreamedListObjects many times over to ensure no race conditions (see https://github.com/openfga/openfga/pull/762)
func BenchmarkListObjectsNoRaceCondition(b *testing.B) {
	ctx := context.Background()
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

	mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), store, modelID).AnyTimes().Return(&openfgav1.AuthorizationModel{
		SchemaVersion:   typesystem.SchemaVersion1_1,
		TypeDefinitions: typedefs,
	}, nil)
	mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), store, gomock.Any()).AnyTimes().Return(nil, errors.New("error reading from storage"))

	s := MustNewServerWithOpts(
		WithDatastore(mockDatastore),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := s.ListObjects(ctx, &openfgav1.ListObjectsRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			Type:                 "repo",
			Relation:             "viewer",
			User:                 "user:bob",
		})

		require.ErrorIs(b, err, serverErrors.NewInternalError("", errors.New("error reading from storage")))

		err = s.StreamedListObjects(&openfgav1.StreamedListObjectsRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			Type:                 "repo",
			Relation:             "viewer",
			User:                 "user:bob",
		}, NewMockStreamServer())

		require.ErrorIs(b, err, serverErrors.NewInternalError("", errors.New("error reading from storage")))
	}
}

func TestListObjects_ErrorCases(t *testing.T) {
	ctx := context.Background()
	store := ulid.Make().String()

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	t.Run("database_errors", func(t *testing.T) {
		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

		s := MustNewServerWithOpts(
			WithDatastore(mockDatastore),
		)

		modelID := ulid.Make().String()

		mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), store, modelID).AnyTimes().Return(&openfgav1.AuthorizationModel{
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`
			type user

			type document
			  relations
				define viewer: [user, user:*] as self
			`),
		}, nil)

		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), store, storage.ReadStartingWithUserFilter{
			ObjectType: "document",
			Relation:   "viewer",
			UserFilter: []*openfgav1.ObjectRelation{
				{Object: "user:*"},
				{Object: "user:bob"},
			}}).AnyTimes().Return(nil, errors.New("error reading from storage"))

		t.Run("error_listing_objects_from_storage_in_non-streaming_version", func(t *testing.T) {
			res, err := s.ListObjects(ctx, &openfgav1.ListObjectsRequest{
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
			err := s.StreamedListObjects(&openfgav1.StreamedListObjectsRequest{
				StoreId:              store,
				AuthorizationModelId: modelID,
				Type:                 "document",
				Relation:             "viewer",
				User:                 "user:bob",
			}, NewMockStreamServer())

			require.ErrorIs(t, err, serverErrors.NewInternalError("", errors.New("error reading from storage")))
		})
	})

	t.Run("graph_resolution_errors", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(memory.New()),
			WithResolveNodeLimit(2),
		)

		writeModelResp, err := s.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       store,
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustParse(`
			type user

			type group
			  relations
			    define member: [user, group#member] as self

			type document
			  relations
				define viewer: [group#member] as self
			`),
		})
		require.NoError(t, err)

		_, err = s.Write(ctx, &openfgav1.WriteRequest{
			StoreId: store,
			Writes: &openfgav1.TupleKeys{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey("document:1", "viewer", "group:1#member"),
					tuple.NewTupleKey("group:1", "member", "group:2#member"),
					tuple.NewTupleKey("group:2", "member", "group:3#member"),
					tuple.NewTupleKey("group:3", "member", "user:jon"),
				},
			},
		})
		require.NoError(t, err)

		t.Run("resolution_depth_exceeded_error_unary", func(t *testing.T) {
			res, err := s.ListObjects(ctx, &openfgav1.ListObjectsRequest{
				StoreId:              store,
				AuthorizationModelId: writeModelResp.GetAuthorizationModelId(),
				Type:                 "document",
				Relation:             "viewer",
				User:                 "user:jon",
			})

			require.Nil(t, res)
			require.ErrorIs(t, err, serverErrors.AuthorizationModelResolutionTooComplex)
		})

		t.Run("resolution_depth_exceeded_error_streaming", func(t *testing.T) {
			err := s.StreamedListObjects(&openfgav1.StreamedListObjectsRequest{
				StoreId:              store,
				AuthorizationModelId: writeModelResp.GetAuthorizationModelId(),
				Type:                 "document",
				Relation:             "viewer",
				User:                 "user:jon",
			}, NewMockStreamServer())

			require.ErrorIs(t, err, serverErrors.AuthorizationModelResolutionTooComplex)
		})
	})
}

func TestAuthorizationModelInvalidSchemaVersion(t *testing.T) {
	ctx := context.Background()
	store := ulid.Make().String()
	modelID := ulid.Make().String()

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), store, modelID).AnyTimes().Return(&openfgav1.AuthorizationModel{
		SchemaVersion: typesystem.SchemaVersion1_0,
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{
				Type: "user",
			},
			{
				Type: "team",
				Relations: map[string]*openfgav1.Userset{
					"member": typesystem.This(),
				},
			},
		},
	}, nil)

	s := MustNewServerWithOpts(
		WithDatastore(mockDatastore),
	)

	t.Run("invalid_schema_error_in_check", func(t *testing.T) {
		_, err := s.Check(ctx, &openfgav1.CheckRequest{
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
		require.Equal(t, codes.Code(openfgav1.ErrorCode_validation_error), e.Code())
	})

	t.Run("invalid_schema_error_in_list_objects", func(t *testing.T) {
		_, err := s.ListObjects(ctx, &openfgav1.ListObjectsRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			Type:                 "team",
			Relation:             "member",
			User:                 "user:anne",
		})
		require.Error(t, err)
		e, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(openfgav1.ErrorCode_validation_error), e.Code())
	})

	t.Run("invalid_schema_error_in_streamed_list_objects", func(t *testing.T) {
		err := s.StreamedListObjects(&openfgav1.StreamedListObjectsRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			Type:                 "team",
			Relation:             "member",
			User:                 "user:anne",
		}, NewMockStreamServer())
		require.Error(t, err)
		e, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(openfgav1.ErrorCode_validation_error), e.Code())
	})

	t.Run("invalid_schema_error_in_expand", func(t *testing.T) {
		_, err := s.Expand(ctx, &openfgav1.ExpandRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			TupleKey: tuple.NewTupleKey("repo:openfga",
				"reader",
				"user:anne"),
		})
		require.Error(t, err)
		e, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(openfgav1.ErrorCode_validation_error), e.Code())
	})

	t.Run("invalid_schema_error_in_write", func(t *testing.T) {
		_, err := s.Write(ctx, &openfgav1.WriteRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			Writes: &openfgav1.TupleKeys{TupleKeys: []*openfgav1.TupleKey{
				tuple.NewTupleKey("repo:openfga/openfga",
					"reader",
					"user:anne"),
			}},
		})
		require.Error(t, err)
		e, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(openfgav1.ErrorCode_validation_error), e.Code())
	})

	t.Run("invalid_schema_error_in_write_model", func(t *testing.T) {
		mockDatastore.EXPECT().MaxTypesPerAuthorizationModel().Return(100)

		_, err := s.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
			StoreId:         store,
			SchemaVersion:   typesystem.SchemaVersion1_0,
			TypeDefinitions: parser.MustParse(`type repo`),
		})
		require.Error(t, err)
		e, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(openfgav1.ErrorCode_invalid_authorization_model), e.Code(), err)
	})

	t.Run("invalid_schema_error_in_write_assertion", func(t *testing.T) {
		_, err := s.WriteAssertions(ctx, &openfgav1.WriteAssertionsRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			Assertions: []*openfgav1.Assertion{{
				TupleKey:    tuple.NewTupleKey("repo:test", "reader", "user:elbuo"),
				Expectation: false,
			}},
		})
		require.Error(t, err)
		e, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(openfgav1.ErrorCode_validation_error), e.Code())
	})
}

func TestDefaultMaxConcurrentReadSettings(t *testing.T) {
	cfg := serverconfig.DefaultConfig()
	require.EqualValues(t, math.MaxUint32, cfg.MaxConcurrentReadsForCheck)
	require.EqualValues(t, math.MaxUint32, cfg.MaxConcurrentReadsForListObjects)

	s := MustNewServerWithOpts(
		WithDatastore(memory.New()),
	)
	require.EqualValues(t, math.MaxUint32, s.maxConcurrentReadsForCheck)
	require.EqualValues(t, math.MaxUint32, s.maxConcurrentReadsForListObjects)
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
