package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	language "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/openfga/openfga/cmd/migrate"
	"github.com/openfga/openfga/cmd/util"
	"github.com/openfga/openfga/internal/build"
	mockstorage "github.com/openfga/openfga/internal/mocks"
	serverconfig "github.com/openfga/openfga/internal/server/config"
	"github.com/openfga/openfga/pkg/server/commands"
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
)

func init() {
	_, filename, _, _ := runtime.Caller(0)
	dir := path.Join(path.Dir(filename), "..", "..")
	err := os.Chdir(dir)
	if err != nil {
		panic(err)
	}
}

func ExampleNewServerWithOpts() {
	datastore := memory.New() // other supported datastores include Postgres and MySQL
	defer datastore.Close()

	openfga, err := NewServerWithOpts(WithDatastore(datastore),
		WithCheckQueryCacheEnabled(true),
		// more options available
	)
	if err != nil {
		panic(err)
	}
	defer openfga.Close()

	// create store
	store, err := openfga.CreateStore(context.Background(),
		&openfgav1.CreateStoreRequest{Name: "demo"})
	if err != nil {
		panic(err)
	}

	model := language.MustTransformDSLToProto(`
	model
		schema 1.1
	type user
	
	type document
		relations
			define reader: [user]`)

	// write the model to the store
	authorizationModel, err := openfga.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         store.Id,
		TypeDefinitions: model.TypeDefinitions,
		Conditions:      model.Conditions,
		SchemaVersion:   model.SchemaVersion,
	})
	if err != nil {
		panic(err)
	}

	// write tuples to the store
	_, err = openfga.Write(context.Background(), &openfgav1.WriteRequest{
		StoreId: store.Id,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				{Object: "document:budget", Relation: "reader", User: "user:anne"},
			},
		},
		Deletes: nil,
	})
	if err != nil {
		panic(err)
	}

	// make an authorization check
	checkResponse, err := openfga.Check(context.Background(), &openfgav1.CheckRequest{
		StoreId:              store.Id,
		AuthorizationModelId: authorizationModel.AuthorizationModelId, // optional, but recommended for speed
		TupleKey: &openfgav1.CheckRequestTupleKey{
			User:     "user:anne",
			Relation: "reader",
			Object:   "document:budget",
		},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(checkResponse.Allowed)
	// Output: true
}

func TestServerPanicIfNoDatastore(t *testing.T) {
	require.PanicsWithError(t, "failed to construct the OpenFGA server: a datastore option must be provided", func() {
		_ = MustNewServerWithOpts()
	})
}

func TestServerNotReadyDueToDatastoreRevision(t *testing.T) {
	engines := []string{"postgres", "mysql"}

	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			_, ds, stopFunc, uri, err := util.MustBootstrapDatastore(t, engine)
			defer stopFunc()
			require.NoError(t, err)

			targetVersion := build.MinimumSupportedDatastoreSchemaRevision - 1

			migrateCommand := migrate.NewMigrateCommand()

			migrateCommand.SetArgs([]string{"--datastore-engine", engine, "--datastore-uri", uri, "--version", strconv.Itoa(int(targetVersion))})

			err = migrateCommand.Execute()
			require.NoError(t, err)

			status, _ := ds.IsReady(context.Background())
			require.Contains(t, status.Message, fmt.Sprintf("datastore requires migrations: at revision '%d', but requires '%d'.", targetVersion, build.MinimumSupportedDatastoreSchemaRevision))
			require.False(t, status.IsReady)
		})
	}
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
	ds, stopFunc := MustBootstrapDatastore(t, "postgres")
	defer func() {
		stopFunc()
		goleak.VerifyNone(t)
	}()

	test.RunAllTests(t, ds)
}

func TestServerWithPostgresDatastoreAndExplicitCredentials(t *testing.T) {
	testDatastore, stopFunc := storagefixtures.RunDatastoreTestContainer(t, "postgres")
	defer func() {
		stopFunc()
		goleak.VerifyNone(t)
	}()

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
	ds, stopFunc := MustBootstrapDatastore(t, "memory")
	defer func() {
		stopFunc()
		goleak.VerifyNone(t)
	}()

	test.RunAllTests(t, ds)
}

func TestServerWithMySQLDatastore(t *testing.T) {
	ds, stopFunc := MustBootstrapDatastore(t, "mysql")
	defer func() {
		stopFunc()
		goleak.VerifyNone(t)
	}()

	test.RunAllTests(t, ds)
}

func TestServerWithMySQLDatastoreAndExplicitCredentials(t *testing.T) {
	testDatastore, stopFunc := storagefixtures.RunDatastoreTestContainer(t, "mysql")
	defer func() {
		stopFunc()
		goleak.VerifyNone(t)
	}()

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
		testDatastore, stopFunc := storagefixtures.RunDatastoreTestContainer(b, "postgres")
		defer stopFunc()

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
		testDatastore, stopFunc := storagefixtures.RunDatastoreTestContainer(b, "mysql")
		defer stopFunc()

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

	typedefs := language.MustTransformDSLToProto(`model
	schema 1.1
type user

type repo
  relations
	define reader: [user]
`).TypeDefinitions

	tk := tuple.NewCheckRequestTupleKey("repo:openfga", "reader", "user:anne")
	returnedTuple := &openfgav1.Tuple{Key: tuple.ConvertCheckRequestTupleKeyToTupleKey(tk)}

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
		Return(returnedTuple, nil)

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
	require.True(t, checkResponse.Allowed)
}

func TestListObjectsReleasesConnections(t *testing.T) {
	testDatastore, stopFunc := storagefixtures.RunDatastoreTestContainer(t, "postgres")
	defer stopFunc()

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
		TypeDefinitions: language.MustTransformDSLToProto(`model
	schema 1.1
type user

type document
  relations
	define editor: [user]`).TypeDefinitions,
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
		Writes: &openfgav1.WriteRequestWrites{
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
	status, err := ds.IsReady(timeoutCtx)
	require.NoError(t, err)
	require.True(t, status.IsReady)
}

func TestOperationsWithInvalidModel(t *testing.T) {
	ctx := context.Background()
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	// The model is invalid
	typedefs := language.MustTransformDSLToProto(`model
	schema 1.1
type user

type repo
  relations
	define admin: [user]
	define r1: [user] and r2 and r3
	define r2: [user] and r1 and r3
	define r3: [user] and r1 and r2`).TypeDefinitions

	tk := tuple.NewCheckRequestTupleKey("repo:openfga", "r1", "user:anne")
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
		TupleKey:             tuple.NewExpandRequestTupleKey(tk.Object, tk.Relation),
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

	typedefs := language.MustTransformDSLToProto(`model
  schema 1.1
type user

type repo
  relations
	define reader: [user:*]`).TypeDefinitions

	tk := tuple.NewCheckRequestTupleKey("repo:openfga", "reader", "user:*")
	returnedTuple := &openfgav1.Tuple{Key: tuple.ConvertCheckRequestTupleKeyToTupleKey(tk)}

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
				return storage.NewStaticTupleIterator([]*openfgav1.Tuple{returnedTuple}), nil
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
	require.Lessf(t, end, 200*time.Millisecond, fmt.Sprintf("end was %s", end))
	require.NoError(t, err)
	require.True(t, checkResponse.Allowed)
}

func TestCheckWithCachedResolution(t *testing.T) {
	ctx := context.Background()

	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	typedefs := language.MustTransformDSLToProto(`model
  schema 1.1
type user

type repo
  relations
	define reader: [user]`).TypeDefinitions

	tk := tuple.NewCheckRequestTupleKey("repo:openfga", "reader", "user:mike")
	returnedTuple := &openfgav1.Tuple{Key: tuple.ConvertCheckRequestTupleKeyToTupleKey(tk)}

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
		Return(returnedTuple, nil)

	s := MustNewServerWithOpts(
		WithDatastore(mockDatastore),
		WithCheckQueryCacheEnabled(true),
		WithCheckQueryCacheLimit(10),
		WithCheckQueryCacheTTL(1*time.Minute),
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

func TestWriteAssertionModelDSError(t *testing.T) {
	ctx := context.Background()

	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	typedefs := language.MustTransformDSLToProto(`model
	schema 1.1
type user

type repo
  relations
	define reader: [user]`).TypeDefinitions

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDSOldSchema := mockstorage.NewMockOpenFGADatastore(mockController)

	mockDSOldSchema.EXPECT().
		ReadAuthorizationModel(gomock.Any(), storeID, modelID).
		AnyTimes().
		Return(&openfgav1.AuthorizationModel{
			SchemaVersion:   typesystem.SchemaVersion1_0,
			TypeDefinitions: typedefs,
		}, nil)

	mockDSBadReadAuthModel := mockstorage.NewMockOpenFGADatastore(mockController)

	mockDSBadReadAuthModel.EXPECT().
		ReadAuthorizationModel(gomock.Any(), storeID, modelID).
		AnyTimes().
		Return(nil, fmt.Errorf("unable to read"))

	mockDSBadWriteAssertions := mockstorage.NewMockOpenFGADatastore(mockController)
	mockDSBadWriteAssertions.EXPECT().
		ReadAuthorizationModel(gomock.Any(), storeID, modelID).
		AnyTimes().
		Return(&openfgav1.AuthorizationModel{
			SchemaVersion:   typesystem.SchemaVersion1_1,
			TypeDefinitions: typedefs,
		}, nil)
	mockDSBadWriteAssertions.EXPECT().
		WriteAssertions(gomock.Any(), storeID, modelID, gomock.Any()).
		AnyTimes().
		Return(fmt.Errorf("unable to write"))

	tests := []struct {
		name          string
		assertions    []*openfgav1.Assertion
		mockDatastore *mockstorage.MockOpenFGADatastore
		expectedError error
	}{
		{
			name:          "unsupported_schema",
			assertions:    []*openfgav1.Assertion{},
			mockDatastore: mockDSOldSchema,
			expectedError: serverErrors.ValidationError(
				fmt.Errorf("invalid schema version"),
			),
		},
		{
			name:          "failed_to_read",
			assertions:    []*openfgav1.Assertion{},
			mockDatastore: mockDSBadReadAuthModel,
			expectedError: serverErrors.NewInternalError(
				"", fmt.Errorf("unable to read"),
			),
		},
		{
			name:          "failed_to_write",
			assertions:    []*openfgav1.Assertion{},
			mockDatastore: mockDSBadWriteAssertions,
			expectedError: serverErrors.NewInternalError(
				"", fmt.Errorf("unable to write"),
			),
		},
	}

	for _, curTest := range tests {
		t.Run(curTest.name, func(t *testing.T) {
			request := &openfgav1.WriteAssertionsRequest{
				StoreId:              storeID,
				Assertions:           curTest.assertions,
				AuthorizationModelId: modelID,
			}

			writeAssertionCmd := commands.NewWriteAssertionsCommand(curTest.mockDatastore)
			_, err := writeAssertionCmd.Execute(ctx, request)
			require.ErrorIs(t, curTest.expectedError, err)
		})
	}
}

func TestReadAssertionModelDSError(t *testing.T) {
	ctx := context.Background()

	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDSBadReadAssertions := mockstorage.NewMockOpenFGADatastore(mockController)
	mockDSBadReadAssertions.EXPECT().
		ReadAssertions(gomock.Any(), storeID, modelID).
		AnyTimes().
		Return(nil, fmt.Errorf("unable to read"))

	readAssertionQuery := commands.NewReadAssertionsQuery(mockDSBadReadAssertions)
	_, err := readAssertionQuery.Execute(ctx, storeID, modelID)
	expectedError := serverErrors.NewInternalError(
		"", fmt.Errorf("unable to read"),
	)
	require.ErrorIs(t, expectedError, err)
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

	typedefs := language.MustTransformDSLToProto(`model
  schema 1.1
type user

type repo
  relations
	define allowed: [user]
	define viewer: [user] and allowed`).TypeDefinitions

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
			TypeDefinitions: language.MustTransformDSLToProto(`model
  schema 1.1
type user

type document
  relations
	define viewer: [user, user:*]`).TypeDefinitions,
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
			TypeDefinitions: language.MustTransformDSLToProto(`model
  schema 1.1
type user

type group
  relations
	define member: [user, group#member]

type document
  relations
	define viewer: [group#member]`).TypeDefinitions,
		})
		require.NoError(t, err)

		_, err = s.Write(ctx, &openfgav1.WriteRequest{
			StoreId: store,
			Writes: &openfgav1.WriteRequestWrites{
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
			TupleKey: tuple.NewCheckRequestTupleKey(
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

	t.Run("invalid_schema_error_in_write", func(t *testing.T) {
		_, err := s.Write(ctx, &openfgav1.WriteRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					{
						Object:   "repo:openfga/openfga",
						Relation: "reader",
						User:     "user:anne",
					},
				},
			},
		})
		require.Error(t, err)
		e, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(openfgav1.ErrorCode_validation_error), e.Code())
	})

	t.Run("invalid_schema_error_in_write_model", func(t *testing.T) {
		mockDatastore.EXPECT().MaxTypesPerAuthorizationModel().Return(100)

		_, err := s.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       store,
			SchemaVersion: typesystem.SchemaVersion1_0,
			TypeDefinitions: language.MustTransformDSLToProto(`model
	schema 1.1
type repo
`).TypeDefinitions,
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
				TupleKey:    tuple.NewAssertionTupleKey("repo:test", "reader", "user:elbuo"),
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

func MustBootstrapDatastore(t testing.TB, engine string) (storage.OpenFGADatastore, func()) {
	testDatastore, stopFunc := storagefixtures.RunDatastoreTestContainer(t, engine)

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

	return ds, func() {
		ds.Close()
		stopFunc()
	}
}
