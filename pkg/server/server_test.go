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
	"sync"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/cmd/migrate"
	"github.com/openfga/openfga/cmd/util"
	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/internal/cachecontroller"
	"github.com/openfga/openfga/internal/graph"
	mockstorage "github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/featureflags"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/server/test"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/mysql"
	"github.com/openfga/openfga/pkg/storage/postgres"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	"github.com/openfga/openfga/pkg/storage/sqlite"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	storageTest "github.com/openfga/openfga/pkg/storage/test"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/testutils"
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
	datastore := memory.New() // other supported datastores include Postgres, MySQL and SQLite
	defer datastore.Close()

	openfga, err := NewServerWithOpts(WithDatastore(datastore),
		WithCheckQueryCacheEnabled(true),
		// more options available
		WithFeatureFlagClient(featureflags.NewHardcodedBooleanClient(true)),
		WithShadowListObjectsQueryTimeout(17*time.Millisecond),
		WithShadowListObjectsQueryMaxDeltaItems(20),
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

	model := parser.MustTransformDSLToProto(`
	model
		schema 1.1

	type user

	type document
		relations
			define reader: [user]`)

	// write the model to the store
	authorizationModel, err := openfga.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         store.GetId(),
		TypeDefinitions: model.GetTypeDefinitions(),
		Conditions:      model.GetConditions(),
		SchemaVersion:   model.GetSchemaVersion(),
	})
	if err != nil {
		panic(err)
	}

	// write tuples to the store
	_, err = openfga.Write(context.Background(), &openfgav1.WriteRequest{
		StoreId: store.GetId(),
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
		StoreId:              store.GetId(),
		AuthorizationModelId: authorizationModel.GetAuthorizationModelId(), // optional, but recommended for speed
		TupleKey: &openfgav1.CheckRequestTupleKey{
			User:     "user:anne",
			Relation: "reader",
			Object:   "document:budget",
		},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(checkResponse.GetAllowed())
	// Output: true
}

func TestServerPanicIfValidationsFail(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("no_datastore", func(t *testing.T) {
		require.PanicsWithError(t, "failed to construct the OpenFGA server: a datastore option must be provided", func() {
			_ = MustNewServerWithOpts()
		})
	})
	t.Run("no_datastore_and_check_query_cache_enabled", func(t *testing.T) {
		require.PanicsWithError(t, "failed to construct the OpenFGA server: a datastore option must be provided", func() {
			_ = MustNewServerWithOpts(
				WithCheckQueryCacheEnabled(true))
		})
	})

	t.Run("no_request_duration_by_query_histogram_buckets", func(t *testing.T) {
		require.PanicsWithError(t, "failed to construct the OpenFGA server: request duration datastore count buckets must not be empty", func() {
			mockController := gomock.NewController(t)
			defer mockController.Finish()
			mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
			s := MustNewServerWithOpts(
				WithDatastore(mockDatastore),
				WithRequestDurationByQueryHistogramBuckets([]uint{}),
			)
			defer s.Close()
		})
	})
	t.Run("no_request_duration_by_dispatch_count_histogram_buckets", func(t *testing.T) {
		require.PanicsWithError(t, "failed to construct the OpenFGA server: request duration by dispatch count buckets must not be empty", func() {
			mockController := gomock.NewController(t)
			defer mockController.Finish()
			mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
			_ = MustNewServerWithOpts(
				WithDatastore(mockDatastore),
				WithRequestDurationByDispatchCountHistogramBuckets([]uint{}),
			)
		})
	})

	t.Run("invalid_dispatch_throttle_threshold", func(t *testing.T) {
		require.PanicsWithError(t, "failed to construct the OpenFGA server: check default dispatch throttling threshold must be equal or smaller than max dispatch threshold for Check", func() {
			mockController := gomock.NewController(t)
			defer mockController.Finish()
			mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
			_ = MustNewServerWithOpts(
				WithDatastore(mockDatastore),
				WithDispatchThrottlingCheckResolverEnabled(true),
				WithDispatchThrottlingCheckResolverThreshold(100),
				WithDispatchThrottlingCheckResolverMaxThreshold(80),
			)
		})
	})

	t.Run("invalid_access_control_setup", func(t *testing.T) {
		require.PanicsWithError(t, "failed to construct the OpenFGA server: access control parameters are not enabled. They can be enabled for experimental use by passing the `--experimentals enable-access-control` configuration option when running OpenFGA server. Additionally, the `--access-control-store-id` and `--access-control-model-id` parameters must not be empty", func() {
			mockController := gomock.NewController(t)
			defer mockController.Finish()
			mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
			_ = MustNewServerWithOpts(
				WithDatastore(mockDatastore),
				WithExperimentals(serverconfig.ExperimentalAccessControlParams),
				WithAccessControlParams(true, "", "", ""),
			)
		})
	})

	t.Run("errors_when_oidc_is_not_enabled", func(t *testing.T) {
		require.PanicsWithError(t, "failed to construct the OpenFGA server: access control is enabled, but the authentication method is not OIDC. Access control is only supported with OIDC authentication", func() {
			mockController := gomock.NewController(t)
			defer mockController.Finish()
			mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
			_ = MustNewServerWithOpts(
				WithDatastore(mockDatastore),
				WithExperimentals(serverconfig.ExperimentalAccessControlParams),
				WithAccessControlParams(true, ulid.Make().String(), ulid.Make().String(), ""),
			)
		})
	})

	t.Run("errors_when_access_control_store_id_is_not_a_valid_ulid", func(t *testing.T) {
		require.PanicsWithError(t, "failed to construct the OpenFGA server: config '--access-control-store-id' must be a valid ULID", func() {
			mockController := gomock.NewController(t)
			defer mockController.Finish()
			mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
			_ = MustNewServerWithOpts(
				WithDatastore(mockDatastore),
				WithExperimentals(serverconfig.ExperimentalAccessControlParams),
				WithAccessControlParams(true, "not-a-valid-ulid", ulid.Make().String(), "oidc"),
			)
		})
	})

	t.Run("errors_when_access_control_model_id_is_not_a_valid_ulid", func(t *testing.T) {
		require.PanicsWithError(t, "failed to construct the OpenFGA server: config '--access-control-model-id' must be a valid ULID", func() {
			mockController := gomock.NewController(t)
			defer mockController.Finish()
			mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
			_ = MustNewServerWithOpts(
				WithDatastore(mockDatastore),
				WithExperimentals(serverconfig.ExperimentalAccessControlParams),
				WithAccessControlParams(true, ulid.Make().String(), "not-a-valid-ulid", "oidc"),
			)
		})
	})

	t.Run("invalid_dialect", func(t *testing.T) {
		require.PanicsWithValue(t, `failed to set database dialect: "invalid-dialect": unknown dialect`, func() {
			sqlcommon.NewDBInfo(nil, sq.StatementBuilder, nil, "invalid-dialect")
		})
	})

	t.Run("invalid_shadow_list_objects_query_timeout", func(t *testing.T) {
		require.PanicsWithError(t, "failed to construct the OpenFGA server: shadow list objects check resolver timeout must be greater than 0, got -1s", func() {
			mockController := gomock.NewController(t)
			mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
			_ = MustNewServerWithOpts(
				WithDatastore(mockDatastore),
				WithFeatureFlagClient(featureflags.NewHardcodedBooleanClient(true)),
				WithShadowListObjectsQueryTimeout(-1*time.Second),
			)
		})
	})
}

func TestServerNotReadyDueToDatastoreRevision(t *testing.T) {
	// skipping sqlite here because the lowest supported schema revision is 4
	engines := []string{"postgres", "mysql"}

	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			_, ds, uri := util.MustBootstrapDatastore(t, engine)

			targetVersion := build.MinimumSupportedDatastoreSchemaRevision - 1

			migrateCommand := migrate.NewMigrateCommand()

			migrateCommand.SetArgs([]string{"--datastore-engine", engine, "--datastore-uri", uri, "--version", strconv.Itoa(int(targetVersion))})

			err := migrateCommand.Execute()
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

func TestServerPanicIfNilContext(t *testing.T) {
	require.PanicsWithError(t, "failed to construct the OpenFGA server: server cannot be started with nil context", func() {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
		_ = MustNewServerWithOpts(
			WithDatastore(mockDatastore),
			WithRequestDurationByQueryHistogramBuckets([]uint{}),
			WithContext(nil), //nolint
		)
	})
}

func TestServerPanicIfEmptyRequestDurationDispatchCountBuckets(t *testing.T) {
	require.PanicsWithError(t, "failed to construct the OpenFGA server: request duration by dispatch count buckets must not be empty", func() {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
		_ = MustNewServerWithOpts(
			WithDatastore(mockDatastore),
			WithRequestDurationByDispatchCountHistogramBuckets([]uint{}),
		)
	})
}

func TestServerPanicIfDefaultDispatchThresholdGreaterThanMaxDispatchThreshold(t *testing.T) {
	require.PanicsWithError(t, "failed to construct the OpenFGA server: check default dispatch throttling threshold must be equal or smaller than max dispatch threshold for Check", func() {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
		_ = MustNewServerWithOpts(
			WithDatastore(mockDatastore),
			WithDispatchThrottlingCheckResolverEnabled(true),
			WithDispatchThrottlingCheckResolverThreshold(100),
			WithDispatchThrottlingCheckResolverMaxThreshold(80),
		)
	})
}

func TestServerPanicIfDefaultListObjectsThresholdGreaterThanMaxDispatchThreshold(t *testing.T) {
	require.PanicsWithError(t, "failed to construct the OpenFGA server: ListObjects default dispatch throttling threshold must be equal or smaller than max dispatch threshold for ListObjects", func() {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
		_ = MustNewServerWithOpts(
			WithDatastore(mockDatastore),
			WithListObjectsDispatchThrottlingEnabled(true),
			WithListObjectsDispatchThrottlingThreshold(100),
			WithListObjectsDispatchThrottlingMaxThreshold(80),
		)
	})
}

func TestServerPanicIfDefaultListUsersThresholdGreaterThanMaxDispatchThreshold(t *testing.T) {
	require.PanicsWithError(t, "failed to construct the OpenFGA server: ListUsers default dispatch throttling threshold must be equal or smaller than max dispatch threshold for ListUsers", func() {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
		_ = MustNewServerWithOpts(
			WithDatastore(mockDatastore),
			WithListUsersDispatchThrottlingEnabled(true),
			WithListUsersDispatchThrottlingThreshold(100),
			WithListUsersDispatchThrottlingMaxThreshold(80),
		)
	})
}
func TestServerWithPostgresDatastore(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	_, ds, _ := util.MustBootstrapDatastore(t, "postgres")

	test.RunAllTests(t, ds)
}

func TestServerWithPostgresDatastoreAndExplicitCredentials(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
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
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	_, ds, _ := util.MustBootstrapDatastore(t, "memory")

	test.RunAllTests(t, ds)
}

func TestServerWithMySQLDatastore(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	_, ds, _ := util.MustBootstrapDatastore(t, "mysql")

	test.RunAllTests(t, ds)
}

func TestServerWithMySQLDatastoreAndExplicitCredentials(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
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

func TestServerWithSQLiteDatastore(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	_, ds, _ := util.MustBootstrapDatastore(t, "sqlite")

	test.RunAllTests(t, ds)
}

func TestAvoidDeadlockAcrossCheckRequests(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	_, ds, _ := util.MustBootstrapDatastore(t, "memory")

	s := MustNewServerWithOpts(
		WithDatastore(ds),
	)
	t.Cleanup(s.Close)

	createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
		Name: "openfga-test",
	})
	require.NoError(t, err)

	storeID := createStoreResp.GetId()

	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1

		type user

		type document
			relations
				define viewer: [user, document#viewer] or editor
				define editor: [user, document#viewer]`)

	writeAuthModelResp, err := s.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   model.GetSchemaVersion(),
		TypeDefinitions: model.GetTypeDefinitions(),
	})
	require.NoError(t, err)

	modelID := writeAuthModelResp.GetAuthorizationModelId()

	_, err = s.Write(context.Background(), &openfgav1.WriteRequest{
		StoreId: storeID,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "editor", "document:1#viewer"),
				tuple.NewTupleKey("document:1", "editor", "user:andres"),
			},
		},
	})
	require.NoError(t, err)

	var wg sync.WaitGroup

	wg.Add(3)

	var resp1 *openfgav1.CheckResponse
	var err1 error
	go func() {
		defer wg.Done()

		resp1, err1 = s.Check(context.Background(), &openfgav1.CheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: modelID,
			TupleKey:             tuple.NewCheckRequestTupleKey("document:1", "editor", "user:jon"),
		})
	}()

	var resp2 *openfgav1.CheckResponse
	var err2 error
	go func() {
		defer wg.Done()

		resp2, err2 = s.Check(context.Background(), &openfgav1.CheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: modelID,
			TupleKey:             tuple.NewCheckRequestTupleKey("document:1", "viewer", "user:jon"),
		})
	}()

	var resp3 *openfgav1.CheckResponse
	var err3 error
	go func() {
		defer wg.Done()

		resp3, err3 = s.Check(context.Background(), &openfgav1.CheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: modelID,
			TupleKey:             tuple.NewCheckRequestTupleKey("document:1", "viewer", "user:andres"),
		})
	}()

	wg.Wait()

	require.NoError(t, err1)
	require.NotNil(t, resp1)
	require.False(t, resp1.GetAllowed())

	require.NoError(t, err2)
	require.NotNil(t, resp2)

	require.NoError(t, err3)
	require.NotNil(t, resp3)
	require.True(t, resp3.GetAllowed())
}

func TestAvoidDeadlockWithinSingleCheckRequest(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	_, ds, _ := util.MustBootstrapDatastore(t, "memory")

	s := MustNewServerWithOpts(
		WithDatastore(ds),
	)
	t.Cleanup(s.Close)

	createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
		Name: "openfga-test",
	})
	require.NoError(t, err)

	storeID := createStoreResp.GetId()

	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1

		type user

		type document
			relations
				define editor1: [user, document#viewer1]

				define viewer2: [document#viewer1] or editor1
				define viewer1: [user] or viewer2
				define can_view: viewer1 or editor1`)

	writeAuthModelResp, err := s.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   model.GetSchemaVersion(),
		TypeDefinitions: model.GetTypeDefinitions(),
	})
	require.NoError(t, err)

	modelID := writeAuthModelResp.GetAuthorizationModelId()

	_, err = s.Write(context.Background(), &openfgav1.WriteRequest{
		StoreId: storeID,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "editor1", "document:1#viewer1"),
			},
		},
	})
	require.NoError(t, err)

	resp, err := s.Check(context.Background(), &openfgav1.CheckRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		TupleKey:             tuple.NewCheckRequestTupleKey("document:1", "can_view", "user:jon"),
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.False(t, resp.GetAllowed())
}

func TestRequestContextPropagation(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	for _, tc := range []struct {
		name                   string
		shouldPropagateContext bool
	}{
		{name: "disabled", shouldPropagateContext: false},
		{name: "enabled", shouldPropagateContext: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storeID := ulid.Make().String()
			modelID := ulid.Make().String()

			mockController := gomock.NewController(t)
			t.Cleanup(mockController.Finish)

			parentCtx, cancelParentCtx := context.WithCancel(context.Background())
			t.Cleanup(cancelParentCtx)

			mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

			model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1

			type user

			type repo
				relations
					define reader: [user]`)

			mockDatastore.EXPECT().
				ReadAuthorizationModel(gomock.Any(), gomock.Eq(storeID), gomock.Eq(modelID)).
				Return(model, nil)

			mockDatastore.EXPECT().
				ReadUserTuple(gomock.Any(), gomock.Eq(storeID), gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, _, _, _ any) (*openfgav1.Tuple, error) {
					cancelParentCtx()
					if tc.shouldPropagateContext {
						require.ErrorIs(t, ctx.Err(), context.Canceled, "storage context must get cancelled if the request context is propagated")
					} else {
						require.NoError(t, ctx.Err(), "storage context must not get canceled if request context propagation is disabled")
					}
					// Return dummy error, we don't care about the check result for this testcase
					return nil, storage.ErrNotFound
				})

			s := MustNewServerWithOpts(
				WithDatastore(mockDatastore),
				WithContextPropagationToDatastore(tc.shouldPropagateContext),
			)
			t.Cleanup(func() {
				mockDatastore.EXPECT().Close().Times(1)
				s.Close()
			})

			// We do not care about the check result as we assert via mockDatastore.
			_, _ = s.Check(parentCtx, &openfgav1.CheckRequest{
				StoreId:              storeID,
				TupleKey:             tuple.NewCheckRequestTupleKey("repo:openfga", "reader", "user:mike"),
				AuthorizationModelId: modelID,
			})
		})
	}
}

func TestThreeProngThroughVariousLayers(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	_, ds, _ := util.MustBootstrapDatastore(t, "memory")

	s := MustNewServerWithOpts(
		WithDatastore(ds),
	)
	t.Cleanup(func() {
		s.Close()
	})

	createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
		Name: "openfga-test",
	})
	require.NoError(t, err)

	storeID := createStoreResp.GetId()

	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1

		type user
		type module
			relations
				define owner: [user] or owner from parent
				define parent: [document, module]
				define viewer: [user] or owner or viewer from parent
		type folder
			relations
				define owner: [user] or owner from parent
				define parent: [module, folder]
				define viewer: [user] or owner or viewer from parent
		type document
			relations
				define owner: [user] or owner from parent
				define parent: [folder, document]
				define viewer: [user] or owner or viewer from parent`)

	writeAuthModelResp, err := s.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   model.GetSchemaVersion(),
		TypeDefinitions: model.GetTypeDefinitions(),
	})
	require.NoError(t, err)

	modelID := writeAuthModelResp.GetAuthorizationModelId()

	_, err = s.Write(context.Background(), &openfgav1.WriteRequest{
		StoreId: storeID,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				tuple.NewTupleKey("module:a", "owner", "user:anne"),
				tuple.NewTupleKey("folder:a", "parent", "module:a"),
				tuple.NewTupleKey("document:a", "parent", "folder:a"),
				tuple.NewTupleKey("module:b", "parent", "document:a"),
				tuple.NewTupleKey("folder:b", "parent", "module:b"),
				tuple.NewTupleKey("document:b", "parent", "folder:b"),
				tuple.NewTupleKey("module:a", "parent", "document:b"),
			},
		},
	})
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			tupleKeys := []*openfgav1.CheckRequestTupleKey{
				tuple.NewCheckRequestTupleKey("module:a", "viewer", "user:anne"),
				tuple.NewCheckRequestTupleKey("module:b", "viewer", "user:anne"),
				tuple.NewCheckRequestTupleKey("folder:a", "viewer", "user:anne"),
				tuple.NewCheckRequestTupleKey("folder:b", "viewer", "user:anne"),
				tuple.NewCheckRequestTupleKey("document:a", "viewer", "user:anne"),
				tuple.NewCheckRequestTupleKey("document:b", "viewer", "user:anne"),
			}

			for _, tupleKey := range tupleKeys {
				resp, err := s.Check(context.Background(), &openfgav1.CheckRequest{
					StoreId:              storeID,
					AuthorizationModelId: modelID,
					TupleKey:             tupleKey,
				})
				require.NoError(t, err)
				require.True(t, resp.GetAllowed())
			}
		})
	}
}

func BenchmarkOpenFGAServer(b *testing.B) {
	b.Cleanup(func() {
		goleak.VerifyNone(b,
			// https://github.com/uber-go/goleak/discussions/89
			goleak.IgnoreTopFunction("testing.(*B).run1"),
			goleak.IgnoreTopFunction("testing.(*B).doBench"),
			// Ignore CPU profiler goroutine when running benchmarks with -cpuprofile
			goleak.IgnoreAnyFunction("runtime/pprof.profileWriter"),
		)
	})
	b.Run("BenchmarkPostgresDatastore", func(b *testing.B) {
		testDatastore := storagefixtures.RunDatastoreTestContainer(b, "postgres")

		uri := testDatastore.GetConnectionURI(true)
		ds, err := postgres.New(uri, sqlcommon.NewConfig(
			sqlcommon.WithMaxOpenConns(10),
			sqlcommon.WithMaxIdleConns(10),
		))
		require.NoError(b, err)
		b.Cleanup(ds.Close)
		test.RunAllBenchmarks(b, ds)
	})

	b.Run("BenchmarkMemoryDatastore", func(b *testing.B) {
		ds := memory.New()
		b.Cleanup(ds.Close)
		test.RunAllBenchmarks(b, ds)
	})

	b.Run("BenchmarkMySQLDatastore", func(b *testing.B) {
		testDatastore := storagefixtures.RunDatastoreTestContainer(b, "mysql")

		uri := testDatastore.GetConnectionURI(true)
		ds, err := mysql.New(uri, sqlcommon.NewConfig(
			sqlcommon.WithMaxOpenConns(10),
			sqlcommon.WithMaxIdleConns(10),
		))
		require.NoError(b, err)
		b.Cleanup(ds.Close)
		test.RunAllBenchmarks(b, ds)
	})

	b.Run("BenchmarkSQLiteDatastore", func(b *testing.B) {
		testDatastore := storagefixtures.RunDatastoreTestContainer(b, "sqlite")

		uri := testDatastore.GetConnectionURI(true)
		ds, err := sqlite.New(uri, sqlcommon.NewConfig())
		require.NoError(b, err)
		b.Cleanup(ds.Close)
		test.RunAllBenchmarks(b, ds)
	})
}

func TestCheckDoesNotThrowBecauseDirectTupleWasFound(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ctx := context.Background()
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	typedefs := parser.MustTransformDSLToProto(`
		model
			schema 1.1

		type user

		type repo
			relations
				define reader: [user]
		`).GetTypeDefinitions()

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
			Id:              modelID,
		}, nil)

	// it could happen that one of the following two mocks won't be necessary because the goroutine will be short-circuited
	mockDatastore.EXPECT().
		ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
		AnyTimes().
		Return(returnedTuple, nil)

	mockDatastore.EXPECT().
		ReadUsersetTuples(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(
			func(_ context.Context, _ string, _ storage.ReadUsersetTuplesFilter, _ storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
				time.Sleep(50 * time.Millisecond)
				return nil, errors.New("some error")
			})

	s := MustNewServerWithOpts(
		WithDatastore(mockDatastore),
	)
	t.Cleanup(func() {
		mockDatastore.EXPECT().Close().Times(1)
		s.Close()
	})

	checkResponse, err := s.Check(ctx, &openfgav1.CheckRequest{
		StoreId:              storeID,
		TupleKey:             tk,
		AuthorizationModelId: modelID,
	})
	require.NoError(t, err)
	require.True(t, checkResponse.GetAllowed())
}

func TestReleasesConnections(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

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
	)
	t.Cleanup(s.Close)

	storeID := ulid.Make().String()

	writeAuthzModelResp, err := s.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId: storeID,
		TypeDefinitions: parser.MustTransformDSLToProto(`
			model
				schema 1.1

			type user

			type document
				relations
					define editor: [user]`).GetTypeDefinitions(),
		SchemaVersion: typesystem.SchemaVersion1_1,
	})
	require.NoError(t, err)

	modelID := writeAuthzModelResp.GetAuthorizationModelId()

	numTuples := 2000

	t.Run("list_objects", func(t *testing.T) {
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
	})

	t.Run("list_users", func(t *testing.T) {
		tuples := make([]*openfgav1.TupleKey, 0, numTuples)
		for i := 0; i < numTuples; i++ {
			tk := tuple.NewTupleKey("document:1", "editor", fmt.Sprintf("user:%d", i))

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

		_, err = s.ListUsers(context.Background(), &openfgav1.ListUsersRequest{
			StoreId:              storeID,
			AuthorizationModelId: modelID,
			Relation:             "editor",
			Object: &openfgav1.Object{
				Type: "document",
				Id:   "1",
			},
			UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
		})
		require.NoError(t, err)

		timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer timeoutCancel()

		// If ListUsers is still hogging the database connection pool even after responding, then this fails.
		// If ListUsers is closing up its connections effectively then this will not fail.
		status, err := ds.IsReady(timeoutCtx)
		require.NoError(t, err)
		require.True(t, status.IsReady)
	})
}

func TestOperationsWithInvalidModel(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	// The model is invalid
	typedefs := parser.MustTransformDSLToProto(`
		model
			schema 1.1

		type user

		type repo
			relations
				define admin: [user]
				define r1: [user] and r2 and r3
				define r2: [user] and r1 and r3
				define r3: [user] and r1 and r2`).GetTypeDefinitions()

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
	t.Cleanup(func() {
		mockDatastore.EXPECT().Close().Times(1)
		s.Close()
	})

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
	}, NewMockStreamServer(context.Background()))
	require.Error(t, err)
	e, ok = status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Code(openfgav1.ErrorCode_validation_error), e.Code())

	_, err = s.ListUsers(ctx, &openfgav1.ListUsersRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Relation:             tk.GetRelation(),
		Object:               &openfgav1.Object{Type: "repo", Id: "openfga"},
		UserFilters:          []*openfgav1.UserTypeFilter{{Type: "user"}},
	})
	require.Error(t, err)
	e, ok = status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Code(openfgav1.ErrorCode_validation_error), e.Code())

	_, err = s.Expand(ctx, &openfgav1.ExpandRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		TupleKey:             tuple.NewExpandRequestTupleKey(tk.GetObject(), tk.GetRelation()),
	})
	require.Error(t, err)
	e, ok = status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Code(openfgav1.ErrorCode_validation_error), e.Code())
}

func TestShortestPathToSolutionWins(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()

	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	typedefs := parser.MustTransformDSLToProto(`
		model
			schema 1.1

		type user

		type repo
			relations
				define reader: [user:*]`).GetTypeDefinitions()

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
			Id:              modelID,
		}, nil)

	// it could happen that one of the following two mocks won't be necessary because the goroutine will be short-circuited
	mockDatastore.EXPECT().
		ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(
			func(ctx context.Context, _ string, _ *openfgav1.TupleKey, _ storage.ReadUserTupleOptions) (storage.TupleIterator, error) {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(500 * time.Millisecond):
					return nil, storage.ErrNotFound
				}
			})

	mockDatastore.EXPECT().
		ReadUsersetTuples(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(
			func(_ context.Context, _ string, _ storage.ReadUsersetTuplesFilter, _ storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
				time.Sleep(100 * time.Millisecond)
				return storage.NewStaticTupleIterator([]*openfgav1.Tuple{returnedTuple}), nil
			})

	s := MustNewServerWithOpts(
		WithDatastore(mockDatastore),
	)
	t.Cleanup(func() {
		mockDatastore.EXPECT().Close().Times(1)
		s.Close()
	})

	start := time.Now()
	checkResponse, err := s.Check(ctx, &openfgav1.CheckRequest{
		StoreId:              storeID,
		TupleKey:             tk,
		AuthorizationModelId: modelID,
	})
	end := time.Since(start)

	// we expect the Check call to be short-circuited after ReadUsersetTuples runs
	require.Lessf(t, end, 200*time.Millisecond, "end was "+end.String())
	require.NoError(t, err)
	require.True(t, checkResponse.GetAllowed())
}

func TestCheckWithCachedResolution(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()

	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	typedefs := parser.MustTransformDSLToProto(`
		model
			schema 1.1

		type user

		type repo
			relations
				define reader: [user]`).GetTypeDefinitions()

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
			Id:              modelID,
		}, nil)

	mockDatastore.EXPECT().
		ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
		Times(1).
		Return(returnedTuple, nil)

	s := MustNewServerWithOpts(
		WithDatastore(mockDatastore),
		WithCheckQueryCacheEnabled(true),
		WithCheckCacheLimit(10),
		WithCheckQueryCacheTTL(1*time.Minute),
	)
	t.Cleanup(func() {
		mockDatastore.EXPECT().Close().Times(1)
		s.Close()
	})

	checkResponse, err := s.Check(ctx, &openfgav1.CheckRequest{
		StoreId:              storeID,
		TupleKey:             tk,
		AuthorizationModelId: modelID,
	})

	require.NoError(t, err)
	require.True(t, checkResponse.GetAllowed())

	// If we check for the same request, data should come from cache and number of ReadUserTuple should still be 1
	checkResponse, err = s.Check(ctx, &openfgav1.CheckRequest{
		StoreId:              storeID,
		TupleKey:             tk,
		AuthorizationModelId: modelID,
	})

	require.NoError(t, err)
	require.True(t, checkResponse.GetAllowed())
}

func TestResolveAuthorizationModel(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()

	t.Run("no_latest_authorization_model_id_found", func(t *testing.T) {
		store := ulid.Make().String()

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
		mockDatastore.EXPECT().FindLatestAuthorizationModel(gomock.Any(), store).Return(nil, storage.ErrNotFound)

		s := MustNewServerWithOpts(
			WithDatastore(mockDatastore),
		)
		t.Cleanup(func() {
			mockDatastore.EXPECT().Close().Times(1)
			s.Close()
		})

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
		mockDatastore.EXPECT().FindLatestAuthorizationModel(gomock.Any(), store).Return(
			&openfgav1.AuthorizationModel{
				Id:            modelID,
				SchemaVersion: typesystem.SchemaVersion1_1,
			},
			nil,
		)

		s := MustNewServerWithOpts(
			WithDatastore(mockDatastore),
		)
		t.Cleanup(func() {
			mockDatastore.EXPECT().Close().Times(1)
			s.Close()
		})

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
		t.Cleanup(func() {
			mockDatastore.EXPECT().Close().Times(1)
			s.Close()
		})

		_, err := s.resolveTypesystem(ctx, store, modelID)
		require.Equal(t, want, err)
	})
}

type mockStreamServer struct {
	grpc.ServerStream

	ctx context.Context
}

func NewMockStreamServer(ctx context.Context) *mockStreamServer {
	return &mockStreamServer{
		ctx: ctx,
	}
}

func (m *mockStreamServer) Context() context.Context {
	return m.ctx
}

func (m *mockStreamServer) Send(*openfgav1.StreamedListObjectsResponse) error {
	return nil
}

// This runs ListObjects and StreamedListObjects many times over to ensure no race conditions (see https://github.com/openfga/openfga/pull/762)
func BenchmarkListObjectsNoRaceCondition(b *testing.B) {
	b.Cleanup(func() {
		goleak.VerifyNone(b,
			// https://github.com/uber-go/goleak/discussions/89
			goleak.IgnoreTopFunction("testing.(*B).run1"),
			goleak.IgnoreTopFunction("testing.(*B).doBench"),
		)
	})
	ctx := context.Background()
	store := ulid.Make().String()
	modelID := ulid.Make().String()

	mockController := gomock.NewController(b)
	defer mockController.Finish()

	typedefs := parser.MustTransformDSLToProto(`
		model
			schema 1.1

		type user

		type repo
			relations
				define allowed: [user]
				define viewer: [user] and allowed`).GetTypeDefinitions()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), store, modelID).AnyTimes().Return(&openfgav1.AuthorizationModel{
		SchemaVersion:   typesystem.SchemaVersion1_1,
		TypeDefinitions: typedefs,
		Id:              modelID,
	}, nil)
	mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), store, gomock.Any(), gomock.Any()).AnyTimes().Return(nil, errors.New("error reading from storage"))

	s := MustNewServerWithOpts(
		WithDatastore(mockDatastore),
	)
	b.Cleanup(func() {
		mockDatastore.EXPECT().Close().Times(1)
		s.Close()
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := s.ListObjects(ctx, &openfgav1.ListObjectsRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			Type:                 "repo",
			Relation:             "viewer",
			User:                 "user:bob",
		})

		require.EqualError(b, err, serverErrors.NewInternalError("", errors.New("error reading from storage")).Error())

		err = s.StreamedListObjects(&openfgav1.StreamedListObjectsRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			Type:                 "repo",
			Relation:             "viewer",
			User:                 "user:bob",
		}, NewMockStreamServer(context.Background()))

		require.EqualError(b, err, serverErrors.NewInternalError("", errors.New("error reading from storage")).Error())
	}
}

func TestListObjects_ErrorCases(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()
	store := ulid.Make().String()

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	t.Run("database_errors", func(t *testing.T) {
		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

		s := MustNewServerWithOpts(
			WithDatastore(mockDatastore),
		)
		t.Cleanup(func() {
			mockDatastore.EXPECT().Close().Times(1)
			s.Close()
		})

		modelID := ulid.Make().String()

		mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), store, modelID).AnyTimes().Return(&openfgav1.AuthorizationModel{
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user

				type document
					relations
						define viewer: [user, user:*]`).GetTypeDefinitions(),
		}, nil)

		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), store, storage.ReadStartingWithUserFilter{
			ObjectType: "document",
			Relation:   "viewer",
			UserFilter: []*openfgav1.ObjectRelation{
				{Object: "user:*"},
				{Object: "user:bob"},
			}}, gomock.Any()).AnyTimes().Return(nil, errors.New("error reading from storage"))

		t.Run("error_listing_objects_from_storage_in_non-streaming_version", func(t *testing.T) {
			res, err := s.ListObjects(ctx, &openfgav1.ListObjectsRequest{
				StoreId:              store,
				AuthorizationModelId: modelID,
				Type:                 "document",
				Relation:             "viewer",
				User:                 "user:bob",
			})

			require.Nil(t, res)
			require.EqualError(t, err, serverErrors.NewInternalError("", errors.New("error reading from storage")).Error())
		})

		t.Run("error_listing_objects_from_storage_in_streaming_version", func(t *testing.T) {
			err := s.StreamedListObjects(&openfgav1.StreamedListObjectsRequest{
				StoreId:              store,
				AuthorizationModelId: modelID,
				Type:                 "document",
				Relation:             "viewer",
				User:                 "user:bob",
			}, NewMockStreamServer(context.Background()))

			require.EqualError(t, err, serverErrors.NewInternalError("", errors.New("error reading from storage")).Error())
		})
	})

	t.Run("graph_resolution_errors", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(memory.New()),
			WithResolveNodeLimit(2),
		)
		t.Cleanup(s.Close)

		writeModelResp, err := s.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       store,
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user

				type group
					relations
						define member: [user, group#member]

				type document
					relations
						define viewer: [group#member]`).GetTypeDefinitions(),
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
			require.ErrorIs(t, err, serverErrors.ErrAuthorizationModelResolutionTooComplex)
		})

		t.Run("resolution_depth_exceeded_error_streaming", func(t *testing.T) {
			err := s.StreamedListObjects(&openfgav1.StreamedListObjectsRequest{
				StoreId:              store,
				AuthorizationModelId: writeModelResp.GetAuthorizationModelId(),
				Type:                 "document",
				Relation:             "viewer",
				User:                 "user:jon",
			}, NewMockStreamServer(context.Background()))

			require.ErrorIs(t, err, serverErrors.ErrAuthorizationModelResolutionTooComplex)
		})
	})
}

func TestAuthorizationModelInvalidSchemaVersion(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

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
	t.Cleanup(func() {
		mockDatastore.EXPECT().Close().Times(1)
		s.Close()
	})

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
		}, NewMockStreamServer(context.Background()))
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
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	cfg := serverconfig.DefaultConfig()
	require.EqualValues(t, math.MaxUint32, cfg.MaxConcurrentReadsForCheck)
	require.EqualValues(t, math.MaxUint32, cfg.MaxConcurrentReadsForListObjects)
	require.EqualValues(t, math.MaxUint32, cfg.MaxConcurrentReadsForListUsers)

	s := MustNewServerWithOpts(
		WithDatastore(memory.New()),
	)
	t.Cleanup(s.Close)
	require.EqualValues(t, math.MaxUint32, s.maxConcurrentReadsForCheck)
	require.EqualValues(t, math.MaxUint32, s.maxConcurrentReadsForListObjects)
	require.EqualValues(t, math.MaxUint32, s.maxConcurrentReadsForListUsers)
}

func TestDelegateCheckResolver(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	t.Run("default_check_resolver_alone", func(t *testing.T) {
		cfg := serverconfig.DefaultConfig()
		require.False(t, cfg.CheckDispatchThrottling.Enabled)
		require.False(t, cfg.ListObjectsDispatchThrottling.Enabled)
		require.False(t, cfg.ListUsersDispatchThrottling.Enabled)
		require.False(t, cfg.CheckQueryCache.Enabled)

		ds := memory.New()
		t.Cleanup(ds.Close)
		s := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(s.Close)
		require.False(t, s.checkDispatchThrottlingEnabled)

		require.False(t, s.cacheSettings.CheckQueryCacheEnabled)

		checkResolver, closer, _ := s.getCheckResolverBuilder().Build()
		defer closer()
		require.NotNil(t, checkResolver)

		localCheckResolver, ok := checkResolver.GetDelegate().(*graph.LocalChecker)
		require.True(t, ok)

		_, ok = localCheckResolver.GetDelegate().(*graph.LocalChecker)
		require.True(t, ok)
	})

	t.Run("dispatch_throttling_check_resolver_enabled", func(t *testing.T) {
		ds := memory.New()
		t.Cleanup(ds.Close)
		const dispatchThreshold = 50
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithDispatchThrottlingCheckResolverEnabled(true),
			WithDispatchThrottlingCheckResolverThreshold(dispatchThreshold),
		)
		t.Cleanup(s.Close)

		require.False(t, s.cacheSettings.CheckQueryCacheEnabled)

		require.True(t, s.checkDispatchThrottlingEnabled)
		require.EqualValues(t, dispatchThreshold, s.checkDispatchThrottlingDefaultThreshold)
		require.EqualValues(t, 0, s.checkDispatchThrottlingMaxThreshold)
		checkResolver, closer, _ := s.getCheckResolverBuilder().Build()
		defer closer()
		require.NotNil(t, checkResolver)

		dispatchThrottlingResolver, ok := checkResolver.(*graph.DispatchThrottlingCheckResolver)
		require.True(t, ok)

		localChecker, ok := dispatchThrottlingResolver.GetDelegate().(*graph.LocalChecker)
		require.True(t, ok)

		_, ok = localChecker.GetDelegate().(*graph.DispatchThrottlingCheckResolver)
		require.True(t, ok)
	})

	t.Run("dispatch_throttling_check_resolver_enabled_zero_max_threshold", func(t *testing.T) {
		ds := memory.New()
		t.Cleanup(ds.Close)
		const dispatchThreshold = 50
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithDispatchThrottlingCheckResolverEnabled(true),
			WithDispatchThrottlingCheckResolverThreshold(dispatchThreshold),
			WithDispatchThrottlingCheckResolverMaxThreshold(0),
		)
		t.Cleanup(s.Close)

		require.False(t, s.cacheSettings.CheckQueryCacheEnabled)

		require.True(t, s.checkDispatchThrottlingEnabled)
		require.EqualValues(t, dispatchThreshold, s.checkDispatchThrottlingDefaultThreshold)
		require.EqualValues(t, 0, s.checkDispatchThrottlingMaxThreshold)
		checkResolver, closer, _ := s.getCheckResolverBuilder().Build()
		defer closer()
		require.NotNil(t, checkResolver)

		dispatchThrottlingResolver, ok := checkResolver.(*graph.DispatchThrottlingCheckResolver)
		require.True(t, ok)

		localChecker, ok := dispatchThrottlingResolver.GetDelegate().(*graph.LocalChecker)
		require.True(t, ok)

		_, ok = localChecker.GetDelegate().(*graph.DispatchThrottlingCheckResolver)
		require.True(t, ok)
	})

	t.Run("dispatch_throttling_check_resolver_enabled_non_zero_max_threshold", func(t *testing.T) {
		ds := memory.New()
		t.Cleanup(ds.Close)
		const dispatchThreshold = 50
		const maxDispatchThreshold = 60

		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithDispatchThrottlingCheckResolverEnabled(true),
			WithDispatchThrottlingCheckResolverThreshold(dispatchThreshold),
			WithDispatchThrottlingCheckResolverMaxThreshold(maxDispatchThreshold),
		)
		t.Cleanup(s.Close)

		require.False(t, s.cacheSettings.CheckQueryCacheEnabled)

		require.True(t, s.checkDispatchThrottlingEnabled)
		require.EqualValues(t, dispatchThreshold, s.checkDispatchThrottlingDefaultThreshold)
		require.EqualValues(t, maxDispatchThreshold, s.checkDispatchThrottlingMaxThreshold)
		checkResolver, closer, _ := s.getCheckResolverBuilder().Build()
		defer closer()
		require.NotNil(t, checkResolver)
		dispatchThrottlingResolver, ok := checkResolver.(*graph.DispatchThrottlingCheckResolver)
		require.True(t, ok)

		localChecker, ok := dispatchThrottlingResolver.GetDelegate().(*graph.LocalChecker)
		require.True(t, ok)

		_, ok = localChecker.GetDelegate().(*graph.DispatchThrottlingCheckResolver)
		require.True(t, ok)
	})

	t.Run("cache_check_resolver_enabled", func(t *testing.T) {
		ds := memory.New()
		t.Cleanup(ds.Close)
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithCheckQueryCacheEnabled(true),
		)
		t.Cleanup(s.Close)

		require.False(t, s.checkDispatchThrottlingEnabled)

		require.True(t, s.cacheSettings.CheckQueryCacheEnabled)
		checkResolver, closer, _ := s.getCheckResolverBuilder().Build()
		defer closer()
		require.NotNil(t, checkResolver)

		cachedCheckResolver, ok := checkResolver.(*graph.CachedCheckResolver)
		require.True(t, ok)

		localChecker, ok := cachedCheckResolver.GetDelegate().(*graph.LocalChecker)
		require.True(t, ok)

		_, ok = localChecker.GetDelegate().(*graph.CachedCheckResolver)
		require.True(t, ok)
	})

	t.Run("both_dispatch_throttling_and_cache_check_resolver_enabled", func(t *testing.T) {
		ds := memory.New()
		t.Cleanup(ds.Close)
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithCheckQueryCacheEnabled(true),
			WithDispatchThrottlingCheckResolverEnabled(true),
			WithDispatchThrottlingCheckResolverThreshold(50),
			WithDispatchThrottlingCheckResolverMaxThreshold(100),
		)
		t.Cleanup(s.Close)

		require.True(t, s.checkDispatchThrottlingEnabled)
		require.EqualValues(t, 50, s.checkDispatchThrottlingDefaultThreshold)
		require.EqualValues(t, 100, s.checkDispatchThrottlingMaxThreshold)

		checkResolver, closer, _ := s.getCheckResolverBuilder().Build()
		defer closer()
		require.NotNil(t, checkResolver)

		cachedCheckResolver, ok := checkResolver.(*graph.CachedCheckResolver)
		require.True(t, ok)
		require.True(t, s.cacheSettings.CheckQueryCacheEnabled)

		dispatchThrottlingResolver, ok := cachedCheckResolver.GetDelegate().(*graph.DispatchThrottlingCheckResolver)
		require.True(t, ok)

		localChecker, ok := dispatchThrottlingResolver.GetDelegate().(*graph.LocalChecker)
		require.True(t, ok)

		_, ok = localChecker.GetDelegate().(*graph.CachedCheckResolver)
		require.True(t, ok)
	})
}

func TestWithFeatureFlagClient(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New() // Datastore required for server instantiation
	t.Cleanup(ds.Close)

	t.Run("it_initializes_a_noop_client_if_no_client_passed", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithFeatureFlagClient(nil),
		)
		t.Cleanup(s.Close)
		// NoopClient() always false
		require.False(t, s.featureFlagClient.Boolean("should-be-false", nil))
	})

	t.Run("if_a_client_is_provided", func(t *testing.T) {
		t.Run("it_uses_it", func(t *testing.T) {
			s := MustNewServerWithOpts(
				WithDatastore(ds),
				WithFeatureFlagClient(featureflags.NewHardcodedBooleanClient(true)),
			)
			t.Cleanup(s.Close)
			require.True(t, s.featureFlagClient.Boolean("should-be-true", nil))
		})
	})
}

func TestIsAccessControlEnabled(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New() // Datastore required for server instantiation
	t.Cleanup(ds.Close)

	t.Run("returns_false_if_experimentals_does_not_have_access_control", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals("some-other-feature"),
			WithAccessControlParams(true, "some-model-id", "some-store-id", ""),
		)
		t.Cleanup(s.Close)
		require.False(t, s.IsAccessControlEnabled())
	})

	t.Run("returns_false_if_access_control_is_disabled", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalAccessControlParams),
			WithAccessControlParams(false, "some-model-id", "some-store-id", ""),
		)
		t.Cleanup(s.Close)
		require.False(t, s.IsAccessControlEnabled())
	})

	t.Run("returns_true_if_access_control_is_enabled", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalAccessControlParams),
			WithAccessControlParams(true, ulid.Make().String(), ulid.Make().String(), "oidc"),
		)
		t.Cleanup(s.Close)
		require.True(t, s.IsAccessControlEnabled())
	})
}

func TestServer_ThrottleUntilDeadline(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)

	modelStr := `
		model
			schema 1.1
		type user

		type group
		relations
			define other: [user]
			define member: [user, group#member, group#other]

		type document
		relations
			define viewer: [user, group#member]`

	tuples := []string{
		"document:1#viewer@user:jon", // Observed before first dispatch
		"document:1#viewer@group:eng#member",
		"group:eng#member@group:backend#member",
		"group:backend#member@user:tyler", // Requires two dispatches, gets throttled

		"document:2#viewer@user:tyler",
	}

	storeID, model := storageTest.BootstrapFGAStore(t, ds, modelStr, tuples)
	t.Cleanup(ds.Close)

	deadline := 50 * time.Millisecond

	s := MustNewServerWithOpts(
		WithDatastore(ds),

		WithDispatchThrottlingCheckResolverEnabled(true),
		WithDispatchThrottlingCheckResolverFrequency(3*deadline), // Forces time-out when throttling occurs
		WithDispatchThrottlingCheckResolverThreshold(1),          // Applies throttling after first dispatch

		WithListObjectsDeadline(deadline),
		WithListObjectsDispatchThrottlingEnabled(true),
		WithListObjectsDispatchThrottlingThreshold(1),          // Applies throttling after first dispatch
		WithListObjectsDispatchThrottlingFrequency(3*deadline), // Forces time-out when throttling occurs

		WithListUsersDeadline(deadline),
		WithListUsersDispatchThrottlingEnabled(true),
		WithListUsersDispatchThrottlingThreshold(1),          // Applies throttling after first dispatch
		WithListUsersDispatchThrottlingFrequency(2*deadline), // Forces time-out when throttling occurs
	)
	t.Cleanup(s.Close)

	ctx := context.Background()

	t.Run("list_users_return_no_error_and_partial_results", func(t *testing.T) {
		resp, err := s.ListUsers(ctx, &openfgav1.ListUsersRequest{
			StoreId:              storeID,
			AuthorizationModelId: model.GetId(),
			Object: &openfgav1.Object{
				Type: "document",
				Id:   "1",
			},
			Relation: "viewer",
			UserFilters: []*openfgav1.UserTypeFilter{
				{Type: "user"},
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.LessOrEqual(t, len(resp.GetUsers()), 1) // race condition of context cancellation
	})

	t.Run("list_objects_return_no_error_and_partial_results", func(t *testing.T) {
		resp, err := s.ListObjects(ctx, &openfgav1.ListObjectsRequest{
			StoreId:              storeID,
			AuthorizationModelId: model.GetId(),
			User:                 "user:tyler",
			Relation:             "viewer",
			Type:                 "document",
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.LessOrEqual(t, len(resp.GetObjects()), 1) // race condition of context cancellation
	})
}

func TestServerCheckCache(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("query_cache_enabled_iterator_cache_enabled", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(memory.New()),
			WithCheckQueryCacheEnabled(true),
			WithCheckQueryCacheTTL(1*time.Minute),
			WithCheckCacheLimit(10),
			WithCheckIteratorCacheEnabled(true),
			WithCheckIteratorCacheMaxResults(10),
		)
		t.Cleanup(s.Close)

		require.NotNil(t, s.sharedDatastoreResources.CheckCache)
		require.True(t, s.cacheSettings.ShouldCacheCheckQueries())
		require.True(t, s.cacheSettings.ShouldCacheCheckIterators())
	})

	t.Run("query_cache_disabled_iterator_cache_enabled", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(memory.New()),
			WithCheckQueryCacheEnabled(false),
			WithCheckQueryCacheTTL(1*time.Minute),
			WithCheckCacheLimit(10),
			WithCheckIteratorCacheEnabled(true),
			WithCheckIteratorCacheMaxResults(10),
		)
		t.Cleanup(s.Close)

		require.NotNil(t, s.sharedDatastoreResources.CheckCache)
	})

	t.Run("query_cache_enabled_iterator_cache_disabled", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(memory.New()),
			WithCheckQueryCacheEnabled(true),
			WithCheckQueryCacheTTL(1*time.Minute),
			WithCheckCacheLimit(10),
			WithCheckIteratorCacheEnabled(false),
			WithCheckIteratorCacheMaxResults(10),
		)
		t.Cleanup(s.Close)

		require.NotNil(t, s.sharedDatastoreResources.CheckCache)
	})

	t.Run("query_cache_disabled_iterator_cache_disabled", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(memory.New()),
			WithCheckQueryCacheEnabled(false),
			WithCheckQueryCacheTTL(1*time.Minute),
			WithCheckCacheLimit(10),
			WithCheckIteratorCacheEnabled(false),
			WithCheckIteratorCacheMaxResults(10),
		)
		t.Cleanup(s.Close)

		require.Nil(t, s.sharedDatastoreResources.CheckCache)
	})
}

func TestServerListObjectsCache(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("list_objects_cache_enabled", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(memory.New()),
			WithListObjectsIteratorCacheEnabled(true),
		)
		t.Cleanup(s.Close)

		require.NotNil(t, s.sharedDatastoreResources.CheckCache)
		require.True(t, s.cacheSettings.ShouldCacheListObjectsIterators())
		require.False(t, s.cacheSettings.ShouldCacheCheckQueries())
		require.False(t, s.cacheSettings.ShouldCacheCheckIterators())
	})

	t.Run("all_caches_enabled", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(memory.New()),
			WithCheckQueryCacheEnabled(true),
			WithCheckQueryCacheTTL(1*time.Minute),
			WithCheckCacheLimit(10),
			WithCheckIteratorCacheEnabled(true),
			WithCheckIteratorCacheMaxResults(10),
			WithListObjectsIteratorCacheEnabled(true),
		)
		t.Cleanup(s.Close)

		require.NotNil(t, s.sharedDatastoreResources.CheckCache)
		require.True(t, s.cacheSettings.ShouldCacheListObjectsIterators())
		require.True(t, s.cacheSettings.ShouldCacheCheckQueries())
		require.True(t, s.cacheSettings.ShouldCacheCheckIterators())
	})

	t.Run("no_caches_enabled", func(t *testing.T) {
		s := MustNewServerWithOpts(WithDatastore(memory.New()))
		t.Cleanup(s.Close)

		require.Nil(t, s.sharedDatastoreResources.CheckCache)
	})
}

func TestCheckWithCachedControllerEnabled(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("cache_controller_is_noop", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
		s := MustNewServerWithOpts(
			WithDatastore(mockDatastore),
			WithCacheControllerEnabled(true),
		)

		t.Cleanup(func() {
			mockDatastore.EXPECT().Close().Times(1)
			s.Close()
		})

		require.NotNil(t, s.sharedDatastoreResources.CacheController)
		_, ok := s.sharedDatastoreResources.CacheController.(*cachecontroller.NoopCacheController)
		require.True(t, ok)
	})

	t.Run("cache_controller_is_not_nil_if_check_query_cache_enabled", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
		s := MustNewServerWithOpts(
			WithDatastore(mockDatastore),
			WithCacheControllerEnabled(true),
			WithCheckQueryCacheEnabled(true),
		)

		t.Cleanup(func() {
			mockDatastore.EXPECT().Close().Times(1)
			s.Close()
		})

		require.NotNil(t, s.sharedDatastoreResources.CacheController)
	})

	t.Run("cache_controller_is_not_nil_if_check_iterator_cache_enabled", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
		s := MustNewServerWithOpts(
			WithDatastore(mockDatastore),
			WithCacheControllerEnabled(true),
			WithCheckIteratorCacheEnabled(true),
		)

		t.Cleanup(func() {
			mockDatastore.EXPECT().Close().Times(1)
			s.Close()
		})

		require.NotNil(t, s.sharedDatastoreResources.CacheController)
	})
}

func TestCheckWithCachedIterator(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()

	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	typedefs := parser.MustTransformDSLToProto(`
		model
			schema 1.1
		type user
		type company
			relations
				define viewer: [user]
		type license
			relations
				define viewer: [user, company#viewer]`).GetTypeDefinitions()

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	mockDatastore.EXPECT().
		ReadAuthorizationModel(gomock.Any(), storeID, modelID).
		Times(1).
		Return(&openfgav1.AuthorizationModel{
			SchemaVersion:   typesystem.SchemaVersion1_1,
			TypeDefinitions: typedefs,
			Id:              modelID,
		}, nil)

	mockDatastore.EXPECT().
		ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(
			func(_ context.Context, _ string, tk *openfgav1.TupleKey, _ storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
				if tk.GetObject() == "company:1" {
					return &openfgav1.Tuple{
						Key:       tk,
						Timestamp: timestamppb.Now(),
					}, nil
				}

				return nil, storage.ErrNotFound
			})

	mockDatastore.EXPECT().
		ReadUsersetTuples(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
		Times(1).
		Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
			{
				Key:       tuple.NewTupleKey("license:1", "viewer", "company:1#viewer"),
				Timestamp: timestamppb.Now(),
			},
		}), nil)

	mockDatastore.EXPECT().
		ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
		Times(1).
		Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
			{
				Key:       tuple.NewTupleKey("company:1", "viewer", "user:1"),
				Timestamp: timestamppb.Now(),
			},
		}), nil)

	s := MustNewServerWithOpts(
		WithContext(ctx),
		WithDatastore(mockDatastore),
		WithCheckCacheLimit(10),
		WithCheckQueryCacheTTL(1*time.Minute),
		WithCheckIteratorCacheEnabled(true),
		WithCheckIteratorCacheMaxResults(10),
	)

	t.Cleanup(func() {
		mockDatastore.EXPECT().Close().Times(1)
		s.Close()
	})

	checkResponse, err := s.Check(ctx, &openfgav1.CheckRequest{
		StoreId:              storeID,
		TupleKey:             tuple.NewCheckRequestTupleKey("license:1", "viewer", "user:1"),
		AuthorizationModelId: modelID,
	})

	require.NoError(t, err)
	require.True(t, checkResponse.GetAllowed())

	// Sleep for a while to ensure that the iterator is cached
	time.Sleep(1 * time.Millisecond)

	mockDatastore.EXPECT().
		ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
		Times(1).
		Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
			{
				Key:       tuple.NewTupleKey("company:1", "viewer", "user:2"),
				Timestamp: timestamppb.Now(),
			},
		}), nil)

	// If we check for the same request, data should come from cached iterator and number of ReadUsersetTuples should still be 1
	checkResponse, err = s.Check(ctx, &openfgav1.CheckRequest{
		StoreId:              storeID,
		TupleKey:             tuple.NewCheckRequestTupleKey("license:1", "viewer", "user:2"),
		AuthorizationModelId: modelID,
	})

	require.NoError(t, err)
	require.True(t, checkResponse.GetAllowed())
}

func TestBatchCheckWithCachedIterator(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()

	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	typedefs := parser.MustTransformDSLToProto(`
		model
			schema 1.1
		type user
		type company
			relations
				define viewer: [user]
		type license
			relations
				define viewer: [user, company#viewer]`).GetTypeDefinitions()

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	mockDatastore.EXPECT().
		ReadAuthorizationModel(gomock.Any(), storeID, modelID).
		Times(1).
		Return(&openfgav1.AuthorizationModel{
			SchemaVersion:   typesystem.SchemaVersion1_1,
			TypeDefinitions: typedefs,
			Id:              modelID,
		}, nil)

	mockDatastore.EXPECT().
		ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(
			func(_ context.Context, _ string, tk *openfgav1.TupleKey, _ storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
				if tk.GetObject() == "company:1" {
					return &openfgav1.Tuple{
						Key:       tk,
						Timestamp: timestamppb.Now(),
					}, nil
				}

				return nil, storage.ErrNotFound
			})

	mockDatastore.EXPECT().
		ReadUsersetTuples(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
		Times(1).
		Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
			{
				Key:       tuple.NewTupleKey("license:1", "viewer", "company:1#viewer"),
				Timestamp: timestamppb.Now(),
			},
		}), nil)

	mockDatastore.EXPECT().
		ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
		Times(1).
		Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
			{
				Key:       tuple.NewTupleKey("company:1", "viewer", "user:1"),
				Timestamp: timestamppb.Now(),
			},
		}), nil)

	s := MustNewServerWithOpts(
		WithContext(ctx),
		WithDatastore(mockDatastore),
		WithCheckCacheLimit(10),
		WithCheckQueryCacheTTL(1*time.Minute),
		WithCheckIteratorCacheEnabled(true),
		WithCheckIteratorCacheMaxResults(10),
	)

	t.Cleanup(func() {
		mockDatastore.EXPECT().Close().Times(1)
		s.Close()
	})

	fakeID := "abc123"
	batchCheckResponse, err := s.BatchCheck(ctx, &openfgav1.BatchCheckRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Checks: []*openfgav1.BatchCheckItem{
			{
				TupleKey: &openfgav1.CheckRequestTupleKey{
					User:     "user:1",
					Relation: "viewer",
					Object:   "license:1",
				},
				CorrelationId: fakeID,
			},
		},
	})

	require.NoError(t, err)
	require.True(t, batchCheckResponse.GetResult()[fakeID].GetAllowed())

	// Sleep for a while to ensure that the iterator is cached
	time.Sleep(1 * time.Millisecond)

	mockDatastore.EXPECT().
		ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
		Times(1).
		Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
			{
				Key:       tuple.NewTupleKey("company:1", "viewer", "user:2"),
				Timestamp: timestamppb.Now(),
			},
		}), nil)

	// If we check for the same request, data should come from cached iterator and number of ReadUsersetTuples should still be 1
	batchCheckResponse, err = s.BatchCheck(ctx, &openfgav1.BatchCheckRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Checks: []*openfgav1.BatchCheckItem{
			{
				TupleKey: &openfgav1.CheckRequestTupleKey{
					User:     "user:2", // New user
					Relation: "viewer",
					Object:   "license:1",
				},
				CorrelationId: fakeID,
			},
		},
	})

	require.NoError(t, err)
	require.True(t, batchCheckResponse.GetResult()[fakeID].GetAllowed())
}
