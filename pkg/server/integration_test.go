//go:build integration

package server

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	storageTest "github.com/openfga/openfga/pkg/storage/test"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"

	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/cmd/migrate"
	"github.com/openfga/openfga/cmd/util"
	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/pkg/server/test"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/mysql"
	"github.com/openfga/openfga/pkg/storage/postgres"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

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

func TestCheckWithCachedIterator(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()

	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	model := parser.MustTransformDSLToProto(`
		model
			schema 1.1
		type user
		type company
			relations
				define viewer: [user]
		type license
			relations
				define viewer: [user, company#viewer]
	`)

	model.Id = modelID

	ds := memory.New()

	err := ds.WriteAuthorizationModel(context.Background(), storeID, model)
	require.NoError(t, err)

	tuples := []*openfgav1.TupleKey{
		tuple.NewTupleKey("license:1", "viewer", "company:1#viewer"),
		tuple.NewTupleKey("company:1", "viewer", "user:1"),
		tuple.NewTupleKey("company:1", "viewer", "user:2"),
	}

	err = ds.Write(context.Background(), storeID, nil, tuples)
	require.NoError(t, err)

	cache := storageTest.NewMapCache()

	s := MustNewServerWithOpts(
		WithContext(ctx),
		WithDatastore(ds),
		WithCheckCacheLimit(10),
		WithCheckCache(cache),
		WithCheckQueryCacheTTL(1*time.Minute),
		WithCheckIteratorCacheEnabled(true),
		WithCheckIteratorCacheMaxResults(10),
	)

	t.Cleanup(func() {
		s.Close()
	})

	checkResponse, err := s.Check(ctx, &openfgav1.CheckRequest{
		StoreId:              storeID,
		TupleKey:             tuple.NewCheckRequestTupleKey("license:1", "viewer", "user:1"),
		AuthorizationModelId: modelID,
	})

	require.NoError(t, err)
	require.True(t, checkResponse.GetAllowed())
	require.Equal(t, 0, cache.Hits())

	// Sleep for a while to ensure that the iterator is cached
	time.Sleep(100 * time.Millisecond)

	checkResponse, err = s.Check(ctx, &openfgav1.CheckRequest{
		StoreId:              storeID,
		TupleKey:             tuple.NewCheckRequestTupleKey("license:1", "viewer", "user:2"),
		AuthorizationModelId: modelID,
	})

	require.NoError(t, err)
	require.True(t, checkResponse.GetAllowed())
	// Check cache must have been called and found a result for 'company:1#viewer'.
	require.Equal(t, 1, cache.Hits())
}

func TestBatchCheckWithCachedIterator(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()

	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	model := parser.MustTransformDSLToProto(`
		model
			schema 1.1
		type user
		type company
			relations
				define viewer: [user]
		type license
			relations
				define viewer: [user, company#viewer]
	`)

	model.Id = modelID

	ds := memory.New()

	err := ds.WriteAuthorizationModel(context.Background(), storeID, model)
	require.NoError(t, err)

	tuples := []*openfgav1.TupleKey{
		tuple.NewTupleKey("license:1", "viewer", "company:1#viewer"),
		tuple.NewTupleKey("company:1", "viewer", "user:1"),
		tuple.NewTupleKey("company:1", "viewer", "user:2"),
	}

	err = ds.Write(context.Background(), storeID, nil, tuples)
	require.NoError(t, err)

	cache := storageTest.NewMapCache()

	s := MustNewServerWithOpts(
		WithContext(ctx),
		WithDatastore(ds),
		WithCheckCache(cache),
		WithCheckCacheLimit(10),
		WithCheckQueryCacheTTL(1*time.Minute),
		WithCheckIteratorCacheEnabled(true),
		WithCheckIteratorCacheMaxResults(10),
	)

	t.Cleanup(func() {
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
	require.Equal(t, 0, cache.Hits())

	// Sleep for a while to ensure that the iterator is cached
	time.Sleep(100 * time.Millisecond)

	// If we check for the same request, data should come from cached iterator.
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
	require.Equal(t, 1, cache.Hits())
}
