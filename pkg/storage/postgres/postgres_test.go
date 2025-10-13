package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	"github.com/openfga/openfga/pkg/storage/test"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestPostgresDatastore(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI(true)
	cfg := sqlcommon.NewConfig()
	ds, err := New(uri, cfg)
	require.NoError(t, err)
	defer ds.Close()
	test.RunAllTests(t, ds)

	// Run tests with a custom large max_tuples_per_write value.
	dsCustom, err := New(uri, sqlcommon.NewConfig(
		sqlcommon.WithMaxTuplesPerWrite(5000),
	))
	require.NoError(t, err)
	defer dsCustom.Close()

	t.Run("WriteTuplesWithMaxTuplesPerWrite", test.WriteTuplesWithMaxTuplesPerWrite(dsCustom, context.Background()))
}

func TestPostgresDatastoreStartup(t *testing.T) {
	primaryDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")
	primaryURI := primaryDatastore.GetConnectionURI(true)

	cfg := sqlcommon.NewConfig()
	cfg.ExportMetrics = true

	ds, err := New(primaryURI, cfg)
	require.NoError(t, err)
	defer ds.Close()

	status, err := ds.IsReady(context.Background())
	require.NoError(t, err)
	require.True(t, status.IsReady)
}

func TestPostgresDatastoreStatusWithSecondaryDB(t *testing.T) {
	primaryDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")
	err := primaryDatastore.CreateSecondary(t)
	require.NoError(t, err)

	primaryURI := primaryDatastore.GetConnectionURI(true)

	cfg := sqlcommon.NewConfig()
	cfg.SecondaryURI = primaryDatastore.GetSecondaryConnectionURI(true)
	cfg.ExportMetrics = true

	ds, err := New(primaryURI, cfg)
	require.NoError(t, err)
	defer ds.Close()

	status, err := ds.IsReady(context.Background())
	require.NoError(t, err)
	require.True(t, status.IsReady)
	require.Equal(t, "primary: ready, secondary: ready", status.Message)
}

func TestPostgresDatastoreAfterCloseIsNotReady(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI(true)
	cfg := sqlcommon.NewConfig()
	ds, err := New(uri, cfg)
	require.NoError(t, err)
	ds.Close()
	status, err := ds.IsReady(context.Background())
	require.Error(t, err)
	require.False(t, status.IsReady)
}

func TestParseConfig(t *testing.T) {
	defaultConfig, err := pgxpool.ParseConfig("postgres://default@localhost:9999/dbname")
	require.NoError(t, err)

	tests := []struct {
		name        string
		uri         string
		override    bool
		cfg         sqlcommon.Config
		expected    pgxpool.Config
		expectedErr bool
	}{
		{
			name:     "default_with_no_overrides",
			uri:      "postgres://abc:passwd@localhost:5346/dbname",
			override: false,
			cfg:      sqlcommon.Config{},
			expected: pgxpool.Config{
				ConnConfig: &pgx.ConnConfig{
					Config: pgconn.Config{
						User:     "abc",
						Password: "passwd",
						Host:     "localhost",
						Port:     5346,
						Database: "dbname",
					},
				},
				MinIdleConns:          defaultConfig.MinIdleConns,
				MaxConns:              defaultConfig.MaxConns,
				MinConns:              defaultConfig.MinConns,
				MaxConnIdleTime:       defaultConfig.MaxConnIdleTime,
				MaxConnLifetimeJitter: defaultConfig.MaxConnLifetimeJitter,
				MaxConnLifetime:       defaultConfig.MaxConnLifetime,
			},
		},
		{
			name:     "override_username_and_password",
			uri:      "postgres://abc:passwd@localhost:5346/dbname",
			override: true,
			cfg: sqlcommon.Config{
				Username:        "override_user",
				Password:        "override_passwd",
				MinIdleConns:    10,
				MaxOpenConns:    50,
				MinOpenConns:    15,
				ConnMaxLifetime: time.Minute * 20,
				ConnMaxIdleTime: 1 * time.Minute,
			},
			expected: pgxpool.Config{
				ConnConfig: &pgx.ConnConfig{
					Config: pgconn.Config{
						User:     "override_user",
						Password: "override_passwd",
						Host:     "localhost",
						Port:     5346,
						Database: "dbname",
					},
				},
				MinIdleConns:          10,
				MaxConns:              50,
				MinConns:              15,
				MaxConnIdleTime:       1 * time.Minute,
				MaxConnLifetimeJitter: 2 * time.Minute,
				MaxConnLifetime:       20 * time.Minute,
			},
		},
		{
			name:     "override_password_only",
			uri:      "postgres://abc:passwd@localhost:5346/dbname",
			override: true,
			cfg: sqlcommon.Config{
				Username:     "",
				Password:     "override_passwd",
				MinIdleConns: 10,
				MaxOpenConns: 50,
				MinOpenConns: 15,
			},
			expected: pgxpool.Config{
				ConnConfig: &pgx.ConnConfig{
					Config: pgconn.Config{
						User:     "abc",
						Password: "override_passwd",
						Host:     "localhost",
						Port:     5346,
						Database: "dbname",
					},
				},
				MinIdleConns:          10,
				MaxConns:              50,
				MinConns:              15,
				MaxConnIdleTime:       defaultConfig.MaxConnIdleTime,
				MaxConnLifetimeJitter: defaultConfig.MaxConnLifetimeJitter,
				MaxConnLifetime:       defaultConfig.MaxConnLifetime,
			},
		},
		{
			name:     "override_username_only",
			uri:      "postgres://abc:passwd@localhost:5346/dbname",
			override: true,
			cfg: sqlcommon.Config{
				Username:     "override_user",
				Password:     "",
				MinIdleConns: 10,
				MaxOpenConns: 50,
				MinOpenConns: 15,
			},
			expected: pgxpool.Config{
				ConnConfig: &pgx.ConnConfig{
					Config: pgconn.Config{
						User:     "override_user",
						Password: "passwd",
						Host:     "localhost",
						Port:     5346,
						Database: "dbname",
					},
				},
				MinIdleConns:          10,
				MaxConns:              50,
				MinConns:              15,
				MaxConnIdleTime:       defaultConfig.MaxConnIdleTime,
				MaxConnLifetimeJitter: defaultConfig.MaxConnLifetimeJitter,
				MaxConnLifetime:       defaultConfig.MaxConnLifetime,
			},
		},
		{
			name:        "bad_uri",
			uri:         "bad_uri",
			override:    true,
			expectedErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := parseConfig(tt.uri, tt.override, &tt.cfg)
			if tt.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)

				require.Equal(t, tt.expected.ConnConfig.Host, parsed.ConnConfig.Host)
				require.Equal(t, tt.expected.ConnConfig.Port, parsed.ConnConfig.Port)
				require.Equal(t, tt.expected.ConnConfig.Database, parsed.ConnConfig.Database)
				require.Equal(t, tt.expected.ConnConfig.User, parsed.ConnConfig.User)
				require.Equal(t, tt.expected.ConnConfig.Password, parsed.ConnConfig.Password)
				require.Equal(t, tt.expected.MaxConns, parsed.MaxConns)
				require.Equal(t, tt.expected.MinConns, parsed.MinConns)
				require.Equal(t, tt.expected.MaxConnIdleTime, parsed.MaxConnIdleTime)
				require.Equal(t, tt.expected.MaxConnLifetime, parsed.MaxConnLifetime)
				require.Equal(t, tt.expected.MaxConnLifetimeJitter, parsed.MaxConnLifetimeJitter)
				require.Equal(t, tt.expected.MaxConnIdleTime, parsed.MaxConnIdleTime)
			}
		})
	}
}

// mostly test various error scenarios.
func TestNewDB(t *testing.T) {
	t.Run("bad_uri", func(t *testing.T) {
		uri := "bad_uri"
		_, err := New(uri, &sqlcommon.Config{})
		require.Error(t, err)
	})
	t.Run("bad_secondary_uri", func(t *testing.T) {
		primaryDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")
		primaryURI := primaryDatastore.GetConnectionURI(true)
		_, err := New(primaryURI, &sqlcommon.Config{
			SecondaryURI: "bad_uri",
			Logger:       logger.NewNoopLogger(),
		})
		require.Error(t, err)
	})
	t.Run("cannot_ping_primary_uri", func(t *testing.T) {
		uri :=
			"postgres://abc:passwd@localhost:5346/dbname"
		cfg := sqlcommon.Config{
			Username:        "override_user",
			Password:        "override_passwd",
			MinIdleConns:    10,
			MaxOpenConns:    50,
			MinOpenConns:    15,
			ConnMaxLifetime: time.Minute * 20,
			ConnMaxIdleTime: 1 * time.Minute,
			Logger:          logger.NewNoopLogger(),
		}
		_, err := New(uri, &cfg)
		require.Error(t, err)
	})
}

func TestGetPgxPool(t *testing.T) {
	pool1 := &pgxpool.Pool{}
	pool2 := &pgxpool.Pool{}
	t.Run("high_consistency", func(t *testing.T) {
		ds := &Datastore{
			primaryDB:   pool1,
			secondaryDB: pool2,
		}
		dut := ds.getPgxPool(openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY)
		require.Equal(t, dut, pool1)
	})
	t.Run("min_latency", func(t *testing.T) {
		ds := &Datastore{
			primaryDB:   pool1,
			secondaryDB: pool2,
		}
		dut := ds.getPgxPool(openfgav1.ConsistencyPreference_MINIMIZE_LATENCY)
		require.Equal(t, dut, pool2)
	})
	t.Run("no_secondary_DB_specified", func(t *testing.T) {
		ds := &Datastore{
			primaryDB: pool1,
		}
		dut := ds.getPgxPool(openfgav1.ConsistencyPreference_MINIMIZE_LATENCY)
		require.Equal(t, dut, pool1)
	})
}

// TestReadEnsureNoOrder asserts that the read response is not ordered by ulid.
func TestReadEnsureNoOrder(t *testing.T) {
	tests := []struct {
		name  string
		mixed bool
	}{
		{
			name:  "nextOnly",
			mixed: false,
		},
		{
			name:  "mixed",
			mixed: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

			uri := testDatastore.GetConnectionURI(true)
			cfg := sqlcommon.NewConfig()
			ds, err := New(uri, cfg)
			require.NoError(t, err)
			defer ds.Close()

			ctx := context.Background()

			store := "store"
			firstTuple := tuple.NewTupleKey("doc:object_id_1", "relation", "user:user_1")
			secondTuple := tuple.NewTupleKey("doc:object_id_2", "relation", "user:user_2")
			thirdTuple := tuple.NewTupleKey("doc:object_id_3", "relation", "user:user_3")

			err = ds.write(ctx,
				store,
				[]*openfgav1.TupleKeyWithoutCondition{},
				[]*openfgav1.TupleKey{firstTuple},
				storage.NewTupleWriteOptions(),
				time.Now())
			require.NoError(t, err)

			// Tweak time so that ULID is smaller.
			err = ds.write(ctx,
				store,
				[]*openfgav1.TupleKeyWithoutCondition{},
				[]*openfgav1.TupleKey{secondTuple},
				storage.NewTupleWriteOptions(),
				time.Now().Add(time.Minute*-1))
			require.NoError(t, err)

			err = ds.write(ctx,
				store,
				[]*openfgav1.TupleKeyWithoutCondition{},
				[]*openfgav1.TupleKey{thirdTuple},
				storage.NewTupleWriteOptions(),
				time.Now().Add(time.Minute*-2))
			require.NoError(t, err)

			iter, err := ds.Read(ctx, store, tuple.
				NewTupleKey("doc:", "relation", ""), storage.ReadOptions{})
			defer iter.Stop()
			require.NoError(t, err)

			// We expect that objectID1 will return first because it is inserted first.
			if tt.mixed {
				curTuple, err := iter.Head(ctx)
				require.NoError(t, err)
				require.Equal(t, firstTuple, curTuple.GetKey())

				// calling head should not move the item
				curTuple, err = iter.Head(ctx)
				require.NoError(t, err)
				require.Equal(t, firstTuple, curTuple.GetKey())
			}

			curTuple, err := iter.Next(ctx)
			require.NoError(t, err)
			require.Equal(t, firstTuple, curTuple.GetKey())

			curTuple, err = iter.Next(ctx)
			require.NoError(t, err)
			require.Equal(t, secondTuple, curTuple.GetKey())

			if tt.mixed {
				curTuple, err = iter.Head(ctx)
				require.NoError(t, err)
				require.Equal(t, thirdTuple, curTuple.GetKey())
			}

			curTuple, err = iter.Next(ctx)
			require.NoError(t, err)
			require.Equal(t, thirdTuple, curTuple.GetKey())

			if tt.mixed {
				_, err = iter.Head(ctx)
				require.ErrorIs(t, err, storage.ErrIteratorDone)
			}

			_, err = iter.Next(ctx)
			require.ErrorIs(t, err, storage.ErrIteratorDone)
		})
	}
}

func TestCtxCancel(t *testing.T) {
	tests := []struct {
		name  string
		mixed bool
	}{
		{
			name:  "nextOnly",
			mixed: false,
		},
		{
			name:  "mixed",
			mixed: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

			uri := testDatastore.GetConnectionURI(true)
			cfg := sqlcommon.NewConfig()
			ds, err := New(uri, cfg)
			require.NoError(t, err)
			defer ds.Close()

			ctx, cancel := context.WithCancel(context.Background())

			store := "store"
			firstTuple := tuple.NewTupleKey("doc:object_id_1", "relation", "user:user_1")
			secondTuple := tuple.NewTupleKey("doc:object_id_2", "relation", "user:user_2")
			thirdTuple := tuple.NewTupleKey("doc:object_id_3", "relation", "user:user_3")

			err = ds.write(ctx,
				store,
				[]*openfgav1.TupleKeyWithoutCondition{},
				[]*openfgav1.TupleKey{firstTuple},
				storage.NewTupleWriteOptions(),
				time.Now())
			require.NoError(t, err)

			// Tweak time so that ULID is smaller.
			err = ds.write(ctx,
				store,
				[]*openfgav1.TupleKeyWithoutCondition{},
				[]*openfgav1.TupleKey{secondTuple},
				storage.NewTupleWriteOptions(),
				time.Now().Add(time.Minute*-1))
			require.NoError(t, err)

			err = ds.write(ctx,
				store,
				[]*openfgav1.TupleKeyWithoutCondition{},
				[]*openfgav1.TupleKey{thirdTuple},
				storage.NewTupleWriteOptions(),
				time.Now().Add(time.Minute*-2))
			require.NoError(t, err)

			iter, err := ds.Read(ctx, store, tuple.
				NewTupleKey("doc:", "relation", ""), storage.ReadOptions{})
			defer iter.Stop()
			require.NoError(t, err)

			cancel()
			if tt.mixed {
				_, err = iter.Head(ctx)
				require.Error(t, err)
				require.NotEqual(t, storage.ErrIteratorDone, err)
			}

			_, err = iter.Next(ctx)
			require.Error(t, err)
			require.NotEqual(t, storage.ErrIteratorDone, err)
		})
	}
}

// TestReadPageEnsureNoOrder asserts that the read page is ordered by ulid.
func TestReadPageEnsureOrder(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI(true)
	cfg := sqlcommon.NewConfig()
	ds, err := New(uri, cfg)
	require.NoError(t, err)
	defer ds.Close()

	ctx := context.Background()

	store := "store"
	firstTuple := tuple.NewTupleKey("doc:object_id_1", "relation", "user:user_1")
	secondTuple := tuple.NewTupleKey("doc:object_id_2", "relation", "user:user_2")

	err = ds.write(ctx,
		store,
		[]*openfgav1.TupleKeyWithoutCondition{},
		[]*openfgav1.TupleKey{firstTuple},
		storage.NewTupleWriteOptions(),
		time.Now())
	require.NoError(t, err)

	// Tweak time so that ULID is smaller.
	err = ds.write(ctx,
		store,
		[]*openfgav1.TupleKeyWithoutCondition{},
		[]*openfgav1.TupleKey{secondTuple},
		storage.NewTupleWriteOptions(),
		time.Now().Add(time.Minute*-1))
	require.NoError(t, err)

	opts := storage.ReadPageOptions{
		Pagination: storage.NewPaginationOptions(storage.DefaultPageSize, ""),
	}
	tuples, _, err := ds.ReadPage(ctx,
		store,
		tuple.NewTupleKey("doc:", "relation", ""),
		opts)
	require.NoError(t, err)

	require.Len(t, tuples, 2)
	// We expect that objectID2 will return first because it has a smaller ulid.
	require.Equal(t, secondTuple, tuples[0].GetKey())
	require.Equal(t, firstTuple, tuples[1].GetKey())
}

func TestReadAuthorizationModelUnmarshallError(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI(true)
	cfg := sqlcommon.NewConfig()
	ds, err := New(uri, cfg)
	require.NoError(t, err)

	ctx := context.Background()
	defer ds.Close()
	store := "store"
	modelID := "foo"
	schemaVersion := typesystem.SchemaVersion1_0

	bytes, err := proto.Marshal(&openfgav1.TypeDefinition{Type: "document"})
	require.NoError(t, err)
	pbdata := []byte{0x01, 0x02, 0x03}

	_, err = ds.primaryDB.Exec(ctx, "INSERT INTO authorization_model (store, authorization_model_id, schema_version, type, type_definition, serialized_protobuf) VALUES ($1, $2, $3, $4, $5, $6)", store, modelID, schemaVersion, "document", bytes, pbdata)
	require.NoError(t, err)

	_, err = ds.ReadAuthorizationModel(ctx, store, modelID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot parse invalid wire-format data")
}

func TestReadAuthorizationModelReturnValue(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI(true)
	cfg := sqlcommon.NewConfig()
	ds, err := New(uri, cfg)
	require.NoError(t, err)

	ctx := context.Background()
	defer ds.Close()
	store := "store"
	modelID := "foo"
	schemaVersion := typesystem.SchemaVersion1_0

	bytes, err := proto.Marshal(&openfgav1.TypeDefinition{Type: "document"})
	require.NoError(t, err)

	_, err = ds.primaryDB.Exec(ctx, "INSERT INTO authorization_model (store, authorization_model_id, schema_version, type, type_definition, serialized_protobuf) VALUES ($1, $2, $3, $4, $5, $6)", store, modelID, schemaVersion, "document", bytes, nil)

	require.NoError(t, err)

	res, err := ds.ReadAuthorizationModel(ctx, store, modelID)

	require.NoError(t, err)
	// AuthorizationModel should return only 1 type which is of type "document"
	require.Len(t, res.GetTypeDefinitions(), 1)
	require.Equal(t, "document", res.GetTypeDefinitions()[0].GetType())
}

func TestFindLatestModel(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI(true)
	cfg := sqlcommon.NewConfig()
	ds, err := New(uri, cfg)
	require.NoError(t, err)
	defer ds.Close()

	ctx := context.Background()
	store := ulid.Make().String()

	schemaVersion := typesystem.SchemaVersion1_1

	t.Run("works_when_first_model_is_one_row_and_second_model_is_split", func(t *testing.T) {
		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user1`)
		err := ds.WriteAuthorizationModel(ctx, store, model)
		require.NoError(t, err)

		latestModel, err := ds.FindLatestAuthorizationModel(ctx, store)
		require.NoError(t, err)
		require.Len(t, latestModel.GetTypeDefinitions(), 1)

		modelID := ulid.Make().String()
		// write type "document"
		bytesDocumentType, err := proto.Marshal(&openfgav1.TypeDefinition{Type: "document"})
		require.NoError(t, err)
		_, err = ds.primaryDB.Exec(ctx, "INSERT INTO authorization_model (store, authorization_model_id, schema_version, type, type_definition, serialized_protobuf) VALUES ($1, $2, $3, $4, $5, $6)",
			store, modelID, schemaVersion, "document", bytesDocumentType, nil)
		require.NoError(t, err)

		// write type "user"
		bytesUserType, err := proto.Marshal(&openfgav1.TypeDefinition{Type: "user"})
		require.NoError(t, err)
		_, err = ds.primaryDB.Exec(ctx, "INSERT INTO authorization_model (store, authorization_model_id, schema_version, type, type_definition, serialized_protobuf) VALUES ($1, $2, $3, $4, $5, $6)",
			store, modelID, schemaVersion, "user", bytesUserType, nil)
		require.NoError(t, err)

		latestModel, err = ds.FindLatestAuthorizationModel(ctx, store)
		require.NoError(t, err)
		require.Len(t, latestModel.GetTypeDefinitions(), 2)
	})

	t.Run("works_when_first_model_is_split_and_second_model_is_one_row", func(t *testing.T) {
		modelID := ulid.Make().String()
		// write type "document"
		bytesDocumentType, err := proto.Marshal(&openfgav1.TypeDefinition{Type: "document"})
		require.NoError(t, err)
		_, err = ds.primaryDB.Exec(ctx, "INSERT INTO authorization_model (store, authorization_model_id, schema_version, type, type_definition, serialized_protobuf) VALUES ($1, $2, $3, $4, $5, $6)",
			store, modelID, schemaVersion, "document", bytesDocumentType, nil)
		require.NoError(t, err)

		// write type "user"
		bytesUserType, err := proto.Marshal(&openfgav1.TypeDefinition{Type: "user"})
		require.NoError(t, err)
		_, err = ds.primaryDB.Exec(ctx, "INSERT INTO authorization_model (store, authorization_model_id, schema_version, type, type_definition, serialized_protobuf) VALUES ($1, $2, $3, $4, $5, $6)",
			store, modelID, schemaVersion, "user", bytesUserType, nil)
		require.NoError(t, err)

		latestModel, err := ds.FindLatestAuthorizationModel(ctx, store)
		require.NoError(t, err)
		require.Len(t, latestModel.GetTypeDefinitions(), 2)

		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user1`)
		err = ds.WriteAuthorizationModel(ctx, store, model)
		require.NoError(t, err)

		latestModel, err = ds.FindLatestAuthorizationModel(ctx, store)
		require.NoError(t, err)
		require.Len(t, latestModel.GetTypeDefinitions(), 1)
	})
}

// TestAllowNullCondition tests that tuple and changelog rows existing before
// migration 005_add_conditions_to_tuples can be successfully read.
func TestAllowNullCondition(t *testing.T) {
	ctx := context.Background()
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI(true)
	cfg := sqlcommon.NewConfig()
	ds, err := New(uri, cfg)
	require.NoError(t, err)
	defer ds.Close()

	stmt := `
		INSERT INTO tuple (
			store, object_type, object_id, relation, _user, user_type, ulid,
			condition_name, condition_context, inserted_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW());
	`
	_, err = ds.primaryDB.Exec(
		ctx, stmt, "store", "folder", "2021-budget", "owner", "user:anne", "user",
		ulid.Make().String(), nil, nil,
	)
	require.NoError(t, err)

	tk := tuple.NewTupleKey("folder:2021-budget", "owner", "user:anne")
	iter, err := ds.Read(ctx, "store", tk, storage.ReadOptions{})
	require.NoError(t, err)
	defer iter.Stop()

	curTuple, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, tk, curTuple.GetKey())

	opts := storage.ReadPageOptions{
		Pagination: storage.NewPaginationOptions(2, ""),
	}
	tuples, _, err := ds.ReadPage(ctx, "store", &openfgav1.TupleKey{}, opts)
	require.NoError(t, err)
	require.Len(t, tuples, 1)
	require.Equal(t, tk, tuples[0].GetKey())

	userTuple, err := ds.ReadUserTuple(ctx, "store", tk, storage.ReadUserTupleOptions{})
	require.NoError(t, err)
	require.Equal(t, tk, userTuple.GetKey())

	tk2 := tuple.NewTupleKey("folder:2022-budget", "viewer", "user:anne")
	_, err = ds.primaryDB.Exec(
		ctx, stmt, "store", "folder", "2022-budget", "viewer", "user:anne", "userset",
		ulid.Make().String(), nil, nil,
	)

	require.NoError(t, err)
	iter, err = ds.ReadUsersetTuples(ctx, "store", storage.ReadUsersetTuplesFilter{Object: "folder:2022-budget"}, storage.ReadUsersetTuplesOptions{})
	require.NoError(t, err)
	defer iter.Stop()

	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, tk2, curTuple.GetKey())

	iter, err = ds.ReadStartingWithUser(ctx, "store", storage.ReadStartingWithUserFilter{
		ObjectType: "folder",
		Relation:   "owner",
		UserFilter: []*openfgav1.ObjectRelation{
			{Object: "user:anne"},
		},
	}, storage.ReadStartingWithUserOptions{})
	require.NoError(t, err)
	defer iter.Stop()

	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, tk, curTuple.GetKey())

	stmt = `
	INSERT INTO changelog (
		store, object_type, object_id, relation, _user, ulid,
		condition_name, condition_context, inserted_at, operation
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), $9);
`
	_, err = ds.primaryDB.Exec(
		ctx, stmt, "store", "folder", "2021-budget", "owner", "user:anne",
		ulid.Make().String(), nil, nil, openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
	)
	require.NoError(t, err)

	_, err = ds.primaryDB.Exec(
		ctx, stmt, "store", "folder", "2021-budget", "owner", "user:anne",
		ulid.Make().String(), nil, nil, openfgav1.TupleOperation_TUPLE_OPERATION_DELETE,
	)
	require.NoError(t, err)

	readChangesOpts := storage.ReadChangesOptions{
		Pagination: storage.NewPaginationOptions(storage.DefaultPageSize, ""),
	}
	changes, _, err := ds.ReadChanges(ctx, "store", storage.ReadChangesFilter{ObjectType: "folder"}, readChangesOpts)
	require.NoError(t, err)
	require.Len(t, changes, 2)
	require.Equal(t, tk, changes[0].GetTupleKey())
	require.Equal(t, tk, changes[1].GetTupleKey())
}

// TestMarshalledAssertions tests that previously persisted marshalled
// assertions can be read back. In any case where the Assertions proto model
// needs to change, we'll likely need to introduce a series of data migrations.
func TestMarshalledAssertions(t *testing.T) {
	ctx := context.Background()
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI(true)
	cfg := sqlcommon.NewConfig()
	ds, err := New(uri, cfg)
	require.NoError(t, err)
	defer ds.Close()

	// Note: this represents an assertion written on v1.3.7.
	stmt := `
		INSERT INTO assertion (
			store, authorization_model_id, assertions
		) VALUES ($1, $2, DECODE('0a2b0a270a12666f6c6465723a323032312d62756467657412056f776e65721a0a757365723a616e6e657a1001','hex'));
	`
	_, err = ds.primaryDB.Exec(ctx, stmt, "store", "model")
	require.NoError(t, err)

	assertions, err := ds.ReadAssertions(ctx, "store", "model")
	require.NoError(t, err)

	expectedAssertions := []*openfgav1.Assertion{
		{
			TupleKey: &openfgav1.AssertionTupleKey{
				Object:   "folder:2021-budget",
				Relation: "owner",
				User:     "user:annez",
			},
			Expectation: true,
		},
	}
	require.Equal(t, expectedAssertions, assertions)
}

func TestHandleSQLError(t *testing.T) {
	t.Run("duplicate_key_value_error_with_tuple_key_wraps_ErrInvalidWriteInput", func(t *testing.T) {
		err := HandleSQLError(errors.New("duplicate key value"), &openfgav1.TupleKey{
			Object:   "object",
			Relation: "relation",
			User:     "user",
		})
		require.ErrorIs(t, err, storage.ErrInvalidWriteInput)
	})

	t.Run("duplicate_key_value_error_without_tuple_key_returns_collision", func(t *testing.T) {
		duplicateKeyError := errors.New("duplicate key value")
		err := HandleSQLError(duplicateKeyError)
		require.ErrorIs(t, err, storage.ErrCollision)
	})

	t.Run("sql.ErrNoRows_is_converted_to_storage.ErrNotFound_error", func(t *testing.T) {
		err := HandleSQLError(sql.ErrNoRows)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})
}
