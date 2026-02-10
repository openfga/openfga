package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	"github.com/openfga/openfga/pkg/storage/test"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/testutils"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

type MemoryPassfileProvider struct {
	Content    string
	ReadCloser io.ReadCloser
	Err        error
}

type MemFile struct {
	Content  io.ReadCloser
	ModeBits os.FileMode
	StatErr  error
}

func (m *MemFile) Name() string {
	panic("unimplemented")
}

func (m *MemFile) Size() int64 {
	panic("unimplemented")
}

func (m *MemFile) Mode() fs.FileMode {
	return m.ModeBits
}

func (m *MemFile) ModTime() time.Time {
	panic("unimplemented")
}

func (m *MemFile) IsDir() bool {
	panic("unimplemented")
}

func (m *MemFile) Sys() any {
	panic("unimplemented")
}

func (m *MemFile) Read(p []byte) (n int, err error) {
	return m.Content.Read(p)
}

func (m *MemFile) Close() error {
	return m.Content.Close()
}

func (m *MemFile) Stat() (os.FileInfo, error) {
	return m, m.StatErr
}

func (p *MemoryPassfileProvider) OpenPassfile() (io.ReadCloser, error) {
	if p.ReadCloser != nil {
		return p.ReadCloser, p.Err
	}
	return io.NopCloser(strings.NewReader(p.Content)), p.Err
}

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
			cfg: sqlcommon.Config{
				Logger: logger.NewNoopLogger(),
			},
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
				BeforeConnect:         nil,
			},
		},
		{
			name:     "override_username_and_password",
			uri:      "postgres://abc:passwd@localhost:5346/dbname",
			override: true,
			cfg: sqlcommon.Config{
				Logger:          logger.NewNoopLogger(),
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
				BeforeConnect:         nil,
			},
		},
		{
			name:     "override_password_only",
			uri:      "postgres://abc:passwd@localhost:5346/dbname",
			override: true,
			cfg: sqlcommon.Config{
				Logger:       logger.NewNoopLogger(),
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
				BeforeConnect:         nil,
			},
		},
		{
			name:     "override_username_only",
			uri:      "postgres://abc:passwd@localhost:5346/dbname",
			override: true,
			cfg: sqlcommon.Config{
				Logger:       logger.NewNoopLogger(),
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
				BeforeConnect:         nil,
			},
		},
		{
			name: "bad_uri",
			uri:  "bad_uri",
			cfg: sqlcommon.Config{
				Logger: logger.NewNoopLogger(),
			},
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
				require.NotNil(t, parsed.BeforeConnect)
			}
		})
	}
}

func TestBeforeConnectHook(t *testing.T) {
	noOpLogger := logger.NewNoopLogger()
	t.Run("sets the password from the file", func(t *testing.T) {
		provider := &MemoryPassfileProvider{"*:*:*:*:password", nil, nil}
		hook := createBeforeConnect(noOpLogger, provider)
		ctx := context.Background()
		config := new(pgx.ConnConfig)
		err := hook(ctx, config)
		require.NoError(t, err)
		require.Equal(t, "password", config.Password)

		// It updates the password upon change
		provider.Content = "*:*:*:*:secondpassword"
		err = hook(ctx, config)
		require.NoError(t, err)
		require.Equal(t, "secondpassword", config.Password)
	})
	t.Run("does not set the password if there is no file", func(t *testing.T) {
		provider := &MemoryPassfileProvider{"", nil, ErrNoPassfile}
		hook := createBeforeConnect(noOpLogger, provider)
		ctx := context.Background()
		config := new(pgx.ConnConfig)
		err := hook(ctx, config)
		require.NoError(t, err)
		require.Empty(t, config.Password)
	})
	t.Run("does not set the password if the file perms are too permissive", func(t *testing.T) {
		provider := &MemoryPassfileProvider{"", nil, ErrInsecurePassfilePermissions}
		hook := createBeforeConnect(noOpLogger, provider)
		ctx := context.Background()
		config := new(pgx.ConnConfig)
		err := hook(ctx, config)
		require.NoError(t, err)
		require.Empty(t, config.Password)
	})
	t.Run("does not set the password if the file is empty", func(t *testing.T) {
		provider := &MemoryPassfileProvider{"", nil, nil}
		hook := createBeforeConnect(noOpLogger, provider)
		ctx := context.Background()
		config := new(pgx.ConnConfig)
		err := hook(ctx, config)
		require.NoError(t, err)
		require.Empty(t, config.Password)
	})
	t.Run("the hook errors when the file cannot be read", func(t *testing.T) {
		provider := &MemoryPassfileProvider{"", nil, errors.New("cannot read file")}
		hook := createBeforeConnect(noOpLogger, provider)
		ctx := context.Background()
		config := new(pgx.ConnConfig)
		err := hook(ctx, config)
		require.ErrorContains(t, err, "cannot read file")
		require.Empty(t, config.Password)
	})
	t.Run("the hook does not set the password when the pgpass file is in an invalid format", func(t *testing.T) {
		provider := &MemoryPassfileProvider{Content: "invalid format"}
		hook := createBeforeConnect(noOpLogger, provider)
		ctx := context.Background()
		config := new(pgx.ConnConfig)
		err := hook(ctx, config)
		// Not a typo - pgpassfile.ParsePassfile only errors upon i/o errors
		// Given an invalid pgpass format, the line is simply not added to the struct
		require.NoError(t, err)
		require.Empty(t, config.Password)
	})
	t.Run("the hook returns an error if reading the file fails", func(t *testing.T) {
		provider := &MemoryPassfileProvider{"", io.NopCloser(iotest.ErrReader(errors.New("read error"))), nil}
		hook := createBeforeConnect(noOpLogger, provider)
		ctx := context.Background()
		config := new(pgx.ConnConfig)
		err := hook(ctx, config)
		require.ErrorContains(t, err, "read error")
		require.Empty(t, config.Password)
	})
	t.Run("retrieves localhost entries for unix socket hosts", func(t *testing.T) {
		provider := &MemoryPassfileProvider{"localhost:*:*:*:password", nil, nil}
		hook := createBeforeConnect(noOpLogger, provider)
		ctx := context.Background()
		config := new(pgx.ConnConfig)
		config.Host = "/docker.sock"
		err := hook(ctx, config)
		require.NoError(t, err)
		require.Equal(t, "password", config.Password)
	})
}

func TestFSPassfileProvider_OpenPassfile(t *testing.T) {
	t.Run("uses home dir without PGPASSFILE env var set", func(t *testing.T) {
		file := &MemFile{Content: io.NopCloser(strings.NewReader("*:*:*:*:password")), ModeBits: 0o600}
		provider := &FSPassfileProvider{
			Logger: logger.NewNoopLogger(),
			GetHomeDir: func() (string, error) {
				return "HOME", nil
			},
			OpenFile: func(name string) (FileStat, error) {
				if name == filepath.Join("HOME", ".pgpass") {
					return file, nil
				}
				return nil, ErrNoPassfile
			},
		}
		actual, err := provider.OpenPassfile()
		require.NoError(t, err)
		require.Equal(t, file, actual)
	})
	t.Run("uses PGPASSFILE env var when set over homedir", func(t *testing.T) {
		file := &MemFile{Content: io.NopCloser(strings.NewReader("*:*:*:*:password")), ModeBits: 0o600}
		filename := "anotherdir/.pgpass"
		t.Setenv("PGPASSFILE", filename)
		provider := &FSPassfileProvider{
			Logger: logger.NewNoopLogger(),
			GetHomeDir: func() (string, error) {
				return "HOME", nil
			},
			OpenFile: func(name string) (FileStat, error) {
				if name == filename {
					return file, nil
				}
				return nil, ErrNoPassfile
			},
		}
		actual, err := provider.OpenPassfile()
		require.NoError(t, err)
		require.Equal(t, file, actual)
	})
	t.Run("returns ErrNoPassfile when homedir is not found", func(t *testing.T) {
		provider := &FSPassfileProvider{
			Logger: logger.NewNoopLogger(),
			GetHomeDir: func() (string, error) {
				return "", errors.New("homedir not found")
			},
		}
		actual, err := provider.OpenPassfile()
		require.ErrorIs(t, err, ErrNoPassfile)
		require.Nil(t, actual)
	})
	t.Run("returns ErrNoPassfile when PGPASSFILE is set but file is not found", func(t *testing.T) {
		t.Setenv("PGPASSFILE", "anotherdir/.pgpass")
		provider := &FSPassfileProvider{
			Logger: logger.NewNoopLogger(),
			GetHomeDir: func() (string, error) {
				return "HOME", nil
			},
			OpenFile: func(name string) (FileStat, error) {
				return nil, os.ErrNotExist
			},
		}
		actual, err := provider.OpenPassfile()
		require.ErrorIs(t, err, ErrNoPassfile)
		require.Nil(t, actual)
	})
	t.Run("returns ErrInsecurePassfilePermissions when passfile is more permissive than 0600", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("checking pgpass file permissions is not supported on Windows")
		}
		file := &MemFile{Content: io.NopCloser(strings.NewReader("*:*:*:*:password")), ModeBits: 0o644}
		provider := &FSPassfileProvider{
			Logger: logger.NewNoopLogger(),
			GetHomeDir: func() (string, error) {
				return "HOME", nil
			},
			OpenFile: func(name string) (FileStat, error) {
				return file, nil
			},
		}
		actual, err := provider.OpenPassfile()
		require.ErrorIs(t, err, ErrInsecurePassfilePermissions)
		require.Nil(t, actual)
	})
	t.Run("returns the underlying stat error if permissions are not able to be retrieved", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("checking pgpass file permissions is not supported on Windows")
		}
		file := &MemFile{Content: io.NopCloser(strings.NewReader("*:*:*:*:password")), ModeBits: 0o644, StatErr: errors.New("stat error")}
		provider := &FSPassfileProvider{
			Logger: logger.NewNoopLogger(),
			GetHomeDir: func() (string, error) {
				return "HOME", nil
			},
			OpenFile: func(name string) (FileStat, error) {
				return file, nil
			},
		}
		actual, err := provider.OpenPassfile()
		require.Error(t, err, "stat error")
		require.Nil(t, actual)
	})
	t.Run("returns the underlying error if unable to open the file", func(t *testing.T) {
		provider := &FSPassfileProvider{
			Logger: logger.NewNoopLogger(),
			GetHomeDir: func() (string, error) {
				return "HOME", nil
			},
			OpenFile: func(name string) (FileStat, error) {
				return nil, errors.New("open error")
			},
		}
		actual, err := provider.OpenPassfile()
		require.Error(t, err, "open error")
		require.Nil(t, actual)
	})
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
			firstTuple := tupleUtils.NewTupleKey("doc:object_id_1", "relation", "user:user_1")
			secondTuple := tupleUtils.NewTupleKey("doc:object_id_2", "relation", "user:user_2")
			thirdTuple := tupleUtils.NewTupleKey("doc:object_id_3", "relation", "user:user_3")

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

			iter, err := ds.Read(ctx, store, storage.ReadFilter{Object: "doc:", Relation: "relation", User: ""}, storage.ReadOptions{})
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
			firstTuple := tupleUtils.NewTupleKey("doc:object_id_1", "relation", "user:user_1")
			secondTuple := tupleUtils.NewTupleKey("doc:object_id_2", "relation", "user:user_2")
			thirdTuple := tupleUtils.NewTupleKey("doc:object_id_3", "relation", "user:user_3")

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

			iter, err := ds.Read(ctx, store, storage.ReadFilter{Object: "doc:", Relation: "relation", User: ""}, storage.ReadOptions{})
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
	firstTuple := tupleUtils.NewTupleKey("doc:object_id_1", "relation", "user:user_1")
	secondTuple := tupleUtils.NewTupleKey("doc:object_id_2", "relation", "user:user_2")

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
		storage.ReadFilter{Object: "doc:", Relation: "relation", User: ""},
		opts)
	require.NoError(t, err)

	require.Len(t, tuples, 2)
	// We expect that objectID2 will return first because it has a smaller ulid.
	require.Equal(t, secondTuple, tuples[0].GetKey())
	require.Equal(t, firstTuple, tuples[1].GetKey())
}

func TestPostgresDatastore_ReadPageWithUserFiltering(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI(true)
	cfg := sqlcommon.NewConfig()
	ds, err := New(uri, cfg)
	require.NoError(t, err)
	defer ds.Close()

	ctx := context.Background()

	store := ulid.Make().String()

	// Create multiple tuples with same user type for pagination testing
	var tuples []*openfgav1.TupleKey
	for i := range 10 {
		tuples = append(tuples, &openfgav1.TupleKey{
			Object:   "doc:group1",
			Relation: "viewer",
			User:     fmt.Sprintf("user:user%d", i),
		})
		tuples = append(tuples, &openfgav1.TupleKey{
			Object:   "doc:group1",
			Relation: "viewer",
			User:     fmt.Sprintf("group:admin%d", i),
		})
	}

	err = ds.Write(ctx, store, nil, tuples)
	require.NoError(t, err)

	// Test pagination with user type filtering
	filter := storage.ReadFilter{
		Object:   "doc:group1",
		Relation: "viewer",
		User:     "user:",
	}

	readPageOptions := storage.ReadPageOptions{
		Pagination: storage.PaginationOptions{
			PageSize: 5,
		},
	}

	// First page
	tuples1, token, err := ds.ReadPage(ctx, store, filter, readPageOptions)
	require.NoError(t, err)
	require.Len(t, tuples1, 5)
	require.NotEmpty(t, token)

	// All returned tuples should have user type "user"
	for _, tuple := range tuples1 {
		userType, _, _ := tupleUtils.ToUserParts(tuple.GetKey().GetUser())
		require.Equal(t, "user", userType)
	}

	// Second page
	readPageOptions.Pagination.From = token
	tuples2, token2, err := ds.ReadPage(ctx, store, filter, readPageOptions)
	require.NoError(t, err)
	require.Len(t, tuples2, 5)
	require.Empty(t, token2)
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

func TestReadFilterWithConditions(t *testing.T) {
	ctx := context.Background()
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI(true)
	cfg := sqlcommon.NewConfig()
	ds, err := New(uri, cfg)
	require.NoError(t, err)
	defer ds.Close()

	var writeItems [][]interface{}
	writeItems = append(writeItems, []interface{}{
		"store",
		"folder",
		"2021-budget",
		"owner",
		"user:anne",
		"user",
		"cond1",
		"",
		ulid.Make().String(),
		sq.Expr("NOW()"),
	})

	writeItems = append(writeItems, []interface{}{
		"store",
		"folder",
		"2022-budget",
		"owner",
		"user:anne",
		"user",
		"",
		"",
		ulid.Make().String(),
		sq.Expr("NOW()"),
	})

	err = executeWriteTuples(ctx, ds.primaryDB, writeItems)
	require.NoError(t, err)

	// Read: if the tuple has condition and the filter has the same condition the tuple should be returned
	tk := tupleUtils.NewTupleKeyWithCondition("folder:2021-budget", "owner", "user:anne", "cond1", nil)
	filter := storage.ReadFilter{Object: "folder:2021-budget", Relation: "owner", User: "user:anne", Conditions: []string{"cond1"}}
	iter, err := ds.Read(ctx, "store", filter, storage.ReadOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	curTuple, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, tk.GetUser(), curTuple.GetKey().GetUser())
	require.Equal(t, tk.GetObject(), curTuple.GetKey().GetObject())
	require.Equal(t, tk.GetRelation(), curTuple.GetKey().GetRelation())
	require.Equal(t, tk.GetCondition().String(), curTuple.GetKey().GetCondition().String())
	_, err = iter.Next(ctx)
	require.Error(t, err)

	// Read: if filter has no condition the tuple cannot be returned
	filter = storage.ReadFilter{Object: "folder:2021-budget", Relation: "owner", User: "user:anne", Conditions: []string{""}}
	iter, err = ds.Read(ctx, "store", filter, storage.ReadOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	_, err = iter.Next(ctx)
	require.Error(t, err)

	// Read: if filter has a condition but the tuple stored does not have any condition, then the tuple cannot be returned
	filter = storage.ReadFilter{Object: "folder:2022-budget", Relation: "owner", User: "user:anne", Conditions: []string{"cond1"}}
	iter, err = ds.Read(ctx, "store", filter, storage.ReadOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	_, err = iter.Next(ctx)
	require.Error(t, err)

	// Read: if filter does not have condition and the tuple stored does not have any condition, then the tuple cannot be returned
	tk = tupleUtils.NewTupleKeyWithCondition("folder:2022-budget", "owner", "user:anne", "", nil)
	filter = storage.ReadFilter{Object: "folder:2022-budget", Relation: "owner", User: "user:anne", Conditions: []string{""}}
	iter, err = ds.Read(ctx, "store", filter, storage.ReadOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, tk, curTuple.GetKey())
	_, err = iter.Next(ctx)
	require.Error(t, err)

	// Read: without condition specification in the filter, backward compatibility should be maintained
	tk = tupleUtils.NewTupleKeyWithCondition("folder:2021-budget", "owner", "user:anne", "cond1", nil)
	filter = storage.ReadFilter{Object: "folder:2021-budget", Relation: "owner", User: "user:anne"}
	iter, err = ds.Read(ctx, "store", filter, storage.ReadOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, tk.GetUser(), curTuple.GetKey().GetUser())
	require.Equal(t, tk.GetObject(), curTuple.GetKey().GetObject())
	require.Equal(t, tk.GetRelation(), curTuple.GetKey().GetRelation())
	require.Equal(t, tk.GetCondition().String(), curTuple.GetKey().GetCondition().String())
	_, err = iter.Next(ctx)
	require.Error(t, err)
}

func TestReadUserTupleFilterWithConditions(t *testing.T) {
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

	// Insert tuple with condition
	_, err = ds.primaryDB.Exec(
		ctx, stmt, "store", "folder", "2021-budget", "owner", "user:anne", "user",
		ulid.Make().String(), "cond1", nil,
	)
	require.NoError(t, err)

	// Insert tuple without condition
	_, err = ds.primaryDB.Exec(
		ctx, stmt, "store", "folder", "2022-budget", "owner", "user:anne", "user",
		ulid.Make().String(), nil, nil,
	)
	require.NoError(t, err)

	// ReadUserTuple: if the tuple has condition and the filter has the same condition, the tuple should be returned
	tk := tupleUtils.NewTupleKeyWithCondition("folder:2021-budget", "owner", "user:anne", "cond1", nil)
	filter := storage.ReadUserTupleFilter{
		Object:     "folder:2021-budget",
		Relation:   "owner",
		User:       "user:anne",
		Conditions: []string{"cond1"},
	}
	tuple, err := ds.ReadUserTuple(ctx, "store", filter, storage.ReadUserTupleOptions{})
	require.NoError(t, err)
	require.Equal(t, tk, tuple.GetKey())

	// ReadUserTuple: if filter has no condition, the tuple with condition should not be returned
	filter = storage.ReadUserTupleFilter{
		Object:     "folder:2021-budget",
		Relation:   "owner",
		User:       "user:anne",
		Conditions: []string{""},
	}
	_, err = ds.ReadUserTuple(ctx, "store", filter, storage.ReadUserTupleOptions{})
	require.Error(t, err)
	require.ErrorIs(t, err, storage.ErrNotFound)

	// ReadUserTuple: if filter has a condition but the tuple stored does not have any condition, the tuple should not be returned
	filter = storage.ReadUserTupleFilter{
		Object:     "folder:2022-budget",
		Relation:   "owner",
		User:       "user:anne",
		Conditions: []string{"cond1"},
	}
	_, err = ds.ReadUserTuple(ctx, "store", filter, storage.ReadUserTupleOptions{})
	require.Error(t, err)
	require.ErrorIs(t, err, storage.ErrNotFound)

	// ReadUserTuple: if filter does not have condition and the tuple stored does not have any condition, the tuple should be returned
	tk = tupleUtils.NewTupleKeyWithCondition("folder:2022-budget", "owner", "user:anne", "", nil)
	filter = storage.ReadUserTupleFilter{
		Object:     "folder:2022-budget",
		Relation:   "owner",
		User:       "user:anne",
		Conditions: []string{""},
	}
	tuple, err = ds.ReadUserTuple(ctx, "store", filter, storage.ReadUserTupleOptions{})
	require.NoError(t, err)
	require.Equal(t, tk, tuple.GetKey())

	// ReadUserTuple: without condition specification in the filter, backward compatibility should be maintained
	tk = tupleUtils.NewTupleKeyWithCondition("folder:2021-budget", "owner", "user:anne", "cond1", nil)
	filter = storage.ReadUserTupleFilter{
		Object:   "folder:2021-budget",
		Relation: "owner",
		User:     "user:anne",
	}
	tuple, err = ds.ReadUserTuple(ctx, "store", filter, storage.ReadUserTupleOptions{})
	require.NoError(t, err)
	require.Equal(t, tk, tuple.GetKey())
}

func TestReadStartingWithUserFilterWithConditions(t *testing.T) {
	ctx := context.Background()
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI(true)
	cfg := sqlcommon.NewConfig()
	ds, err := New(uri, cfg)
	require.NoError(t, err)
	defer ds.Close()
	store := ulid.Make().String()

	// Setup test data with various combinations
	tuples := []*openfgav1.TupleKey{
		{Object: "folder:2021-budget", Relation: "owner", User: "user:anne", Condition: &openfgav1.RelationshipCondition{Name: "cond1"}},
		{Object: "folder:2023-budget", Relation: "owner", User: "user:anne", Condition: &openfgav1.RelationshipCondition{Name: "cond1"}},
		{Object: "folder:2022-budget", Relation: "owner", User: "user:anne"},
		{Object: "folder:2024-budget", Relation: "owner", User: "user:anne"},
		{Object: "folder:2022-budget", Relation: "owner", User: "user:jack"},
		{Object: "folder:2024-budget", Relation: "owner", User: "user:jack"},
	}
	err = ds.Write(ctx, store, nil, tuples)
	require.NoError(t, err)

	// ReadStartingWithUser: if the tuple has condition and the filter has the same condition the tuple should be returned
	filter := storage.ReadStartingWithUserFilter{
		ObjectType: "folder",
		Relation:   "owner",
		UserFilter: []*openfgav1.ObjectRelation{
			{Object: "user:anne"},
		},
		Conditions: []string{"cond1"},
	}
	iter, err := ds.ReadStartingWithUser(ctx, store, filter, storage.ReadStartingWithUserOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	curTuple, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"folder:2021-budget", "folder:2023-budget"}, curTuple.GetKey().GetObject())
	require.Equal(t, "cond1", curTuple.GetKey().GetCondition().GetName())
	require.Equal(t, "user:anne", curTuple.GetKey().GetUser())
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"folder:2021-budget", "folder:2023-budget"}, curTuple.GetKey().GetObject())
	require.Equal(t, "cond1", curTuple.GetKey().GetCondition().GetName())
	require.Equal(t, "user:anne", curTuple.GetKey().GetUser())
	_, err = iter.Next(ctx)
	require.Error(t, err)

	// ReadStartingWithUser: if filter has a condition but the tuple stored does not have any condition, then the tuple cannot be returned
	filter = storage.ReadStartingWithUserFilter{
		ObjectType: "folder",
		Relation:   "owner",
		UserFilter: []*openfgav1.ObjectRelation{
			{Object: "user:jack"},
		},
		Conditions: []string{"cond1"},
	}
	iter, err = ds.ReadStartingWithUser(ctx, store, filter, storage.ReadStartingWithUserOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	_, err = iter.Next(ctx)
	require.Error(t, err)

	// ReadStartingWithUser: if filter does not have condition and the tuple stored does not have any condition, then the tuple cannot be returned
	filter = storage.ReadStartingWithUserFilter{
		ObjectType: "folder",
		Relation:   "owner",
		UserFilter: []*openfgav1.ObjectRelation{
			{Object: "user:anne"},
		},
		Conditions: []string{""},
	}
	iter, err = ds.ReadStartingWithUser(ctx, store, filter, storage.ReadStartingWithUserOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"folder:2022-budget", "folder:2024-budget"}, curTuple.GetKey().GetObject())
	require.Empty(t, curTuple.GetKey().GetCondition().GetName())
	require.Equal(t, "user:anne", curTuple.GetKey().GetUser())
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"folder:2022-budget", "folder:2024-budget"}, curTuple.GetKey().GetObject())
	require.Empty(t, curTuple.GetKey().GetCondition().GetName())
	require.Equal(t, "user:anne", curTuple.GetKey().GetUser())
	_, err = iter.Next(ctx)
	require.Error(t, err)

	// ReadStartingWithUser: without condition specification in the filter, backward compatibility should be maintained
	filter = storage.ReadStartingWithUserFilter{
		ObjectType: "folder",
		Relation:   "owner",
		UserFilter: []*openfgav1.ObjectRelation{
			{Object: "user:anne"},
		},
	}
	iter, err = ds.ReadStartingWithUser(ctx, store, filter, storage.ReadStartingWithUserOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"folder:2022-budget", "folder:2024-budget", "folder:2021-budget", "folder:2023-budget"}, curTuple.GetKey().GetObject())
	require.Equal(t, "user:anne", curTuple.GetKey().GetUser())
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"folder:2022-budget", "folder:2024-budget", "folder:2021-budget", "folder:2023-budget"}, curTuple.GetKey().GetObject())
	require.Equal(t, "user:anne", curTuple.GetKey().GetUser())
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"folder:2022-budget", "folder:2024-budget", "folder:2021-budget", "folder:2023-budget"}, curTuple.GetKey().GetObject())
	require.Equal(t, "user:anne", curTuple.GetKey().GetUser())
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"folder:2022-budget", "folder:2024-budget", "folder:2021-budget", "folder:2023-budget"}, curTuple.GetKey().GetObject())
	require.Equal(t, "user:anne", curTuple.GetKey().GetUser())
	_, err = iter.Next(ctx)
	require.Error(t, err)
}

func TestReadUsersetTuplesFilterWithConditions(t *testing.T) {
	ctx := context.Background()
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI(true)
	cfg := sqlcommon.NewConfig()
	ds, err := New(uri, cfg)
	require.NoError(t, err)
	defer ds.Close()
	store := ulid.Make().String()

	// Setup test data with various combinations
	tuples := []*openfgav1.TupleKey{
		{Object: "document:2021-budget", Relation: "member", User: "group:g1#member", Condition: &openfgav1.RelationshipCondition{Name: "cond1"}},
		{Object: "document:2021-budget", Relation: "member", User: "group:g2#member", Condition: &openfgav1.RelationshipCondition{Name: "cond1"}},
		{Object: "document:2021-budget", Relation: "member", User: "group:g3#member"},
		{Object: "document:2021-budget", Relation: "member", User: "group:g4#member"},
		{Object: "document:2022-budget", Relation: "member", User: "group:g2#member"},
		{Object: "document:2022-budget", Relation: "member", User: "group:g1#member"},
	}
	err = ds.Write(ctx, store, nil, tuples)
	require.NoError(t, err)

	// ReadUsersetTuples: if the tuple has condition and the filter has the same condition the tuple should be returned
	filter := storage.ReadUsersetTuplesFilter{
		Object:   "document:2021-budget",
		Relation: "member",
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
			{
				Type:               "group",
				RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "member"},
			},
		},
		Conditions: []string{"cond1"},
	}
	iter, err := ds.ReadUsersetTuples(ctx, store, filter, storage.ReadUsersetTuplesOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	curTuple, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"group:g1#member", "group:g2#member"}, curTuple.GetKey().GetUser())
	require.Equal(t, "cond1", curTuple.GetKey().GetCondition().GetName())
	require.Equal(t, "document:2021-budget", curTuple.GetKey().GetObject())
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"group:g1#member", "group:g2#member"}, curTuple.GetKey().GetUser())
	require.Equal(t, "cond1", curTuple.GetKey().GetCondition().GetName())
	require.Equal(t, "document:2021-budget", curTuple.GetKey().GetObject())
	_, err = iter.Next(ctx)
	require.Error(t, err)

	// ReadUsersetTuples: if filter has a condition but the tuple stored does not have any condition, then the tuple cannot be returned
	filter = storage.ReadUsersetTuplesFilter{
		Object:   "document:2022-budget",
		Relation: "member",
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
			{
				Type:               "group",
				RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "member"},
			},
		},
		Conditions: []string{"cond1"},
	}
	iter, err = ds.ReadUsersetTuples(ctx, store, filter, storage.ReadUsersetTuplesOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	_, err = iter.Next(ctx)
	require.Error(t, err)

	// ReadUsersetTuples: if filter does not have condition and the tuple stored does not have any condition, then the tuple cannot be returned
	filter = storage.ReadUsersetTuplesFilter{
		Object:   "document:2021-budget",
		Relation: "member",
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
			{
				Type:               "group",
				RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "member"},
			},
		},
		Conditions: []string{""},
	}
	iter, err = ds.ReadUsersetTuples(ctx, store, filter, storage.ReadUsersetTuplesOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"group:g3#member", "group:g4#member"}, curTuple.GetKey().GetUser())
	require.Empty(t, curTuple.GetKey().GetCondition().GetName())
	require.Equal(t, "document:2021-budget", curTuple.GetKey().GetObject())
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"group:g3#member", "group:g4#member"}, curTuple.GetKey().GetUser())
	require.Empty(t, curTuple.GetKey().GetCondition().GetName())
	require.Equal(t, "document:2021-budget", curTuple.GetKey().GetObject())
	_, err = iter.Next(ctx)
	require.Error(t, err)

	// ReadUsersetTuples: without condition specification in the filter, backward compatibility should be maintained
	filter = storage.ReadUsersetTuplesFilter{
		Object:   "document:2021-budget",
		Relation: "member",
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
			{
				Type:               "group",
				RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "member"},
			},
		},
	}
	iter, err = ds.ReadUsersetTuples(ctx, store, filter, storage.ReadUsersetTuplesOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"group:g3#member", "group:g4#member", "group:g1#member", "group:g2#member"}, curTuple.GetKey().GetUser())
	require.Equal(t, "document:2021-budget", curTuple.GetKey().GetObject())
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"group:g3#member", "group:g4#member", "group:g1#member", "group:g2#member"}, curTuple.GetKey().GetUser())
	require.Equal(t, "document:2021-budget", curTuple.GetKey().GetObject())
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"group:g3#member", "group:g4#member", "group:g1#member", "group:g2#member"}, curTuple.GetKey().GetUser())
	require.Equal(t, "document:2021-budget", curTuple.GetKey().GetObject())
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"group:g3#member", "group:g4#member", "group:g1#member", "group:g2#member"}, curTuple.GetKey().GetUser())
	require.Equal(t, "document:2021-budget", curTuple.GetKey().GetObject())
	_, err = iter.Next(ctx)
	require.Error(t, err)
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

	tk := tupleUtils.NewTupleKey("folder:2021-budget", "owner", "user:anne")
	filter := storage.ReadFilter{Object: "folder:2021-budget", Relation: "owner", User: "user:anne"}
	iter, err := ds.Read(ctx, "store", filter, storage.ReadOptions{})
	require.NoError(t, err)
	defer iter.Stop()

	curTuple, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, tk, curTuple.GetKey())

	opts := storage.ReadPageOptions{
		Pagination: storage.NewPaginationOptions(2, ""),
	}
	tuples, _, err := ds.ReadPage(ctx, "store", storage.ReadFilter{}, opts)
	require.NoError(t, err)
	require.Len(t, tuples, 1)
	require.Equal(t, tk, tuples[0].GetKey())

	userTuple, err := ds.ReadUserTuple(ctx, "store", filter, storage.ReadUserTupleOptions{})
	require.NoError(t, err)
	require.Equal(t, tk, userTuple.GetKey())

	tk2 := tupleUtils.NewTupleKey("folder:2022-budget", "viewer", "user:anne")
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

// The following are tests for internal functions for related to write

// Testing deletion of tuples.
func TestExecuteDeleteTuples(t *testing.T) {
	t.Run("empty_statement", func(t *testing.T) {
		deleteConditions := sq.Or{}
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)
		mockPgxExec := mocks.NewMockpgxExec(ctrl)
		err := executeDeleteTuples(context.Background(), mockPgxExec, "123", deleteConditions)
		require.NoError(t, err)
	})

	t.Run("txn_error", func(t *testing.T) {
		deleteConditions := sq.Or{
			sq.Eq{
				"object_type": "folder",
			},
		}
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)
		mockPgxExec := mocks.NewMockpgxExec(ctrl)
		mockPgxExec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(pgconn.NewCommandTag(""), fmt.Errorf("error"))
		err := executeDeleteTuples(context.Background(), mockPgxExec, "123", deleteConditions)
		require.Error(t, err)
	})

	t.Run("empty_result_conflict", func(t *testing.T) {
		deleteConditions := sq.Or{
			sq.Eq{
				"object_type": "folder",
			},
		}
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)
		mockPgxExec := mocks.NewMockpgxExec(ctrl)
		mockPgxExec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(pgconn.NewCommandTag(""), nil)
		err := executeDeleteTuples(context.Background(), mockPgxExec, "123", deleteConditions)
		require.ErrorIs(t, err, storage.ErrWriteConflictOnDelete)
	})
	t.Run("correct_row", func(t *testing.T) {
		deleteConditions := sq.Or{
			sq.Eq{
				"object_type": "folder",
			},
		}
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)
		mockPgxExec := mocks.NewMockpgxExec(ctrl)
		mockPgxExec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(pgconn.NewCommandTag("DELETE 1"), nil)
		err := executeDeleteTuples(context.Background(), mockPgxExec, "123", deleteConditions)
		require.NoError(t, err)
	})
}

func TestExecuteWriteTuples(t *testing.T) {
	t.Run("empty_statement", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)
		mockPgxExec := mocks.NewMockpgxExec(ctrl)
		err := executeWriteTuples(context.Background(), mockPgxExec, nil)
		require.NoError(t, err)
	})
	t.Run("txn_exec_good", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)
		mockPgxExec := mocks.NewMockpgxExec(ctrl)
		mockPgxExec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(pgconn.NewCommandTag("INSERT 1"), nil)

		writeItems := [][]interface{}{}
		writeItems = append(writeItems, []interface{}{
			"storeID",
			"objectType1",
			"objectID1",
			"rel1",
			"user1",
			"userType",
			"",
			"",
			"1234",
			sq.Expr("NOW()"), // missing time
		})
		err := executeWriteTuples(context.Background(), mockPgxExec, writeItems)
		require.NoError(t, err)
	})
	t.Run("txn_exec_collision_error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)
		mockPgxExec := mocks.NewMockpgxExec(ctrl)
		mockPgxExec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(pgconn.NewCommandTag(""), fmt.Errorf("duplicate key value"))

		writeItems := [][]interface{}{}
		writeItems = append(writeItems, []interface{}{
			"storeID",
			"objectType1",
			"objectID1",
			"rel1",
			"user1",
			"userType",
			"",
			"",
			"1234",
			sq.Expr("NOW()"),
		})
		err := executeWriteTuples(context.Background(), mockPgxExec, writeItems)
		require.ErrorIs(t, err, storage.ErrWriteConflictOnInsert)
	})
	t.Run("txn_exec_no_collision_error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)
		mockPgxExec := mocks.NewMockpgxExec(ctrl)
		mockPgxExec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(pgconn.NewCommandTag(""), fmt.Errorf("error"))

		writeItems := [][]interface{}{}
		writeItems = append(writeItems, []interface{}{
			"storeID",
			"objectType1",
			"objectID1",
			"rel1",
			"user1",
			"userType",
			"",
			"",
			"1234",
			sq.Expr("NOW()"),
		})
		err := executeWriteTuples(context.Background(), mockPgxExec, writeItems)
		require.ErrorContains(t, err, "sql error: error")
	})
}

func TestExecuteInsertChanges(t *testing.T) {
	t.Run("empty_statement", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)
		mockPgxExec := mocks.NewMockpgxExec(ctrl)
		err := executeInsertChanges(context.Background(), mockPgxExec, nil)
		require.NoError(t, err)
	})
	t.Run("txn_exec_good", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)
		mockPgxExec := mocks.NewMockpgxExec(ctrl)
		mockPgxExec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(pgconn.NewCommandTag("INSERT 1"), nil)

		writeItems := [][]interface{}{}
		writeItems = append(writeItems, []interface{}{
			"storeID",
			"objectType1",
			"objectID1",
			"rel1",
			"user1",
			"userType",
			"",
			"",
			"1234",
			sq.Expr("NOW()"), // missing time
		})
		err := executeInsertChanges(context.Background(), mockPgxExec, writeItems)
		require.NoError(t, err)
	})

	t.Run("txn_exec_error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)
		mockPgxExec := mocks.NewMockpgxExec(ctrl)
		mockPgxExec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(pgconn.NewCommandTag(""), fmt.Errorf("error"))

		writeItems := [][]interface{}{}
		writeItems = append(writeItems, []interface{}{
			"storeID",
			"objectType1",
			"objectID1",
			"rel1",
			"user1",
			"userType",
			"",
			"",
			"1234",
			sq.Expr("NOW()"),
		})
		err := executeInsertChanges(context.Background(), mockPgxExec, writeItems)
		require.ErrorContains(t, err, "sql error: error")
	})
}
