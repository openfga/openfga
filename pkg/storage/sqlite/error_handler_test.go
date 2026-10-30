package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
)

func TestNewWithDBErrorHandler(t *testing.T) {
	rawErr := errors.New("alternative SQLite driver error")
	translatedErr := errors.New("translated error")
	for _, tt := range []struct {
		name string
		mode string
		run  func(context.Context, *Datastore) error
	}{
		{"transaction_begin", "begin", func(ctx context.Context, ds *Datastore) error {
			return ds.Write(ctx, "store", nil, []*openfgav1.TupleKey{{Object: "doc:1", Relation: "viewer", User: "user:1"}})
		}},
		{"transaction_write", "exec", func(ctx context.Context, ds *Datastore) error {
			return ds.Write(ctx, "store", nil, []*openfgav1.TupleKey{{Object: "doc:1", Relation: "viewer", User: "user:1"}})
		}},
		{"transaction_commit", "commit", func(ctx context.Context, ds *Datastore) error {
			return ds.Write(ctx, "store", nil, []*openfgav1.TupleKey{{Object: "doc:1", Relation: "viewer", User: "user:1"}})
		}},
		{"transaction_delete", "delete", func(ctx context.Context, ds *Datastore) error {
			return ds.Write(ctx, "store", []*openfgav1.TupleKeyWithoutCondition{{Object: "doc:1", Relation: "viewer", User: "user:1"}}, nil)
		}},
		{"transaction_delete_rows_affected", "rows_affected", func(ctx context.Context, ds *Datastore) error {
			return ds.Write(ctx, "store", []*openfgav1.TupleKeyWithoutCondition{{Object: "doc:1", Relation: "viewer", User: "user:1"}}, nil)
		}},
		{"transaction_changelog", "changelog", func(ctx context.Context, ds *Datastore) error {
			return ds.Write(ctx, "store", nil, []*openfgav1.TupleKey{{Object: "doc:1", Relation: "viewer", User: "user:1"}})
		}},
		{"write", "exec", func(ctx context.Context, ds *Datastore) error {
			return ds.DeleteStore(ctx, "store")
		}},
		{"query", "query", func(ctx context.Context, ds *Datastore) error {
			_, err := ds.GetStore(ctx, "store")
			return err
		}},
		{"iterator_query", "query", func(ctx context.Context, ds *Datastore) error {
			iter, err := ds.Read(ctx, "store", storage.ReadFilter{}, storage.ReadOptions{})
			if err != nil {
				return err
			}
			defer iter.Stop()
			_, err = iter.Next(ctx)
			return err
		}},
		{"iterator_rows", "rows", func(ctx context.Context, ds *Datastore) error {
			iter, err := ds.Read(ctx, "store", storage.ReadFilter{}, storage.ReadOptions{})
			if err != nil {
				return err
			}
			defer iter.Stop()
			_, err = iter.Head(ctx)
			return err
		}},
		{"iterator_scan", "scan", func(ctx context.Context, ds *Datastore) error {
			iter, err := ds.Read(ctx, "store", storage.ReadFilter{}, storage.ReadOptions{})
			if err != nil {
				return err
			}
			defer iter.Stop()
			_, err = iter.Next(ctx)
			return err
		}},
		{"model_rows", "rows", func(ctx context.Context, ds *Datastore) error {
			_, err := ds.ReadAuthorizationModel(ctx, "store", "model")
			return err
		}},
		{"model_scan", "scan", func(ctx context.Context, ds *Datastore) error {
			_, err := ds.FindLatestAuthorizationModel(ctx, "store")
			return err
		}},
		{"model_query", "query", func(ctx context.Context, ds *Datastore) error {
			_, err := ds.ReadAuthorizationModel(ctx, "store", "model")
			return err
		}},
		{"latest_model_query", "query", func(ctx context.Context, ds *Datastore) error {
			_, err := ds.FindLatestAuthorizationModel(ctx, "store")
			return err
		}},
		{"models_query", "query", func(ctx context.Context, ds *Datastore) error {
			_, _, err := ds.ReadAuthorizationModels(ctx, "store", storage.ReadAuthorizationModelsOptions{})
			return err
		}},
		{"models_scan", "scan", func(ctx context.Context, ds *Datastore) error {
			_, _, err := ds.ReadAuthorizationModels(ctx, "store", storage.ReadAuthorizationModelsOptions{})
			return err
		}},
		{"models_rows", "rows", func(ctx context.Context, ds *Datastore) error {
			_, _, err := ds.ReadAuthorizationModels(ctx, "store", storage.ReadAuthorizationModelsOptions{})
			return err
		}},
		{"model_write", "exec", func(ctx context.Context, ds *Datastore) error {
			return ds.WriteAuthorizationModel(ctx, "store", &openfgav1.AuthorizationModel{
				Id: "model", SchemaVersion: "1.1",
				TypeDefinitions: []*openfgav1.TypeDefinition{{Type: "user"}},
			})
		}},
		{"list_query", "query", func(ctx context.Context, ds *Datastore) error {
			_, _, err := ds.ListStores(ctx, storage.ListStoresOptions{})
			return err
		}},
		{"list_scan", "scan", func(ctx context.Context, ds *Datastore) error {
			_, _, err := ds.ListStores(ctx, storage.ListStoresOptions{})
			return err
		}},
		{"list_rows", "rows", func(ctx context.Context, ds *Datastore) error {
			_, _, err := ds.ListStores(ctx, storage.ListStoresOptions{})
			return err
		}},
		{"assertions_write", "exec", func(ctx context.Context, ds *Datastore) error {
			return ds.WriteAssertions(ctx, "store", "model", nil)
		}},
		{"assertions_query", "query", func(ctx context.Context, ds *Datastore) error {
			_, err := ds.ReadAssertions(ctx, "store", "model")
			return err
		}},
		{"changes_query", "query", func(ctx context.Context, ds *Datastore) error {
			_, _, err := ds.ReadChanges(ctx, "store", storage.ReadChangesFilter{}, storage.ReadChangesOptions{})
			return err
		}},
		{"changes_scan", "scan", func(ctx context.Context, ds *Datastore) error {
			_, _, err := ds.ReadChanges(ctx, "store", storage.ReadChangesFilter{}, storage.ReadChangesOptions{})
			return err
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db := sql.OpenDB(errorConnector{mode: tt.mode, err: rawErr})
			defer db.Close()
			var received []error
			ds, err := NewWithDB(db, sqlcommon.NewConfig(), WithErrorHandler(func(err error, _ ...interface{}) error {
				received = append(received, err)
				return translatedErr
			}))
			require.NoError(t, err)
			defer ds.Close()
			require.ErrorIs(t, tt.run(context.Background(), ds), translatedErr)
			require.Len(t, received, 1)
			if tt.mode == "scan" {
				require.Contains(t, received[0].Error(), "Scan error")
			} else {
				require.ErrorIs(t, received[0], rawErr)
			}
			require.ErrorIs(t, ds.dbInfo.HandleSQLError(rawErr), translatedErr)
		})
	}
}

func TestNewWithDBDefaultErrorHandler(t *testing.T) {
	db := sql.OpenDB(errorConnector{mode: "query", err: sql.ErrNoRows})
	defer db.Close()
	ds, err := NewWithDB(db, sqlcommon.NewConfig())
	require.NoError(t, err)
	defer ds.Close()
	_, err = ds.ReadAuthorizationModel(context.Background(), "store", "model")
	require.ErrorIs(t, err, storage.ErrNotFound)
}

func TestNewWithDBNilErrorHandler(t *testing.T) {
	db := sql.OpenDB(errorConnector{})
	defer db.Close()
	ds, err := NewWithDB(db, sqlcommon.NewConfig(sqlcommon.WithMetrics()), WithErrorHandler(nil))
	require.Nil(t, ds)
	require.EqualError(t, err, "initialize sqlite datastore: error handler must not be nil")
}

func TestNewWithDBMetrics(t *testing.T) {
	db := sql.OpenDB(errorConnector{})
	defer db.Close()
	ds, err := NewWithDB(db, sqlcommon.NewConfig(sqlcommon.WithMetrics()))
	require.NoError(t, err)
	defer ds.Close()
	require.NotNil(t, ds.dbStatsCollector)
}

type errorConnector struct {
	mode string
	err  error
}

func (c errorConnector) Connect(context.Context) (driver.Conn, error) {
	return &errorConnection{mode: c.mode, err: c.err}, nil
}

func (c errorConnector) Driver() driver.Driver { return errorDriver{} }

type errorDriver struct{}

func (errorDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use the connector")
}

type errorConnection struct {
	mode string
	err  error
}

func (c *errorConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected Prepare")
}

func (c *errorConnection) Close() error { return nil }

func (c *errorConnection) Begin() (driver.Tx, error) { return nil, c.err }

func (c *errorConnection) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	if c.mode == "begin" {
		return nil, c.err
	}
	return errorTransaction{mode: c.mode, err: c.err}, nil
}

func (c *errorConnection) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if c.mode == "rows_affected" {
		return errorResult{err: c.err}, nil
	}
	if c.mode == "commit" || (c.mode == "changelog" && !strings.Contains(query, "INSERT INTO changelog")) {
		return driver.RowsAffected(1), nil
	}
	return nil, c.err
}

func (c *errorConnection) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if c.mode == "query" {
		return nil, c.err
	}
	columns := tupleColumns
	if strings.Contains(query, "FROM authorization_model") {
		columns = []string{"authorization_model_id", "schema_version", "serialized_protobuf"}
	} else if strings.Contains(query, "FROM store") {
		columns = []string{"id", "name", "created_at", "updated_at"}
	} else if strings.Contains(query, "FROM changelog") {
		columns = []string{"ulid", "object_type", "object_id", "relation", "user_object_type",
			"user_object_id", "user_relation", "operation", "condition_name", "condition_context", "inserted_at"}
	}
	return &errorRows{columns: columns, mode: c.mode, err: c.err,
		done: c.mode == "exec" || c.mode == "commit" || c.mode == "changelog"}, nil
}

type errorResult struct {
	err error
}

func (r errorResult) LastInsertId() (int64, error) { return 0, r.err }
func (r errorResult) RowsAffected() (int64, error) { return 0, r.err }

type errorTransaction struct {
	mode string
	err  error
}

func (tx errorTransaction) Commit() error {
	if tx.mode == "commit" {
		return tx.err
	}
	return nil
}

func (tx errorTransaction) Rollback() error { return nil }

type errorRows struct {
	columns []string
	mode    string
	err     error
	done    bool
}

func (r *errorRows) Columns() []string { return r.columns }
func (r *errorRows) Close() error      { return nil }

func (r *errorRows) Next(values []driver.Value) error {
	if r.mode == "rows" {
		return r.err
	}
	if r.done {
		return io.EOF
	}
	r.done = true
	if r.mode == "delete" || r.mode == "rows_affected" {
		copy(values, []driver.Value{
			"store", "doc", "1", "viewer", "user", "1", "", nil, nil, "ulid", time.Now(),
		})
		return nil
	}
	// NULL cannot be scanned into the datastore's required string columns.
	clear(values)
	return nil
}
