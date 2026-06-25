package sqlcommon

import (
	"context"
	"database/sql"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
)

// newSQLiteDBInfo provides an in-memory SQLite database with the authorization_model
// schema that sqlcommon's shared (mysql-style, single `_user` column) queries expect.
// SQLite is pure-Go (modernc) so this runs in the unit test job without a container.
func newSQLiteDBInfo(t *testing.T) (*sql.DB, *DBInfo) {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.Exec(`CREATE TABLE authorization_model (
		store TEXT NOT NULL,
		authorization_model_id TEXT NOT NULL,
		schema_version TEXT NOT NULL,
		type TEXT NOT NULL,
		type_definition BLOB,
		serialized_protobuf BLOB
	);`)
	require.NoError(t, err)

	info := NewDBInfo(sq.StatementBuilder.RunWith(db), passthroughErr, "sqlite3")
	return db, info
}

func writeModel(t *testing.T, info *DBInfo, store string, model *openfgav1.AuthorizationModel) {
	t.Helper()
	require.NoError(t, WriteAuthorizationModel(context.Background(), info, store, model))
}

func sampleModel(id string) *openfgav1.AuthorizationModel {
	return &openfgav1.AuthorizationModel{
		Id:            id,
		SchemaVersion: "1.1",
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{Type: "user"},
			{Type: "document"},
		},
	}
}

func TestWriteAndReadAuthorizationModel_SQLite(t *testing.T) {
	ctx := context.Background()
	_, info := newSQLiteDBInfo(t)
	const store = "store-1"

	t.Run("write_then_read_round_trip", func(t *testing.T) {
		model := sampleModel("01MODEL1")
		writeModel(t, info, store, model)

		got, err := ReadAuthorizationModel(ctx, info, store, "01MODEL1")
		require.NoError(t, err)
		require.Equal(t, "01MODEL1", got.GetId())
		require.Equal(t, "1.1", got.GetSchemaVersion())
		require.Len(t, got.GetTypeDefinitions(), 2)
	})

	t.Run("read_missing_model_returns_not_found", func(t *testing.T) {
		_, err := ReadAuthorizationModel(ctx, info, store, "does-not-exist")
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("write_model_without_type_definitions_is_noop", func(t *testing.T) {
		empty := &openfgav1.AuthorizationModel{Id: "01EMPTY", SchemaVersion: "1.1"}
		require.NoError(t, WriteAuthorizationModel(ctx, info, store, empty))

		_, err := ReadAuthorizationModel(ctx, info, store, "01EMPTY")
		require.ErrorIs(t, err, storage.ErrNotFound)
	})
}

func TestFindLatestAuthorizationModel_SQLite(t *testing.T) {
	ctx := context.Background()
	_, info := newSQLiteDBInfo(t)
	const store = "store-2"

	t.Run("returns_not_found_when_empty", func(t *testing.T) {
		_, err := FindLatestAuthorizationModel(ctx, info, store)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("returns_highest_model_id", func(t *testing.T) {
		writeModel(t, info, store, sampleModel("01AAA"))
		writeModel(t, info, store, sampleModel("01BBB"))
		writeModel(t, info, store, sampleModel("01CCC"))

		got, err := FindLatestAuthorizationModel(ctx, info, store)
		require.NoError(t, err)
		require.Equal(t, "01CCC", got.GetId())
	})
}

func TestIsReady_SQLite(t *testing.T) {
	db, _ := newSQLiteDBInfo(t)

	// skipVersionCheck short-circuits the migration revision lookup but still pings the DB.
	status, err := IsReady(context.Background(), true, db)
	require.NoError(t, err)
	require.True(t, status.IsReady)
}
