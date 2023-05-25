package postgres

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/cmd"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	"github.com/openfga/openfga/pkg/storage/test"
	"github.com/openfga/openfga/pkg/testfixtures/server"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestPostgresDatastore(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI(true)
	ds, err := New(uri, sqlcommon.NewConfig())
	require.NoError(t, err)
	defer ds.Close()
	test.RunAllTests(t, ds)
}

func TestMigrate(t *testing.T) {
	engine := "postgres"
	// starts the container and runs migration up to the latest migration version available
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, engine)

	uri := testDatastore.GetConnectionURI(true)
	ds, err := New(uri, sqlcommon.NewConfig())
	require.NoError(t, err)
	defer ds.Close()

	// going from version 3 to 4 when migration #4 doesn't exist is a no-op
	version := testDatastore.GetDatabaseSchemaVersion() + 1

	migrateCommand := cmd.NewMigrateCommand()

	for version >= 0 {
		t.Logf("migrating to version %d", version)
		migrateCommand.SetArgs([]string{"--datastore-engine", engine, "--datastore-uri", uri, "--version", strconv.Itoa(int(version))})
		err = migrateCommand.Execute()
		require.NoError(t, err)
		version--
	}
}

func TestMigrateVersion4(t *testing.T) {
	store := ulid.Make().String()
	engine := "postgres"

	t.Logf("start the database container and run migrations up to the latest migration version available")
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, engine)
	uri := testDatastore.GetConnectionURI(true)

	t.Logf("migrate down to version 3")
	migrateCommand := cmd.NewMigrateCommand()
	migrateCommand.SetArgs([]string{"--datastore-engine", engine, "--datastore-uri", uri, "--version", strconv.Itoa(3)})
	err := migrateCommand.Execute()
	require.NoError(t, err)

	t.Logf("start openfga 1.1.0 and wait for it to be ready")
	datastoreConnectionURI := strings.Replace(uri, "localhost", "host.docker.internal", -1)
	s := server.RunOpenfgaInContainer(t, "1.1.0", engine, datastoreConnectionURI)
	err = backoff.Retry(func() error {
		_, err := http.Get(fmt.Sprintf("http://%s/healthz", s.URL))
		return err
	}, backoff.NewExponentialBackOff())
	require.NoError(t, err)

	t.Logf("writing authorization model")
	data := []byte(`{"type_definitions":[{"type":"user","relations":{}},{"type":"group","relations":{"member":{"this":{}}},"metadata":{"relations":{"member":{"directly_related_user_types":[{"type":"user"}]}}}},{"type":"folder","relations":{"viewer":{"this":{}}},"metadata":{"relations":{"viewer":{"directly_related_user_types":[{"type":"user"},{"type":"user","wildcard":{}},{"type":"group","relation":"member"}]}}}}],"schema_version":"1.1"}`)
	resp, _ := http.Post(fmt.Sprintf("http://%s/stores/%s/authorization-models", s.URL, store), "application/json", bytes.NewBuffer(data))
	require.Equal(t, 201, resp.StatusCode)

	t.Logf("write one tuple")
	data = []byte(`{"writes":{"tuple_keys":[{"object":"group:fga","relation":"member","user":"user:a"}]}}`)
	resp, _ = http.Post(fmt.Sprintf("http://%s/stores/%s/write", s.URL, store), "application/json", bytes.NewBuffer(data))
	require.Equal(t, 200, resp.StatusCode)

	t.Logf("downgrade database to version 3")
	migrateCommand.SetArgs([]string{"--verbose", "--datastore-engine", engine, "--datastore-uri", uri, "--version", strconv.Itoa(4)})
	err = migrateCommand.Execute()
	require.NoError(t, err)

	t.Logf("writing another tuple")
	data = []byte(`{"writes":{"tuple_keys":[{"object":"group:fga","relation":"member","user":"user:b"}]}}`)
	resp, _ = http.Post(fmt.Sprintf("http://%s/stores/%s/write", s.URL, store), "application/json", bytes.NewBuffer(data))
	require.Equal(t, 200, resp.StatusCode, fmt.Sprintf("received %s", resp.Status))

	t.Logf("read changes")
	resp, _ = http.Get(fmt.Sprintf("http://%s/stores/%s/changes", s.URL, store))
	require.Equal(t, 200, resp.StatusCode)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var readChangesResponse openfgapb.ReadChangesResponse
	err = protojson.Unmarshal(body, &readChangesResponse)
	require.NoError(t, err)
	require.Equal(t, 2, len(readChangesResponse.Changes))
}

func TestReadAuthorizationModelPostgresSpecificCases(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI(true)
	ds, err := New(uri, sqlcommon.NewConfig())
	require.NoError(t, err)

	ctx := context.Background()
	defer ds.Close()
	store := "store"
	modelID := "foo"
	schemaVersion := "7.8"

	bytes, err := proto.Marshal(&openfgapb.TypeDefinition{Type: "document"})
	require.NoError(t, err)

	_, err = ds.db.ExecContext(ctx, "INSERT INTO authorization_model (store, authorization_model_id, schema_version, type, type_definition) VALUES ($1, $2, $3, $4, $5)", store, modelID, schemaVersion, "document", bytes)
	require.NoError(t, err)

	model, err := ds.ReadAuthorizationModel(ctx, store, modelID)
	require.NoError(t, err)
	require.Equal(t, typesystem.SchemaVersion1_0, model.SchemaVersion)
}

// TestReadEnsureNoOrder asserts that the read response is not ordered by ulid
func TestReadEnsureNoOrder(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI(true)
	ds, err := New(uri, sqlcommon.NewConfig())
	require.NoError(t, err)
	defer ds.Close()

	ctx := context.Background()

	store := "store"
	firstTuple := tuple.NewTupleKey("doc:object_id_1", "relation", "user:user_1")
	secondTuple := tuple.NewTupleKey("doc:object_id_2", "relation", "user:user_2")

	err = sqlcommon.Write(ctx,
		sqlcommon.NewDBInfo(ds.db, ds.stbl, "NOW()"),
		store,
		[]*openfgapb.TupleKey{},
		[]*openfgapb.TupleKey{firstTuple},
		time.Now())
	require.NoError(t, err)

	// tweak time so that ULID is smaller
	err = sqlcommon.Write(ctx,
		sqlcommon.NewDBInfo(ds.db, ds.stbl, "NOW()"),
		store,
		[]*openfgapb.TupleKey{},
		[]*openfgapb.TupleKey{secondTuple},
		time.Now().Add(time.Minute*-1))
	require.NoError(t, err)

	iter, err := ds.Read(ctx,
		store, tuple.
			NewTupleKey("doc:", "relation", ""))
	defer iter.Stop()
	require.NoError(t, err)

	// we expect that objectID1 will return first because it is inserted first
	curTuple, err := iter.Next()
	require.NoError(t, err)
	require.Equal(t, firstTuple, curTuple.Key)

	curTuple, err = iter.Next()
	require.NoError(t, err)
	require.Equal(t, secondTuple, curTuple.Key)

}

// TestReadPageEnsureNoOrder asserts that the read page is ordered by ulid
func TestReadPageEnsureOrder(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI(true)
	ds, err := New(uri, sqlcommon.NewConfig())
	require.NoError(t, err)
	defer ds.Close()

	ctx := context.Background()

	store := "store"
	firstTuple := tuple.NewTupleKey("doc:object_id_1", "relation", "user:user_1")
	secondTuple := tuple.NewTupleKey("doc:object_id_2", "relation", "user:user_2")

	err = sqlcommon.Write(ctx,
		sqlcommon.NewDBInfo(ds.db, ds.stbl, "NOW()"),
		store,
		[]*openfgapb.TupleKey{},
		[]*openfgapb.TupleKey{firstTuple},
		time.Now())
	require.NoError(t, err)

	// tweak time so that ULID is smaller
	err = sqlcommon.Write(ctx,
		sqlcommon.NewDBInfo(ds.db, ds.stbl, "NOW()"),
		store,
		[]*openfgapb.TupleKey{},
		[]*openfgapb.TupleKey{secondTuple},
		time.Now().Add(time.Minute*-1))
	require.NoError(t, err)

	tuples, _, err := ds.ReadPage(ctx,
		store,
		tuple.NewTupleKey("doc:", "relation", ""),
		storage.NewPaginationOptions(0, ""))
	require.NoError(t, err)

	require.Equal(t, 2, len(tuples))
	// we expect that objectID2 will return first because it has a smaller ulid
	require.Equal(t, secondTuple, tuples[0].Key)
	require.Equal(t, firstTuple, tuples[1].Key)

}
