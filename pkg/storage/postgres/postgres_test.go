package postgres

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/common"
	"github.com/openfga/openfga/pkg/storage/test"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/proto"
)

func TestPostgresDatastore(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI()
	ds, err := New(uri, common.NewConfig())
	require.NoError(t, err)
	defer ds.Close()
	test.RunAllTests(t, ds)
}

func TestReadAuthorizationModelPostgresSpecificCases(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI()
	ds, err := New(uri, common.NewConfig())
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

	uri := testDatastore.GetConnectionURI()
	ds, err := New(uri, common.NewConfig())
	require.NoError(t, err)

	ctx := context.Background()
	defer ds.Close()

	store := "store"
	objectType := "doc"
	userType := "user"
	objectID1 := "object_id_1"
	relation := "relation"
	user1 := "user:user_1"
	ulid1 := "zzz123"
	insertTime1 := "2023-04-04 01:00:00"

	// important that the ulid2 is smaller than ulid1
	objectID2 := "object_id_2"
	user2 := "user:user_2"
	ulid2 := "zzz100"
	insertTime2 := "2023-04-04 01:00:02"

	// we need to use db.ExecContext instead of using Write to control the ulid
	_, err = ds.db.ExecContext(ctx, "INSERT INTO tuple (store, object_type, object_id, relation, _user, user_type, ulid, inserted_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", store, objectType, objectID1, relation, user1, userType, ulid1, insertTime1)
	require.NoError(t, err)

	_, err = ds.db.ExecContext(ctx, "INSERT INTO tuple (store, object_type, object_id, relation, _user, user_type, ulid, inserted_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", store, objectType, objectID2, relation, user2, userType, ulid2, insertTime2)
	require.NoError(t, err)

	iter, err := ds.Read(ctx, store, tuple.NewTupleKey("doc:", relation, ""))
	defer iter.Stop()
	require.NoError(t, err)

	// we expect that objectID1 will return first because it is inserted first
	curTuple, err := iter.Next()
	require.NoError(t, err)
	require.Equal(t, tuple.NewTupleKey(objectType+":"+objectID1, relation, user1), curTuple.Key)

	curTuple, err = iter.Next()
	require.NoError(t, err)
	require.Equal(t, tuple.NewTupleKey(objectType+":"+objectID2, relation, user2), curTuple.Key)

}

// TestReadPageEnsureNoOrder asserts that the read page is ordered by ulid
func TestReadPageEnsureOrder(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI()
	ds, err := New(uri, common.NewConfig())
	require.NoError(t, err)

	ctx := context.Background()
	defer ds.Close()

	store := "store"
	objectType := "doc"
	userType := "user"
	objectID1 := "object_id_1"
	relation := "relation"
	user1 := "user:user_1"
	ulid1 := "zzz123"
	insertTime1 := "2023-04-04 02:00:00"

	// important that the ulid2 is smaller than ulid1
	objectID2 := "object_id_2"
	user2 := "user:user_2"
	ulid2 := "zzz100"
	insertTime2 := "2023-04-04 02:00:02"

	// we need to use db.ExecContext instead of using Write to control the ulid
	_, err = ds.db.ExecContext(ctx, "INSERT INTO tuple (store, object_type, object_id, relation, _user, user_type, ulid, inserted_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", store, objectType, objectID1, relation, user1, userType, ulid1, insertTime1)
	require.NoError(t, err)

	_, err = ds.db.ExecContext(ctx, "INSERT INTO tuple (store, object_type, object_id, relation, _user, user_type, ulid, inserted_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", store, objectType, objectID2, relation, user2, userType, ulid2, insertTime2)
	require.NoError(t, err)

	tuples, _, err := ds.ReadPage(ctx, store, tuple.NewTupleKey("doc:", relation, ""), storage.NewPaginationOptions(0, ""))
	require.NoError(t, err)

	require.Equal(t, 2, len(tuples))
	// we expect that objectID2 will return first because it has a smaller ulid
	require.Equal(t, tuple.NewTupleKey(objectType+":"+objectID2, relation, user2), tuples[0].Key)
	require.Equal(t, tuple.NewTupleKey(objectType+":"+objectID1, relation, user1), tuples[1].Key)

}
