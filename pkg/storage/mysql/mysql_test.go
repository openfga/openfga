package mysql

import (
	"context"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	"github.com/openfga/openfga/pkg/storage/test"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/stretchr/testify/require"
)

func TestMySQLDatastore(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "mysql")

	uri := testDatastore.GetConnectionURI(true)
	ds, err := New(uri, sqlcommon.NewConfig())
	require.NoError(t, err)
	defer ds.Close()
	test.RunAllTests(t, ds)
}

// TestReadEnsureNoOrder asserts that the read response is not ordered by ulid
func TestReadEnsureNoOrder(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "mysql")

	uri := testDatastore.GetConnectionURI(true)
	ds, err := New(uri, sqlcommon.NewConfig())
	require.NoError(t, err)
	defer ds.Close()

	ctx := context.Background()
	store := "store"
	firstTuple := tuple.NewTupleKey("doc:object_id_1", "relation", "user:user_1")
	secondTuple := tuple.NewTupleKey("doc:object_id_2", "relation", "user:user_2")

	err = sqlcommon.Write(ctx,
		sqlcommon.NewDBInfo(ds.db, ds.stbl, sq.Expr("NOW()")),
		store,
		[]*openfgav1.TupleKey{},
		[]*openfgav1.TupleKey{firstTuple},
		time.Now())
	require.NoError(t, err)

	// tweak time so that ULID is smaller
	err = sqlcommon.Write(ctx,
		sqlcommon.NewDBInfo(ds.db, ds.stbl, sq.Expr("NOW()")),
		store,
		[]*openfgav1.TupleKey{},
		[]*openfgav1.TupleKey{secondTuple},
		time.Now().Add(time.Minute*-1))
	require.NoError(t, err)

	iter, err := ds.Read(ctx,
		store,
		tuple.NewTupleKey("doc:", "relation", ""))
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
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "mysql")

	uri := testDatastore.GetConnectionURI(true)
	ds, err := New(uri, sqlcommon.NewConfig())
	require.NoError(t, err)
	defer ds.Close()

	ctx := context.Background()

	store := "store"
	firstTuple := tuple.NewTupleKey("doc:object_id_1", "relation", "user:user_1")
	secondTuple := tuple.NewTupleKey("doc:object_id_2", "relation", "user:user_2")

	err = sqlcommon.Write(ctx,
		sqlcommon.NewDBInfo(ds.db, ds.stbl, sq.Expr("NOW()")),
		store,
		[]*openfgav1.TupleKey{},
		[]*openfgav1.TupleKey{firstTuple},
		time.Now())
	require.NoError(t, err)

	// tweak time so that ULID is smaller
	err = sqlcommon.Write(ctx,
		sqlcommon.NewDBInfo(ds.db, ds.stbl, sq.Expr("NOW()")),
		store,
		[]*openfgav1.TupleKey{},
		[]*openfgav1.TupleKey{secondTuple},
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
