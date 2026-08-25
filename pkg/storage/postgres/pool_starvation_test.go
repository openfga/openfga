package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/server/commands/reverseexpand"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/postgres"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// closeWithTimeout closes the datastore in the background. Closing a pgxpool
// blocks until every acquired connection is released, which never happens if a
// regression reintroduces the connection starvation these tests guard against,
// so a plain ds.Close() would hang test teardown.
func closeWithTimeout(t *testing.T, ds storage.OpenFGADatastore, d time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		ds.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Log("datastore.Close() timed out; connections are still held")
	}
}

// TestIteratorNextRespectsContextDuringPoolAcquire verifies that a tuple
// iterator's lazy query execution honors context cancellation while waiting
// for a pool connection.
//
// Regression test for https://github.com/openfga/openfga/pull/3255. Before
// that fix, SQLTupleIterator.fetchBuffer called context.WithoutCancel(ctx)
// around the whole fetch, so the pgxpool.Acquire underneath it blocked with no
// deadline: with an exhausted pool, Next() blocked indefinitely even though
// the caller's context had expired long ago. That was the primitive that
// turned pool contention into the permanent server-wide stall covered by
// TestReverseExpandPoolStarvationDeadlock below. The fix scopes the
// cancellation stripping to query execution only, keeping the wait for a
// connection cancellable.
func TestIteratorNextRespectsContextDuringPoolAcquire(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI(true)
	ds, err := postgres.New(uri, sqlcommon.NewConfig(sqlcommon.WithMaxOpenConns(1)))
	require.NoError(t, err)
	t.Cleanup(func() { closeWithTimeout(t, ds, 5*time.Second) })

	ctx := context.Background()
	store := ulid.Make().String()

	var writes []*openfgav1.TupleKey
	for i := 0; i < 5; i++ {
		writes = append(writes, tuple.NewTupleKey(fmt.Sprintf("document:%d", i), "viewer", "user:anne"))
	}
	require.NoError(t, ds.Write(ctx, store, nil, writes))

	// Hold the pool's only connection: iterators execute their query on the
	// first Next() and keep the connection until drained or stopped.
	holder, err := ds.Read(ctx, store, storage.ReadFilter{}, storage.ReadOptions{})
	require.NoError(t, err)
	defer holder.Stop()
	_, err = holder.Next(ctx)
	require.NoError(t, err)

	// A second reader whose context expires quickly must return an error
	// promptly instead of blocking until the first reader lets go.
	blockedCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	blocked, err := ds.Read(blockedCtx, store, storage.ReadFilter{}, storage.ReadOptions{})
	require.NoError(t, err)
	defer blocked.Stop()

	result := make(chan error, 1)
	go func() {
		_, err := blocked.Next(blockedCtx)
		result <- err
	}()

	select {
	case err := <-result:
		require.ErrorIs(t, err, context.DeadlineExceeded, "Next must fail with its context deadline once it expires")
	case <-time.After(5 * time.Second):
		holder.Stop() // release the connection so the goroutine can unwind
		t.Fatal("Next() blocked for 5s waiting for a pool connection, ignoring its 500ms context deadline")
	}
}

// TestReverseExpandPoolStarvationDeadlock guards against a permanent deadlock
// between reverse expansion (ListObjects) and the connection pool, fixed by
// https://github.com/openfga/openfga/pull/3255.
//
// The readTuplesAndExecute step holds an open tuple iterator (which pins a
// pool connection) while it dispatches one child expansion per row through a
// concurrency pool bounded by resolveNodeBreadthLimit. Each child immediately
// opens its own iterator, which needs a connection of its own. Before the fix:
//
//   - every child blocked in pgxpool.Acquire, unbounded, because
//     fetchBuffer stripped the context (see the test above);
//   - once breadthLimit children were in flight, the parent blocked in
//     pool.Go, so it could neither finish draining its iterator nor release
//     the connection the children were waiting for.
//
// Nothing in the cycle observed cancellation, so the request deadline never
// unwound it: the connections stayed checked out forever and every subsequent
// request that touched the datastore timed out until the server was restarted.
// Observed in production as a total stall of a v1.11.5 server with
// datastore-max-open-conns=10 under concurrent ListObjects load: all 10
// connections idle on Postgres yet permanently acquired in pgxpool, hundreds
// of goroutines parked in WaitGroup.Wait / pgxpool.Acquire.
//
// A single request against a single-connection pool was enough to reproduce
// it, which is what this test exercises: the expansion must return by its
// context deadline and the pool must be usable afterwards.
func TestReverseExpandPoolStarvationDeadlock(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI(true)
	ds, err := postgres.New(uri, sqlcommon.NewConfig(sqlcommon.WithMaxOpenConns(1)))
	require.NoError(t, err)
	t.Cleanup(func() { closeWithTimeout(t, ds, 5*time.Second) })

	ctx := context.Background()
	store := ulid.Make().String()

	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1

		type user

		type group
			relations
				define member: [user]

		type document
			relations
				define viewer: [user, group#member]`)

	ts, err := typesystem.New(model)
	require.NoError(t, err)

	// user:anne belongs to more groups than the default
	// resolveNodeBreadthLimit (10), so the parent expansion is guaranteed to
	// block dispatching children before it can drain its own iterator.
	var writes []*openfgav1.TupleKey
	for i := 0; i < 20; i++ {
		writes = append(writes,
			tuple.NewTupleKey(fmt.Sprintf("group:%d", i), "member", "user:anne"),
			tuple.NewTupleKey(fmt.Sprintf("document:%d", i), "viewer", fmt.Sprintf("group:%d#member", i)),
		)
	}
	require.NoError(t, ds.Write(ctx, store, nil, writes))

	requestCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	resultChan := make(chan *reverseexpand.ReverseExpandResult)
	go func() {
		for range resultChan {
		}
	}()

	execDone := make(chan error, 1)
	go func() {
		query := reverseexpand.NewReverseExpandQuery(ds, ts)
		execDone <- query.Execute(requestCtx, &reverseexpand.ReverseExpandRequest{
			StoreID:    store,
			ObjectType: "document",
			Relation:   "viewer",
			User: &reverseexpand.UserRefObject{
				Object: &openfgav1.Object{Type: "user", Id: "anne"},
			},
		}, resultChan, reverseexpand.NewResolutionMetadata())
	}()

	// The request context expires after 2s, so Execute must return by then.
	// Allow a generous margin before declaring it deadlocked.
	select {
	case err := <-execDone:
		require.ErrorIs(t, err, context.DeadlineExceeded, "Execute must return its context deadline error, not nil or an unrelated error")
	case <-time.After(10 * time.Second):
		t.Fatal("reverse expansion deadlocked: iterator holds the pool's only connection while its children wait for one, and the request deadline cannot unwind the cycle")
	}
	close(resultChan)

	// Returning is not enough: the pool must be usable again afterwards.
	// If the cancelled expansion leaked its connection, this read blocks
	// until its own deadline and the server is one request closer to a
	// total stall.
	probeCtx, probeCancel := context.WithTimeout(ctx, 3*time.Second)
	defer probeCancel()
	_, err = ds.ReadUserTuple(probeCtx, store, storage.ReadUserTupleFilter{
		Object:   "group:0",
		Relation: "member",
		User:     "user:anne",
	}, storage.ReadUserTupleOptions{})
	require.NoError(t, err, "datastore still starved after the request finished: the connection was never returned to the pool")
}
