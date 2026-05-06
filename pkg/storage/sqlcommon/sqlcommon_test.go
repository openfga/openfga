package sqlcommon

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
)

// changelogOperationIndex is the index of the "operation" field within each
// changeLogItems row produced by GetDeleteWriteChangelogItems.
// The row layout is: store, object_type, object_id, relation,
// _user, condition_name, condition_context, operation, ulid, inserted_at.
const changelogOperationIndex = 7

// Background: pgx uses the fmt.Stringer interface to encode values when it
// falls back to the "simple query" protocol (e.g. when behind PgBouncer or
// Azure Flexible Server's built-in connection pooler). Because
// openfgav1.TupleOperation implements fmt.Stringer it would be serialised as
// "TUPLE_OPERATION_WRITE" / "TUPLE_OPERATION_DELETE" — strings that PostgreSQL
// rejects for an INTEGER column (SQLSTATE 22P02). Passing a bare int32 avoids
// this code path entirely and is safe for both binary and text wire formats.
// See: https://github.com/openfga/openfga/issues/3011
func TestGetDeleteWriteChangelogItems_OperationIsInt32(t *testing.T) {
	t.Parallel()

	store := "test-store"
	now := time.Now()

	t.Run("write operation type is int32", func(t *testing.T) {
		t.Parallel()

		tk := tupleUtils.NewTupleKey("document:1", "viewer", "user:alice")
		_, _, changeLogItems, err := GetDeleteWriteChangelogItems(
			store,
			map[string]*openfgav1.Tuple{}, // no pre-existing tuples
			WriteData{
				Deletes: storage.Deletes{},
				Writes:  storage.Writes{tk},
				Opts:    storage.NewTupleWriteOptions(),
				Now:     now,
			},
		)
		require.NoError(t, err)
		require.Len(t, changeLogItems, 1)

		opVal := changeLogItems[0][changelogOperationIndex]

		assert.Equal(t, reflect.TypeOf(int32(0)), reflect.TypeOf(opVal),
			"operation value must be int32, got %T", opVal)
		assert.Equal(t, int32(openfgav1.TupleOperation_TUPLE_OPERATION_WRITE), opVal)
	})

	t.Run("delete operation type is int32", func(t *testing.T) {
		t.Parallel()

		tk := &openfgav1.TupleKeyWithoutCondition{Object: "document:1", Relation: "viewer", User: "user:alice"}
		existingTuple := &openfgav1.Tuple{
			Key: tupleUtils.NewTupleKey("document:1", "viewer", "user:alice"),
		}
		existing := map[string]*openfgav1.Tuple{
			tupleUtils.TupleKeyToString(existingTuple.GetKey()): existingTuple,
		}

		_, _, changeLogItems, err := GetDeleteWriteChangelogItems(
			store,
			existing,
			WriteData{
				Deletes: storage.Deletes{tk},
				Writes:  storage.Writes{},
				Opts:    storage.NewTupleWriteOptions(),
				Now:     now,
			},
		)
		require.NoError(t, err)
		require.Len(t, changeLogItems, 1)

		opVal := changeLogItems[0][changelogOperationIndex]

		assert.Equal(t, reflect.TypeOf(int32(0)), reflect.TypeOf(opVal),
			"operation value must be int32, got %T", opVal)
		assert.Equal(t, int32(openfgav1.TupleOperation_TUPLE_OPERATION_DELETE), opVal)
	})

	t.Run("mixed write and delete both produce int32 operations", func(t *testing.T) {
		t.Parallel()

		writeTk := tupleUtils.NewTupleKey("document:2", "editor", "user:bob")
		deleteTk := &openfgav1.TupleKeyWithoutCondition{Object: "document:1", Relation: "viewer", User: "user:alice"}
		existingTuple := &openfgav1.Tuple{
			Key: tupleUtils.NewTupleKey("document:1", "viewer", "user:alice"),
		}
		existing := map[string]*openfgav1.Tuple{
			tupleUtils.TupleKeyToString(existingTuple.GetKey()): existingTuple,
		}

		_, _, changeLogItems, err := GetDeleteWriteChangelogItems(
			store,
			existing,
			WriteData{
				Deletes: storage.Deletes{deleteTk},
				Writes:  storage.Writes{writeTk},
				Opts:    storage.NewTupleWriteOptions(),
				Now:     now,
			},
		)
		require.NoError(t, err)
		require.Len(t, changeLogItems, 2)

		for i, item := range changeLogItems {
			opVal := item[changelogOperationIndex]
			assert.Equal(t, reflect.TypeOf(int32(0)), reflect.TypeOf(opVal),
				"changeLogItems[%d] operation must be int32, got %T", i, opVal)

			intVal, ok := opVal.(int32)
			require.True(t, ok, "changeLogItems[%d] operation could not be asserted as int32", i)
			assert.Contains(t,
				[]int32{
					int32(openfgav1.TupleOperation_TUPLE_OPERATION_WRITE),
					int32(openfgav1.TupleOperation_TUPLE_OPERATION_DELETE),
				},
				intVal,
			)
		}
	})
}

// stubRowGetter is a minimal SQLIteratorRowGetter for testing fetchBuffer.
type stubRowGetter struct {
	rows Rows
	err  error
}

func (s *stubRowGetter) GetRows(_ context.Context) (Rows, error) {
	return s.rows, s.err
}

// stubRows is a no-op Rows implementation.
type stubRows struct{}

func (r *stubRows) Close() error      { return nil }
func (r *stubRows) Err() error        { return nil }
func (r *stubRows) Next() bool        { return false }
func (r *stubRows) Scan(...any) error { return nil }

func sqlIterQuerySampleCount(t *testing.T, successVal string) uint64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == "openfga_iter_query_duration_ms" {
			for _, m := range mf.GetMetric() {
				for _, l := range m.GetLabel() {
					if l.GetName() == "success" && l.GetValue() == successVal {
						h := m.GetHistogram()
						// SampleCountFloat overrides SampleCount when native histograms are active.
						if f := h.GetSampleCountFloat(); f > 0 {
							return uint64(f)
						}
						return h.GetSampleCount()
					}
				}
			}
		}
	}
	return 0
}

// identityErrHandler passes errors through unchanged (no translation).
func identityErrHandler(err error, _ ...interface{}) error { return err }

func TestFetchBufferMetric(t *testing.T) {
	t.Run("success_records_true_label", func(t *testing.T) {
		iter := NewSQLTupleIterator(&stubRowGetter{rows: &stubRows{}}, identityErrHandler)
		before := sqlIterQuerySampleCount(t, "true")
		err := iter.fetchBuffer(context.Background())
		require.NoError(t, err)
		require.GreaterOrEqual(t, sqlIterQuerySampleCount(t, "true"), before+1)
	})

	t.Run("infrastructure_error_records_false_label", func(t *testing.T) {
		dbErr := errors.New("connection reset")
		iter := NewSQLTupleIterator(&stubRowGetter{err: dbErr}, identityErrHandler)
		before := sqlIterQuerySampleCount(t, "false")
		err := iter.fetchBuffer(context.Background())
		require.Error(t, err)
		require.GreaterOrEqual(t, sqlIterQuerySampleCount(t, "false"), before+1)
	})

	t.Run("not_found_error_records_true_label", func(t *testing.T) {
		// errHandler translates the raw error to the storage sentinel, as real backends do.
		notFoundHandler := func(err error, _ ...interface{}) error { return storage.ErrNotFound }
		iter := NewSQLTupleIterator(&stubRowGetter{err: errors.New("no rows")}, notFoundHandler)
		before := sqlIterQuerySampleCount(t, "true")
		err := iter.fetchBuffer(context.Background())
		require.ErrorIs(t, err, storage.ErrNotFound)
		require.GreaterOrEqual(t, sqlIterQuerySampleCount(t, "true"), before+1)
	})
}

// blockingRowGetter blocks in GetRows until the supplied context is done,
// emulating a slow / hanging database query.
type blockingRowGetter struct{}

func (blockingRowGetter) GetRows(ctx context.Context) (Rows, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestFetchBufferRespectsDeadline verifies that a deadline on the caller's
// context is honored by fetchBuffer even though the underlying parent
// cancellation is intentionally stripped to avoid poisoning the connection
// pool (see PR #2508). Regression test for issue #3098.
func TestFetchBufferRespectsDeadline(t *testing.T) {
	iter := NewSQLTupleIterator(blockingRowGetter{}, identityErrHandler)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := iter.fetchBuffer(ctx)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	// Generous upper bound — the call must not hang for the full test timeout.
	require.Less(t, elapsed, 5*time.Second, "fetchBuffer did not honor deadline; took %s", elapsed)
}
