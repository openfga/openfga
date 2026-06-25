package sqlcommon

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
)

// fakeRows is a data-yielding Rows implementation for exercising the SQL scanners
// without a live database.
type fakeRows struct {
	data     [][]any
	idx      int
	scanErr  error
	errAfter error // returned by Err() after iteration is exhausted
	closed   bool
}

func (r *fakeRows) Close() error { r.closed = true; return nil }

func (r *fakeRows) Err() error { return r.errAfter }

func (r *fakeRows) Next() bool {
	if r.idx >= len(r.data) {
		return false
	}
	r.idx++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	row := r.data[r.idx-1]
	if len(row) != len(dest) {
		return fmt.Errorf("scan column count mismatch: have %d want %d", len(row), len(dest))
	}
	for i, d := range dest {
		switch p := d.(type) {
		case *string:
			*p = row[i].(string)
		case *sql.NullString:
			*p = row[i].(sql.NullString)
		case *[]byte:
			if row[i] == nil {
				*p = nil
			} else {
				*p = row[i].([]byte)
			}
		case *time.Time:
			*p = row[i].(time.Time)
		default:
			return fmt.Errorf("unsupported scan target %T", d)
		}
	}
	return nil
}

type fakeRowGetter struct {
	rows *fakeRows
	err  error
}

func (g *fakeRowGetter) GetRows(_ context.Context) (Rows, error) {
	if g.err != nil {
		return nil, g.err
	}
	return g.rows, nil
}

func tupleRow(store, objType, objID, relation, user, ulidVal string, condName sql.NullString, condCtx []byte) []any {
	return []any{store, objType, objID, relation, user, condName, condCtx, ulidVal, time.Now()}
}

func passthroughErr(err error, _ ...interface{}) error { return err }

func TestSQLContinuationTokenSerializer(t *testing.T) {
	s := NewSQLContinuationTokenSerializer()

	t.Run("round_trip", func(t *testing.T) {
		b, err := s.Serialize("01ULID", "document")
		require.NoError(t, err)

		gotUlid, gotType, err := s.Deserialize(string(b))
		require.NoError(t, err)
		require.Equal(t, "01ULID", gotUlid)
		require.Equal(t, "document", gotType)
	})

	t.Run("empty_ulid_errors", func(t *testing.T) {
		_, err := s.Serialize("", "document")
		require.Error(t, err)
	})

	t.Run("invalid_token_errors", func(t *testing.T) {
		_, _, err := s.Deserialize("not-json")
		require.ErrorIs(t, err, storage.ErrInvalidContinuationToken)
	})
}

func TestNewContToken(t *testing.T) {
	tok := NewContToken("u", "document")
	require.Equal(t, "u", tok.Ulid)
	require.Equal(t, "document", tok.ObjectType)
}

func TestSQLIteratorColumns(t *testing.T) {
	cols := SQLIteratorColumns()
	require.Equal(t, []string{
		"store", "object_type", "object_id", "relation", "_user",
		"condition_name", "condition_context", "ulid", "inserted_at",
	}, cols)
}

func TestMakeTupleLockKeys(t *testing.T) {
	t.Run("dedupes_and_sorts", func(t *testing.T) {
		writes := storage.Writes{
			tupleUtils.NewTupleKey("document:2", "viewer", "user:bob"),
			tupleUtils.NewTupleKey("document:1", "viewer", "user:alice"),
			tupleUtils.NewTupleKey("document:1", "viewer", "user:alice"), // duplicate
		}
		deletes := storage.Deletes{
			&openfgav1.TupleKeyWithoutCondition{Object: "document:1", Relation: "editor", User: "user:alice"},
		}

		keys := MakeTupleLockKeys(deletes, writes)
		require.Len(t, keys, 3) // duplicate removed

		// Verify deterministic sort order: document:1/editor, document:1/viewer, document:2/viewer.
		require.Equal(t, "1", keys[0].objectID)
		require.Equal(t, "editor", keys[0].relation)
		require.Equal(t, "1", keys[1].objectID)
		require.Equal(t, "viewer", keys[1].relation)
		require.Equal(t, "2", keys[2].objectID)
	})

	t.Run("empty_input", func(t *testing.T) {
		require.Empty(t, MakeTupleLockKeys(storage.Deletes{}, storage.Writes{}))
	})
}

func TestBuildRowConstructorIN(t *testing.T) {
	t.Run("empty_keys", func(t *testing.T) {
		expr, args := BuildRowConstructorIN(nil)
		require.Empty(t, expr)
		require.Nil(t, args)
	})

	t.Run("multiple_keys", func(t *testing.T) {
		keys := MakeTupleLockKeys(storage.Deletes{}, storage.Writes{
			tupleUtils.NewTupleKey("document:1", "viewer", "user:alice"),
			tupleUtils.NewTupleKey("document:2", "viewer", "user:bob"),
		})
		expr, args := BuildRowConstructorIN(keys)
		require.Equal(t, "((?,?,?,?,?),(?,?,?,?,?))", expr)
		require.Len(t, args, 10) // 2 keys * 5 columns
	})
}

func TestNewDBInfo(t *testing.T) {
	t.Run("valid_dialect", func(t *testing.T) {
		info := NewDBInfo(sq.StatementBuilder, passthroughErr, "sqlite3")
		require.NotNil(t, info)
		require.NotNil(t, info.HandleSQLError)
	})

	t.Run("invalid_dialect_panics", func(t *testing.T) {
		require.Panics(t, func() {
			NewDBInfo(sq.StatementBuilder, passthroughErr, "not-a-dialect")
		})
	})
}

func TestAddFromUlid(t *testing.T) {
	base := sq.Select("ulid").From("tuple")

	t.Run("ascending_uses_gt", func(t *testing.T) {
		sqlStr, args, err := AddFromUlid(base, "01ABC", false).ToSql()
		require.NoError(t, err)
		require.Contains(t, sqlStr, "ulid >")
		require.Equal(t, []interface{}{"01ABC"}, args)
	})

	t.Run("descending_uses_lt", func(t *testing.T) {
		sqlStr, args, err := AddFromUlid(base, "01ABC", true).ToSql()
		require.NoError(t, err)
		require.Contains(t, sqlStr, "ulid <")
		require.Equal(t, []interface{}{"01ABC"}, args)
	})
}

func TestIsVersionReady_SkipCheck(t *testing.T) {
	status, err := IsVersionReady(context.Background(), true, nil)
	require.NoError(t, err)
	require.True(t, status.IsReady)
}

func newTupleIterator(rows *fakeRows) *SQLTupleIterator {
	return NewSQLTupleIterator(&fakeRowGetter{rows: rows}, passthroughErr)
}

func TestSQLTupleIterator_Next(t *testing.T) {
	t.Run("iterates_all_then_done", func(t *testing.T) {
		rows := &fakeRows{data: [][]any{
			tupleRow("store", "document", "1", "viewer", "user:alice", "01A", sql.NullString{}, nil),
			tupleRow("store", "document", "2", "viewer", "user:bob", "01B", sql.NullString{}, nil),
		}}
		it := newTupleIterator(rows)
		defer it.Stop()

		tup, err := it.Next(context.Background())
		require.NoError(t, err)
		require.Equal(t, "document:1", tup.GetKey().GetObject())

		tup, err = it.Next(context.Background())
		require.NoError(t, err)
		require.Equal(t, "document:2", tup.GetKey().GetObject())

		_, err = it.Next(context.Background())
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("context_canceled", func(t *testing.T) {
		it := newTupleIterator(&fakeRows{})
		defer it.Stop()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := it.Next(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("fetch_error_is_propagated", func(t *testing.T) {
		wantErr := errors.New("db down")
		it := NewSQLTupleIterator(&fakeRowGetter{err: wantErr}, passthroughErr)
		defer it.Stop()
		_, err := it.Next(context.Background())
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("scan_error_is_propagated", func(t *testing.T) {
		wantErr := errors.New("scan boom")
		rows := &fakeRows{
			data:    [][]any{tupleRow("s", "document", "1", "viewer", "user:a", "01A", sql.NullString{}, nil)},
			scanErr: wantErr,
		}
		it := newTupleIterator(rows)
		defer it.Stop()
		_, err := it.Next(context.Background())
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("rows_err_after_exhaustion", func(t *testing.T) {
		wantErr := errors.New("iteration error")
		rows := &fakeRows{data: [][]any{}, errAfter: wantErr}
		it := newTupleIterator(rows)
		defer it.Stop()
		_, err := it.Next(context.Background())
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("decodes_condition_context", func(t *testing.T) {
		ctxStruct, err := structpb.NewStruct(map[string]interface{}{"ip": "10.0.0.1"})
		require.NoError(t, err)
		marshalled, err := proto.Marshal(ctxStruct)
		require.NoError(t, err)

		rows := &fakeRows{data: [][]any{
			tupleRow("store", "document", "1", "viewer", "user:alice", "01A",
				sql.NullString{String: "in_cidr", Valid: true}, marshalled),
		}}
		it := newTupleIterator(rows)
		defer it.Stop()

		tup, err := it.Next(context.Background())
		require.NoError(t, err)
		require.Equal(t, "in_cidr", tup.GetKey().GetCondition().GetName())
	})
}

func TestSQLTupleIterator_Head(t *testing.T) {
	t.Run("head_peeks_without_advancing", func(t *testing.T) {
		rows := &fakeRows{data: [][]any{
			tupleRow("store", "document", "1", "viewer", "user:alice", "01A", sql.NullString{}, nil),
			tupleRow("store", "document", "2", "viewer", "user:bob", "01B", sql.NullString{}, nil),
		}}
		it := newTupleIterator(rows)
		defer it.Stop()

		// Head twice yields the same first item.
		h1, err := it.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, "document:1", h1.GetKey().GetObject())

		h2, err := it.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, "document:1", h2.GetKey().GetObject())

		// Next then returns the same first item, not the second.
		n1, err := it.Next(context.Background())
		require.NoError(t, err)
		require.Equal(t, "document:1", n1.GetKey().GetObject())

		n2, err := it.Next(context.Background())
		require.NoError(t, err)
		require.Equal(t, "document:2", n2.GetKey().GetObject())
	})

	t.Run("head_on_empty_returns_done", func(t *testing.T) {
		it := newTupleIterator(&fakeRows{data: [][]any{}})
		defer it.Stop()
		_, err := it.Head(context.Background())
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("context_canceled", func(t *testing.T) {
		it := newTupleIterator(&fakeRows{})
		defer it.Stop()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := it.Head(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestSQLTupleIterator_ToArray(t *testing.T) {
	t.Run("returns_continuation_token_when_more_remain", func(t *testing.T) {
		rows := &fakeRows{data: [][]any{
			tupleRow("store", "document", "1", "viewer", "user:a", "01A", sql.NullString{}, nil),
			tupleRow("store", "document", "2", "viewer", "user:b", "01B", sql.NullString{}, nil),
			tupleRow("store", "document", "3", "viewer", "user:c", "01C", sql.NullString{}, nil),
		}}
		it := newTupleIterator(rows)
		defer it.Stop()

		res, token, err := it.ToArray(context.Background(), storage.PaginationOptions{PageSize: 2})
		require.NoError(t, err)
		require.Len(t, res, 2)
		require.Equal(t, "01C", token) // ulid of the lookahead element
	})

	t.Run("no_token_when_exhausted", func(t *testing.T) {
		rows := &fakeRows{data: [][]any{
			tupleRow("store", "document", "1", "viewer", "user:a", "01A", sql.NullString{}, nil),
		}}
		it := newTupleIterator(rows)
		defer it.Stop()

		res, token, err := it.ToArray(context.Background(), storage.PaginationOptions{PageSize: 5})
		require.NoError(t, err)
		require.Len(t, res, 1)
		require.Empty(t, token)
	})
}

func TestSQLTupleIterator_StopAndIsOrdered(t *testing.T) {
	rows := &fakeRows{data: [][]any{
		tupleRow("store", "document", "1", "viewer", "user:a", "01A", sql.NullString{}, nil),
	}}
	it := newTupleIterator(rows)

	require.False(t, it.IsOrdered())

	_, err := it.Next(context.Background())
	require.NoError(t, err)

	it.Stop()
	require.True(t, rows.closed)
}

func modelRow(modelID, schema, typeName string, typeDef, model []byte) []any {
	var td any = typeDef
	var m any = model
	return []any{modelID, schema, typeName, td, m}
}

func TestConstructAuthorizationModelFromSQLRows(t *testing.T) {
	userType := &openfgav1.TypeDefinition{Type: "user"}
	userTypeBytes, err := proto.Marshal(userType)
	require.NoError(t, err)

	docType := &openfgav1.TypeDefinition{Type: "document"}
	docTypeBytes, err := proto.Marshal(docType)
	require.NoError(t, err)

	t.Run("prefers_serialized_model_when_present", func(t *testing.T) {
		fullModel := &openfgav1.AuthorizationModel{
			Id:              "01MODEL",
			SchemaVersion:   "1.1",
			TypeDefinitions: []*openfgav1.TypeDefinition{userType},
		}
		modelBytes, err := proto.Marshal(fullModel)
		require.NoError(t, err)

		rows := &fakeRows{data: [][]any{
			modelRow("01MODEL", "1.1", "user", userTypeBytes, modelBytes),
		}}

		got, err := ConstructAuthorizationModelFromSQLRows(rows)
		require.NoError(t, err)
		require.Equal(t, "01MODEL", got.GetId())
		require.Len(t, got.GetTypeDefinitions(), 1)
	})

	t.Run("assembles_type_defs_across_rows", func(t *testing.T) {
		rows := &fakeRows{data: [][]any{
			modelRow("01MODEL", "1.1", "user", userTypeBytes, nil),
			modelRow("01MODEL", "1.1", "document", docTypeBytes, nil),
			modelRow("01OTHER", "1.1", "folder", docTypeBytes, nil), // different model id -> breaks loop
		}}

		got, err := ConstructAuthorizationModelFromSQLRows(rows)
		require.NoError(t, err)
		require.Equal(t, "01MODEL", got.GetId())
		require.Equal(t, "1.1", got.GetSchemaVersion())
		require.Len(t, got.GetTypeDefinitions(), 2)
	})

	t.Run("empty_rows_returns_not_found", func(t *testing.T) {
		_, err := ConstructAuthorizationModelFromSQLRows(&fakeRows{data: [][]any{}})
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("scan_error", func(t *testing.T) {
		rows := &fakeRows{
			data:    [][]any{modelRow("01MODEL", "1.1", "user", userTypeBytes, nil)},
			scanErr: errors.New("scan failed"),
		}
		_, err := ConstructAuthorizationModelFromSQLRows(rows)
		require.Error(t, err)
	})

	t.Run("rows_err", func(t *testing.T) {
		rows := &fakeRows{data: [][]any{}, errAfter: errors.New("iteration failed")}
		_, err := ConstructAuthorizationModelFromSQLRows(rows)
		require.Error(t, err)
	})

	t.Run("invalid_typedef_proto", func(t *testing.T) {
		rows := &fakeRows{data: [][]any{
			modelRow("01MODEL", "1.1", "user", []byte("not-a-proto"), nil),
		}}
		_, err := ConstructAuthorizationModelFromSQLRows(rows)
		require.Error(t, err)
	})
}
