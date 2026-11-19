package storagewrappers

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	_ "modernc.org/sqlite"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage/adapter"
	"github.com/openfga/openfga/pkg/storage/adapter/query"
	"github.com/openfga/openfga/pkg/storage/adapter/sqlite"
)

// recordingQuerier is an adapter.Querier that records the context passed to Execute.
type recordingQuerier struct {
	adapter.Querier
	ctx context.Context
}

func (q *recordingQuerier) Execute(ctx context.Context, stmt *query.Statement) (adapter.Rows, error) {
	q.ctx = ctx
	return q.Querier.Execute(ctx, stmt)
}

func TestContextTracerWrapperQuerier(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("returns nil when the delegate returns nil", func(t *testing.T) {
		ds := mocks.NewMockOpenFGADatastore(ctrl)
		ds.EXPECT().Querier(openfgav1.ConsistencyPreference_UNSPECIFIED).Return(nil)
		w := NewContextWrapper(ds)
		require.Nil(t, w.Querier(openfgav1.ConsistencyPreference_UNSPECIFIED))
	})

	t.Run("wraps a non-nil delegate Querier", func(t *testing.T) {
		ds := mocks.NewMockOpenFGADatastore(ctrl)
		inner := &fakeQuerier{}
		ds.EXPECT().Querier(openfgav1.ConsistencyPreference_UNSPECIFIED).Return(inner)
		w := NewContextWrapper(ds)

		got := w.Querier(openfgav1.ConsistencyPreference_UNSPECIFIED)
		cq, ok := got.(*contextQuerier)
		require.True(t, ok)
		require.Same(t, inner, cq.Querier)
	})
}

func TestContextQuerierExecute(t *testing.T) {
	t.Run("no request deadline means no query deadline", func(t *testing.T) {
		inner := &recordingQuerier{Querier: &fakeQuerier{rows: &fakeRows{remaining: 1}}}
		q := &contextQuerier{Querier: inner}

		rows, err := q.Execute(context.Background(), nil)
		require.NoError(t, err)
		_, ok := inner.ctx.Deadline()
		require.False(t, ok)

		require.True(t, rows.Next())
		require.False(t, rows.Next())
		require.NoError(t, rows.Close())
	})

	t.Run("request deadline is honored", func(t *testing.T) {
		inner := &recordingQuerier{Querier: &fakeQuerier{rows: &fakeRows{}}}
		q := &contextQuerier{Querier: inner}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		want, _ := ctx.Deadline()

		rows, err := q.Execute(ctx, nil)
		require.NoError(t, err)
		defer rows.Close()

		got, ok := inner.ctx.Deadline()
		require.True(t, ok)
		require.Equal(t, want, got)

		select {
		case <-inner.ctx.Done():
		case <-time.After(time.Second):
			t.Fatal("query context was not cancelled at the request deadline")
		}
		require.ErrorIs(t, inner.ctx.Err(), context.DeadlineExceeded)
	})

	t.Run("request cancellation is ignored", func(t *testing.T) {
		for name, newCtx := range map[string]func() (context.Context, context.CancelFunc){
			"without deadline": func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			"with deadline": func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), time.Minute)
			},
		} {
			t.Run(name, func(t *testing.T) {
				inner := &recordingQuerier{Querier: &fakeQuerier{rows: &fakeRows{remaining: 1}}}
				q := &contextQuerier{Querier: inner}

				ctx, cancel := newCtx()
				rows, err := q.Execute(ctx, nil)
				require.NoError(t, err)
				defer rows.Close()

				cancel()
				require.NoError(t, inner.ctx.Err())
				require.True(t, rows.Next())
			})
		}
	})

	t.Run("request values are preserved", func(t *testing.T) {
		type ctxKey struct{}
		inner := &recordingQuerier{Querier: &fakeQuerier{rows: &fakeRows{}}}
		q := &contextQuerier{Querier: inner}

		rows, err := q.Execute(context.WithValue(context.Background(), ctxKey{}, "v"), nil)
		require.NoError(t, err)
		defer rows.Close()

		require.Equal(t, "v", inner.ctx.Value(ctxKey{}))
	})

	t.Run("query context is released", func(t *testing.T) {
		t.Run("on Execute error", func(t *testing.T) {
			wantErr := errors.New("boom")
			inner := &recordingQuerier{Querier: &fakeQuerier{err: wantErr}}
			q := &contextQuerier{Querier: inner}

			rows, err := q.Execute(context.Background(), nil)
			require.ErrorIs(t, err, wantErr)
			require.Nil(t, rows)
			require.ErrorIs(t, inner.ctx.Err(), context.Canceled)
		})

		t.Run("when rows are exhausted", func(t *testing.T) {
			inner := &recordingQuerier{Querier: &fakeQuerier{rows: &fakeRows{remaining: 1}}}
			q := &contextQuerier{Querier: inner}

			rows, err := q.Execute(context.Background(), nil)
			require.NoError(t, err)
			defer rows.Close()

			require.True(t, rows.Next())
			require.NoError(t, inner.ctx.Err())
			require.False(t, rows.Next())
			require.ErrorIs(t, inner.ctx.Err(), context.Canceled)
		})

		t.Run("when rows are closed early", func(t *testing.T) {
			innerRows := &fakeRows{remaining: 2}
			inner := &recordingQuerier{Querier: &fakeQuerier{rows: innerRows}}
			q := &contextQuerier{Querier: inner}

			rows, err := q.Execute(context.Background(), nil)
			require.NoError(t, err)

			require.True(t, rows.Next())
			require.NoError(t, rows.Close())
			require.True(t, innerRows.closed)
			require.ErrorIs(t, inner.ctx.Err(), context.Canceled)
		})

		t.Run("Close after exhaustion is safe", func(t *testing.T) {
			innerRows := &fakeRows{}
			q := &contextQuerier{Querier: &fakeQuerier{rows: innerRows}}

			rows, err := q.Execute(context.Background(), nil)
			require.NoError(t, err)

			require.False(t, rows.Next())
			require.NoError(t, rows.Close())
			require.True(t, innerRows.closed)
		})
	})
}

func TestContextQuerierSQLite(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1) // each :memory: connection is a separate database
	_, err = db.Exec("CREATE TABLE tuple (object_id TEXT)")
	require.NoError(t, err)
	for _, id := range []string{"1", "2", "3"} {
		_, err := db.Exec("INSERT INTO tuple (object_id) VALUES (?)", id)
		require.NoError(t, err)
	}

	a := query.NewTuple("a")
	stmt := query.Select(a.ObjectID()).From(a).OrderBy(query.Asc(a.ObjectID()))
	inner := &recordingQuerier{Querier: sqlite.New(db)}
	q := &contextQuerier{Querier: inner}

	ctx, cancel := context.WithCancel(context.Background())
	rows, err := q.Execute(ctx, stmt)
	require.NoError(t, err)
	defer rows.Close()

	// Cancelling the request mid-iteration must not reach the context database/sql
	// watches, otherwise it would asynchronously close the rows.
	require.True(t, rows.Next())
	cancel()
	require.NoError(t, inner.ctx.Err())

	got := []string{}
	for ok := true; ok; ok = rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		got = append(got, id)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{"1", "2", "3"}, got)
}
