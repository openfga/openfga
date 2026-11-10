package storagewrappers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage/adapter"
	"github.com/openfga/openfga/pkg/storage/adapter/query"
)

// capturingQuerier is an adapter.Querier that records the context passed to Execute.
type capturingQuerier struct {
	fakeQuerier
	ctx context.Context
}

func (q *capturingQuerier) Execute(ctx context.Context, stmt *query.Statement) (adapter.Rows, error) {
	q.ctx = ctx
	return q.fakeQuerier.Execute(ctx, stmt)
}

type contextTestKey struct{}

func TestContextTracerWrapperQuerier(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	newWrapper := func(inner adapter.Querier, opts ...ContextTracerWrapperOpt) adapter.Querier {
		ds := mocks.NewMockOpenFGADatastore(ctrl)
		ds.EXPECT().Querier(openfgav1.ConsistencyPreference_UNSPECIFIED).Return(inner)
		return NewContextWrapper(ds, opts...).Querier(openfgav1.ConsistencyPreference_UNSPECIFIED)
	}

	t.Run("returns nil when the delegate returns nil", func(t *testing.T) {
		require.Nil(t, newWrapper(nil))
	})

	t.Run("request cancellation does not reach the datastore", func(t *testing.T) {
		inner := &capturingQuerier{fakeQuerier: fakeQuerier{rows: &fakeRows{}}}
		q := newWrapper(inner)

		ctx, cancel := context.WithCancel(context.Background())
		rows, err := q.Execute(ctx, &query.Statement{})
		require.NoError(t, err)
		defer rows.Close()

		cancel()
		require.NoError(t, inner.ctx.Err())
	})

	t.Run("request deadline does not reach the datastore", func(t *testing.T) {
		inner := &capturingQuerier{fakeQuerier: fakeQuerier{rows: &fakeRows{}}}
		q := newWrapper(inner, WithContextWrapperQueryTimeout(time.Hour))

		// Simulates a client-supplied grpc-timeout.
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		rows, err := q.Execute(ctx, &query.Statement{})
		require.NoError(t, err)
		defer rows.Close()

		<-ctx.Done()
		require.NoError(t, inner.ctx.Err())
		deadline, ok := inner.ctx.Deadline()
		require.True(t, ok)
		require.WithinDuration(t, time.Now().Add(time.Hour), deadline, time.Minute)
	})

	t.Run("defaults to the default request timeout", func(t *testing.T) {
		inner := &capturingQuerier{fakeQuerier: fakeQuerier{rows: &fakeRows{}}}
		q := newWrapper(inner)

		rows, err := q.Execute(context.Background(), &query.Statement{})
		require.NoError(t, err)
		defer rows.Close()

		deadline, ok := inner.ctx.Deadline()
		require.True(t, ok)
		require.WithinDuration(t, time.Now().Add(config.DefaultRequestTimeout), deadline, time.Second)
	})

	t.Run("non-positive timeout keeps the default", func(t *testing.T) {
		inner := &capturingQuerier{fakeQuerier: fakeQuerier{rows: &fakeRows{}}}
		q := newWrapper(inner, WithContextWrapperQueryTimeout(0))

		rows, err := q.Execute(context.Background(), &query.Statement{})
		require.NoError(t, err)
		defer rows.Close()

		deadline, ok := inner.ctx.Deadline()
		require.True(t, ok)
		require.WithinDuration(t, time.Now().Add(config.DefaultRequestTimeout), deadline, time.Second)
	})

	t.Run("server timeout cancels the datastore context", func(t *testing.T) {
		inner := &capturingQuerier{fakeQuerier: fakeQuerier{rows: &fakeRows{}}}
		q := newWrapper(inner, WithContextWrapperQueryTimeout(10*time.Millisecond))

		rows, err := q.Execute(context.Background(), &query.Statement{})
		require.NoError(t, err)
		defer rows.Close()

		select {
		case <-inner.ctx.Done():
		case <-time.After(time.Second):
			t.Fatal("datastore context was not cancelled by the server timeout")
		}
		require.ErrorIs(t, inner.ctx.Err(), context.DeadlineExceeded)
	})

	t.Run("request values reach the datastore", func(t *testing.T) {
		inner := &capturingQuerier{fakeQuerier: fakeQuerier{rows: &fakeRows{}}}
		q := newWrapper(inner)

		ctx := context.WithValue(context.Background(), contextTestKey{}, "value")
		rows, err := q.Execute(ctx, &query.Statement{})
		require.NoError(t, err)
		defer rows.Close()

		require.Equal(t, "value", inner.ctx.Value(contextTestKey{}))
	})

	t.Run("delegate error cancels the datastore context", func(t *testing.T) {
		wantErr := errors.New("boom")
		inner := &capturingQuerier{fakeQuerier: fakeQuerier{err: wantErr}}
		q := newWrapper(inner, WithContextWrapperQueryTimeout(time.Hour))

		rows, err := q.Execute(context.Background(), &query.Statement{})
		require.ErrorIs(t, err, wantErr)
		require.Nil(t, rows)
		require.ErrorIs(t, inner.ctx.Err(), context.Canceled)
	})

	t.Run("Close cancels the datastore context and closes the rows", func(t *testing.T) {
		underlying := &fakeRows{remaining: 1}
		inner := &capturingQuerier{fakeQuerier: fakeQuerier{rows: underlying}}
		q := newWrapper(inner, WithContextWrapperQueryTimeout(time.Hour))

		rows, err := q.Execute(context.Background(), &query.Statement{})
		require.NoError(t, err)
		require.NoError(t, inner.ctx.Err(), "context stays alive while rows are open")

		require.NoError(t, rows.Close())
		require.ErrorIs(t, inner.ctx.Err(), context.Canceled)
		require.True(t, underlying.closed)
		require.NoError(t, rows.Close(), "Close is safe to call twice")
	})

	t.Run("exhausting rows cancels the datastore context", func(t *testing.T) {
		inner := &capturingQuerier{fakeQuerier: fakeQuerier{rows: &fakeRows{remaining: 1}}}
		q := newWrapper(inner, WithContextWrapperQueryTimeout(time.Hour))

		rows, err := q.Execute(context.Background(), &query.Statement{})
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		require.NoError(t, inner.ctx.Err(), "context stays alive while rows remain")
		require.False(t, rows.Next())
		require.ErrorIs(t, inner.ctx.Err(), context.Canceled)
	})
}
