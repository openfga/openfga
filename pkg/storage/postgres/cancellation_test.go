package postgres

import (
	"context"
	"fmt"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
)

// Cancel at the next socket read, after the query has started. Wait for the
// driver's cancellation handler so the test does not depend on goroutine ordering.
type cancelReadConn struct {
	net.Conn
	cancel   <-chan context.CancelFunc
	canceled <-chan struct{}
}

func (c *cancelReadConn) Read(b []byte) (int, error) {
	select {
	case cancel := <-c.cancel:
		cancel()
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		select {
		case <-c.canceled:
		case <-timer.C:
			return 0, fmt.Errorf("waiting for pgx cancellation handler: %w", context.DeadlineExceeded)
		}
	default:
	}
	return c.Conn.Read(b)
}

func TestCancelReadConnNotificationTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()
		cancelRead := make(chan context.CancelFunc, 1)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		cancelRead <- cancel
		conn := &cancelReadConn{Conn: client, cancel: cancelRead, canceled: make(chan struct{})}

		// A missing watcher notification must fail without reaching the socket read.
		n, err := conn.Read(make([]byte, 1))
		require.Zero(t, n)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.ErrorContains(t, err, "waiting for pgx cancellation handler")
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	})
}

type notifyCancellation struct {
	ctxwatch.Handler
	canceled chan struct{}
}

func (h *notifyCancellation) HandleCancel(ctx context.Context) {
	h.Handler.HandleCancel(ctx)
	close(h.canceled)
}

func TestReadUserTupleCancellation(t *testing.T) {
	for _, tc := range []struct {
		name, object string
		blocked      bool
		cold         bool
		expire       bool
	}{
		{name: "existing", object: "document:1"},
		{name: "missing", object: "document:missing"},
		{name: "existing_during_prepare", object: "document:1", cold: true},
		{name: "missing_during_prepare", object: "document:missing", cold: true},
		{name: "deadline_expired", object: "document:1", blocked: true, expire: true},
		{name: "blocked_beyond_grace", object: "document:1", blocked: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uri := storagefixtures.RunDatastoreTestContainer(t, "postgres").GetConnectionURI(true)
			cfg := sqlcommon.NewConfig()
			poolCfg, err := parseConfig(uri, false, cfg)
			require.NoError(t, err)
			poolCfg.MaxConns = 1
			poolCfg.ShouldPing = func(context.Context, pgxpool.ShouldPingParams) bool { return false }
			cancelRead := make(chan context.CancelFunc, 1)
			canceled := make(chan struct{})
			dial := poolCfg.ConnConfig.DialFunc
			poolCfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
				conn, err := dial(ctx, network, address)
				if err != nil {
					return nil, err
				}
				return &cancelReadConn{Conn: conn, cancel: cancelRead, canceled: canceled}, nil
			}
			watcher := poolCfg.ConnConfig.BuildContextWatcherHandler
			poolCfg.ConnConfig.BuildContextWatcherHandler = func(conn *pgconn.PgConn) ctxwatch.Handler {
				return &notifyCancellation{Handler: watcher(conn), canceled: canceled}
			}
			pool, err := pgxpool.NewWithConfig(t.Context(), poolCfg)
			require.NoError(t, err)
			ds, err := NewWithDB(pool, nil, cfg)
			require.NoError(t, err)
			defer ds.Close()
			store := ulid.Make().String()
			tk := tupleUtils.NewTupleKey("document:1", "viewer", "user:anne")
			require.NoError(t, ds.Write(t.Context(), store, nil, storage.Writes{tk}))
			filter := storage.ReadUserTupleFilter{Object: tk.GetObject(), Relation: tk.GetRelation(), User: tk.GetUser()}
			if !tc.cold {
				// A cold query instead cancels on Prepare's first socket read.
				_, err = ds.ReadUserTuple(t.Context(), store, filter, storage.ReadUserTupleOptions{})
				require.NoError(t, err)
			}
			connections := pool.Stat().NewConnsCount()
			var tx pgx.Tx
			if tc.blocked {
				locker, err := pgx.Connect(t.Context(), uri)
				require.NoError(t, err)
				defer locker.Close(t.Context())
				tx, err = locker.Begin(t.Context())
				require.NoError(t, err)
				defer tx.Rollback(t.Context())
				// Keep the query blocked until cancellation interrupts the socket.
				_, err = tx.Exec(t.Context(), "LOCK TABLE tuple IN ACCESS EXCLUSIVE MODE")
				require.NoError(t, err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			wantErr := context.Canceled
			if tc.expire {
				deadlineCtx, cancelDeadline := context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancelDeadline()
				ctx = deadlineCtx
				wantErr = context.DeadlineExceeded
				// Let the real deadline, rather than explicit cancellation, fire
				// while the socket read is blocked by the exclusive table lock.
				cancelRead <- func() { <-ctx.Done() }
			} else {
				cancelRead <- cancel
			}
			filter.Object = tc.object
			started := time.Now()
			_, err = ds.ReadUserTuple(ctx, store, filter, storage.ReadUserTupleOptions{})
			require.ErrorIs(t, err, wantErr)
			if tc.blocked {
				require.GreaterOrEqual(t, time.Since(started), tupleReadCancellationGrace)
				require.NoError(t, tx.Rollback(t.Context()))
				connections++
			}
			filter.Object = tk.GetObject()
			got, err := ds.ReadUserTuple(t.Context(), store, filter, storage.ReadUserTupleOptions{})
			require.NoError(t, err)
			require.Equal(t, tk, got.GetKey())
			require.Equal(t, connections, pool.Stat().NewConnsCount())
		})
	}
}

type deadlineConn struct {
	net.Conn
	deadline time.Time
}

func (c *deadlineConn) SetDeadline(deadline time.Time) error {
	c.deadline = deadline
	return nil
}

func TestTupleReadCancellationDeadline(t *testing.T) {
	for _, tc := range []struct {
		name   string
		marked bool
		budget time.Duration
	}{
		{name: "direct_read", marked: true},
		{name: "other_operation"},
		{name: "earlier_deadline", marked: true, budget: 10 * time.Millisecond},
		{name: "expired_deadline", marked: true, budget: -time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Virtual time cannot advance while the handler is running.
			synctest.Test(t, func(t *testing.T) {
				ctx := t.Context()
				var cancel context.CancelFunc
				var deadline time.Time
				if tc.budget != 0 {
					deadline = time.Now().Add(tc.budget)
					ctx, cancel = context.WithDeadline(ctx, deadline)
				} else {
					ctx, cancel = context.WithCancel(ctx)
				}
				defer cancel()
				cancel()
				if tc.marked {
					ctx = context.WithValue(ctx, tupleReadCtxKey{}, struct{}{})
				}
				conn := &deadlineConn{}
				handler := &tupleReadCancellationHandler{DeadlineContextWatcherHandler: &pgconn.DeadlineContextWatcherHandler{Conn: conn}}
				before := time.Now()
				handler.HandleCancel(ctx)
				after := time.Now()
				switch {
				case tc.budget > 0:
					require.Equal(t, deadline, conn.deadline)
				case tc.marked && tc.budget == 0:
					require.False(t, conn.deadline.Before(before.Add(tupleReadCancellationGrace)))
					require.False(t, conn.deadline.After(after.Add(tupleReadCancellationGrace)))
				default:
					require.False(t, conn.deadline.Before(before))
					require.False(t, conn.deadline.After(after))
				}
				handler.HandleUnwatchAfterCancel()
				require.True(t, conn.deadline.IsZero())
			})
		})
	}
}
