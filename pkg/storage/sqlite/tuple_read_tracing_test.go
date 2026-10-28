package sqlite

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	modernsqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestSQLiteTupleReadSpans(t *testing.T) {
	for _, operation := range []string{"eager", "lazy_next", "lazy_head"} {
		t.Run(operation, func(t *testing.T) {
			for _, outcome := range []string{"success", "not_found", "cancelled", "closed_database", "busy"} {
				t.Run(outcome, func(t *testing.T) {
					ctx := context.Background()
					fixture := storagefixtures.RunDatastoreTestContainer(t, "sqlite")
					uri := fixture.GetConnectionURI(true)
					if outcome == "busy" {
						// WAL permits readers while a writer holds a lock. DELETE mode
						// makes the exclusive transaction below reject reads immediately.
						uri = strings.Replace(uri, "journal_mode(WAL)", "journal_mode(DELETE)", 1)
						uri = strings.Replace(uri, "busy_timeout(5000)", "busy_timeout(0)", 1)
					}
					ds, err := New(uri, sqlcommon.NewConfig())
					require.NoError(t, err)
					t.Cleanup(ds.Close)
					ds.db.SetMaxOpenConns(1)
					require.NoError(t, ds.db.PingContext(ctx))
					store := ulid.Make().String()
					if outcome != "not_found" {
						require.NoError(t, ds.Write(ctx, store, nil, storage.Writes{
							tuple.NewTupleKey("document:1", "viewer", "user:anne"),
						}))
					}

					switch outcome {
					case "closed_database":
						ds.Close()
					case "cancelled":
						var cancel context.CancelFunc
						ctx, cancel = context.WithCancel(ctx)
						cancel()
					case "busy":
						locker, err := sql.Open("sqlite", uri)
						require.NoError(t, err)
						t.Cleanup(func() { require.NoError(t, locker.Close()) })
						require.NoError(t, locker.PingContext(ctx))
						conn, err := locker.Conn(ctx)
						require.NoError(t, err)
						t.Cleanup(func() { require.NoError(t, conn.Close()) })
						_, err = conn.ExecContext(ctx, "BEGIN EXCLUSIVE")
						require.NoError(t, err)
						t.Cleanup(func() {
							_, err := conn.ExecContext(context.Background(), "ROLLBACK")
							require.NoError(t, err)
						})
					}

					recorder := tracetest.NewSpanRecorder()
					provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
					previousTracer := tracer
					tracer = provider.Tracer("openfga/pkg/storage/sqlite")
					t.Cleanup(func() {
						tracer = previousTracer
						require.NoError(t, provider.Shutdown(context.Background()))
					})

					spanName := "sqlite.ReadUserTuple"
					var gotErr error
					if operation == "eager" {
						_, gotErr = ds.ReadUserTuple(ctx, store, storage.ReadUserTupleFilter{
							Object: "document:1", Relation: "viewer", User: "user:anne",
						}, storage.ReadUserTupleOptions{})
					} else {
						spanName = "sqlite.fetchBuffer"
						iter, err := ds.Read(ctx, store, storage.ReadFilter{
							Object: "document:1", Relation: "viewer",
						}, storage.ReadOptions{})
						require.NoError(t, err)
						t.Cleanup(iter.Stop)
						if operation == "lazy_next" {
							_, gotErr = iter.Next(ctx)
						} else {
							_, gotErr = iter.Head(ctx)
						}
					}

					switch outcome {
					case "success":
						require.NoError(t, gotErr)
					case "not_found":
						if operation == "eager" {
							require.ErrorIs(t, gotErr, storage.ErrNotFound)
						} else {
							require.ErrorIs(t, gotErr, storage.ErrIteratorDone)
						}
					case "cancelled":
						require.ErrorIs(t, gotErr, context.Canceled)
					case "closed_database":
						require.ErrorContains(t, gotErr, "database is closed")
					case "busy":
						var sqliteErr *modernsqlite.Error
						require.ErrorAs(t, gotErr, &sqliteErr)
						require.Equal(t, sqlite3.SQLITE_BUSY, sqliteErr.Code()&0xff)
					}

					var ended []sdktrace.ReadOnlySpan
					for _, span := range recorder.Ended() {
						if span.Name() == spanName {
							ended = append(ended, span)
						}
					}
					if outcome == "cancelled" && operation != "eager" {
						// Iteration rejects an already-cancelled context before SQL
						// execution, so there should be no fetch span to classify.
						require.Empty(t, ended)
						return
					}
					require.Len(t, ended, 1, "actual SQL execution span must end")
					if outcome == "closed_database" || outcome == "busy" {
						require.Equal(t, codes.Error, ended[0].Status().Code)
						require.Len(t, ended[0].Events(), 1)
						require.Equal(t, "exception", ended[0].Events()[0].Name)
					} else {
						require.Equal(t, codes.Unset, ended[0].Status().Code)
						require.Empty(t, ended[0].Events())
					}
				})
			}
		})
	}
}
