package postgres

//go:generate mockgen -source=pgxpool_iter_query.go --destination ../../../internal/mocks/mock_pgx_tx.go --package mocks

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
)

// PgxQuery interface allows Query that returns pgx.Rows.
type PgxQuery interface {
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
}

// PgxExec interface allows pgx Exec functionality.
type PgxExec interface {
	Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error)
}

// SQLBuilder represents any SQL statement builder that can generate
// SQL strings with parameterized arguments.
type SQLBuilder interface {
	ToSql() (string, []interface{}, error)
}

var pgxIterQueryDurationHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace:                       build.ProjectName,
	Name:                            "pgx_txn_iter_query_duration_ms",
	Help:                            "The duration (in ms) of a PgxTxnIterQuery GetRows query labeled by success.",
	Buckets:                         []float64{1, 5, 10, 25, 50, 100, 200, 300, 1000},
	NativeHistogramBucketFactor:     1.1,
	NativeHistogramMaxBucketNumber:  100,
	NativeHistogramMinResetDuration: time.Hour,
}, []string{"success"})

// PgxTxnIterQuery is a helper to run queries using pgxpool when used in sqlcommon iterator.
type PgxTxnIterQuery struct {
	txn   PgxQuery
	query string
	args  []interface{}
}

var _ sqlcommon.SQLIteratorRowGetter = (*PgxTxnIterQuery)(nil)

// NewPgxTxnGetRows creates a PgxTxnIterQuery which allows the GetRows functionality via the specified PgxQuery txn.
func NewPgxTxnGetRows(txn PgxQuery, sb SQLBuilder) (*PgxTxnIterQuery, error) {
	stmt, args, err := sb.ToSql()
	if err != nil {
		return nil, err
	}
	return &PgxTxnIterQuery{
		txn:   txn,
		query: stmt,
		args:  args,
	}, nil
}

// GetRows executes the txn query and returns the sqlcommon.Rows.
func (p *PgxTxnIterQuery) GetRows(ctx context.Context) (sqlcommon.Rows, error) {
	start := time.Now()
	rows, err := p.txn.Query(ctx, p.query, p.args...)
	if err != nil {
		storageErr := HandleSQLError(err)
		pgxIterQueryDurationHistogram.WithLabelValues(successLabel(storageErr)).Observe(float64(time.Since(start).Milliseconds()))
		return nil, storageErr
	}
	pgxIterQueryDurationHistogram.WithLabelValues("true").Observe(float64(time.Since(start).Milliseconds()))
	return &pgxRowsWrapper{rows: rows}, nil
}

// successLabel returns the prometheus success label for an error returned by HandleSQLError.
// Expected storage errors (not found, collision, invalid write) are not infrastructure failures,
// so they are labelled "true". Only genuine internal SQL errors are labelled "false".
func successLabel(err error) string {
	if errors.Is(err, storage.ErrNotFound) ||
		errors.Is(err, storage.ErrCollision) ||
		errors.Is(err, storage.ErrInvalidWriteInput) {
		return "true"
	}
	return "false"
}

// pgxRowsWrapper wraps pgx.Rows to implement sqlcommon.Rows interface.
type pgxRowsWrapper struct {
	rows pgx.Rows
}

func (r *pgxRowsWrapper) Err() error {
	return r.rows.Err()
}

func (r *pgxRowsWrapper) Next() bool {
	return r.rows.Next()
}

func (r *pgxRowsWrapper) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

func (r *pgxRowsWrapper) Close() error {
	r.rows.Close()
	return nil
}

var _ sqlcommon.Rows = (*pgxRowsWrapper)(nil)
