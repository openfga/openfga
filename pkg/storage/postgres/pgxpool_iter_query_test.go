package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/internal/mocks"
)

// stubPgxRows is a minimal pgx.Rows implementation for testing.
type stubPgxRows struct{}

func (s *stubPgxRows) Close()                                       {}
func (s *stubPgxRows) Err() error                                   { return nil }
func (s *stubPgxRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (s *stubPgxRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (s *stubPgxRows) Next() bool                                   { return false }
func (s *stubPgxRows) Scan(_ ...any) error                          { return nil }
func (s *stubPgxRows) Values() ([]any, error)                       { return nil, nil }
func (s *stubPgxRows) RawValues() [][]byte                          { return nil }
func (s *stubPgxRows) Conn() *pgx.Conn                              { return nil }

func pgxIterQuerySampleCount(t *testing.T, successLabel string) uint64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == "openfga_pgx_txn_iter_query_duration_ms" {
			for _, m := range mf.GetMetric() {
				for _, l := range m.GetLabel() {
					if l.GetName() == "success" && l.GetValue() == successLabel {
						return m.GetHistogram().GetSampleCount()
					}
				}
			}
		}
	}
	return 0
}

func TestPgxTxnIterQueryGetRows(t *testing.T) {
	t.Run("success_records_true_label", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		mockTxn := mocks.NewMockPgxQuery(ctrl)
		mockTxn.EXPECT().
			Query(gomock.Any(), "SELECT 1", gomock.Any()).
			Return(&stubPgxRows{}, nil)

		before := pgxIterQuerySampleCount(t, "true")

		q := &PgxTxnIterQuery{txn: mockTxn, query: "SELECT 1", args: nil}
		rows, err := q.GetRows(context.Background())

		require.NoError(t, err)
		require.NotNil(t, rows)
		require.Equal(t, before+1, pgxIterQuerySampleCount(t, "true"))
	})

	t.Run("error_records_false_label", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		mockTxn := mocks.NewMockPgxQuery(ctrl)
		mockTxn.EXPECT().
			Query(gomock.Any(), "SELECT 1", gomock.Any()).
			Return(nil, errors.New("db error"))

		before := pgxIterQuerySampleCount(t, "false")

		q := &PgxTxnIterQuery{txn: mockTxn, query: "SELECT 1", args: nil}
		rows, err := q.GetRows(context.Background())

		require.Error(t, err)
		require.Nil(t, rows)
		require.Equal(t, before+1, pgxIterQuerySampleCount(t, "false"))
	})
}
