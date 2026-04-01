package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
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

func TestPgxTxnIterQueryGetRows(t *testing.T) {
	t.Run("success_returns_rows", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		mockTxn := mocks.NewMockPgxQuery(ctrl)
		mockTxn.EXPECT().
			Query(gomock.Any(), "SELECT 1").
			Return(&stubPgxRows{}, nil)

		q := &PgxTxnIterQuery{txn: mockTxn, query: "SELECT 1", args: nil}
		rows, err := q.GetRows(context.Background())

		require.NoError(t, err)
		require.NotNil(t, rows)
	})

	t.Run("internal_error_returns_error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		mockTxn := mocks.NewMockPgxQuery(ctrl)
		mockTxn.EXPECT().
			Query(gomock.Any(), "SELECT 1").
			Return(nil, errors.New("db error"))

		q := &PgxTxnIterQuery{txn: mockTxn, query: "SELECT 1", args: nil}
		rows, err := q.GetRows(context.Background())

		require.Error(t, err)
		require.Nil(t, rows)
	})

	t.Run("not_found_returns_ErrNotFound", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		mockTxn := mocks.NewMockPgxQuery(ctrl)
		mockTxn.EXPECT().
			Query(gomock.Any(), "SELECT 1").
			Return(nil, pgx.ErrNoRows)

		q := &PgxTxnIterQuery{txn: mockTxn, query: "SELECT 1", args: nil}
		rows, err := q.GetRows(context.Background())

		require.ErrorIs(t, err, storage.ErrNotFound)
		require.Nil(t, rows)
	})

	t.Run("collision_returns_ErrCollision", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		mockTxn := mocks.NewMockPgxQuery(ctrl)
		mockTxn.EXPECT().
			Query(gomock.Any(), "SELECT 1").
			Return(nil, errors.New("duplicate key value"))

		q := &PgxTxnIterQuery{txn: mockTxn, query: "SELECT 1", args: nil}
		rows, err := q.GetRows(context.Background())

		require.ErrorIs(t, err, storage.ErrCollision)
		require.Nil(t, rows)
	})
}
