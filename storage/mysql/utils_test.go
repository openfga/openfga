package mysql

import (
	"database/sql"
	"testing"

	"github.com/go-errors/errors"
	"github.com/openfga/openfga/storage"
	"github.com/go-sql-driver/mysql"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestHandleMySQLError(t *testing.T) {
	t.Run("duplicate entry value error with tuple key wraps ErrInvalidWriteInput", func(t *testing.T) {
        duplicateKeyError := &mysql.MySQLError{
        	Number:  1062,
        	Message: "Duplicate entry '' for key ''",
        }
		err := handleMySQLError(duplicateKeyError, &openfgapb.TupleKey{
			Object:   "object",
			Relation: "relation",
			User:     "user",
		})
		if !errors.Is(err, storage.ErrInvalidWriteInput) {
			t.Fatalf("got '%v', expected wrapped '%v'", err, storage.ErrInvalidWriteInput)
		}
	})

	t.Run("duplicate entry value error without tuple key returns collision", func(t *testing.T) {
        duplicateKeyError := &mysql.MySQLError{
        	Number:  1062,
        	Message: "Duplicate entry '' for key ''",
        }
		err := handleMySQLError(duplicateKeyError)

		if !errors.Is(err, storage.ErrCollision) {
			t.Fatalf("got '%v', expected '%v'", err, storage.ErrCollision)
		}
	})

	t.Run("sql.ErrNoRows is converted to storage.ErrNotFound error", func(t *testing.T) {
		err := handleMySQLError(sql.ErrNoRows)

		if !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("got '%v', expected '%v'", err, storage.ErrNotFound)
		}
	})
}
