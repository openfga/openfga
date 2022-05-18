package postgres

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/openfga/openfga/storage"
	"go.buf.build/openfga/go/openfga/api/openfga"
)

func TestHandlePostgresError(t *testing.T) {
	t.Run("duplicate key value error with tuple key wraps ErrInvalidWriteInput", func(t *testing.T) {
		err := handlePostgresError(errors.New("duplicate key value"), &openfga.TupleKey{
			Object:   "object",
			Relation: "relation",
			User:     "user",
		})
		if !errors.Is(err, storage.ErrInvalidWriteInput) {
			t.Fatalf("got '%v', expected wrapped '%v'", err, storage.ErrInvalidWriteInput)
		}
	})

	t.Run("duplicate key value error without tuple key returns collision", func(t *testing.T) {
		duplicateKeyError := errors.New("duplicate key value")
		err := handlePostgresError(duplicateKeyError)

		if !errors.Is(err, storage.ErrCollision) {
			t.Fatalf("got '%v', expected '%v'", err, storage.ErrCollision)
		}
	})

	t.Run("pgx.ErrNoRows is converted to storage.ErrNotFound error", func(t *testing.T) {
		err := handlePostgresError(pgx.ErrNoRows)

		if !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("got '%v', expected '%v'", err, storage.ErrNotFound)
		}
	})
}
