package postgres

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/openfga/openfga/storage"
	"go.buf.build/openfga/go/openfga/api/openfga"
)

func TestHandlePostgresError(t *testing.T) {
	t.Run("duplicate key value error with tuple key wraps InvalidWriteInput", func(t *testing.T) {
		err := handlePostgresError(errors.New("duplicate key value"), &openfga.TupleKey{
			Object:   "object",
			Relation: "relation",
			User:     "user",
		})
		if !errors.Is(err, storage.InvalidWriteInput) {
			t.Fatalf("got '%v', expected wrapped '%v'", err, storage.InvalidWriteInput)
		}
	})

	t.Run("duplicate key value error without tuple key returns the error", func(t *testing.T) {
		duplicateKeyError := errors.New("duplicate key value")
		err := handlePostgresError(duplicateKeyError)

		if !errors.Is(err, duplicateKeyError) {
			t.Fatalf("got '%v', expected '%v'", err, duplicateKeyError)
		}
	})

	t.Run("pgx.ErrNoRows is converted to storage.NotFound error", func(t *testing.T) {
		err := handlePostgresError(pgx.ErrNoRows)

		if !errors.Is(err, storage.NotFound) {
			t.Fatalf("got '%v', expected '%v'", err, storage.NotFound)
		}
	})
}
