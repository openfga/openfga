package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

func TestSuccessLabel(t *testing.T) {
	require.Equal(t, "true", SuccessLabel(nil))
	require.Equal(t, "true", SuccessLabel(ErrNotFound))
	require.Equal(t, "true", SuccessLabel(ErrCollision))
	require.Equal(t, "true", SuccessLabel(ErrInvalidWriteInput))
	require.Equal(t, "true", SuccessLabel(InvalidWriteInputError(
		&openfgav1.TupleKey{Object: "document:1", Relation: "viewer", User: "user:alice"},
		openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
	)))
	require.Equal(t, "false", SuccessLabel(errors.New("sql error: connection reset")))
}
