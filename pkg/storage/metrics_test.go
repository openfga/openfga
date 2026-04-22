package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

func TestSuccessLabel(t *testing.T) {
	require.True(t, SuccessLabel(nil))
	require.True(t, SuccessLabel(ErrNotFound))
	require.True(t, SuccessLabel(ErrCollision))
	require.True(t, SuccessLabel(ErrInvalidWriteInput))
	require.True(t, SuccessLabel(InvalidWriteInputError(
		&openfgav1.TupleKey{Object: "document:1", Relation: "viewer", User: "user:alice"},
		openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
	)))
	require.False(t, SuccessLabel(errors.New("sql error: connection reset")))
}
