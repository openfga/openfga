package encoder

import (
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage"
)

func TestStringContinuationTokenSerializer(t *testing.T) {
	s := NewStringContinuationTokenSerializer()

	t.Run("round_trip", func(t *testing.T) {
		expectedULID := ulid.Make().String()
		token, err := s.Serialize(expectedULID, "document")
		require.NoError(t, err)

		actualULID, objType, err := s.Deserialize(string(token))
		require.NoError(t, err)
		require.Equal(t, expectedULID, actualULID)
		require.Equal(t, "document", objType)
	})

	t.Run("pipe_in_type_name", func(t *testing.T) {
		// Object types may legally contain '|'; Deserialize must recover the
		// full type, not just the portion before the second '|'.
		expectedULID := ulid.Make().String()
		token, err := s.Serialize(expectedULID, "doc|v2")
		require.NoError(t, err)

		actualULID, objType, err := s.Deserialize(string(token))
		require.NoError(t, err)
		require.Equal(t, expectedULID, actualULID)
		require.Equal(t, "doc|v2", objType)
	})

	t.Run("empty_type", func(t *testing.T) {
		// Read serializes tokens with an empty object type.
		expectedULID := ulid.Make().String()
		token, err := s.Serialize(expectedULID, "")
		require.NoError(t, err)

		actualULID, objType, err := s.Deserialize(string(token))
		require.NoError(t, err)
		require.Equal(t, expectedULID, actualULID)
		require.Empty(t, objType)
	})

	t.Run("serialize_empty_ulid_errors", func(t *testing.T) {
		_, err := s.Serialize("", "document")
		require.Error(t, err)
	})

	t.Run("deserialize_no_delimiter_errors", func(t *testing.T) {
		_, _, err := s.Deserialize("nopipe")
		require.ErrorIs(t, err, storage.ErrInvalidContinuationToken)
	})

	t.Run("deserialize_empty_ulid_errors", func(t *testing.T) {
		_, _, err := s.Deserialize("|document")
		require.ErrorIs(t, err, storage.ErrInvalidContinuationToken)
	})

	t.Run("deserialize_invalid_ulid_errors", func(t *testing.T) {
		_, _, err := s.Deserialize("notaulid|document")
		require.ErrorIs(t, err, storage.ErrInvalidContinuationToken)
	})
}
