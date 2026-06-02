package encoder

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStringContinuationTokenSerializer(t *testing.T) {
	s := NewStringContinuationTokenSerializer()

	t.Run("round_trip", func(t *testing.T) {
		token, err := s.Serialize("01JTEST", "document")
		require.NoError(t, err)

		ulid, objType, err := s.Deserialize(string(token))
		require.NoError(t, err)
		require.Equal(t, "01JTEST", ulid)
		require.Equal(t, "document", objType)
	})

	t.Run("pipe_in_type_name", func(t *testing.T) {
		// Object types may legally contain '|'; Deserialize must recover the
		// full type, not just the portion before the second '|'.
		token, err := s.Serialize("01JTEST", "doc|v2")
		require.NoError(t, err)

		ulid, objType, err := s.Deserialize(string(token))
		require.NoError(t, err)
		require.Equal(t, "01JTEST", ulid)
		require.Equal(t, "doc|v2", objType)
	})

	t.Run("empty_type", func(t *testing.T) {
		// Read serializes tokens with an empty object type.
		token, err := s.Serialize("01JTEST", "")
		require.NoError(t, err)

		ulid, objType, err := s.Deserialize(string(token))
		require.NoError(t, err)
		require.Equal(t, "01JTEST", ulid)
		require.Equal(t, "", objType)
	})

	t.Run("serialize_empty_ulid_errors", func(t *testing.T) {
		_, err := s.Serialize("", "document")
		require.Error(t, err)
	})

	t.Run("deserialize_no_delimiter_errors", func(t *testing.T) {
		_, _, err := s.Deserialize("nopipe")
		require.Error(t, err)
	})
}
