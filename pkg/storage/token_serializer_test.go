package storage

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

func TestContToken_Serialize_Valid(t *testing.T) {
	uid, _ := ulid.New(ulid.Now(), nil)
	token := NewContToken(uid.String(), "document")
	encoded := token.Serialize()

	require.NotEmpty(t, encoded)
	decoded, err := base64.URLEncoding.DecodeString(encoded)
	require.NoError(t, err)

	var parsed ContToken
	require.NoError(t, json.Unmarshal(decoded, &parsed))
	require.Equal(t, token.Ulid, parsed.Ulid)
	require.Equal(t, token.ObjectType, parsed.ObjectType)
}

func TestContToken_Serialize_EmptyUlid(t *testing.T) {
	token := NewContToken("", "document")
	encoded := token.Serialize()
	require.Empty(t, encoded)
}

func TestContToken_Deserialize_Valid(t *testing.T) {
	uid, _ := ulid.New(ulid.Now(), nil)
	token := NewContToken(uid.String(), "document")
	encoded := token.Serialize()
	require.NotEmpty(t, encoded)

	var decoded ContToken
	err := decoded.Deserialize(encoded)
	require.NoError(t, err)
	require.Equal(t, token.Ulid, decoded.Ulid)
	require.Equal(t, token.ObjectType, decoded.ObjectType)
}

func TestContToken_Deserialize_Empty(t *testing.T) {
	var token ContToken
	err := token.Deserialize("")
	require.NoError(t, err)
	require.Empty(t, token.Ulid)
	require.Empty(t, token.ObjectType)
}

func TestContToken_Deserialize_OldStylePipeFormat(t *testing.T) {
	uid, _ := ulid.New(ulid.Now(), nil)
	legacy := uid.String() + "|document"
	encoded := base64.URLEncoding.EncodeToString([]byte(legacy))

	var token ContToken
	err := token.Deserialize(encoded)
	require.NoError(t, err)
	require.Equal(t, uid.String(), token.Ulid)
	require.Equal(t, "document", token.ObjectType)
}

func TestContToken_Deserialize_Invalid(t *testing.T) {
	tests := []struct {
		name  string
		token string
	}{
		{"invalid_base64", "not!!!valid!!!base64!!!"},
		{"base64_not_json", base64.URLEncoding.EncodeToString([]byte("plain text"))},
		{"base64_malformed_json", base64.URLEncoding.EncodeToString([]byte(`{"ulid":"x"`))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var token ContToken
			err := token.Deserialize(tt.token)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrInvalidContinuationToken)
		})
	}
}

func TestDecodeContToken_Valid(t *testing.T) {
	uid, _ := ulid.New(ulid.Now(), nil)
	token := NewContToken(uid.String(), "folder")
	encoded := token.Serialize()

	decoded, err := DecodeContToken(encoded)
	require.NoError(t, err)
	require.Equal(t, token.Ulid, decoded.Ulid)
	require.Equal(t, token.ObjectType, decoded.ObjectType)
}

func TestDecodeContToken_Empty(t *testing.T) {
	decoded, err := DecodeContToken("")
	require.NoError(t, err)
	require.NotNil(t, decoded)
	require.Empty(t, decoded.Ulid)
	require.Empty(t, decoded.ObjectType)
}

func TestDecodeContToken_Invalid(t *testing.T) {
	decoded, err := DecodeContToken("invalid!!!")
	require.Error(t, err)
	require.Nil(t, decoded)
	require.ErrorIs(t, err, ErrInvalidContinuationToken)
}

func TestDecodeContTokenOrULID_ValidToken(t *testing.T) {
	uid, _ := ulid.New(ulid.Now(), nil)
	token := NewContToken(uid.String(), "document")
	encoded := token.Serialize()

	decoded, err := DecodeContTokenOrULID(encoded)
	require.NoError(t, err)
	require.Equal(t, token.Ulid, decoded.Ulid)
	require.Equal(t, token.ObjectType, decoded.ObjectType)
}

func TestDecodeContTokenOrULID_PlainULID(t *testing.T) {
	uid, _ := ulid.New(ulid.Now(), nil)

	decoded, err := DecodeContTokenOrULID(uid.String())
	require.NoError(t, err)
	require.Equal(t, uid.String(), decoded.Ulid)
	require.Empty(t, decoded.ObjectType)
}

func TestDecodeContTokenOrULID_Empty(t *testing.T) {
	decoded, err := DecodeContTokenOrULID("")
	require.NoError(t, err)
	require.NotNil(t, decoded)
	require.Empty(t, decoded.Ulid)
	require.Empty(t, decoded.ObjectType)
}

func TestDecodeContTokenOrULID_Invalid(t *testing.T) {
	tests := []string{
		"not-a-ulid",
		"!!!invalid!!!",
		"tooshort",
	}
	for _, invalid := range tests {
		t.Run(invalid, func(t *testing.T) {
			decoded, err := DecodeContTokenOrULID(invalid)
			require.Error(t, err)
			require.Nil(t, decoded)
			require.ErrorIs(t, err, ErrInvalidContinuationToken)
		})
	}
}

func TestContToken_RoundTrip(t *testing.T) {
	uid, _ := ulid.New(ulid.Now(), nil)
	original := NewContToken(uid.String(), "document")

	encoded := original.Serialize()
	require.NotEmpty(t, encoded)

	decoded, err := DecodeContToken(encoded)
	require.NoError(t, err)
	require.Equal(t, original.Ulid, decoded.Ulid)
	require.Equal(t, original.ObjectType, decoded.ObjectType)

	reencoded := decoded.Serialize()
	require.Equal(t, encoded, reencoded)
}
