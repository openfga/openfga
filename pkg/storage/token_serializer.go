package storage

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/oklog/ulid/v2"
)

// ContToken represents a continuation token structure used in pagination.
type ContToken struct {
	Ulid       string `json:"ulid"`
	ObjectType string `json:"objectType"`
}

// NewContToken creates a new instance of ContToken
// with the provided ULID and object type.
func NewContToken(ulid, objectType string) *ContToken {
	return &ContToken{
		Ulid:       ulid,
		ObjectType: objectType,
	}
}

// DecodeContToken decodes the continuation token into a ContToken struct.
func DecodeContToken(continuationToken string) (*ContToken, error) {
	var token ContToken

	err := token.Deserialize(continuationToken)
	if err != nil {
		return nil, err
	}

	return &token, nil
}

// DecodeContTokenOrULID decodes a continuation token, supporting both ContToken format
// (base64-encoded JSON) and plain ULID (for backward compatibility).
// Returns a ContToken with the ULID extracted, and an empty ObjectType if the token was a plain ULID.
func DecodeContTokenOrULID(continuationToken string) (*ContToken, error) {
	token, err := DecodeContToken(continuationToken)
	if err == nil {
		return token, nil
	}

	// If decoding fails, try parsing as plain ULID for backward compatibility
	parsed, parseErr := ulid.Parse(continuationToken)
	if parseErr != nil {
		return nil, ErrInvalidContinuationToken
	}

	return NewContToken(parsed.String(), ""), nil
}

func (c *ContToken) Serialize() string {
	// if the ulid is empty, return an empty string
	if c.Ulid == "" {
		return ""
	}

	// custom encoding of the struct into a json string
	// this ensures the token is always valid and avoids needing to handle any encoding errors
	encoded := fmt.Sprintf("{%q:%q,%q:%q}", "ulid", c.Ulid, "objectType", c.ObjectType)
	return base64.URLEncoding.EncodeToString([]byte(encoded))
}

func (c *ContToken) Deserialize(continuationToken string) error {
	// if the continuation token is empty, set the ulid and object type to empty strings
	if continuationToken == "" {
		c.Ulid = ""
		c.ObjectType = ""
		return nil
	}

	// first decode the base64 string
	decoded, err := base64.URLEncoding.DecodeString(continuationToken)
	if err != nil {
		return ErrInvalidContinuationToken
	}

	// then unmarshal the json string
	if err := json.Unmarshal(decoded, c); err == nil {
		return nil
	}

	// if we couldn't unmarshal the json string, try the old-style pipe-separated string
	if u, o, exists := strings.Cut(string(decoded), "|"); exists {
		c.Ulid = u
		c.ObjectType = o
		return nil
	}

	// we couldn't find a valid token format
	return ErrInvalidContinuationToken
}
