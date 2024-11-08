//go:generate mockgen -source token_serializer.go -destination ../../internal/mocks/mock_token_serializer.go -package mocks OpenFGADatastore

package encoder

import (
	"errors"
	"fmt"
	"strings"

	"github.com/openfga/openfga/pkg/storage"
)

type ContinuationTokenSerializer interface {
	// SerializeReadChanges serializes the continuation token into a format readable by ReadChanges
	SerializeReadChanges(ulid string, objType string) (token []byte, err error)

	// DeserializeReadChanges deserializes the continuation token into a format readable by ReadChanges
	DeserializeReadChanges(token string) (ulid string, objType string, err error)
}

// StringContinuationTokenSerializer is a ContinuationTokenSerializer that serializes the continuation token as a string.
type StringContinuationTokenSerializer struct{}

// NewStringContinuationTokenSerializer returns a new instance of StringContinuationTokenSerializer.
// Serializes the continuation token into a string, as ulid & type concatenated by a pipe.
func NewStringContinuationTokenSerializer() ContinuationTokenSerializer {
	return &StringContinuationTokenSerializer{}
}

func (ts *StringContinuationTokenSerializer) SerializeReadChanges(ulid string, objType string) ([]byte, error) {
	if ulid == "" {
		return nil, errors.New("empty ulid provided for continuation token")
	}
	return []byte(fmt.Sprintf("%s|%s", ulid, objType)), nil
}

func (ts *StringContinuationTokenSerializer) DeserializeReadChanges(continuationToken string) (ulid string, objType string, err error) {
	if !strings.Contains(continuationToken, "|") {
		return "", "", storage.ErrInvalidContinuationToken
	}
	tokenParts := strings.Split(continuationToken, "|")
	return tokenParts[0], tokenParts[1], nil
}
