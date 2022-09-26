package id

import (
	"time"

	"github.com/oklog/ulid/v2"
)

type ID struct {
	value ulid.ULID
}

func New() (*ID, error) {
	return NewFromTime(time.Now())
}

// NewFromTime returns a new *ID based on the given time.Time.
// TODO: since ulid.DefaultEntropy() can't fail it may be worth refactoring this to remove the error.
func NewFromTime(time time.Time) (*ID, error) {
	id, err := ulid.New(uint64(time.UnixMilli()), ulid.DefaultEntropy())
	if err != nil {
		return nil, err
	}

	return &ID{id}, nil
}

func NewStringFromTime(time time.Time) (string, error) {
	id, err := NewFromTime(time)
	if err != nil {
		return "", err
	}

	return id.value.String(), nil
}

func NewString() (string, error) {
	return NewStringFromTime(time.Now())
}

func Parse(s string) (*ID, error) {
	id, err := ulid.ParseStrict(s)
	if err != nil {
		return nil, err
	}

	return &ID{id}, nil
}

func IsValid(s string) bool {
	if _, err := Parse(s); err != nil {
		return false
	}
	return true
}

func (id *ID) Time() time.Time {
	return ulid.Time(id.value.Time())
}

func (id *ID) String() string {
	return id.value.String()
}

// Must is a convenience function that is useful for tests and when starting up.
func Must(id *ID, err error) *ID {
	if err != nil {
		panic(err)
	}
	return id
}
