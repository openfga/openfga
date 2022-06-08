package id

import (
	"math/rand"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

var (
	mutex   sync.Mutex
	entropy = ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0)
)

type ID struct {
	value ulid.ULID
}

func NewFromTime(time time.Time) (*ID, error) {
	mutex.Lock()
	defer mutex.Unlock()

	id, err := ulid.New(uint64(time.UnixMilli()), entropy)
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
