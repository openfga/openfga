package id

import (
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	t.Run("successful parse", func(t *testing.T) {
		id, err := NewString()
		if err != nil {
			t.Fatal(err)
		}

		if _, err := Parse(id); err != nil {
			t.Error("unable to parse generated id")
		}
	})

	t.Run("error when trying to parse non-id", func(t *testing.T) {
		badID := "foobar" // foobar is not an ulid, which is what ids are

		if _, err := Parse(badID); err == nil {
			t.Errorf("was able to parse '%s' as an id", badID)
		}
	})
}

func TestIsValid(t *testing.T) {
	t.Run("valid id is valid", func(t *testing.T) {
		id, err := NewString()
		if err != nil {
			t.Fatal(err)
		}

		if !IsValid(id) {
			t.Fatalf("valid id is not valid: %s", id)
		}
	})

	t.Run("non-valid id is not valid", func(t *testing.T) {
		badID := "foobar" // foobar is not an ulid, which is what ids are
		if IsValid(badID) {
			t.Fatalf("non-valid id is valid: %s", badID)
		}
	})
}

func TestThatProbablyNoCollisionsHappen(t *testing.T) {
	now := time.Now()
	length := 10000
	m := make(map[string]struct{}, length)
	for i := 0; i < length; i++ {
		id, err := NewStringFromTime(now)
		if err != nil {
			t.Fatal(err)
		}
		m[id] = struct{}{}
	}

	if len(m) != length {
		t.Error("ids collided")
	}
}
