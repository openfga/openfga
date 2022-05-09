package id

import (
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	t.Run("successful parse", func(t *testing.T) {
		id, _ := NewString()
		_, err := Parse(id)
		if err != nil {
			t.Error("unable to parse generated id")
		}
	})

	t.Run("error when trying to parse non-id", func(t *testing.T) {
		badId := "foobar" // foobar is not an ulid, which is what ids are
		_, err := Parse(badId)
		if err == nil {
			t.Errorf("was able to parse '%s' as an id", badId)
		}
	})
}

func TestThatProbablyNoCollisionsHappen(t *testing.T) {
	now := time.Now()
	length := 10000
	m := make(map[string]struct{}, length)
	for i := 0; i < length; i++ {
		id, _ := NewStringFromTime(now)
		m[id] = struct{}{}
	}

	if len(m) != length {
		t.Error("ids collided!!")
	}
}
