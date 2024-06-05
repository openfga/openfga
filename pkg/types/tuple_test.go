package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObject_String(t *testing.T) {
	obj := Object{
		Type: "document",
		ID:   "readme",
	}

	require.Equal(t, "document:readme", obj.String())
}

func TestUserset_String(t *testing.T) {
	userset := Userset{
		Type:     "group",
		ID:       "eng",
		Relation: "member",
	}

	require.Equal(t, "group:eng#member", userset.String())
}

func TestTypedWildcard(t *testing.T) {
	typedWildcard := TypedWildcardSubject{
		Type: "user",
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, "user:*", typedWildcard.String())
	})

	t.Run("GetType", func(t *testing.T) {
		require.Equal(t, "user", typedWildcard.GetType())
	})
}

func TestRelationshipTuple_String(t *testing.T) {
	relationshipTuple := RelationshipTuple{
		Object: Object{
			Type: "document",
			ID:   "roadmap",
		},
		Relation: "viewer",
		Subject: &Object{
			Type: "user",
			ID:   "jon",
		},
	}

	require.Equal(t, "document:roadmap#viewer@user:jon", relationshipTuple.String())
}
