package storage

import (
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

func TestAsTuple(t *testing.T) {
	now := time.Now().UTC()
	tr := &TupleRecord{
		Store:      "store-id",
		ObjectType: "object-type",
		ObjectID:   "object-id",
		Relation:   "relation",
		User:       "user-id",
		Ulid:       ulid.Make().String(),
		InsertedAt: now,
	}

	tuple := tr.AsTuple()
	require.Equal(t, "user-id", tuple.Key.GetUser())
	require.Equal(t, "object-type:object-id", tuple.Key.GetObject())
	require.Equal(t, "relation", tuple.Key.GetRelation())
	require.Equal(t, now, tuple.GetTimestamp().AsTime().UTC())
}
