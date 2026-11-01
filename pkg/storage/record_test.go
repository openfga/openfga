package storage

import (
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
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
	require.Equal(t, "user-id", tuple.GetKey().GetUser())
	require.Equal(t, "object-type:object-id", tuple.GetKey().GetObject())
	require.Equal(t, "relation", tuple.GetKey().GetRelation())
	require.Equal(t, now, tuple.GetTimestamp().AsTime().UTC())
}

func TestSameCondition(t *testing.T) {
	withContext := func(name string, fields map[string]any) *openfgav1.RelationshipCondition {
		ctx, err := structpb.NewStruct(fields)
		require.NoError(t, err)
		return &openfgav1.RelationshipCondition{Name: name, Context: ctx}
	}

	tests := map[string]struct {
		a, b     *openfgav1.RelationshipCondition
		expected bool
	}{
		"both_nil":               {nil, nil, true},
		"nil_and_named":          {nil, &openfgav1.RelationshipCondition{Name: "c"}, false},
		"nil_and_empty_context":  {&openfgav1.RelationshipCondition{Name: "c"}, withContext("c", map[string]any{}), true},
		"different_names":        {&openfgav1.RelationshipCondition{Name: "c"}, &openfgav1.RelationshipCondition{Name: "d"}, false},
		"same_context":           {withContext("c", map[string]any{"a": "b"}), withContext("c", map[string]any{"a": "b"}), true},
		"different_context":      {withContext("c", map[string]any{"a": "b"}), withContext("c", map[string]any{"a": "x"}), false},
		"context_and_no_context": {withContext("c", map[string]any{"a": "b"}), &openfgav1.RelationshipCondition{Name: "c"}, false},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, test.expected, SameCondition(test.a, test.b))
			require.Equal(t, test.expected, SameCondition(test.b, test.a))
		})
	}
}
