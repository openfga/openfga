package storage

import (
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/stretchr/testify/require"
)

func TestTupleRecordMatchTupleKey(t *testing.T) {
	var testCases = map[string]struct {
		target *openfgav1.TupleKey
		source *TupleRecord
		match  bool
	}{
		`no_match_in_user`: {
			target: tuple.NewTupleKey("document:x", "viewer", "user:1"),
			source: &TupleRecord{
				ObjectType: "document",
				ObjectID:   "x",
				Relation:   "viewer",
				User:       "user:2",
			},
			match: false,
		},
		`no_match_in_relation`: {
			target: tuple.NewTupleKey("document:x", "viewer", "user:1"),
			source: &TupleRecord{
				ObjectType: "document",
				ObjectID:   "x",
				Relation:   "writer",
				User:       "user:1",
			},
			match: false,
		},
		`no_match_in_object_type`: {
			target: tuple.NewTupleKey("document", "viewer", "user:1"),
			source: &TupleRecord{
				ObjectType: "group",
				ObjectID:   "x",
				Relation:   "viewer",
				User:       "user:1",
			},
			match: false,
		},
		`no_match_in_object_id`: {
			target: tuple.NewTupleKey("document:x", "viewer", "user:1"),
			source: &TupleRecord{
				ObjectType: "document",
				ObjectID:   "z",
				Relation:   "viewer",
				User:       "user:1",
			},
			match: false,
		},
		`match`: {
			target: tuple.NewTupleKey("document:x", "viewer", "user:1"),
			source: &TupleRecord{
				ObjectType: "document",
				ObjectID:   "x",
				Relation:   "viewer",
				User:       "user:1",
			},
			match: true,
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, test.match, test.source.Match(test.target))
		})
	}
}

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

func TestFindTupleKey(t *testing.T) {
	var testCases = map[string]struct {
		records  []*TupleRecord
		tupleKey *openfgav1.TupleKey
		found    bool
	}{
		`not_find`: {
			tupleKey: tuple.NewTupleKey("document:x", "viewer", "user:1"),
			records: []*TupleRecord{{
				ObjectType: "document",
				ObjectID:   "x",
				Relation:   "viewer",
				User:       "user:2",
			}},
			found: false,
		},
		`find`: {
			tupleKey: tuple.NewTupleKey("document:x", "viewer", "user:1"),
			records: []*TupleRecord{{
				ObjectType: "document",
				ObjectID:   "x",
				Relation:   "viewer",
				User:       "user:1",
			}},
			found: true,
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, test.found, Find(test.records, test.tupleKey))
		})
	}
}
