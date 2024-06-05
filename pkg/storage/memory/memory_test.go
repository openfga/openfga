package memory

import (
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/test"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestMemdbStorage(t *testing.T) {
	ds := New()
	test.RunAllTests(t, ds)
}

func TestTupleRecordMatchTupleKey(t *testing.T) {
	var testCases = map[string]struct {
		target *openfgav1.TupleKey
		source *storage.TupleRecord
		match  bool
	}{
		`no_match_in_user`: {
			target: tuple.NewTupleKey("document:x", "viewer", "user:1"),
			source: &storage.TupleRecord{
				ObjectType: "document",
				ObjectID:   "x",
				Relation:   "viewer",
				User:       "user:2",
			},
			match: false,
		},
		`no_match_in_relation`: {
			target: tuple.NewTupleKey("document:x", "viewer", "user:1"),
			source: &storage.TupleRecord{
				ObjectType: "document",
				ObjectID:   "x",
				Relation:   "writer",
				User:       "user:1",
			},
			match: false,
		},
		`no_match_in_object_type`: {
			target: tuple.NewTupleKey("document", "viewer", "user:1"),
			source: &storage.TupleRecord{
				ObjectType: "group",
				ObjectID:   "x",
				Relation:   "viewer",
				User:       "user:1",
			},
			match: false,
		},
		`no_match_in_object_id`: {
			target: tuple.NewTupleKey("document:x", "viewer", "user:1"),
			source: &storage.TupleRecord{
				ObjectType: "document",
				ObjectID:   "z",
				Relation:   "viewer",
				User:       "user:1",
			},
			match: false,
		},
		`match`: {
			target: tuple.NewTupleKey("document:x", "viewer", "user:1"),
			source: &storage.TupleRecord{
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
			require.Equal(t, test.match, match(test.source, test.target))
		})
	}
}

func TestFindTupleKey(t *testing.T) {
	var testCases = map[string]struct {
		records  []*storage.TupleRecord
		tupleKey *openfgav1.TupleKey
		found    bool
	}{
		`not_find`: {
			tupleKey: tuple.NewTupleKey("document:x", "viewer", "user:1"),
			records: []*storage.TupleRecord{{
				ObjectType: "document",
				ObjectID:   "x",
				Relation:   "viewer",
				User:       "user:2",
			}},
			found: false,
		},
		`find`: {
			tupleKey: tuple.NewTupleKey("document:x", "viewer", "user:1"),
			records: []*storage.TupleRecord{{
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
			require.Equal(t, test.found, find(test.records, test.tupleKey))
		})
	}
}
