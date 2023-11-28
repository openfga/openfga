package memory

import (
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/test"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMemdbStorage(t *testing.T) {
	ds := New()
	test.RunAllTests(t, ds)
}

func TestStaticTupleIteratorNoRace(t *testing.T) {
	now := timestamppb.Now()

	iter := &staticIterator{
		records: []*storage.TupleRecord{
			{
				Store:      "store",
				ObjectType: "document",
				ObjectID:   "1",
				Relation:   "viewer",
				User:       "user:jon",
				Ulid:       ulid.MustNew(ulid.Timestamp(now.AsTime()), ulid.DefaultEntropy()).String(),
				InsertedAt: now.AsTime(),
			},
			{
				Store:      "store",
				ObjectType: "document",
				ObjectID:   "1",
				Relation:   "viewer",
				User:       "user:jon",
				Ulid:       ulid.MustNew(ulid.Timestamp(now.AsTime()), ulid.DefaultEntropy()).String(),
				InsertedAt: now.AsTime(),
			},
		},
	}
	defer iter.Stop()

	go func() {
		_, err := iter.Next()
		require.NoError(t, err)
	}()

	go func() {
		_, err := iter.Next()
		require.NoError(t, err)
	}()
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
