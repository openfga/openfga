package memory

import (
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/test"
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
