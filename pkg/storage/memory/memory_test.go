package memory

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/test"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestMemdbStorage(t *testing.T) {
	ds := New()
	test.RunAllTests(t, ds)
}

func TestStaticTupleIterator(t *testing.T) {
	t.Run("empty_iterator", func(t *testing.T) {
		tests := []struct {
			name  string
			mixed bool
		}{
			{
				name:  "next_only",
				mixed: false,
			},
			{
				name:  "mixed",
				mixed: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				iter := &staticIterator{
					records: []*storage.TupleRecord{},
				}
				defer iter.Stop()
				if tt.mixed {
					_, err := iter.Head(context.Background())
					require.ErrorIs(t, err, storage.ErrIteratorDone)
				}
				_, err := iter.Next(context.Background())
				require.ErrorIs(t, err, storage.ErrIteratorDone)
				if tt.mixed {
					_, err := iter.Head(context.Background())
					require.ErrorIs(t, err, storage.ErrIteratorDone)
				}
			})
		}
	})
}

func TestStaticTupleIteratorNoRace(t *testing.T) {
	t.Run("next_only", func(t *testing.T) {
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
					Store:            "store",
					ObjectType:       "document",
					ObjectID:         "1",
					Relation:         "viewer",
					User:             "user:jon",
					ConditionName:    "condition",
					ConditionContext: &structpb.Struct{},
					Ulid:             ulid.MustNew(ulid.Timestamp(now.AsTime()), ulid.DefaultEntropy()).String(),
					InsertedAt:       now.AsTime(),
				},
			},
		}
		defer iter.Stop()

		var wg errgroup.Group

		wg.Go(func() error {
			_, err := iter.Next(context.Background())
			return err
		})

		wg.Go(func() error {
			_, err := iter.Next(context.Background())
			return err
		})

		err := wg.Wait()
		require.NoError(t, err)
	})
	t.Run("mixing_head_next", func(t *testing.T) {
		now := timestamppb.Now()
		const numIterator = 50

		tupleRecords := make([]*storage.TupleRecord, numIterator)
		for i := 0; i < numIterator; i++ {
			tupleRecords[i] = &storage.TupleRecord{
				Store:            "store",
				ObjectType:       "document",
				ObjectID:         strconv.Itoa(i),
				Relation:         "viewer",
				User:             "user:jon",
				ConditionName:    "condition",
				ConditionContext: &structpb.Struct{},
				Ulid:             ulid.MustNew(ulid.Timestamp(now.AsTime()), ulid.DefaultEntropy()).String(),
				InsertedAt:       now.AsTime(),
			}
		}
		iter := &staticIterator{
			records: tupleRecords,
		}
		defer iter.Stop()

		var wg errgroup.Group

		for i := 0; i < numIterator; i++ {
			wg.Go(func() error {
				_, err := iter.Head(context.Background())
				return err
			})
		}
		// important that when we call next, we will have 1 less
		// element because we don't want to be done.
		for i := 0; i < numIterator-1; i++ {
			wg.Go(func() error {
				_, err := iter.Next(context.Background())
				return err
			})
		}
		err := wg.Wait()
		require.NoError(t, err)
	})
}

func TestStaticTupleIteratorContextCanceled(t *testing.T) {
	tests := []struct {
		_name string
		next  bool
	}{
		{
			_name: "next",
			next:  true,
		},
		{
			_name: "head",
			next:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt._name, func(t *testing.T) {
			iter := &staticIterator{
				records: []*storage.TupleRecord{
					{
						ObjectType: "document",
						ObjectID:   "1",
						Relation:   "viewer",
						User:       "user:jon",
					},
				},
			}
			defer iter.Stop()

			ctx, cancel := context.WithCancel(context.Background())

			var err error
			if tt.next {
				_, err = iter.Next(ctx)
			} else {
				_, err = iter.Head(ctx)
			}
			require.NoError(t, err)

			cancel()

			if tt.next {
				_, err = iter.Next(ctx)
			} else {
				_, err = iter.Head(ctx)
			}
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}

func TestStaticTupleIteratorContextDeadlineExceeded(t *testing.T) {
	tests := []struct {
		_name string
		next  bool
	}{
		{
			_name: "next",
			next:  true,
		},
		{
			_name: "head",
			next:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt._name, func(t *testing.T) {
			iter := &staticIterator{
				records: []*storage.TupleRecord{
					{
						ObjectType: "document",
						ObjectID:   "1",
						Relation:   "viewer",
						User:       "user:jon",
					},
				},
			}
			defer iter.Stop()

			deadlineCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			var err error

			if tt.next {
				_, err = iter.Next(deadlineCtx)
			} else {
				_, err = iter.Head(deadlineCtx)
			}
			require.NoError(t, err)

			time.Sleep(2 * time.Second)

			if tt.next {
				_, err = iter.Next(deadlineCtx)
			} else {
				_, err = iter.Head(deadlineCtx)
			}
			require.ErrorIs(t, err, context.DeadlineExceeded)
		})
	}
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
			found := find(test.records, test.tupleKey) != nil
			require.Equal(t, test.found, found)
		})
	}
}
