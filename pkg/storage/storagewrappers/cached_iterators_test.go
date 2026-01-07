package storagewrappers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestCachedTupleIterator(t *testing.T) {
	ctx := context.Background()

	cmpOpts := []cmp.Option{
		testutils.TupleKeyCmpTransformer,
		protocmp.Transform(),
	}

	ts := time.Now()

	t.Run("next_object_and_relation", func(t *testing.T) {
		cachedTuples := []*storage.TupleRecord{
			{
				ObjectType:     "",
				ObjectID:       "",
				Relation:       "",
				UserObjectType: "user",
				UserObjectID:   "1",
				UserRelation:   "",
				InsertedAt:     ts,
				ConditionName:  "cond",
			},
		}
		staticIter := storage.NewStaticIterator[*storage.TupleRecord](cachedTuples)
		iter := &cachedTupleIterator{
			objectType: "document",
			objectID:   "1",
			relation:   "viewer",
			iter:       staticIter,
		}

		var actual []*openfgav1.Tuple

		for {
			tk, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tk)
		}

		tuples := []*openfgav1.Tuple{
			{
				Key:       tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:1", "cond", nil),
				Timestamp: timestamppb.New(ts),
			},
		}

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("next_user_type_and_object_type", func(t *testing.T) {
		cachedTuples := []*storage.TupleRecord{
			{
				ObjectType:     "",
				ObjectID:       "1",
				Relation:       "viewer",
				UserObjectType: "user",
				UserObjectID:   "1",
				UserRelation:   "",
				InsertedAt:     ts,
				ConditionName:  "cond",
			},
		}
		staticIter := storage.NewStaticIterator[*storage.TupleRecord](cachedTuples)
		iter := &cachedTupleIterator{
			objectType: "document",
			objectID:   "",
			relation:   "",
			userType:   "user",
			iter:       staticIter,
		}

		var actual []*openfgav1.Tuple

		for {
			tk, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tk)
		}

		tuples := []*openfgav1.Tuple{
			{
				Key:       tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:1", "cond", nil),
				Timestamp: timestamppb.New(ts),
			},
		}

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("next_error", func(t *testing.T) {
		cachedTuples := []*storage.TupleRecord{
			{
				ObjectType:     "",
				ObjectID:       "1",
				Relation:       "viewer",
				UserObjectType: "user",
				UserObjectID:   "1",
				UserRelation:   "",
				InsertedAt:     ts,
				ConditionName:  "cond",
			},
		}

		staticIter := mocks.NewErrorIterator[*storage.TupleRecord](cachedTuples)

		iter := &cachedTupleIterator{
			objectType: "document",
			objectID:   "",
			relation:   "",
			userType:   "user",
			iter:       staticIter,
		}

		_, err := iter.Next(ctx)
		require.NoError(t, err)

		_, err = iter.Next(ctx)
		require.ErrorIs(t, err, mocks.ErrSimulatedError)
	})

	t.Run("head_object_and_relation", func(t *testing.T) {
		cachedTuples := []*storage.TupleRecord{
			{
				ObjectType:     "",
				ObjectID:       "",
				Relation:       "",
				UserObjectType: "user",
				UserObjectID:   "1",
				UserRelation:   "",
				InsertedAt:     ts,
				ConditionName:  "cond",
			},
		}
		staticIter := storage.NewStaticIterator[*storage.TupleRecord](cachedTuples)
		iter := &cachedTupleIterator{
			objectType: "document",
			objectID:   "1",
			relation:   "viewer",
			iter:       staticIter,
		}

		tk, err := iter.Head(ctx)
		require.NoError(t, err)

		tuple := &openfgav1.Tuple{
			Key:       tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:1", "cond", nil),
			Timestamp: timestamppb.New(ts),
		}

		if diff := cmp.Diff(tuple, tk, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("head_user_type_and_object_type", func(t *testing.T) {
		cachedTuples := []*storage.TupleRecord{
			{
				ObjectType:     "",
				ObjectID:       "1",
				Relation:       "viewer",
				UserObjectType: "user",
				UserObjectID:   "1",
				UserRelation:   "",
				InsertedAt:     ts,
				ConditionName:  "cond",
			},
		}
		staticIter := storage.NewStaticIterator[*storage.TupleRecord](cachedTuples)
		iter := &cachedTupleIterator{
			objectType: "document",
			objectID:   "",
			relation:   "",
			userType:   "user",
			iter:       staticIter,
		}

		tk, err := iter.Head(ctx)
		require.NoError(t, err)

		tuple := &openfgav1.Tuple{
			Key:       tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:1", "cond", nil),
			Timestamp: timestamppb.New(ts),
		}

		if diff := cmp.Diff(tuple, tk, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("head_error", func(t *testing.T) {
		cachedTuples := []*storage.TupleRecord{
			{
				ObjectType:     "",
				ObjectID:       "1",
				Relation:       "viewer",
				UserObjectType: "user",
				UserObjectID:   "1",
				UserRelation:   "",
				InsertedAt:     ts,
				ConditionName:  "cond",
			},
		}

		staticIter := mocks.NewErrorIterator[*storage.TupleRecord](cachedTuples)

		iter := &cachedTupleIterator{
			objectType: "document",
			objectID:   "",
			relation:   "",
			userType:   "user",
			iter:       staticIter,
		}

		_, err := iter.Next(ctx)
		require.NoError(t, err)

		_, err = iter.Head(ctx)
		require.ErrorIs(t, err, mocks.ErrSimulatedError)
	})
}
