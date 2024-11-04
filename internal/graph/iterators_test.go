package graph

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCachedTupleIterator(t *testing.T) {
	ctx := context.Background()

	cmpOpts := []cmp.Option{
		testutils.TupleKeyCmpTransformer,
		protocmp.Transform(),
	}

	ts := timestamppb.New(time.Now())
	condition := tuple.NewRelationshipCondition("cond", nil)

	t.Run("object_and_relation", func(t *testing.T) {
		cachedTuples := []storage.CachedTuple{
			{
				ObjectType: "",
				ObjectID:   "",
				Relation:   "",
				User:       "user:1",
				Timestamp:  ts,
				Condition:  condition,
			},
		}
		staticIter := storage.NewStaticIterator[storage.CachedTuple](cachedTuples)
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
				Key:       tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:1", condition.Name, nil),
				Timestamp: ts,
			},
		}

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("object_type", func(t *testing.T) {
		cachedTuples := []storage.CachedTuple{
			{
				ObjectType: "",
				ObjectID:   "1",
				Relation:   "viewer",
				User:       "user:1",
				Timestamp:  ts,
				Condition:  condition,
			},
		}
		staticIter := storage.NewStaticIterator[storage.CachedTuple](cachedTuples)
		iter := &cachedTupleIterator{
			objectType: "document",
			objectID:   "",
			relation:   "",
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
				Timestamp: ts,
			},
		}

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

}
