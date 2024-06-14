package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestStaticTupleKeyIterator(t *testing.T) {
	expected := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:doc1", "viewer", "bill"),
		tuple.NewTupleKey("document:doc2", "editor", "bob"),
	}

	iter := NewStaticTupleKeyIterator(expected)

	var actual []*openfgav1.TupleKey
	for {
		tk, err := iter.Next(context.Background())
		if err != nil {
			if errors.Is(err, ErrIteratorDone) {
				break
			}
			require.Fail(t, "no error was expected")
		}

		actual = append(actual, tk)
	}

	require.Equal(t, expected, actual)
}

func TestCombinedIterator(t *testing.T) {
	expected := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:doc1", "viewer", "bill"),
		tuple.NewTupleKey("document:doc2", "editor", "bob"),
	}

	iter1 := NewStaticTupleKeyIterator([]*openfgav1.TupleKey{expected[0]})
	iter2 := NewStaticTupleKeyIterator([]*openfgav1.TupleKey{expected[1]})
	iter := NewCombinedIterator(iter1, iter2)

	var actual []*openfgav1.TupleKey
	for {
		tk, err := iter.Next(context.Background())
		if err != nil {
			if errors.Is(err, ErrIteratorDone) {
				break
			}
			require.Fail(t, "no error was expected")
		}

		actual = append(actual, tk)
	}

	cmpOpts := []cmp.Option{
		testutils.TupleKeyCmpTransformer,
		protocmp.Transform(),
	}

	if diff := cmp.Diff(expected, actual, cmpOpts...); diff != "" {
		t.Fatalf("mismatch (-want +got):\n%s", diff)
	}
}

func ExampleNewFilteredTupleKeyIterator() {
	tuples := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:doc1", "viewer", "user:jon"),
		tuple.NewTupleKey("document:doc1", "editor", "user:elbuo"),
	}

	iter := NewFilteredTupleKeyIterator(
		NewStaticTupleKeyIterator(tuples),
		func(tk *openfgav1.TupleKey) bool {
			return tk.GetRelation() == "editor"
		},
	)
	defer iter.Stop()

	var filtered []string
	for {
		tuple, err := iter.Next(context.Background())
		if err != nil {
			if err == ErrIteratorDone {
				break
			}

			// Handle the error in some way.
			panic(err)
		}

		filtered = append(filtered, fmt.Sprintf("%s#%s@%s", tuple.GetObject(), tuple.GetRelation(), tuple.GetUser()))
	}

	fmt.Println(filtered)
	// Output: [document:doc1#editor@user:elbuo]
}

func TestTupleRecordIteratorNoRace(t *testing.T) {
	now := timestamppb.Now()

	iter := NewTupleRecordSliceIterator([]*TupleRecord{
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
	}, nil, MapTupleRecordToTupleProtobuf)
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
}

func TestTupleRecordIteratorContextCanceled(t *testing.T) {
	iter := NewTupleRecordSliceIterator([]*TupleRecord{
		{
			ObjectType: "document",
			ObjectID:   "1",
			Relation:   "viewer",
			User:       "user:jon",
		},
	}, nil, MapTupleRecordToTupleProtobuf)
	defer iter.Stop()

	ctx, cancel := context.WithCancel(context.Background())

	_, err := iter.Next(ctx)
	require.NoError(t, err)

	cancel()

	_, err = iter.Next(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestTupleRecordIteratorContextDeadlineExceeded(t *testing.T) {
	iter := NewTupleRecordSliceIterator([]*TupleRecord{
		{
			ObjectType: "document",
			ObjectID:   "1",
			Relation:   "viewer",
			User:       "user:jon",
		},
	}, nil, MapTupleRecordToTupleProtobuf)
	defer iter.Stop()

	deadlineCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err := iter.Next(deadlineCtx)
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	_, err = iter.Next(deadlineCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
