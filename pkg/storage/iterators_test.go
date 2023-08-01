package storage

import (
	"errors"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/stretchr/testify/require"
)

func TestStaticTupleKeyIterator(t *testing.T) {
	expected := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:doc1", "viewer", "bill"),
		tuple.NewTupleKey("document:doc2", "editor", "bob"),
	}

	iter := NewStaticTupleKeyIterator(expected)

	var actual []*openfgav1.TupleKey
	for {
		tk, err := iter.Next()
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
		tk, err := iter.Next()
		if err != nil {
			if errors.Is(err, ErrIteratorDone) {
				break
			}
			require.Fail(t, "no error was expected")
		}

		actual = append(actual, tk)
	}

	cmpOpts := []cmp.Option{
		cmpopts.IgnoreUnexported(openfgav1.TupleKey{}),
		testutils.TupleKeyCmpTransformer,
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
		tuple, err := iter.Next()
		if err != nil {
			if err == ErrIteratorDone {
				break
			}

			// handle the error in some way
			panic(err)
		}

		filtered = append(filtered, fmt.Sprintf("%s#%s@%s", tuple.GetObject(), tuple.GetRelation(), tuple.GetUser()))
	}

	fmt.Println(filtered)
	// Output: [document:doc1#editor@user:elbuo]
}
