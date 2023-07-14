package storage

import (
	"errors"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestStaticTupleKeyIterator(t *testing.T) {
	expected := []*openfgapb.TupleKey{
		tuple.NewTupleKey("document:doc1", "viewer", "bill"),
		tuple.NewTupleKey("document:doc2", "editor", "bob"),
	}

	iter := NewStaticTupleKeyIterator(expected)

	var actual []*openfgapb.TupleKey
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

	expected := []*openfgapb.TupleKey{
		tuple.NewTupleKey("document:doc1", "viewer", "bill"),
		tuple.NewTupleKey("document:doc2", "editor", "bob"),
	}

	iter1 := NewStaticTupleKeyIterator([]*openfgapb.TupleKey{expected[0]})
	iter2 := NewStaticTupleKeyIterator([]*openfgapb.TupleKey{expected[1]})
	iter := NewCombinedIterator(iter1, iter2)

	var actual []*openfgapb.TupleKey
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
		cmpopts.IgnoreUnexported(openfgapb.TupleKey{}),
		testutils.TupleKeyCmpTransformer,
	}

	if diff := cmp.Diff(expected, actual, cmpOpts...); diff != "" {
		t.Fatalf("mismatch (-want +got):\n%s", diff)
	}
}

func ExampleNewFilteredTupleKeyIterator() {

	tuples := []*openfgapb.TupleKey{
		tuple.NewTupleKey("document:doc1", "viewer", "user:jon"),
		tuple.NewTupleKey("document:doc1", "editor", "user:elbuo"),
	}

	iter := NewFilteredTupleKeyIterator(
		NewStaticTupleKeyIterator(tuples),
		func(tk *openfgapb.TupleKey) bool {
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
