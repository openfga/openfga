package storage

import (
	"context"
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

func TestNewPaginationOptions(t *testing.T) {
	type test struct {
		size             int32
		token            string
		expectedPageSize int
		expectedFrom     string
	}
	tests := []test{
		{
			size:             0,
			token:            "",
			expectedPageSize: DefaultPageSize,
			expectedFrom:     "",
		},
		{
			size:             0,
			token:            "test",
			expectedPageSize: DefaultPageSize,
			expectedFrom:     "test",
		},
	}

	for _, test := range tests {
		opts := NewPaginationOptions(test.size, test.token)
		if opts.PageSize != test.expectedPageSize {
			t.Errorf("Expected PageSize: %d, got %d", test.expectedPageSize, opts.PageSize)
		}
		if opts.From != test.expectedFrom {
			t.Errorf("Expected From: %s, got %s", test.expectedFrom, opts.From)
		}
	}
}

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
	iter := NewCombinedIterator(iter1, iter2, context.Background())

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

	if diff := cmp.Diff(actual, expected, cmpOpts...); diff != "" {
		t.Fatalf("mismatch (-got +want):\n%s", diff)
	}
}

func TestUniqueObjectIterator(t *testing.T) {

	expected := []string{
		"document:1",
		"document:2",
		"document:3",
		"document:4",
	}

	iter1 := NewStaticObjectIterator([]*openfgapb.Object{
		{Type: "document", Id: "1"},
		{Type: "document", Id: "2"},
		{Type: "document", Id: "2"},
	})
	iter2 := NewStaticObjectIterator([]*openfgapb.Object{
		{Type: "document", Id: "2"},
		{Type: "document", Id: "3"},
		{Type: "document", Id: "4"},
	})

	iter := NewUniqueObjectIterator(iter1, iter2, context.Background())
	defer iter.Stop()

	var actual []string
	for {
		obj, err := iter.Next()
		if err != nil {
			if errors.Is(err, ErrIteratorDone) {
				break
			}

			require.Fail(t, "no error was expected")
		}

		actual = append(actual, tuple.ObjectKey(obj))
	}

	require.Equal(t, expected, actual)
}

func ExampleNewUniqueObjectIterator() {

	contextualTuples := []*openfgapb.TupleKey{
		tuple.NewTupleKey("document:doc1", "viewer", "jon"),
		tuple.NewTupleKey("document:doc1", "viewer", "elbuo"),
	}

	iter1 := NewTupleKeyObjectIterator(contextualTuples)

	// this would generally be a database call
	iter2 := NewStaticObjectIterator([]*openfgapb.Object{
		{
			Type: "document",
			Id:   "doc1",
		},
		{
			Type: "document",
			Id:   "doc2",
		},
	})

	// pass the contextual tuples iterator (iter1) first since it's more
	// constrained than the other iterator (iter2). In practice iter2 will
	// be coming from a database that should guarantee uniqueness over the
	// objects yielded.
	iter := NewUniqueObjectIterator(iter1, iter2, context.Background())
	defer iter.Stop()

	var objects []string
	for {
		obj, err := iter.Next()
		if err != nil {
			if err == ErrIteratorDone {
				break
			}

			// handle the error in some way
			panic(err)
		}

		objects = append(objects, tuple.ObjectKey(obj))
	}

	fmt.Println(objects)
	// Output: [document:doc1 document:doc2]
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
