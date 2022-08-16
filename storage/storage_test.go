package storage

import (
	"errors"
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
			if errors.Is(err, IteratorDone) {
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
			if errors.Is(err, IteratorDone) {
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
