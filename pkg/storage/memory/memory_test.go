package memory

import (
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage/test"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/stretchr/testify/require"
)

func TestMemdbStorage(t *testing.T) {
	ds := New()
	test.RunAllTests(t, ds)
}

func TestStaticTupleIteratorNoRace(t *testing.T) {
	iter := &staticIterator{
		tuples: []*openfgav1.Tuple{
			{
				Key: tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			},
			{
				Key: tuple.NewTupleKey("document:1", "viewer", "user:jon"),
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
