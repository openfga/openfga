package memory

import (
	"testing"

	"github.com/openfga/openfga/pkg/storage/test"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

var memoryStorage *MemoryBackend

func init() {
	memoryStorage = New(10000, 10000)
}

func TestMemdbStorage(t *testing.T) {
	ds := New(10, 24)
	test.RunAllTests(t, ds)
}

func TestStaticTupleIteratorNoRace(t *testing.T) {
	iter := &staticIterator{
		tuples: []*openfgapb.Tuple{
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
