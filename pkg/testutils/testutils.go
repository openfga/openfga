package testutils

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/openfga/openfga/pkg/id"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

const (
	AllChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

var (
	TupleKeyCmpTransformer = cmp.Transformer("Sort", func(in []*openfgapb.TupleKey) []*openfgapb.TupleKey {
		out := append([]*openfgapb.TupleKey(nil), in...) // Copy input to avoid mutating it

		sort.SliceStable(out, func(i, j int) bool {
			if out[i].Object > out[j].Object {
				return false
			}

			if out[i].Relation > out[j].Relation {
				return false
			}

			if out[i].User > out[j].User {
				return false
			}

			return true
		})

		return out
	})
)

func CreateRandomString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = AllChars[rand.Intn(len(AllChars))]
	}
	return string(b)
}

func RandomID(t *testing.T) string {
	id, err := id.NewString()
	require.NoError(t, err)

	return id
}
