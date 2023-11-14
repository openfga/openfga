// Package testutils contains code that is useful in tests.
package testutils

import (
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	AllChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

var (
	TupleKeyCmpTransformer = cmp.Transformer("Sort", func(in []*openfgav1.TupleKey) []*openfgav1.TupleKey {
		out := append([]*openfgav1.TupleKey(nil), in...) // Copy input to avoid mutating it

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

func MustNewStruct(t *testing.T, v map[string]interface{}) *structpb.Struct {
	conditionContext, err := structpb.NewStruct(v)
	require.NoError(t, err)
	return conditionContext
}

// MakeSliceWithGenerator generates a slice of length 'n' and populates the contents
// with values based on the generator provided.
func MakeSliceWithGenerator[T any](n uint64, generator func(n uint64) any) []T {

	s := make([]T, 0, n)

	for i := uint64(0); i < n; i++ {
		s = append(s, generator(i).(T))
	}

	return s
}

// NumericalStringGenerator generates a string representation of the provided
// uint value.
func NumericalStringGenerator(n uint64) any {
	return strconv.FormatUint(uint64(n), 10)
}
