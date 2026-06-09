package iterator

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrorIteratorIsOrdered(t *testing.T) {
	iter := Error[string](errors.New("sentinel"))
	defer iter.Stop()
	require.True(t, iter.IsOrdered())
}
