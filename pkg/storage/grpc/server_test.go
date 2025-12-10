package grpc

import (
	"testing"

	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/stretchr/testify/require"
)

func TestNewServer(t *testing.T) {
	ds := memory.New()
	server := NewServer(ds)
	require.NotNil(t, server)
}
