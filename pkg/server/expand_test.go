package server

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/tuple"
)

func TestExpand_ContextualTupleLimit(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	testServer := &Server{maxContextualTuples: 2}

	_, err := testServer.Expand(ctx, &openfgav1.ExpandRequest{
		StoreId:  "01K3RZVNE3NJ4FYKK99QN013G2",
		TupleKey: tuple.NewExpandRequestTupleKey("document:1", "viewer"),
		ContextualTuples: &openfgav1.ContextualTupleKeys{
			TupleKeys: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:bob"),
				tuple.NewTupleKey("document:1", "viewer", "user:carol"),
				tuple.NewTupleKey("document:1", "viewer", "user:dave"),
			},
		},
	})

	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "exceeds server limit")
}