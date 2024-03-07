package telemetry

import (
	"context"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
)

func TestKnownRPCInfo(t *testing.T) {
	rpcInfo := RPCInfo{
		Method:  "check",
		Service: openfgav1.OpenFGAService_ServiceDesc.ServiceName,
	}
	ctx := ContextWithRPCInfo(context.Background(), rpcInfo)

	output := RPCInfoFromContext(ctx)
	require.Equal(t, rpcInfo, output)
}

func TestUnknownRPCInfo(t *testing.T) {
	output := RPCInfoFromContext(context.Background())
	require.Equal(t, RPCInfo{
		Method:  "unknown",
		Service: "unknown",
	}, output)
}
