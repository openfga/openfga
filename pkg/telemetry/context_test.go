package telemetry

import (
	"context"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
)

func TestDefaultInfo(t *testing.T) {
	ctx := ContextWithRPCInfo(context.Background())

	rpcInfo := RPCInfoFromContext(ctx)
	require.Equal(t, RPCInfo{
		Method:  "unknown",
		Service: openfgav1.OpenFGAService_ServiceDesc.ServiceName,
	}, rpcInfo)

}

func TestUnsetInfo(t *testing.T) {
	rpcInfo := RPCInfoFromContext(context.Background())
	require.Equal(t, RPCInfo{
		Method:  "unknown",
		Service: "unknown",
	}, rpcInfo)
}

func TestKnownInfo(t *testing.T) {
	ctx := saveMethodServiceInContext(context.Background(), "/grpc/check", "openfga")

	rpcInfo := RPCInfoFromContext(ctx)
	require.Equal(t, RPCInfo{
		Method:  "check",
		Service: openfgav1.OpenFGAService_ServiceDesc.ServiceName,
	}, rpcInfo)
}
