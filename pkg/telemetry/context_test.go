package telemetry

import (
	"context"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
)

func TestDefaultInfo(t *testing.T) {
	ctx := SaveRPCInfoInContext(context.Background())

	method := Method(ctx)
	require.Equal(t, "unknown", method)

	service := Service(ctx)
	require.Equal(t, openfgav1.OpenFGAService_ServiceDesc.ServiceName, service)
}

func TestUnsetInfo(t *testing.T) {
	method := Method(context.Background())
	require.Equal(t, "unknown", method)

	service := Service(context.Background())
	require.Equal(t, "unknown", service)
}

func TestKnownInfo(t *testing.T) {
	ctx := saveMethodServiceInContext(context.Background(), "/grpc/check", "openfga")

	method := Method(ctx)
	require.Equal(t, "check", method)

	service := Service(ctx)
	require.Equal(t, "openfga", service)
}
