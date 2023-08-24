package telemetry

import (
	"context"
	"strings"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/grpc"
)

type rpcContextName string

const (
	methodContextName  rpcContextName = "method"
	serviceContextName rpcContextName = "service"
)

func saveMethodServiceInContext(ctx context.Context, method string, service string) context.Context {
	methods := strings.Split(method, "/")

	ctx = context.WithValue(ctx, methodContextName, methods[len(methods)-1])
	return context.WithValue(ctx, serviceContextName, service)
}

// SaveRPCInfoInContext will save the rpc method and service information in context
func SaveRPCInfoInContext(ctx context.Context) context.Context {
	method, found := grpc.Method(ctx)
	if !found {
		method = "unknown"
	}
	return saveMethodServiceInContext(ctx, method, openfgav1.OpenFGAService_ServiceDesc.ServiceName)
}

func getContextInfo(ctx context.Context, key rpcContextName) string {
	stringVal, ok := ctx.Value(key).(string)
	if !ok {
		stringVal = "unknown"
	}
	return stringVal
}

// Method returns the rpc method stored in the context
func Method(ctx context.Context) string {
	return getContextInfo(ctx, methodContextName)
}

// Service returns the rpc service stored in the context
func Service(ctx context.Context) string {
	return getContextInfo(ctx, serviceContextName)
}
