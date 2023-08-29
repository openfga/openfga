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

// ContextWithRPCInfo will save the rpc method and service information in context
func ContextWithRPCInfo(ctx context.Context) context.Context {
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

type RPCInfo struct {
	Method  string
	Service string
}

// RPCInfoFromContext returns method and service stored in context
func RPCInfoFromContext(ctx context.Context) RPCInfo {
	return RPCInfo{
		Method:  getContextInfo(ctx, methodContextName),
		Service: getContextInfo(ctx, serviceContextName),
	}
}
