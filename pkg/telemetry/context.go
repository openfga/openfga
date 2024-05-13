package telemetry

import (
	"context"
)

type rpcContextName string
type dispatchThrottlingThresholdType uint32

const (
	rpcInfoContextName rpcContextName = "rpcInfo"
	Throttled          string         = "Throttled"
)

const (
	dispatchThrottlingThreshold dispatchThrottlingThresholdType = iota
)

type RPCInfo struct {
	Method  string
	Service string
}

// ContextWithRPCInfo will save the rpc method and service information in context.
func ContextWithRPCInfo(ctx context.Context, rpcInfo RPCInfo) context.Context {
	return context.WithValue(ctx, rpcInfoContextName, rpcInfo)
}

// RPCInfoFromContext returns method and service stored in context.
func RPCInfoFromContext(ctx context.Context) RPCInfo {
	rpcInfo, ok := ctx.Value(rpcInfoContextName).(RPCInfo)
	if ok {
		return rpcInfo
	}
	return RPCInfo{
		Method:  "unknown",
		Service: "unknown",
	}
}

// ContextWithDispatchThrottlingThreshold will save the dispatch throttling threshold in context.
func ContextWithDispatchThrottlingThreshold(ctx context.Context, threshold uint32) context.Context {
	return context.WithValue(ctx, dispatchThrottlingThreshold, threshold)
}

// DispatchThrottlingThresholdFromContext returns the dispatch throttling threshold saved in context
// Return 0 if not found.
func DispatchThrottlingThresholdFromContext(ctx context.Context) uint32 {
	thresholdInContext := ctx.Value(dispatchThrottlingThreshold)
	if thresholdInContext != nil {
		thresholdInInt, ok := thresholdInContext.(uint32)
		if ok {
			return thresholdInInt
		}
	}
	return 0
}
