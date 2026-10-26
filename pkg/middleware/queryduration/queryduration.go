package queryduration

import (
	"context"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// QueryDurationHeader is the response header that holds the time, in whole
// milliseconds, the server took to resolve a unary request. It is set for both
// successful and failed requests.
const QueryDurationHeader = "Fga-Query-Duration-Ms"

// NewUnaryInterceptor creates a grpc.UnaryServerInterceptor that measures how
// long the rest of the chain and the handler take, and returns that duration in
// the QueryDurationHeader response header. Place it as early in the chain as
// possible so the value covers the work done by the other interceptors.
//
// Streaming RPCs are not covered: their headers are sent with the first
// message, before the total duration is known.
func NewUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()

		resp, err := handler(ctx, req)

		durationMs := strconv.FormatInt(time.Since(start).Milliseconds(), 10)
		_ = grpc.SetHeader(ctx, metadata.Pairs(QueryDurationHeader, durationMs))

		return resp, err
	}
}
