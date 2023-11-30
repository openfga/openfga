package validator

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	maxRequestContextSizeBytes = 64 * 1_204 // 64KB
)

type requestWithContext interface {
	GetContext() *structpb.Struct
}

func requestContextWithinLimits(req interface{}) error {
	reqWithContext, ok := req.(requestWithContext)
	if !ok {
		return nil
	}

	reqContext := reqWithContext.GetContext()

	contextSize := proto.Size(reqContext)
	if contextSize > maxRequestContextSizeBytes {
		return status.Errorf(codes.InvalidArgument, "request 'context' size limit exceeded - %d (bytes) exceeds limit of %d (bytes)", contextSize, maxRequestContextSizeBytes)
	}

	return nil
}
