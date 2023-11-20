package validator

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	maxRequestContextSizeBytes = 131072 // 128KiB
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

	bytes, err := proto.Marshal(reqContext)
	if err != nil {
		return err
	}

	contextSize := len(bytes)
	if contextSize > maxRequestContextSizeBytes {
		return status.Errorf(codes.InvalidArgument, "request 'context' size limit exceeded - %d (bytes) exceeds limit of %d (bytes)", contextSize, maxRequestContextSizeBytes)
	}

	return nil
}
