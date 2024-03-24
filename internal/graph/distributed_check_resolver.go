package graph

import (
	"context"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/dispatcher"
)

type DistributedCheckResolver struct {
	delegate dispatcher.Dispatcher
}

var _ dispatcher.Dispatcher = (*DistributedCheckResolver)(nil)

// Close implements CheckResolver.
func (*DistributedCheckResolver) Close() {}

func (c DistributedCheckResolver) Dispatch(ctx context.Context, request *openfgav1.BaseRequest, metadata *openfgav1.DispatchMetadata, additionalParameters any) (*openfgav1.BaseResponse, *openfgav1.DispatchMetadata, error) {
	return nil, nil, nil
}
