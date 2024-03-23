package dispatcher

import (
	"context"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

type Dispatcher interface {
	Dispatch(ctx context.Context, request *openfgav1.BaseRequest, metadata *openfgav1.DispatchMetadata) (*openfgav1.BaseResponse, *openfgav1.DispatchMetadata, error)
	Close()
}
