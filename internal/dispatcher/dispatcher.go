package dispatcher

import "context"

type DispatchRequest interface {
	SetDispatchCount()
	GetDispatchCount() uint32
}

type DispatchResponse interface {
}

type Dispatcher interface {
	Dispatch(ctx context.Context, request DispatchRequest) (DispatchResponse, error)
	Close()
}
