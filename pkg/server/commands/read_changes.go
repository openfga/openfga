package commands

import (
	"context"
	"errors"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/logger"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
)

type ReadChangesQuery struct {
	backend       storage.ChangelogBackend
	logger        logger.Logger
	horizonOffset time.Duration
}

type ReadChangesQueryOption func(*ReadChangesQuery)

func WithReadChangesQueryLogger(l logger.Logger) ReadChangesQueryOption {
	return func(rq *ReadChangesQuery) {
		rq.logger = l
	}
}

// WithReadChangeQueryHorizonOffset specifies duration in minutes.
func WithReadChangeQueryHorizonOffset(horizonOffset int) ReadChangesQueryOption {
	return func(rq *ReadChangesQuery) {
		rq.horizonOffset = time.Duration(horizonOffset) * time.Minute
	}
}

// NewReadChangesQuery creates a ReadChangesQuery with specified `ChangelogBackend`.
func NewReadChangesQuery(backend storage.ChangelogBackend, opts ...ReadChangesQueryOption) *ReadChangesQuery {
	rq := &ReadChangesQuery{
		backend:       backend,
		logger:        logger.NewNoopLogger(),
		horizonOffset: time.Duration(serverconfig.DefaultChangelogHorizonOffset) * time.Minute,
	}

	for _, opt := range opts {
		opt(rq)
	}
	return rq
}

// Execute the ReadChangesQuery, returning paginated `openfga.TupleChange`(s) and a possibly non-empty continuation token.
func (q *ReadChangesQuery) Execute(ctx context.Context, req *openfgav1.ReadChangesRequest) (*openfgav1.ReadChangesResponse, error) {
	var startTime time.Time
	if req.GetStartTime() != nil {
		startTime = req.GetStartTime().AsTime()
	}

	opts := storage.ReadChangesOptions{
		Pagination: storage.NewPaginationOptions(
			req.GetPageSize().GetValue(),
			req.GetContinuationToken(),
		),
	}
	filter := storage.ReadChangesFilter{
		ObjectType:    req.GetType(),
		HorizonOffset: q.horizonOffset,
		StartTime:     startTime,
	}
	changes, contToken, err := q.backend.ReadChanges(ctx, req.GetStoreId(), filter, opts)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return &openfgav1.ReadChangesResponse{
				ContinuationToken: req.GetContinuationToken(),
			}, nil
		}
		return nil, serverErrors.HandleError("", err)
	}

	return &openfgav1.ReadChangesResponse{
		Changes:           changes,
		ContinuationToken: contToken,
	}, nil
}
