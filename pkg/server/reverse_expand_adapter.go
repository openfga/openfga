package server

import (
	"context"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/throttler"
	"github.com/openfga/openfga/internal/throttler/threshold"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/commands/reverseexpand"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// reverseExpandAdapter adapts reverseexpand.ReverseExpandQuery to graph.ReverseExpandQueryExecutor
// to break the import cycle between graph and reverseexpand packages.
type reverseExpandAdapter struct {
	query     *reverseexpand.ReverseExpandQuery
	datastore storage.RelationshipTupleReader
}

var _ graph.ReverseExpandQueryExecutor = (*reverseExpandAdapter)(nil)

func (a *reverseExpandAdapter) Execute(
	ctx context.Context,
	req graph.ReverseExpandRequest,
	resultChan chan<- graph.ReverseExpandResult,
	metadata graph.ReverseExpandResolutionMetadata,
) error {
	// Convert graph types to reverseexpand types
	userRef := convertUserRefToReverseExpand(req.User)
	if userRef == nil {
		return nil
	}

	var contextStruct *structpb.Struct
	if req.Context != nil {
		if s, ok := req.Context.(*structpb.Struct); ok {
			contextStruct = s
		}
	}

	reverseExpandReq := &reverseexpand.ReverseExpandRequest{
		StoreID:          req.StoreID,
		ObjectType:       req.ObjectType,
		Relation:         req.Relation,
		User:             userRef,
		ContextualTuples: req.ContextualTuples,
		Context:          contextStruct,
		Consistency:      req.Consistency,
	}

	reverseExpandResultChan := make(chan *reverseexpand.ReverseExpandResult, cap(resultChan))
	reverseExpandMetadata := reverseexpand.NewResolutionMetadata()

	err := a.query.Execute(ctx, reverseExpandReq, reverseExpandResultChan, reverseExpandMetadata)
	if err != nil {
		return err
	}

	// Convert results
	for result := range reverseExpandResultChan {
		graphResult := graph.ReverseExpandResult{
			Object:       result.Object,
			ResultStatus: int(result.ResultStatus),
		}
		select {
		case resultChan <- graphResult:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Update metadata after query completes
	// Copy dispatch counter from reverse expand metadata
	metadata.SetDispatchCounter(reverseExpandMetadata.DispatchCounter.Load())

	// Try to get datastore metadata if the datastore supports it
	if ds, ok := a.datastore.(storagewrappers.StorageInstrumentation); ok {
		dsMeta := ds.GetMetadata()
		metadata.SetDatastoreQueryCount(dsMeta.DatastoreQueryCount)
		metadata.SetDatastoreItemCount(dsMeta.DatastoreItemCount)
	}

	return nil
}

func convertUserRefToReverseExpand(userRef graph.ReverseExpandUserRef) reverseexpand.IsUserRef {
	if userRef == nil {
		return nil
	}

	// Try to determine the type by checking the string representation
	userStr := userRef.String()
	if tuple.IsTypedWildcard(userStr) {
		return &reverseexpand.UserRefTypedWildcard{
			Type: tuple.GetType(userStr),
		}
	}

	if tuple.IsObjectRelation(userStr) {
		obj, rel := tuple.SplitObjectRelation(userStr)
		return &reverseexpand.UserRefObjectRelation{
			ObjectRelation: &openfgav1.ObjectRelation{
				Object:   obj,
				Relation: rel,
			},
		}
	}

	objType, objID := tuple.SplitObject(userStr)
	return &reverseexpand.UserRefObject{
		Object: &openfgav1.Object{
			Type: objType,
			Id:   objID,
		},
	}
}

// newReverseExpandAdapter creates a new adapter for reverse expand queries.
func newReverseExpandAdapter(
	datastore storage.RelationshipTupleReader,
	typesystem *typesystem.TypeSystem,
	resolveNodeLimit uint32,
	resolveNodeBreadthLimit uint32,
	logger logger.Logger,
) graph.ReverseExpandQueryExecutor {
	query := reverseexpand.NewReverseExpandQuery(
		datastore,
		typesystem,
		reverseexpand.WithResolveNodeLimit(resolveNodeLimit),
		reverseexpand.WithResolveNodeBreadthLimit(resolveNodeBreadthLimit),
		reverseexpand.WithLogger(logger),
		reverseexpand.WithDispatchThrottlerConfig(threshold.Config{
			Throttler:    throttler.NewNoopThrottler(),
			Enabled:      false, // Disable throttling for check optimization
			Threshold:    0,
			MaxThreshold: 0,
		}),
	)
	return &reverseExpandAdapter{
		query:     query,
		datastore: datastore,
	}
}
