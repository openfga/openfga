package commands

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/typesystem"
)

// ActionSearchQuery handles AuthZEN action search requests.
type ActionSearchQuery struct {
	typesystemResolver   typesystem.TypesystemResolverFunc
	batchCheckFunc       func(ctx context.Context, req *openfgav1.BatchCheckRequest) (*openfgav1.BatchCheckResponse, error)
	authorizationModelID string
}

// ActionSearchQueryOption is a functional option for ActionSearchQuery.
type ActionSearchQueryOption func(*ActionSearchQuery)

// WithTypesystemResolver sets the typesystem resolver function.
func WithTypesystemResolver(resolver typesystem.TypesystemResolverFunc) ActionSearchQueryOption {
	return func(q *ActionSearchQuery) {
		q.typesystemResolver = resolver
	}
}

// WithBatchCheckFunc sets the BatchCheck function to use.
func WithBatchCheckFunc(fn func(ctx context.Context, req *openfgav1.BatchCheckRequest) (*openfgav1.BatchCheckResponse, error)) ActionSearchQueryOption {
	return func(q *ActionSearchQuery) {
		q.batchCheckFunc = fn
	}
}

// WithActionSearchAuthorizationModelID sets the authorization model ID to use.
func WithActionSearchAuthorizationModelID(id string) ActionSearchQueryOption {
	return func(q *ActionSearchQuery) {
		q.authorizationModelID = id
	}
}

// NewActionSearchQuery creates a new ActionSearchQuery.
func NewActionSearchQuery(opts ...ActionSearchQueryOption) *ActionSearchQuery {
	q := &ActionSearchQuery{}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

// Execute runs the action search and returns permitted actions.
// Note: Pagination is not currently supported per AuthZEN spec (optional feature).
// The page request parameter is ignored and all results are returned.
func (q *ActionSearchQuery) Execute(
	ctx context.Context,
	req *authzenv1.ActionSearchRequest,
) (*authzenv1.ActionSearchResponse, error) {
	// Get typesystem for the model
	typesys, err := q.typesystemResolver(ctx, req.GetStoreId(), q.authorizationModelID)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve typesystem: %w", err)
	}

	// Get all relations for the resource type
	relations, err := typesys.GetRelations(req.GetResource().GetType())
	if err != nil {
		return nil, fmt.Errorf("failed to get relations for type %s: %w", req.GetResource().GetType(), err)
	}

	// Merge properties to context
	mergedContext, err := MergePropertiesToContext(
		req.GetContext(),
		req.GetSubject(),
		req.GetResource(),
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to merge properties to context: %w", err)
	}

	// Prepare user and object strings
	user := fmt.Sprintf("%s:%s", req.GetSubject().GetType(), req.GetSubject().GetId())
	object := fmt.Sprintf("%s:%s", req.GetResource().GetType(), req.GetResource().GetId())

	// Sort relation names for consistent ordering
	relationNames := make([]string, 0, len(relations))
	for name := range relations {
		relationNames = append(relationNames, name)
	}
	sort.Strings(relationNames)

	// Build batch check request with all relations
	checks := make([]*openfgav1.BatchCheckItem, 0, len(relationNames))
	for i, relationName := range relationNames {
		checks = append(checks, &openfgav1.BatchCheckItem{
			TupleKey: &openfgav1.CheckRequestTupleKey{
				User:     user,
				Relation: relationName,
				Object:   object,
			},
			Context:       mergedContext,
			CorrelationId: strconv.Itoa(i),
		})
	}

	// Execute batch check - single call instead of N individual checks
	batchResp, err := q.batchCheckFunc(ctx, &openfgav1.BatchCheckRequest{
		StoreId:              req.GetStoreId(),
		AuthorizationModelId: q.authorizationModelID,
		Checks:               checks,
	})
	if err != nil {
		return nil, fmt.Errorf("batch check failed: %w", err)
	}

	// Process batch check results
	var actions []*authzenv1.Action
	for correlationID, result := range batchResp.GetResult() {
		// Skip if there was an error for this check
		if result.GetError() != nil {
			continue
		}

		if result.GetAllowed() {
			idx, err := strconv.Atoi(correlationID)
			if err != nil {
				continue
			}
			if idx >= 0 && idx < len(relationNames) {
				actions = append(actions, &authzenv1.Action{
					Name: relationNames[idx],
				})
			}
		}
	}

	// Sort actions alphabetically for consistent ordering
	sort.Slice(actions, func(i, j int) bool {
		return actions[i].GetName() < actions[j].GetName()
	})

	// Return all results without pagination (pagination not supported)
	return &authzenv1.ActionSearchResponse{
		Results: actions,
	}, nil
}
