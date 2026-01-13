package commands

import (
	"context"
	"fmt"
	"sort"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/typesystem"
)

// ActionSearchQuery handles AuthZEN action search requests.
type ActionSearchQuery struct {
	typesystemResolver typesystem.TypesystemResolverFunc
	checkFunc          func(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error)
}

// ActionSearchQueryOption is a functional option for ActionSearchQuery.
type ActionSearchQueryOption func(*ActionSearchQuery)

// WithTypesystemResolver sets the typesystem resolver function.
func WithTypesystemResolver(resolver typesystem.TypesystemResolverFunc) ActionSearchQueryOption {
	return func(q *ActionSearchQuery) {
		q.typesystemResolver = resolver
	}
}

// WithCheckFunc sets the Check function to use.
func WithCheckFunc(fn func(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error)) ActionSearchQueryOption {
	return func(q *ActionSearchQuery) {
		q.checkFunc = fn
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

// Execute runs the action search and returns permitted actions with pagination.
func (q *ActionSearchQuery) Execute(
	ctx context.Context,
	req *authzenv1.ActionSearchRequest,
) (*authzenv1.ActionSearchResponse, error) {
	// Get typesystem for the model
	typesys, err := q.typesystemResolver(ctx, req.GetStoreId(), req.GetAuthorizationModelId())
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

	// Check each relation to see if subject has access
	var permittedActions []*authzenv1.Action

	// Sort relation names for consistent ordering
	relationNames := make([]string, 0, len(relations))
	for name := range relations {
		relationNames = append(relationNames, name)
	}
	sort.Strings(relationNames)

	for _, relationName := range relationNames {
		checkReq := &openfgav1.CheckRequest{
			StoreId:              req.GetStoreId(),
			AuthorizationModelId: req.GetAuthorizationModelId(),
			TupleKey: &openfgav1.CheckRequestTupleKey{
				User:     user,
				Relation: relationName,
				Object:   object,
			},
			Context: mergedContext,
		}

		checkResp, err := q.checkFunc(ctx, checkReq)
		if err != nil {
			// Log error but continue with other relations
			continue
		}

		if checkResp.GetAllowed() {
			permittedActions = append(permittedActions, &authzenv1.Action{
				Name: relationName,
			})
		}
	}

	// Apply pagination
	limit := getLimit(req.GetPage())
	offset := 0

	if req.GetPage() != nil && req.GetPage().GetToken() != "" {
		token, err := decodePaginationToken(req.GetPage().GetToken())
		if err != nil {
			return nil, fmt.Errorf("invalid pagination token: %w", err)
		}
		offset = token.Offset
	}

	// Slice results
	total := len(permittedActions)
	start := offset
	end := offset + int(limit)
	if start > total {
		start = total
	}
	if end > total {
		end = total
	}

	pagedActions := permittedActions[start:end]

	// Generate next token
	var nextToken string
	if end < total {
		nextToken = encodePaginationToken(&PaginationToken{Offset: end})
	}

	totalUint32 := uint32(total)
	return &authzenv1.ActionSearchResponse{
		Actions: pagedActions,
		Page: &authzenv1.PageResponse{
			NextToken: nextToken,
			Count:     uint32(len(pagedActions)),
			Total:     &totalUint32,
		},
	}, nil
}
