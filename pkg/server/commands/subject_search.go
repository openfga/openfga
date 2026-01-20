package commands

import (
	"context"
	"fmt"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

// SubjectSearchQuery handles AuthZEN subject search requests.
type SubjectSearchQuery struct {
	listUsersFunc        func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error)
	authorizationModelID string
}

// SubjectSearchQueryOption is a functional option for SubjectSearchQuery.
type SubjectSearchQueryOption func(*SubjectSearchQuery)

// WithListUsersFunc sets the ListUsers function to use.
func WithListUsersFunc(fn func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error)) SubjectSearchQueryOption {
	return func(q *SubjectSearchQuery) {
		q.listUsersFunc = fn
	}
}

// WithAuthorizationModelID sets the authorization model ID to use.
func WithAuthorizationModelID(id string) SubjectSearchQueryOption {
	return func(q *SubjectSearchQuery) {
		q.authorizationModelID = id
	}
}

// NewSubjectSearchQuery creates a new SubjectSearchQuery.
func NewSubjectSearchQuery(opts ...SubjectSearchQueryOption) *SubjectSearchQuery {
	q := &SubjectSearchQuery{}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

// Execute runs the subject search and returns matching subjects.
// Note: Pagination is not currently supported per AuthZEN spec (optional feature).
// The page request parameter is ignored and all results are returned.
func (q *SubjectSearchQuery) Execute(
	ctx context.Context,
	req *authzenv1.SubjectSearchRequest,
) (*authzenv1.SubjectSearchResponse, error) {
	// Merge properties to context
	mergedContext, err := MergePropertiesToContext(
		req.GetContext(),
		req.GetSubject(),
		req.GetResource(),
		req.GetAction(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to merge properties to context: %w", err)
	}

	// Validate that subject type is specified (required for ListUsers)
	if req.GetSubject() == nil || req.GetSubject().GetType() == "" {
		return nil, fmt.Errorf("subject type is required for subject search")
	}

	// Validate resource is provided
	if req.GetResource() == nil {
		return nil, fmt.Errorf("resource is required for subject search")
	}

	// Validate action is provided
	if req.GetAction() == nil {
		return nil, fmt.Errorf("action is required for subject search")
	}

	// Validate listUsersFunc is configured
	if q.listUsersFunc == nil {
		return nil, fmt.Errorf("listUsersFunc not configured")
	}

	// Build ListUsers request
	listUsersReq := &openfgav1.ListUsersRequest{
		StoreId:              req.GetStoreId(),
		AuthorizationModelId: q.authorizationModelID,
		Object: &openfgav1.Object{
			Type: req.GetResource().GetType(),
			Id:   req.GetResource().GetId(),
		},
		Relation: req.GetAction().GetName(),
		Context:  mergedContext,
		UserFilters: []*openfgav1.UserTypeFilter{
			{Type: req.GetSubject().GetType()},
		},
	}

	// Execute ListUsers (returns all results)
	resp, err := q.listUsersFunc(ctx, listUsersReq)
	if err != nil {
		return nil, fmt.Errorf("ListUsers failed: %w", err)
	}

	// Convert to AuthZEN subjects
	var subjects []*authzenv1.Subject
	for _, user := range resp.GetUsers() {
		subject := userToSubject(user)
		if subject != nil {
			subjects = append(subjects, subject)
		}
	}

	// Return all results without pagination (pagination not supported)
	return &authzenv1.SubjectSearchResponse{
		Results: subjects,
	}, nil
}

func userToSubject(user *openfgav1.User) *authzenv1.Subject {
	if user.GetObject() != nil {
		obj := user.GetObject()
		return &authzenv1.Subject{
			Type: obj.GetType(),
			Id:   obj.GetId(),
		}
	}
	// Handle wildcard users
	if user.GetWildcard() != nil {
		return &authzenv1.Subject{
			Type: user.GetWildcard().GetType(),
			Id:   "*",
		}
	}
	return nil
}
