package commands

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

const (
	DefaultSearchLimit = 50
	MaxSearchLimit     = 1000
)

// PaginationToken stores pagination state for search operations.
type PaginationToken struct {
	Offset int `json:"offset"`
}

// SubjectSearchQuery handles AuthZEN subject search requests.
type SubjectSearchQuery struct {
	listUsersFunc func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error)
}

// SubjectSearchQueryOption is a functional option for SubjectSearchQuery.
type SubjectSearchQueryOption func(*SubjectSearchQuery)

// WithListUsersFunc sets the ListUsers function to use.
func WithListUsersFunc(fn func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error)) SubjectSearchQueryOption {
	return func(q *SubjectSearchQuery) {
		q.listUsersFunc = fn
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

// Execute runs the subject search and returns matching subjects with pagination.
func (q *SubjectSearchQuery) Execute(
	ctx context.Context,
	req *authzenv1.SubjectSearchRequest,
) (*authzenv1.SubjectSearchResponse, error) {
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

	// Validate that subject type is specified (required for ListUsers)
	if req.GetSubject() == nil || req.GetSubject().GetType() == "" {
		return nil, fmt.Errorf("subject type is required for subject search")
	}

	// Build ListUsers request
	listUsersReq := &openfgav1.ListUsersRequest{
		StoreId:              req.GetStoreId(),
		AuthorizationModelId: req.GetAuthorizationModelId(),
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

	// Execute ListUsers (returns all results - no native pagination)
	resp, err := q.listUsersFunc(ctx, listUsersReq)
	if err != nil {
		return nil, fmt.Errorf("ListUsers failed: %w", err)
	}

	// Convert to AuthZEN subjects
	var allSubjects []*authzenv1.Subject
	for _, user := range resp.GetUsers() {
		subject := userToSubject(user)
		if subject != nil {
			allSubjects = append(allSubjects, subject)
		}
	}

	// Sort subjects for consistent pagination across calls (only when pagination is requested)
	if req.GetPage() != nil {
		sort.Slice(allSubjects, func(i, j int) bool {
			if allSubjects[i].GetType() != allSubjects[j].GetType() {
				return allSubjects[i].GetType() < allSubjects[j].GetType()
			}
			return allSubjects[i].GetId() < allSubjects[j].GetId()
		})
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
	total := len(allSubjects)
	start := offset
	end := offset + int(limit)
	if start > total {
		start = total
	}
	if end > total {
		end = total
	}

	pagedSubjects := allSubjects[start:end]

	// Generate next token
	var nextToken string
	if end < total {
		nextToken = encodePaginationToken(&PaginationToken{Offset: end})
	}

	totalUint32 := uint32(total)
	return &authzenv1.SubjectSearchResponse{
		Subjects: pagedSubjects,
		Page: &authzenv1.PageResponse{
			NextToken: nextToken,
			Count:     uint32(len(pagedSubjects)),
			Total:     &totalUint32,
		},
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

func getLimit(page *authzenv1.PageRequest) uint32 {
	if page == nil || page.GetLimit() == 0 {
		return DefaultSearchLimit
	}
	if page.GetLimit() > MaxSearchLimit {
		return MaxSearchLimit
	}
	return page.GetLimit()
}

func encodePaginationToken(token *PaginationToken) string {
	data, _ := json.Marshal(token)
	return base64.StdEncoding.EncodeToString(data)
}

func decodePaginationToken(s string) (*PaginationToken, error) {
	data, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	var token PaginationToken
	if err := json.Unmarshal(data, &token); err != nil {
		return nil, err
	}
	return &token, nil
}
