package commands

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// StreamedListObjectsFunc is a function type for StreamedListObjects.
type StreamedListObjectsFunc func(req *openfgav1.StreamedListObjectsRequest, srv openfgav1.OpenFGAService_StreamedListObjectsServer) error

// ResourceSearchQuery handles AuthZEN resource search requests.
type ResourceSearchQuery struct {
	streamedListObjectsFunc StreamedListObjectsFunc
}

// ResourceSearchQueryOption is a functional option for ResourceSearchQuery.
type ResourceSearchQueryOption func(*ResourceSearchQuery)

// WithStreamedListObjectsFunc sets the StreamedListObjects function to use.
func WithStreamedListObjectsFunc(fn StreamedListObjectsFunc) ResourceSearchQueryOption {
	return func(q *ResourceSearchQuery) {
		q.streamedListObjectsFunc = fn
	}
}

// NewResourceSearchQuery creates a new ResourceSearchQuery.
func NewResourceSearchQuery(opts ...ResourceSearchQueryOption) *ResourceSearchQuery {
	q := &ResourceSearchQuery{}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

// errLimitReached is a sentinel error to stop streaming when limit is reached.
var errLimitReached = errors.New("limit reached")

// Execute runs the resource search and returns matching resources with pagination.
func (q *ResourceSearchQuery) Execute(
	ctx context.Context,
	req *authzenv1.ResourceSearchRequest,
) (*authzenv1.ResourceSearchResponse, error) {
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

	// Get resource type (required for ListObjects)
	resourceType := ""
	if req.GetResource() != nil && req.GetResource().GetType() != "" {
		resourceType = req.GetResource().GetType()
	} else {
		return nil, fmt.Errorf("resource type is required for resource search")
	}

	// Get pagination parameters
	limit := getLimit(req.GetPage())
	offset := 0

	if req.GetPage() != nil && req.GetPage().GetToken() != "" {
		token, err := decodePaginationToken(req.GetPage().GetToken())
		if err != nil {
			return nil, fmt.Errorf("invalid pagination token: %w", err)
		}
		offset = token.Offset
	}

	// Build StreamedListObjects request
	streamedReq := &openfgav1.StreamedListObjectsRequest{
		StoreId:              req.GetStoreId(),
		AuthorizationModelId: req.GetAuthorizationModelId(),
		User:                 fmt.Sprintf("%s:%s", req.GetSubject().GetType(), req.GetSubject().GetId()),
		Relation:             req.GetAction().GetName(),
		Type:                 resourceType,
		Context:              mergedContext,
	}

	// Create a collector that stops after collecting enough objects
	// We need offset + limit + 1 objects to determine if there are more results
	maxNeeded := offset + int(limit) + 1
	collector := &objectCollector{
		ctx:      ctx,
		objects:  make([]string, 0, maxNeeded),
		maxCount: maxNeeded,
	}

	// Execute StreamedListObjects - will stop early when limit is reached
	err = q.streamedListObjectsFunc(streamedReq, collector)
	if err != nil && !errors.Is(err, errLimitReached) {
		return nil, fmt.Errorf("StreamedListObjects failed: %w", err)
	}

	// Convert to AuthZEN resources
	var allResources []*authzenv1.Resource
	for _, objID := range collector.objects {
		resource := objectIDToResource(objID)
		if resource != nil {
			allResources = append(allResources, resource)
		}
	}

	// Sort resources for consistent pagination across calls (only when pagination is requested)
	if req.GetPage() != nil {
		sort.Slice(allResources, func(i, j int) bool {
			if allResources[i].Type != allResources[j].Type {
				return allResources[i].Type < allResources[j].Type
			}
			return allResources[i].Id < allResources[j].Id
		})
	}

	// Apply pagination (offset already handled by maxNeeded calculation)
	total := len(allResources)
	start := offset
	end := offset + int(limit)
	if start > total {
		start = total
	}
	if end > total {
		end = total
	}

	pagedResources := allResources[start:end]

	// Generate next token - there are more results if we collected more than offset + limit
	var nextToken string
	hasMore := len(collector.objects) > offset+int(limit)
	if hasMore {
		nextToken = encodePaginationToken(&PaginationToken{Offset: end})
	}

	// Note: Total is not available when using streaming with early termination
	return &authzenv1.ResourceSearchResponse{
		Resources: pagedResources,
		Page: &authzenv1.PageResponse{
			NextToken: nextToken,
			Count:     uint32(len(pagedResources)),
		},
	}, nil
}

// objectCollector implements OpenFGAService_StreamedListObjectsServer to collect streamed objects.
type objectCollector struct {
	ctx      context.Context
	objects  []string
	maxCount int
	grpc.ServerStream
}

func (c *objectCollector) Context() context.Context {
	return c.ctx
}

func (c *objectCollector) Send(resp *openfgav1.StreamedListObjectsResponse) error {
	c.objects = append(c.objects, resp.GetObject())
	// Stop streaming once we have enough objects
	if len(c.objects) >= c.maxCount {
		return errLimitReached
	}
	return nil
}

func (c *objectCollector) SetHeader(metadata.MD) error {
	return nil
}

func (c *objectCollector) SendHeader(metadata.MD) error {
	return nil
}

func (c *objectCollector) SetTrailer(metadata.MD) {
}

func (c *objectCollector) SendMsg(m interface{}) error {
	return nil
}

func (c *objectCollector) RecvMsg(m interface{}) error {
	return nil
}

func objectIDToResource(objID string) *authzenv1.Resource {
	// Parse "type:id" format
	parts := strings.SplitN(objID, ":", 2)
	if len(parts) != 2 {
		return nil
	}
	return &authzenv1.Resource{
		Type: parts[0],
		Id:   parts[1],
	}
}
