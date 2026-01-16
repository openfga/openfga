package commands

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

// StreamedListObjectsFunc is a function type for StreamedListObjects.
type StreamedListObjectsFunc func(req *openfgav1.StreamedListObjectsRequest, srv openfgav1.OpenFGAService_StreamedListObjectsServer) error

// ResourceSearchQuery handles AuthZEN resource search requests.
type ResourceSearchQuery struct {
	streamedListObjectsFunc StreamedListObjectsFunc
	authorizationModelID    string
}

// ResourceSearchQueryOption is a functional option for ResourceSearchQuery.
type ResourceSearchQueryOption func(*ResourceSearchQuery)

// WithStreamedListObjectsFunc sets the StreamedListObjects function to use.
func WithStreamedListObjectsFunc(fn StreamedListObjectsFunc) ResourceSearchQueryOption {
	return func(q *ResourceSearchQuery) {
		q.streamedListObjectsFunc = fn
	}
}

// WithResourceSearchAuthorizationModelID sets the authorization model ID to use.
func WithResourceSearchAuthorizationModelID(id string) ResourceSearchQueryOption {
	return func(q *ResourceSearchQuery) {
		q.authorizationModelID = id
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

// Execute runs the resource search and returns matching resources.
// Note: Pagination is not currently supported per AuthZEN spec (optional feature).
// The page request parameter is ignored and all results are returned.
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
	if req.GetResource() == nil || req.GetResource().GetType() == "" {
		return nil, fmt.Errorf("resource type is required for resource search")
	}
	resourceType := req.GetResource().GetType()

	// Build StreamedListObjects request
	streamedReq := &openfgav1.StreamedListObjectsRequest{
		StoreId:              req.GetStoreId(),
		AuthorizationModelId: q.authorizationModelID,
		User:                 fmt.Sprintf("%s:%s", req.GetSubject().GetType(), req.GetSubject().GetId()),
		Relation:             req.GetAction().GetName(),
		Type:                 resourceType,
		Context:              mergedContext,
	}

	// Create a collector that consumes the full stream
	collector := &objectCollector{
		ctx:     ctx,
		objects: make([]string, 0),
	}

	// Execute StreamedListObjects - consume full stream
	err = q.streamedListObjectsFunc(streamedReq, collector)
	if err != nil {
		return nil, fmt.Errorf("StreamedListObjects failed: %w", err)
	}

	// Convert to AuthZEN resources
	var resources []*authzenv1.Resource
	for _, objID := range collector.objects {
		resource := objectIDToResource(objID)
		if resource != nil {
			resources = append(resources, resource)
		}
	}

	// Return all results without pagination (pagination not supported)
	return &authzenv1.ResourceSearchResponse{
		Resources: resources,
	}, nil
}

// objectCollector implements OpenFGAService_StreamedListObjectsServer to collect streamed objects.
type objectCollector struct {
	ctx     context.Context
	objects []string
	grpc.ServerStream
}

func (c *objectCollector) Context() context.Context {
	return c.ctx
}

func (c *objectCollector) Send(resp *openfgav1.StreamedListObjectsResponse) error {
	c.objects = append(c.objects, resp.GetObject())
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
