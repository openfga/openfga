package server

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
)

// GetConfiguration returns PDP metadata and capabilities per AuthZEN spec section 13.
// Following the AuthZEN spec's multi-tenant pattern, this endpoint is scoped to a specific
// store and returns absolute endpoint URLs for that store.
func (s *Server) GetConfiguration(ctx context.Context, req *authzenv1.GetConfigurationRequest) (*authzenv1.GetConfigurationResponse, error) {
	_, end, err := s.prepareAuthZenRequest(ctx, "authzen.GetConfiguration", req)
	if err != nil {
		return nil, err
	}
	defer end()

	storeID := req.GetStoreId()

	if s.authzenBaseURL == "" {
		return nil, status.Error(codes.FailedPrecondition, "AuthZEN discovery base URL is not configured")
	}

	storeBase := fmt.Sprintf("%s/stores/%s", s.authzenBaseURL, storeID)

	return &authzenv1.GetConfigurationResponse{
		PolicyDecisionPoint:       storeBase,
		AccessEvaluationEndpoint:  storeBase + "/access/v1/evaluation",
		AccessEvaluationsEndpoint: storeBase + "/access/v1/evaluations",
		SearchSubjectEndpoint:     storeBase + "/access/v1/search/subject",
		SearchResourceEndpoint:    storeBase + "/access/v1/search/resource",
		SearchActionEndpoint:      storeBase + "/access/v1/search/action",
	}, nil
}
