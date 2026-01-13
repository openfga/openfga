package server

import (
	"context"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	"github.com/openfga/openfga/internal/build"
)

// GetConfiguration returns PDP metadata and capabilities per AuthZEN spec section 13.
// This endpoint is NOT gated behind the experimental flag as it's needed for discovery.
func (s Server) GetConfiguration(ctx context.Context, req *authzenv1.GetConfigurationRequest) (*authzenv1.GetConfigurationResponse, error) {
	ctx, span := tracer.Start(ctx, "authzen.GetConfiguration")
	defer span.End()

	return &authzenv1.GetConfigurationResponse{
		PolicyDecisionPoint: &authzenv1.PolicyDecisionPoint{
			Name:        "OpenFGA",
			Version:     build.Version,
			Description: "OpenFGA is a high-performance and flexible authorization system that supports Fine-Grained Authorization (FGA) and implements the AuthZEN specification.",
		},
		AccessEndpoints: &authzenv1.Endpoints{
			Evaluation:     "/stores/{store_id}/access/v1/evaluation",
			Evaluations:    "/stores/{store_id}/access/v1/evaluations",
			SubjectSearch:  "/stores/{store_id}/access/v1/search/subject",
			ResourceSearch: "/stores/{store_id}/access/v1/search/resource",
			ActionSearch:   "/stores/{store_id}/access/v1/search/action",
		},
		Capabilities: []string{
			"evaluation",
			"evaluations",
			"subject_search",
			"resource_search",
			"action_search",
		},
	}, nil
}
