package server

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"

	"github.com/openfga/openfga/internal/build"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
)

// getBaseURLFromContext extracts the base URL from gRPC metadata.
// It uses x-forwarded-proto and x-forwarded-host headers if available,
// otherwise falls back to :authority header with https scheme.
func getBaseURLFromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}

	// Try to get scheme from x-forwarded-proto
	scheme := "https"
	if values := md.Get("x-forwarded-proto"); len(values) > 0 && values[0] != "" {
		scheme = values[0]
	}

	// Try to get host from x-forwarded-host, then :authority
	var host string
	if values := md.Get("x-forwarded-host"); len(values) > 0 && values[0] != "" {
		host = values[0]
	} else if values := md.Get(":authority"); len(values) > 0 && values[0] != "" {
		host = values[0]
	}

	if host == "" {
		return ""
	}

	// Remove any trailing slashes
	host = strings.TrimSuffix(host, "/")

	return fmt.Sprintf("%s://%s", scheme, host)
}

// GetConfiguration returns PDP metadata and capabilities per AuthZEN spec section 13.
// Following the AuthZEN spec's multi-tenant pattern, this endpoint is scoped to a specific
// store and returns absolute endpoint URLs for that store.
func (s *Server) GetConfiguration(ctx context.Context, req *authzenv1.GetConfigurationRequest) (*authzenv1.GetConfigurationResponse, error) {
	storeID := req.GetStoreId()

	// Gate behind experimental flag
	if !s.featureFlagClient.Boolean(serverconfig.ExperimentalEnableAuthZen, storeID) {
		return nil, status.Error(codes.Unimplemented, "AuthZEN endpoints are experimental. Enable with --experimentals=enable_authzen")
	}

	_, span := tracer.Start(ctx, "authzen.GetConfiguration")
	defer span.End()

	// Get base URL from request context for absolute URLs per AuthZEN spec
	baseURL := getBaseURLFromContext(ctx)

	return &authzenv1.GetConfigurationResponse{
		PolicyDecisionPoint: &authzenv1.PolicyDecisionPoint{
			Name:        "OpenFGA",
			Version:     build.Version,
			Description: "OpenFGA is a high-performance and flexible authorization system that supports Fine-Grained Authorization (FGA) and implements the AuthZEN specification.",
		},
		AccessEndpoints: &authzenv1.Endpoints{
			Evaluation:     fmt.Sprintf("%s/stores/%s/access/v1/evaluation", baseURL, storeID),
			Evaluations:    fmt.Sprintf("%s/stores/%s/access/v1/evaluations", baseURL, storeID),
			SubjectSearch:  fmt.Sprintf("%s/stores/%s/access/v1/search/subject", baseURL, storeID),
			ResourceSearch: fmt.Sprintf("%s/stores/%s/access/v1/search/resource", baseURL, storeID),
			ActionSearch:   fmt.Sprintf("%s/stores/%s/access/v1/search/action", baseURL, storeID),
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
