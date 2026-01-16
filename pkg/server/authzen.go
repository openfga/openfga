package server

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"

	"github.com/openfga/openfga/pkg/middleware/validator"
	"github.com/openfga/openfga/pkg/server/commands"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/typesystem"
)

// AuthZenAuthorizationModelIDHeader is the HTTP header name for specifying the authorization model ID.
const AuthZenAuthorizationModelIDHeader = "Openfga-Authorization-Model-Id"

// IsAuthZenEnabled returns true if the AuthZEN experimental feature is enabled for the given store.
func (s *Server) IsAuthZenEnabled(storeID string) bool {
	return s.featureFlagClient.Boolean(serverconfig.ExperimentalEnableAuthZen, storeID)
}

// getAuthorizationModelIDFromHeader extracts the authorization model ID from the gRPC metadata.
// Returns empty string if the header is not present.
func getAuthorizationModelIDFromHeader(ctx context.Context) string {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		// grpc-gateway converts header names to lowercase
		headerKey := strings.ToLower(AuthZenAuthorizationModelIDHeader)
		if values := md.Get(headerKey); len(values) > 0 && values[0] != "" {
			return values[0]
		}
	}
	return ""
}

func (s *Server) Evaluation(ctx context.Context, req *authzenv1.EvaluationRequest) (*authzenv1.EvaluationResponse, error) {
	// Gate behind experimental flag
	if !s.featureFlagClient.Boolean(serverconfig.ExperimentalEnableAuthZen, req.GetStoreId()) {
		return nil, status.Error(codes.Unimplemented, "AuthZEN endpoints are experimental. Enable with --experimentals=enable_authzen")
	}

	ctx, span := tracer.Start(ctx, "authzen.Evaluation")
	defer span.End()

	if !validator.RequestIsValidatedFromContext(ctx) {
		if err := req.Validate(); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}

	ctx = telemetry.ContextWithRPCInfo(ctx, telemetry.RPCInfo{
		Service: s.serviceName,
		Method:  "authzen.Evaluation",
	})

	// Get authorization model ID from header
	authorizationModelID := getAuthorizationModelIDFromHeader(ctx)

	evalReqCmd, err := commands.NewEvaluateRequestCommand(req, authorizationModelID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	checkResponse, err := s.Check(ctx, evalReqCmd.GetCheckRequest())
	if err != nil {
		return nil, err
	}

	return &authzenv1.EvaluationResponse{
		Decision: checkResponse.GetAllowed(),
	}, nil
}

func (s *Server) Evaluations(ctx context.Context, req *authzenv1.EvaluationsRequest) (*authzenv1.EvaluationsResponse, error) {
	// Gate behind experimental flag
	if !s.featureFlagClient.Boolean(serverconfig.ExperimentalEnableAuthZen, req.GetStoreId()) {
		return nil, status.Error(codes.Unimplemented, "AuthZEN endpoints are experimental. Enable with --experimentals=enable_authzen")
	}

	ctx, span := tracer.Start(ctx, "authzen.Evaluations")
	defer span.End()

	if !validator.RequestIsValidatedFromContext(ctx) {
		if err := req.Validate(); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}

	ctx = telemetry.ContextWithRPCInfo(ctx, telemetry.RPCInfo{
		Service: s.serviceName,
		Method:  "authzen.Evaluations",
	})

	// Get authorization model ID from header
	authorizationModelID := getAuthorizationModelIDFromHeader(ctx)

	// Check for short-circuit semantics
	semantic := authzenv1.EvaluationsSemantic_execute_all
	if req.GetOptions() != nil {
		semantic = req.GetOptions().GetEvaluationsSemantic()
	}

	if semantic == authzenv1.EvaluationsSemantic_deny_on_first_deny ||
		semantic == authzenv1.EvaluationsSemantic_permit_on_first_permit {
		return s.evaluateWithShortCircuit(ctx, req, semantic)
	}

	// Default: batch all evaluations
	evalReqCmd, err := commands.NewBatchEvaluateRequestCommand(req, authorizationModelID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	batchCheckResponse, err := s.BatchCheck(ctx, evalReqCmd.GetBatchCheckRequests())
	if err != nil {
		return nil, err
	}

	evaluationsResponse, err := commands.TransformResponse(batchCheckResponse)
	if err != nil {
		return nil, err
	}

	return evaluationsResponse, nil
}

// evaluateWithShortCircuit handles deny_on_first_deny and permit_on_first_permit semantics.
//
//nolint:unparam // error is always nil but kept for interface consistency
func (s *Server) evaluateWithShortCircuit(
	ctx context.Context,
	req *authzenv1.EvaluationsRequest,
	semantic authzenv1.EvaluationsSemantic,
) (*authzenv1.EvaluationsResponse, error) {
	responses := make([]*authzenv1.EvaluationResponse, 0, len(req.GetEvaluations()))

	// Get defaults from request
	defaultSubject := req.GetSubject()
	defaultResource := req.GetResource()
	defaultAction := req.GetAction()
	defaultContext := req.GetContext()

	for _, eval := range req.GetEvaluations() {
		// Resolve effective values (per-evaluation overrides defaults)
		subject := eval.GetSubject()
		if subject == nil {
			subject = defaultSubject
		}
		resource := eval.GetResource()
		if resource == nil {
			resource = defaultResource
		}
		action := eval.GetAction()
		if action == nil {
			action = defaultAction
		}
		evalContext := eval.GetContext()
		if evalContext == nil {
			evalContext = defaultContext
		}

		// Build single evaluation request
		singleReq := &authzenv1.EvaluationRequest{
			Subject:  subject,
			Resource: resource,
			Action:   action,
			Context:  evalContext,
			StoreId:  req.GetStoreId(),
		}

		// Use the Evaluation method
		evalResp, err := s.Evaluation(ctx, singleReq)
		if err != nil {
			responses = append(responses, &authzenv1.EvaluationResponse{
				Decision: false,
				Context: &authzenv1.EvaluationResponseContext{
					Error: &authzenv1.ResponseContextError{
						Status:  500,
						Message: err.Error(),
					},
				},
			})
			continue
		}

		responses = append(responses, evalResp)

		// Short-circuit logic
		if semantic == authzenv1.EvaluationsSemantic_deny_on_first_deny && !evalResp.GetDecision() {
			// Stop on first deny
			break
		}
		if semantic == authzenv1.EvaluationsSemantic_permit_on_first_permit && evalResp.GetDecision() {
			// Stop on first permit
			break
		}
	}

	return &authzenv1.EvaluationsResponse{
		EvaluationResponses: responses,
	}, nil
}

// SubjectSearch returns subjects that have access to the specified resource.
func (s *Server) SubjectSearch(ctx context.Context, req *authzenv1.SubjectSearchRequest) (*authzenv1.SubjectSearchResponse, error) {
	// Gate behind experimental flag
	if !s.featureFlagClient.Boolean(serverconfig.ExperimentalEnableAuthZen, req.GetStoreId()) {
		return nil, status.Error(codes.Unimplemented, "AuthZEN endpoints are experimental. Enable with --experimentals=enable_authzen")
	}

	ctx, span := tracer.Start(ctx, "authzen.SubjectSearch")
	defer span.End()

	if !validator.RequestIsValidatedFromContext(ctx) {
		if err := req.Validate(); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}

	ctx = telemetry.ContextWithRPCInfo(ctx, telemetry.RPCInfo{
		Service: s.serviceName,
		Method:  "authzen.SubjectSearch",
	})

	// Get authorization model ID from header
	authorizationModelID := getAuthorizationModelIDFromHeader(ctx)

	query := commands.NewSubjectSearchQuery(
		commands.WithAuthorizationModelID(authorizationModelID),
		commands.WithListUsersFunc(s.ListUsers),
	)

	return query.Execute(ctx, req)
}

// ResourceSearch returns resources that a subject has access to.
func (s *Server) ResourceSearch(ctx context.Context, req *authzenv1.ResourceSearchRequest) (*authzenv1.ResourceSearchResponse, error) {
	// Gate behind experimental flag
	if !s.featureFlagClient.Boolean(serverconfig.ExperimentalEnableAuthZen, req.GetStoreId()) {
		return nil, status.Error(codes.Unimplemented, "AuthZEN endpoints are experimental. Enable with --experimentals=enable_authzen")
	}

	ctx, span := tracer.Start(ctx, "authzen.ResourceSearch")
	defer span.End()

	if !validator.RequestIsValidatedFromContext(ctx) {
		if err := req.Validate(); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}

	ctx = telemetry.ContextWithRPCInfo(ctx, telemetry.RPCInfo{
		Service: s.serviceName,
		Method:  "authzen.ResourceSearch",
	})

	// Get authorization model ID from header
	authorizationModelID := getAuthorizationModelIDFromHeader(ctx)

	query := commands.NewResourceSearchQuery(
		commands.WithResourceSearchAuthorizationModelID(authorizationModelID),
		commands.WithStreamedListObjectsFunc(s.StreamedListObjects),
	)

	return query.Execute(ctx, req)
}

// ActionSearch returns actions a subject can perform on a resource.
func (s *Server) ActionSearch(ctx context.Context, req *authzenv1.ActionSearchRequest) (*authzenv1.ActionSearchResponse, error) {
	// Gate behind experimental flag
	if !s.featureFlagClient.Boolean(serverconfig.ExperimentalEnableAuthZen, req.GetStoreId()) {
		return nil, status.Error(codes.Unimplemented, "AuthZEN endpoints are experimental. Enable with --experimentals=enable_authzen")
	}

	ctx, span := tracer.Start(ctx, "authzen.ActionSearch")
	defer span.End()

	if !validator.RequestIsValidatedFromContext(ctx) {
		if err := req.Validate(); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}

	ctx = telemetry.ContextWithRPCInfo(ctx, telemetry.RPCInfo{
		Service: s.serviceName,
		Method:  "authzen.ActionSearch",
	})

	// Get authorization model ID from header
	authorizationModelID := getAuthorizationModelIDFromHeader(ctx)

	// Resolve typesystem once to set the header and get the model ID
	typesys, err := s.resolveTypesystem(ctx, req.GetStoreId(), authorizationModelID)
	if err != nil {
		return nil, err
	}

	// Get the resolved model ID
	resolvedModelID := typesys.GetAuthorizationModelID()

	// Use a typesystem resolver that returns the already-resolved typesystem
	// to avoid re-resolving it in the action search query
	cachedTypesystemResolver := func(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
		return typesys, nil
	}

	query := commands.NewActionSearchQuery(
		commands.WithTypesystemResolver(cachedTypesystemResolver),
		commands.WithBatchCheckFunc(s.BatchCheck),
		commands.WithActionSearchAuthorizationModelID(resolvedModelID),
	)

	return query.Execute(ctx, req)
}
