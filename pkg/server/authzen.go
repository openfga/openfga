package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"

	"github.com/openfga/openfga/pkg/middleware/validator"
	"github.com/openfga/openfga/pkg/server/commands"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/typesystem"
)

// IsAuthZenEnabled returns true if the AuthZEN experimental feature is enabled for the given store.
func (s *Server) IsAuthZenEnabled(storeID string) bool {
	return s.featureFlagClient.Boolean(serverconfig.ExperimentalEnableAuthZen, storeID)
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

	evalReqCmd, err := commands.NewEvaluateRequestCommand(req)
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
	evalReqCmd, err := commands.NewBatchEvaluateRequestCommand(req)
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
			Subject:              subject,
			Resource:             resource,
			Action:               action,
			Context:              evalContext,
			StoreId:              req.GetStoreId(),
			AuthorizationModelId: "", // Use latest if not specified
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

	query := commands.NewSubjectSearchQuery(
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

	query := commands.NewResourceSearchQuery(
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

	// Resolve typesystem once to set the header and get the model ID
	typesys, err := s.resolveTypesystem(ctx, req.GetStoreId(), req.GetAuthorizationModelId())
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

	// Set the resolved model ID on the request
	req.AuthorizationModelId = resolvedModelID

	query := commands.NewActionSearchQuery(
		commands.WithTypesystemResolver(cachedTypesystemResolver),
		commands.WithBatchCheckFunc(s.BatchCheck),
	)

	return query.Execute(ctx, req)
}
