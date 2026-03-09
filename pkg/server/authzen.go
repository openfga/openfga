package server

import (
	"context"
	"regexp"
	"strings"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"

	"github.com/openfga/openfga/pkg/middleware/validator"
	"github.com/openfga/openfga/pkg/server/commands"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	servererrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/typesystem"
)

// AuthZenAuthorizationModelIDHeader is the HTTP header name for specifying the authorization model ID.
const AuthZenAuthorizationModelIDHeader = "Openfga-Authorization-Model-Id"

var authZenAuthorizationModelIDPattern = regexp.MustCompile(`^[ABCDEFGHJKMNPQRSTVWXYZ0-9]{26}$`)

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
			authorizationModelID := strings.TrimSpace(values[0])
			if authZenAuthorizationModelIDPattern.MatchString(authorizationModelID) {
				return authorizationModelID
			}
		}
	}
	return ""
}

// errorContext builds an arbitrary JSON context object for error responses,
// per the AuthZen spec where context is a free-form JSON object.
func errorContext(status uint32, message string) *structpb.Struct {
	ctx, _ := structpb.NewStruct(map[string]interface{}{
		"error": map[string]interface{}{
			"status":  status,
			"message": message,
		},
	})
	return ctx
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

	// AuthZEN compatibility: if evaluations is omitted or empty,
	// behave like a single Access Evaluation request.
	if len(req.GetEvaluations()) == 0 {
		evalResp, err := s.Evaluation(ctx, &authzenv1.EvaluationRequest{
			Subject:  req.GetSubject(),
			Resource: req.GetResource(),
			Action:   req.GetAction(),
			Context:  req.GetContext(),
			StoreId:  req.GetStoreId(),
		})
		if err != nil {
			return nil, err
		}

		return &authzenv1.EvaluationsResponse{
			Evaluations: []*authzenv1.EvaluationResponse{evalResp},
		}, nil
	}

	// Get authorization model ID from header
	authorizationModelID := getAuthorizationModelIDFromHeader(ctx)

	// Check for short-circuit semantics
	semantic := authzenv1.EvaluationsSemantic_execute_all
	if req.GetOptions() != nil {
		semantic = req.GetOptions().GetEvaluationsSemantic()
		switch semantic {
		case authzenv1.EvaluationsSemantic_execute_all,
			authzenv1.EvaluationsSemantic_deny_on_first_deny,
			authzenv1.EvaluationsSemantic_permit_on_first_permit:
			// valid values
		default:
			return nil, status.Error(codes.InvalidArgument, "invalid evaluations_semantic: value must be one of the defined enum values")
		}
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
			// Extract gRPC status code and map to HTTP status
			httpStatus := uint32(500)
			if st, ok := status.FromError(err); ok {
				grpcCode := st.Code()
				// Check if it's a standard gRPC code (0-16) or OpenFGA custom code (>= 1000)
				if grpcCode < 17 {
					// Standard gRPC code - use grpc-gateway's mapping
					httpStatus = uint32(runtime.HTTPStatusFromCode(grpcCode))
				} else {
					// OpenFGA custom error code - use encoded error mapping
					encodedErr := servererrors.NewEncodedError(int32(grpcCode), st.Message())
					httpStatus = uint32(encodedErr.HTTPStatus())
				}
			}
			responses = append(responses, &authzenv1.EvaluationResponse{
				Decision: false,
				Context:  errorContext(httpStatus, err.Error()),
			})
			// Error results in Decision: false, honor deny_on_first_deny short-circuit
			if semantic == authzenv1.EvaluationsSemantic_deny_on_first_deny {
				break
			}
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
		Evaluations: responses,
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
