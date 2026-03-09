package server

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/telemetry"
	"github.com/openfga/openfga/pkg/middleware/validator"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	servererrors "github.com/openfga/openfga/pkg/server/errors"
)

// AuthZenAuthorizationModelIDHeader is the HTTP header name for specifying the authorization model ID.
const AuthZenAuthorizationModelIDHeader = "Openfga-Authorization-Model-Id"

// propertiesProvider is an interface for types that provide properties.
// Subject, SubjectFilter, Resource, and ResourceFilter all implement this.
type propertiesProvider interface {
	GetProperties() *structpb.Struct
}

// mergePropertiesToContext merges subject, resource, and action properties into
// the context struct. Properties are namespaced with their source prefix using
// underscore as separator (e.g., "subject_department") because OpenFGA does not
// allow condition parameters with "." in their names.
// Precedence (lowest to highest): subject.properties, resource.properties,
// action.properties, request context (request context wins on conflicts).
func mergePropertiesToContext(
	requestContext *structpb.Struct,
	subject propertiesProvider,
	resource propertiesProvider,
	action *authzenv1.Action,
) (*structpb.Struct, error) {
	merged := make(map[string]any)

	if subject != nil && subject.GetProperties() != nil {
		for k, v := range subject.GetProperties().AsMap() {
			merged["subject_"+k] = v
		}
	}

	if resource != nil && resource.GetProperties() != nil {
		for k, v := range resource.GetProperties().AsMap() {
			merged["resource_"+k] = v
		}
	}

	if action != nil && action.GetProperties() != nil {
		for k, v := range action.GetProperties().AsMap() {
			merged["action_"+k] = v
		}
	}

	if requestContext != nil {
		for k, v := range requestContext.AsMap() {
			merged[k] = v
		}
	}

	if len(merged) == 0 {
		return nil, nil
	}

	return structpb.NewStruct(merged)
}

var authZenAuthorizationModelIDPattern = regexp.MustCompile(`^[ABCDEFGHJKMNPQRSTVWXYZ0-9]{26}$`)

// IsAuthZenEnabled returns true if the AuthZEN experimental feature is enabled for the given store.
func (s *Server) IsAuthZenEnabled(storeID string) bool {
	return s.featureFlagClient.Boolean(serverconfig.ExperimentalAuthZen, storeID)
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
func errorContext(httpStatus uint32, message string) *structpb.Struct {
	ctx, _ := structpb.NewStruct(map[string]any{
		"error": map[string]any{
			"status":  httpStatus,
			"message": message,
		},
	})
	return ctx
}

// buildCheckRequest translates AuthZen evaluation fields into an OpenFGA CheckRequest.
func buildCheckRequest(
	storeID, authorizationModelID string,
	subject *authzenv1.Subject,
	resource *authzenv1.Resource,
	action *authzenv1.Action,
	reqContext *structpb.Struct,
) (*openfgav1.CheckRequest, error) {
	if subject == nil {
		return nil, fmt.Errorf("missing subject")
	}
	if resource == nil {
		return nil, fmt.Errorf("missing resource")
	}
	if action == nil {
		return nil, fmt.Errorf("missing action")
	}

	mergedContext, err := mergePropertiesToContext(reqContext, subject, resource, action)
	if err != nil {
		return nil, err
	}

	return &openfgav1.CheckRequest{
		StoreId:              storeID,
		AuthorizationModelId: authorizationModelID,
		TupleKey: &openfgav1.CheckRequestTupleKey{
			User:     fmt.Sprintf("%s:%s", subject.GetType(), subject.GetId()),
			Relation: action.GetName(),
			Object:   fmt.Sprintf("%s:%s", resource.GetType(), resource.GetId()),
		},
		Context: mergedContext,
	}, nil
}

// grpcErrorToHTTPStatus maps a gRPC error to an HTTP status code.
func grpcErrorToHTTPStatus(err error) uint32 {
	httpStatus := uint32(500)
	if st, ok := status.FromError(err); ok {
		grpcCode := st.Code()
		if grpcCode < 17 {
			httpStatus = uint32(runtime.HTTPStatusFromCode(grpcCode))
		} else {
			encodedErr := servererrors.NewEncodedError(int32(grpcCode), st.Message())
			httpStatus = uint32(encodedErr.HTTPStatus())
		}
	}
	return httpStatus
}

func (s *Server) Evaluation(ctx context.Context, req *authzenv1.EvaluationRequest) (*authzenv1.EvaluationResponse, error) {
	if !s.featureFlagClient.Boolean(serverconfig.ExperimentalAuthZen, req.GetStoreId()) {
		return nil, status.Error(codes.Unimplemented, "AuthZEN endpoints are experimental. Enable with --experimentals=authzen")
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

	authorizationModelID := getAuthorizationModelIDFromHeader(ctx)

	checkReq, err := buildCheckRequest(
		req.GetStoreId(), authorizationModelID,
		req.GetSubject(), req.GetResource(), req.GetAction(), req.GetContext(),
	)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	checkResponse, err := s.Check(ctx, checkReq)
	if err != nil {
		return nil, err
	}

	return &authzenv1.EvaluationResponse{
		Decision: checkResponse.GetAllowed(),
	}, nil
}

func (s *Server) Evaluations(ctx context.Context, req *authzenv1.EvaluationsRequest) (*authzenv1.EvaluationsResponse, error) {
	if !s.featureFlagClient.Boolean(serverconfig.ExperimentalAuthZen, req.GetStoreId()) {
		return nil, status.Error(codes.Unimplemented, "AuthZEN endpoints are experimental. Enable with --experimentals=authzen")
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

	// If evaluations is omitted or empty, behave like a single Evaluation.
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

	authorizationModelID := getAuthorizationModelIDFromHeader(ctx)

	semantic := authzenv1.EvaluationsSemantic_execute_all
	if req.GetOptions() != nil {
		semantic = req.GetOptions().GetEvaluationsSemantic()
		switch semantic {
		case authzenv1.EvaluationsSemantic_execute_all,
			authzenv1.EvaluationsSemantic_deny_on_first_deny,
			authzenv1.EvaluationsSemantic_permit_on_first_permit:
			// valid
		default:
			return nil, status.Error(codes.InvalidArgument, "invalid evaluations_semantic: value must be one of the defined enum values")
		}
	}

	if semantic == authzenv1.EvaluationsSemantic_deny_on_first_deny ||
		semantic == authzenv1.EvaluationsSemantic_permit_on_first_permit {
		return s.evaluateWithShortCircuit(ctx, req, authorizationModelID, semantic)
	}

	return s.evaluateAll(ctx, req, authorizationModelID)
}

// evaluateAll uses BatchCheck to evaluate all items in parallel.
func (s *Server) evaluateAll(
	ctx context.Context,
	req *authzenv1.EvaluationsRequest,
	authorizationModelID string,
) (*authzenv1.EvaluationsResponse, error) {
	topSubject := req.GetSubject()
	topResource := req.GetResource()
	topAction := req.GetAction()
	topContext := req.GetContext()

	batchReq := &openfgav1.BatchCheckRequest{
		StoreId:              req.GetStoreId(),
		AuthorizationModelId: authorizationModelID,
		Checks:               make([]*openfgav1.BatchCheckItem, 0, len(req.GetEvaluations())),
	}

	for i, eval := range req.GetEvaluations() {
		subject := eval.GetSubject()
		if subject == nil {
			subject = topSubject
		}
		resource := eval.GetResource()
		if resource == nil {
			resource = topResource
		}
		action := eval.GetAction()
		if action == nil {
			action = topAction
		}
		evalContext := eval.GetContext()
		if evalContext == nil {
			evalContext = topContext
		}

		item := &openfgav1.BatchCheckItem{
			TupleKey:      &openfgav1.CheckRequestTupleKey{},
			CorrelationId: strconv.Itoa(i),
		}
		if action != nil {
			item.TupleKey.Relation = action.GetName()
		}
		if resource != nil {
			item.TupleKey.Object = fmt.Sprintf("%s:%s", resource.GetType(), resource.GetId())
		}
		if subject != nil {
			item.TupleKey.User = fmt.Sprintf("%s:%s", subject.GetType(), subject.GetId())
		}

		mergedContext, err := mergePropertiesToContext(evalContext, subject, resource, action)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "failed to merge properties for evaluation %d: %v", i, err)
		}
		item.Context = mergedContext

		batchReq.Checks = append(batchReq.Checks, item)
	}

	batchResp, err := s.BatchCheck(ctx, batchReq)
	if err != nil {
		return nil, err
	}

	// Map results back to ordered response
	responses := make([]*authzenv1.EvaluationResponse, len(req.GetEvaluations()))
	for i := range responses {
		key := strconv.Itoa(i)
		result, ok := batchResp.GetResult()[key]
		if !ok || result == nil {
			responses[i] = &authzenv1.EvaluationResponse{
				Decision: false,
				Context:  errorContext(500, fmt.Sprintf("missing result for evaluation %d", i)),
			}
			continue
		}

		if errResult, ok := result.GetCheckResult().(*openfgav1.BatchCheckSingleResult_Error); ok {
			httpStatus := uint32(500)
			errMessage := "internal error"
			if errResult.Error != nil {
				errMessage = errResult.Error.GetMessage()
				switch code := errResult.Error.GetCode().(type) {
				case *openfgav1.CheckError_InputError:
					encodedErr := servererrors.NewEncodedError(int32(code.InputError), errMessage)
					httpStatus = uint32(encodedErr.HTTPStatus())
				case *openfgav1.CheckError_InternalError:
					httpStatus = 500
				}
			}
			responses[i] = &authzenv1.EvaluationResponse{
				Decision: false,
				Context:  errorContext(httpStatus, errMessage),
			}
		} else {
			responses[i] = &authzenv1.EvaluationResponse{
				Decision: result.GetAllowed(),
			}
		}
	}

	return &authzenv1.EvaluationsResponse{Evaluations: responses}, nil
}

// evaluateWithShortCircuit handles deny_on_first_deny and permit_on_first_permit semantics.
// It builds CheckRequests directly so the authorizationModelID is resolved once and reused.
//
//nolint:unparam
func (s *Server) evaluateWithShortCircuit(
	ctx context.Context,
	req *authzenv1.EvaluationsRequest,
	authorizationModelID string,
	semantic authzenv1.EvaluationsSemantic,
) (*authzenv1.EvaluationsResponse, error) {
	responses := make([]*authzenv1.EvaluationResponse, 0, len(req.GetEvaluations()))

	topSubject := req.GetSubject()
	topResource := req.GetResource()
	topAction := req.GetAction()
	topContext := req.GetContext()

	for _, eval := range req.GetEvaluations() {
		subject := eval.GetSubject()
		if subject == nil {
			subject = topSubject
		}
		resource := eval.GetResource()
		if resource == nil {
			resource = topResource
		}
		action := eval.GetAction()
		if action == nil {
			action = topAction
		}
		evalContext := eval.GetContext()
		if evalContext == nil {
			evalContext = topContext
		}

		checkReq, err := buildCheckRequest(
			req.GetStoreId(), authorizationModelID,
			subject, resource, action, evalContext,
		)
		if err != nil {
			responses = append(responses, &authzenv1.EvaluationResponse{
				Decision: false,
				Context:  errorContext(uint32(runtime.HTTPStatusFromCode(codes.InvalidArgument)), err.Error()),
			})
			if semantic == authzenv1.EvaluationsSemantic_deny_on_first_deny {
				break
			}
			continue
		}

		checkResponse, err := s.Check(ctx, checkReq)
		if err != nil {
			responses = append(responses, &authzenv1.EvaluationResponse{
				Decision: false,
				Context:  errorContext(grpcErrorToHTTPStatus(err), err.Error()),
			})
			if semantic == authzenv1.EvaluationsSemantic_deny_on_first_deny {
				break
			}
			continue
		}

		decision := checkResponse.GetAllowed()
		responses = append(responses, &authzenv1.EvaluationResponse{Decision: decision})

		if semantic == authzenv1.EvaluationsSemantic_deny_on_first_deny && !decision {
			break
		}
		if semantic == authzenv1.EvaluationsSemantic_permit_on_first_permit && decision {
			break
		}
	}

	return &authzenv1.EvaluationsResponse{Evaluations: responses}, nil
}

// SubjectSearch returns subjects that have access to the specified resource.
func (s *Server) SubjectSearch(ctx context.Context, req *authzenv1.SubjectSearchRequest) (*authzenv1.SubjectSearchResponse, error) {
	if !s.featureFlagClient.Boolean(serverconfig.ExperimentalAuthZen, req.GetStoreId()) {
		return nil, status.Error(codes.Unimplemented, "AuthZEN endpoints are experimental. Enable with --experimentals=authzen")
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

	authorizationModelID := getAuthorizationModelIDFromHeader(ctx)

	mergedContext, err := mergePropertiesToContext(
		req.GetContext(), req.GetSubject(), req.GetResource(), req.GetAction(),
	)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to merge properties: %v", err)
	}

	listUsersResp, err := s.ListUsers(ctx, &openfgav1.ListUsersRequest{
		StoreId:              req.GetStoreId(),
		AuthorizationModelId: authorizationModelID,
		Object: &openfgav1.Object{
			Type: req.GetResource().GetType(),
			Id:   req.GetResource().GetId(),
		},
		Relation: req.GetAction().GetName(),
		Context:  mergedContext,
		UserFilters: []*openfgav1.UserTypeFilter{
			{Type: req.GetSubject().GetType()},
		},
	})
	if err != nil {
		return nil, err
	}

	var subjects []*authzenv1.Subject
	for _, user := range listUsersResp.GetUsers() {
		if obj := user.GetObject(); obj != nil {
			subjects = append(subjects, &authzenv1.Subject{Type: obj.GetType(), Id: obj.GetId()})
		} else if w := user.GetWildcard(); w != nil {
			subjects = append(subjects, &authzenv1.Subject{Type: w.GetType(), Id: "*"})
		}
	}

	return &authzenv1.SubjectSearchResponse{Results: subjects}, nil
}

// ResourceSearch returns resources that a subject has access to.
func (s *Server) ResourceSearch(ctx context.Context, req *authzenv1.ResourceSearchRequest) (*authzenv1.ResourceSearchResponse, error) {
	if !s.featureFlagClient.Boolean(serverconfig.ExperimentalAuthZen, req.GetStoreId()) {
		return nil, status.Error(codes.Unimplemented, "AuthZEN endpoints are experimental. Enable with --experimentals=authzen")
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

	authorizationModelID := getAuthorizationModelIDFromHeader(ctx)

	mergedContext, err := mergePropertiesToContext(
		req.GetContext(), req.GetSubject(), req.GetResource(), req.GetAction(),
	)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to merge properties: %v", err)
	}

	collector := &objectCollector{ctx: ctx}

	err = s.StreamedListObjects(&openfgav1.StreamedListObjectsRequest{
		StoreId:              req.GetStoreId(),
		AuthorizationModelId: authorizationModelID,
		User:                 fmt.Sprintf("%s:%s", req.GetSubject().GetType(), req.GetSubject().GetId()),
		Relation:             req.GetAction().GetName(),
		Type:                 req.GetResource().GetType(),
		Context:              mergedContext,
	}, collector)
	if err != nil {
		return nil, err
	}

	var resources []*authzenv1.Resource
	for _, objID := range collector.objects {
		parts := strings.SplitN(objID, ":", 2)
		if len(parts) == 2 {
			resources = append(resources, &authzenv1.Resource{Type: parts[0], Id: parts[1]})
		}
	}

	return &authzenv1.ResourceSearchResponse{Results: resources}, nil
}

// objectCollector implements OpenFGAService_StreamedListObjectsServer to collect streamed objects.
type objectCollector struct {
	ctx     context.Context
	objects []string
	grpc.ServerStream
}

func (c *objectCollector) Context() context.Context     { return c.ctx }
func (c *objectCollector) SetHeader(metadata.MD) error  { return nil }
func (c *objectCollector) SendHeader(metadata.MD) error { return nil }
func (c *objectCollector) SetTrailer(metadata.MD)       {}
func (c *objectCollector) SendMsg(any) error            { return nil }
func (c *objectCollector) RecvMsg(any) error            { return nil }
func (c *objectCollector) Send(resp *openfgav1.StreamedListObjectsResponse) error {
	c.objects = append(c.objects, resp.GetObject())
	return nil
}

// ActionSearch returns actions a subject can perform on a resource.
func (s *Server) ActionSearch(ctx context.Context, req *authzenv1.ActionSearchRequest) (*authzenv1.ActionSearchResponse, error) {
	if !s.featureFlagClient.Boolean(serverconfig.ExperimentalAuthZen, req.GetStoreId()) {
		return nil, status.Error(codes.Unimplemented, "AuthZEN endpoints are experimental. Enable with --experimentals=authzen")
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

	authorizationModelID := getAuthorizationModelIDFromHeader(ctx)

	// Resolve typesystem to get all relations for the resource type
	typesys, err := s.resolveTypesystem(ctx, req.GetStoreId(), authorizationModelID)
	if err != nil {
		return nil, err
	}
	resolvedModelID := typesys.GetAuthorizationModelID()

	relations, err := typesys.GetRelations(req.GetResource().GetType())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to get relations for type %s: %v", req.GetResource().GetType(), err)
	}

	mergedContext, err := mergePropertiesToContext(
		req.GetContext(), req.GetSubject(), req.GetResource(), nil,
	)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to merge properties: %v", err)
	}

	user := fmt.Sprintf("%s:%s", req.GetSubject().GetType(), req.GetSubject().GetId())
	object := fmt.Sprintf("%s:%s", req.GetResource().GetType(), req.GetResource().GetId())

	relationNames := make([]string, 0, len(relations))
	for name := range relations {
		relationNames = append(relationNames, name)
	}

	checks := make([]*openfgav1.BatchCheckItem, 0, len(relationNames))
	for i, rel := range relationNames {
		checks = append(checks, &openfgav1.BatchCheckItem{
			TupleKey: &openfgav1.CheckRequestTupleKey{
				User:     user,
				Relation: rel,
				Object:   object,
			},
			Context:       mergedContext,
			CorrelationId: strconv.Itoa(i),
		})
	}

	batchResp, err := s.BatchCheck(ctx, &openfgav1.BatchCheckRequest{
		StoreId:              req.GetStoreId(),
		AuthorizationModelId: resolvedModelID,
		Checks:               checks,
	})
	if err != nil {
		return nil, err
	}

	var actions []*authzenv1.Action
	for correlationID, result := range batchResp.GetResult() {
		if result.GetError() != nil {
			continue
		}
		if result.GetAllowed() {
			idx, err := strconv.Atoi(correlationID)
			if err != nil || idx < 0 || idx >= len(relationNames) {
				continue
			}
			actions = append(actions, &authzenv1.Action{Name: relationNames[idx]})
		}
	}

	sort.Slice(actions, func(i, j int) bool {
		return actions[i].GetName() < actions[j].GetName()
	})

	return &authzenv1.ActionSearchResponse{Results: actions}, nil
}
