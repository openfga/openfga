package authz

import (
	"context"
	"errors"
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/openfga/openfga/pkg/authclaims"
	"github.com/openfga/openfga/pkg/logger"
)

const (
	// API methods.
	ReadAuthorizationModel  = "ReadAuthorizationModel"
	ReadAuthorizationModels = "ReadAuthorizationModels"
	Read                    = "Read"
	Write                   = "Write"
	ListObjects             = "ListObjects"
	StreamedListObjects     = "StreamedListObjects"
	Check                   = "Check"
	ListUsers               = "ListUsers"
	WriteAssertions         = "WriteAssertions"
	ReadAssertions          = "ReadAssertions"
	WriteAuthorizationModel = "WriteAuthorizationModel"
	CreateStore             = "CreateStore"
	GetStore                = "GetStore"
	DeleteStore             = "DeleteStore"
	Expand                  = "Expand"
	ReadChanges             = "ReadChanges"

	// Relations.
	CanCallReadAuthorizationModels  = "can_call_read_authorization_models"
	CanCallRead                     = "can_call_read"
	CanCallWrite                    = "can_call_write"
	CanCallListObjects              = "can_call_list_objects"
	CanCallCheck                    = "can_call_check"
	CanCallListUsers                = "can_call_list_users"
	CanCallWriteAssertions          = "can_call_write_assertions"
	CanCallReadAssertions           = "can_call_read_assertions"
	CanCallWriteAuthorizationModels = "can_call_write_authorization_models"
	CanCallCreateStore              = "can_call_create_stores"
	CanCallGetStore                 = "can_call_get_store"
	CanCallDeleteStore              = "can_call_delete_store"
	CanCallExpand                   = "can_call_expand"
	CanCallReadChanges              = "can_call_read_changes"

	StoreType       = "store"
	ApplicationType = "application"
	SystemType      = "system"
	RootSystemID    = "fga"
)

var (
	ErrUnauthorizedResponse = &openfgav1.ForbiddenResponse{Code: 403, Message: "the principal is not authorized to perform the action"}
	ErrUnknownAPIMethod     = errors.New("unknown API method")

	System = fmt.Sprintf("%s:%s", SystemType, RootSystemID)
)

type StoreIDType string

func (s StoreIDType) String() string {
	return fmt.Sprintf("%s:%s", StoreType, string(s))
}

type ClientIDType string

func (c ClientIDType) String() string {
	return fmt.Sprintf("%s:%s", ApplicationType, string(c))
}

type Config struct {
	StoreID string
	ModelID string
}

type AuthorizerInterface interface {
	Authorize(ctx context.Context, storeID, apiMethod string) error
	AuthorizeCreateStore(ctx context.Context) error
}

type NoopAuthorizer struct {
	config *Config
	server ServerInterface
	logger logger.Logger
}

func NewAuthorizerNoop(config *Config, server ServerInterface, logger logger.Logger) *NoopAuthorizer {
	return &NoopAuthorizer{
		config: config,
		server: server,
		logger: logger,
	}
}

func (a *NoopAuthorizer) Authorize(ctx context.Context, storeID, apiMethod string) error {
	return nil
}

func (a *NoopAuthorizer) AuthorizeCreateStore(ctx context.Context) error {
	return nil
}

type Authorizer struct {
	config *Config
	server ServerInterface
	logger logger.Logger
}

func NewAuthorizer(config *Config, server ServerInterface, logger logger.Logger) *Authorizer {
	return &Authorizer{
		config: config,
		server: server,
		logger: logger,
	}
}

type AuthorizationError struct {
	Err error
}

func (e *AuthorizationError) Error() string {
	return fmt.Sprintf("error authorizing request: %s", e.Err)
}

func (e *AuthorizationError) Unwrap() error {
	return e.Err
}

func (a *Authorizer) getRelation(apiMethod string) (string, error) {
	switch apiMethod {
	case ReadAuthorizationModel, ReadAuthorizationModels:
		return CanCallReadAuthorizationModels, nil
	case Read:
		return CanCallRead, nil
	case Write:
		return CanCallWrite, nil
	case ListObjects, StreamedListObjects:
		return CanCallListObjects, nil
	case Check:
		return CanCallCheck, nil
	case ListUsers:
		return CanCallListUsers, nil
	case WriteAssertions:
		return CanCallWriteAssertions, nil
	case ReadAssertions:
		return CanCallReadAssertions, nil
	case WriteAuthorizationModel:
		return CanCallWriteAuthorizationModels, nil
	case CreateStore:
		return CanCallCreateStore, nil
	case GetStore:
		return CanCallGetStore, nil
	case DeleteStore:
		return CanCallDeleteStore, nil
	case Expand:
		return CanCallExpand, nil
	case ReadChanges:
		return CanCallReadChanges, nil
	default:
		return "", AuthorizationError{Err: ErrUnknownAPIMethod}.Err
	}
}

// Authorize checks if the user has access to the resource.
func (a *Authorizer) Authorize(ctx context.Context, storeID, apiMethod string) error {
	claims, err := checkAuthClaims(ctx)
	if err != nil {
		return err
	}

	relation, err := a.getRelation(apiMethod)
	if err != nil {
		return err
	}

	return a.individualAuthorize(ctx, claims.ClientID, relation, StoreIDType(storeID).String(), &openfgav1.ContextualTupleKeys{})
}

// AuthorizeCreateStore checks if the user has access to create a store.
func (a *Authorizer) AuthorizeCreateStore(ctx context.Context) error {
	claims, err := checkAuthClaims(ctx)
	if err != nil {
		return err
	}

	relation, err := a.getRelation(CreateStore)
	if err != nil {
		return err
	}

	return a.individualAuthorize(ctx, claims.ClientID, relation, System, &openfgav1.ContextualTupleKeys{})
}

func (a *Authorizer) individualAuthorize(ctx context.Context, clientID, relation, object string, contextualTuples *openfgav1.ContextualTupleKeys) error {
	req := &openfgav1.CheckRequest{
		StoreId:              a.config.StoreID,
		AuthorizationModelId: a.config.ModelID,
		TupleKey: &openfgav1.CheckRequestTupleKey{
			User:     ClientIDType(clientID).String(),
			Relation: relation,
			Object:   object,
		},
		ContextualTuples: contextualTuples,
	}

	// Disable authz check for the check request.
	ctx = authclaims.ContextWithSkipAuthzCheck(ctx, true)
	resp, err := a.server.Check(ctx, req)
	if err != nil {
		return err
	}

	if !resp.GetAllowed() {
		return status.Error(codes.Code(ErrUnauthorizedResponse.GetCode()), ErrUnauthorizedResponse.GetMessage())
	}

	return nil
}

// checkAuthClaims checks the auth claims in the context.
func checkAuthClaims(ctx context.Context) (*authclaims.AuthClaims, error) {
	claims, found := authclaims.AuthClaimsFromContext(ctx)
	if !found || claims.ClientID == "" {
		return nil, status.Error(codes.InvalidArgument, "client ID not found in context")
	}
	return claims, nil
}
