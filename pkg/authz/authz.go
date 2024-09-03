package authz

import (
	"context"
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/openfga/openfga/pkg/logger"
)

type ctxKey string

const (
	// SkipAuthz is the key to store whether to skip authz check in the context.
	skipAuthz ctxKey = "skip-authz-key"

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
	ListStores              = "ListStores"
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
	CanCallListStores               = "can_call_list_stores"
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
	ErrorResponse = &openfgav1.ForbiddenResponse{Code: 403, Message: "the principal is not authorized to perform the action"}
)

type Config struct {
	StoreID string
	ModelID string
}

type Authorizer struct {
	config *Config
	server ServerInterface
	logger logger.Logger
}

// NewAuthorizer creates a new authorizer.
func NewAuthorizer(config *Config, server ServerInterface, logger logger.Logger) *Authorizer {
	return &Authorizer{
		config: config,
		server: server,
		logger: logger,
	}
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
	case ListStores:
		return CanCallListStores, nil
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
		return "", fmt.Errorf("unknown api method: %s", apiMethod)
	}
}

// AuthorizeCreateStore checks if the user has access to create a store.
func (a *Authorizer) AuthorizeCreateStore(ctx context.Context, clientID string) (bool, error) {
	relation, err := a.getRelation(CreateStore)
	if err != nil {
		return false, err
	}
	allowed, err := a.individualAuthorize(ctx, clientID, relation, a.getSystem(), &openfgav1.ContextualTupleKeys{})
	if !allowed || err != nil {
		return false, err
	}

	return true, nil
}

// ListAuthorizedStores returns the list of stores that the user has access to.
func (a *Authorizer) ListAuthorizedStores(ctx context.Context, clientID string) ([]string, error) {
	relation, err := a.getRelation(ListStores)
	if err != nil {
		return nil, err
	}

	allowed, err := a.individualAuthorize(ctx, clientID, relation, a.getSystem(), &openfgav1.ContextualTupleKeys{})
	if !allowed || err != nil {
		return nil, status.Error(codes.Code(ErrorResponse.GetCode()), ErrorResponse.GetMessage())
	}

	req := &openfgav1.ListObjectsRequest{
		StoreId:              a.config.StoreID,
		AuthorizationModelId: a.config.ModelID,
		User:                 a.getApplication(clientID),
		Relation:             CanCallGetStore,
		Type:                 StoreType,
	}

	resp, err := a.server.ListObjectsWithoutAuthz(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.GetObjects(), nil
}

// ContextWithSkipAuthzCheck attaches whether to skip authz check to the parent context.
func ContextWithSkipAuthzCheck(parent context.Context, skipAuthzCheck bool) context.Context {
	return context.WithValue(parent, skipAuthz, skipAuthzCheck)
}

// SkipAuthzCheckFromContext returns whether the authorize check can be skipped.
func SkipAuthzCheckFromContext(ctx context.Context) bool {
	isSkipped, ok := ctx.Value(skipAuthz).(bool)
	return isSkipped && ok
}

// Authorize checks if the user has access to the resource.
func (a *Authorizer) Authorize(ctx context.Context, clientID, storeID, apiMethod string) (bool, error) {
	relation, err := a.getRelation(apiMethod)
	if err != nil {
		return false, err
	}

	return a.individualAuthorize(ctx, clientID, relation, a.getStore(storeID), &openfgav1.ContextualTupleKeys{})
}

func (a *Authorizer) individualAuthorize(ctx context.Context, clientID, relation, object string, contextualTuples *openfgav1.ContextualTupleKeys) (bool, error) {
	req := &openfgav1.CheckRequest{
		StoreId:              a.config.StoreID,
		AuthorizationModelId: a.config.ModelID,
		TupleKey: &openfgav1.CheckRequestTupleKey{
			User:     a.getApplication(clientID),
			Relation: relation,
			Object:   object,
		},
		ContextualTuples: contextualTuples,
	}

	resp, err := a.server.CheckWithoutAuthz(ctx, req)
	if err != nil {
		return false, err
	}

	if !resp.GetAllowed() {
		return false, nil
	}

	return true, nil
}

func (a *Authorizer) getStore(storeID string) string {
	return fmt.Sprintf(`%s:%s`, StoreType, storeID)
}

func (a *Authorizer) getApplication(clientID string) string {
	return fmt.Sprintf(`%s:%s`, ApplicationType, clientID)
}

func (a *Authorizer) getSystem() string {
	return fmt.Sprintf(`%s:%s`, SystemType, RootSystemID)
}
