package authz

import (
	"context"
	"fmt"
	"sync"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/openfga/openfga/pkg/authcontext"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
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
	ModuleType      = "module"
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

// Authorize checks if the user has access to the resource.
func (a *Authorizer) Authorize(ctx context.Context, clientID, storeID, apiMethod string, modules ...string) error {
	relation, err := a.getRelation(apiMethod)
	if err != nil {
		return err
	}

	if len(modules) > 0 {
		return a.moduleAuthorize(ctx, clientID, relation, storeID, modules)
	}

	authorized, err := a.individualAuthorize(ctx, clientID, relation, a.getStore(storeID), &openfgav1.ContextualTupleKeys{})
	if err != nil {
		return err
	}

	if !authorized {
		return status.Error(codes.Code(ErrorResponse.GetCode()), ErrorResponse.GetMessage())
	}

	return nil
}

// AuthorizeCreateStore checks if the user has access to create a store.
func (a *Authorizer) AuthorizeCreateStore(ctx context.Context, clientID string) error {
	relation, err := a.getRelation(CreateStore)
	if err != nil {
		return err
	}
	authorized, err := a.individualAuthorize(ctx, clientID, relation, a.getSystem(), &openfgav1.ContextualTupleKeys{})
	if err != nil {
		return err
	}

	if !authorized {
		return status.Error(codes.Code(ErrorResponse.GetCode()), ErrorResponse.GetMessage())
	}

	return nil
}

// AuthorizeListStores checks if the user has access to list stores.
func (a *Authorizer) AuthorizeListStores(ctx context.Context, clientID string) error {
	relation, err := a.getRelation(ListStores)
	if err != nil {
		return err
	}

	authorized, err := a.individualAuthorize(ctx, clientID, relation, a.getSystem(), &openfgav1.ContextualTupleKeys{})
	if err != nil {
		return err
	}
	if !authorized {
		return status.Error(codes.Code(ErrorResponse.GetCode()), ErrorResponse.GetMessage())
	}

	return nil
}

// ListAuthorizedStores returns the list of stores that the user has access to.
func (a *Authorizer) ListAuthorizedStores(ctx context.Context, clientID string) ([]string, error) {
	err := a.AuthorizeListStores(ctx, clientID)
	if err != nil {
		return nil, err
	}

	req := &openfgav1.ListObjectsRequest{
		StoreId:              a.config.StoreID,
		AuthorizationModelId: a.config.ModelID,
		User:                 a.getApplication(clientID),
		Relation:             CanCallGetStore,
		Type:                 StoreType,
	}

	// Disable authz check for the list objects request.
	ctx = authcontext.ContextWithSkipAuthzCheck(ctx, true)
	resp, err := a.server.ListObjects(ctx, req)
	if err != nil {
		return nil, err
	}
	authcontext.ContextWithSkipAuthzCheck(ctx, false)

	return resp.GetObjects(), nil
}

// GetModulesForWriteRequest returns the modules that should be checked for the write request.
// If we encounter a type with no attached module, we should break and return no modules so that the authz check will be against the store
// Otherwise we return a list of unique modules encountered so that FGA on FGA can check them after.
func (a *Authorizer) GetModulesForWriteRequest(req *openfgav1.WriteRequest, typesys *typesystem.TypeSystem) ([]string, error) {
	modulesMap := make(map[string]struct{})

	// We keep track of shouldCheckOnStore to avoid checking on store if we encounter a type with no module
	shouldCheckOnStore := false
	for _, tupleKey := range req.GetWrites().GetTupleKeys() {
		objType, _ := tuple.SplitObject(tupleKey.GetObject())
		objectType, ok := typesys.GetTypeDefinition(objType)
		if !ok {
			return nil, serverErrors.TypeNotFound(objType)
		}
		module, err := parser.GetModuleForObjectTypeRelation(objectType, tupleKey.GetRelation())
		if err != nil {
			return nil, err
		}
		if module == "" {
			// If we encounter a type with no module, we should break and return no modules so that
			// the authz check will be against the store
			shouldCheckOnStore = true
			break
		}
		modulesMap[module] = struct{}{}
	}

	if !shouldCheckOnStore {
		for _, tupleKey := range req.GetDeletes().GetTupleKeys() {
			objType, _ := tuple.SplitObject(tupleKey.GetObject())
			objectType, ok := typesys.GetTypeDefinition(objType)
			if !ok {
				return nil, serverErrors.TypeNotFound(objType)
			}
			module, err := parser.GetModuleForObjectTypeRelation(objectType, tupleKey.GetRelation())
			if err != nil {
				return nil, err
			}
			if module == "" {
				// If we encounter a type with no module, we should break and return no modules so that
				// the authz check will be against the store
				shouldCheckOnStore = true
				break
			}
			modulesMap[module] = struct{}{}
		}
	}

	if shouldCheckOnStore {
		return []string{}, nil
	}

	modules := make([]string, 0, len(modulesMap))
	for module := range modulesMap {
		modules = append(modules, module)
	}

	return modules, nil
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

	// Disable authz check for the check request.
	ctx = authcontext.ContextWithSkipAuthzCheck(ctx, true)
	resp, err := a.server.Check(ctx, req)
	if err != nil {
		return false, err
	}
	authcontext.ContextWithSkipAuthzCheck(ctx, false)

	if !resp.GetAllowed() {
		return false, nil
	}

	return true, nil
}

func (a *Authorizer) moduleAuthorize(ctx context.Context, clientID, relation, storeID string, modules []string) error {
	var err error
	var wg sync.WaitGroup
	errorChannel := make(chan error, len(modules))
	defer close(errorChannel)
	done := make(chan struct{})
	defer close(done)

	for _, module := range modules {
		wg.Add(1)
		go func(module string) {
			defer wg.Done()
			contextualTuples := openfgav1.ContextualTupleKeys{
				TupleKeys: []*openfgav1.TupleKey{
					{
						User:     a.getStore(storeID),
						Relation: "store",
						Object:   a.getModule(storeID, module),
					},
				},
			}

			allowed, err := a.individualAuthorize(ctx, clientID, relation, a.getModule(storeID, module), &contextualTuples)

			if err != nil || !allowed {
				errorChannel <- err
			}
		}(module)
	}

	go func() {
		wg.Wait()
		done <- struct{}{}
	}()

	select {
	case err = <-errorChannel:
		return err
	case <-done:
		return nil
	}
}

func (a *Authorizer) getStore(storeID string) string {
	return fmt.Sprintf(`%s:%s`, StoreType, storeID)
}

func (a *Authorizer) getModule(storeID, module string) string {
	return fmt.Sprintf(`%s:%s|%s`, ModuleType, storeID, module)
}

func (a *Authorizer) getApplication(clientID string) string {
	return fmt.Sprintf(`%s:%s`, ApplicationType, clientID)
}

func (a *Authorizer) getSystem() string {
	return fmt.Sprintf(`%s:%s`, SystemType, RootSystemID)
}
