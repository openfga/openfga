package authz

import (
	"context"
	"errors"
	"fmt"
	"sync"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/openfga/openfga/pkg/authclaims"
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
	ModuleType      = "module"
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

type ModuleIDType string

func (m ModuleIDType) String(module string) string {
	return fmt.Sprintf(`%s:%s|%s`, ModuleType, string(m), module)
}

type Config struct {
	StoreID string
	ModelID string
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
func (a *Authorizer) Authorize(ctx context.Context, storeID, apiMethod string, modules ...string) error {
	claims, err := checkAuthClaims(ctx)
	if err != nil {
		return err
	}

	relation, err := a.getRelation(apiMethod)
	if err != nil {
		return err
	}

	// Check if there is top-level authorization first, before checking modules
	err = a.individualAuthorize(ctx, claims.ClientID, relation, StoreIDType(storeID).String(), &openfgav1.ContextualTupleKeys{})
	if err == nil {
		return nil
	}

	if len(modules) > 0 {
		return a.moduleAuthorize(ctx, claims.ClientID, relation, storeID, modules)
	}
	// If there are no modules to check, return the top-level authorization error
	return err
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

// GetModulesForWriteRequest returns the modules that should be checked for the write request.
// If we encounter a type with no attached module, we should break and return no modules so that the authz check will be against the store
// Otherwise we return a list of unique modules encountered so that FGA on FGA can check them after.
func (a *Authorizer) GetModulesForWriteRequest(req *openfgav1.WriteRequest, typesys *typesystem.TypeSystem) ([]string, error) {
	modulesMap := make(map[string]struct{})

	modulesMap, shouldCheckOnStore, err := processTupleKeys(req.GetWrites().GetTupleKeys(), typesys, modulesMap)
	if err != nil {
		return nil, err
	}

	if !shouldCheckOnStore {
		modulesMap, shouldCheckOnStore, err = processTupleKeys(req.GetDeletes().GetTupleKeys(), typesys, modulesMap)
		if err != nil {
			return nil, err
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

// Define an interface that both TupleKeyWithoutCondition and TupleKey implement.
type TupleKeyInterface interface {
	GetObject() string
	GetRelation() string
}

func processTupleKeys[T TupleKeyInterface](tupleKeys []T, typesys *typesystem.TypeSystem, modulesMap map[string]struct{}) (map[string]struct{}, bool, error) {
	for _, tupleKey := range tupleKeys {
		objType, _ := tuple.SplitObject(tupleKey.GetObject())
		objectType, ok := typesys.GetTypeDefinition(objType)
		if !ok {
			return nil, false, serverErrors.TypeNotFound(objType)
		}
		module, err := parser.GetModuleForObjectTypeRelation(objectType, tupleKey.GetRelation())
		if err != nil {
			return nil, false, err
		}
		if module == "" {
			// If we encounter a type with no module,
			// we should return no modules and set the shouldCheckOnStore flag
			// to true so that the authz check will be against the store
			return nil, true, nil
		}
		modulesMap[module] = struct{}{}
	}

	return modulesMap, false, nil
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

// moduleAuthorize checks if the user has access to each of the modules, and exits if an error is encountered.
func (a *Authorizer) moduleAuthorize(ctx context.Context, clientID, relation, storeID string, modules []string) error {
	var err error
	var wg sync.WaitGroup
	errorChannel := make(chan error, len(modules))
	done := make(chan struct{})

	for _, module := range modules {
		wg.Add(1)
		go func(module string) {
			defer wg.Done()
			contextualTuples := openfgav1.ContextualTupleKeys{
				TupleKeys: []*openfgav1.TupleKey{
					{
						User:     StoreIDType(storeID).String(),
						Relation: StoreType,
						Object:   ModuleIDType(storeID).String(module),
					},
				},
			}

			err := a.individualAuthorize(ctx, clientID, relation, ModuleIDType(storeID).String(module), &contextualTuples)

			if err != nil {
				errorChannel <- err
			}
		}(module)
	}

	go func() {
		wg.Wait()
		close(done)
		close(errorChannel)
	}()

	select {
	case err = <-errorChannel:
		return err
	case <-done:
		return nil
	}
}

// checkAuthClaims checks the auth claims in the context.
func checkAuthClaims(ctx context.Context) (*authclaims.AuthClaims, error) {
	claims, found := authclaims.AuthClaimsFromContext(ctx)
	if !found || claims.ClientID == "" {
		return nil, status.Error(codes.Internal, "client ID not found in context")
	}
	return claims, nil
}
