package authz

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/internal/utils/apimethod"
	"github.com/openfga/openfga/pkg/authclaims"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestGetRelation(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockServer := mocks.NewMockServerInterface(mockController)

	authorizer := NewAuthorizer(&Config{StoreID: "test-store", ModelID: "test-model"}, mockServer, logger.NewNoopLogger())

	tests := []struct {
		method         apimethod.APIMethod
		expectedResult string
		errorMsg       string
	}{
		{method: apimethod.ReadAuthorizationModel, expectedResult: CanCallReadAuthorizationModels},
		{method: apimethod.ReadAuthorizationModels, expectedResult: CanCallReadAuthorizationModels},
		{method: apimethod.Read, expectedResult: CanCallRead},
		{method: apimethod.Write, expectedResult: CanCallWrite},
		{method: apimethod.ListObjects, expectedResult: CanCallListObjects},
		{method: apimethod.StreamedListObjects, expectedResult: CanCallListObjects},
		{method: apimethod.Check, expectedResult: CanCallCheck},
		{method: apimethod.BatchCheck, expectedResult: CanCallCheck},
		{method: apimethod.ListUsers, expectedResult: CanCallListUsers},
		{method: apimethod.WriteAssertions, expectedResult: CanCallWriteAssertions},
		{method: apimethod.ReadAssertions, expectedResult: CanCallReadAssertions},
		{method: apimethod.WriteAuthorizationModel, expectedResult: CanCallWriteAuthorizationModels},
		{method: apimethod.CreateStore, expectedResult: CanCallCreateStore},
		{method: apimethod.GetStore, expectedResult: CanCallGetStore},
		{method: apimethod.DeleteStore, expectedResult: CanCallDeleteStore},
		{method: apimethod.Expand, expectedResult: CanCallExpand},
		{method: apimethod.ReadChanges, expectedResult: CanCallReadChanges},
		{method: "Unknown", errorMsg: "unknown API method: Unknown"},
	}

	for _, test := range tests {
		t.Run(string(test.method), func(t *testing.T) {
			result, err := authorizer.getRelation(test.method)
			if test.errorMsg != "" {
				require.Error(t, err)
				require.Equal(t, test.errorMsg, err.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expectedResult, result)
			}
		})
	}
}

func TestGetStoreID(t *testing.T) {
	t.Run("has_store_ID", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockServer := mocks.NewMockServerInterface(mockController)

		authorizer := NewAuthorizer(&Config{StoreID: "test-store", ModelID: "test-model"}, mockServer, logger.NewNoopLogger())
		require.Equal(t, "test-store", authorizer.AccessControlStoreID())
	})
	t.Run("no_config", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockServer := mocks.NewMockServerInterface(mockController)

		authorizer := NewAuthorizer(nil, mockServer, logger.NewNoopLogger())
		require.Empty(t, authorizer.AccessControlStoreID())
	})
}

func TestAuthorizeCreateStore(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockServer := mocks.NewMockServerInterface(mockController)

	authorizer := NewAuthorizer(&Config{StoreID: "test-store", ModelID: "test-model"}, mockServer, logger.NewNoopLogger())

	t.Run("error_when_authorized_errors", func(t *testing.T) {
		errorMessage := fmt.Errorf("unable to perform action")
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(nil, errorMessage)

		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})
		err := authorizer.AuthorizeCreateStore(ctx)

		require.Error(t, err)
		var authError *authorizationError
		ok := errors.As(err, &authError)
		require.True(t, ok)
		require.ErrorContains(t, authError, "check returned error")
	})

	t.Run("error_when_not_authorized", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, nil)

		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})
		err := authorizer.AuthorizeCreateStore(ctx)

		require.Error(t, err)
		var authError *authorizationError
		ok := errors.As(err, &authError)
		require.True(t, ok)
		require.ErrorContains(t, authError, "check returned not allowed")
	})

	t.Run("succeed", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: true}, nil)

		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})
		err := authorizer.AuthorizeCreateStore(ctx)

		require.NoError(t, err)
	})
}

func TestAuthorizeListStores(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockServer := mocks.NewMockServerInterface(mockController)

	authorizer := NewAuthorizer(&Config{StoreID: "test-store", ModelID: "test-model"}, mockServer, logger.NewNoopLogger())

	t.Run("error_when_authorized_errors", func(t *testing.T) {
		errorMessage := fmt.Errorf("unable to perform action")
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(nil, errorMessage)

		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})
		err := authorizer.AuthorizeListStores(ctx)

		require.Error(t, err)
		var authError *authorizationError
		ok := errors.As(err, &authError)
		require.True(t, ok)
		require.ErrorContains(t, authError, "check returned error")
	})

	t.Run("error_when_not_authorized", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, nil)

		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})
		err := authorizer.AuthorizeListStores(ctx)

		require.Error(t, err)
		var authError *authorizationError
		ok := errors.As(err, &authError)
		require.True(t, ok)
		require.ErrorContains(t, authError, "check returned not allowed")
	})

	t.Run("succeed", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: true}, nil)

		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})
		err := authorizer.AuthorizeListStores(ctx)

		require.NoError(t, err)
	})
}

func setupAuthorizerAndController(t *testing.T, storeID, modelID string) (*gomock.Controller, *mocks.MockServerInterface, *Authorizer) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()
	mockServer := mocks.NewMockServerInterface(mockController)
	authorizer := NewAuthorizer(&Config{StoreID: storeID, ModelID: modelID}, mockServer, logger.NewNoopLogger())
	return mockController, mockServer, authorizer
}

func TestAuthorize(t *testing.T) {
	storeID := "test-store"
	modelID := "test-model"

	t.Run("error_when_given_invalid_api_method", func(t *testing.T) {
		t.Parallel()

		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})
		_, _, authorizer := setupAuthorizerAndController(t, storeID, modelID)

		err := authorizer.Authorize(ctx, "store-id", "invalid-api-method")

		require.Error(t, err)
		var authError *authorizationError
		ok := errors.As(err, &authError)
		require.True(t, ok)
		require.ErrorContains(t, authError, "unknown API method")
	})

	t.Run("error_when_modules_errors", func(t *testing.T) {
		t.Parallel()

		mockController, mockServer, authorizer := setupAuthorizerAndController(t, storeID, modelID)
		defer mockController.Finish()

		errorMessage := fmt.Errorf("error")
		modules := []string{}

		for moduleIndex := range MaxModulesInRequest {
			modules = append(modules, fmt.Sprintf("module%d", moduleIndex+1))
		}

		// First error is the store level check, second error is the first module level check
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).MinTimes(2).Return(nil, errorMessage)
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).MinTimes(MaxModulesInRequest-1).Return(&openfgav1.CheckResponse{Allowed: true}, nil)

		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})
		err := authorizer.Authorize(ctx, "store-id", apimethod.Write, modules...)

		require.Error(t, err)
		var authError *authorizationError
		ok := errors.As(err, &authError)
		require.True(t, ok)
		require.ErrorContains(t, authError, "check returned error")
	})

	t.Run("error_when_check_errors", func(t *testing.T) {
		t.Parallel()

		mockController, mockServer, authorizer := setupAuthorizerAndController(t, storeID, modelID)
		defer mockController.Finish()

		errorMessage := fmt.Errorf("error")
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, errorMessage)

		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})
		err := authorizer.Authorize(ctx, "store-id", apimethod.CreateStore)

		require.Error(t, err)
		var authError *authorizationError
		ok := errors.As(err, &authError)
		require.True(t, ok)
		require.ErrorContains(t, authError, "check returned error")
	})

	t.Run("error_when_unauthorized", func(t *testing.T) {
		t.Parallel()

		mockController, mockServer, authorizer := setupAuthorizerAndController(t, storeID, modelID)
		defer mockController.Finish()

		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, nil)
		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})

		err := authorizer.Authorize(ctx, "store-id", apimethod.CreateStore)

		var authError *authorizationError
		ok := errors.As(err, &authError)
		require.True(t, ok)
		require.ErrorContains(t, authError, "check returned not allowed")
	})

	t.Run("succeed", func(t *testing.T) {
		t.Parallel()

		mockController, mockServer, authorizer := setupAuthorizerAndController(t, storeID, modelID)
		defer mockController.Finish()

		clientID := "test-client"
		contextualTuples := openfgav1.ContextualTupleKeys{
			TupleKeys: []*openfgav1.TupleKey{
				getSystemAccessTuple(storeID),
			},
		}
		checkReq := &openfgav1.CheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: modelID,
			TupleKey: &openfgav1.CheckRequestTupleKey{
				User:     ClientIDType(clientID).String(),
				Relation: CanCallCreateStore,
				Object:   StoreIDType(storeID).String(),
			},
			ContextualTuples: &contextualTuples,
		}
		mockServer.EXPECT().Check(gomock.Any(), checkReq).Return(&openfgav1.CheckResponse{Allowed: true}, nil)
		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})

		err := authorizer.Authorize(ctx, storeID, apimethod.CreateStore)

		require.NoError(t, err)
	})

	t.Run("succeed_with_modules", func(t *testing.T) {
		t.Parallel()

		mockController, mockServer, authorizer := setupAuthorizerAndController(t, storeID, modelID)
		defer mockController.Finish()

		modules := []string{}
		for moduleIndex := range MaxModulesInRequest {
			modules = append(modules, fmt.Sprintf("module%d", moduleIndex))
		}

		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).MinTimes(MaxModulesInRequest).Return(&openfgav1.CheckResponse{Allowed: true}, nil)

		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})
		err := authorizer.Authorize(ctx, "store-id", apimethod.Write, modules...)

		require.NoError(t, err)
	})
}

func TestExtractModulesFromTuples(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user]
		type folder
			relations
				define viewer: [user]
		type folder-without-module
			relations
				define viewer: [user]`
	module1 := "module1"
	module2 := "module2"

	authModel := testutils.MustTransformDSLToProtoWithID(model)

	typeDef := authModel.GetTypeDefinitions()
	for _, td := range typeDef {
		if td.GetType() == "document" {
			td.Metadata.Module = module1
		}
		if td.GetType() == "folder" {
			td.Metadata.Module = module2
		}
	}
	ts, err := typesystem.New(authModel)
	require.NoError(t, err)

	t.Run("return_empty_map_when_no_tuples", func(t *testing.T) {
		tuples := []TupleKeyInterface{}
		modules, err := extractModulesFromTuples(tuples, ts)
		require.Empty(t, modules)
		require.NoError(t, err)
	})

	t.Run("return_error_when_objectType_not_found", func(t *testing.T) {
		tuples := []TupleKeyInterface{
			&openfgav1.TupleKey{Object: "unknown:1", Relation: "viewer", User: "user:jon"},
		}
		modules, err := extractModulesFromTuples(tuples, ts)
		require.Empty(t, modules)
		require.Error(t, err)
		require.Equal(t, "type 'unknown' not found", err.Error())
	})

	t.Run("return_error_when_GetModuleForObjectTypeRelation_errors", func(t *testing.T) {
		tuples := []TupleKeyInterface{
			&openfgav1.TupleKey{Object: "document:1", Relation: "unknown", User: "user:jon"},
		}
		modules, err := extractModulesFromTuples(tuples, ts)
		require.Empty(t, modules)
		require.Error(t, err)
		require.Equal(t, "relation unknown does not exist in type document", err.Error())
	})

	t.Run("return_empty_when_first_tuple_has_no_modules", func(t *testing.T) {
		tuples := []TupleKeyInterface{
			&openfgav1.TupleKey{Object: "folder-without-module:1", Relation: "viewer", User: "user:jon"},
			&openfgav1.TupleKey{Object: "document:1", Relation: "viewer", User: "user:jon"},
		}
		modules, err := extractModulesFromTuples(tuples, ts)
		require.NoError(t, err)
		require.Empty(t, modules)
	})

	t.Run("return_empty_when_any_tuple_has_no_modules", func(t *testing.T) {
		tuples := []TupleKeyInterface{
			&openfgav1.TupleKey{Object: "document:2", Relation: "viewer", User: "user:jon"},
			&openfgav1.TupleKey{Object: "folder-without-module:1", Relation: "viewer", User: "user:jon"},
			&openfgav1.TupleKey{Object: "document:1", Relation: "viewer", User: "user:jon"},
			&openfgav1.TupleKey{Object: "folder:1", Relation: "viewer", User: "user:jon"},
		}
		modules, err := extractModulesFromTuples(tuples, ts)
		require.NoError(t, err)
		require.Empty(t, modules)
	})

	t.Run("return_empty_when_last_tuple_has_no_modules", func(t *testing.T) {
		tuples := []TupleKeyInterface{
			&openfgav1.TupleKey{Object: "document:2", Relation: "viewer", User: "user:jon"},
			&openfgav1.TupleKey{Object: "document:1", Relation: "viewer", User: "user:jon"},
			&openfgav1.TupleKey{Object: "folder:1", Relation: "viewer", User: "user:jon"},
			&openfgav1.TupleKey{Object: "folder-without-module:1", Relation: "viewer", User: "user:jon"},
		}
		modules, err := extractModulesFromTuples(tuples, ts)
		require.NoError(t, err)
		require.Empty(t, modules)
	})

	t.Run("return_modules", func(t *testing.T) {
		tuples := []TupleKeyInterface{
			&openfgav1.TupleKey{Object: "document:2", Relation: "viewer", User: "user:jon"},
			&openfgav1.TupleKey{Object: "document:1", Relation: "viewer", User: "user:jon"},
			&openfgav1.TupleKey{Object: "folder:1", Relation: "viewer", User: "user:jon"},
		}
		modules, err := extractModulesFromTuples(tuples, ts)
		require.NoError(t, err)
		require.Equal(t, map[string]struct{}{"module1": {}, "module2": {}}, modules)
	})
}

func TestListAuthorizedStores(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockServer := mocks.NewMockServerInterface(mockController)

	authorizer := NewAuthorizer(&Config{StoreID: "test-store", ModelID: "test-model"}, mockServer, logger.NewNoopLogger())

	t.Run("error_when_invalid_claims", func(t *testing.T) {
		ctx := context.Background()
		_, err := authorizer.ListAuthorizedStores(ctx)

		require.Error(t, err)
		var authError *authorizationError
		ok := errors.As(err, &authError)
		require.True(t, ok)
		require.ErrorContains(t, authError, "client ID not found in context")
	})

	t.Run("error_when_list_objects_errors", func(t *testing.T) {
		errorMessage := fmt.Errorf("error")
		mockServer.EXPECT().ListObjects(gomock.Any(), gomock.Any()).Return(nil, errorMessage)

		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})
		_, err := authorizer.ListAuthorizedStores(ctx)

		require.Error(t, err)
		var authError *authorizationError
		ok := errors.As(err, &authError)
		require.True(t, ok)
		require.ErrorContains(t, authError, "list objects returned error")
	})

	t.Run("succeed", func(t *testing.T) {
		ID1 := "1234"
		ID2 := "5678"
		mockServer.EXPECT().ListObjects(gomock.Any(), gomock.Any()).Return(&openfgav1.ListObjectsResponse{Objects: []string{fmt.Sprintf("store:%s", ID1), fmt.Sprintf("store:%s", ID2)}}, nil)

		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})
		stores, err := authorizer.ListAuthorizedStores(ctx)

		require.NoError(t, err)
		require.Equal(t, []string{ID1, ID2}, stores)
	})
}

func TestIndividualAuthorize(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockServer := mocks.NewMockServerInterface(mockController)

	authorizer := NewAuthorizer(&Config{StoreID: "test-store", ModelID: "test-model"}, mockServer, logger.NewNoopLogger())

	t.Run("error_when_check_errors", func(t *testing.T) {
		errorMessage := fmt.Errorf("error")
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, errorMessage)

		err := authorizer.individualAuthorize(context.Background(), "client-id", CanCallCreateStore, "system", &openfgav1.ContextualTupleKeys{})

		require.Error(t, err)
		var authError *authorizationError
		ok := errors.As(err, &authError)
		require.True(t, ok)
		require.ErrorContains(t, authError, "check returned error")
	})
	t.Run("error_when_unauthorized", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, nil)

		err := authorizer.individualAuthorize(context.Background(), "client-id", CanCallCreateStore, "system", &openfgav1.ContextualTupleKeys{})

		require.Error(t, err)
		var authError *authorizationError
		ok := errors.As(err, &authError)
		require.True(t, ok)
		require.ErrorContains(t, authError, "check returned not allowed")
	})
	t.Run("succeed", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: true}, nil)

		err := authorizer.individualAuthorize(context.Background(), "client-id", CanCallCreateStore, "system", &openfgav1.ContextualTupleKeys{})

		require.NoError(t, err)
	})
}

func TestModuleAuthorize(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockServer := mocks.NewMockServerInterface(mockController)

	storeID := "test-store"
	modelID := "test-model"
	authorizer := NewAuthorizer(&Config{StoreID: storeID, ModelID: modelID}, mockServer, logger.NewNoopLogger())

	t.Run("return_no_error_when_no_modules", func(t *testing.T) {
		err := authorizer.moduleAuthorize(context.Background(), "client-id", CanCallWrite, "store-id", []string{})
		require.NoError(t, err)
	})

	t.Run("error_when_first_module_errors", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, nil)
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: true}, nil)
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: true}, nil)

		err := authorizer.moduleAuthorize(context.Background(), "client-id", CanCallWrite, "store-id", []string{"module1", "module2", "module3"})
		require.Error(t, err)
		var authError *authorizationError
		ok := errors.As(err, &authError)
		require.True(t, ok)
		require.ErrorContains(t, authError, "check returned not allowed")
	})

	t.Run("error_when_last_module_errors", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: true}, nil)
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: true}, nil)
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, nil)

		err := authorizer.moduleAuthorize(context.Background(), "client-id", CanCallWrite, "store-id", []string{"module1", "module2", "module3"})
		require.Error(t, err)
		var authError *authorizationError
		ok := errors.As(err, &authError)
		require.True(t, ok)
		require.ErrorContains(t, authError, "check returned not allowed")
	})

	t.Run("error_when_all_modules_error", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, nil)
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, nil)
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, nil)

		err := authorizer.moduleAuthorize(context.Background(), "client-id", CanCallWrite, "store-id", []string{"module1", "module2", "module3"})
		require.Error(t, err)
		var authError *authorizationError
		ok := errors.As(err, &authError)
		require.True(t, ok)
		require.ErrorContains(t, authError, "check returned not allowed")
	})

	t.Run("succeed", func(t *testing.T) {
		module1 := "module1"
		contextualTuples := openfgav1.ContextualTupleKeys{
			TupleKeys: []*openfgav1.TupleKey{
				{
					User:     StoreIDType(storeID).String(),
					Relation: StoreType,
					Object:   ModuleIDType(storeID).String(module1),
				},
				getSystemAccessTuple(storeID),
			},
		}
		clientID := "client-id"
		checkReq := &openfgav1.CheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: modelID,
			TupleKey: &openfgav1.CheckRequestTupleKey{
				User:     ClientIDType(clientID).String(),
				Relation: CanCallWrite,
				Object:   ModuleIDType(storeID).String(module1),
			},
			ContextualTuples: &contextualTuples,
		}
		mockServer.EXPECT().Check(gomock.Any(), checkReq).Return(&openfgav1.CheckResponse{Allowed: true}, nil)

		err := authorizer.moduleAuthorize(context.Background(), clientID, CanCallWrite, storeID, []string{module1})
		require.NoError(t, err)
	})

	t.Run("succeed_when_all_modules_are_allowed", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: true}, nil)
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: true}, nil)
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: true}, nil)

		err := authorizer.moduleAuthorize(context.Background(), "client-id", CanCallWrite, storeID, []string{"module1", "module2", "module3"})
		require.NoError(t, err)
	})

	t.Run("should_error_when_panic_occurs", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
			panic("test panic")
		}).AnyTimes()

		err := authorizer.moduleAuthorize(context.Background(), "client-id", CanCallWrite, storeID, []string{"module1", "module2", "module3"})
		require.Error(t, err)
		var authError *authorizationError
		ok := errors.As(err, &authError)
		require.True(t, ok)
		require.ErrorContains(t, authError, "panic recovered")
	})
}

func TestGetModulesForWriteRequest(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockServer := mocks.NewMockServerInterface(mockController)

	authorizer := NewAuthorizer(&Config{StoreID: "test-store", ModelID: "test-model"}, mockServer, logger.NewNoopLogger())

	module1 := "module1"
	model := &openfgav1.AuthorizationModel{
		SchemaVersion: typesystem.SchemaVersion1_1,
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{
				Type: "user",
			},
			{
				Type: "folder",
				Relations: map[string]*openfgav1.Userset{
					"viewer": typesystem.This(),
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"viewer": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								{Type: "user"},
							},
						},
					},
				},
			},
			{
				Type: "folder-with-module",
				Relations: map[string]*openfgav1.Userset{
					"viewer": typesystem.This(),
				},
				Metadata: &openfgav1.Metadata{
					Module: module1,
					Relations: map[string]*openfgav1.RelationMetadata{
						"parent": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								{Type: "folder"},
							},
						},
					},
				},
			},
		},
	}
	typesys, err := typesystem.New(model)
	require.NoError(t, err)

	t.Run("error_when_write_tuples_errors", func(t *testing.T) {
		modules, err := authorizer.GetModulesForWriteRequest(
			context.Background(),
			&openfgav1.WriteRequest{
				StoreId: "store-id",
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						{Object: "unknown:2", Relation: "viewer", User: "user:jon"},
					},
				},
			},
			typesys,
		)
		require.Error(t, err)
		require.Empty(t, modules)
	})

	t.Run("error_when_delete_tuples_errors", func(t *testing.T) {
		modules, err := authorizer.GetModulesForWriteRequest(
			context.Background(),
			&openfgav1.WriteRequest{
				StoreId: "store-id",
				Deletes: &openfgav1.WriteRequestDeletes{
					TupleKeys: []*openfgav1.TupleKeyWithoutCondition{
						{Object: "unknown:2", Relation: "viewer", User: "user:jon"},
					},
				},
			},
			typesys,
		)
		require.Error(t, err)
		require.Empty(t, modules)
	})

	t.Run("return_empty_when_a_write_tuple_has_no_modules", func(t *testing.T) {
		modules, err := authorizer.GetModulesForWriteRequest(
			context.Background(),
			&openfgav1.WriteRequest{
				StoreId: "store-id",
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						{Object: "folder-with-module:2", Relation: "viewer", User: "user:jon"},
						{Object: "folder:2", Relation: "viewer", User: "user:jon"},
					},
				},
			},
			typesys,
		)
		require.NoError(t, err)
		require.Empty(t, modules)
	})

	t.Run("return_empty_when_a_delete_tuple_has_no_modules", func(t *testing.T) {
		modules, err := authorizer.GetModulesForWriteRequest(
			context.Background(),
			&openfgav1.WriteRequest{
				StoreId: "store-id",
				Deletes: &openfgav1.WriteRequestDeletes{
					TupleKeys: []*openfgav1.TupleKeyWithoutCondition{
						{Object: "folder-with-module:2", Relation: "viewer", User: "user:jon"},
						{Object: "folder:2", Relation: "viewer", User: "user:jon"},
					},
				},
			},
			typesys,
		)
		require.NoError(t, err)
		require.Empty(t, modules)
	})

	t.Run("return_modules", func(t *testing.T) {
		modules, err := authorizer.GetModulesForWriteRequest(
			context.Background(),
			&openfgav1.WriteRequest{
				StoreId: "store-id",
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						{Object: "folder-with-module:1", Relation: "viewer", User: "user:jon"},
					},
				},
				Deletes: &openfgav1.WriteRequestDeletes{
					TupleKeys: []*openfgav1.TupleKeyWithoutCondition{
						{Object: "folder-with-module:2", Relation: "viewer", User: "user:jon"},
					},
				},
			},
			typesys,
		)
		require.NoError(t, err)
		require.Equal(t, []string{module1}, modules)
	})
}
