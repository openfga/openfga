package authz

import (
	"context"
	"fmt"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/internal/mocks"
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
		name           string
		expectedResult string
		errorMsg       string
	}{
		{name: "ReadAuthorizationModel", expectedResult: CanCallReadAuthorizationModels},
		{name: "ReadAuthorizationModels", expectedResult: CanCallReadAuthorizationModels},
		{name: "Read", expectedResult: CanCallRead},
		{name: "Write", expectedResult: CanCallWrite},
		{name: "ListObjects", expectedResult: CanCallListObjects},
		{name: "StreamedListObjects", expectedResult: CanCallListObjects},
		{name: "Check", expectedResult: CanCallCheck},
		{name: "ListUsers", expectedResult: CanCallListUsers},
		{name: "WriteAssertions", expectedResult: CanCallWriteAssertions},
		{name: "ReadAssertions", expectedResult: CanCallReadAssertions},
		{name: "WriteAuthorizationModel", expectedResult: CanCallWriteAuthorizationModels},
		{name: "CreateStore", expectedResult: CanCallCreateStore},
		{name: "GetStore", expectedResult: CanCallGetStore},
		{name: "DeleteStore", expectedResult: CanCallDeleteStore},
		{name: "Expand", expectedResult: CanCallExpand},
		{name: "ReadChanges", expectedResult: CanCallReadChanges},
		{name: "Unknown", errorMsg: ErrUnknownAPIMethod.Error()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := authorizer.getRelation(test.name)
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
		require.Equal(t, fmt.Sprintf("error authorizing the call: %s", errorMessage.Error()), err.Error())
	})

	t.Run("error_when_not_authorized", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, nil)

		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})
		err := authorizer.AuthorizeCreateStore(ctx)

		require.Error(t, err)
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
		require.Equal(t, fmt.Sprintf("error authorizing the call: %s", errorMessage.Error()), err.Error())
	})

	t.Run("error_when_not_authorized", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, nil)

		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})
		err := authorizer.AuthorizeListStores(ctx)

		require.Error(t, err)
	})

	t.Run("succeed", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: true}, nil)

		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})
		err := authorizer.AuthorizeListStores(ctx)

		require.NoError(t, err)
	})
}

func TestAuthorize(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockServer := mocks.NewMockServerInterface(mockController)

	storeID := "test-store"
	modelID := "test-model"
	authorizer := NewAuthorizer(&Config{StoreID: storeID, ModelID: modelID}, mockServer, logger.NewNoopLogger())

	t.Run("error_when_given_invalid_api_method", func(t *testing.T) {
		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})

		err := authorizer.Authorize(ctx, "store-id", "invalid-api-method")

		require.Error(t, err)
		require.Equal(t, ErrUnknownAPIMethod.Error(), err.Error())
	})

	t.Run("error_when_modules_errors", func(t *testing.T) {
		modules := []string{"module1", "module2", "module3"}
		errorMessage := fmt.Errorf("error")
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, nil)
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: true}, nil)
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(nil, errorMessage)
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: true}, nil)
		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})

		err := authorizer.Authorize(ctx, "store-id", Write, modules...)

		require.Error(t, err)
		require.Equal(t, fmt.Sprintf("error authorizing the call: %s", errorMessage.Error()), err.Error())
	})

	t.Run("error_when_check_errors", func(t *testing.T) {
		errorMessage := fmt.Errorf("error")
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, errorMessage)

		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})
		err := authorizer.Authorize(ctx, "store-id", CreateStore)

		require.Error(t, err)
		require.Equal(t, fmt.Sprintf("error authorizing the call: %s", errorMessage.Error()), err.Error())
	})

	t.Run("error_when_unauthorized", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, nil)
		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})

		err := authorizer.Authorize(ctx, "store-id", CreateStore)

		require.Error(t, err)
		require.Equal(t, "rpc error: code = Code(1600) desc = the principal is not authorized to perform the action", err.Error())
	})

	t.Run("succeed", func(t *testing.T) {
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

		err := authorizer.Authorize(ctx, storeID, CreateStore)

		require.NoError(t, err)
	})

	t.Run("succeed_with_modules", func(t *testing.T) {
		modules := []string{"module1", "module2", "module3"}
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, nil)
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).MinTimes(3).Return(&openfgav1.CheckResponse{Allowed: true}, nil)
		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})

		err := authorizer.Authorize(ctx, "store-id", Write, modules...)

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
		require.Equal(t, "rpc error: code = Code(2021) desc = type 'unknown' not found", err.Error())
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
		require.Equal(t, "rpc error: code = InvalidArgument desc = client ID not found in context", err.Error())
	})

	t.Run("error_when_list_objects_errors", func(t *testing.T) {
		errorMessage := fmt.Errorf("error")
		mockServer.EXPECT().ListObjects(gomock.Any(), gomock.Any()).Return(nil, errorMessage)

		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})
		_, err := authorizer.ListAuthorizedStores(ctx)

		require.Error(t, err)
		require.Equal(t, errorMessage.Error(), err.Error())
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
		require.Equal(t, fmt.Sprintf("error authorizing the call: %s", errorMessage.Error()), err.Error())
	})
	t.Run("error_when_unauthorized", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, nil)

		err := authorizer.individualAuthorize(context.Background(), "client-id", CanCallCreateStore, "system", &openfgav1.ContextualTupleKeys{})

		require.Error(t, err)
		require.Equal(t, "rpc error: code = Code(1600) desc = the principal is not authorized to perform the action", err.Error())
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
		require.Equal(t, "rpc error: code = Code(1600) desc = the principal is not authorized to perform the action", err.Error())
	})

	t.Run("error_when_last_module_errors", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: true}, nil)
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: true}, nil)
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, nil)

		err := authorizer.moduleAuthorize(context.Background(), "client-id", CanCallWrite, "store-id", []string{"module1", "module2", "module3"})
		require.Error(t, err)
		require.Equal(t, "rpc error: code = Code(1600) desc = the principal is not authorized to perform the action", err.Error())
	})

	t.Run("error_when_all_modules_error", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, nil)
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, nil)
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, nil)

		err := authorizer.moduleAuthorize(context.Background(), "client-id", CanCallWrite, "store-id", []string{"module1", "module2", "module3"})
		require.Error(t, err)
		require.Equal(t, "rpc error: code = Code(1600) desc = the principal is not authorized to perform the action", err.Error())
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
