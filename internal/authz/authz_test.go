package authz

import (
	"context"
	"fmt"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/authclaims"
	"github.com/openfga/openfga/pkg/logger"
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
		require.Equal(t, errorMessage.Error(), err.Error())
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

func TestAuthorize(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockServer := mocks.NewMockServerInterface(mockController)

	authorizer := NewAuthorizer(&Config{StoreID: "test-store", ModelID: "test-model"}, mockServer, logger.NewNoopLogger())

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
		require.Equal(t, errorMessage.Error(), err.Error())
	})

	t.Run("error_when_check_errors", func(t *testing.T) {
		errorMessage := fmt.Errorf("error")
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, errorMessage)

		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})
		err := authorizer.Authorize(ctx, "store-id", CreateStore)

		require.Error(t, err)
		require.Equal(t, errorMessage.Error(), err.Error())
	})

	t.Run("error_when_unauthorized", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, nil)
		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})

		err := authorizer.Authorize(ctx, "store-id", CreateStore)

		require.Error(t, err)
		require.Equal(t, "rpc error: code = Code(403) desc = the principal is not authorized to perform the action", err.Error())
	})

	t.Run("succeed", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: true}, nil)
		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})

		err := authorizer.Authorize(ctx, "store-id", CreateStore)

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
		require.Equal(t, errorMessage.Error(), err.Error())
	})
	t.Run("error_when_unauthorized", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, nil)

		err := authorizer.individualAuthorize(context.Background(), "client-id", CanCallCreateStore, "system", &openfgav1.ContextualTupleKeys{})

		require.Error(t, err)
		require.Equal(t, "rpc error: code = Code(403) desc = the principal is not authorized to perform the action", err.Error())
	})
	t.Run("succeed", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: true}, nil)

		err := authorizer.individualAuthorize(context.Background(), "client-id", CanCallCreateStore, "system", &openfgav1.ContextualTupleKeys{})

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
	typesys := typesystem.New(model)

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
