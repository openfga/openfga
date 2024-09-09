package authz

import (
	"context"
	"fmt"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/logger"
)

func TestGetRelation(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockServer := mocks.NewMockServerInterface(mockController)

	authorizer := NewAuthorizer(&Config{StoreID: "test-store", ModelID: "test-model"}, mockServer, logger.NewNoopLogger())

	t.Run("ReadAuthorizationModel", func(t *testing.T) {
		result, err := authorizer.getRelation(ReadAuthorizationModel)
		require.NoError(t, err)
		require.Equal(t, CanCallReadAuthorizationModels, result)
	})

	t.Run("ReadAuthorizationModels", func(t *testing.T) {
		result, err := authorizer.getRelation(ReadAuthorizationModels)
		require.NoError(t, err)
		require.Equal(t, CanCallReadAuthorizationModels, result)
	})

	t.Run("Read", func(t *testing.T) {
		result, err := authorizer.getRelation(Read)
		require.NoError(t, err)
		require.Equal(t, CanCallRead, result)
	})

	t.Run("Write", func(t *testing.T) {
		result, err := authorizer.getRelation(Write)
		require.NoError(t, err)
		require.Equal(t, CanCallWrite, result)
	})

	t.Run("ListObjects", func(t *testing.T) {
		result, err := authorizer.getRelation(ListObjects)
		require.NoError(t, err)
		require.Equal(t, CanCallListObjects, result)
	})

	t.Run("StreamedListObjects", func(t *testing.T) {
		result, err := authorizer.getRelation(StreamedListObjects)
		require.NoError(t, err)
		require.Equal(t, CanCallListObjects, result)
	})

	t.Run("Check", func(t *testing.T) {
		result, err := authorizer.getRelation(Check)
		require.NoError(t, err)
		require.Equal(t, CanCallCheck, result)
	})

	t.Run("ListUsers", func(t *testing.T) {
		result, err := authorizer.getRelation(ListUsers)
		require.NoError(t, err)
		require.Equal(t, CanCallListUsers, result)
	})

	t.Run("WriteAssertions", func(t *testing.T) {
		result, err := authorizer.getRelation(WriteAssertions)
		require.NoError(t, err)
		require.Equal(t, CanCallWriteAssertions, result)
	})

	t.Run("ReadAssertions", func(t *testing.T) {
		result, err := authorizer.getRelation(ReadAssertions)
		require.NoError(t, err)
		require.Equal(t, CanCallReadAssertions, result)
	})

	t.Run("WriteAuthorizationModel", func(t *testing.T) {
		result, err := authorizer.getRelation(WriteAuthorizationModel)
		require.NoError(t, err)
		require.Equal(t, CanCallWriteAuthorizationModels, result)
	})

	t.Run("CreateStore", func(t *testing.T) {
		result, err := authorizer.getRelation(CreateStore)
		require.NoError(t, err)
		require.Equal(t, CanCallCreateStore, result)
	})

	t.Run("GetStore", func(t *testing.T) {
		result, err := authorizer.getRelation(GetStore)
		require.NoError(t, err)
		require.Equal(t, CanCallGetStore, result)
	})

	t.Run("DeleteStore", func(t *testing.T) {
		result, err := authorizer.getRelation(DeleteStore)
		require.NoError(t, err)
		require.Equal(t, CanCallDeleteStore, result)
	})

	t.Run("Expand", func(t *testing.T) {
		result, err := authorizer.getRelation(Expand)
		require.NoError(t, err)
		require.Equal(t, CanCallExpand, result)
	})

	t.Run("ReadChanges", func(t *testing.T) {
		result, err := authorizer.getRelation(ReadChanges)
		require.NoError(t, err)
		require.Equal(t, CanCallReadChanges, result)
	})

	t.Run("Unknown", func(t *testing.T) {
		_, err := authorizer.getRelation("unknown")
		require.Error(t, err)
		require.Equal(t, "unknown api method: unknown", err.Error())
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

		err := authorizer.AuthorizeCreateStore(context.Background(), "test-client")
		require.Error(t, err)
		require.Equal(t, errorMessage.Error(), err.Error())
	})

	t.Run("error_when_not_authorized", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, nil)

		err := authorizer.AuthorizeCreateStore(context.Background(), "test-client")
		require.Error(t, err)
	})

	t.Run("succeed", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: true}, nil)

		err := authorizer.AuthorizeCreateStore(context.Background(), "test-client")
		require.NoError(t, err)
	})
}

func TestAuthorize(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockServer := mocks.NewMockServerInterface(mockController)

	authorizer := NewAuthorizer(&Config{StoreID: "test-store", ModelID: "test-model"}, mockServer, logger.NewNoopLogger())

	t.Run("error_when_given_invalid_api_method", func(t *testing.T) {
		err := authorizer.Authorize(context.Background(), "client-id", "store-id", "invalid-api-method")
		require.Error(t, err)
		require.Equal(t, "unknown api method: invalid-api-method", err.Error())
	})

	t.Run("error_when_check_errors", func(t *testing.T) {
		errorMessage := fmt.Errorf("error")
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, errorMessage)
		err := authorizer.Authorize(context.Background(), "client-id", "store-id", CreateStore)
		require.Error(t, err)
		require.Equal(t, errorMessage.Error(), err.Error())
	})

	t.Run("error_when_unauthorized", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, nil)
		err := authorizer.Authorize(context.Background(), "client-id", "store-id", CreateStore)
		require.Error(t, err)
	})

	t.Run("succeed", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: true}, nil)
		err := authorizer.Authorize(context.Background(), "client-id", "store-id", CreateStore)
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
		valid, err := authorizer.individualAuthorize(context.Background(), "client-id", CanCallCreateStore, "system", &openfgav1.ContextualTupleKeys{})
		require.Error(t, err)
		require.Equal(t, errorMessage.Error(), err.Error())
		require.False(t, valid)
	})
	t.Run("return_false_when_unauthorized", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, nil)
		valid, err := authorizer.individualAuthorize(context.Background(), "client-id", CanCallCreateStore, "system", &openfgav1.ContextualTupleKeys{})
		require.NoError(t, err)
		require.False(t, valid)
	})
	t.Run("succeed", func(t *testing.T) {
		mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: true}, nil)
		valid, err := authorizer.individualAuthorize(context.Background(), "client-id", CanCallCreateStore, "system", &openfgav1.ContextualTupleKeys{})
		require.NoError(t, err)
		require.True(t, valid)
	})
}

func TestGetStore(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockServer := mocks.NewMockServerInterface(mockController)
	authorizer := NewAuthorizer(&Config{StoreID: "test-store", ModelID: "test-model"}, mockServer, logger.NewNoopLogger())

	storeID := "test-store"
	store := authorizer.getStore(storeID)

	require.Equal(t, "store:test-store", store)
}

func TestGetApplication(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockServer := mocks.NewMockServerInterface(mockController)
	authorizer := NewAuthorizer(&Config{StoreID: "test-store", ModelID: "test-model"}, mockServer, logger.NewNoopLogger())

	clientID := "test-client"
	application := authorizer.getApplication(clientID)

	require.Equal(t, "application:test-client", application)
}

func TestGetSystem(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockServer := mocks.NewMockServerInterface(mockController)
	authorizer := NewAuthorizer(&Config{StoreID: "test-store", ModelID: "test-model"}, mockServer, logger.NewNoopLogger())

	system := authorizer.getSystem()

	require.Equal(t, "system:fga", system)
}
