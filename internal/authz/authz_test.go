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
		require.Equal(t, errorMessage.Error(), err.Error())
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

	authorizer := NewAuthorizer(&Config{StoreID: "test-store", ModelID: "test-model"}, mockServer, logger.NewNoopLogger())

	t.Run("error_when_given_invalid_api_method", func(t *testing.T) {
		ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "test-client"})

		err := authorizer.Authorize(ctx, "store-id", "invalid-api-method")

		require.Error(t, err)
		require.Equal(t, ErrUnknownAPIMethod.Error(), err.Error())
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
