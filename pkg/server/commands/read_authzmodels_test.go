package commands

import (
	"context"
	"errors"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/wrapperspb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestReadAuthorizationModelsQuery(t *testing.T) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()
	models := []*openfgav1.AuthorizationModel{
		{
			Id:              modelID,
			SchemaVersion:   typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgav1.TypeDefinition{{Type: "folder"}},
			Conditions: map[string]*openfgav1.Condition{
				"condx": {
					Name: "condx",
					Parameters: map[string]*openfgav1.ConditionParamTypeRef{
						"x": {TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_INT},
					},
					Expression: "x < 100",
				},
			},
		},
	}

	t.Run("success", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		mockDatastore.EXPECT().
			ReadAuthorizationModels(gomock.Any(), storeID, storage.ReadAuthorizationModelsOptions{
				Pagination: storage.PaginationOptions{
					PageSize: 1,
					From:     "",
				},
			}).
			Return([]*openfgav1.AuthorizationModel{models[0]}, "", nil)

		resp, err := NewReadAuthorizationModelsQuery(mockDatastore).
			Execute(context.Background(), &openfgav1.ReadAuthorizationModelsRequest{
				StoreId:           storeID,
				PageSize:          wrapperspb.Int32(1),
				ContinuationToken: "",
			})
		require.NoError(t, err)
		require.Len(t, resp.GetAuthorizationModels(), 1)
		require.Equal(t, modelID, resp.GetAuthorizationModels()[0].GetId())
		require.NotEmpty(t, resp.GetAuthorizationModels()[0].GetSchemaVersion())
		require.NotEmpty(t, resp.GetAuthorizationModels()[0].GetTypeDefinitions())
		require.NotEmpty(t, resp.GetAuthorizationModels()[0].GetConditions())
		require.Empty(t, resp.GetContinuationToken())
	})

	t.Run("error_decoding_token", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		mockEncoder := mocks.NewMockEncoder(mockController)
		mockEncoder.EXPECT().Decode(gomock.Any()).Return(nil, errors.New("error"))

		cmd := NewReadAuthorizationModelsQuery(mockDatastore)
		resp, actualError := cmd.Execute(context.Background(), &openfgav1.ReadAuthorizationModelsRequest{
			StoreId:           storeID,
			PageSize:          wrapperspb.Int32(1),
			ContinuationToken: "",
		})
		require.Nil(t, resp)
		require.ErrorIs(t, actualError, serverErrors.ErrInvalidContinuationToken)
	})

	t.Run("error_encoding_token", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		mockEncoder := mocks.NewMockEncoder(mockController)
		gomock.InOrder(
			mockEncoder.EXPECT().Decode(gomock.Any()).Return([]byte{}, nil),
			mockDatastore.EXPECT().ReadAuthorizationModels(gomock.Any(), gomock.Any(), gomock.Any()).Return([]*openfgav1.AuthorizationModel{models[0]}, "cont-token", nil),
			mockEncoder.EXPECT().Encode(gomock.Any()).Return("", errors.New("error")),
		)

		cmd := NewReadAuthorizationModelsQuery(mockDatastore)
		resp, actualError := cmd.Execute(context.Background(), &openfgav1.ReadAuthorizationModelsRequest{
			StoreId:           storeID,
			PageSize:          wrapperspb.Int32(1),
			ContinuationToken: "",
		})
		require.Nil(t, resp)
		require.Error(t, actualError)
	})

	t.Run("error_from_datastore", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		mockDatastore.EXPECT().ReadAuthorizationModels(gomock.Any(), storeID, gomock.Any()).Return(nil, "", errors.New("internal"))

		cmd := NewReadAuthorizationModelsQuery(mockDatastore)
		resp, err := cmd.Execute(context.Background(), &openfgav1.ReadAuthorizationModelsRequest{
			StoreId:           storeID,
			PageSize:          wrapperspb.Int32(1),
			ContinuationToken: "",
		})
		require.Nil(t, resp)
		require.Error(t, err)
	})
}
