package storagewrappers

import (
	"context"
	"fmt"

	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestReadAuthorizationModel(t *testing.T) {
	ctx := context.Background()
	mockController := gomock.NewController(t)
	mockController.Finish()

	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	cachingBackend := NewCachedOpenFGADatastore(mockDatastore, 5)
	model := &openfgav1.AuthorizationModel{
		Id:            ulid.Make().String(),
		SchemaVersion: typesystem.SchemaVersion1_1,
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{
				Type: "documents",
				Relations: map[string]*openfgav1.Userset{
					"admin": typesystem.This(),
				},
			},
		},
	}
	storeID := ulid.Make().String()
	gomock.InOrder(
		mockDatastore.EXPECT().WriteAuthorizationModel(gomock.Any(), storeID, gomock.Any()).Times(1).Return(nil),
		mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, model.GetId()).Times(1).Return(model, nil),
		mockDatastore.EXPECT().FindLatestAuthorizationModel(gomock.Any(), storeID).Times(1).Return(model, nil),
	)

	err := cachingBackend.WriteAuthorizationModel(ctx, storeID, model)
	require.NoError(t, err)

	// Check that first hit to cache -> miss.
	gotModel, err := cachingBackend.ReadAuthorizationModel(ctx, storeID, model.Id)
	require.NoError(t, err)
	require.Equal(t, model, gotModel)

	// Check what's stored inside the cache.
	modelKey := fmt.Sprintf("%s:%s", storeID, model.Id)
	cachedModel := cachingBackend.cache.Get(modelKey).Value()
	require.Equal(t, model, cachedModel)

	// Check that second hit to cache -> hit.
	gotModel, err = cachingBackend.ReadAuthorizationModel(ctx, storeID, model.Id)
	require.NoError(t, err)
	require.Equal(t, model, gotModel)

	// ensure find latest authorization model will get hte latest model
	latestModel, err := cachingBackend.FindLatestAuthorizationModel(ctx, storeID)
	require.NoError(t, err)
	require.Equal(t, model, latestModel)
}
