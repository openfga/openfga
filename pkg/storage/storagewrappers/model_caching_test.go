package storagewrappers

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestReadAuthorizationModel(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	mockController := gomock.NewController(t)
	mockController.Finish()

	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	cachingBackend, err := NewCachedOpenFGADatastore(mockDatastore, 5)
	require.NoError(t, err)
	t.Cleanup(cachingBackend.Close)
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
		mockDatastore.EXPECT().Close().Times(1),
	)

	err = cachingBackend.WriteAuthorizationModel(ctx, storeID, model)
	require.NoError(t, err)

	// Check that first hit to cache -> miss.
	gotModel, err := cachingBackend.ReadAuthorizationModel(ctx, storeID, model.GetId())
	require.NoError(t, err)
	require.Equal(t, model, gotModel)

	// Check what's stored inside the cache.
	modelKey := fmt.Sprintf("%s:%s", storeID, model.GetId())
	cachedModel := cachingBackend.cache.Get(modelKey)
	require.NotNil(t, cachedModel)
	require.Equal(t, model, cachedModel.AuthorizationModel)

	// Check that second hit to cache -> hit.
	gotModel, err = cachingBackend.ReadAuthorizationModel(ctx, storeID, model.GetId())
	require.NoError(t, err)
	require.Equal(t, model, gotModel)

	// ensure find latest authorization model will get hte latest model
	latestModel, err := cachingBackend.FindLatestAuthorizationModel(ctx, storeID)
	require.NoError(t, err)
	require.Equal(t, model, latestModel)
}

func TestSingleFlightFindLatestAuthorizationModel(t *testing.T) {
	const numGoroutines = 2

	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	mockController := gomock.NewController(t)
	mockController.Finish()

	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	cachingBackend, err := NewCachedOpenFGADatastore(mockDatastore, 5)
	require.NoError(t, err)
	t.Cleanup(cachingBackend.Close)
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
		mockDatastore.EXPECT().FindLatestAuthorizationModel(gomock.Any(), storeID).DoAndReturn(
			func(ctx context.Context, storeID string) (*openfgav1.AuthorizationModel, error) {
				time.Sleep(1 * time.Second)
				return model, nil
			}).Times(1),
		mockDatastore.EXPECT().Close().Times(1),
	)

	var wg errgroup.Group
	for i := 0; i < numGoroutines; i++ {
		wg.Go(func() error {
			latestModel, err := cachingBackend.FindLatestAuthorizationModel(context.Background(), storeID)
			if err != nil {
				return err
			}
			require.NoError(t, err)
			require.Equal(t, model, latestModel)
			return nil
		})
	}
	err = wg.Wait()
	require.NoError(t, err)
}
