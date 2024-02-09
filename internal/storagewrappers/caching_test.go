package storagewrappers

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"

	mockstorage "github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestReadAuthorizationModel(t *testing.T) {
	ctx := context.Background()
	memoryBackend := memory.New()
	defer memoryBackend.Close()
	cachingBackend := NewCachedOpenFGADatastore(memoryBackend, 5)
	defer cachingBackend.Close()

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

	err := memoryBackend.WriteAuthorizationModel(ctx, storeID, model)
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
}

func TestSingleFlightFindLatestAuthorizationModelID(t *testing.T) {
	const numGoroutines = 2

	mockController := gomock.NewController(t)
	defer mockController.Finish()
	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	expectedModelID := "expectedId"
	mockDatastore.EXPECT().FindLatestAuthorizationModelID(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, storeID string) (string, error) {
		time.Sleep(1 * time.Second)
		return expectedModelID, nil
	}).Times(1)
	mockDatastore.EXPECT().Close().Times(1)
	defer mockDatastore.Close()

	cachingBackend := NewCachedOpenFGADatastore(mockDatastore, 5)
	defer cachingBackend.Close()

	var wg errgroup.Group
	for i := 0; i < numGoroutines; i++ {
		wg.Go(func() error {
			id, err := cachingBackend.FindLatestAuthorizationModelID(context.Background(), "id")
			if err != nil {
				return err
			}
			if id != expectedModelID {
				return fmt.Errorf("expected model ID %s, actual %s", expectedModelID, id)
			}
			return nil
		})
	}
	err := wg.Wait()
	require.NoError(t, err)
}
