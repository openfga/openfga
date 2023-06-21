package caching

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/storage/memory"
	mockstorage "github.com/openfga/openfga/pkg/storage/mocks"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestReadAuthorizationModel(t *testing.T) {
	ctx := context.Background()
	memoryBackend := memory.New()
	cachingBackend := NewCachedOpenFGADatastore(memoryBackend, 5)
	defer cachingBackend.Close()

	model := &openfgapb.AuthorizationModel{
		Id:            ulid.Make().String(),
		SchemaVersion: typesystem.SchemaVersion1_1,
		TypeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "documents",
				Relations: map[string]*openfgapb.Userset{
					"admin": typesystem.This(),
				},
			},
		},
	}
	storeID := ulid.Make().String()

	err := memoryBackend.WriteAuthorizationModel(ctx, storeID, model)
	require.NoError(t, err)

	// check that first hit to cache -> miss
	gotModel, err := cachingBackend.ReadAuthorizationModel(ctx, storeID, model.Id)
	require.NoError(t, err)
	require.Equal(t, model, gotModel)

	// check what's stored inside the cache
	modelKey := fmt.Sprintf("%s:%s", storeID, model.Id)
	cachedModel := cachingBackend.cache.Get(modelKey).Value()
	require.Equal(t, model, cachedModel)

	// check that second hit to cache -> hit
	gotModel, err = cachingBackend.ReadAuthorizationModel(ctx, storeID, model.Id)
	require.NoError(t, err)
	require.Equal(t, model, gotModel)
}

func TestFindLatestAuthorizationModelID(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()
	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	expectedId := "expectedId"
	mockDatastore.EXPECT().FindLatestAuthorizationModelID(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, storeID string) (string, error) {
		time.Sleep(1 * time.Second)
		return expectedId, nil
	}).Times(1)
	cachingBackend := NewCachedOpenFGADatastore(mockDatastore, 5)
	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			id, err := cachingBackend.FindLatestAuthorizationModelID(context.Background(), "id")
			require.NoError(t, err)
			require.Equal(t, expectedId, id)
		}()
	}
	wg.Wait()
}
