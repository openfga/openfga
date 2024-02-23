package storagewrappers

import (
	"context"
	"fmt"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReadAuthorizationModel(t *testing.T) {
	ctx := context.Background()
	memoryBackend := memory.New()
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

	// ensure find latest authorization model will get hte latest model
	latestModel, err := cachingBackend.FindLatestAuthorizationModel(ctx, storeID)
	require.NoError(t, err)
	require.Equal(t, model, latestModel)
}
