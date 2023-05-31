package caching

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestCache(t *testing.T) {
	ctx := context.Background()
	memoryBackend := memory.New(10000, 10000)
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
