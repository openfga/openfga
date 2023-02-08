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

	storeID := ulid.Make().String()
	objectType := "documents"
	typeDefinition := &openfgapb.TypeDefinition{Type: objectType}

	model := &openfgapb.AuthorizationModel{
		Id:              ulid.Make().String(),
		SchemaVersion:   typesystem.SchemaVersion1_0,
		TypeDefinitions: []*openfgapb.TypeDefinition{typeDefinition},
	}

	err := memoryBackend.WriteAuthorizationModel(ctx, storeID, model)
	require.NoError(t, err)

	gotModel, err := cachingBackend.ReadAuthorizationModel(ctx, storeID, model.Id)
	require.NoError(t, err)
	require.Equal(t, model, gotModel)

	modelKey := fmt.Sprintf("%s:%s", storeID, model.Id)
	cachedModel := cachingBackend.cache.Get(modelKey).Value().(*openfgapb.AuthorizationModel)
	require.Equal(t, model, cachedModel)

	gotTypeDef, err := cachingBackend.ReadTypeDefinition(ctx, storeID, model.Id, objectType)
	require.NoError(t, err)
	require.Equal(t, typeDefinition, gotTypeDef)

	typeDefKey := fmt.Sprintf("%s:%s:%s", storeID, model.Id, objectType)
	cachedTypeDef := cachingBackend.cache.Get(typeDefKey).Value().(*openfgapb.TypeDefinition)
	require.Equal(t, typeDefinition, cachedTypeDef)

}
