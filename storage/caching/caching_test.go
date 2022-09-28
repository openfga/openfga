package caching

import (
	"context"
	"fmt"
	"testing"

	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/storage/memory"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

const store = "openfga"

type readTypeDefinitionTest struct {
	_name                  string
	store                  string
	name                   string
	model                  *openfgapb.AuthorizationModel
	expectedTypeDefinition *openfgapb.TypeDefinition
}

var readTypeDefinitionTests = []readTypeDefinitionTest{
	{
		_name: "ShouldReturnTypeDefinitionFromInnerBackendAndSetItInCache",
		store: store,
		name:  "clients",
		model: &openfgapb.AuthorizationModel{
			Id:            id.Must(id.New()).String(),
			SchemaVersion: "1.0",
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "clients",
				},
			},
		},
		expectedTypeDefinition: &openfgapb.TypeDefinition{
			Type: "clients",
		},
	},
}

func TestReadTypeDefinition(t *testing.T) {
	for _, test := range readTypeDefinitionTests {
		ctx := context.Background()
		memoryBackend := memory.New(telemetry.NewNoopTracer(), 10000, 10000)
		cachingBackend := NewCachedOpenFGADatastore(memoryBackend, 5)

		err := memoryBackend.WriteAuthorizationModel(ctx, store, test.model)
		require.NoError(t, err)

		td, err := cachingBackend.ReadTypeDefinition(ctx, test.store, test.model.Id, test.name)
		require.NoError(t, err)
		require.Equal(t, test.expectedTypeDefinition, td)

		cacheKey := fmt.Sprintf("%s:%s:%s", test.store, test.model.Id, test.name)
		cachedTD := cachingBackend.cache.Get(cacheKey).Value().(*openfgapb.TypeDefinition)
		require.Equal(t, test.expectedTypeDefinition.GetType(), cachedTD.GetType())
	}
}
