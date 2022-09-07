package caching

import (
	"context"
	"strings"
	"testing"

	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/storage/memory"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel"
)

const store = "openfga"

type readTypeDefinitionTest struct {
	_name                  string
	store                  string
	name                   string
	dbState                []*openfgapb.TypeDefinition
	expectedTypeDefinition *openfgapb.TypeDefinition
	expectedError          error
}

var readTypeDefinitionTests = []readTypeDefinitionTest{
	{
		_name: "ShouldReturnTypeDefinitionFromInnerBackendAndSetItInCache",
		store: store,
		name:  "clients",
		dbState: []*openfgapb.TypeDefinition{
			{
				Type: "clients",
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
		memoryBackend := memory.New(otel.Tracer("noop"), 10000, 10000)
		cachingBackend := NewCachedOpenFGADatastore(memoryBackend, 5)

		modelID, err := id.NewString()
		require.NoError(t, err)

		err = memoryBackend.WriteAuthorizationModel(ctx, store, modelID, test.dbState)
		require.NoError(t, err)

		td, actualError := cachingBackend.ReadTypeDefinition(ctx, test.store, modelID, test.name)

		if test.expectedError != nil && test.expectedError != actualError {
			t.Errorf("[%s] Expected error '%s', actual '%s'", test._name, test.expectedError, actualError)
			continue
		}

		if test.expectedTypeDefinition != nil {
			if td == nil {
				t.Errorf("[%s] Expected authorizationmodel to not be nil, actual nil", test._name)
				continue
			}

			if test.expectedTypeDefinition.GetType() != td.GetType() {
				t.Errorf("[%s] Expected name to be '%s', actual '%s'", test._name, test.expectedTypeDefinition.GetType(), td.GetType())
				continue
			}

			cacheKey := strings.Join([]string{test.store, modelID, test.name}, Separator)
			cachedEntry := cachingBackend.cache.Get(cacheKey)

			if cachedEntry == nil {
				t.Errorf("[%s] Expected entry '%s' to be in cache but it wasn't", test._name, cacheKey)
				continue
			}

			cachedNS := cachedEntry.Value().(*openfgapb.TypeDefinition)

			if test.expectedTypeDefinition.GetType() != cachedNS.GetType() {
				t.Errorf("[%s] Expected cached name to be '%s', actual '%s'", test._name, test.expectedTypeDefinition.GetType(), cachedNS.GetType())
				continue
			}
		}
	}
}
