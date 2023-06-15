package util

import (
	"context"
	"fmt"
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestValidationResult(t *testing.T) {
	engines := []string{"postgres", "mysql"}

	totalStores := 200
	totalModelsForOneStore := 200

	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			_, ds, _, err := MustBootstrapDatastore(t, engine)
			require.NoError(t, err)

			ctx := context.Background()

			// write a bunch of stores (to trigger pagination)
			var storeID string
			for i := 0; i < totalStores; i++ {
				storeID = ulid.Make().String()
				_, err := ds.CreateStore(ctx, &openfgapb.Store{
					Id:   storeID,
					Name: fmt.Sprintf("store-%d", i),
				})
				require.NoError(t, err)
				t.Logf("added store %s\n", storeID)
			}

			// for the last store, write a bunch of models (to trigger pagination)
			for j := 0; j < totalModelsForOneStore; j++ {
				modelID := ulid.Make().String()
				err = ds.WriteAuthorizationModel(ctx, storeID, &openfgapb.AuthorizationModel{
					Id:            modelID,
					SchemaVersion: typesystem.SchemaVersion1_1,
					// invalid
					TypeDefinitions: parser.MustParse(`
									type document
									  relations
										define viewer:[user] as self
									`),
				})
				require.NoError(t, err)
				t.Logf("added model %s for store %s\n", modelID, storeID)
			}

			t.Run("validate returns success", func(t *testing.T) {
				validationResults, err := ValidateAllAuthorizationModels(ctx, ds)
				require.NoError(t, err)
				require.Equal(t, totalModelsForOneStore, len(validationResults))
				require.Contains(t, "the relation type 'user' on 'viewer' in object type 'document' is not valid", validationResults[0].Error)
				require.Equal(t, true, validationResults[0].IsLatestModel)

				require.Contains(t, "the relation type 'user' on 'viewer' in object type 'document' is not valid", validationResults[1].Error)
				require.Equal(t, false, validationResults[1].IsLatestModel)
			})
		})
	}
}
