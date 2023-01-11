package postgres

import (
	"context"
	"testing"

	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/storage/sql"
	"github.com/openfga/openfga/storage/test"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/proto"
)

func TestPostgresDatastore(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI()
	ds, err := New(uri, sql.NewConfig())
	require.NoError(t, err)
	defer ds.Close()
	test.RunAllTests(t, ds)
}

func TestReadAuthorizationModelPostgresSpecificCases(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI()
	ds, err := New(uri, sql.NewConfig())
	require.NoError(t, err)

	ctx := context.Background()
	defer ds.Close()
	store := "store"
	modelID := "foo"
	schemaVersion := "7.8"

	bytes, err := proto.Marshal(&openfgapb.TypeDefinition{Type: "document"})
	require.NoError(t, err)

	_, err = ds.db.ExecContext(ctx, "INSERT INTO authorization_model (store, authorization_model_id, schema_version, type, type_definition) VALUES ($1, $2, $3, $4, $5)", store, modelID, schemaVersion, "document", bytes)
	require.NoError(t, err)

	model, err := ds.ReadAuthorizationModel(ctx, store, modelID)
	require.NoError(t, err)
	require.Equal(t, typesystem.SchemaVersion1_0, model.SchemaVersion)
}
