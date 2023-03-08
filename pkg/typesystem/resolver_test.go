package typesystem

import (
	"context"
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/golang/mock/gomock"
	"github.com/oklog/ulid/v2"
	mockstorage "github.com/openfga/openfga/pkg/storage/mocks"
	"github.com/stretchr/testify/require"
	openfgav1 "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestMemoizedTypesystemResolverFunc(t *testing.T) {

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	typedefs := parser.MustParse(`
	type user
	type document
	  relations
	    define viewer: [user] as self
	`)

	mockDatastore.EXPECT().
		ReadAuthorizationModel(gomock.Any(), storeID, modelID).
		Return(&openfgav1.AuthorizationModel{
			SchemaVersion:   SchemaVersion1_1,
			TypeDefinitions: typedefs,
		}, nil)

	resolver := MemoizedTypesystemResolverFunc(
		NewTypesystemResolver(mockDatastore),
	)

	typesys, err := resolver(context.Background(), storeID, modelID)
	require.NoError(t, err)
	require.NotNil(t, typesys)

	relation, err := typesys.GetRelation("document", "viewer")
	require.NoError(t, err)
	require.NotNil(t, relation)
}
