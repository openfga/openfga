package typesystem

import (
	"context"
	"sync"
	"testing"
	"time"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/golang/mock/gomock"
	"github.com/oklog/ulid/v2"
	mockstorage "github.com/openfga/openfga/internal/mocks"
	"github.com/stretchr/testify/require"
	openfgav1 "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestMemoizedTypesystemResolverFunc(t *testing.T) {

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	storeID := ulid.Make().String()
	modelID1 := ulid.Make().String()
	modelID2 := ulid.Make().String()

	typedefs := parser.MustParse(`
	type user
	type document
	  relations
	    define viewer: [user] as self
	`)

	gomock.InOrder(
		mockDatastore.EXPECT().
			ReadAuthorizationModel(gomock.Any(), storeID, modelID1).
			Return(&openfgav1.AuthorizationModel{
				Id:              modelID1,
				SchemaVersion:   SchemaVersion1_1,
				TypeDefinitions: typedefs,
			}, nil),

		mockDatastore.EXPECT().
			FindLatestAuthorizationModelID(gomock.Any(), storeID).
			Return(modelID2, nil),

		mockDatastore.EXPECT().
			ReadAuthorizationModel(gomock.Any(), storeID, modelID2).
			Return(&openfgav1.AuthorizationModel{
				Id:              modelID2,
				SchemaVersion:   SchemaVersion1_1,
				TypeDefinitions: typedefs,
			}, nil),
	)

	resolver := MemoizedTypesystemResolverFunc(
		mockDatastore,
	)

	typesys, err := resolver(context.Background(), storeID, modelID1)
	require.NoError(t, err)
	require.NotNil(t, typesys)

	relation, err := typesys.GetRelation("document", "viewer")
	require.NoError(t, err)
	require.NotNil(t, relation)

	typesys, err = resolver(context.Background(), storeID, "")
	require.NoError(t, err)
	require.NotNil(t, typesys)
	require.Equal(t, modelID2, typesys.GetAuthorizationModelID())

	relation, err = typesys.GetRelation("document", "viewer")
	require.NoError(t, err)
	require.NotNil(t, relation)
}

func TestSingleFlightMemoizedTypesystemResolverFunc(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	gomock.InOrder(
		mockDatastore.EXPECT().
			FindLatestAuthorizationModelID(gomock.Any(), storeID).
			DoAndReturn(func(ctx context.Context, storeID string) (string, error) {
				time.Sleep(1 * time.Second)
				return modelID, nil
			}).
			Times(1),

		mockDatastore.EXPECT().
			ReadAuthorizationModel(gomock.Any(), storeID, modelID).
			Return(&openfgav1.AuthorizationModel{
				Id:            modelID,
				SchemaVersion: SchemaVersion1_1,
			}, nil).Times(1),
	)

	resolver := MemoizedTypesystemResolverFunc(
		mockDatastore,
	)

	var wg sync.WaitGroup
	wg.Add(2)

	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			typesys, err := resolver(context.Background(), storeID, "")
			require.NoError(t, err)
			require.Equal(t, modelID, typesys.GetAuthorizationModelID())
		}()
	}

	wg.Wait()
}
