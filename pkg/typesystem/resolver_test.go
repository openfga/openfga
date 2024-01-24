package typesystem

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	mockstorage "github.com/openfga/openfga/internal/mocks"
)

func TestMemoizedTypesystemResolverFunc(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	storeID := ulid.Make().String()
	modelID1 := ulid.Make().String()
	modelID2 := ulid.Make().String()

	typedefs := parser.MustTransformDSLToProto(`model
  schema 1.1
type user
type document
  relations
	define viewer: [user]`).TypeDefinitions

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

	resolver, resolverStop := MemoizedTypesystemResolverFunc(
		mockDatastore,
	)
	defer resolverStop()

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
	const numGoroutines = 2

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
			}, nil).MinTimes(1).MaxTimes(numGoroutines),
	)

	resolver, resolverStop := MemoizedTypesystemResolverFunc(
		mockDatastore,
	)
	defer resolverStop()

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			typesys, err := resolver(context.Background(), storeID, "")
			require.NoError(t, err)
			require.Equal(t, modelID, typesys.GetAuthorizationModelID())
		}()
	}

	wg.Wait()
}
