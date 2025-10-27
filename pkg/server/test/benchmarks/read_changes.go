package test

import (
	"context"
	"strconv"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
)

func BenchmarkReadChanges(b *testing.B, ds storage.OpenFGADatastore) {
	b.StopTimer()
	inputModel := `
				model
					schema 1.1
				type user
				type doc
					relations
						define viewer: [user]
			`
	ctx := context.Background()
	storeID := ulid.Make().String()
	model := testutils.MustTransformDSLToProtoWithID(inputModel)
	modelId, err := ds.WriteAuthorizationModel(context.Background(), storeID, model, "fakehash")
	require.NoError(b, err)
	require.Equal(b, modelId, model.GetId())

	const numTupleSet = 500
	for i := 0; i < numTupleSet; i++ {
		tuples := make([]*openfgav1.TupleKey, 40)
		for j := 0; j < len(tuples); j++ {
			tuples[j] = tuple.NewTupleKey("doc:"+strconv.Itoa(i)+"_"+strconv.Itoa(j), "viewer", "user:maria")
		}
		err := ds.Write(ctx, storeID, nil, tuples)
		require.NoError(b, err)
	}

	changesQuery := commands.NewReadChangesQuery(ds)
	b.StartTimer()
	b.Run("change_1st_page", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			response, err := changesQuery.Execute(ctx, &openfgav1.ReadChangesRequest{
				StoreId:  storeID,
				PageSize: wrapperspb.Int32(100),
			})

			require.NoError(b, err)
			require.NotEmpty(b, response.GetChanges())
		}
	})
	b.StopTimer()

	response, err := changesQuery.Execute(ctx, &openfgav1.ReadChangesRequest{
		StoreId:  storeID,
		PageSize: wrapperspb.Int32(100),
	})
	require.NoError(b, err)
	contToken := response.GetContinuationToken()
	b.StartTimer()

	b.Run("change_2nd_page", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			response, err := changesQuery.Execute(ctx, &openfgav1.ReadChangesRequest{
				StoreId:           storeID,
				PageSize:          wrapperspb.Int32(100),
				ContinuationToken: contToken,
			})

			require.NoError(b, err)
			require.NotEmpty(b, response.GetChanges())
		}
	})
}
