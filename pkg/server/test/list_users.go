package test

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/server/commands/listusers"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
)

// setupListUsersBenchmark writes the model and lots of tuples
func setupListUsersBenchmark(b *testing.B, ds storage.OpenFGADatastore, storeID string) (*openfgav1.AuthorizationModel, int) {
	b.Helper()
	// this model exercises many execution paths: union, computed usersets, tuple to userset, and direct relationships.
	model := testutils.MustTransformDSLToProtoWithID(
		`model
			schema 1.1
		type user
		type folder
			relations
				define viewer: [user]
		type document
			relations
				define viewer: [user]
				define parent: [folder]
				define can_view: viewer or viewer from parent`)
	err := ds.WriteAuthorizationModel(context.Background(), storeID, model)
	require.NoError(b, err)

	numberOfUsersWithAccess := 0
	for i := 0; i < 100; i++ {
		var tuples []*openfgav1.TupleKey

		for j := 0; j < ds.MaxTuplesPerWrite(); j++ {
			user := fmt.Sprintf("user:%s", strconv.Itoa(numberOfUsersWithAccess))

			tuples = append(tuples, tuple.NewTupleKey("document:1", "viewer", user))

			numberOfUsersWithAccess += 1
		}

		err := ds.Write(context.Background(), storeID, nil, tuples)
		require.NoError(b, err)
	}

	return model, numberOfUsersWithAccess
}

func BenchmarkListUsers(b *testing.B, ds storage.OpenFGADatastore) {
	ctx := context.Background()
	store := ulid.Make().String()

	model, numberOfUsersWithAccess := setupListUsersBenchmark(b, ds, store)

	req := &openfgav1.ListUsersRequest{
		StoreId:              store,
		AuthorizationModelId: model.GetId(),
		Object:               &openfgav1.Object{Type: "document", Id: "1"},
		Relation:             "can_view",
		UserFilters:          []*openfgav1.ListUsersFilter{{Type: "user"}},
	}

	// TODO add benchmark for when max_results = 1
	// so that we assert that we don't waste time computing extra results in that case
	oneResultIterations := int(math.Inf(1))
	var allResultsIterations int

	b.Run("allResults", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ctx = typesystem.ContextWithTypesystem(ctx, typesystem.New(model))

			listUsersQuery := listusers.NewListUsersQuery(ds,
				listusers.WithListUserMaxResults(uint32(numberOfUsersWithAccess)))

			resp, err := listUsersQuery.ListUsers(ctx, req)
			require.NoError(b, err)
			require.Len(b, resp.GetUsers(), numberOfUsersWithAccess)
		}
		allResultsIterations = b.N
	})

	require.Greater(b, oneResultIterations, allResultsIterations)
}
