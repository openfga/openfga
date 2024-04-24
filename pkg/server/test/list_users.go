package test

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/server/commands/listusers"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// setupListUsersBenchmark writes the model and lots of tuples
func setupListUsersBenchmark(b *testing.B, ds storage.OpenFGADatastore, storeID string) (*typesystem.TypeSystem, int) {
	b.Helper()
	model := testutils.MustTransformDSLToProtoWithID(
		`model
			schema 1.1
		type user
		type folder
			relations
				define viewer: [user]
		type document
			relations
				define viewer: [user, user with condTrue]
				define parent: [folder]
				define can_view: viewer or viewer from parent
				define can_view_conditional: viewer or viewer from parent
				define blocked: [user]
				define public_but_not: [user:*] but not blocked

		condition condTrue(param: bool) {
			param == true
		}`)
	typeSystem, err := typesystem.NewAndValidate(context.Background(), model)
	require.NoError(b, err)
	err = ds.WriteAuthorizationModel(context.Background(), storeID, model)
	require.NoError(b, err)

	numberOfUsersWithAccess := 0
	for i := 0; i < 100; i++ {
		var tuples []*openfgav1.TupleKey

		for j := 0; j < ds.MaxTuplesPerWrite(); j++ {
			user := fmt.Sprintf("user:%s", strconv.Itoa(numberOfUsersWithAccess))

			// one document viewable by many users
			tuples = append(tuples, tuple.NewTupleKey("document:1", "viewer", user))
			// one document viewable by many users with a condition
			tuples = append(tuples, tuple.NewTupleKeyWithCondition("document:2", "viewer", user, "condTrue", nil))

			numberOfUsersWithAccess += 1
		}

		err = ds.Write(context.Background(), storeID, nil, tuples)
		require.NoError(b, err)
	}
	// and one public document
	err = ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:3", "public_but_not", "user:*"),
	})
	require.NoError(b, err)

	return typeSystem, numberOfUsersWithAccess
}

func BenchmarkListUsers(b *testing.B, ds storage.OpenFGADatastore) {
	ctx := context.Background()
	storeID := ulid.Make().String()

	ts, numberOfUsersWithAccess := setupListUsersBenchmark(b, ds, storeID)
	ctx = typesystem.ContextWithTypesystem(ctx, ts)

	var oneResultIterations, allResultsIterations int

	b.Run("oneResult_without_conditions", func(b *testing.B) {
		oneResultIterations = int(math.Inf(1))
		// TODO add benchmark for when max_results = 1, use the same model and tuples as "allResults"
		// so that we assert that we don't waste time computing extra results in that case
		b.Skip("unimplemented")
	})

	b.Run("allResults_without_conditions", func(b *testing.B) {
		b.ResetTimer()
		listUsersQuery := listusers.NewListUsersQuery(ds,
			listusers.WithListUserMaxResults(uint32(numberOfUsersWithAccess)))
		for i := 0; i < b.N; i++ {
			resp, err := listUsersQuery.ListUsers(ctx, &openfgav1.ListUsersRequest{
				StoreId:              storeID,
				AuthorizationModelId: ts.GetAuthorizationModelID(),
				Object:               &openfgav1.Object{Type: "document", Id: "1"},
				Relation:             "can_view",
				UserFilters:          []*openfgav1.ListUsersFilter{{Type: "user"}},
			})
			require.NoError(b, err)
			require.Len(b, resp.GetUsers(), numberOfUsersWithAccess)
		}
		allResultsIterations = b.N
	})

	require.Greater(b, oneResultIterations, allResultsIterations)

	b.Run("allResults_with_conditions", func(b *testing.B) {
		listUsersQuery := listusers.NewListUsersQuery(ds,
			listusers.WithListUserMaxResults(uint32(numberOfUsersWithAccess)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resp, err := listUsersQuery.ListUsers(ctx, &openfgav1.ListUsersRequest{
				StoreId:              storeID,
				AuthorizationModelId: ts.GetAuthorizationModelID(),
				Object:               &openfgav1.Object{Type: "document", Id: "2"},
				Relation:             "can_view_conditional",
				UserFilters:          []*openfgav1.ListUsersFilter{{Type: "user"}},
				Context:              testutils.MustNewStruct(b, map[string]interface{}{"param": true}),
			})
			require.NoError(b, err)
			require.Len(b, resp.GetUsers(), numberOfUsersWithAccess)
		}
	})

	b.Run("exclusion_without_conditions", func(b *testing.B) {
		listUsersQuery := listusers.NewListUsersQuery(ds,
			listusers.WithListUserMaxResults(uint32(numberOfUsersWithAccess)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resp, err := listUsersQuery.ListUsers(ctx, &openfgav1.ListUsersRequest{
				StoreId:              storeID,
				AuthorizationModelId: ts.GetAuthorizationModelID(),
				Object:               &openfgav1.Object{Type: "document", Id: "3"},
				Relation:             "public_but_not",
				UserFilters:          []*openfgav1.ListUsersFilter{{Type: "user"}},
			})
			require.NoError(b, err)
			require.Len(b, resp.GetUsers(), 1)
		}
	})
}
