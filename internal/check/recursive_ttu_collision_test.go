package check

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/internal/planner"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
)

// TestRecursiveTTUVisitedCollision reproduces a false-negative caused by two
// distinct recursive TTU relations ("billing_user from parent" and
// "full_admin from parent") sharing the same `visited` map. Both branches read
// the same physical parent tuples (organization#parent) and the dedup filter
// keys only on the parent object (the tuple's user, e.g. "organization:o2"),
// with no computed relation. Whichever branch stores the key first causes the
// other's iterator to drop the only parent, so it cannot recurse and returns
// false.
//
// The harm is order-dependent (the two branches run concurrently and race on
// the shared visited map), so this exercises many independent checks under
// scheduling pressure to reliably observe the false negative.
func TestRecursiveTTUVisitedCollision(t *testing.T) {
	// Intentionally not t.Parallel(): this test runs many iterations to exercise
	// the concurrent branch race, and doing so in parallel with the timing-sensitive
	// MaxTimes(1) mock tests in this package can starve them under -race.
	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1
		type user
		type organization
			relations
				define billing_user: [user] or full_admin or billing_user from parent
				define parent: [organization]
				define full_admin: [user] or full_admin from parent`)

	mg, err := modelgraph.New(model)
	require.NoError(t, err)

	// anne is a direct billing_user on the *parent* org o2. o1's parent is o2.
	// So organization:o1#billing_user@user:anne must be true via
	// "billing_user from parent" -> o2#billing_user (direct).
	const iterations = 300
	for i := 0; i < iterations; i++ {
		ds := memory.New()
		storeID := ulid.Make().String()
		writeErr := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
			tuple.NewTupleKey("organization:o1", "parent", "organization:o2"),
			tuple.NewTupleKey("organization:o2", "billing_user", "user:anne"),
		})
		require.NoError(t, writeErr)

		resolver := New(Config{
			Model:            mg,
			Datastore:        ds,
			ConcurrencyLimit: 10,
			Planner:          planner.New(&planner.Config{}),
		})

		req, reqErr := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("organization:o1", "billing_user", "user:anne"),
		})
		require.NoError(t, reqErr)

		res, resErr := resolver.ResolveCheck(context.Background(), req)
		require.NoError(t, resErr)
		require.True(t, res.GetAllowed(), "iteration %d: billing_user via parent must be allowed", i)

		ds.Close()
	}
}
