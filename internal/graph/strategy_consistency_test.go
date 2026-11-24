package graph

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/internal/iterator"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// checkStrategies are the values of ResolveCheckRequest.SelectedStrategy that bypass the planner.
var checkStrategies = []string{defaultResolver, weightTwoResolver, recursiveResolver}

// conditionalFirstReader puts conditional tuples first for the same object: memory's sort is not stable, and this order
// is what made the sorted merge drop the wrong tuple.
type conditionalFirstReader struct {
	storage.RelationshipTupleReader
}

func (r conditionalFirstReader) ReadStartingWithUser(ctx context.Context, store string, filter storage.ReadStartingWithUserFilter, options storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
	iter, err := r.RelationshipTupleReader.ReadStartingWithUser(ctx, store, filter, options)
	if err != nil {
		return nil, err
	}
	defer iter.Stop()

	var tuples []*openfgav1.Tuple
	for {
		t, err := iter.Next(ctx)
		if err != nil {
			if storage.IterIsDoneOrCancelled(err) {
				break
			}
			return nil, err
		}
		tuples = append(tuples, t)
	}
	slices.SortStableFunc(tuples, func(a, b *openfgav1.Tuple) int {
		switch {
		case a.GetKey().GetObject() != b.GetKey().GetObject():
			if a.GetKey().GetObject() < b.GetKey().GetObject() {
				return -1
			}
			return 1
		case a.GetKey().GetCondition() != nil && b.GetKey().GetCondition() == nil:
			return -1
		case a.GetKey().GetCondition() == nil && b.GetKey().GetCondition() != nil:
			return 1
		}
		return 0
	})
	return storage.NewStaticTupleIterator(tuples), nil
}

// checkWithStrategy runs a Check forcing the given strategy on the first node that can use one.
func checkWithStrategy(t *testing.T, dsl string, tuples []*openfgav1.TupleKey, relation string, reqContext map[string]any, strategy string) (*ResolveCheckResponse, error) {
	t.Helper()

	model := parser.MustTransformDSLToProto(dsl)
	storeID := ulid.Make().String()
	ds := memory.New()
	t.Cleanup(ds.Close)
	require.NoError(t, ds.Write(context.Background(), storeID, nil, tuples))

	typesys, err := typesystem.NewAndValidate(context.Background(), model)
	require.NoError(t, err)

	ctx := setRequestContext(context.Background(), typesys, conditionalFirstReader{ds}, nil)

	var reqCtx *structpb.Struct
	if reqContext != nil {
		reqCtx, err = structpb.NewStruct(reqContext)
		require.NoError(t, err)
	}

	checker := NewLocalChecker()
	t.Cleanup(checker.Close)
	return checker.ResolveCheck(ctx, &ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: model.GetId(),
		TupleKey:             tuple.NewTupleKey("document:d", relation, "user:u"),
		Context:              reqCtx,
		RequestMetadata:      NewCheckRequestMetadata(),
		SelectedStrategy:     strategy,
	})
}

func TestCheckStrategiesAgreeOnWildcardWithConditionalTuple(t *testing.T) {
	const wildcardModel = `
		model
			schema 1.1
		type user
		type group
			relations
				define member: [user with xcond, user:*]
				define blocked: [user with xcond, user:*]
				define viewer: [user:*]
				define allowed: viewer but not blocked
		type document
			relations
				define viewer: [group#member]
				define access: [group#allowed]
		condition xcond(x: string) {
			x == '1'
		}`

	// The request context makes xcond evaluate to false, so only the unconditional user:* tuple can grant.
	falseCond := map[string]any{"x": "2"}

	for _, strategy := range checkStrategies {
		t.Run("wildcard_grants_when_conditional_tuple_is_false/"+strategy, func(t *testing.T) {
			resp, err := checkWithStrategy(t, wildcardModel, []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("group:g", "member", "user:u", "xcond", nil),
				tuple.NewTupleKey("group:g", "member", "user:*"),
				tuple.NewTupleKey("document:d", "viewer", "group:g#member"),
			}, "viewer", falseCond, strategy)
			require.NoError(t, err)
			require.True(t, resp.GetAllowed())
		})

		t.Run("wildcard_blocks_when_conditional_tuple_is_false/"+strategy, func(t *testing.T) {
			resp, err := checkWithStrategy(t, wildcardModel, []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:g", "viewer", "user:*"),
				tuple.NewTupleKeyWithCondition("group:g", "blocked", "user:u", "xcond", nil),
				tuple.NewTupleKey("group:g", "blocked", "user:*"),
				tuple.NewTupleKey("document:d", "access", "group:g#allowed"),
			}, "access", falseCond, strategy)
			require.NoError(t, err)
			require.False(t, resp.GetAllowed())
		})
	}
}

func TestCheckStrategiesAgreeOnUserAndWildcardOnSameObjectInExclusion(t *testing.T) {
	const model = `
		model
			schema 1.1
		type user
		type group
			relations
				define viewer: [user, user:*]
				define blocked: [user]
				define allowed: viewer but not blocked
		type document
			relations
				define access: [group#allowed]`

	for _, strategy := range checkStrategies {
		// viewer yields group:g twice; without deduplication the base drain would emit it although blocked.
		t.Run("blocked_object_is_not_granted_through_duplicate_base_entry/"+strategy, func(t *testing.T) {
			resp, err := checkWithStrategy(t, model, []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:g", "viewer", "user:u"),
				tuple.NewTupleKey("group:g", "viewer", "user:*"),
				tuple.NewTupleKey("group:g", "blocked", "user:u"),
				tuple.NewTupleKey("document:d", "access", "group:g#allowed"),
			}, "access", nil, strategy)
			require.NoError(t, err)
			require.False(t, resp.GetAllowed())
		})

		t.Run("unblocked_object_is_granted/"+strategy, func(t *testing.T) {
			resp, err := checkWithStrategy(t, model, []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:g", "viewer", "user:u"),
				tuple.NewTupleKey("group:g", "viewer", "user:*"),
				tuple.NewTupleKey("document:d", "access", "group:g#allowed"),
			}, "access", nil, strategy)
			require.NoError(t, err)
			require.True(t, resp.GetAllowed())
		})
	}
}

// A condition error must only fail a Check when its tuple is on the path being checked, whatever the strategy.
// The weight2 and recursive strategies read every object related to the user, so they also see tuples off the path.
func TestCheckStrategiesAgreeOnConditionErrors(t *testing.T) {
	const cond = `
		condition xcond(x: string) {
			x == '1'
		}`
	const usersetModel = `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user with xcond, user]
				define editor: [document#viewer]` + cond
	const ttuModel = `
		model
			schema 1.1
		type user
		type folder
			relations
				define viewer: [user with xcond, user]
		type document
			relations
				define parent: [folder]
				define viewer: viewer from parent` + cond
	const recursiveUsersetModel = `
		model
			schema 1.1
		type user
		type robot
		type document
			relations
				define other: [robot]
				define viewer: [user with xcond, user, user:*, document#viewer, document#other]` + cond
	const recursiveTTUModel = `
		model
			schema 1.1
		type user
		type document
			relations
				define parent: [document]
				define viewer: [user with xcond, user] or viewer from parent` + cond
	const exclusionModel = `
		model
			schema 1.1
		type user
		type group
			relations
				define viewer: [user]
				define blocked: [user with xcond, user]
				define allowed: viewer but not blocked
		type document
			relations
				define access: [group#allowed]` + cond
	const unionModel = `
		model
			schema 1.1
		type user
		type group
			relations
				define viewer: [user with xcond]
				define editor: [user]
				define allowed: viewer or editor
		type document
			relations
				define access: [group#allowed]` + cond

	conditional := func(object, relation, user string) *openfgav1.TupleKey {
		return tuple.NewTupleKeyWithCondition(object, relation, user, "xcond", nil)
	}
	tk := tuple.NewTupleKey

	tests := []struct {
		name     string
		model    string
		relation string
		tuples   []*openfgav1.TupleKey
		allowed  bool
		// errTuple is the tuple named by the expected condition error, if any.
		errTuple string
	}{
		{
			name:     "userset_off_path_error_is_ignored",
			model:    usersetModel,
			relation: "editor",
			tuples: []*openfgav1.TupleKey{
				conditional("document:off", "viewer", "user:u"),
				tk("document:d", "editor", "document:on#viewer"),
			},
		},
		{
			name:     "userset_without_off_path_tuple",
			model:    usersetModel,
			relation: "editor",
			tuples: []*openfgav1.TupleKey{
				tk("document:d", "editor", "document:on#viewer"),
			},
		},
		{
			name:     "userset_on_path_error",
			model:    usersetModel,
			relation: "editor",
			tuples: []*openfgav1.TupleKey{
				conditional("document:on", "viewer", "user:u"),
				tk("document:d", "editor", "document:on#viewer"),
			},
			errTuple: "document:on#viewer@user:u",
		},
		{
			name:     "userset_on_path_error_with_off_path_valid_tuple",
			model:    usersetModel,
			relation: "editor",
			tuples: []*openfgav1.TupleKey{
				conditional("document:on", "viewer", "user:u"),
				tk("document:off", "viewer", "user:u"),
				tk("document:d", "editor", "document:on#viewer"),
			},
			errTuple: "document:on#viewer@user:u",
		},
		{
			name:     "userset_on_path_valid_tuple_with_off_path_error",
			model:    usersetModel,
			relation: "editor",
			tuples: []*openfgav1.TupleKey{
				conditional("document:off", "viewer", "user:u"),
				tk("document:on", "viewer", "user:u"),
				tk("document:d", "editor", "document:on#viewer"),
			},
			allowed: true,
		},
		{
			name:     "ttu_off_path_error_is_ignored",
			model:    ttuModel,
			relation: "viewer",
			tuples: []*openfgav1.TupleKey{
				conditional("folder:off", "viewer", "user:u"),
				tk("document:d", "parent", "folder:on"),
			},
		},
		{
			name:     "ttu_on_path_error",
			model:    ttuModel,
			relation: "viewer",
			tuples: []*openfgav1.TupleKey{
				conditional("folder:on", "viewer", "user:u"),
				tk("document:d", "parent", "folder:on"),
			},
			errTuple: "folder:on#viewer@user:u",
		},
		{
			name:     "recursive_userset_off_path_error_is_ignored",
			model:    recursiveUsersetModel,
			relation: "viewer",
			tuples: []*openfgav1.TupleKey{
				conditional("document:off", "viewer", "user:u"),
				tk("document:d", "viewer", "document:a#viewer"),
				tk("document:a", "viewer", "document:on#viewer"),
			},
		},
		{
			name:     "recursive_userset_on_path_error",
			model:    recursiveUsersetModel,
			relation: "viewer",
			tuples: []*openfgav1.TupleKey{
				conditional("document:on", "viewer", "user:u"),
				tk("document:d", "viewer", "document:on#viewer"),
			},
			errTuple: "document:on#viewer@user:u",
		},
		{
			name:     "recursive_userset_deep_on_path_error",
			model:    recursiveUsersetModel,
			relation: "viewer",
			tuples: []*openfgav1.TupleKey{
				conditional("document:on", "viewer", "user:u"),
				tk("document:d", "viewer", "document:a#viewer"),
				tk("document:a", "viewer", "document:on#viewer"),
			},
			errTuple: "document:on#viewer@user:u",
		},
		{
			name:     "recursive_userset_on_path_error_and_on_path_valid_tuple",
			model:    recursiveUsersetModel,
			relation: "viewer",
			tuples: []*openfgav1.TupleKey{
				conditional("document:on", "viewer", "user:u"),
				tk("document:on2", "viewer", "user:u"),
				tk("document:d", "viewer", "document:on#viewer"),
				tk("document:d", "viewer", "document:a#viewer"),
				tk("document:a", "viewer", "document:on2#viewer"),
			},
			allowed: true,
		},
		{
			name:     "recursive_userset_on_path_error_and_wildcard_on_same_object",
			model:    recursiveUsersetModel,
			relation: "viewer",
			tuples: []*openfgav1.TupleKey{
				conditional("document:on", "viewer", "user:u"),
				tk("document:on", "viewer", "user:*"),
				tk("document:d", "viewer", "document:on#viewer"),
			},
			allowed: true,
		},
		{
			name:     "recursive_userset_off_path_error_via_other_relation_is_ignored",
			model:    recursiveUsersetModel,
			relation: "viewer",
			tuples: []*openfgav1.TupleKey{
				conditional("document:x", "viewer", "user:u"),
				tk("document:d", "viewer", "document:x#other"),
			},
		},
		{
			name:     "recursive_ttu_off_path_error_is_ignored",
			model:    recursiveTTUModel,
			relation: "viewer",
			tuples: []*openfgav1.TupleKey{
				conditional("document:off", "viewer", "user:u"),
				tk("document:d", "parent", "document:a"),
				tk("document:a", "parent", "document:on"),
			},
		},
		{
			name:     "recursive_ttu_deep_on_path_error",
			model:    recursiveTTUModel,
			relation: "viewer",
			tuples: []*openfgav1.TupleKey{
				conditional("document:on", "viewer", "user:u"),
				tk("document:d", "parent", "document:a"),
				tk("document:a", "parent", "document:on"),
			},
			errTuple: "document:on#viewer@user:u",
		},
		{
			name:     "exclusion_on_path_error_with_off_path_valid_tuple",
			model:    exclusionModel,
			relation: "access",
			tuples: []*openfgav1.TupleKey{
				tk("group:on", "viewer", "user:u"),
				conditional("group:on", "blocked", "user:u"),
				tk("group:z", "blocked", "user:u"),
				tk("document:d", "access", "group:on#allowed"),
			},
			errTuple: "group:on#blocked@user:u",
		},
		{
			name:     "exclusion_on_path_error",
			model:    exclusionModel,
			relation: "access",
			tuples: []*openfgav1.TupleKey{
				tk("group:on", "viewer", "user:u"),
				conditional("group:on", "blocked", "user:u"),
				tk("document:d", "access", "group:on#allowed"),
			},
			errTuple: "group:on#blocked@user:u",
		},
		{
			name:     "exclusion_off_path_error_is_ignored",
			model:    exclusionModel,
			relation: "access",
			tuples: []*openfgav1.TupleKey{
				tk("group:on", "viewer", "user:u"),
				conditional("group:off", "blocked", "user:u"),
				tk("document:d", "access", "group:on#allowed"),
			},
			allowed: true,
		},
		{
			name:     "union_off_path_error_is_ignored",
			model:    unionModel,
			relation: "access",
			tuples: []*openfgav1.TupleKey{
				conditional("group:off", "viewer", "user:u"),
				tk("group:on", "editor", "user:u"),
				tk("document:d", "access", "group:on#allowed"),
			},
			allowed: true,
		},
		{
			name:     "union_on_path_error_and_on_path_valid_tuple",
			model:    unionModel,
			relation: "access",
			tuples: []*openfgav1.TupleKey{
				conditional("group:on", "viewer", "user:u"),
				tk("group:on2", "editor", "user:u"),
				tk("document:d", "access", "group:on#allowed"),
				tk("document:d", "access", "group:on2#allowed"),
			},
			allowed: true,
		},
		{
			name:     "union_on_path_error_and_valid_tuple_on_same_object",
			model:    unionModel,
			relation: "access",
			tuples: []*openfgav1.TupleKey{
				conditional("group:on", "viewer", "user:u"),
				tk("group:on", "editor", "user:u"),
				tk("document:d", "access", "group:on#allowed"),
			},
			allowed: true,
		},
	}

	for _, test := range tests {
		for _, strategy := range checkStrategies {
			t.Run(test.name+"/"+strategy, func(t *testing.T) {
				resp, err := checkWithStrategy(t, test.model, test.tuples, test.relation, nil, strategy)
				if test.errTuple != "" {
					require.ErrorContains(t, err, test.errTuple)
					return
				}
				require.NoError(t, err)
				require.Equal(t, test.allowed, resp.GetAllowed())
			})
		}
	}
}

// An object with a condition error is checked from the depth the search reached it at, and a search that ends before
// the max depth does not fail, as with the default strategy.
func TestCheckStrategiesAgreeOnConditionErrorNearMaxDepth(t *testing.T) {
	const model = `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user with xcond, user, document#viewer]
		condition xcond(x: string) {
			x == '1'
		}`

	// chain links document:d to document:1 and each document:i to document:i+1, so document:n is reached at depth n.
	chain := func(n int, tuples ...*openfgav1.TupleKey) []*openfgav1.TupleKey {
		tuples = append(tuples, tuple.NewTupleKey("document:d", "viewer", "document:1#viewer"))
		for i := 2; i <= n; i++ {
			tuples = append(tuples, tuple.NewTupleKey(fmt.Sprintf("document:%d", i-1), "viewer", fmt.Sprintf("document:%d#viewer", i)))
		}
		return tuples
	}

	tests := []struct {
		name    string
		tuples  []*openfgav1.TupleKey
		wantErr bool
	}{
		{
			// The grant on document:26 is past the default max depth of 25.
			name: "grant_past_max_depth_below_on_path_error",
			tuples: chain(26,
				tuple.NewTupleKeyWithCondition("document:11", "viewer", "user:u", "xcond", nil),
				tuple.NewTupleKey("document:26", "viewer", "user:u"),
			),
			wantErr: true,
		},
		{
			name:   "off_path_error_and_chain_ending_before_max_depth",
			tuples: chain(24, tuple.NewTupleKeyWithCondition("document:off", "viewer", "user:u", "xcond", nil)),
		},
	}

	for _, test := range tests {
		for _, strategy := range checkStrategies {
			t.Run(test.name+"/"+strategy, func(t *testing.T) {
				resp, err := checkWithStrategy(t, model, test.tuples, "viewer", nil, strategy)
				if test.wantErr {
					require.Error(t, err)
					require.Nil(t, resp)
					return
				}
				require.NoError(t, err)
				require.False(t, resp.GetAllowed())
			})
		}
	}
}

type failingStringIterator struct{ err error }

func (f failingStringIterator) Next(context.Context) (string, error) { return "", f.err }
func (f failingStringIterator) Head(context.Context) (string, error) { return "", f.err }
func (f failingStringIterator) Stop()                                {}
func (f failingStringIterator) IsOrdered() bool                      { return true }

// A stopped iterator may keep returning its error (Dedupe reads Head, which the ordered merge caches), so weight2
// must not read it again.
func TestWeight2LeftIteratorError(t *testing.T) {
	// the first left iterator fails forever, the second holds the object the right side points to
	done := make(chan struct{})
	var resp *ResolveCheckResponse
	var err error
	go func() {
		defer close(done)
		left := make(chan *iterator.Msg)
		go func() {
			defer close(left)
			left <- &iterator.Msg{Iter: failingStringIterator{err: errors.New("transient datastore error")}}
			left <- &iterator.Msg{Iter: storage.NewStaticIterator([]string{"doc:m"})}
		}()
		right := storage.WrapIterator(storage.TTUKind,
			storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{tuple.NewTupleKey("folder:f", "parent", "doc:m")}))

		checker := NewLocalChecker()
		defer checker.Close()
		resp, err = checker.weight2(context.Background(), []<-chan *iterator.Msg{left}, right, newObjectSet(), nil)
	}()

	select {
	case <-done:
		require.NoError(t, err)
		require.True(t, resp.GetAllowed(), "a match from another iterator must win over the error")
	case <-time.After(5 * time.Second):
		t.Fatal("weight2 kept reading an iterator that failed")
	}
}
