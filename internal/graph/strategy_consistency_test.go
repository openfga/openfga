package graph

import (
	"context"
	"errors"
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
		resp, err = checker.weight2(context.Background(), []<-chan *iterator.Msg{left}, right)
	}()

	select {
	case <-done:
		require.NoError(t, err)
		require.True(t, resp.GetAllowed(), "a match from another iterator must win over the error")
	case <-time.After(5 * time.Second):
		t.Fatal("weight2 kept reading an iterator that failed")
	}
}
