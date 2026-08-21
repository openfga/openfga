package worker_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	weightedGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/containers/mpsc"
	"github.com/openfga/openfga/internal/listobjects/pipeline"
	"github.com/openfga/openfga/internal/listobjects/pipeline/internal/worker"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// --- DifferenceDirectSubtract Test Helpers ---

const testStoreID string = "difference-direct-subtract"

// storeInterpreter is a minimal [worker.Interpreter] backed by a
// [pipeline.ObjectStore]. It resolves direct edges only, which is all the
// [worker.DifferenceDirectSubtract] worker requires: the base edge of an
// exclusion whose subtract branch flattens to direct wildcard edges.
type storeInterpreter struct {
	store pipeline.ObjectStore
}

func (i *storeInterpreter) Interpret(
	ctx context.Context,
	edge *worker.Edge,
	items []string,
) worker.Receiver[worker.Item] {
	if len(items) == 0 {
		return worker.NewEmptyReceiver[worker.Item]()
	}

	objectType, relation, _ := strings.Cut(edge.GetRelationDefinition(), "#")

	return i.store.Read(ctx, pipeline.ObjectQuery{
		ObjectType: objectType,
		Relation:   relation,
		Users:      items,
		Conditions: edge.GetConditions(),
	})
}

func (i *storeInterpreter) Get(
	ctx context.Context,
	object, relation, user string,
	conditions []string,
) *worker.Item {
	return i.store.Get(ctx, pipeline.ObjectGet{
		Object:     object,
		Relation:   relation,
		User:       user,
		Conditions: conditions,
	})
}

// getErrorStore fails every [pipeline.ObjectStore.Get] with a fixed error
// while delegating reads to the embedded store.
type getErrorStore struct {
	pipeline.ObjectStore
	err error
}

func (s *getErrorStore) Get(_ context.Context, _ pipeline.ObjectGet) *pipeline.Item {
	return &pipeline.Item{Err: s.err}
}

// newTestModel transforms dsl and returns the resulting type system and
// weighted graph.
func newTestModel(t testing.TB, dsl string) (*typesystem.TypeSystem, *pipeline.Graph) {
	t.Helper()

	model := testutils.MustTransformDSLToProtoWithID(dsl)

	typesys, err := typesystem.NewAndValidate(context.Background(), model)
	require.NoError(t, err)

	g := typesys.GetWeightedGraph()
	require.NotNil(t, g)

	return typesys, g
}

// newTestObjectStore writes tuples ("object#relation@user") to an in-memory
// datastore and returns a store validated against typesys.
func newTestObjectStore(t testing.TB, typesys *typesystem.TypeSystem, tuples ...string) pipeline.ObjectStore {
	t.Helper()

	ds := memory.New()
	t.Cleanup(ds.Close)

	if len(tuples) > 0 {
		keys := make([]*openfgav1.TupleKey, len(tuples))
		for i, tpl := range tuples {
			objectRelation, user, _ := strings.Cut(tpl, "@")
			object, relation, _ := strings.Cut(objectRelation, "#")
			keys[i] = tuple.NewTupleKey(object, relation, user)
		}
		require.NoError(t, ds.Write(context.Background(), testStoreID, nil, keys))
	}

	return pipeline.NewValidatingStore(
		ds,
		testStoreID,
		pipeline.WithStoreValidator(pipeline.NewValidator(context.Background(), typesys, nil)),
	)
}

// exclusionEdges returns the base and subtract edges of the exclusion
// operator that defines document#viewer, mirroring how the pipeline builder
// locates them.
func exclusionEdges(t testing.TB, g *pipeline.Graph) (*worker.Edge, *worker.Edge) {
	t.Helper()

	node, ok := g.GetNodeByID("document#viewer")
	require.True(t, ok)

	edges, ok := g.GetEdgesFromNode(node)
	require.True(t, ok)
	require.Len(t, edges, 1)

	operator := edges[0].GetTo()
	require.Equal(t, weightedGraph.OperatorNode, operator.GetNodeType())
	require.Equal(t, weightedGraph.ExclusionOperator, operator.GetLabel())

	edges, ok = g.GetEdgesFromNode(operator)
	require.True(t, ok)
	require.Len(t, edges, 2)

	return edges[0], edges[1]
}

// edgesTo returns the edges leaving object#relation whose destination label
// matches one of the given labels.
func edgesTo(t testing.TB, g *pipeline.Graph, id string, labels ...string) []*worker.Edge {
	t.Helper()

	node, ok := g.GetNodeByID(id)
	require.True(t, ok)

	edges, ok := g.GetEdgesFromNode(node)
	require.True(t, ok)

	out := make([]*worker.Edge, 0, len(labels))
	for _, label := range labels {
		for _, edge := range edges {
			if edge.GetTo().GetUniqueLabel() == label {
				out = append(out, edge)
				break
			}
		}
	}
	require.Len(t, out, len(labels))

	return out
}

// sendOn creates a mockSender bound to edge that delivers each batch as a
// separate message, then closes.
func sendOn(edge *worker.Edge, batches ...[]string) *mockSender {
	ch := make(chan *worker.Message, len(batches))
	for _, batch := range batches {
		items := make([]string, len(batch))
		copy(items, batch)
		ch <- &worker.Message{Value: items}
	}
	close(ch)
	return &mockSender{edge: edge, ch: ch}
}

// wildcardSubtractModel resolves `document#viewer` through an exclusion whose
// subtract branch reaches `user:*` exclusively, which is the shape that
// qualifies for the direct-subtract optimization.
const wildcardSubtractModel string = `
model
  schema 1.1

type user

type document
  relations
    define blocked: [user:*]
    define rel_a: [user:*] or blocked
    define rel_x: [user:*] or rel_a
    define viewer: [user] but not rel_x
`

// newWildcardSubtractWorker builds a worker over [wildcardSubtractModel] whose
// subtract edges are flattened exactly as the pipeline builder flattens them.
func newWildcardSubtractWorker(
	t testing.TB,
	errs *mpsc.Accumulator[error],
	tuples ...string,
) (*worker.DifferenceDirectSubtract, *worker.Edge) {
	t.Helper()

	typesys, g := newTestModel(t, wildcardSubtractModel)

	base, subtract := exclusionEdges(t, g)

	subtracts := pipeline.FlattenWildcardEdges(g, subtract, "user")
	require.NotEmpty(t, subtracts, "model must qualify for the direct subtract optimization")

	interp := &storeInterpreter{store: newTestObjectStore(t, typesys, tuples...)}

	w := &worker.DifferenceDirectSubtract{
		Subtracts:         subtracts,
		SubjectType:       "user",
		SubjectIdentifier: "bob",
		Core:              newCore(interp, errs),
	}

	return w, base
}

// --- DifferenceDirectSubtract Worker Tests ---

func TestDifferenceDirectSubtract_Execute_NoSenders(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w, _ := newWildcardSubtractWorker(t, errs)
	output := w.Subscribe(nil, chunkSize)

	assert.Panics(t, func() {
		w.Execute(context.Background())
	})

	assert.Empty(t, collectOutput(output))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestDifferenceDirectSubtract_Execute_MoreThanOneSender(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w, base := newWildcardSubtractWorker(t, errs)
	w.Listen(sendOn(base, []string{"user:bob"}))
	w.Listen(sendOn(base, []string{"user:bob"}))
	output := w.Subscribe(nil, chunkSize)

	assert.Panics(t, func() {
		w.Execute(context.Background())
	})

	assert.Empty(t, collectOutput(output))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestDifferenceDirectSubtract_Execute_SubtractsWildcardMatches(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w, base := newWildcardSubtractWorker(t, errs,
		"document:0#viewer@user:bob",
		"document:1#viewer@user:bob",
		"document:2#viewer@user:bob",
		"document:3#viewer@user:bob",
		// Each subtract relation in the flattened branch removes its object.
		"document:0#rel_x@user:*",
		"document:1#rel_a@user:*",
		"document:2#blocked@user:*",
	)
	w.Listen(sendOn(base, []string{"user:bob"}))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Equal(t, []string{"document:3"}, sorted(collectOutput(output)))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestDifferenceDirectSubtract_Execute_NoSubtractTuples(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w, base := newWildcardSubtractWorker(t, errs,
		"document:1#viewer@user:bob",
		"document:2#viewer@user:bob",
		// A subtract wildcard on an object outside the base set must not
		// remove anything from it.
		"document:3#rel_x@user:*",
	)
	w.Listen(sendOn(base, []string{"user:bob"}))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Equal(t, []string{"document:1", "document:2"}, sorted(collectOutput(output)))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestDifferenceDirectSubtract_Execute_AllSubtracted(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w, base := newWildcardSubtractWorker(t, errs,
		"document:1#viewer@user:bob",
		"document:2#viewer@user:bob",
		"document:1#rel_x@user:*",
		"document:2#rel_x@user:*",
	)
	w.Listen(sendOn(base, []string{"user:bob"}))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Empty(t, collectOutput(output))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestDifferenceDirectSubtract_Execute_EmptyBase(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w, base := newWildcardSubtractWorker(t, errs,
		"document:1#viewer@user:betty",
	)
	w.Listen(sendOn(base, []string{"user:bob"}))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Empty(t, collectOutput(output))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestDifferenceDirectSubtract_Execute_NoMessages(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w, base := newWildcardSubtractWorker(t, errs,
		"document:1#viewer@user:bob",
	)
	w.Listen(sendOn(base))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Empty(t, collectOutput(output))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestDifferenceDirectSubtract_Execute_MultipleMessages(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w, base := newWildcardSubtractWorker(t, errs,
		"document:1#viewer@user:bob",
		"document:2#viewer@user:betty",
		"document:2#rel_x@user:*",
	)
	// Each message is interpreted independently against the same store.
	w.Listen(sendOn(base, []string{"user:bob"}, []string{"user:betty"}))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Equal(t, []string{"document:1"}, sorted(collectOutput(output)))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestDifferenceDirectSubtract_Execute_ChunksOutput(t *testing.T) {
	defer goleak.VerifyNone(t)

	const total int = 5

	tuples := make([]string, 0, total)
	expected := make([]string, 0, total)
	for i := range total {
		id := strconv.Itoa(i)
		tuples = append(tuples, "document:"+id+"#viewer@user:bob")
		expected = append(expected, "document:"+id)
	}

	errs := mpsc.NewAccumulator[error]()
	w, base := newWildcardSubtractWorker(t, errs, tuples...)
	w.ChunkSize = 2
	w.Listen(sendOn(base, []string{"user:bob"}))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	messages := collectMessages(output)
	require.Len(t, messages, 3, "5 objects at a chunk size of 2 must produce 3 messages")
	assert.Len(t, messages[0], 2)
	assert.Len(t, messages[1], 2)
	assert.Len(t, messages[2], 1)

	var got []string
	for _, batch := range messages {
		got = append(got, batch...)
	}
	assert.ElementsMatch(t, expected, got)

	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestDifferenceDirectSubtract_Execute_ContextCancelled(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w, base := newWildcardSubtractWorker(t, errs,
		"document:1#viewer@user:bob",
	)
	w.Listen(sendOn(base, []string{"user:bob"}))
	output := w.Subscribe(nil, chunkSize)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	w.Execute(ctx)

	assert.Empty(t, collectOutput(output))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

// TestDifferenceDirectSubtract_Execute_SubtractsSpecificSubject covers the
// non-wildcard branch of the subtract loop, where the subject is rebuilt from
// SubjectType and SubjectIdentifier rather than taken from the edge.
func TestDifferenceDirectSubtract_Execute_SubtractsSpecificSubject(t *testing.T) {
	defer goleak.VerifyNone(t)

	const dsl string = `
	model
	  schema 1.1

	type user

	type document
	  relations
	    define blocked: [user, user:*]
	    define viewer: [user] but not blocked
	`

	typesys, g := newTestModel(t, dsl)

	base, _ := exclusionEdges(t, g)

	// Both terminal edges of document#blocked are subtracted: the specific
	// type edge resolves against user:bob, the wildcard edge against user:*.
	subtracts := edgesTo(t, g, "document#blocked", "user", "user:*")

	interp := &storeInterpreter{store: newTestObjectStore(t, typesys,
		"document:1#viewer@user:bob",
		"document:2#viewer@user:bob",
		"document:3#viewer@user:bob",
		"document:2#blocked@user:bob",
		"document:3#blocked@user:*",
	)}

	errs := mpsc.NewAccumulator[error]()
	w := &worker.DifferenceDirectSubtract{
		Subtracts:         subtracts,
		SubjectType:       "user",
		SubjectIdentifier: "bob",
		Core:              newCore(interp, errs),
	}
	w.Listen(sendOn(base, []string{"user:bob"}))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Equal(t, []string{"document:1"}, sorted(collectOutput(output)))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestDifferenceDirectSubtract_Execute_UnexpectedObjectType(t *testing.T) {
	defer goleak.VerifyNone(t)

	_, g := newTestModel(t, wildcardSubtractModel)

	base, subtract := exclusionEdges(t, g)
	subtracts := pipeline.FlattenWildcardEdges(g, subtract, "user")
	require.NotEmpty(t, subtracts)

	// The base yields folder objects while every subtract edge is defined on
	// document, so the object type check must fail.
	interp := &mockInterpreter{
		fn: func(_ context.Context, _ *worker.Edge, items []string) worker.Receiver[worker.Item] {
			return worker.MapReceiver(worker.NewSliceReceiver(items), func(string) worker.Item {
				return worker.Item{Value: "folder:1"}
			})
		},
	}

	errs := mpsc.NewAccumulator[error]()
	w := &worker.DifferenceDirectSubtract{
		Subtracts:         subtracts,
		SubjectType:       "user",
		SubjectIdentifier: "bob",
		Core:              newCore(interp, errs),
	}
	w.Listen(sendOn(base, []string{"user:bob"}))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())
	errs.Close()

	assert.Empty(t, collectOutput(output))
	got := collectErrors(errs)
	require.NotEmpty(t, got)
	assert.ErrorIs(t, got[0], worker.ErrUnexpectedObjectType)
}

func TestDifferenceDirectSubtract_Execute_UnexpectedUserType(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w, base := newWildcardSubtractWorker(t, errs,
		"document:1#viewer@user:bob",
	)
	// The subtract edges terminate at user:*, so an employee subject cannot
	// be resolved through them.
	w.SubjectType = "employee"
	w.Listen(sendOn(base, []string{"user:bob"}))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())
	errs.Close()

	assert.Empty(t, collectOutput(output))
	got := collectErrors(errs)
	require.NotEmpty(t, got)
	assert.ErrorIs(t, got[0], worker.ErrUnexpectedUserType)
}

func TestDifferenceDirectSubtract_Execute_BaseInterpreterError(t *testing.T) {
	defer goleak.VerifyNone(t)

	sentinelErr := errors.New("interpret failed")

	_, g := newTestModel(t, wildcardSubtractModel)

	base, subtract := exclusionEdges(t, g)
	subtracts := pipeline.FlattenWildcardEdges(g, subtract, "user")
	require.NotEmpty(t, subtracts)

	interp := &mockInterpreter{
		fn: func(_ context.Context, _ *worker.Edge, items []string) worker.Receiver[worker.Item] {
			return worker.MapReceiver(worker.NewSliceReceiver(items), func(string) worker.Item {
				return worker.Item{Err: sentinelErr}
			})
		},
	}

	errs := mpsc.NewAccumulator[error]()
	w := &worker.DifferenceDirectSubtract{
		Subtracts:         subtracts,
		SubjectType:       "user",
		SubjectIdentifier: "bob",
		Core:              newCore(interp, errs),
	}
	w.Listen(sendOn(base, []string{"user:bob"}))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())
	errs.Close()

	assert.Empty(t, collectOutput(output))
	got := collectErrors(errs)
	require.NotEmpty(t, got)
	assert.ErrorIs(t, got[0], sentinelErr)
}

func TestDifferenceDirectSubtract_Execute_SubtractLookupError(t *testing.T) {
	defer goleak.VerifyNone(t)

	sentinelErr := errors.New("get failed")

	typesys, g := newTestModel(t, wildcardSubtractModel)

	base, subtract := exclusionEdges(t, g)
	subtracts := pipeline.FlattenWildcardEdges(g, subtract, "user")
	require.NotEmpty(t, subtracts)

	store := &getErrorStore{
		ObjectStore: newTestObjectStore(t, typesys, "document:1#viewer@user:bob"),
		err:         sentinelErr,
	}

	errs := mpsc.NewAccumulator[error]()
	w := &worker.DifferenceDirectSubtract{
		Subtracts:         subtracts,
		SubjectType:       "user",
		SubjectIdentifier: "bob",
		Core:              newCore(&storeInterpreter{store: store}, errs),
	}
	w.Listen(sendOn(base, []string{"user:bob"}))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())
	errs.Close()

	// A failed subtract lookup must not emit the base object.
	assert.Empty(t, collectOutput(output))
	got := collectErrors(errs)
	require.NotEmpty(t, got)
	assert.ErrorIs(t, got[0], sentinelErr)
}
