package reverseexpand

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	aq "github.com/emirpasic/gods/queues/arrayqueue"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	weightedGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/checkutil"
	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/stack"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

const (
	listObjectsResultChannelLength = 100
)

var (
	ErrEmptyStack           = errors.New("unexpected empty stack")
	ErrLowestWeightFail     = errors.New("failed to get lowest weight edge")
	ErrConstructUsersetFail = errors.New("failed to construct userset")
)

type ExecutionError struct {
	operation string
	object    string
	relation  string
	user      string
	cause     error
}

func (e *ExecutionError) Error() string {
	return fmt.Sprintf("failed to execute: operation: %s: object: %s: relation: %s: user: %s: cause: %s",
		e.operation,
		e.object,
		e.relation,
		e.user,
		e.cause.Error(),
	)
}

// typeRelEntry represents a step in the path taken to reach a leaf node.
// As reverseExpand traverses from a requested type#rel to its leaf nodes, it stack.Pushes typeRelEntry structs to a stack.
// After reaching a leaf, this stack is consumed by the `queryForTuples` function to build the precise chain of
// database queries needed to find the resulting objects.
type typeRelEntry struct {
	typeRel string // e.g. "organization#admin"

	// Only present for userset relations. Will be the userset relation string itself.
	// For `rel admin: [team#member]`, usersetRelation is "member"
	usersetRelation string
}

// queryJob represents a single task in the reverse expansion process.
// It holds the `foundObject` from a previous step in the traversal
// and the `ReverseExpandRequest` containing the current state of the request.
type queryJob struct {
	foundObject string
	req         *ReverseExpandRequest
}

// jobQueue is a thread-safe queue for managing `queryJob` instances.
// It's used to hold jobs that need to be processed during the recursive
// `queryForTuples` operation, allowing concurrent processing of branches
// in the authorization graph.
type jobQueue struct {
	queue aq.Queue
	mu    sync.Mutex
}

func newJobQueue() *jobQueue {
	return &jobQueue{queue: *aq.New()}
}

func (q *jobQueue) Empty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.queue.Empty()
}

func (q *jobQueue) enqueue(value ...queryJob) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, item := range value {
		q.queue.Enqueue(item)
	}
}

func (q *jobQueue) dequeue() (queryJob, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	val, ok := q.queue.Dequeue()
	if !ok {
		return queryJob{}, false
	}
	job, ok := val.(queryJob)
	if !ok {
		return queryJob{}, false
	}

	return job, true
}

// emptySequence represents an `iter.Seq[Item]` that does nothing.
var emptySequence = func(yield func(Item) bool) {}

type (
	Graph = weightedGraph.WeightedAuthorizationModelGraph
	Node  = weightedGraph.WeightedAuthorizationModelNode
	Edge  = weightedGraph.WeightedAuthorizationModelEdge

	EdgeType = weightedGraph.EdgeType
)

var (
	EdgeTypeDirect        = weightedGraph.DirectEdge
	EdgeTypeComputed      = weightedGraph.ComputedEdge
	EdgeTypeRewrite       = weightedGraph.RewriteEdge
	EdgeTypeTTU           = weightedGraph.TTUEdge
	EdgeTypeDirectLogical = weightedGraph.DirectLogicalEdge
	EdgeTypeTTULogical    = weightedGraph.TTULogicalEdge

	NodeTypeSpecificType            = weightedGraph.SpecificType
	NodeTypeSpecificTypeWildcard    = weightedGraph.SpecificTypeWildcard
	NodeTypeSpecificTypeAndRelation = weightedGraph.SpecificTypeAndRelation
	NodeTypeOperator                = weightedGraph.OperatorNode
	NodeTypeLogicalDirectGrouping   = weightedGraph.LogicalDirectGrouping
	NodeTypeLogicalTTUGrouping      = weightedGraph.LogicalTTUGrouping

	ErrNoSuchNode         = errors.New("no such node in graph")
	ErrNoSuchPath         = errors.New("no path between source and target nodes")
	ErrConnectionOverflow = errors.New("too many connections")
	ErrTupleCycle         = errors.New("cycle detected in tuples")
)

// Item is a struct that contains an object `string` as its `Value` or an
// encountered error as its `Err`. Item is the primary container used to
// communicate values as they pass through a `Pipeline`.
type Item struct {
	Value string
	Err   error
}

// Group is a struct that acts as a container for a set of `Item` values.
type Group struct {
	Items []Item
}

// sequence is a function that turns its input into an `iter.Seq[T]` that
// yields values in the order that they were provided to the function.
func sequence[T any](items ...T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, item := range items {
			if !yield(item) {
				return
			}
		}
	}
}

// flatten is a function that merges a set of provided `iter.Seq[T]`
// values into a single `iter.Seq[T]` value. The values of each input are
// yielded in the order yielded by each `iter.Seq[T]`, in the order provided
// to the function.
func flatten[T any](seqs ...iter.Seq[T]) iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, seq := range seqs {
			for item := range seq {
				if !yield(item) {
					return
				}
			}
		}
	}
}

// transform is a function that maps the values yielded by the input `seq`
// to values produced by the input function `fn`, and returns an `iter.Seq`
// that yields those new values.
func transform[T any, U any](seq iter.Seq[T], fn func(T) U) iter.Seq[U] {
	return func(yield func(U) bool) {
		for item := range seq {
			yield(fn(item))
		}
	}
}

// strtoItem is a function that accepts a string input and returns an Item
// that contains the input as its `Value` value.
func strToItem(s string) Item {
	return Item{Value: s}
}

// Backend is a struct that serves as a container for all backend elements
// necessary for creating and running a `Pipeline`.
type Backend struct {
	Datastore  storage.RelationshipTupleReader
	StoreID    string
	TypeSystem *typesystem.TypeSystem
	Context    *structpb.Struct
	Graph      *Graph
}

type queryInput struct {
	objectType     string
	objectRelation string
	userFilter     []*openfgav1.ObjectRelation
	conditions     []string
}

func (b *Backend) query(ctx context.Context, input queryInput) iter.Seq[Item] {
	ctx, cancel := context.WithCancel(ctx)

	it, err := b.Datastore.ReadStartingWithUser(
		ctx,
		b.StoreID,
		storage.ReadStartingWithUserFilter{
			ObjectType: input.objectType,
			Relation:   input.objectRelation,
			UserFilter: input.userFilter,
		},
		storage.ReadStartingWithUserOptions{},
	)

	if err != nil {
		cancel()
		return sequence(Item{Err: err})
	}

	var hasConditions bool

	for _, cond := range input.conditions {
		if cond != weightedGraph.NoCond {
			hasConditions = true
			break
		}
	}

	var itr storage.TupleKeyIterator

	if hasConditions {
		itr = storage.NewConditionsFilteredTupleKeyIterator(
			storage.NewFilteredTupleKeyIterator(
				storage.NewTupleKeyIteratorFromTupleIterator(it),
				validation.FilterInvalidTuples(b.TypeSystem),
			),
			checkutil.BuildTupleKeyConditionFilter(ctx, b.Context, b.TypeSystem),
		)
	} else {
		itr = storage.NewFilteredTupleKeyIterator(
			storage.NewTupleKeyIteratorFromTupleIterator(it),
			validation.FilterInvalidTuples(b.TypeSystem),
		)
	}

	return func(yield func(Item) bool) {
		defer cancel()
		defer itr.Stop()

		for ctx.Err() == nil {
			t, err := itr.Next(ctx)

			var item Item

			if err != nil {
				if err == storage.ErrIteratorDone {
					break
				}

				item.Err = err

				yield(item)
				break
			}

			if t == nil {
				continue
			}

			item.Value = t.GetObject()

			if !yield(item) {
				break
			}
		}
	}
}

// resolver is an interface that is consumed by a Worker struct.
// a resolver is responsible for consuming messages from a Worker's
// senders and broadcasting the result of processing the consumed
// messages to the Worker's listeners.
type resolver interface {
	// Resolve is a function that consumes messages from the
	// provided senders, and broadcasts the results of processing
	// the consumed messages to the provided listeners.
	Resolve(senders []*sender, listeners []*listener)

	Ready() bool
}

// interpreter is an interface that exposes a method for interpreting input for an edge into output.
type interpreter interface {
	Interpret(edge *Edge, items []Item) iter.Seq[Item]
}

// baseResolver is a struct that implements the `resolver` interface and acts as the standard resolver for most
// workers. A baseResolver handles both recursive and non-recursive edges concurrently. The baseResolver's "ready"
// status will remain `true` until all of its senders that produce input from external sources have finished, and
// there exist no more in-flight messages for the parent worker. When recursive edges exist, the parent worker for
// this resolver type requires its internal watchdog process to initiate a shutdown.
type baseResolver struct {
	id  int
	ctx context.Context

	// interpreter is an `interpreter` that transforms a sender's input into output which it broadcasts to all
	// of the parent worker's listeners.
	interpreter interpreter

	// done indicates when all of the senders that produce input from external sources have completed.
	// done is used as the resolver's "ready" indicator, which is exposed by the resolver's `Status` method.
	// When done is `true`, it is possible for recursive senders to still be processing messages. The
	// resolver's parent worker's watchdog process won't kill the parent worker until both done is `true`, and
	// there a no more messages in-flight for this resolver.
	done atomic.Bool

	// mutexes each protect map access for a buffer within inBuffers at the same index.
	mutexes []sync.Mutex

	// inBuffers contains a slice of maps, used as hash sets, for deduplicating each individual sender's
	// input feed. Each buffer's index corresponds to its associated sender's index. Each sender needs
	// a separate deduplication buffer because it is valid for the same object to be receieved on multiple
	// edges producing to the same node. This is specifically true in the case of recursive edges, where
	// a single resolver may have multiple recursive edges that must receive the same objects.
	inBuffers []map[string]struct{}

	// outMu protects map access to outBuffer.
	outMu sync.Mutex

	// outBuffer contains a map, used as a hash set, for deduplicating each resolver's output. A single
	// resolver should only output an object once.
	outBuffer map[string]struct{}

	status *StatusPool
}

func (r *baseResolver) Ready() bool {
	return r.status.Status()
}

func (r *baseResolver) process(ndx int, senders []*sender, listeners []*listener) {
	for senders[ndx].more() && r.ctx.Err() == nil {
		var results iter.Seq[Item]
		var outGroup Group
		var items, unseen []Item

		msg, ok := senders[ndx].recv()
		if !ok {
			goto ProcessEnd
		}

		// Deduplicate items within this group based on the buffer for this sender
		for _, item := range msg.Value.Items {
			if item.Err != nil {
				unseen = append(unseen, item)
				continue
			}

			r.mutexes[ndx].Lock()
			if _, ok := r.inBuffers[ndx][item.Value]; ok {
				r.mutexes[ndx].Unlock()
				continue
			}
			r.inBuffers[ndx][item.Value] = struct{}{}
			r.mutexes[ndx].Unlock()
			unseen = append(unseen, item)
		}

		// If there are no unseen items, skip processing
		if len(unseen) == 0 {
			goto ProcessEnd
		}

		results = r.interpreter.Interpret(senders[ndx].edge, unseen)

		for item := range results {
			r.outMu.Lock()
			if _, ok := r.outBuffer[item.Value]; ok {
				r.outMu.Unlock()
				continue
			}
			r.outBuffer[item.Value] = struct{}{}
			r.outMu.Unlock()

			items = append(items, item)

			if len(items) < senders[ndx].chunkSize || senders[ndx].chunkSize == 0 {
				continue
			}

			outGroup := Group{
				Items: items,
			}

			items = nil

			for _, lst := range listeners {
				lst.send(outGroup)
			}
		}

		if len(items) == 0 {
			goto ProcessEnd
		}

		outGroup.Items = items

		for _, lst := range listeners {
			lst.send(outGroup)
		}

	ProcessEnd:
		msg.Done()
		runtime.Gosched()
	}
}

func (r *baseResolver) Resolve(senders []*sender, listeners []*listener) {
	r.mutexes = make([]sync.Mutex, len(senders))
	r.inBuffers = make([]map[string]struct{}, len(senders))
	r.outBuffer = make(map[string]struct{})

	for ndx := range len(senders) {
		r.inBuffers[ndx] = make(map[string]struct{})
	}

	var standard []func()
	var recursive []func()

	for ndx := range len(senders) {
		snd := senders[ndx]
		isRecursive := snd.edge != nil && len(snd.edge.GetRecursiveRelation()) > 0 && !snd.edge.IsPartOfTupleCycle()

		proc := func() {
			r.process(ndx, senders, listeners)
		}

		for range snd.numProcs {
			if isRecursive {
				recursive = append(recursive, proc)
				continue
			}
			standard = append(standard, proc)
		}
	}

	var wgStandard sync.WaitGroup

	for _, proc := range standard {
		wgStandard.Add(1)
		go func() {
			defer wgStandard.Done()
			proc()
		}()
	}

	var wgRecursive sync.WaitGroup

	for _, proc := range recursive {
		wgRecursive.Add(1)
		go func() {
			defer wgRecursive.Done()
			proc()
		}()
	}
	wgStandard.Wait()

	r.status.Set(r.id, false)

	wgRecursive.Wait()
}

type edgeHandler func(*Edge, []Item) iter.Seq[Item]

func (b *Backend) handleDirectEdge(edge *Edge, items []Item) iter.Seq[Item] {
	parts := strings.Split(edge.GetRelationDefinition(), "#")
	nodeType := parts[0]
	nodeRelation := parts[1]

	userParts := strings.Split(edge.GetTo().GetLabel(), "#")

	var userRelation string

	if len(userParts) > 1 {
		userRelation = userParts[1]
	}

	var userFilter []*openfgav1.ObjectRelation

	var errs []Item

	for _, item := range items {
		if item.Err != nil {
			errs = append(errs, item)
			continue
		}

		userFilter = append(userFilter, &openfgav1.ObjectRelation{
			Object:   item.Value,
			Relation: userRelation,
		})
	}

	var results iter.Seq[Item]

	if len(userFilter) > 0 {
		input := queryInput{
			objectType:     nodeType,
			objectRelation: nodeRelation,
			userFilter:     userFilter,
			conditions:     edge.GetConditions(),
		}
		results = b.query(context.Background(), input)
	} else {
		results = emptySequence
	}

	if len(errs) > 0 {
		results = flatten(sequence(errs...), results)
	}
	return results
}

func (b *Backend) handleTTUEdge(edge *Edge, items []Item) iter.Seq[Item] {
	parts := strings.Split(edge.GetTuplesetRelation(), "#")
	tuplesetType := parts[0]
	tuplesetRelation := parts[1]

	tuplesetNode, ok := b.Graph.GetNodeByID(edge.GetTuplesetRelation())
	if !ok {
		panic("tupleset node not in graph")
	}

	edges, ok := b.Graph.GetEdgesFromNode(tuplesetNode)
	if !ok {
		panic("no edges found for tupleset node")
	}

	targetType := strings.Split(edge.GetTo().GetLabel(), "#")[0]

	var targetEdge *Edge

	for _, e := range edges {
		if e.GetTo().GetLabel() == targetType {
			targetEdge = e
			break
		}
	}

	if targetEdge == nil {
		panic("ttu target type is not an edge of tupleset")
	}

	var userFilter []*openfgav1.ObjectRelation

	var errs []Item

	for _, item := range items {
		if item.Err != nil {
			errs = append(errs, item)
			continue
		}

		userFilter = append(userFilter, &openfgav1.ObjectRelation{
			Object:   item.Value,
			Relation: "",
		})
	}

	var results iter.Seq[Item]

	if len(userFilter) > 0 {
		input := queryInput{
			objectType:     tuplesetType,
			objectRelation: tuplesetRelation,
			userFilter:     userFilter,
			conditions:     targetEdge.GetConditions(),
		}
		results = b.query(context.Background(), input)
	} else {
		results = emptySequence
	}

	if len(errs) > 0 {
		results = flatten(sequence(errs...), results)
	}
	return results
}

func handleIdentity(_ *Edge, items []Item) iter.Seq[Item] {
	return sequence(items...)
}

func handleUnsupported(_ *Edge, _ []Item) iter.Seq[Item] {
	panic("unsupported state")
}

func handleLeafNode(node *Node) edgeHandler {
	return func(_ *Edge, items []Item) iter.Seq[Item] {
		objectType := strings.Split(node.GetLabel(), "#")[0]

		results := transform(sequence(items...), func(item Item) Item {
			var value string

			switch node.GetNodeType() {
			case NodeTypeSpecificTypeWildcard:
				value = ""
			case NodeTypeSpecificType, NodeTypeSpecificTypeAndRelation:
				value = ":" + item.Value
			default:
				panic("unsupported leaf node type")
			}
			item.Value = objectType + value
			return item
		})
		return results
	}
}

type omniInterpreter struct {
	hndNil           edgeHandler
	hndDirect        edgeHandler
	hndTTU           edgeHandler
	hndComputed      edgeHandler
	hndRewrite       edgeHandler
	hndDirectLogical edgeHandler
	hndTTULogical    edgeHandler
}

func (o *omniInterpreter) Interpret(edge *Edge, items []Item) iter.Seq[Item] {
	var results iter.Seq[Item]

	if edge == nil {
		results = o.hndNil(edge, items)
		return results
	}

	switch edge.GetEdgeType() {
	case EdgeTypeDirect:
		results = o.hndDirect(edge, items)
	case EdgeTypeTTU:
		results = o.hndTTU(edge, items)
	case EdgeTypeComputed:
		results = o.hndComputed(edge, items)
	case EdgeTypeRewrite:
		results = o.hndRewrite(edge, items)
	case EdgeTypeDirectLogical:
		results = o.hndDirectLogical(edge, items)
	case EdgeTypeTTULogical:
		results = o.hndTTULogical(edge, items)
	default:
		panic("unexpected edge type")
	}
	return results
}

type intersectionResolver struct {
	ctx         context.Context
	interpreter interpreter
	done        bool
	tracker     Tracker
}

func (r *intersectionResolver) Ready() bool {
	return !r.done
}

func (r *intersectionResolver) Resolve(senders []*sender, listeners []*listener) {
	defer func() {
		r.tracker.Add(-1)
		r.done = true
	}()

	r.tracker.Add(1)

	var wg sync.WaitGroup

	objects := make(map[string]struct{})

	buffers := make([]map[string]struct{}, len(senders))

	output := make([]map[string]struct{}, len(senders))

	for i := range senders {
		buffers[i] = make(map[string]struct{})
		output[i] = make(map[string]struct{})
	}

	errs := make([][]Item, len(senders))

	for i, snd := range senders {
		wg.Add(1)

		go func(i int, snd *sender) {
			defer wg.Done()

			for snd.more() && r.ctx.Err() == nil {
				msg, ok := snd.recv()
				if !ok {
					runtime.Gosched()
					continue
				}

				var unseen []Item

				// Deduplicate items within this group based on the buffer for this sender
				for _, item := range msg.Value.Items {
					if item.Err != nil {
						continue
					}

					if _, ok := buffers[i][item.Value]; ok {
						continue
					}
					unseen = append(unseen, item)
					buffers[i][item.Value] = struct{}{}
				}

				// If there are no unseen items, skip processing
				if len(unseen) == 0 {
					msg.Done()
					continue
				}

				results := r.interpreter.Interpret(snd.edge, unseen)

				for item := range results {
					if item.Err != nil {
						errs[i] = append(errs[i], item)
					}
					output[i][item.Value] = struct{}{}
				}
				msg.Done()
			}
		}(i, snd)
	}
	wg.Wait()

	for obj := range output[0] {
		objects[obj] = struct{}{}
	}

	for i := 1; i < len(output); i++ {
		found := make(map[string]struct{})

		for obj := range output[i] {
			if _, ok := objects[obj]; ok {
				found[obj] = struct{}{}
			}
		}
		objects = found
	}

	var allErrs []Item

	for _, errList := range errs {
		allErrs = append(allErrs, errList...)
	}

	seq := flatten(sequence(allErrs...), transform(maps.Keys(objects), strToItem))

	var items []Item

	for item := range seq {
		items = append(items, item)
	}

	outGroup := Group{
		Items: items,
	}

	for _, lst := range listeners {
		lst.send(outGroup)
	}

}

type exclusionResolver struct {
	ctx         context.Context
	interpreter interpreter
	done        bool
	tracker     Tracker
}

func (r *exclusionResolver) Ready() bool {
	return !r.done
}

func (r *exclusionResolver) Resolve(senders []*sender, listeners []*listener) {
	defer func() {
		r.tracker.Add(-1)
		r.done = true
	}()

	r.tracker.Add(1)

	if len(senders) < 2 {
		panic("exclusion resolver requires at least two senders")
	}
	var wg sync.WaitGroup

	included := make(map[string]struct{})
	excluded := make(map[string]struct{})

	errs := make([][]Item, len(senders))

	var mu1 sync.Mutex
	var mu2 sync.Mutex

	for i, snd := range senders {
		wg.Add(1)

		go func(i int, snd *sender) {
			defer wg.Done()

			for snd.more() && r.ctx.Err() == nil {
				msg, ok := snd.recv()
				if !ok {
					runtime.Gosched()
					continue
				}

				results := r.interpreter.Interpret(snd.edge, msg.Value.Items)

				for item := range results {
					if item.Err != nil {
						errs[i] = append(errs[i], item)
						continue
					}
					if i == len(senders)-1 {
						mu2.Lock()
						excluded[item.Value] = struct{}{}
						mu2.Unlock()
					} else {
						mu1.Lock()
						included[item.Value] = struct{}{}
						mu1.Unlock()
					}
				}
				msg.Done()
			}
		}(i, snd)
	}
	wg.Wait()

	for obj := range excluded {
		delete(included, obj)
	}

	var allErrs []Item

	for _, errList := range errs {
		allErrs = append(allErrs, errList...)
	}

	seq := flatten(sequence(allErrs...), transform(maps.Keys(included), strToItem))

	var items []Item

	for item := range seq {
		items = append(items, item)
	}

	outGroup := Group{
		Items: items,
	}

	for _, lst := range listeners {
		lst.send(outGroup)
	}
}

// StatusPool is a struct that aggregates status values, as booleans, from multiple sources
// into a single boolean status value. Each source must register itself using the `Register`
// method and supply the returned value in each call to `Set` when updating the source's status
// value. The default state of a StatusPool is `false` for all sources. All StatusPool methods
// are thread safe.
type StatusPool struct {
	mu   sync.Mutex
	pool []uint64
	top  int
}

// Register is a function that creates a new entry in the StatusPool for a source and returns
// an identifier that is unique within the context of the StatusPool instance. The returned
// integer identifier values are predictable incrementing values beginning at 0. The `Register`
// method is thread safe.
func (sp *StatusPool) Register() int {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	capacity := len(sp.pool)

	if sp.top/64 >= capacity {
		sp.pool = append(sp.pool, 0)
	}
	id := sp.top
	sp.top++
	return id
}

// Set is a function that accepts a registered identifier and a boolean status. The caller must
// provide an integer identifier returned from an initial call to the `Register` function associated
// with the desired source. The `Set` function is thread safe.
func (sp *StatusPool) Set(id int, status bool) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	ndx := id / 64
	pos := uint64(1 << (id % 64))

	if status {
		sp.pool[ndx] |= pos
		return
	}
	sp.pool[ndx] &^= pos
}

// Status is a function that returns the cummulative status of all sources registered within the pool.
// If any registered source's status is set to `true`, the return value of the `Status` function will
// be `true`. The default value is `false`. The `Status` function is thread safe.
func (sp *StatusPool) Status() bool {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	var status uint64

	for _, s := range sp.pool {
		status |= s
	}
	return status != 0
}

type Worker struct {
	status    *StatusPool
	name      string
	senders   []*sender
	listeners []*listener
	resolver  resolver
	tracker   Tracker
	wg        sync.WaitGroup
}

type resolverFactory struct {
	ctx     context.Context
	backend *Backend
}

func (rf *resolverFactory) builder(node *Node) *resolverBuilder {
	return &resolverBuilder{
		ctx:     rf.ctx,
		backend: rf.backend,
		node:    node,
	}
}

type resolverBuilder struct {
	ctx     context.Context
	backend *Backend
	node    *Node
	tracker Tracker
	status  *StatusPool
}

func (b *resolverBuilder) WithTracker(tracker Tracker) *resolverBuilder {
	b.tracker = tracker
	return b
}

func (b *resolverBuilder) WithStatus(status *StatusPool) *resolverBuilder {
	b.status = status
	return b
}

func (b *resolverBuilder) build() resolver {
	var r resolver

	id := b.status.Register()
	b.status.Set(id, true)

	switch b.node.GetNodeType() {
	case NodeTypeSpecificTypeAndRelation:
		omni := &omniInterpreter{
			hndNil:           handleLeafNode(b.node),
			hndDirect:        b.backend.handleDirectEdge,
			hndTTU:           b.backend.handleTTUEdge,
			hndComputed:      handleIdentity,
			hndRewrite:       handleIdentity,
			hndDirectLogical: handleUnsupported,
			hndTTULogical:    handleUnsupported,
		}

		r = &baseResolver{
			id:          id,
			ctx:         b.ctx,
			interpreter: omni,
			status:      b.status,
		}
	case NodeTypeSpecificType:
		omni := &omniInterpreter{
			hndNil:           handleLeafNode(b.node),
			hndDirect:        handleUnsupported,
			hndTTU:           handleUnsupported,
			hndComputed:      handleUnsupported,
			hndRewrite:       handleUnsupported,
			hndDirectLogical: handleUnsupported,
			hndTTULogical:    handleUnsupported,
		}

		r = &baseResolver{
			id:          id,
			ctx:         b.ctx,
			interpreter: omni,
			status:      b.status,
		}
	case NodeTypeSpecificTypeWildcard:
		omni := &omniInterpreter{
			hndNil:           handleLeafNode(b.node),
			hndDirect:        handleUnsupported,
			hndTTU:           handleUnsupported,
			hndComputed:      handleUnsupported,
			hndRewrite:       handleUnsupported,
			hndDirectLogical: handleUnsupported,
			hndTTULogical:    handleUnsupported,
		}

		r = &baseResolver{
			id:          id,
			ctx:         b.ctx,
			interpreter: omni,
			status:      b.status,
		}
	case NodeTypeOperator:
		switch b.node.GetLabel() {
		case weightedGraph.IntersectionOperator:
			omni := &omniInterpreter{
				hndNil:           handleUnsupported,
				hndDirect:        b.backend.handleDirectEdge,
				hndTTU:           b.backend.handleTTUEdge,
				hndComputed:      handleIdentity,
				hndRewrite:       handleIdentity,
				hndDirectLogical: handleIdentity,
				hndTTULogical:    handleIdentity,
			}

			r = &intersectionResolver{
				ctx:         b.ctx,
				interpreter: omni,
				tracker:     b.tracker,
			}
		case weightedGraph.UnionOperator:
			omni := &omniInterpreter{
				hndNil:           handleUnsupported,
				hndDirect:        b.backend.handleDirectEdge,
				hndTTU:           b.backend.handleTTUEdge,
				hndComputed:      handleIdentity,
				hndRewrite:       handleIdentity,
				hndDirectLogical: handleIdentity,
				hndTTULogical:    handleIdentity,
			}

			r = &baseResolver{
				id:          id,
				ctx:         b.ctx,
				interpreter: omni,
				status:      b.status,
			}
		case weightedGraph.ExclusionOperator:
			omni := &omniInterpreter{
				hndNil:           handleUnsupported,
				hndDirect:        b.backend.handleDirectEdge,
				hndTTU:           b.backend.handleTTUEdge,
				hndComputed:      handleIdentity,
				hndRewrite:       handleIdentity,
				hndDirectLogical: handleIdentity,
				hndTTULogical:    handleIdentity,
			}

			r = &exclusionResolver{
				ctx:         b.ctx,
				interpreter: omni,
				tracker:     b.tracker,
			}
		default:
			panic("unsupported operator node for reverse expand worker")
		}
	case NodeTypeLogicalDirectGrouping:
		omni := &omniInterpreter{
			hndNil:           handleUnsupported,
			hndDirect:        b.backend.handleDirectEdge,
			hndTTU:           handleUnsupported,
			hndComputed:      handleUnsupported,
			hndRewrite:       handleUnsupported,
			hndDirectLogical: handleUnsupported,
			hndTTULogical:    handleUnsupported,
		}

		r = &baseResolver{
			id:          id,
			ctx:         b.ctx,
			interpreter: omni,
			status:      b.status,
		}
	case NodeTypeLogicalTTUGrouping:
		omni := &omniInterpreter{
			hndNil:           handleUnsupported,
			hndDirect:        handleUnsupported,
			hndTTU:           b.backend.handleTTUEdge,
			hndComputed:      handleUnsupported,
			hndRewrite:       handleUnsupported,
			hndDirectLogical: handleUnsupported,
			hndTTULogical:    handleUnsupported,
		}

		r = &baseResolver{
			id:          id,
			ctx:         b.ctx,
			interpreter: omni,
			status:      b.status,
		}
	default:
		panic("unsupported node type for reverse expand worker")
	}

	return r
}

func NewWorker(r resolver, tracker Tracker, status *StatusPool) *Worker {
	return &Worker{
		status:   status,
		tracker:  tracker,
		resolver: r,
	}
}

func (w *Worker) Active() bool {
	return w.resolver.Ready() || w.tracker.Load() != 0
}

func (w *Worker) Start() {
	w.wg.Add(1)

	go func() {
		defer w.wg.Done()

		defer func() {
			for _, lst := range w.listeners {
				lst.close()
			}
		}()

		w.resolver.Resolve(w.senders, w.listeners)
	}()
}

func (w *Worker) Close() {
	for _, lst := range w.listeners {
		lst.close()
	}
}

func (w *Worker) Wait() {
	w.wg.Wait()
}

type Tracker interface {
	Add(int64) int64
	Load() int64
}

type echoTracker struct {
	local  atomic.Int64
	parent Tracker
}

func (t *echoTracker) Add(i int64) int64 {
	value := t.local.Add(i)
	if t.parent != nil {
		t.parent.Add(i)
	}
	return value
}

func (t *echoTracker) Load() int64 {
	return t.local.Load()
}

func EchoTracker(parent Tracker) Tracker {
	return &echoTracker{
		parent: parent,
	}
}

type Message[T any] struct {
	Value T
	done  func()
}

func (m *Message[T]) Done() {
	if m.done != nil {
		m.done()
	}
}

type Producer[T any] interface {
	Recv() (Message[T], bool)
	Done() bool
}

type Consumer[T any] interface {
	Send(T)
	Close()
}

type Pipe struct {
	ch      chan Group
	mu      sync.Mutex
	tracker Tracker
	done    bool
}

func (p *Pipe) Send(g Group) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.done {
		// fmt.Printf("CLOSED PIPE %#v", g)
		panic("called Send on a closed Pipe")
	}
	p.tracker.Add(1)
	p.ch <- g
}

func (p *Pipe) Recv() (Message[Group], bool) {
	select {
	case group, ok := <-p.ch:
		if !ok {
			return Message[Group]{}, ok
		}

		fn := func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			p.tracker.Add(-1)
		}
		return Message[Group]{Value: group, done: sync.OnceFunc(fn)}, ok
	default:
	}
	return Message[Group]{}, false
}

func (p *Pipe) Done() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.done && p.tracker.Load() == 0
}

func (p *Pipe) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.done {
		return
	}
	close(p.ch)
	p.done = true
}

// listener is a struct that contains fields relevant to the listening
// end of a pipeline connection.
type listener struct {
	cons Consumer[Group]

	// node is the weighted graph node that is listening.
	node *Node
}

func (lst *listener) send(g Group) {
	lst.cons.Send(g)
}

func (lst *listener) close() {
	lst.cons.Close()
}

// sender is a struct that contains fields relevant to the producing
// end of a pipeline connection.
type sender struct {
	// edge is the weighted graph edge that is producing.
	edge *Edge

	prod Producer[Group]

	// chunkSize is the target number of items to include in each
	// outbound message. A value less than 1 indicates an unlimited
	// number of items per message.
	chunkSize int

	numProcs int
}

func (snd *sender) recv() (Message[Group], bool) {
	return snd.prod.Recv()
}

func (snd *sender) more() bool {
	return !snd.prod.Done()
}

func (w *Worker) Listen(edge *Edge, p Producer[Group], chunkSize int, numProcs int) {
	w.senders = append(w.senders, &sender{
		edge:      edge,
		prod:      p,
		chunkSize: chunkSize,
		numProcs:  numProcs,
	})
}

func (w *Worker) Subscribe(node *Node) *Pipe {
	ch := make(chan Group, 100)

	p := &Pipe{
		ch:      ch,
		tracker: EchoTracker(w.tracker),
	}

	w.listeners = append(w.listeners, &listener{
		cons: p,
		node: node,
	})

	return p
}

type staticProducer struct {
	groups  []Group
	pos     int
	tracker Tracker
}

func (s *staticProducer) Recv() (Message[Group], bool) {
	if s.pos < len(s.groups) {
		value := s.groups[s.pos]
		s.pos++

		fn := func() {
			s.tracker.Add(-1)
		}
		return Message[Group]{Value: value, done: sync.OnceFunc(fn)}, true
	}
	return Message[Group]{}, false
}

func (s *staticProducer) Done() bool {
	return s.pos >= len(s.groups)
}

func StaticProducer(tracker Tracker, groups ...Group) Producer[Group] {
	if tracker != nil {
		tracker.Add(int64(len(groups)))
	}

	return &staticProducer{
		groups:  groups,
		tracker: tracker,
	}
}

type Pipeline struct {
	backend   *Backend
	chunkSize int
	numProcs  int
}

func NewPipeline(backend *Backend, options ...PipelineOption) *Pipeline {
	p := &Pipeline{
		backend:   backend,
		chunkSize: 100,
		numProcs:  3,
	}

	for _, option := range options {
		option(p)
	}
	return p
}

type Target struct {
	node *Node
	id   string
}

type Source *Node

func (pipe *Pipeline) Target(name, identifier string) (Target, bool) {
	if identifier == "*" {
		name += ":*"
		identifier = ""
	}
	targetNode, ok := pipe.backend.Graph.GetNodeByID(name)

	return Target{
		node: targetNode,
		id:   identifier,
	}, ok
}

func (pipe *Pipeline) Source(name, relation string) (Source, bool) {
	sourceNode, ok := pipe.backend.Graph.GetNodeByID(name + "#" + relation)
	return (Source)(sourceNode), ok
}

type PipelineOption func(*Pipeline)

func WithChunkSize(size int) PipelineOption {
	if size < 0 {
		size = 0
	}

	return func(pipe *Pipeline) {
		pipe.chunkSize = size
	}
}

func WithNumProcs(num int) PipelineOption {
	if num < 1 {
		num = 1
	}

	return func(pipe *Pipeline) {
		pipe.numProcs = num
	}
}

type path struct {
	pipe     *Pipeline
	rfactory *resolverFactory
	workers  map[*Node]*Worker
	tracker  atomic.Int64
}

func (p *path) resolve(ctx context.Context, source *Node, target Target, tracker Tracker, status *StatusPool) bool {
	if _, ok := p.workers[source]; ok {
		return true
	}

	if tracker == nil {
		tracker = EchoTracker(&p.tracker)
	}

	if status == nil {
		status = new(StatusPool)
	}

	r := p.rfactory.builder(source).
		WithTracker(tracker).
		WithStatus(status).
		build()

	worker := NewWorker(r, tracker, status)

	p.workers[source] = worker

	switch source.GetNodeType() {
	case NodeTypeSpecificType, NodeTypeSpecificTypeAndRelation:
		if source == target.node {
			// source node is the target node.
			var group Group
			group.Items = []Item{Item{Value: target.id}}
			worker.Listen(nil, StaticProducer(&p.tracker, group), p.pipe.chunkSize, 1) // only one value to consume, so only one processor necessary.
		}
	case NodeTypeSpecificTypeWildcard:
		label := source.GetLabel()
		typePart := strings.Split(label, ":")[0]

		if source == target.node || typePart == target.node.GetLabel() {
			// source node is the target node or has the same type as the target.
			var group Group
			group.Items = []Item{Item{Value: "*"}}
			worker.Listen(nil, StaticProducer(&p.tracker, group), p.pipe.chunkSize, 1) // only one value to consume, so only one processor necessary.
		}
	}

	edges, ok := p.pipe.backend.Graph.GetEdgesFromNode(source)
	if !ok {
		return true
	}

	for _, edge := range edges {
		var track Tracker
		var stat *StatusPool

		isRecursive := len(edge.GetRecursiveRelation()) > 0

		if isRecursive {
			track = worker.tracker
			stat = worker.status
		}

		p.resolve(ctx, edge.GetTo(), target, track, stat)

		worker.Listen(edge, p.workers[edge.GetTo()].Subscribe(source), p.pipe.chunkSize, p.pipe.numProcs)
	}

	return false
}

func (pipe *Pipeline) Build(ctx context.Context, source Source, target Target) iter.Seq[Item] {
	ctx, cancel := context.WithCancel(ctx)

	p := path{
		pipe:    pipe,
		workers: make(map[*Node]*Worker),
		rfactory: &resolverFactory{
			ctx:     ctx,
			backend: pipe.backend,
		},
	}
	p.resolve(ctx, (*Node)(source), target, nil, nil)

	sourceWorker, ok := p.workers[(*Node)(source)]
	if !ok {
		panic("no such source worker")
	}

	var wg sync.WaitGroup

	results := sourceWorker.Subscribe(nil)

	for _, worker := range p.workers {
		worker.Start()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for ctx.Err() == nil {
			var inactiveCount int

			for _, worker := range p.workers {
				if !worker.Active() {
					worker.Close()
					inactiveCount++
				}
			}

			if inactiveCount == len(p.workers) {
				break
			}

			messageCount := p.tracker.Load()

			if messageCount < 1 || ctx.Err() != nil {
				// cancel all running workers
				for _, worker := range p.workers {
					worker.Close()
				}

				// wait for all workers to finish
				for _, worker := range p.workers {
					worker.Wait()
				}
				break
			}

			runtime.Gosched()
		}
	}()

	return func(yield func(Item) bool) {
		defer wg.Wait()
		defer results.Close()
		defer cancel()

		for !results.Done() {
			msg, ok := results.Recv()
			if !ok {
				runtime.Gosched()
				continue
			}

			for _, item := range msg.Value.Items {
				if !yield(item) {
					msg.Done()
					return
				}
			}
			msg.Done()
		}
	}

}

// loopOverEdges iterates over a set of weightedGraphEdges and acts as a dispatcher,
// processing each edge according to its type to continue the reverse expansion process.
//
// While traversing, loopOverEdges appends relation entries to a stack for use in querying after traversal is complete.
// It will continue to dispatch and traverse the graph until it reaches a DirectEdge, which
// leads to a leaf node in the authorization graph. Once a DirectEdge is found, loopOverEdges invokes
// queryForTuples, passing it the stack of relations it constructed on the way to that particular leaf.
//
// For each edge, it creates a new ReverseExpandRequest, preserving the context of the overall query
// but updating the traversal state (the 'stack') based on the edge being processed.
//
// The behavior is determined by the edge type:
//
//   - DirectEdge: This represents a direct path to data. Here we initiate a call to
//     `queryForTuples` to query the datastore for tuples that match the relationship path
//     accumulated in the stack. This is the end of the traversal.
//
//   - ComputedEdge, RewriteEdge, and TTUEdge: These represent indirections in the authorization model.
//     The function modifies the traversal 'stack' to reflect the next relationship that needs to be resolved.
//     It then calls `dispatch` to continue traversing the graph with this new state until it reaches a DirectEdge.
func (c *ReverseExpandQuery) loopOverEdges(
	ctx context.Context,
	req *ReverseExpandRequest,
	edges []*weightedGraph.WeightedAuthorizationModelEdge,
	needsCheck bool,
	resolutionMetadata *ResolutionMetadata,
	resultChan chan<- *ReverseExpandResult,
	sourceUserType string,
) error {
	pool := concurrency.NewPool(ctx, int(c.resolveNodeBreadthLimit))

	for _, edge := range edges {
		newReq := req.clone()
		newReq.weightedEdge = edge

		toNode := edge.GetTo()
		goingToUserset := toNode.GetNodeType() == weightedGraph.SpecificTypeAndRelation

		// Going to a userset presents risk of infinite loop. Checking the edge and the traversal stack
		// ensures we don't perform the same traversal multiple times.
		if goingToUserset {
			key := edge.GetFrom().GetUniqueLabel() + toNode.GetUniqueLabel() + edge.GetTuplesetRelation() + stack.String(newReq.relationStack)
			_, loaded := c.visitedUsersetsMap.LoadOrStore(key, struct{}{})
			if loaded {
				// we've already visited this userset through this edge, exit to avoid an infinite cycle
				continue
			}
		}

		switch edge.GetEdgeType() {
		case weightedGraph.DirectEdge:
			if goingToUserset {
				// Attach the userset relation to the previous stack entry
				//  type team:
				//		define member: [user]
				//	type org:
				//		define teammate: [team#member]
				// A direct edge here is org#teammate --> team#member
				// so if we find team:fga for this user, we need to know to check for
				// team:fga#member when we check org#teammate
				if newReq.relationStack == nil {
					return ErrEmptyStack
				}
				entry, newStack := stack.Pop(newReq.relationStack)
				entry.usersetRelation = tuple.GetRelation(toNode.GetUniqueLabel())

				newStack = stack.Push(newStack, entry)
				newStack = stack.Push(newStack, typeRelEntry{typeRel: toNode.GetUniqueLabel()})
				newReq.relationStack = newStack

				// Now continue traversing
				pool.Go(func(ctx context.Context) error {
					return c.dispatch(ctx, newReq, resultChan, needsCheck, resolutionMetadata)
				})
				continue
			}

			// We have reached a leaf node in the graph (e.g. `user` or `user:*`),
			// and the traversal for this path is complete. Now we use the stack of relations
			// we've built to query the datastore for matching tuples.
			pool.Go(func(ctx context.Context) error {
				return c.queryForTuples(
					ctx,
					newReq,
					needsCheck,
					resultChan,
					"",
				)
			})
		case weightedGraph.ComputedEdge:
			// A computed edge is an alias (e.g., `define viewer: editor`).
			// We replace the current relation on the stack (`viewer`) with the computed one (`editor`),
			// as tuples are only written against `editor`.
			if toNode.GetNodeType() != weightedGraph.OperatorNode {
				if newReq.relationStack == nil {
					return ErrEmptyStack
				}
				_, newStack := stack.Pop(newReq.relationStack)
				newStack = stack.Push(newStack, typeRelEntry{typeRel: toNode.GetUniqueLabel()})
				newReq.relationStack = newStack
			}

			pool.Go(func(ctx context.Context) error {
				return c.dispatch(ctx, newReq, resultChan, needsCheck, resolutionMetadata)
			})
		case weightedGraph.TTUEdge:
			// Replace the existing type#rel on the stack with the tuple-to-userset relation:
			//
			// 	type document
			//		define parent: [folder]
			//		define viewer: admin from parent
			//
			// We need to remove document#viewer from the stack and replace it with the tupleset relation (`document#parent`).
			// Then we have to add the .To() relation `folder#admin`.
			// The stack becomes `[document#parent, folder#admin]`, and on evaluation we will first
			// query for folder#admin, then if folders exist we will see if they are related to
			// any documents as #parent.
			if newReq.relationStack == nil {
				return ErrEmptyStack
			}
			_, newStack := stack.Pop(newReq.relationStack)

			// stack.Push tupleset relation (`document#parent`)
			tuplesetRel := typeRelEntry{typeRel: edge.GetTuplesetRelation()}
			newStack = stack.Push(newStack, tuplesetRel)

			// stack.Push target type#rel (`folder#admin`)
			newStack = stack.Push(newStack, typeRelEntry{typeRel: toNode.GetUniqueLabel()})
			newReq.relationStack = newStack

			pool.Go(func(ctx context.Context) error {
				return c.dispatch(ctx, newReq, resultChan, needsCheck, resolutionMetadata)
			})
		case weightedGraph.RewriteEdge:
			// Behaves just like ComputedEdge above
			// Operator nodes (union, intersection, exclusion) are not real types, they never get added
			// to the stack.
			if toNode.GetNodeType() != weightedGraph.OperatorNode {
				if newReq.relationStack == nil {
					return ErrEmptyStack
				}
				_, newStack := stack.Pop(newReq.relationStack)
				newStack = stack.Push(newStack, typeRelEntry{typeRel: toNode.GetUniqueLabel()})
				newReq.relationStack = newStack

				pool.Go(func(ctx context.Context) error {
					return c.dispatch(ctx, newReq, resultChan, needsCheck, resolutionMetadata)
				})
				// continue to the next edge
				break
			}

			// If the edge is an operator node, we need to handle it differently.
			switch toNode.GetLabel() {
			case weightedGraph.IntersectionOperator:
				err := c.intersectionHandler(pool, newReq, resultChan, toNode, sourceUserType, resolutionMetadata)
				if err != nil {
					return err
				}
			case weightedGraph.ExclusionOperator:
				err := c.exclusionHandler(ctx, pool, newReq, resultChan, toNode, sourceUserType, resolutionMetadata)
				if err != nil {
					return err
				}
			case weightedGraph.UnionOperator:
				pool.Go(func(ctx context.Context) error {
					return c.dispatch(ctx, newReq, resultChan, needsCheck, resolutionMetadata)
				})
			default:
				return fmt.Errorf("unsupported operator node: %s", toNode.GetLabel())
			}
		case weightedGraph.TTULogicalEdge, weightedGraph.DirectLogicalEdge:
			pool.Go(func(ctx context.Context) error {
				return c.dispatch(ctx, newReq, resultChan, needsCheck, resolutionMetadata)
			})
		default:
			return fmt.Errorf("unsupported edge type: %v", edge.GetEdgeType())
		}
	}

	// In order to maintain the current ListObjects behavior, in the case of timeout in reverse_expand_weighted
	// we will return partial results.
	// For more detail, see here: https://openfga.dev/api/service#/Relationship%20Queries/ListObjects
	err := pool.Wait()
	if err != nil {
		var executionError *ExecutionError
		if errors.As(err, &executionError) {
			if errors.Is(executionError.cause, context.Canceled) || errors.Is(executionError.cause, context.DeadlineExceeded) {
				return nil
			}
		}
	}
	return err
}

// queryForTuples performs all datastore-related reverse expansion logic. After a leaf node has been found in loopOverEdges,
// this function works backwards from a specified user (using the stack created in loopOverEdges)
// and an initial relationship edge to find all the objects that the given user has the given relationship with.
//
// This function orchestrates the concurrent execution of individual query jobs. It initializes a memoization
// map (`jobDedupeMap`) to prevent redundant database queries and a job queue to manage pending tasks.
// It kicks off the initial query and then continuously processes jobs from the queue using a concurrency pool
// until all branches leading up from the leaf have been explored.
func (c *ReverseExpandQuery) queryForTuples(
	ctx context.Context,
	req *ReverseExpandRequest,
	needsCheck bool,
	resultChan chan<- *ReverseExpandResult,
	foundObject string,
) error {
	span := trace.SpanFromContext(ctx)

	queryJobQueue := newJobQueue()

	// Now kick off the chain of queries
	items, err := c.executeQueryJob(ctx, queryJob{req: req, foundObject: foundObject}, resultChan, needsCheck)
	if err != nil {
		telemetry.TraceError(span, err)
		return err
	}

	// Populate the jobQueue with the initial jobs
	queryJobQueue.enqueue(items...)

	// We could potentially have c.resolveNodeBreadthLimit active routines reaching this point.
	// Limit querying routines to avoid explosion of routines.
	pool := concurrency.NewPool(ctx, int(c.resolveNodeBreadthLimit))

	for !queryJobQueue.Empty() {
		job, ok := queryJobQueue.dequeue()
		if !ok {
			// this shouldn't be possible
			return nil
		}

		// Each goroutine will take its first job from the original queue above
		// and then continue generating and processing jobs until there are no more.
		pool.Go(func(ctx context.Context) error {
			localQueue := newJobQueue()
			localQueue.enqueue(job)

			// While this goroutine's queue has items, keep looking for more
			for !localQueue.Empty() {
				nextJob, ok := localQueue.dequeue()
				if !ok {
					break
				}
				newItems, err := c.executeQueryJob(ctx, nextJob, resultChan, needsCheck)
				if err != nil {
					return err
				}
				localQueue.enqueue(newItems...)
			}

			return nil
		})
	}

	err = pool.Wait()
	if err != nil {
		telemetry.TraceError(span, err)
		return err
	}

	return nil
}

// executeQueryJob represents a single recursive step in the reverse expansion query process.
// It takes a `queryJob`, which encapsulates the current state of the traversal (found object,
// and the reverse expand request with its relation stack).
// The method constructs a database query based on the current relation at the top of the stack
// and the `foundObject` from the previous step. It queries the datastore, and for each result:
//   - If the relation stack is empty, it means a candidate object has been found, which is then sent to `resultChan`.
//   - If matching tuples are found, it prepares new `queryJob` instances to continue the traversal further up the graph,
//     using the newly found object as the `foundObject` for the next step.
//   - If no matching objects are found in the datastore, this branch of reverse expand is a dead end, and no more jobs are needed.
func (c *ReverseExpandQuery) executeQueryJob(
	ctx context.Context,
	job queryJob,
	resultChan chan<- *ReverseExpandResult,
	needsCheck bool,
) ([]queryJob, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Ensure we're always working with a copy
	currentReq := job.req.clone()

	userFilter, err := buildUserFilter(currentReq, job.foundObject)
	if err != nil {
		return nil, err
	}

	if currentReq.relationStack == nil {
		return nil, ErrEmptyStack
	}

	// Now pop the top relation off of the stack for querying
	entry, newStack := stack.Pop(currentReq.relationStack)
	typeRel := entry.typeRel

	currentReq.relationStack = newStack

	objectType, relation := tuple.SplitObjectRelation(typeRel)

	filteredIter, err := c.buildFilteredIterator(ctx, currentReq, objectType, relation, userFilter)
	if err != nil {
		return nil, err
	}
	defer filteredIter.Stop()

	var nextJobs []queryJob

	for {
		tupleKey, err := filteredIter.Next(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}
			return nil, err
		}

		// This will be a "type:id" e.g. "document:roadmap"
		foundObject := tupleKey.GetObject()

		// If there are no more type#rel to look for in the stack that means we have hit the base case
		// and this object is a candidate for return to the user.
		if currentReq.relationStack == nil {
			c.trySendCandidate(ctx, needsCheck, foundObject, resultChan)
			continue
		}

		// For non-recursive relations (majority of cases), if there are more items on the stack, we continue
		// the evaluation one level higher up the tree with the `foundObject`.
		nextJobs = append(nextJobs, queryJob{foundObject: foundObject, req: currentReq})
	}

	return nextJobs, err
}

func buildUserFilter(
	req *ReverseExpandRequest,
	object string,
) ([]*openfgav1.ObjectRelation, error) {
	var filter *openfgav1.ObjectRelation
	// This is true on every call to queryFunc except the first, since we only trigger subsequent
	// calls if we successfully found an object.
	if object != "" {
		if req.relationStack == nil {
			return nil, ErrEmptyStack
		}

		entry := stack.Peek(req.relationStack)
		filter = &openfgav1.ObjectRelation{Object: object}
		if entry.usersetRelation != "" {
			filter.Relation = entry.usersetRelation
		}
	} else {
		// This else block ONLY hits on the first call to queryFunc.
		toNode := req.weightedEdge.GetTo()

		switch toNode.GetNodeType() {
		case weightedGraph.SpecificType: // Direct User Reference. To() -> "user"
			// req.User will always be either a UserRefObject or UserRefTypedWildcard here. Queries that come in for
			// pure usersets do not take this code path. e.g. ListObjects(team:fga#member, document, viewer) will not make it here.
			var userID string
			val, ok := req.User.(*UserRefObject)
			if ok {
				userID = val.Object.GetId()
			} else {
				// It might be a wildcard user, which is ok
				_, ok = req.User.(*UserRefTypedWildcard)
				if !ok {
					return nil, fmt.Errorf("unexpected user type when building User filter: %T", val)
				}
				return []*openfgav1.ObjectRelation{}, nil
			}

			filter = &openfgav1.ObjectRelation{Object: tuple.BuildObject(toNode.GetUniqueLabel(), userID)}

		case weightedGraph.SpecificTypeWildcard: // Wildcard Referece To() -> "user:*"
			filter = &openfgav1.ObjectRelation{Object: toNode.GetUniqueLabel()}
		}
	}

	return []*openfgav1.ObjectRelation{filter}, nil
}

// buildFilteredIterator constructs the iterator used when reverse_expand queries for tuples.
// The returned iterator MUST have .Stop() called on it.
func (c *ReverseExpandQuery) buildFilteredIterator(
	ctx context.Context,
	req *ReverseExpandRequest,
	objectType string,
	relation string,
	userFilter []*openfgav1.ObjectRelation,
) (storage.TupleKeyIterator, error) {
	iter, err := c.datastore.ReadStartingWithUser(ctx, req.StoreID, storage.ReadStartingWithUserFilter{
		ObjectType: objectType,
		Relation:   relation,
		UserFilter: userFilter,
	}, storage.ReadStartingWithUserOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: req.Consistency,
		},
	})
	if err != nil {
		return nil, err
	}

	// filter out invalid tuples yielded by the database iterator
	return storage.NewConditionsFilteredTupleKeyIterator(
		storage.NewFilteredTupleKeyIterator(
			storage.NewTupleKeyIteratorFromTupleIterator(iter),
			validation.FilterInvalidTuples(c.typesystem),
		),
		checkutil.BuildTupleKeyConditionFilter(ctx, req.Context, c.typesystem),
	), nil
}

// findCandidatesForLowestWeightEdge finds the candidate objects for the lowest weight edge for intersection or exclusion.
func (c *ReverseExpandQuery) findCandidatesForLowestWeightEdge(
	pool *concurrency.Pool,
	req *ReverseExpandRequest,
	tmpResultChan chan<- *ReverseExpandResult,
	edge *weightedGraph.WeightedAuthorizationModelEdge,
	sourceUserType string,
	resolutionMetadata *ResolutionMetadata,
) {
	// We need to create a new stack with the top item from the original request's stack
	// and use it to get the candidates for the lowest weight edge.
	// If the edge is a tuple to userset edge, we need to later check the candidates against the
	// original relationStack with the top item removed.
	var topItemStack stack.Stack[typeRelEntry]
	if req.relationStack != nil {
		topItem, newStack := stack.Pop(req.relationStack)
		req.relationStack = newStack
		topItemStack = stack.Push(nil, topItem)
	}

	edges, err := c.typesystem.GetInternalEdges(edge, sourceUserType)
	if err != nil {
		return
	}

	// getting list object candidates from the lowest weight edge and have its result
	// pass through tmpResultChan.
	pool.Go(func(ctx context.Context) error {
		defer close(tmpResultChan)
		// stack with only the top item in it
		newReq := req.clone()
		newReq.relationStack = topItemStack
		err := c.shallowClone().loopOverEdges(
			ctx,
			newReq,
			edges,
			false,
			resolutionMetadata,
			tmpResultChan,
			sourceUserType,
		)
		return err
	})
}

// checkCandidateInfo holds the information (req, userset, relation) needed to construct check request on a candidate object.
type checkCandidateInfo struct {
	req                *ReverseExpandRequest
	userset            *openfgav1.Userset
	relation           string
	isAllowed          bool
	resolutionMetadata *ResolutionMetadata
}

// callCheckForCandidates calls check on the list objects candidate against non lowest weight edges.
func (c *ReverseExpandQuery) callCheckForCandidate(
	ctx context.Context,
	tmpResult *ReverseExpandResult,
	resultChan chan<- *ReverseExpandResult,
	info checkCandidateInfo,
) error {
	info.resolutionMetadata.CheckCounter.Add(1)
	handlerFunc := c.localCheckResolver.CheckRewrite(ctx,
		&graph.ResolveCheckRequest{
			StoreID:              info.req.StoreID,
			AuthorizationModelID: c.typesystem.GetAuthorizationModelID(),
			TupleKey:             tuple.NewTupleKey(tmpResult.Object, info.relation, info.req.User.String()),
			ContextualTuples:     info.req.ContextualTuples,
			Context:              info.req.Context,
			Consistency:          info.req.Consistency,
			RequestMetadata:      graph.NewCheckRequestMetadata(),
		}, info.userset)
	tmpCheckResult, err := handlerFunc(ctx)
	if err != nil {
		operation := "intersection"
		if !info.isAllowed {
			operation = "exclusion"
		}

		return &ExecutionError{
			operation: operation,
			object:    tmpResult.Object,
			relation:  info.relation,
			user:      info.req.User.String(),
			cause:     err,
		}
	}

	// If the allowed value does not match what we expect, we skip this candidate.
	// eg, for intersection we expect the check result to be true
	// and for exclusion we expect the check result to be false.
	if tmpCheckResult.GetAllowed() != info.isAllowed {
		return nil
	}

	// If the original stack only had 1 value, we can trySendCandidate right away (nothing more to check)
	if stack.Len(info.req.relationStack) == 0 {
		c.trySendCandidate(ctx, false, tmpResult.Object, resultChan)
		return nil
	}

	// If the original stack had more than 1 value, we need to query the parent values
	// new stack with top item in stack
	err = c.queryForTuples(ctx, info.req, false, resultChan, tmpResult.Object)
	if err != nil {
		return err
	}
	return nil
}

// callCheckForCandidates calls check on the list objects candidates against non lowest weight edges.
func (c *ReverseExpandQuery) callCheckForCandidates(
	pool *concurrency.Pool,
	tmpResultChan <-chan *ReverseExpandResult,
	resultChan chan<- *ReverseExpandResult,
	info checkCandidateInfo,
) {
	pool.Go(func(ctx context.Context) error {
		// note that we create a separate goroutine pool instead of the main pool
		// to avoid starvation on the main pool as there could be many candidates
		// arriving concurrently.
		tmpResultPool := concurrency.NewPool(ctx, int(c.resolveNodeBreadthLimit))

		for tmpResult := range tmpResultChan {
			tmpResultPool.Go(func(ctx context.Context) error {
				return c.callCheckForCandidate(ctx, tmpResult, resultChan, info)
			})
		}
		return tmpResultPool.Wait()
	})
}

// invoke loopOverWeightedEdges to get list objects candidate. Check
// will then be invoked on the non-lowest weight edges against these
// list objects candidates. If check returns true, then the list
// object candidates are true candidates and will be returned via
// resultChan. If check returns false, then these list object candidates
// are invalid because it does not satisfy all paths for intersection.
func (c *ReverseExpandQuery) intersectionHandler(
	pool *concurrency.Pool,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	intersectionNode *weightedGraph.WeightedAuthorizationModelNode,
	sourceUserType string,
	resolutionMetadata *ResolutionMetadata,
) error {
	if intersectionNode == nil || intersectionNode.GetNodeType() != weightedGraph.OperatorNode || intersectionNode.GetLabel() != weightedGraph.IntersectionOperator {
		return fmt.Errorf("%w: operation: intersection: %s", errors.ErrUnsupported, "invalid intersection node")
	}

	// verify if the node has weight to the sourceUserType

	edges, err := c.typesystem.GetEdgesFromNode(intersectionNode, sourceUserType)
	if err != nil {
		return err
	}

	// when the intersection node has a weight to the sourceUserType then it means all the group edges has weight to the sourceUserType
	intersectionEdges, err := typesystem.GetEdgesForIntersection(edges, sourceUserType)
	if err != nil {
		return fmt.Errorf("%w: operation: intersection: %s", ErrLowestWeightFail, err.Error())
	}

	// note that we should never see a case where no edges to call LO
	// i.e., len(intersectionEdges.LowestEdges) == 0 or we cannot call check (i.e., len(intersectionEdges.SiblingEdges) == 0)
	// because typesystem.GetEdgesFromNode should have returned an error

	tmpResultChan := make(chan *ReverseExpandResult, listObjectsResultChannelLength)
	intersectEdges := intersectionEdges.SiblingEdges
	usersets := make([]*openfgav1.Userset, 0, len(intersectEdges))

	// the check's relation should be the same for all intersect edges.
	// It is derived from the definition's relation of the intersect edge
	checkRelation := ""
	for _, intersectEdge := range intersectEdges {
		// no matter how many direct edges we have, or ttu edges  they for typesystem only required this
		// no matter how many parent types have for the same ttu rel from parent will be only one created in the typesystem
		// for any other case, does not have more than one edge, the logical groupings only occur in direct edges or ttu edges
		userset, err := c.typesystem.ConstructUserset(intersectEdge, sourceUserType)
		if err != nil {
			// this should never happen
			return fmt.Errorf("%w: operation: intersection: %s", ErrConstructUsersetFail, err.Error())
		}
		usersets = append(usersets, userset)
		var intersectRelation string
		_, intersectRelation = tuple.SplitObjectRelation(intersectEdge.GetRelationDefinition())
		if checkRelation != "" && checkRelation != intersectRelation {
			// this should never happen
			return fmt.Errorf("%w: operation: intersection: %s", errors.ErrUnsupported, "multiple relations in intersection is not supported")
		}
		checkRelation = intersectRelation
	}

	var userset *openfgav1.Userset
	switch len(usersets) {
	case 0:
		return fmt.Errorf("%w: empty connected edges", ErrConstructUsersetFail) // defensive; should be handled by the early return above
	case 1:
		userset = usersets[0]
	default:
		userset = typesystem.Intersection(usersets...)
	}

	// Concurrently find candidates and call check on them as they are found
	c.findCandidatesForLowestWeightEdge(pool, req, tmpResultChan, intersectionEdges.LowestEdge, sourceUserType, resolutionMetadata)
	c.callCheckForCandidates(pool, tmpResultChan, resultChan,
		checkCandidateInfo{req: req, userset: userset, relation: checkRelation, isAllowed: true, resolutionMetadata: resolutionMetadata})
	return nil
}

// invoke loopOverWeightedEdges to get list objects candidate. Check
// will then be invoked on the excluded edge against these
// list objects candidates. If check returns false, then the list
// object candidates are true candidates and will be returned via
// resultChan. If check returns true, then these list object candidates
// are invalid because it does not satisfy all paths for exclusion.
func (c *ReverseExpandQuery) exclusionHandler(
	ctx context.Context,
	pool *concurrency.Pool,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	exclusionNode *weightedGraph.WeightedAuthorizationModelNode,
	sourceUserType string,
	resolutionMetadata *ResolutionMetadata,
) error {
	if exclusionNode == nil || exclusionNode.GetNodeType() != weightedGraph.OperatorNode || exclusionNode.GetLabel() != weightedGraph.ExclusionOperator {
		return fmt.Errorf("%w: operation: exclusion: %s", errors.ErrUnsupported, "invalid exclusion node")
	}

	// verify if the node has weight to the sourceUserType
	exclusionEdges, err := c.typesystem.GetEdgesFromNode(exclusionNode, sourceUserType)
	if err != nil {
		return err
	}

	edges, err := typesystem.GetEdgesForExclusion(exclusionEdges, sourceUserType)
	if err != nil {
		return fmt.Errorf("%w: operation: exclusion: %s", ErrLowestWeightFail, err.Error())
	}

	// This means the exclusion edge does not have a path to the terminal type.
	// e.g. `B` in `A but not B` is not relevant to this query.
	if edges.ExcludedEdge == nil {
		baseEdges, err := c.typesystem.GetInternalEdges(edges.BaseEdge, sourceUserType)
		if err != nil {
			return fmt.Errorf("%w: operation: exclusion: failed to get base edges: %s", ErrLowestWeightFail, err.Error())
		}

		newReq := req.clone()
		return c.shallowClone().loopOverEdges(
			ctx,
			newReq,
			baseEdges,
			false,
			resolutionMetadata,
			resultChan,
			sourceUserType,
		)
	}

	tmpResultChan := make(chan *ReverseExpandResult, listObjectsResultChannelLength)
	var checkRelation string
	_, checkRelation = tuple.SplitObjectRelation(edges.ExcludedEdge.GetRelationDefinition())
	userset, err := c.typesystem.ConstructUserset(edges.ExcludedEdge, sourceUserType)
	if err != nil {
		// This should never happen.
		return fmt.Errorf("%w: operation: exclusion: %s", ErrConstructUsersetFail, err.Error())
	}

	// Concurrently find candidates and call check on them as they are found
	c.findCandidatesForLowestWeightEdge(pool, req, tmpResultChan, edges.BaseEdge, sourceUserType, resolutionMetadata)
	c.callCheckForCandidates(pool, tmpResultChan, resultChan,
		checkCandidateInfo{req: req, userset: userset, relation: checkRelation, isAllowed: false, resolutionMetadata: resolutionMetadata})
	return nil
}
