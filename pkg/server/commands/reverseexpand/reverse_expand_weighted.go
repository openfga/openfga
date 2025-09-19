package reverseexpand

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"
	"strings"
	"sync"

	aq "github.com/emirpasic/gods/queues/arrayqueue"
	"go.opentelemetry.io/otel/trace"

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

var emptySequence = func(yield func(Item) bool) {}

type (
	Graph = weightedGraph.WeightedAuthorizationModelGraph
	Node  = weightedGraph.WeightedAuthorizationModelNode
	Edge  = weightedGraph.WeightedAuthorizationModelEdge

	EdgeType = weightedGraph.EdgeType
)

var (
	EdgeTypeDirect   = weightedGraph.DirectEdge
	EdgeTypeComputed = weightedGraph.ComputedEdge
	EdgeTypeRewrite  = weightedGraph.RewriteEdge
	EdgeTypeTTU      = weightedGraph.TTUEdge

	NodeTypeSpecificType            = weightedGraph.SpecificType
	NodeTypeSpecificTypeWildcard    = weightedGraph.SpecificTypeWildcard
	NodeTypeSpecificTypeAndRelation = weightedGraph.SpecificTypeAndRelation
	NodeTypeOperator                = weightedGraph.OperatorNode

	ErrNoSuchNode = errors.New("no such node in graph")
	ErrNoSuchPath = errors.New("no path between source and target nodes")
)

var ErrTupleCycle = errors.New("cycle detected in tuples")

type Item struct {
	Value string
	Err   error
}

type element struct {
	node *Node
	next *element
}

func contains(e *element, node *Node) bool {
	if e == nil {
		return false
	}

	next := e

	for next != nil {
		if next.node == node {
			return true
		}
		next = next.next
	}
	return false
}

func sequence[T any](items ...T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, item := range items {
			if !yield(item) {
				return
			}
		}
	}
}

func mergeOrdered[T any](seqs ...iter.Seq[T]) iter.Seq[T] {
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

func mergeUnordered[T any](seqs ...iter.Seq[T]) iter.Seq[T] {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan T)

	var wg sync.WaitGroup

	for _, seq := range seqs {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for item := range seq {
				select {
				case ch <- item:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	var wg2 sync.WaitGroup

	wg2.Add(1)
	go func() {
		defer wg2.Done()
		defer close(ch)
		wg.Wait()
	}()

	return func(yield func(T) bool) {
		defer wg2.Wait()
		defer cancel()

		for {
			select {
			case <-ctx.Done():
				return
			case result, ok := <-ch:
				if !ok {
					return
				}

				if !yield(result) {
					return
				}
			}
		}
	}
}

func transform[T any, U any](seq iter.Seq[T], fn func(T) U) iter.Seq[U] {
	return func(yield func(U) bool) {
		for item := range seq {
			yield(fn(item))
		}
	}
}

func dedup[T comparable](seq iter.Seq[T]) iter.Seq[T] {
	seen := make(map[T]struct{})

	return func(yield func(T) bool) {
		for item := range seq {
			if _, ok := seen[item]; ok {
				continue
			}
			seen[item] = struct{}{}
			yield(item)
		}
	}
}

type canceler struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func tee[T any](seq iter.Seq[T], n int) []iter.Seq[T] {
	seqs := make([]iter.Seq[T], n)

	chans := make([]chan T, n)
	cancelers := make([]canceler, n)

	for i := 0; i < n; i++ {
		chans[i] = make(chan T)

		ctx, cancel := context.WithCancel(context.Background())

		cancelers[i] = canceler{
			ctx:    ctx,
			cancel: cancel,
		}
	}

	go func() {
		defer func() {
			for _, ch := range chans {
				close(ch)
			}
		}()

		var done int

		for value := range seq {
			for i, ch := range chans {
				select {
				case ch <- value:
				case <-cancelers[i].ctx.Done():
					done++
				}
			}

			if done == n {
				return
			}
		}
	}()

	for i := 0; i < n; i++ {
		seqs[i] = func(yield func(T) bool) {
			defer cancelers[i].cancel()

			for item := range seq {
				yield(item)
			}
		}
	}
	return seqs
}

type backend struct {
	datastore storage.RelationshipTupleReader
	storeId   string
}

func (b *backend) query(ctx context.Context, objectType, objectRelation string, userFilter []*openfgav1.ObjectRelation) iter.Seq[Item] {
	ctx, cancel := context.WithCancel(ctx)

	ch := make(chan Item)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer close(ch)
		defer wg.Done()

		it, err := b.datastore.ReadStartingWithUser(
			ctx,
			b.storeId,
			storage.ReadStartingWithUserFilter{
				ObjectType: objectType,
				Relation:   objectRelation,
				UserFilter: userFilter,
			},
			storage.ReadStartingWithUserOptions{},
		)

		if err != nil {
			select {
			case <-ctx.Done():
			case ch <- Item{Err: err}:
			}
			return
		}

		defer it.Stop()

		for {
			t, err := it.Next(ctx)

			if err != nil {
				if err == storage.ErrIteratorDone {
					break
				}

				select {
				case <-ctx.Done():
				case ch <- Item{Err: err}:
				}
				return
			}

			if t == nil {
				continue
			}

			select {
			case <-ctx.Done():
				return
			case ch <- Item{Value: t.GetKey().Object}:
			}
		}
	}()

	return func(yield func(Item) bool) {
		defer wg.Wait()
		defer cancel()

		for {
			select {
			case <-ctx.Done():
				return
			case result, ok := <-ch:
				if !ok {
					return
				}

				if !yield(result) {
					return
				}
			}
		}
	}
}

type listener struct {
	ch  chan iter.Seq[Item]
	ctx context.Context
}

type sender struct {
	edge *Edge
	seq  iter.Seq[iter.Seq[Item]]
}

type resolver interface {
	Resolve(ctx context.Context, senders []sender, listeners []listener)
}

type specificTypeResolver struct {
	backend *backend
	node    *Node
}

func (s *specificTypeResolver) Resolve(ctx context.Context, senders []sender, listeners []listener) {
	if len(senders) == 0 {
		return
	}
	var wg sync.WaitGroup

	for _, snd := range senders {
		wg.Add(1)

		go func(snd sender) {
			defer wg.Done()

			for items := range snd.seq {
				values := transform(items, func(item Item) Item { return Item{Value: s.node.GetLabel() + ":" + item.Value} })

				var seqs []iter.Seq[Item]

				if len(listeners) > 1 {
					seqs = tee(values, len(listeners))
				} else {
					seqs = []iter.Seq[Item]{values}
				}

				for i, lst := range listeners {
					select {
					case lst.ch <- seqs[i]:
					case <-lst.ctx.Done():
					}
				}
			}
		}(snd)
	}
	wg.Wait()
}

type specificTypeAndRelationResolver struct {
	backend *backend
	node    *Node
}

func (r *specificTypeAndRelationResolver) Resolve(ctx context.Context, senders []sender, listeners []listener) {
	if len(senders) == 0 {
		return
	}
	var wg sync.WaitGroup

	for _, snd := range senders {
		wg.Add(1)
		go func(snd sender) {
			defer wg.Done()

			fromLabel := snd.edge.GetRelationDefinition()

			for items := range snd.seq {
				parts := strings.Split(fromLabel, "#")
				nodeType := parts[0]
				nodeRelation := parts[1]

				userParts := strings.Split(r.node.GetLabel(), "#")

				var userRelation string

				if len(userParts) > 1 {
					userRelation = userParts[1]
				}

				var userFilter []*openfgav1.ObjectRelation

				for item := range items {
					userFilter = append(userFilter, &openfgav1.ObjectRelation{
						Object:   item.Value,
						Relation: userRelation,
					})
				}
				results := r.backend.query(ctx, nodeType, nodeRelation, userFilter)

				var seqs []iter.Seq[Item]

				if len(listeners) > 1 {
					seqs = tee(results, len(listeners))
				} else {
					seqs = []iter.Seq[Item]{results}
				}

				for i, lst := range listeners {
					select {
					case lst.ch <- seqs[i]:
					case <-lst.ctx.Done():
					}
				}
			}
		}(snd)
	}
	wg.Wait()
}

type intersectionResolver struct {
	backend *backend
	node    *Node
	// TODO: perhaps add worker's waitgroup here to manage goroutines
}

func (r *intersectionResolver) Resolve(ctx context.Context, senders []sender, listeners []listener) {
	if len(senders) == 0 {
		return
	}
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		objects := make(map[string]struct{})

		buffers := make([]map[string]struct{}, len(senders))

		for i := range senders {
			buffers[i] = make(map[string]struct{})
		}

		var wg sync.WaitGroup

		for i, snd := range senders {
			wg.Add(1)
			go func(i int, snd sender) {
				defer wg.Done()

				for items := range snd.seq {
					for item := range items {
						buffers[i][item.Value] = struct{}{}
					}
				}
			}(i, snd)
		}
		wg.Wait()

		for obj := range buffers[0] {
			objects[obj] = struct{}{}
		}

		for i := 1; i < len(buffers); i++ {
			found := make(map[string]struct{})

			for obj := range buffers[i] {
				if _, ok := objects[obj]; ok {
					found[obj] = struct{}{}
				}
			}
			objects = found
		}

		items := transform(maps.Keys(objects), func(o string) Item { return Item{Value: o} })

		var seqs []iter.Seq[Item]

		if len(listeners) > 1 {
			seqs = tee(items, len(listeners))
		} else {
			seqs = []iter.Seq[Item]{items}
		}

		for i, sub := range listeners {
			select {
			case sub.ch <- seqs[i]:
			case <-sub.ctx.Done():
			}
		}
	}()
	wg.Wait()
}

type Worker struct {
	senders   []sender
	listeners []listener
	resolver  resolver
	wg        sync.WaitGroup
}

func NewWorker(backend *backend, node *Node) *Worker {
	var r resolver

	switch node.GetNodeType() {
	case NodeTypeSpecificTypeAndRelation:
		r = &specificTypeAndRelationResolver{
			backend: backend,
			node:    node,
		}
	case NodeTypeSpecificType:
		r = &specificTypeResolver{
			backend: backend,
			node:    node,
		}
	case NodeTypeOperator:
		switch node.GetLabel() {
		case weightedGraph.IntersectionOperator:
			r = &intersectionResolver{
				backend: backend,
				node:    node,
			}
		default:
			panic("unsupported operator node for reverse expand worker")
		}
	default:
		panic("unsupported node type for reverse expand worker")
	}

	return &Worker{
		resolver: r,
	}
}

func (w *Worker) Start(ctx context.Context) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.resolver.Resolve(ctx, w.senders, w.listeners)
	}()
}

func (w *Worker) Listen(edge *Edge, i iter.Seq[iter.Seq[Item]]) {
	w.senders = append(w.senders, sender{
		edge: edge,
		seq:  i,
	})
}

func (w *Worker) Subscribe(ctx context.Context) iter.Seq[iter.Seq[Item]] {
	ctx, cancel := context.WithCancel(ctx)
	ch := make(chan iter.Seq[Item])

	w.listeners = append(w.listeners, listener{
		ch:  ch,
		ctx: ctx,
	})

	return func(yield func(iter.Seq[Item]) bool) {
		defer w.wg.Wait()
		defer cancel()

		for {
			select {
			case <-ctx.Done():
				return
			case items, ok := <-ch:
				if !ok {
					return
				}

				if !yield(items) {
					return
				}
			}
		}
	}
}

type Traversal struct {
	graph       *Graph
	workers     map[*Node]*Worker
	breadcrumbs map[*Edge]struct{}
}

type Target *Node
type Source *Node

func (t *Traversal) Target(name string) (Target, bool) {
	targetNode, ok := t.graph.GetNodeByID(name)
	return (Target)(targetNode), ok
}

func (t *Traversal) Source(name, relation string) (Source, bool) {
	sourceNode, ok := t.graph.GetNodeByID(name + "#" + relation)
	return (Source)(sourceNode), ok
}

func (t *Traversal) Traverse(target Target, targetIdentifiers ...string) Path {
	return Path{
		traversal:         t,
		target:            (*Node)(target),
		targetIdentifiers: targetIdentifiers,
	}
}

func (p Path) Objects(ctx context.Context, source Source) iter.Seq[Item] {
	return transform(p.resolve(ctx, source), func(item Item) Item {
		if item.Err != nil {
			return item
		}
		parts := strings.Split(item.Value, "#")
		item.Value = parts[0]
		return item
	})
}

func (t Traversal) query(ctx context.Context, objectType, objectRelation string, userFilter []*openfgav1.ObjectRelation) iter.Seq[Item] {
	ctx, cancel := context.WithCancel(ctx)

	ch := make(chan Item)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer close(ch)
		defer wg.Done()

		it, err := t.datastore.ReadStartingWithUser(
			ctx,
			t.storeId,
			storage.ReadStartingWithUserFilter{
				ObjectType: objectType,
				Relation:   objectRelation,
				UserFilter: userFilter,
			},
			storage.ReadStartingWithUserOptions{},
		)

		if err != nil {
			select {
			case <-ctx.Done():
			case ch <- Item{Err: err}:
			}
			return
		}

		defer it.Stop()

		for {
			t, err := it.Next(ctx)

			if err != nil {
				if err == storage.ErrIteratorDone {
					break
				}

				select {
				case <-ctx.Done():
				case ch <- Item{Err: err}:
				}
				return
			}

			if t == nil {
				continue
			}

			select {
			case <-ctx.Done():
				return
			case ch <- Item{Value: t.GetKey().Object}:
			}
		}
	}()

	return func(yield func(Item) bool) {
		defer wg.Wait()
		defer cancel()

		for {
			select {
			case <-ctx.Done():
				return
			case result, ok := <-ch:
				if !ok {
					return
				}

				if !yield(result) {
					return
				}
			}
		}
	}
}

type Path struct {
	traversal         *Traversal
	target            *Node
	targetIdentifiers []string
	breadcrumb        *element
	cycle             bool
	cycleBegin        *Node
	cycleIdentifiers  []string
}

func (p Path) groupEdgesByType(n *Node) map[EdgeType][]*Edge {
	groups := make(map[EdgeType][]*Edge, 4)

	edges, ok := p.traversal.graph.GetEdgesFromNode(n)
	if !ok {
		panic("given node not a member of graph")
	}

	for _, edge := range edges {
		_, ok := edge.GetWeight(p.target.GetLabel())
		if !ok {
			continue
		}
		groups[edge.GetEdgeType()] = append(groups[edge.GetEdgeType()], edge)
	}
	return groups
}

func extractObject(item Item) Item {
	if item.Err != nil {
		return item
	}
	parts := strings.Split(item.Value, "#")
	item.Value = parts[0]
	return item
}

func intersection(ctx context.Context, seqs ...iter.Seq[Item]) iter.Seq[Item] {
	if len(seqs) == 0 {
		return emptySequence
	}

	ctx, cancel := context.WithCancel(ctx)
	ch := make(chan Item)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(ch)

		var errs []Item
		objects := make(map[string]struct{})

		for item := range seqs[0] {
			if item.Err != nil {
				errs = append(errs, item)
				continue
			}
			objects[item.Value] = struct{}{}
		}

		for i := 1; i < len(seqs); i++ {
			seq := seqs[i]

			found := make(map[string]struct{})

			for item := range seq {
				if item.Err != nil {
					errs = append(errs, item)
					continue
				}

				if _, ok := objects[item.Value]; ok {
					found[item.Value] = struct{}{}
				}
			}
			objects = found
		}

		keys := maps.Keys(objects)

		items := mergeOrdered(sequence(errs...), transform(keys, func(o string) Item { return Item{Value: o} }))

		for item := range items {
			select {
			case <-ctx.Done():
				return
			case ch <- item:
			}
		}
	}()

	return func(yield func(Item) bool) {
		defer wg.Wait()
		defer cancel()

		for {
			select {
			case <-ctx.Done():
				return
			case result, ok := <-ch:
				if !ok {
					return
				}

				if !yield(result) {
					return
				}
			}
		}
	}
}

func (p Path) resolve(ctx context.Context, source *Node) iter.Seq[Item] {
	// if graph traversal has encountered the target node, we can return the target identifiers.
	if source == p.target {
		// the target identifiers are typeless, we need to append the target type to the values.
		targetType := strings.Split(p.target.GetLabel(), "#")[0]
		return transform(sequence(p.targetIdentifiers...), func(id string) Item {
			return Item{Value: targetType + ":" + id}
		})
	}

	// if we have already traversed this node on the current path.
	if contains(p.breadcrumb, source) {
		if p.cycleBegin == source {
			return transform(sequence(p.cycleIdentifiers...), func(id string) Item {
				return Item{Value: id}
			})
		}
		p.cycleBegin = source
		p.breadcrumb = nil
	}

	// does the current node have a path to the target node?
	_, ok := source.GetWeight(p.target.GetLabel())
	if !ok {
		return emptySequence
	}

	edges, ok := p.traversal.graph.GetEdgesFromNode(source)
	if !ok {
		panic("given node not a member of graph")
	}

	p.breadcrumb = &element{node: source, next: p.breadcrumb} // add the current node to our breadcrumb trail

	ctx, cancel := context.WithCancel(ctx)

	ch := make(chan Item)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(ch)

		for {
			var results []iter.Seq[Item]

			for _, edge := range edges {
				// if currently traversing a tuple cycle, but the edge is not part of a tuple cycle, skip it.
				// otherwise, all edges will be processed.
				if p.cycle && !edge.GetTo().IsPartOfTupleCycle() {
					continue
				}

				switch edge.GetEdgeType() {
				case EdgeTypeDirect:
					switch edge.GetTo().GetNodeType() {
					case NodeTypeSpecificTypeAndRelation:
						outcome := p.resolve(ctx, edge.GetTo())

						// for our upcoming query, we need the type and relation off of the specific type and relation ancestor node.
						parts := strings.Split(edge.GetRelationDefinition(), "#")
						nodeType := parts[0]
						nodeRelation := parts[1]

						userParts := strings.Split(edge.GetTo().GetLabel(), "#")

						var userRelation string

						if len(userParts) > 1 {
							userRelation = userParts[1]
						}

						var userFilter []*openfgav1.ObjectRelation

						for item := range outcome {
							userFilter = append(userFilter, &openfgav1.ObjectRelation{
								Object:   item.Value,
								Relation: userRelation,
							})
						}
						results = append(results, p.traversal.query(ctx, nodeType, nodeRelation, userFilter))
					case NodeTypeSpecificType, NodeTypeSpecificTypeWildcard:
						outcome := p.resolve(ctx, edge.GetTo())

						parts := strings.Split(edge.GetRelationDefinition(), "#")
						nodeType := parts[0]
						nodeRelation := parts[1]

						var userFilter []*openfgav1.ObjectRelation

						for item := range outcome {
							userFilter = append(userFilter, &openfgav1.ObjectRelation{
								Object:   item.Value,
								Relation: "",
							})
						}
						results = append(results, p.traversal.query(ctx, nodeType, nodeRelation, userFilter))
					default:
						panic("unexpected node type in direct edge resolve")
					}
				case EdgeTypeRewrite:
					results = append(results, p.resolve(ctx, edge.GetTo()))
				case EdgeTypeComputed:
					results = append(results, p.resolve(ctx, edge.GetTo()))
				case EdgeTypeTTU:
					panic("unexpected TTU edge in resolve")
				default:
					panic("unexpected edge type in resolve")
				}
			}

			var objects iter.Seq[Item]

			switch source.GetNodeType() {
			case NodeTypeOperator:
				switch source.GetLabel() {
				case weightedGraph.IntersectionOperator:
					objects = intersection(ctx, results...)
				case weightedGraph.UnionOperator:
					var seq iter.Seq[Item]

					if len(results) < 1 {
						seq = emptySequence
					} else if len(results) > 1 {
						seq = mergeUnordered(results...)
					} else {
						seq = results[0]
					}
					objects = dedup(seq)
				}
			default:
				var seq iter.Seq[Item]

				if len(results) < 1 {
					seq = emptySequence
				} else if len(results) > 1 {
					seq = mergeUnordered(results...)
				} else {
					seq = results[0]
				}
				objects = dedup(seq)
			}

			var values []string

			for item := range objects {
				select {
				case <-ctx.Done():
					return
				case ch <- item:
				}

				values = append(values, item.Value)
			}

			if len(values) == 0 || p.cycleBegin != source {
				break
			}

			p.cycle = true
			p.cycleIdentifiers = values
		}
	}()

	return func(yield func(Item) bool) {
		defer wg.Wait()
		defer cancel()

		for {
			select {
			case <-ctx.Done():
				return
			case result, ok := <-ch:
				if !ok {
					return
				}

				if !yield(result) {
					return
				}
			}
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
				intersectionEdges, err := c.typesystem.GetEdgesFromNode(toNode, sourceUserType)
				if err != nil {
					return err
				}
				err = c.intersectionHandler(pool, newReq, resultChan, intersectionEdges, sourceUserType, resolutionMetadata)
				if err != nil {
					return err
				}
			case weightedGraph.ExclusionOperator:
				exclusionEdges, err := c.typesystem.GetEdgesFromNode(toNode, sourceUserType)
				if err != nil {
					return err
				}
				err = c.exclusionHandler(ctx, pool, newReq, resultChan, exclusionEdges, sourceUserType, resolutionMetadata)
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
	edges []*weightedGraph.WeightedAuthorizationModelEdge,
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

// callCheckForCandidates calls check on the list objects candidate against non lowest weight edges.
func (c *ReverseExpandQuery) callCheckForCandidate(
	ctx context.Context,
	req *ReverseExpandRequest,
	tmpResult *ReverseExpandResult,
	resultChan chan<- *ReverseExpandResult,
	userset *openfgav1.Userset,
	isAllowed bool,
	resolutionMetadata *ResolutionMetadata,
) error {
	resolutionMetadata.CheckCounter.Add(1)
	handlerFunc := c.localCheckResolver.CheckRewrite(ctx,
		&graph.ResolveCheckRequest{
			StoreID:              req.StoreID,
			AuthorizationModelID: c.typesystem.GetAuthorizationModelID(),
			TupleKey:             tuple.NewTupleKey(tmpResult.Object, req.Relation, req.User.String()),
			ContextualTuples:     req.ContextualTuples,
			Context:              req.Context,
			Consistency:          req.Consistency,
			RequestMetadata:      graph.NewCheckRequestMetadata(),
		}, userset)
	tmpCheckResult, err := handlerFunc(ctx)
	if err != nil {
		operation := "intersection"
		if !isAllowed {
			operation = "exclusion"
		}

		return &ExecutionError{
			operation: operation,
			object:    tmpResult.Object,
			relation:  req.Relation,
			user:      req.User.String(),
			cause:     err,
		}
	}

	// If the allowed value does not match what we expect, we skip this candidate.
	// eg, for intersection we expect the check result to be true
	// and for exclusion we expect the check result to be false.
	if tmpCheckResult.GetAllowed() != isAllowed {
		return nil
	}

	// If the original stack only had 1 value, we can trySendCandidate right away (nothing more to check)
	if stack.Len(req.relationStack) == 0 {
		c.trySendCandidate(ctx, false, tmpResult.Object, resultChan)
		return nil
	}

	// If the original stack had more than 1 value, we need to query the parent values
	// new stack with top item in stack
	err = c.queryForTuples(ctx, req, false, resultChan, tmpResult.Object)
	if err != nil {
		return err
	}
	return nil
}

// callCheckForCandidates calls check on the list objects candidates against non lowest weight edges.
func (c *ReverseExpandQuery) callCheckForCandidates(
	pool *concurrency.Pool,
	req *ReverseExpandRequest,
	tmpResultChan <-chan *ReverseExpandResult,
	resultChan chan<- *ReverseExpandResult,
	userset *openfgav1.Userset,
	isAllowed bool,
	resolutionMetadata *ResolutionMetadata,
) {
	pool.Go(func(ctx context.Context) error {
		// note that we create a separate goroutine pool instead of the main pool
		// to avoid starvation on the main pool as there could be many candidates
		// arriving concurrently.
		tmpResultPool := concurrency.NewPool(ctx, int(c.resolveNodeBreadthLimit))

		for tmpResult := range tmpResultChan {
			tmpResultPool.Go(func(ctx context.Context) error {
				return c.callCheckForCandidate(ctx, req, tmpResult, resultChan, userset, isAllowed, resolutionMetadata)
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
	edges []*weightedGraph.WeightedAuthorizationModelEdge,
	sourceUserType string,
	resolutionMetadata *ResolutionMetadata,
) error {
	intersectionEdgeComparison, err := typesystem.GetEdgesForIntersection(edges, sourceUserType)
	if err != nil {
		return fmt.Errorf("%w: operation: intersection: %s", ErrLowestWeightFail, err.Error())
	}

	if !intersectionEdgeComparison.DirectEdgesAreLeastWeight && intersectionEdgeComparison.LowestEdge == nil {
		// no need to go further because list objects must return empty
		return nil
	}

	lowestWeightEdges := []*weightedGraph.WeightedAuthorizationModelEdge{intersectionEdgeComparison.LowestEdge}

	if intersectionEdgeComparison.DirectEdgesAreLeastWeight {
		lowestWeightEdges = intersectionEdgeComparison.DirectEdges
	}

	tmpResultChan := make(chan *ReverseExpandResult, listObjectsResultChannelLength)

	siblings := intersectionEdgeComparison.Siblings
	usersets := make([]*openfgav1.Userset, 0, len(siblings)+1)

	if !intersectionEdgeComparison.DirectEdgesAreLeastWeight && len(intersectionEdgeComparison.DirectEdges) > 0 {
		// direct weight is not the lowest edge. Therefore, need to call check against directly assigned types.
		usersets = append(usersets, typesystem.This())
	}

	for _, sibling := range siblings {
		userset, err := c.typesystem.ConstructUserset(sibling)
		if err != nil {
			// This should never happen.
			return fmt.Errorf("%w: operation: intersection: %s", ErrConstructUsersetFail, err.Error())
		}
		usersets = append(usersets, userset)
	}
	userset := &openfgav1.Userset{
		Userset: &openfgav1.Userset_Intersection{
			Intersection: &openfgav1.Usersets{
				Child: usersets,
			}}}

	// Concurrently find candidates and call check on them as they are found
	c.findCandidatesForLowestWeightEdge(pool, req, tmpResultChan, lowestWeightEdges, sourceUserType, resolutionMetadata)
	c.callCheckForCandidates(pool, req, tmpResultChan, resultChan, userset, true, resolutionMetadata)

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
	edges []*weightedGraph.WeightedAuthorizationModelEdge,
	sourceUserType string,
	resolutionMetadata *ResolutionMetadata,
) error {
	baseEdges, excludedEdge, err := typesystem.GetEdgesForExclusion(edges, sourceUserType)
	if err != nil {
		return fmt.Errorf("%w: operation: exclusion: %s", ErrLowestWeightFail, err.Error())
	}

	// This means the exclusion edge does not have a path to the terminal type.
	// e.g. `B` in `A but not B` is not relevant to this query.
	if excludedEdge == nil {
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

	userset, err := c.typesystem.ConstructUserset(excludedEdge)
	if err != nil {
		// This should never happen.
		return fmt.Errorf("%w: operation: exclusion: %s", ErrConstructUsersetFail, err.Error())
	}

	// Concurrently find candidates and call check on them as they are found
	c.findCandidatesForLowestWeightEdge(pool, req, tmpResultChan, baseEdges, sourceUserType, resolutionMetadata)
	c.callCheckForCandidates(pool, req, tmpResultChan, resultChan, userset, false, resolutionMetadata)

	return nil
}
