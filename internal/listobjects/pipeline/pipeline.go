package pipeline

import (
	"context"
	"errors"
	"iter"
	"strings"
	"sync"

	"go.opentelemetry.io/otel"

	weightedGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/listobjects/pipeline/internal/worker"
)

type (
	// Edge is an alias for the weighted authorization model edge type.
	Edge = weightedGraph.WeightedAuthorizationModelEdge
	// Graph is an alias for the weighted authorization model graph type.
	Graph = weightedGraph.WeightedAuthorizationModelGraph
	// Node is an alias for the weighted authorization model node type.
	Node = weightedGraph.WeightedAuthorizationModelNode

	// Item is an alias for a single result from a worker, carrying either
	// a value or an error.
	Item = worker.Item
)

var (
	tracer = otel.Tracer("openfga/internal/listobjects/pipeline")

	edgeTypeComputed      = weightedGraph.ComputedEdge
	edgeTypeDirect        = weightedGraph.DirectEdge
	edgeTypeDirectLogical = weightedGraph.DirectLogicalEdge
	edgeTypeRewrite       = weightedGraph.RewriteEdge
	edgeTypeTTU           = weightedGraph.TTUEdge
	edgeTypeTTULogical    = weightedGraph.TTULogicalEdge

	emptySequence = func(yield func(Item) bool) {}

	nodeTypeLogicalDirectGrouping   = weightedGraph.LogicalDirectGrouping
	nodeTypeLogicalTTUGrouping      = weightedGraph.LogicalTTUGrouping
	nodeTypeOperator                = weightedGraph.OperatorNode
	nodeTypeSpecificType            = weightedGraph.SpecificType
	nodeTypeSpecificTypeAndRelation = weightedGraph.SpecificTypeAndRelation
	nodeTypeSpecificTypeWildcard    = weightedGraph.SpecificTypeWildcard
)

const (
	defaultBufferSize int = 1 << 7
	defaultChunkSize  int = 100
	defaultNumProcs   int = 3
)

var (
	ErrInvalidBufferCapacity = errors.New("buffer capacity must be a positive number")
	ErrInvalidChunkSize      = errors.New("chunk size must be greater than zero")
	ErrInvalidNumProcs       = errors.New("process number must be greater than zero")
	ErrInvalidObject         = errors.New("invalid object")
	ErrInvalidUser           = errors.New("invalid user")
)

// ObjectQuery describes a reverse lookup: find objects of ObjectType where
// any of Users holds Relation, optionally constrained by Conditions.
type ObjectQuery struct {
	ObjectType string
	Relation   string
	Users      []string
	Conditions []string
}

// ObjectReader reads relationship tuples from storage.
type ObjectReader interface {
	Read(context.Context, ObjectQuery) iter.Seq[Item]
}

// Spec identifies the target of a reverse expansion query.
type Spec struct {
	ObjectType string
	Relation   string
	User       string
}

// Pipeline holds the dependencies and configuration for reverse expansion queries.
type Pipeline struct {
	graph  *Graph
	reader ObjectReader
	config Config
}

// New creates a Pipeline with the given graph and reader.
func New(graph *Graph, reader ObjectReader, options ...Option) *Pipeline {
	var pl Pipeline
	pl.graph = graph
	pl.reader = reader
	pl.config = DefaultConfig()

	for _, o := range options {
		o(&pl.config)
	}
	return &pl
}

// createInterpreter returns an Interpreter that dispatches to the
// appropriate edge handler (direct, TTU, or identity) based on edge type.
func (pl *Pipeline) createInterpreter() worker.Interpreter {
	directEdgeHandler := directEdgeHandler{pl.reader}

	ttuEdgeHandler := ttuEdgeHandler{
		reader: pl.reader,
		graph:  pl.graph,
	}

	var identityEdgeHandler identityEdgeHandler

	return &edgeInterpreter{
		direct:   &directEdgeHandler,
		ttu:      &ttuEdgeHandler,
		identity: &identityEdgeHandler,
	}
}

// path carries state through recursive worker construction.
type path struct {
	objectNode     *Node
	userNode       *Node
	userIdentifier string
	cycleGroup     *worker.CycleGroup
	errors         chan error
	interpreter    worker.Interpreter
	pool           *worker.BufferPool
}

// resolve recursively constructs a worker for the given graph node,
// wiring it to upstream workers for each outgoing edge. Workers are
// memoized in the workers map so each node is built at most once.
func (pl *Pipeline) resolve(p path, workers map[*Node]worker.Worker) worker.Worker {
	if w, ok := workers[p.objectNode]; ok {
		return w
	}

	// Create cycle group on first encounter with a cyclical path.
	// All workers in the cycle will share this group for coordinated shutdown.
	if p.cycleGroup == nil {
		p.cycleGroup = worker.NewCycleGroup()
	}

	var w worker.Worker

	var core worker.Core
	core.Label = p.objectNode.GetUniqueLabel()
	core.Errors = p.errors
	core.Interpreter = p.interpreter
	core.ChunkSize = pl.config.ChunkSize
	core.NumProcs = pl.config.NumProcs
	core.Pool = p.pool

	switch p.objectNode.GetNodeType() {
	case nodeTypeSpecificType,
		nodeTypeSpecificTypeAndRelation,
		nodeTypeSpecificTypeWildcard,
		nodeTypeLogicalDirectGrouping,
		nodeTypeLogicalTTUGrouping:
		var basic worker.Basic
		basic.Membership = p.cycleGroup.Join(core.Label)
		basic.Core = &core
		basic.MsgFunc = func(m *worker.Message, e *worker.Edge) {
			if worker.IsCyclical(e) {
				basic.Membership.Inc()
				fn := m.Callback
				m.Callback = func() {
					basic.Membership.Dec()
					if fn != nil {
						fn()
					}
				}
			}
		}
		w = &basic
	case nodeTypeOperator:
		switch p.objectNode.GetLabel() {
		case weightedGraph.IntersectionOperator:
			var intersection worker.Intersection
			intersection.Core = &core
			w = &intersection
		case weightedGraph.ExclusionOperator:
			var difference worker.Difference
			difference.Core = &core
			w = &difference
		case weightedGraph.UnionOperator:
			var basic worker.Basic
			basic.Membership = p.cycleGroup.Join(core.Label)
			basic.Core = &core
			basic.MsgFunc = func(m *worker.Message, e *worker.Edge) {
				if worker.IsCyclical(e) {
					basic.Membership.Inc()
					fn := m.Callback
					m.Callback = func() {
						basic.Membership.Dec()
						if fn != nil {
							fn()
						}
					}
				}
			}
			w = &basic
		default:
			panic("unsupported operator node for pipeline resolver")
		}
	default:
		panic("unsupported node type for pipeline resolver")
	}

	workers[p.objectNode] = w

	// Leaf nodes matching the query user are seeded with an initial message
	// containing the user identifier. This bootstraps the pipeline: workers
	// process this seed and propagate results toward the root.
	switch p.objectNode.GetNodeType() {
	case nodeTypeSpecificType, nodeTypeSpecificTypeAndRelation:
		if p.objectNode == p.userNode {
			objectType, _, _ := strings.Cut(p.userNode.GetLabel(), "#")
			var value string

			switch p.userNode.GetNodeType() {
			case nodeTypeSpecificTypeWildcard:
				// the ':*' is part of the type
			case nodeTypeSpecificType, nodeTypeSpecificTypeAndRelation:
				value = ":" + p.userIdentifier
			}
			fullObject := objectType + value
			items := []string{fullObject}
			m := worker.Message{
				Value: items,
			}
			medium := worker.NewChannelMedium(nil, 1)
			medium.Send(context.Background(), &m)
			medium.Close()
			w.Listen(medium)
		}
	case nodeTypeSpecificTypeWildcard:
		label := p.objectNode.GetLabel()
		typePart, _, _ := strings.Cut(label, ":")

		if p.objectNode == p.userNode || typePart == p.userNode.GetLabel() {
			fullObject := typePart + ":*"
			items := []string{fullObject}
			m := worker.Message{
				Value: items,
			}
			medium := worker.NewChannelMedium(nil, 1)
			medium.Send(context.Background(), &m)
			medium.Close()
			w.Listen(medium)
		}
	}

	edges, ok := pl.graph.GetEdgesFromNode(p.objectNode)
	if !ok {
		return w
	}

	for _, edge := range edges {
		nextPath := p

		// Only cyclical edges continue sharing the cycle group.
		// Non-cyclical edges start fresh, each worker gets its own single-member group.
		if !worker.IsCyclical(edge) {
			nextPath.cycleGroup = nil
		}

		nextPath.objectNode = edge.GetTo()

		to := pl.resolve(nextPath, workers)
		w.Listen(to.Subscribe(edge, pl.config.BufferCapacity))
	}
	return w
}

// userResolution holds the resolved graph node and identifier for a user
// string like "user:alice" or "group:eng#member".
type userResolution struct {
	userNode       *Node
	userIdentifier string
}

// resolveUser parses a user string into its graph node and bare identifier.
// It returns ErrInvalidUser if the user's type is not present in the graph.
func (pl *Pipeline) resolveUser(u string) (*userResolution, error) {
	user, userRelation, exists := strings.Cut(u, "#")
	userType, userIdentifier, _ := strings.Cut(user, ":")

	if exists {
		userType += "#" + userRelation
	}

	userNode, ok := pl.graph.GetNodeByID(userType)
	if !ok {
		return nil, ErrInvalidUser
	}

	return &userResolution{userNode, userIdentifier}, nil
}

// resolveObjectNode looks up the graph node for the given type#relation pair.
// It returns ErrInvalidObject if the node is not present in the graph.
func (pl *Pipeline) resolveObjectNode(objectType, objectRelation string) (*Node, error) {
	nodeID := objectType + "#" + objectRelation
	node, ok := pl.graph.GetNodeByID(nodeID)
	if !ok {
		return nil, ErrInvalidObject
	}
	return node, nil
}

// expansion holds the fully constructed worker graph, the shared error
// channel, and the root worker's output sender.
type expansion struct {
	workers map[*Node]worker.Worker
	errors  chan error
	results worker.Sender
}

// buildExpansion constructs the full worker graph for a reverse expansion
// query, seeds leaf workers with the user identifier, and returns an
// expansion ready to be executed.
func (pl *Pipeline) buildExpansion(objectNode, userNode *Node, userIdentifier string) *expansion {
	interpreter := pl.createInterpreter()
	workers := make(map[*Node]worker.Worker)
	chError := make(chan error, pl.config.BufferCapacity)

	p := path{
		objectNode:     objectNode,
		userNode:       userNode,
		userIdentifier: userIdentifier,
		errors:         chError,
		interpreter:    interpreter,
		pool:           worker.NewBufferPool(pl.config.ChunkSize, pl.config.BufferCapacity),
	}

	pl.resolve(p, workers)

	objectWorker, ok := workers[objectNode]
	if !ok {
		panic("unable to find source worker; if you see this, something is broken in the code.")
	}

	return &expansion{
		workers: workers,
		errors:  chError,
		results: objectWorker.Subscribe(nil, pl.config.BufferCapacity),
	}
}

// workerLifecycle manages the goroutines running each worker's Execute method.
type workerLifecycle struct {
	wg *sync.WaitGroup
}

// Wait blocks until all worker goroutines have returned.
func (wl *workerLifecycle) Wait() {
	wl.wg.Wait()
}

// startWorkerLifecycle launches a goroutine for each worker's Execute method
// and returns a handle that can be used to wait for all of them to finish.
func (pl *Pipeline) startWorkerLifecycle(ctx context.Context, workers map[*Node]worker.Worker) *workerLifecycle {
	var wg sync.WaitGroup

	for _, worker := range workers {
		wg.Go(func() {
			worker.Execute(ctx)
		})
	}

	return &workerLifecycle{&wg}
}

// iterateOverResults starts all workers, drains the root worker's output
// and the error channel into the yield callback, and ensures all
// goroutines are cleaned up on return.
func (pl *Pipeline) iterateOverResults(
	ctx context.Context,
	p *expansion,
	yield func(Item) bool,
) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wgErr sync.WaitGroup
	defer wgErr.Wait()

	lifecycle := pl.startWorkerLifecycle(ctx, p.workers)

	var wg sync.WaitGroup

	chOutput := make(chan Item)

	wgErr.Add(1)
	go func(ch chan Item) {
		defer wgErr.Done()
		defer close(ch)
		defer wg.Wait()

	ErrorLoop:
		for err := range p.errors {
			select {
			case ch <- Item{Err: err}:
			case <-ctx.Done():
				break ErrorLoop
			}
		}
	}(chOutput)

	wg.Add(1)
	go func(ch chan<- Item) {
		defer wg.Done()
		defer close(p.errors)
		defer lifecycle.Wait()

		buffer := make([]string, 0, pl.config.ChunkSize)

	BufferLoop:
		for {
			if len(buffer) == 0 {
				msg, ok := p.results.Recv(ctx)
				if !ok {
					break
				}

				buffer = append(buffer, msg.Value...)
				msg.Done()
				continue
			}

			for _, item := range buffer {
				select {
				case ch <- Item{Value: item}:
				case <-ctx.Done():
					break BufferLoop
				}
			}
			clear(buffer[:cap(buffer)])
			buffer = buffer[:0]
		}
	}(chOutput)

OutputLoop:
	for {
		select {
		case obj, ok := <-chOutput:
			if !ok {
				break OutputLoop
			}

			if !yield(obj) {
				cancel()
				return
			}
		case <-ctx.Done():
			break OutputLoop
		}
	}

	// Context cancellation during iteration means results may be incomplete.
	// Yield an error item to signal partial results to the caller.
	if ctx.Err() != nil {
		yield(Item{Err: ctx.Err()})
	}
}

// streamResults builds the worker graph and returns an iter.Seq that, when
// iterated, executes the pipeline and streams results to the caller.
func (pl *Pipeline) streamResults(ctx context.Context, objectNode, userNode *Node, userIdentifier string) iter.Seq[Item] {
	return func(yield func(Item) bool) {
		_, span := tracer.Start(ctx, "pipeline.build")
		expansion := pl.buildExpansion(objectNode, userNode, userIdentifier)
		span.End()

		ctx, span := tracer.Start(ctx, "pipeline.iterate")
		defer span.End()

		// Early exit before any goroutines are created avoids cleanup overhead.
		if ctx.Err() != nil {
			yield(Item{Err: ctx.Err()})
			return
		}

		pl.iterateOverResults(ctx, expansion, yield)
	}
}

// Expand returns a streaming iterator of objects accessible to the user specified in spec.
// Iteration can be stopped early; the pipeline will clean up resources automatically.
func (pl *Pipeline) Expand(ctx context.Context, spec Spec) (iter.Seq[Item], error) {
	if err := pl.config.Validate(); err != nil {
		return emptySequence, err
	}

	ctx, span := tracer.Start(ctx, "pipeline.Query.Execute")
	defer span.End()

	objectNode, err := pl.resolveObjectNode(spec.ObjectType, spec.Relation)
	if err != nil {
		return emptySequence, err
	}

	userRes, err := pl.resolveUser(spec.User)
	if err != nil {
		return emptySequence, err
	}

	return pl.streamResults(ctx, objectNode, userRes.userNode, userRes.userIdentifier), nil
}
