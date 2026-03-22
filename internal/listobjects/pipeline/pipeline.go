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
	Edge  = weightedGraph.WeightedAuthorizationModelEdge
	Graph = weightedGraph.WeightedAuthorizationModelGraph
	Node  = weightedGraph.WeightedAuthorizationModelNode

	Item = worker.Item
)

var (
	pipelineTracer = otel.Tracer("pipeline")

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
	pool           *sync.Pool
}

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

type userResolution struct {
	userNode       *Node
	userIdentifier string
}

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

func (pl *Pipeline) resolveObjectNode(objectType, objectRelation string) (*Node, error) {
	nodeID := objectType + "#" + objectRelation
	node, ok := pl.graph.GetNodeByID(nodeID)
	if !ok {
		return nil, ErrInvalidObject
	}
	return node, nil
}

type expansion struct {
	workers map[*Node]worker.Worker
	errors  chan error
	results worker.Sender
}

func (e *expansion) Cleanup() {
	for _, worker := range e.workers {
		worker.Cleanup()
	}
	close(e.errors)
}

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
		pool: &sync.Pool{
			New: func() any {
				a := make([]string, 0, pl.config.ChunkSize)
				return &a
			},
		},
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

type workerLifecycle struct {
	wg *sync.WaitGroup
}

func (wl *workerLifecycle) Wait() {
	wl.wg.Wait()
}

func (pl *Pipeline) startWorkerLifecycle(ctx context.Context, workers map[*Node]worker.Worker) *workerLifecycle {
	var wg sync.WaitGroup

	for _, worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker.Execute(ctx)
		}()
	}

	return &workerLifecycle{&wg}
}

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

func (pl *Pipeline) streamResults(ctx context.Context, objectNode, userNode *Node, userIdentifier string) iter.Seq[Item] {
	return func(yield func(Item) bool) {
		_, span := pipelineTracer.Start(ctx, "pipeline.build")
		expansion := pl.buildExpansion(objectNode, userNode, userIdentifier)
		span.End()

		ctx, span := pipelineTracer.Start(ctx, "pipeline.iterate")
		defer span.End()

		// Early exit before any goroutines are created avoids cleanup overhead.
		if ctx.Err() != nil {
			expansion.Cleanup()
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

	ctx, span := pipelineTracer.Start(ctx, "pipeline.Query.Execute")
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
