package pipeline

import (
	"context"
	"errors"
	"iter"
	"strings"
	"sync"

	"go.opentelemetry.io/otel"

	weightedGraph "github.com/openfga/language/pkg/go/graph"
)

type (
	Edge  = weightedGraph.WeightedAuthorizationModelEdge
	Graph = weightedGraph.WeightedAuthorizationModelGraph
	Node  = weightedGraph.WeightedAuthorizationModelNode
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
	ErrInvalidChunkSize = errors.New("chunk size must be greater than zero")
	ErrInvalidNumProcs  = errors.New("process number must be greater than zero")
	ErrInvalidObject    = errors.New("invalid object")
	ErrInvalidUser      = errors.New("invalid user")
)

type ObjectQuery struct {
	ObjectType string
	Relation   string
	Users      []string
	Conditions []string
}

type ObjectReader interface {
	Read(context.Context, ObjectQuery) iter.Seq[Item]
}

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

func (pl *Pipeline) createInterpreter() interpreter {
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
	interpreter    interpreter
	bufferPool     *bufferPool
	cycleGroup     *cycleGroup
}

func (pl *Pipeline) resolve(p path, workers workerPool) *worker {
	if w, ok := workers[p.objectNode]; ok {
		return w
	}

	// Create cycle group on first encounter with a cyclical path.
	// All workers in the cycle will share this group for coordinated shutdown.
	if p.cycleGroup == nil {
		p.cycleGroup = newCycleGroup()
	}

	var w worker
	w.bufferConfig = pl.config.Buffer

	membership := p.cycleGroup.Join()

	core := resolverCore{
		interpreter: p.interpreter,
		membership:  membership,
		bufferPool:  p.bufferPool,
		numProcs:    pl.config.NumProcs,
	}

	w.Resolver = createResolver(p.objectNode, core)

	workers[p.objectNode] = &w

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
			w.listenForInitialValue(objectType+value, membership)
		}
	case nodeTypeSpecificTypeWildcard:
		label := p.objectNode.GetLabel()
		typePart, _, _ := strings.Cut(label, ":")

		if p.objectNode == p.userNode || typePart == p.userNode.GetLabel() {
			w.listenForInitialValue(typePart+":*", membership)
		}
	}

	edges, ok := pl.graph.GetEdgesFromNode(p.objectNode)
	if !ok {
		return &w
	}

	for _, edge := range edges {
		nextPath := p

		// Only cyclical edges continue sharing the cycle group.
		// Non-cyclical edges start fresh, each worker gets its own single-member group.
		if !isCyclical(edge) {
			nextPath.cycleGroup = nil
		}

		nextPath.objectNode = edge.GetTo()

		to := pl.resolve(nextPath, workers)
		w.Listen(to.Subscribe(edge))
	}

	return &w
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
	workers workerPool
	results *sender
}

func (pl *Pipeline) buildExpansion(objectNode, userNode *Node, userIdentifier string) (*expansion, error) {
	pool := newBufferPool(pl.config.ChunkSize)
	interpreter := pl.createInterpreter()
	workers := make(workerPool)

	p := path{
		objectNode:     objectNode,
		userNode:       userNode,
		userIdentifier: userIdentifier,
		interpreter:    interpreter,
		bufferPool:     pool,
	}

	pl.resolve(p, workers)

	objectWorker, ok := workers[objectNode]
	if !ok {
		return nil, errors.New("no such source")
	}

	return &expansion{
		workers: workers,
		results: objectWorker.Subscribe(nil),
	}, nil
}

type workerLifecycle struct {
	wg *sync.WaitGroup
}

func (wl *workerLifecycle) Wait() {
	wl.wg.Wait()
}

func (pl *Pipeline) startWorkerLifecycle(ctx context.Context, workers workerPool) *workerLifecycle {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		workers.Start(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		workers.Stop()
	}()

	return &workerLifecycle{&wg}
}

// drain prevents goroutine leaks by consuming remaining messages during shutdown.
func drain(s *sender) {
	var msg *message
	for s.Recv(&msg) {
		msg.Done()
	}
}

func (pl *Pipeline) iterateOverResults(
	ctx context.Context,
	p *expansion,
	yield func(Item) bool,
) {
	ctx, cancel := context.WithCancel(ctx)

	lifecycle := pl.startWorkerLifecycle(ctx, p.workers)
	defer lifecycle.Wait()

	defer drain(p.results)
	defer p.results.Close()
	defer cancel()

	buffer := make([]Item, 0, pl.config.Buffer.Capacity)

	for {
		if len(buffer) == 0 {
			var msg *message
			if !p.results.Recv(&msg) {
				break
			}
			buffer = append(buffer, msg.Value...)
			msg.Done()
			continue
		}
		item := buffer[0]
		buffer = buffer[1:]

		if !yield(item) {
			return
		}
	}

	// Context cancellation during iteration means results may be incomplete.
	// Yield an error item to signal partial results to the caller.
	if ctx.Err() != nil {
		yield(Item{Err: ctx.Err()})
	}
}

func (pl *Pipeline) streamResults(ctx context.Context, p *expansion) iter.Seq[Item] {
	return func(yield func(Item) bool) {
		ctx, span := pipelineTracer.Start(ctx, "pipeline.iterate")
		defer span.End()

		// Early exit before any goroutines are created avoids cleanup overhead.
		if ctx.Err() != nil {
			return
		}

		pl.iterateOverResults(ctx, p, yield)
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

	pipeline, err := pl.buildExpansion(objectNode, userRes.userNode, userRes.userIdentifier)
	if err != nil {
		return emptySequence, err
	}

	return pl.streamResults(ctx, pipeline), nil
}
