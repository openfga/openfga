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

	// EmptySequence represents an `iter.Seq[Item]` that does nothing.
	emptySequence = func(yield func(Object) bool) {}

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

type Object interface {
	Object() (string, error)
}

type ObjectQuery struct {
	ObjectType string
	Relation   string
	Users      []string
	Conditions []string
}

type ObjectReader interface {
	Read(context.Context, ObjectQuery) iter.Seq[Object]
}

type Spec struct {
	ObjectType string
	Relation   string
	User       string
}

// Pipeline represents a reverse expansion query being built.
// Use the fluent API (From, To) to configure the query, then Execute to run it.
type Pipeline struct {
	graph  *Graph
	reader ObjectReader
	config Config
}

// New creates a new reverse expansion query with the given backend and options.
// The query must be configured with From() and To() before calling Execute().
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

// createInterpreter builds the edge interpreter with validators for this query.
// Combines condition evaluation and type system validation to filter invalid tuples.
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

// path holds the state needed to recursively construct workers from the authorization graph.
type path struct {
	objectNode     *Node
	userNode       *Node
	userIdentifier string
	interpreter    interpreter
	bufferPool     *bufferPool
	cycleGroup     *cycleGroup
}

// resolve recursively constructs workers for the authorization model graph.
// Creates workers on-demand and wires them together based on the graph structure.
// Detects cycles and groups cyclical workers for coordinated shutdown.
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
	w.bufferConfig = pl.config.BufferConfig

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
			// object node is the user node or has the same type as the user.
			w.listenForInitialValue(typePart+":*", membership)
		}
	}

	edges, ok := pl.graph.GetEdgesFromNode(p.objectNode)
	if !ok {
		return &w
	}

	// Wire this worker to upstream workers by traversing outgoing edges.
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

// userResolution contains the result of resolving a user string to a graph node and identifier.
type userResolution struct {
	userNode       *Node
	userIdentifier string
}

// resolveUser parses the user string and resolves it to a graph node.
// Returns the user node and the user identifier component.
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

// resolveObjectNode resolves the object type and relation to a graph node.
func (pl *Pipeline) resolveObjectNode(objectType, objectRelation string) (*Node, error) {
	nodeID := objectType + "#" + objectRelation
	node, ok := pl.graph.GetNodeByID(nodeID)
	if !ok {
		return nil, ErrInvalidObject
	}
	return node, nil
}

// expansion encapsulates the constructed worker graph and result stream.
type expansion struct {
	workers workerPool
	results *sender
}

// buildPipeline constructs the worker graph and returns a pipeline ready for execution.
func (pl *Pipeline) buildPipeline(objectNode, userNode *Node, userIdentifier string) (*expansion, error) {
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

// workerLifecycle manages the lifecycle of worker goroutines.
type workerLifecycle struct {
	wg *sync.WaitGroup
}

func (wl *workerLifecycle) Wait() {
	wl.wg.Wait()
}

// startWorkerLifecycle starts goroutines for worker execution and shutdown coordination.
func (pl *Pipeline) startWorkerLifecycle(ctx context.Context, workers workerPool) *workerLifecycle {
	var wg sync.WaitGroup

	// Start workers
	wg.Add(1)
	go func() {
		defer wg.Done()
		workers.Start(ctx)
	}()

	// Monitor context cancellation and trigger shutdown
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		workers.Stop()
	}()

	return &workerLifecycle{&wg}
}

// drain consumes and releases all remaining messages from a sender.
// Called during cleanup to prevent goroutine leaks when shutting down the pipeline.
func drain(s *sender) {
	var msg *message
	for s.Recv(&msg) {
		msg.Done()
	}
}

// iterateOverResults manages the pipeline lifecycle and streams results to the caller.
// Coordinates worker startup, result consumption, and graceful shutdown.
func (pl *Pipeline) iterateOverResults(
	ctx context.Context,
	p *expansion,
	yield func(Object) bool,
) {
	ctx, cancel := context.WithCancel(ctx)

	lifecycle := pl.startWorkerLifecycle(ctx, p.workers)
	defer lifecycle.Wait()

	defer drain(p.results)
	defer p.results.Close()
	defer cancel()

	buffer := make([]Object, 0, pl.config.BufferConfig.Capacity)

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

// streamResults returns an iterator that streams results from the pipeline.
func (pl *Pipeline) streamResults(ctx context.Context, p *expansion) iter.Seq[Object] {
	return func(yield func(Object) bool) {
		ctx, span := pipelineTracer.Start(ctx, "pipeline.iterate")
		defer span.End()

		if ctx.Err() != nil {
			// Exit early if the context was already canceled.
			// No goroutines have been created up to this point
			// so no cleanup is necessary.
			return
		}

		pl.iterateOverResults(ctx, p, yield)
	}
}

// Expand runs the reverse expansion query and returns an iterator of results.
// The iterator yields Items containing either object IDs or errors.
// Results stream as they're discovered; iteration can be stopped early without
// waiting for the full result set.
func (pl *Pipeline) Expand(ctx context.Context, spec Spec) (iter.Seq[Object], error) {
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

	pipeline, err := pl.buildPipeline(objectNode, userRes.userNode, userRes.userIdentifier)
	if err != nil {
		return emptySequence, err
	}

	return pl.streamResults(ctx, pipeline), nil
}
