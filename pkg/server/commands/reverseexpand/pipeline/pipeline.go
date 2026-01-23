package pipeline

import (
	"context"
	"errors"
	"iter"
	"strings"
	"sync"

	"go.opentelemetry.io/otel"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	weightedGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/checkutil"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
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

// Backend is a struct that serves as a container for all backend elements
// necessary for creating and running a `Pipeline`.
type Backend struct {
	Datastore  storage.RelationshipTupleReader
	StoreID    string
	TypeSystem *typesystem.TypeSystem
	Context    *structpb.Struct
	Graph      *Graph
	Preference openfgav1.ConsistencyPreference
}

type Query struct {
	backend *Backend
	config  Config

	objectType     string
	objectRelation string
	user           string
}

func NewQuery(backend *Backend, options ...Option) *Query {
	var q Query
	q.config = DefaultConfig()
	q.backend = backend

	for _, o := range options {
		o(&q.config)
	}
	return &q
}

func (q *Query) From(targetType string, targetRelation string) *Query {
	q.objectType = targetType
	q.objectRelation = targetRelation
	return q
}

func (q *Query) To(user string) *Query {
	q.user = user
	return q
}

func (q *Query) createInterpreter(ctx context.Context) interpreter {
	validator := combineValidators(
		[]falibleValidator[*openfgav1.TupleKey]{
			falibleValidator[*openfgav1.TupleKey](checkutil.BuildTupleKeyConditionFilter(
				ctx,
				q.backend.Context,
				q.backend.TypeSystem,
			)),
			makeValidatorFalible(
				validator[*openfgav1.TupleKey](validation.FilterInvalidTuples(
					q.backend.TypeSystem,
				)),
			),
		},
	)

	queryEngine := queryEngine{
		q.backend.Datastore,
		q.backend.StoreID,
		q.backend.Preference,
		validator,
	}

	directEdgeHandler := directEdgeHandler{&queryEngine}

	ttuEdgeHandler := ttuEdgeHandler{
		queryEngine: &queryEngine,
		graph:       q.backend.Graph,
	}

	var identityEdgeHandler identityEdgeHandler

	return &edgeInterpreter{
		direct:   &directEdgeHandler,
		ttu:      &ttuEdgeHandler,
		identity: &identityEdgeHandler,
	}
}

type path struct {
	objectNode     *Node
	userNode       *Node
	userIdentifier string
	interpreter    interpreter
	bufferPool     *bufferPool
	cycleGroup     *cycleGroup
}

func (q *Query) resolve(p path, workers workerPool) *worker {
	if w, ok := workers[p.objectNode]; ok {
		return w
	}

	if p.cycleGroup == nil {
		p.cycleGroup = newCycleGroup()
	}

	var w worker
	w.bufferConfig = q.config.BufferConfig

	membership := p.cycleGroup.Join()

	core := resolverCore{
		interpreter: p.interpreter,
		membership:  membership,
		bufferPool:  p.bufferPool,
		numProcs:    q.config.NumProcs,
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

	edges, ok := q.backend.Graph.GetEdgesFromNode(p.objectNode)
	if !ok {
		return &w
	}

	for _, edge := range edges {
		nextPath := p

		if !isCyclical(edge) {
			nextPath.cycleGroup = nil
		}

		nextPath.objectNode = edge.GetTo()

		to := q.resolve(nextPath, workers)
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
func (q *Query) resolveUser() (*userResolution, error) {
	user, userRelation, exists := strings.Cut(q.user, "#")
	userType, userIdentifier, _ := strings.Cut(user, ":")

	if exists {
		userType += "#" + userRelation
	}

	userNode, ok := q.backend.Graph.GetNodeByID(userType)
	if !ok {
		return nil, ErrInvalidUser
	}

	return &userResolution{userNode, userIdentifier}, nil
}

// resolveObjectNode resolves the object type and relation to a graph node.
func (q *Query) resolveObjectNode() (*Node, error) {
	nodeID := q.objectType + "#" + q.objectRelation
	node, ok := q.backend.Graph.GetNodeByID(nodeID)
	if !ok {
		return nil, ErrInvalidObject
	}
	return node, nil
}

// pipeline encapsulates the constructed worker graph and result stream.
type pipeline struct {
	workers workerPool
	results *sender
}

// buildPipeline constructs the worker graph and returns a pipeline ready for execution.
func (q *Query) buildPipeline(ctx context.Context, objectNode, userNode *Node, userIdentifier string) (*pipeline, error) {
	pool := newBufferPool(q.config.ChunkSize)
	interpreter := q.createInterpreter(ctx)
	workers := make(workerPool)

	p := path{
		objectNode:     objectNode,
		userNode:       userNode,
		userIdentifier: userIdentifier,
		interpreter:    interpreter,
		bufferPool:     pool,
	}

	q.resolve(p, workers)

	objectWorker, ok := workers[objectNode]
	if !ok {
		return nil, errors.New("no such source")
	}

	return &pipeline{
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
func (q *Query) startWorkerLifecycle(ctx context.Context, workers workerPool) *workerLifecycle {
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

func drain(s *sender) {
	var msg *message
	for s.Recv(&msg) {
		msg.Done()
	}
}

func (q *Query) iterateOverResults(ctx context.Context, p *pipeline, yield func(Item) bool) {
	ctx, cancel := context.WithCancel(ctx)

	lifecycle := q.startWorkerLifecycle(ctx, p.workers)
	defer lifecycle.Wait()

	defer drain(p.results)
	defer p.results.Close()
	defer cancel()

	buffer := make([]Item, 0, q.config.BufferConfig.Capacity)

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

	if ctx.Err() != nil {
		// Context was canceled so there is no guarantee that all
		// objects have been returned. An error must be signaled
		// here to indicate the possibility of a partial result.
		yield(Item{Err: ctx.Err()})
	}
}

// streamResults returns an iterator that streams results from the pipeline.
func (q *Query) streamResults(ctx context.Context, p *pipeline) iter.Seq[Item] {
	return func(yield func(Item) bool) {
		ctx, span := pipelineTracer.Start(ctx, "pipeline.iterate")
		defer span.End()

		if ctx.Err() != nil {
			// Exit early if the context was already canceled.
			// No goroutines have been created up to this point
			// so no cleanup is necessary.
			return
		}

		q.iterateOverResults(ctx, p, yield)
	}
}

func (q *Query) Execute(ctx context.Context) (iter.Seq[Item], error) {
	if err := q.config.Validate(); err != nil {
		return emptySequence, err
	}

	ctx, span := pipelineTracer.Start(ctx, "pipeline.Query.Execute")
	defer span.End()

	objectNode, err := q.resolveObjectNode()
	if err != nil {
		return emptySequence, err
	}

	userRes, err := q.resolveUser()
	if err != nil {
		return emptySequence, err
	}

	pipeline, err := q.buildPipeline(ctx, objectNode, userRes.userNode, userRes.userIdentifier)
	if err != nil {
		return emptySequence, err
	}

	return q.streamResults(ctx, pipeline), nil
}
