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

func (q *Query) Execute(ctx context.Context) (iter.Seq[Item], error) {
	if err := q.config.Validate(); err != nil {
		return emptySequence, err
	}
	ctx, span := pipelineTracer.Start(ctx, "pipeline.Query.Execute")
	defer span.End()

	objectNode, ok := q.backend.Graph.GetNodeByID(q.objectType + "#" + q.objectRelation)
	if !ok {
		return emptySequence, ErrInvalidObject
	}

	user, userRelation, exists := strings.Cut(q.user, "#")
	userType, userIdentifier, _ := strings.Cut(user, ":")

	if exists {
		userType += "#" + userRelation
	}

	userNode, ok := q.backend.Graph.GetNodeByID(userType)
	if !ok {
		return emptySequence, ErrInvalidUser
	}

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
		return emptySequence, errors.New("no such source")
	}

	results := objectWorker.Subscribe(nil)

	return func(yield func(Item) bool) {
		ctx, span := pipelineTracer.Start(ctx, "pipeline.iterate")
		defer span.End()

		if ctx.Err() != nil {
			// Exit early if the context was already canceled.
			// No goroutines have been created up to this point
			// so no cleanup is necessary.
			return
		}

		// This context is used to end the goroutine that waits on
		// the context to cancel. Without it, that goroutine could
		// run indefinitely.
		ctx, cancel := context.WithCancel(ctx)

		var wg sync.WaitGroup
		defer wg.Wait()

		defer cancel()

		// Workers are started here so that the pipeline does
		// not begin producing objects until the caller has begun
		// to iterate over the sequence. This prevents unnecessary
		// processing in the event that the caller decides not to
		// iterate over the sequence.
		wg.Add(1)
		go func(ctx context.Context, wg *sync.WaitGroup, fn func(context.Context)) {
			defer wg.Done()
			fn(ctx)
		}(ctx, &wg, workers.Start)

		// When the context is canceled, for any reason, the pipeline
		// should shut itself down.
		wg.Add(1)
		go func(ctx context.Context, wg *sync.WaitGroup, fn func()) {
			defer wg.Done()
			<-ctx.Done()
			fn()
		}(ctx, &wg, workers.Stop)

		var abandoned bool

		for msg := range results.Seq() {
			if !abandoned {
				for _, item := range msg.Value {
					if !yield(item) {
						// The caller has ended sequence iteration early.
						// Stop the workers so that the pipeline begins
						// its shutdown process.
						abandoned = true
						cancel()
						break
					}
				}
			}
			msg.Done()
		}

		err := ctx.Err()

		if !abandoned && err != nil {
			// Context was canceled so there is no guarantee that all
			// objects have been returned. An error must be signaled
			// here to indicate the possibility of a partial result.
			yield(Item{Err: err})
		}
	}, nil
}
