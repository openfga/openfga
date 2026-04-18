package pipeline

import (
	"context"
	"errors"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	weightedGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/containers/mpsc"
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

	// Receiver is a generic streaming result interface used throughout
	// the pipeline to consume values one at a time without buffering
	// the entire result set.
	Receiver[T any] = worker.Receiver[T]
)

var (
	tracer = otel.Tracer("openfga/internal/listobjects/pipeline")

	edgeTypeComputed      = weightedGraph.ComputedEdge
	edgeTypeDirect        = weightedGraph.DirectEdge
	edgeTypeDirectLogical = weightedGraph.DirectLogicalEdge
	edgeTypeRewrite       = weightedGraph.RewriteEdge
	edgeTypeTTU           = weightedGraph.TTUEdge
	edgeTypeTTULogical    = weightedGraph.TTULogicalEdge

	emptyReceiver = worker.NewEmptyReceiver[Item]()

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
	ErrInvalidStore          = errors.New("store is nil")
	ErrInvalidGraph          = errors.New("graph is nil")
	ErrInvalidBufferCapacity = errors.New("buffer capacity must be a positive number")
	ErrInvalidChunkSize      = errors.New("chunk size must be greater than zero")
	ErrInvalidNumProcs       = errors.New("process number must be greater than zero")
	ErrInvalidObject         = errors.New("invalid object")
	ErrInvalidSubject        = errors.New("invalid subject")
	ErrUnreachable           = errors.New("no path exists")
)

// ObjectQuery describes a reverse lookup: find objects of ObjectType where
// any of Users holds Relation, optionally constrained by Conditions.
type ObjectQuery struct {
	ObjectType string
	Relation   string
	Users      []string
	Conditions []string
}

// ObjectStore reads relationship tuples from storage.
type ObjectStore interface {
	Read(context.Context, ObjectQuery) Receiver[Item]
}

// Spec identifies the target of a reverse expansion query.
type Spec struct {
	ObjectType     string
	ObjectRelation string
	SubjectType    string
	SubjectID      string
}

// Builder holds infrastructure configuration for constructing Pipelines.
// A single Builder can produce multiple Pipelines concurrently.
type Builder struct {
	store  ObjectStore
	config Config
}

// NewBuilder returns a Builder that uses store for tuple reads.
// Options configure tuning parameters (buffer size, chunk size, concurrency).
func NewBuilder(store ObjectStore, options ...Option) (*Builder, error) {
	if store == nil {
		return nil, ErrInvalidStore
	}

	config := DefaultConfig()
	for _, o := range options {
		o(&config)
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &Builder{store: store, config: config}, nil
}

// Pipeline holds the state for a single reverse expansion query.
type Pipeline struct {
	output  worker.Sender
	workers map[string]worker.Worker
	errs    *mpsc.Accumulator[error]
	wg      *sync.WaitGroup
	cancel  context.CancelFunc
	buffer  []string
	pos     int
	err     error
	closed  bool
}

// createInterpreter returns an Interpreter that dispatches to the
// appropriate edge handler (direct, TTU, or identity) based on edge type.
func createInterpreter(graph *Graph, store ObjectStore) worker.Interpreter {
	directEdgeHandler := directEdgeHandler{store}

	ttuEdgeHandler := ttuEdgeHandler{
		reader: store,
		graph:  graph,
	}

	var identityEdgeHandler identityEdgeHandler

	return &edgeInterpreter{
		direct:   &directEdgeHandler,
		ttu:      &ttuEdgeHandler,
		identity: &identityEdgeHandler,
	}
}

// createWorker returns a Worker appropriate for node's type, configured
// with the given core and cycle group. The core is passed by value so
// that each worker gets independent listener/sender slices while sharing
// the pool, error channel, and interpreter via pointers.
func createWorker(node *Node, core worker.Core, group *worker.CycleGroup) worker.Worker {
	var w worker.Worker
	core.Label = node.GetUniqueLabel()

	switch node.GetNodeType() {
	case nodeTypeSpecificType:
		var terminal worker.Terminal
		terminal.Core = &core
		w = &terminal
	case nodeTypeSpecificTypeWildcard:
		var wildcard worker.Wildcard
		wildcard.Core = &core
		w = &wildcard
	case nodeTypeSpecificTypeAndRelation,
		nodeTypeLogicalDirectGrouping,
		nodeTypeLogicalTTUGrouping:
		var basic worker.Basic
		basic.Membership = group.Join(core.Label)
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
		switch node.GetLabel() {
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
			basic.Membership = group.Join(core.Label)
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
	return w
}

// stacker is an intrusive linked-list node used as the DFS stack during
// pipeline construction.
type stacker struct {
	node  *Node
	edge  *Edge
	group *worker.CycleGroup
	next  *stacker
}

// Build constructs a Pipeline for the given graph and spec, starts all
// workers, injects the subject identifier, and closes the input stream.
// The caller must call Close on the returned Pipeline to avoid leaking
// goroutines.
func (b *Builder) Build(
	ctx context.Context,
	graph *Graph,
	spec Spec,
) (*Pipeline, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if graph == nil {
		return nil, ErrInvalidGraph
	}

	config := b.config

	_, span := tracer.Start(ctx, "pipeline.Build", trace.WithAttributes(
		attribute.String("object_type", spec.ObjectType),
		attribute.String("object_relation", spec.ObjectRelation),
		attribute.String("subject_type", spec.SubjectType),
		attribute.String("subject_id", spec.SubjectID),
		attribute.Int("buffer_capacity", config.BufferCapacity),
		attribute.Int("chunk_size", config.ChunkSize),
		attribute.Int("num_procs", config.NumProcs),
	))
	defer span.End()

	buffer := make([]string, 0, config.ChunkSize)
	errs := mpsc.NewAccumulator[error]()

	var core worker.Core
	core.Interpreter = createInterpreter(graph, b.store)
	core.Errors = errs
	core.ChunkSize = config.ChunkSize
	core.NumProcs = config.NumProcs
	core.Pool = new(worker.MessagePool)

	workers := make(map[string]worker.Worker)
	visited := make(map[*Edge]struct{})

	object := spec.ObjectType + "#" + spec.ObjectRelation
	subject := spec.SubjectType
	wildcard := spec.SubjectType + ":*"

	n, ok := graph.GetNodeByID(object)
	if !ok {
		return nil, ErrInvalidObject
	}

	_, ok = graph.GetNodeByID(subject)
	if !ok {
		return nil, ErrInvalidSubject
	}

	var totalListeners int

	var root stacker
	root.node = n

	stack := &root

	for stack != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		current := stack
		stack = stack.next

		label := current.node.GetUniqueLabel()

		if current.edge != nil {
			key := current.edge
			if _, ok := visited[key]; ok {
				continue
			}
			visited[key] = struct{}{}

			if _, ok := key.GetWeight(subject); !ok {
				if _, ok := key.GetWeight(wildcard); !ok {
					if w, ok := workers[key.GetFrom().GetUniqueLabel()]; ok {
						w.Listen(worker.NewNoopMedium(key))
					}
					continue
				}
			}
		}

		var w worker.Worker
		var ok bool

		w, ok = workers[label]
		if !ok {
			if current.group == nil {
				current.group = worker.NewCycleGroup()
			}
			w = createWorker(current.node, core, current.group)
			workers[label] = w
		}

		if current.edge != nil {
			if subscriber, ok := workers[current.edge.GetFrom().GetUniqueLabel()]; ok {
				subscriber.Listen(w.Subscribe(current.edge, config.BufferCapacity))
				totalListeners++
			}
		}

		edges, ok := graph.GetEdgesFromNode(current.node)
		if !ok {
			continue
		}

		for i := len(edges) - 1; i >= 0; i-- {
			var newStack stacker
			newStack.node = edges[i].GetTo()
			newStack.edge = edges[i]
			if worker.IsCyclical(edges[i]) {
				newStack.group = current.group
			}
			newStack.next = stack
			stack = &newStack
		}
	}

	// Size by buffer capacity multiplied by the number of actual listeners.
	// The additional one is included for the pipeline's output channel.
	worker.InitMessagePool(core.Pool, config.ChunkSize, config.BufferCapacity*(totalListeners+1))

	objectWorker, ok := workers[object]
	if !ok {
		return nil, ErrInvalidObject
	}
	// Bind the pipeline's output to the object worker.
	output := objectWorker.Subscribe(nil, config.BufferCapacity)

	subjectWorker, canReachSubject := workers[subject]
	_, canReachWildcard := workers[wildcard]

	if !canReachSubject && !canReachWildcard {
		return nil, ErrUnreachable
	}

	if canReachSubject {
		input := worker.NewStandardMedium(nil, 1)
		subjectWorker.Listen(input)

		if spec.SubjectID != "" {
			msg := worker.Message{Value: []string{spec.SubjectID}}
			if !input.Send(ctx, &msg) {
				msg.Done()
			}
		}
		input.Close()
	}

	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(ctx)

	for _, w := range workers {
		wg.Go(func() {
			var err error
			defer func(err *error) {
				if err != nil && *err != nil {
					errs.Send(*err)
				}
			}(&err)
			defer concurrency.RecoverFromPanic(&err)
			w.Execute(ctx)
		})
	}

	return &Pipeline{
		output:  output,
		workers: workers,
		wg:      &wg,
		cancel:  cancel,
		errs:    errs,
		buffer:  buffer,
	}, nil
}

// Recv returns the next result from the pipeline. It is nil-safe and
// returns ("", false) immediately if the pipeline is nil or has already
// encountered an error. Otherwise it blocks until a value is available,
// draining any buffered values before checking for new errors or output.
// When an error is received from a worker, it is stored and the pipeline
// is automatically closed. When the output stream is exhausted without
// context cancellation, the pipeline is also closed. On context
// cancellation, Recv returns ("", false) without closing, leaving that
// responsibility to the caller. After Recv returns ("", false), call
// Err to distinguish between clean exhaustion and failure.
func (p *Pipeline) Recv(ctx context.Context) (string, bool) {
	if p == nil || p.err != nil {
		return "", false
	}

	for {
		if ctx.Err() != nil {
			return "", false
		}

		// Drain buffered values before checking for errors. Values in
		// the buffer were produced before any concurrently arriving
		// error, so delivering them first preserves ordering.
		if p.pos < len(p.buffer) {
			value := p.buffer[p.pos]
			p.pos++
			return value, true
		}
		p.pos = 0
		p.buffer = p.buffer[:0]

		e, ok := p.errs.TryRecv(ctx)
		if ok {
			p.err = e
			p.Close()
			return "", false
		}

		msg, ok := p.output.Recv(ctx)
		if !ok {
			if ctx.Err() == nil {
				p.Close()
			}
			return "", false
		}
		p.buffer = append(p.buffer, msg.Value...)
		msg.Done()
	}
}

// Err returns the first error encountered during pipeline execution,
// or nil if the pipeline completed successfully. It should be called
// after Recv returns ("", false).
func (p *Pipeline) Err() error {
	return p.err
}

// Close cancels the pipeline context, drains any remaining output
// messages, and waits for all workers to finish. It is nil-safe and
// idempotent: subsequent calls after the first are no-ops. After
// workers complete, it closes and drains the error accumulator,
// storing the first non-context-cancellation error for retrieval
// via Err. Close must be called to avoid leaking goroutines.
func (p *Pipeline) Close() {
	if p == nil || p.closed {
		return
	}
	p.closed = true

	p.cancel()

	for {
		msg, ok := p.output.Recv(context.Background())
		if !ok {
			break
		}
		msg.Done()
	}
	p.wg.Wait()

	p.errs.Close()

	for {
		e, ok := p.errs.TryRecv(context.Background())
		if !ok {
			break
		}
		if p.err == nil &&
			!errors.Is(e, context.Canceled) &&
			!errors.Is(e, context.DeadlineExceeded) {
			p.err = e
		}
	}
}
