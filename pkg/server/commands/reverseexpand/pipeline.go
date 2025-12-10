package reverseexpand

import (
	"context"
	"errors"
	"iter"
	"maps"
	"strings"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	weightedGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/checkutil"
	"github.com/openfga/openfga/internal/seq"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/server/commands/reverseexpand/containers"
	"github.com/openfga/openfga/pkg/server/commands/reverseexpand/pipe"
	"github.com/openfga/openfga/pkg/server/commands/reverseexpand/track"
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

func handleIdentity(_ context.Context, _ *Edge, items []string) iter.Seq[Item] {
	return seq.Transform(seq.Sequence(items...), strToItem)
}

func NewPipeline(backend *Backend, options ...PipelineOption) *Pipeline {
	p := &Pipeline{
		backend:    backend,
		bufferSize: 100,
		chunkSize:  100,
		numProcs:   3,
	}

	for _, option := range options {
		option(p)
	}

	p.bufferPool = newBufferPool(p.chunkSize)
	return p
}

func WithBufferSize(size int) PipelineOption {
	if size < 1 {
		size = 1
	}

	return func(p *Pipeline) {
		p.bufferSize = size
	}
}

func WithChunkSize(size int) PipelineOption {
	if size < 1 {
		size = 1
	}

	return func(p *Pipeline) {
		p.chunkSize = size
	}
}

func WithNumProcs(num int) PipelineOption {
	if num < 1 {
		num = 1
	}

	return func(p *Pipeline) {
		p.numProcs = num
	}
}

type bufferPool struct {
	size int
	pool sync.Pool
}

func (b *bufferPool) Get() []Item {
	return b.pool.Get().([]Item)
}

func (b *bufferPool) Put(buffer []Item) {
	b.pool.Put(buffer)
}

func (b *bufferPool) create() interface{} {
	return make([]Item, b.size)
}

func newBufferPool(size int) *bufferPool {
	var b bufferPool
	b.size = size
	b.pool.New = b.create
	return &b
}

type Message struct {
	Value       []Item
	ReceiptFunc func()
	once        sync.Once
}

func (m *Message) finalize() {
	if m.ReceiptFunc != nil {
		m.ReceiptFunc()
	}
}

func (m *Message) Done() {
	m.once.Do(m.finalize)
	m.Value = nil
	m.ReceiptFunc = nil
}

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

// handleDirectEdge is a function that interprets input on a direct edge and provides output from
// a query to the backend datastore.
func (b *Backend) handleDirectEdge(ctx context.Context, edge *Edge, items []string) iter.Seq[Item] {
	parts := strings.Split(edge.GetRelationDefinition(), "#")
	nodeType := parts[0]
	nodeRelation := parts[1]

	userParts := strings.Split(edge.GetTo().GetLabel(), "#")

	var userRelation string

	if len(userParts) > 1 {
		userRelation = userParts[1]
	}

	userFilter := make([]*openfgav1.ObjectRelation, len(items))

	for i, item := range items {
		userFilter[i] = &openfgav1.ObjectRelation{
			Object:   item,
			Relation: userRelation,
		}
	}

	var results iter.Seq[Item]

	if len(userFilter) > 0 {
		input := queryInput{
			objectType:     nodeType,
			objectRelation: nodeRelation,
			userFilter:     userFilter,
			conditions:     edge.GetConditions(),
		}
		results = b.query(ctx, input)
	} else {
		results = emptySequence
	}

	return results
}

// handleTTUEdge is a function that interprets input on a TTU edge and provides output from
// a query to the backend datastore.
func (b *Backend) handleTTUEdge(ctx context.Context, edge *Edge, items []string) iter.Seq[Item] {
	parts := strings.Split(edge.GetTuplesetRelation(), "#")
	if len(parts) < 2 {
		return seq.Sequence(Item{Err: errors.New("invalid tupleset relation")})
	}
	tuplesetType := parts[0]
	tuplesetRelation := parts[1]

	tuplesetNode, ok := b.Graph.GetNodeByID(edge.GetTuplesetRelation())
	if !ok {
		return seq.Sequence(Item{Err: errors.New("tupleset node not in graph")})
	}

	edges, ok := b.Graph.GetEdgesFromNode(tuplesetNode)
	if !ok {
		return seq.Sequence(Item{Err: errors.New("no edges found for tupleset node")})
	}

	targetParts := strings.Split(edge.GetTo().GetLabel(), "#")
	if len(targetParts) < 1 {
		return seq.Sequence(Item{Err: errors.New("empty edge label")})
	}
	targetType := targetParts[0]

	var targetEdge *Edge

	for _, e := range edges {
		if e.GetTo().GetLabel() == targetType {
			targetEdge = e
			break
		}
	}

	if targetEdge == nil {
		return seq.Sequence(Item{Err: errors.New("ttu target type is not an edge of tupleset")})
	}

	var userFilter []*openfgav1.ObjectRelation

	for _, item := range items {
		userFilter = append(userFilter, &openfgav1.ObjectRelation{
			Object:   item,
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
		results = b.query(ctx, input)
	} else {
		results = emptySequence
	}

	return results
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
			Conditions: input.conditions,
		},
		storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: b.Preference,
			},
		},
	)

	if err != nil {
		cancel()
		return seq.Sequence(Item{Err: err})
	}

	// If more than one element exists, at least one element is guaranteed to be a condition.
	// OR
	// If only one element exists, and it is not `NoCond`, then it is guaranteed to be a condition.
	hasConditions := len(input.conditions) > 1 || (len(input.conditions) > 0 && input.conditions[0] != weightedGraph.NoCond)

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

type chunk struct {
	Values []Item
	Size   int
}

func chunker(items iter.Seq[Item], buffer []Item) iter.Seq[chunk] {
	return func(yield func(chunk) bool) {
		var head int

		for item := range items {
			buffer[head] = item
			head++

			if head >= len(buffer) {
				if !yield(chunk{
					Values: buffer,
					Size:   head,
				}) {
					return
				}
				head = 0
			}
		}

		if head > 0 {
			yield(chunk{
				Values: buffer,
				Size:   head,
			})
		}
	}
}

type mmap[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]V
}

func (m *mmap[K, V]) LoadOrStore(key K, value V) (V, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.m == nil {
		m.m = make(map[K]V)
	}

	v, ok := m.m[key]
	if !ok {
		m.m[key] = value
		return value, ok
	}
	return v, ok
}

func (m *mmap[K, V]) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.m = nil
}

// baseResolver is a struct that implements the `resolver` interface and acts as the standard resolver for most
// workers. A baseResolver handles both recursive and non-recursive edges concurrently. The baseResolver's "ready"
// status will remain `true` until all of its senders that produce input from external sources have finished, and
// there exist no more in-flight messages for the parent worker. When recursive edges exist, the parent worker for
// this resolver type requires its internal watchdog process to initiate a shutdown.
type baseResolver struct {
	// interpreter is an `interpreter` that transforms a sender's input into output which it broadcasts to all
	// of the parent worker's listeners.
	interpreter interpreter

	tracker *track.Tracker

	reporter *track.Reporter

	bufferPool *bufferPool

	numProcs int
}

func (r *baseResolver) process(ctx context.Context, snd Sender[*Edge, *Message], listeners []Listener[*Edge, *Message], inputBuffer *mmap[string, struct{}], outputBuffer *mmap[string, struct{}]) int64 {
	var sentCount int64

	edge := snd.Key()

	edgeTo := "nil"
	edgeFrom := "nil"

	if edge != nil {
		edgeTo = edge.GetTo().GetUniqueLabel()
		edgeFrom = edge.GetFrom().GetUniqueLabel()
	}

	attrs := []attribute.KeyValue{
		attribute.String("edge.to", edgeTo),
		attribute.String("edge.from", edgeFrom),
	}

	var msg *Message

	for snd.Recv(&msg) {
		var results iter.Seq[Item]

		items := make([]Item, 0, len(msg.Value))
		unseen := make([]string, 0, len(msg.Value))

		messageAttrs := make([]attribute.KeyValue, 1, 1+len(attrs))
		messageAttrs[0] = attribute.Int("items.count", len(msg.Value))
		messageAttrs = append(messageAttrs, attrs...)

		ctx, span := pipelineTracer.Start(ctx, "message.received", trace.WithAttributes(messageAttrs...))

		for _, item := range msg.Value {
			if item.Err != nil {
				items = append(items, item)
				continue
			}

			if inputBuffer != nil {
				if _, loaded := inputBuffer.LoadOrStore(item.Value, struct{}{}); !loaded {
					unseen = append(unseen, item.Value)
				}
				continue
			}
			unseen = append(unseen, item.Value)
		}

		results = emptySequence

		if len(unseen) != 0 {
			results = r.interpreter.Interpret(ctx, edge, unseen)
		}

		results = seq.Flatten(seq.Sequence(items...), results)

		results = seq.Filter(results, func(item Item) bool {
			if item.Err != nil {
				return true
			}
			_, loaded := outputBuffer.LoadOrStore(item.Value, struct{}{})
			return !loaded
		})

		buffer := r.bufferPool.Get()
		for i := range chunker(results, buffer) {
			for _, lst := range listeners {
				values := r.bufferPool.Get()
				copy(values, i.Values)
				r.tracker.Add(1)
				msg := Message{
					Value: values[:i.Size],
					ReceiptFunc: func() {
						r.tracker.Dec()
						r.bufferPool.Put(values)
					},
				}
				if !lst.Send(&msg) {
					msg.Done()
				}
			}
			sentCount += int64(i.Size)
		}
		r.bufferPool.Put(buffer)

		msg.Done()
		span.End()
	}
	return sentCount
}

func (r *baseResolver) Resolve(ctx context.Context, senders []Sender[*Edge, *Message], listeners []Listener[*Edge, *Message]) {
	ctx, span := pipelineTracer.Start(ctx, "baseResolver.Resolve")
	defer span.End()

	var outputBuffer mmap[string, struct{}]
	defer outputBuffer.Clear()

	var sentCount atomic.Int64

	// Any senders with a non-recursive edge will be processed in the "standard" queue.
	var wgStandard sync.WaitGroup

	// Any senders with a recursive edge will be processed in the "recursive" queue.
	var wgRecursive sync.WaitGroup

	for _, snd := range senders {
		edge := snd.Key()

		var isCyclical bool

		if edge != nil {
			isCyclical = len(edge.GetRecursiveRelation()) > 0 || edge.IsPartOfTupleCycle()
		}

		if isCyclical {
			var inputBuffer mmap[string, struct{}]

			for range r.numProcs {
				wgRecursive.Add(1)
				go func() {
					defer wgRecursive.Done()
					sentCount.Add(r.process(ctx, snd, listeners, &inputBuffer, &outputBuffer))
				}()
				continue
			}
			continue
		}

		for range r.numProcs {
			wgStandard.Add(1)
			go func() {
				defer wgStandard.Done()
				sentCount.Add(r.process(ctx, snd, listeners, nil, &outputBuffer))
			}()
		}
	}

	// All standard senders are guaranteed to end at some point, with the exception of the presence
	// of a tuple cycle.
	wgStandard.Wait()

	r.reporter.Report(false)

	r.reporter.Wait(func(s bool) bool {
		return !s
	})

	// Once the standard senders have all finished processing, wait until all messages have finished
	// processing.
	r.tracker.Wait(func(i int64) bool {
		return i < 1
	})

	for _, lst := range listeners {
		lst.Close()
	}

	// Recursive senders will process infinitely until the parent worker's watchdog goroutine kills
	// them.
	wgRecursive.Wait()

	span.SetAttributes(attribute.Int64("items.count", sentCount.Load()))
}

type edgeHandler func(context.Context, *Edge, []string) iter.Seq[Item]

type exclusionResolver struct {
	interpreter interpreter
	tracker     *track.Tracker
	reporter    *track.Reporter
	bufferPool  *bufferPool
	numProcs    int
}

func (r *exclusionResolver) process(ctx context.Context, snd Sender[*Edge, *Message], items *containers.Bag[Item], cleanup *containers.Bag[func()]) {
	edge := snd.Key()

	edgeTo := "nil"
	edgeFrom := "nil"

	if edge != nil {
		edgeTo = edge.GetTo().GetUniqueLabel()
		edgeFrom = edge.GetFrom().GetUniqueLabel()
	}

	attrs := []attribute.KeyValue{
		attribute.String("edge.to", edgeTo),
		attribute.String("edge.from", edgeFrom),
	}

	var msg *Message

	for snd.Recv(&msg) {
		r.tracker.Add(1)
		values := r.bufferPool.Get()
		copy(values, msg.Value)
		size := len(msg.Value)
		msg.Done()

		var results iter.Seq[Item]
		unseen := make([]string, 0, size)

		messageAttrs := make([]attribute.KeyValue, 1, 1+len(attrs))
		messageAttrs[0] = attribute.Int("items.count", size)
		messageAttrs = append(messageAttrs, attrs...)

		ctx, span := pipelineTracer.Start(ctx, "message.received", trace.WithAttributes(messageAttrs...))

		for _, item := range values[:size] {
			if item.Err != nil {
				items.Add(item)
				continue
			}
			unseen = append(unseen, item.Value)
		}
		r.bufferPool.Put(values)

		// If there are no unseen items, skip processing.
		if len(unseen) == 0 {
			goto AfterInterpret
		}

		results = r.interpreter.Interpret(ctx, edge, unseen)

		for item := range results {
			items.Add(item)
		}

	AfterInterpret:
		cleanup.Add(r.tracker.Dec)
		span.End()
	}
}

func (r *exclusionResolver) Resolve(ctx context.Context, senders []Sender[*Edge, *Message], listeners []Listener[*Edge, *Message]) {
	ctx, span := pipelineTracer.Start(ctx, "exclusionResolver.Resolve")
	defer span.End()

	defer r.reporter.Report(false)

	if len(senders) != 2 {
		panic("exclusion resolver requires two senders")
	}
	var wg sync.WaitGroup

	var included containers.Bag[Item]
	var excluded containers.Bag[Item]

	var cleanup containers.Bag[func()]

	for range r.numProcs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.process(ctx, senders[0], &included, &cleanup)
		}()
	}

	for range r.numProcs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.process(ctx, senders[1], &excluded, &cleanup)
		}()
	}

	wg.Wait()

	var errs []Item

	exclusions := make(map[string]struct{})

	for item := range excluded.Seq() {
		if item.Err != nil {
			errs = append(errs, item)
			continue
		}
		exclusions[item.Value] = struct{}{}
	}

	results := seq.Filter(included.Seq(), func(item Item) bool {
		if item.Err != nil {
			return true
		}

		_, ok := exclusions[item.Value]
		return !ok
	})

	results = seq.Flatten(seq.Sequence(errs...), results)

	var sentCount int

	buffer := r.bufferPool.Get()
	for i := range chunker(results, buffer) {
		for _, lst := range listeners {
			values := r.bufferPool.Get()
			copy(values, i.Values)
			r.tracker.Add(1)
			msg := Message{
				Value: values[:i.Size],
				ReceiptFunc: func() {
					r.tracker.Dec()
					r.bufferPool.Put(values)
				},
			}
			if !lst.Send(&msg) {
				msg.Done()
			}
			sentCount += i.Size
		}
	}
	r.bufferPool.Put(buffer)

	span.SetAttributes(attribute.Int("items.count", sentCount))

	for fn := range cleanup.Seq() {
		fn()
	}

	for _, lst := range listeners {
		lst.Close()
	}
}

// Item is a struct that contains an object `string` as its `Value` or an
// encountered error as its `Err`. Item is the primary container used to
// communicate values as they pass through a `Pipeline`.
type Item struct {
	Value string
	Err   error
}

// interpreter is an interface that exposes a method for interpreting input for an edge into output.
type interpreter interface {
	Interpret(ctx context.Context, edge *Edge, items []string) iter.Seq[Item]
}

type intersectionResolver struct {
	interpreter interpreter
	tracker     *track.Tracker
	reporter    *track.Reporter
	bufferPool  *bufferPool
	numProcs    int
}

func (r *intersectionResolver) process(ctx context.Context, snd Sender[*Edge, *Message], items *containers.Bag[Item], cleanup *containers.Bag[func()]) {
	edge := snd.Key()

	edgeTo := "nil"
	edgeFrom := "nil"

	if edge != nil {
		edgeTo = edge.GetTo().GetUniqueLabel()
		edgeFrom = edge.GetFrom().GetUniqueLabel()
	}

	attrs := []attribute.KeyValue{
		attribute.String("edge.to", edgeTo),
		attribute.String("edge.from", edgeFrom),
	}

	var msg *Message

	for snd.Recv(&msg) {
		r.tracker.Add(1)
		values := r.bufferPool.Get()
		copy(values, msg.Value)
		size := len(msg.Value)
		msg.Done()

		var results iter.Seq[Item]
		unseen := make([]string, 0, size)

		messageAttrs := make([]attribute.KeyValue, 1, 1+len(attrs))
		messageAttrs[0] = attribute.Int("items.count", size)
		messageAttrs = append(messageAttrs, attrs...)

		ctx, span := pipelineTracer.Start(ctx, "message.received", trace.WithAttributes(messageAttrs...))

		for _, item := range values[:size] {
			if item.Err != nil {
				items.Add(item)
				continue
			}
			unseen = append(unseen, item.Value)
		}
		r.bufferPool.Put(values)

		// If there are no unseen items, skip processing.
		if len(unseen) == 0 {
			goto AfterInterpret
		}

		results = r.interpreter.Interpret(ctx, edge, unseen)

		for item := range results {
			items.Add(item)
		}

	AfterInterpret:
		cleanup.Add(r.tracker.Dec)
		span.End()
	}
}

func (r *intersectionResolver) Resolve(ctx context.Context, senders []Sender[*Edge, *Message], listeners []Listener[*Edge, *Message]) {
	ctx, span := pipelineTracer.Start(ctx, "intersectionResolver.Resolve")
	defer span.End()

	defer r.reporter.Report(false)

	var wg sync.WaitGroup

	bags := make([]containers.Bag[Item], len(senders))

	var cleanup containers.Bag[func()]

	for i, snd := range senders {
		for range r.numProcs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r.process(ctx, snd, &bags[i], &cleanup)
			}()
		}
	}
	wg.Wait()

	var errs []Item

	output := make(map[string]struct{})

	for item := range bags[0].Seq() {
		if item.Err != nil {
			errs = append(errs, item)
			continue
		}
		output[item.Value] = struct{}{}
	}

	for i := 1; i < len(bags); i++ {
		found := make(map[string]struct{}, len(output))
		for item := range bags[i].Seq() {
			if item.Err != nil {
				errs = append(errs, item)
				continue
			}

			if _, ok := output[item.Value]; ok {
				found[item.Value] = struct{}{}
			}
		}
		output = found
	}

	itemSeq := seq.Transform(maps.Keys(output), strToItem)

	itemSeq = seq.Flatten(seq.Sequence(errs...), itemSeq)

	var sentCount int

	buffer := r.bufferPool.Get()
	for i := range chunker(itemSeq, buffer) {
		for _, lst := range listeners {
			values := r.bufferPool.Get()
			copy(values, i.Values)
			r.tracker.Add(1)
			msg := Message{
				Value: values[:i.Size],
				ReceiptFunc: func() {
					r.tracker.Dec()
					r.bufferPool.Put(values)
				},
			}
			if !lst.Send(&msg) {
				msg.Done()
			}
			sentCount += i.Size
		}
	}
	r.bufferPool.Put(buffer)

	span.SetAttributes(attribute.Int("items.count", sentCount))

	for fn := range cleanup.Seq() {
		fn()
	}

	for _, lst := range listeners {
		lst.Close()
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

func (o *omniInterpreter) Interpret(ctx context.Context, edge *Edge, items []string) iter.Seq[Item] {
	var results iter.Seq[Item]

	if edge == nil {
		results = o.hndNil(ctx, edge, items)
		return results
	}

	switch edge.GetEdgeType() {
	case edgeTypeDirect:
		results = o.hndDirect(ctx, edge, items)
	case edgeTypeTTU:
		results = o.hndTTU(ctx, edge, items)
	case edgeTypeComputed:
		results = o.hndComputed(ctx, edge, items)
	case edgeTypeRewrite:
		results = o.hndRewrite(ctx, edge, items)
	case edgeTypeDirectLogical:
		results = o.hndDirectLogical(ctx, edge, items)
	case edgeTypeTTULogical:
		results = o.hndTTULogical(ctx, edge, items)
	default:
		return seq.Sequence(Item{Err: errors.New("unexpected edge type")})
	}
	return results
}

// Pipeline is a struct that is used to construct logical pipelines that traverse all connections
// within a graph from a given source type and relation to a given target type and identifier.
//
// A pipeline consists of a variable number of workers that process data concurrently. Within
// a pipeline, dataflows from the worker that receives the initial input down to any workers
// that have a subscription to its output. Data continues to flow into downstream workers
// through their subscriptions to upstream workers until it arrives at the consumer of the
// pipeline.
type Pipeline struct {
	// provides operations that require interacting with dependencies
	// such as a database or a graph.
	backend *Backend

	// bufferSize is a value that indicates the maximum size of the
	// message buffer that exists between each worker and its subscribers.
	// When a buffer becomes full, send operations on that subscription
	// will block until messages are removed from the buffer or the
	// subscription is closed.
	//
	// The default value is 100. This value can be changed by constructing
	// a pipeline using NewPipeline and providing the option WithBufferSize.
	bufferSize int

	// chunkSize is a value that indicates the maximum size of tuples
	// accummulated from a datastore query before sending the tuples
	// as a message to the next node in the pipeline.
	//
	// As an example, if the chunkSize is set to 100, then a new message
	// is sent for every 100 tuples returned from a datastore query.
	//
	// The default value is 100. This value can be changed by constructing
	// a pipeline using NewPipeline and providing the option WithChunkSize.
	chunkSize int

	bufferPool *bufferPool

	// numProcs is a value that indicates the maximum number of goroutines
	// that will be allocated to processing each subscription for a pipeline
	// worker.
	//
	// The default value is 3. This value can be changed by constructing
	// a pipeline using NewPipeline and providing the option WithNumProcs.
	numProcs int
}

type pipelineWorker = Worker[*Edge, *Message, *Message]
type workerPool = map[*Node]*pipelineWorker

// Build is a function that constructs the actual pipeline which is returned as an iter.Seq[Item].
// The pipeline will not begin generating values until the returned sequence is iterated over. This
// is to prevent unnecessary work and resource accummulation in the event that the sequence is never
// iterated.
func (pipeline *Pipeline) Build(ctx context.Context, source Source, target Target) iter.Seq[Item] {
	ctx, span := pipelineTracer.Start(ctx, "pipeline.build")
	defer span.End()

	ctx, cancel := context.WithCancel(ctx)

	workers := make(workerPool)

	p := path{
		source: (*Node)(source),
		target: target,
	}
	pipeline.resolve(p, workers)

	sourceWorker, ok := workers[(*Node)(source)]
	if !ok {
		panic("no such source worker")
	}

	results := sourceWorker.Subscribe(nil)

	return func(yield func(Item) bool) {
		ctx, span := pipelineTracer.Start(ctx, "pipeline.iterate")
		defer span.End()

		if ctx.Err() != nil {
			return
		}

		var wg sync.WaitGroup

		defer wg.Wait()
		defer cancel()

		// Workers are started here so that the pipeline does
		// not begin producing objects until the caller has begun
		// to iterate over the sequence. This prevents unnecessary
		// processing in the event that the caller decides not to
		// iterate over the sequence.
		for _, w := range workers {
			w.Start(ctx)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ctx.Done()
			// wait for all workers to finish
			for _, w := range workers {
				w.Wait()
			}
		}()

		for msg := range results.Seq() {
			if ctx.Err() == nil {
				for _, item := range msg.Value {
					if !yield(item) {
						cancel()
						break
					}
				}
			}
			msg.Done()
		}
	}
}

func (pipeline *Pipeline) Source(name, relation string) (Source, bool) {
	sourceNode, ok := pipeline.backend.Graph.GetNodeByID(name + "#" + relation)
	return (Source)(sourceNode), ok
}

func (pipeline *Pipeline) Target(name, identifier string) (Target, bool) {
	if identifier == "*" {
		name += ":*"
		identifier = ""
	}
	targetNode, ok := pipeline.backend.Graph.GetNodeByID(name)

	return Target{
		node: targetNode,
		id:   identifier,
	}, ok
}

type PipelineOption func(*Pipeline)

type path struct {
	source     *Node
	target     Target
	tracker    *track.Tracker
	statusPool *track.StatusPool
}

func (pipeline *Pipeline) resolve(p path, workers workerPool) *pipelineWorker {
	if w, ok := workers[p.source]; ok {
		return w
	}

	if p.tracker == nil {
		p.tracker = new(track.Tracker)
	}

	if p.statusPool == nil {
		p.statusPool = new(track.StatusPool)
	}

	var w pipelineWorker
	w.bufferSize = pipeline.bufferSize

	reporter := p.statusPool.Register()
	reporter.Report(true)

	omni := &omniInterpreter{
		hndNil:           handleIdentity,
		hndDirect:        pipeline.backend.handleDirectEdge,
		hndTTU:           pipeline.backend.handleTTUEdge,
		hndComputed:      handleIdentity,
		hndRewrite:       handleIdentity,
		hndDirectLogical: handleIdentity,
		hndTTULogical:    handleIdentity,
	}

	switch p.source.GetNodeType() {
	case nodeTypeSpecificType,
		nodeTypeSpecificTypeAndRelation,
		nodeTypeSpecificTypeWildcard,
		nodeTypeLogicalDirectGrouping,
		nodeTypeLogicalTTUGrouping:
		w.Resolver = &baseResolver{
			interpreter: omni,
			tracker:     p.tracker,
			reporter:    reporter,
			bufferPool:  pipeline.bufferPool,
			numProcs:    pipeline.numProcs,
		}
	case nodeTypeOperator:
		switch p.source.GetLabel() {
		case weightedGraph.IntersectionOperator:
			w.Resolver = &intersectionResolver{
				interpreter: omni,
				tracker:     p.tracker,
				reporter:    reporter,
				bufferPool:  pipeline.bufferPool,
				numProcs:    pipeline.numProcs,
			}
		case weightedGraph.UnionOperator:
			w.Resolver = &baseResolver{
				interpreter: omni,
				tracker:     p.tracker,
				reporter:    reporter,
				bufferPool:  pipeline.bufferPool,
				numProcs:    pipeline.numProcs,
			}
		case weightedGraph.ExclusionOperator:
			w.Resolver = &exclusionResolver{
				interpreter: omni,
				tracker:     p.tracker,
				reporter:    reporter,
				bufferPool:  pipeline.bufferPool,
				numProcs:    pipeline.numProcs,
			}
		default:
			panic("unsupported operator node for reverse expand worker")
		}
	default:
		panic("unsupported node type for reverse expand worker")
	}

	workers[p.source] = &w

	switch p.source.GetNodeType() {
	case nodeTypeSpecificType, nodeTypeSpecificTypeAndRelation:
		if p.source == p.target.node {
			items := []Item{{Value: p.target.Object()}}
			p.tracker.Add(1)
			m := Message{
				Value:       items,
				ReceiptFunc: p.tracker.Dec,
			}
			w.Listen(&sender[*Edge, *Message]{
				nil,
				pipe.StaticRx(&m),
			})
		}
	case nodeTypeSpecificTypeWildcard:
		label := p.source.GetLabel()
		typePart := strings.Split(label, ":")[0]

		if p.source == p.target.node || typePart == p.target.node.GetLabel() {
			// source node is the target node or has the same type as the target.
			items := []Item{{Value: typePart + ":*"}}
			p.tracker.Add(1)
			m := Message{
				Value:       items,
				ReceiptFunc: p.tracker.Dec,
			}
			w.Listen(&sender[*Edge, *Message]{
				nil,
				pipe.StaticRx(&m),
			})
		}
	}

	edges, ok := pipeline.backend.Graph.GetEdgesFromNode(p.source)
	if !ok {
		return &w
	}

	for _, edge := range edges {
		nextPath := p

		if len(edge.GetRecursiveRelation()) == 0 && !edge.IsPartOfTupleCycle() {
			nextPath.tracker = nil
			nextPath.statusPool = nil
		}

		nextPath.source = edge.GetTo()

		to := pipeline.resolve(nextPath, workers)
		w.Listen(to.Subscribe(edge))
	}

	return &w
}

type queryInput struct {
	objectType     string
	objectRelation string
	userFilter     []*openfgav1.ObjectRelation
	conditions     []string
}

// resolver is an interface that is consumed by a worker struct.
// A resolver is responsible for consuming messages from a worker's
// senders and broadcasting the result of processing the consumed
// messages to the worker's listeners.
type Resolver[K any, T any, U any] interface {

	// resolve is a function that consumes messages from the
	// provided senders, and broadcasts the results of processing
	// the consumed messages to the provided listeners.
	Resolve(context.Context, []Sender[K, T], []Listener[K, U])
}

type Source *Node

type Sender[K any, T any] interface {
	Key() K
	pipe.Rx[T]
}

// sender is a struct that contains fields relevant to the producing
// end of a pipeline connection.
type sender[K any, T any] struct {
	key K
	pipe.Rx[T]
}

func (s *sender[K, T]) Key() K {
	return s.key
}

type Listener[K any, T any] interface {
	Key() K
	pipe.TxCloser[T]
}

// listener is a struct that contains fields relevant to the listening
// end of a pipeline connection.
type listener[K any, T any] struct {
	key K
	pipe.TxCloser[T]
}

func (l *listener[K, T]) Key() K {
	return l.key
}

// strtoItem is a function that accepts a string input and returns an Item
// that contains the input as its `Value` value.
func strToItem(s string) Item {
	return Item{Value: s}
}

type Target struct {
	node *Node
	id   string
}

func (t *Target) Object() string {
	objectParts := strings.Split(t.node.GetLabel(), "#")
	var objectType string
	if len(objectParts) > 0 {
		objectType = objectParts[0]
	}
	var value string

	switch t.node.GetNodeType() {
	case nodeTypeSpecificTypeWildcard:
		// the ':*' is part of the type
	case nodeTypeSpecificType, nodeTypeSpecificTypeAndRelation:
		value = ":" + t.id
	}
	return objectType + value
}

type Worker[K any, T any, U any] struct {
	senders    []Sender[K, T]
	listeners  []Listener[K, U]
	Resolver   Resolver[K, T, U]
	finite     func()
	init       sync.Once
	wg         sync.WaitGroup
	bufferSize int
}

func (w *Worker[K, T, U]) Close() {
	if w.finite != nil {
		w.finite()
	}
}

func (w *Worker[K, T, U]) Listen(s Sender[K, T]) {
	w.senders = append(w.senders, s)
}

func (w *Worker[K, T, U]) initialize(ctx context.Context) {
	ctx, span := pipelineTracer.Start(ctx, "worker")
	ctx, cancel := context.WithCancel(ctx)

	w.finite = sync.OnceFunc(func() {
		defer span.End()
		cancel()

		for _, lst := range w.listeners {
			lst.Close()
		}
	})

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		defer w.Close()
		w.Resolver.Resolve(ctx, w.senders, w.listeners)
	}()

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		defer w.Close()
		<-ctx.Done()
	}()
}

func (w *Worker[K, T, U]) Start(ctx context.Context) {
	w.init.Do(func() {
		w.initialize(ctx)
	})
}

func (w *Worker[K, T, U]) Subscribe(key K) Sender[K, U] {
	var p pipe.Pipe[U]
	p.Grow(w.bufferSize)

	w.listeners = append(w.listeners, &listener[K, U]{
		key,
		&p,
	})

	return &sender[K, U]{
		key,
		&p,
	}
}

func (w *Worker[K, T, U]) Wait() {
	w.wg.Wait()
}
