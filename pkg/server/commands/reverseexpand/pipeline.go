package reverseexpand

import (
	"context"
	"errors"
	"iter"
	"maps"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
	if size < 0 {
		size = 0
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

type message[T any] struct {
	Value  T
	finite func()
}

func (m *message[T]) Done() {
	if m.finite != nil {
		m.finite()
	}
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

// baseResolver is a struct that implements the `resolver` interface and acts as the standard resolver for most
// workers. A baseResolver handles both recursive and non-recursive edges concurrently. The baseResolver's "ready"
// status will remain `true` until all of its senders that produce input from external sources have finished, and
// there exist no more in-flight messages for the parent worker. When recursive edges exist, the parent worker for
// this resolver type requires its internal watchdog process to initiate a shutdown.
type baseResolver struct {
	// reporter is a Reporter provided when registering with the baseResolver's StatusPool. The registration happens
	// once, when the baseResolver is created, so this value will remain constant for the lifetime of the instance.
	reporter track.Reporter

	// interpreter is an `interpreter` that transforms a sender's input into output which it broadcasts to all
	// of the parent worker's listeners.
	interpreter interpreter
}

func (r *baseResolver) process(ctx context.Context, snd *sender, lst pipe.Tx[[]Item], outputBuffer *sync.Map) int64 {
	var sentCount int64
	var inputBuffer sync.Map

	edge := snd.Edge
	isCyclical := edge != nil && (len(edge.GetRecursiveRelation()) > 0 || edge.IsPartOfTupleCycle())

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

	var msg message[[]Item]

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

			if isCyclical {
				if _, loaded := inputBuffer.LoadOrStore(item.Value, struct{}{}); !loaded {
					unseen = append(unseen, item.Value)
				}
				continue
			}
			unseen = append(unseen, item.Value)
		}

		// If there are no unseen items, skip processing.
		if len(unseen) == 0 {
			goto AfterInterpret
		}

		results = r.interpreter.Interpret(ctx, edge, unseen)

		// Deduplicate the output and potentially send in chunks.
		for item := range results {
			if item.Err != nil {
				items = append(items, item)
				continue
			}

			if _, loaded := outputBuffer.LoadOrStore(item.Value, struct{}{}); !loaded {
				items = append(items, item)
			}
		}

	AfterInterpret:
		lst.Send(items)
		sentCount += int64(len(items))
		msg.Done()
		span.End()
	}
	return sentCount
}

func (r *baseResolver) Resolve(ctx context.Context, senders []*sender, lst pipe.Tx[[]Item]) {
	ctx, span := pipelineTracer.Start(ctx, "baseResolver.Resolve")
	defer span.End()

	var outputBuffer sync.Map
	defer outputBuffer.Clear()

	var sentCount atomic.Int64

	// Any senders with a non-recursive edge will be processed in the "standard" queue.
	var wgStandard sync.WaitGroup

	// Any senders with a recursive edge will be processed in the "recursive" queue.
	var wgRecursive sync.WaitGroup

	for _, snd := range senders {
		// Any sender with an edge that has a value for its recursive relation will be treated
		// as recursive, so long as it is not part of a tuple cycle. When the edge is part of
		// a tuple cycle, treating it as recursive would cause the parent worker to be closed
		// too early.
		isRecursive := snd.Edge != nil && len(snd.Edge.GetRecursiveRelation()) > 0 && !snd.Edge.IsPartOfTupleCycle()

		for range snd.NumProcs {
			if isRecursive {
				wgRecursive.Add(1)
				go func() {
					defer wgRecursive.Done()
					sentCount.Add(r.process(ctx, snd, lst, &outputBuffer))
				}()
				continue
			}

			wgStandard.Add(1)
			go func() {
				defer wgStandard.Done()
				sentCount.Add(r.process(ctx, snd, lst, &outputBuffer))
			}()
		}
	}

	// All standard senders are guaranteed to end at some point, with the exception of the presence
	// of a tuple cycle.
	wgStandard.Wait()

	// Once the standard senders have all finished processing, we set the resolver's status to `false`
	// indicating that the worker is ready for cleanup once all messages have finished processing.
	r.reporter.Report(false)

	// Recursive senders will process infinitely until the parent worker's watchdog goroutine kills
	// them.
	wgRecursive.Wait()

	span.SetAttributes(attribute.Int64("items.count", sentCount.Load()))
}

type edgeHandler func(context.Context, *Edge, []string) iter.Seq[Item]

type exclusionResolver struct {
	reporter    track.Reporter
	interpreter interpreter
}

func (r *exclusionResolver) process(ctx context.Context, snd *sender, items *containers.Bag[Item], cleanup *containers.Bag[func()]) {
	edge := snd.Edge

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

	var msg message[[]Item]

	for snd.Recv(&msg) {
		var results iter.Seq[Item]
		var unseen []string

		messageAttrs := make([]attribute.KeyValue, 1, 1+len(attrs))
		messageAttrs[0] = attribute.Int("items.count", len(msg.Value))
		messageAttrs = append(messageAttrs, attrs...)

		ctx, span := pipelineTracer.Start(ctx, "message.received", trace.WithAttributes(messageAttrs...))

		for _, item := range msg.Value {
			if item.Err != nil {
				items.Add(item)
				continue
			}
			unseen = append(unseen, item.Value)
		}

		// If there are no unseen items, skip processing.
		if len(unseen) == 0 {
			goto AfterInterpret
		}

		results = r.interpreter.Interpret(ctx, edge, unseen)

		for item := range results {
			items.Add(item)
		}

	AfterInterpret:
		cleanup.Add(msg.Done)
		span.End()
	}
}

func (r *exclusionResolver) Resolve(ctx context.Context, senders []*sender, lst pipe.Tx[[]Item]) {
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

	for range senders[0].NumProcs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.process(ctx, senders[0], &included, &cleanup)
		}()
	}

	for range senders[1].NumProcs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.process(ctx, senders[1], &excluded, &cleanup)
		}()
	}

	wg.Wait()

	var items []Item

	exclusions := make(map[string]struct{})

	for item := range excluded.Seq() {
		if item.Err != nil {
			items = append(items, item)
			continue
		}
		exclusions[item.Value] = struct{}{}
	}

	for item := range included.Seq() {
		if item.Err != nil {
			items = append(items, item)
			continue
		}

		if _, ok := exclusions[item.Value]; !ok {
			items = append(items, item)
		}
	}

	lst.Send(items)

	span.SetAttributes(attribute.Int("items.count", len(items)))

	for fn := range cleanup.Seq() {
		fn()
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
	reporter    track.Reporter
	interpreter interpreter
}

func (r *intersectionResolver) process(ctx context.Context, snd *sender, items *containers.Bag[Item], cleanup *containers.Bag[func()]) {
	edge := snd.Edge

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

	var msg message[[]Item]

	for snd.Recv(&msg) {
		var results iter.Seq[Item]
		var unseen []string

		messageAttrs := make([]attribute.KeyValue, 1, 1+len(attrs))
		messageAttrs[0] = attribute.Int("items.count", len(msg.Value))
		messageAttrs = append(messageAttrs, attrs...)

		ctx, span := pipelineTracer.Start(ctx, "message.received", trace.WithAttributes(messageAttrs...))

		for _, item := range msg.Value {
			if item.Err != nil {
				items.Add(item)
				continue
			}
			unseen = append(unseen, item.Value)
		}

		// If there are no unseen items, skip processing.
		if len(unseen) == 0 {
			goto AfterInterpret
		}

		results = r.interpreter.Interpret(ctx, edge, unseen)

		for item := range results {
			items.Add(item)
		}

	AfterInterpret:
		cleanup.Add(msg.Done)
		span.End()
	}
}

func (r *intersectionResolver) Resolve(ctx context.Context, senders []*sender, lst pipe.Tx[[]Item]) {
	ctx, span := pipelineTracer.Start(ctx, "intersectionResolver.Resolve")
	defer span.End()

	defer r.reporter.Report(false)

	var wg sync.WaitGroup

	bags := make([]containers.Bag[Item], len(senders))

	var cleanup containers.Bag[func()]

	for i, snd := range senders {
		for range snd.NumProcs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r.process(ctx, snd, &bags[i], &cleanup)
			}()
		}
	}
	wg.Wait()

	var items []Item

	output := make(map[string]struct{})

	for item := range bags[0].Seq() {
		if item.Err != nil {
			items = append(items, item)
			continue
		}
		output[item.Value] = struct{}{}
	}

	for i := 1; i < len(bags); i++ {
		found := make(map[string]struct{}, len(output))
		for item := range bags[i].Seq() {
			if item.Err != nil {
				items = append(items, item)
				continue
			}

			if _, ok := output[item.Value]; ok {
				found[item.Value] = struct{}{}
			}
		}
		output = found
	}

	itemSeq := seq.Transform(maps.Keys(output), strToItem)

	for item := range itemSeq {
		items = append(items, item)
	}

	lst.Send(items)

	span.SetAttributes(attribute.Int("items.count", len(items)))

	for fn := range cleanup.Seq() {
		fn()
	}
}

// listener is a struct that contains fields relevant to the listening
// end of a pipeline connection.
type listener struct {
	Tracker   track.Tracker
	ChunkSize int
	cons      []pipe.Tx[message[[]Item]]
}

func (l *listener) inc(n int64) {
	if l.Tracker != nil {
		l.Tracker.Add(n)
	}
}

func (l *listener) pack(items []Item) message[[]Item] {
	l.inc(1)
	return message[[]Item]{
		Value: items,
		finite: sync.OnceFunc(func() {
			l.inc(-1)
		}),
	}
}

func (l *listener) Add(cons ...pipe.Tx[message[[]Item]]) {
	l.cons = append(l.cons, cons...)
}

func (l *listener) send(items []Item) bool {
	var sent bool

	for _, con := range l.cons {
		msg := l.pack(items)
		if !con.Send(msg) {
			msg.Done()
		} else {
			sent = true
		}
	}
	return sent
}

func (l *listener) Send(items []Item) bool {
	if len(items) == 0 {
		return false
	}

	if l.ChunkSize <= 0 {
		return l.send(items)
	}

	var sent bool

	for len(items) > 0 {
		ext := int(math.Min(float64(l.ChunkSize), float64(len(items))))
		if l.send(items[:ext]) {
			sent = true
		}
		items = items[ext:]
	}
	return sent
}

func (l *listener) Close() {
	for _, con := range l.cons {
		con.Close()
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

	// numProcs is a value that indicates the maximum number of goroutines
	// that will be allocated to processing each subscription for a pipeline
	// worker.
	//
	// The default value is 3. This value can be changed by constructing
	// a pipeline using NewPipeline and providing the option WithNumProcs.
	numProcs int
}

// Build is a function that constructs the actual pipeline which is returned as an iter.Seq[Item].
// The pipeline will not begin generating values until the returned sequence is iterated over. This
// is to prevent unnecessary work and resource accummulation in the event that the sequence is never
// iterated.
func (p *Pipeline) Build(ctx context.Context, source Source, target Target) iter.Seq[Item] {
	ctx, span := pipelineTracer.Start(ctx, "pipeline.build")
	defer span.End()

	ctx, cancel := context.WithCancel(ctx)

	pth := path{
		ctx:     ctx,
		pipe:    p,
		workers: make(map[*Node]*worker),
	}
	pth.resolve((*Node)(source), target, nil, nil)

	sourceWorker, ok := pth.workers[(*Node)(source)]
	if !ok {
		panic("no such source worker")
	}

	results := sourceWorker.Subscribe(p.bufferSize)

	return func(yield func(Item) bool) {
		var wg sync.WaitGroup
		ctx, span := pipelineTracer.Start(ctx, "pipeline.iterate")
		defer span.End()

		ticker := time.NewTicker(1 * time.Millisecond)
		defer ticker.Stop()

		defer wg.Wait()
		defer cancel()

		if ctx.Err() != nil {
			return
		}

		ch := ticker.C

		// Workers are started here so that the pipeline does
		// not begin producing objects until the caller has begun
		// to iterate over the sequence. This prevents unnecessary
		// processing in the event that the caller decides not to
		// iterate over the sequence.
		for _, w := range pth.workers {
			w.Start(ctx)
		}

		// Watchdog goroutine is started here to ensure that no
		// goroutines are created before the caller has begun to
		// iterate over the pipeline's sequence.
		wg.Add(1)
		go func() {
			defer wg.Done()

		WatchdogLoop:
			for {
				select {
				case <-ch:
					var inactiveCount int

					for _, w := range pth.workers {
						if !w.Active() {
							w.Close()
							inactiveCount++
						}
					}

					if inactiveCount == len(pth.workers) {
						cancel()
						ch = nil
					}

					messageCount := pth.tracker.Load()

					if messageCount < 1 {
						cancel()
						ch = nil
					}
				case <-ctx.Done():
					// wait for all workers to finish
					for _, w := range pth.workers {
						w.Wait()
					}
					break WatchdogLoop
				}
			}
		}()

		for msg := range results.Seq() {
			for _, item := range msg.Value {
				if !yield(item) {
					msg.Done()
					return
				}
			}
			msg.Done()
		}
	}
}

func (p *Pipeline) Source(name, relation string) (Source, bool) {
	sourceNode, ok := p.backend.Graph.GetNodeByID(name + "#" + relation)
	return (Source)(sourceNode), ok
}

func (p *Pipeline) Target(name, identifier string) (Target, bool) {
	if identifier == "*" {
		name += ":*"
		identifier = ""
	}
	targetNode, ok := p.backend.Graph.GetNodeByID(name)

	return Target{
		node: targetNode,
		id:   identifier,
	}, ok
}

type PipelineOption func(*Pipeline)

type path struct {
	ctx     context.Context
	pipe    *Pipeline
	workers map[*Node]*worker
	tracker atomic.Int64
}

func (p *path) pack(items []Item) message[[]Item] {
	p.tracker.Add(1)
	return message[[]Item]{
		Value: items,
		finite: sync.OnceFunc(func() {
			p.tracker.Add(-1)
		}),
	}
}

func (p *path) resolve(source *Node, target Target, tracker track.Tracker, status *track.StatusPool) {
	if _, ok := p.workers[source]; ok {
		return
	}

	if tracker == nil {
		tracker = track.NewEchoTracker(&p.tracker)
	}

	if status == nil {
		status = new(track.StatusPool)
	}

	lst := listener{
		Tracker:   tracker,
		ChunkSize: p.pipe.chunkSize,
	}

	w := p.worker(source, &lst, tracker, status)

	p.workers[source] = w

	switch source.GetNodeType() {
	case nodeTypeSpecificType, nodeTypeSpecificTypeAndRelation:
		if source == target.node {
			// source node is the target node.
			items := []Item{{Value: target.Object()}}
			m := p.pack(items)
			w.Listen(nil, pipe.StaticRx(m), 1) // only one value to consume, so only one processor necessary.
		}
	case nodeTypeSpecificTypeWildcard:
		label := source.GetLabel()
		typePart := strings.Split(label, ":")[0]

		if source == target.node || typePart == target.node.GetLabel() {
			// source node is the target node or has the same type as the target.
			items := []Item{{Value: typePart + ":*"}}
			m := p.pack(items)
			w.Listen(nil, pipe.StaticRx(m), 1) // only one value to consume, so only one processor necessary.
		}
	}

	edges, ok := p.pipe.backend.Graph.GetEdgesFromNode(source)
	if !ok {
		return
	}

	for _, edge := range edges {
		var tracker track.Tracker
		var stat *track.StatusPool

		isRecursive := len(edge.GetRecursiveRelation()) > 0

		if isRecursive {
			tracker = w.tracker
			stat = status
		}
		p.resolve(edge.GetTo(), target, tracker, stat)
		w.Listen(edge, p.workers[edge.GetTo()].Subscribe(p.pipe.bufferSize), p.pipe.numProcs)
	}
}

func (p *path) worker(node *Node, lst *listener, tracker track.Tracker, status *track.StatusPool) *worker {
	var w worker

	w.node = node
	w.status = status
	w.tracker = tracker
	w.listener = lst

	var r resolver

	reporter := status.Register()
	reporter.Report(true)

	omni := &omniInterpreter{
		hndNil:           handleIdentity,
		hndDirect:        p.pipe.backend.handleDirectEdge,
		hndTTU:           p.pipe.backend.handleTTUEdge,
		hndComputed:      handleIdentity,
		hndRewrite:       handleIdentity,
		hndDirectLogical: handleIdentity,
		hndTTULogical:    handleIdentity,
	}

	switch node.GetNodeType() {
	case nodeTypeSpecificType,
		nodeTypeSpecificTypeAndRelation,
		nodeTypeSpecificTypeWildcard,
		nodeTypeLogicalDirectGrouping,
		nodeTypeLogicalTTUGrouping:
		r = &baseResolver{
			reporter:    reporter,
			interpreter: omni,
		}
	case nodeTypeOperator:
		switch node.GetLabel() {
		case weightedGraph.IntersectionOperator:
			r = &intersectionResolver{
				reporter:    reporter,
				interpreter: omni,
			}
		case weightedGraph.UnionOperator:
			r = &baseResolver{
				reporter:    reporter,
				interpreter: omni,
			}
		case weightedGraph.ExclusionOperator:
			r = &exclusionResolver{
				reporter:    reporter,
				interpreter: omni,
			}
		default:
			panic("unsupported operator node for reverse expand worker")
		}
	default:
		panic("unsupported node type for reverse expand worker")
	}

	w.resolver = r

	pw := &w

	p.workers[node] = pw

	return pw
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
type resolver interface {

	// resolve is a function that consumes messages from the
	// provided senders, and broadcasts the results of processing
	// the consumed messages to the provided listeners.
	Resolve(context.Context, []*sender, pipe.Tx[[]Item])
}

type Source *Node

// sender is a struct that contains fields relevant to the producing
// end of a pipeline connection.
type sender struct {
	Edge     *Edge
	producer pipe.Rx[message[[]Item]]
	tracker  track.Tracker
	NumProcs int
}

func (s *sender) Recv(msg *message[[]Item]) bool {
	var m message[[]Item]
	if !s.producer.Recv(&m) {
		return false
	}
	s.tracker.Add(1)
	m.Done()
	*msg = message[[]Item]{
		Value: m.Value,
		finite: sync.OnceFunc(func() {
			s.tracker.Add(-1)
		}),
	}
	return true
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

type worker struct {
	node     *Node
	senders  []*sender
	listener *listener
	resolver resolver
	tracker  track.Tracker
	status   *track.StatusPool
	finite   func()
	init     sync.Once
	wg       sync.WaitGroup
}

func (w *worker) Active() bool {
	return w.status.Status() || w.tracker.Load() != 0
}

func (w *worker) Close() {
	w.finite()
}

func (w *worker) Listen(edge *Edge, p pipe.Rx[message[[]Item]], numProcs int) {
	w.senders = append(w.senders, &sender{
		Edge:     edge,
		producer: p,
		tracker:  w.tracker,
		NumProcs: numProcs,
	})
}

func (w *worker) initialize(ctx context.Context) {
	ctx, span := pipelineTracer.Start(ctx, "worker")
	ctx, cancel := context.WithCancel(ctx)

	w.finite = sync.OnceFunc(func() {
		defer span.End()
		cancel()
	})

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		defer w.Close()
		w.resolver.Resolve(ctx, w.senders, w.listener)
	}()

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		<-ctx.Done()
		w.listener.Close()
	}()
}

func (w *worker) Start(ctx context.Context) {
	w.init.Do(func() {
		w.initialize(ctx)
	})
}

func (w *worker) Subscribe(bufferSize int) pipe.Rx[message[[]Item]] {
	var p pipe.Pipe[message[[]Item]]
	p.Grow(bufferSize)
	w.listener.Add(&p)
	return &p
}

func (w *worker) Wait() {
	w.wg.Wait()
}
