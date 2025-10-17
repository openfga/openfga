package reverseexpand

import (
	"context"
	"errors"
	"iter"
	"maps"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	weightedGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/checkutil"
	"github.com/openfga/openfga/internal/seq"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
)

// emptySequence represents an `iter.Seq[Item]` that does nothing.
var emptySequence = func(yield func(Item) bool) {}

type (
	Graph = weightedGraph.WeightedAuthorizationModelGraph
	Node  = weightedGraph.WeightedAuthorizationModelNode
	Edge  = weightedGraph.WeightedAuthorizationModelEdge
)

var (
	edgeTypeDirect        = weightedGraph.DirectEdge
	edgeTypeComputed      = weightedGraph.ComputedEdge
	edgeTypeRewrite       = weightedGraph.RewriteEdge
	edgeTypeTTU           = weightedGraph.TTUEdge
	edgeTypeDirectLogical = weightedGraph.DirectLogicalEdge
	edgeTypeTTULogical    = weightedGraph.TTULogicalEdge

	nodeTypeSpecificType            = weightedGraph.SpecificType
	nodeTypeSpecificTypeWildcard    = weightedGraph.SpecificTypeWildcard
	nodeTypeSpecificTypeAndRelation = weightedGraph.SpecificTypeAndRelation
	nodeTypeOperator                = weightedGraph.OperatorNode
	nodeTypeLogicalDirectGrouping   = weightedGraph.LogicalDirectGrouping
	nodeTypeLogicalTTUGrouping      = weightedGraph.LogicalTTUGrouping

	ErrNoSuchNode         = errors.New("no such node in graph")
	ErrNoSuchPath         = errors.New("no path between source and target nodes")
	ErrConnectionOverflow = errors.New("too many connections")
	ErrTupleCycle         = errors.New("cycle detected in tuples")
)

// Item is a struct that contains an object `string` as its `Value` or an
// encountered error as its `Err`. Item is the primary container used to
// communicate values as they pass through a `Pipeline`.
type Item struct {
	Value string
	Err   error
}

// group is a struct that acts as a container for a set of `Item` values.
type group struct {
	Items []Item
}

// strtoItem is a function that accepts a string input and returns an Item
// that contains the input as its `Value` value.
func strToItem(s string) Item {
	return Item{Value: s}
}

// Backend is a struct that serves as a container for all backend elements
// necessary for creating and running a `Pipeline`.
type Backend struct {
	Datastore  storage.RelationshipTupleReader
	StoreID    string
	TypeSystem *typesystem.TypeSystem
	Context    *structpb.Struct
	Graph      *Graph
}

type queryInput struct {
	objectType     string
	objectRelation string
	userFilter     []*openfgav1.ObjectRelation
	conditions     []string
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
		},
		storage.ReadStartingWithUserOptions{},
	)

	if err != nil {
		cancel()
		return seq.Sequence(Item{Err: err})
	}

	var hasConditions bool

	for _, cond := range input.conditions {
		if cond != weightedGraph.NoCond {
			hasConditions = true
			break
		}
	}

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

// resolver is an interface that is consumed by a worker struct.
// A resolver is responsible for consuming messages from a worker's
// senders and broadcasting the result of processing the consumed
// messages to the worker's listeners.
type resolver interface {
	// resolve is a function that consumes messages from the
	// provided senders, and broadcasts the results of processing
	// the consumed messages to the provided listeners.
	resolve(senders []*sender, listeners []*listener)

	ready() bool
}

// interpreter is an interface that exposes a method for interpreting input for an edge into output.
type interpreter interface {
	interpret(edge *Edge, items []Item) iter.Seq[Item]
}

// baseResolver is a struct that implements the `resolver` interface and acts as the standard resolver for most
// workers. A baseResolver handles both recursive and non-recursive edges concurrently. The baseResolver's "ready"
// status will remain `true` until all of its senders that produce input from external sources have finished, and
// there exist no more in-flight messages for the parent worker. When recursive edges exist, the parent worker for
// this resolver type requires its internal watchdog process to initiate a shutdown.
type baseResolver struct {
	// id is an identifier provided when registering with the baseResolver's StatusPool. The registration happens
	// once, when the baseResolver is created, so this value will remain constant for the lifetime of the instance.
	id int

	ctx context.Context

	// interpreter is an `interpreter` that transforms a sender's input into output which it broadcasts to all
	// of the parent worker's listeners.
	interpreter interpreter

	// mutexes each protect map access for a buffer within inBuffers at the same index.
	mutexes []sync.Mutex

	// inBuffers contains a slice of maps, used as hash sets, for deduplicating each individual sender's
	// input feed. Each buffer's index corresponds to its associated sender's index. Each sender needs
	// a separate deduplication buffer because it is valid for the same object to be receieved on multiple
	// edges producing to the same node. This is specifically true in the case of recursive edges, where
	// a single resolver may have multiple recursive edges that must receive the same objects.
	inBuffers []map[string]struct{}

	// outMu protects map access to outBuffer.
	outMu sync.Mutex

	// outBuffer contains a map, used as a hash set, for deduplicating each resolver's output. A single
	// resolver should only output an object once.
	outBuffer map[string]struct{}

	// status is a *StatusPool instance that tracks the status of the baseResolver instance. This *StatusPool value
	// may or may not be shared with other resolver instances and workers. When the current resolver is part of a
	// recursive chain, then this *StatusPool value is shared with each of the participating resolvers. The status
	// of a baseResolver is assumed to be `true` from the point of initialization, until all "standard" senders have completed.
	// A baseResolver's status can be `false` while "recursive" senders are still actively processing messages. In that
	// case, the parent worker is kept alive by the overall status of the *StatusPool instance, and the count of messages
	// in-flight.
	status *StatusPool
}

func (r *baseResolver) ready() bool {
	return r.status.Status()
}

func (r *baseResolver) process(ndx int, senders []*sender, listeners []*listener) {
	// Loop while the sender has a potential to yield a message.
	for senders[ndx].more() {
		var results iter.Seq[Item]
		var outGroup group
		var items, unseen []Item

		// attempt to pull a message from the sender (non-blocking)
		msg, ok := senders[ndx].recv()
		if !ok {
			// no message was currently ready. proceed to end of function.
			goto ProcessEnd
		}

		// Deduplicate items within this group based on the buffer for this sender
		for _, item := range msg.Value.Items {
			if item.Err != nil {
				unseen = append(unseen, item)
				continue
			}

			r.mutexes[ndx].Lock()
			if _, ok := r.inBuffers[ndx][item.Value]; ok {
				r.mutexes[ndx].Unlock()
				continue
			}
			r.inBuffers[ndx][item.Value] = struct{}{}
			r.mutexes[ndx].Unlock()
			unseen = append(unseen, item)
		}

		// If there are no unseen items, skip processing
		if len(unseen) == 0 {
			goto ProcessEnd
		}

		results = r.interpreter.interpret(senders[ndx].edge, unseen)

		// Deduplicate the output and potentially send in chunks.
		for item := range results {
			if r.ctx.Err() != nil {
				break
			}

			if item.Err != nil {
				goto AfterDedup
			}

			r.outMu.Lock()
			if _, ok := r.outBuffer[item.Value]; ok {
				r.outMu.Unlock()
				continue
			}
			r.outBuffer[item.Value] = struct{}{}
			r.outMu.Unlock()

		AfterDedup:
			items = append(items, item)

			if len(items) < senders[ndx].chunkSize || senders[ndx].chunkSize == 0 {
				continue
			}

			g := group{
				Items: items,
			}

			items = nil

			for _, lst := range listeners {
				lst.send(g)
			}
		}

		if len(items) == 0 {
			goto ProcessEnd
		}

		outGroup.Items = items

		for _, lst := range listeners {
			lst.send(outGroup)
		}

	ProcessEnd:
		// Important: Indicating that the message is done will clear its count from the
		// origin worker's tracker.
		msg.done()

		// Important: This is a tight loop and needs to defer to the Go runtime so as not
		// to starve other goroutines for CPU time.
		runtime.Gosched()
	}
}

func (r *baseResolver) resolve(senders []*sender, listeners []*listener) {
	r.mutexes = make([]sync.Mutex, len(senders))
	r.inBuffers = make([]map[string]struct{}, len(senders))
	r.outBuffer = make(map[string]struct{})

	for ndx := range len(senders) {
		r.inBuffers[ndx] = make(map[string]struct{})
	}

	// Any senders with a non-recursive edge will be processed in the "standard" queue.
	var standard []func()

	// Any senders with a recursive edge will be processed in the "recursive" queue.
	var recursive []func()

	for ndx := range len(senders) {
		snd := senders[ndx]

		// Any sender with an edge that has a value for its recursive relation will be treated
		// as recursive, so long as it is not part of a tuple cycle. When the edge is part of
		// a tuple cycle, treating it as recursive would cause the parent worker to be closed
		// too early.
		isRecursive := snd.edge != nil && len(snd.edge.GetRecursiveRelation()) > 0 && !snd.edge.IsPartOfTupleCycle()

		proc := func() {
			r.process(ndx, senders, listeners)
		}

		for range snd.numProcs {
			if isRecursive {
				recursive = append(recursive, proc)
				continue
			}
			standard = append(standard, proc)
		}
	}

	var wgStandard sync.WaitGroup

	for _, proc := range standard {
		wgStandard.Add(1)
		go func() {
			defer wgStandard.Done()
			proc()
		}()
	}

	var wgRecursive sync.WaitGroup

	for _, proc := range recursive {
		wgRecursive.Add(1)
		go func() {
			defer wgRecursive.Done()
			proc()
		}()
	}

	// All standard senders are guaranteed to end at some point, with the exception of the presence
	// of a tuple cycle.
	wgStandard.Wait()

	// Once the standard senders have all finished processing, we set the resolver's status to `false`
	// indicating that the parent is ready for cleanup once all messages have finished processing.
	r.status.Set(r.id, false)

	// Recursive senders will process infinitely until the parent worker's watchdog goroutine kills
	// them.
	wgRecursive.Wait()
}

type edgeHandler func(*Edge, []Item) iter.Seq[Item]

// handleDirectEdge is a function that interprets input on a direct edge and provides output from
// a query to the backend datastore.
func (b *Backend) handleDirectEdge(edge *Edge, items []Item) iter.Seq[Item] {
	parts := strings.Split(edge.GetRelationDefinition(), "#")
	nodeType := parts[0]
	nodeRelation := parts[1]

	userParts := strings.Split(edge.GetTo().GetLabel(), "#")

	var userRelation string

	if len(userParts) > 1 {
		userRelation = userParts[1]
	}

	var userFilter []*openfgav1.ObjectRelation

	var errs []Item

	for _, item := range items {
		if item.Err != nil {
			errs = append(errs, item)
			continue
		}

		userFilter = append(userFilter, &openfgav1.ObjectRelation{
			Object:   item.Value,
			Relation: userRelation,
		})
	}

	var results iter.Seq[Item]

	if len(userFilter) > 0 {
		input := queryInput{
			objectType:     nodeType,
			objectRelation: nodeRelation,
			userFilter:     userFilter,
			conditions:     edge.GetConditions(),
		}
		results = b.query(context.Background(), input)
	} else {
		results = emptySequence
	}

	if len(errs) > 0 {
		results = seq.Flatten(seq.Sequence(errs...), results)
	}
	return results
}

// handleTTUEdge is a function that interprets input on a TTU edge and provides output from
// a query to the backend datastore.
func (b *Backend) handleTTUEdge(edge *Edge, items []Item) iter.Seq[Item] {
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

	var errs []Item

	for _, item := range items {
		if item.Err != nil {
			errs = append(errs, item)
			continue
		}

		userFilter = append(userFilter, &openfgav1.ObjectRelation{
			Object:   item.Value,
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
		results = b.query(context.Background(), input)
	} else {
		results = emptySequence
	}

	if len(errs) > 0 {
		results = seq.Flatten(seq.Sequence(errs...), results)
	}
	return results
}

func handleIdentity(_ *Edge, items []Item) iter.Seq[Item] {
	return seq.Sequence(items...)
}

func handleUnsupported(_ *Edge, _ []Item) iter.Seq[Item] {
	return seq.Sequence(Item{Err: errors.New("unsupported state")})
}

func handleLeafNode(node *Node) edgeHandler {
	return func(_ *Edge, items []Item) iter.Seq[Item] {
		objectParts := strings.Split(node.GetLabel(), "#")
		if len(objectParts) < 1 {
			return seq.Sequence(Item{Err: errors.New("empty label in node")})
		}

		objectType := objectParts[0]

		results := seq.Transform(seq.Sequence(items...), func(item Item) Item {
			var value string

			switch node.GetNodeType() {
			case nodeTypeSpecificTypeWildcard:
				value = ""
			case nodeTypeSpecificType, nodeTypeSpecificTypeAndRelation:
				value = ":" + item.Value
			default:
				return Item{Err: errors.New("unsupported leaf node type")}
			}
			item.Value = objectType + value
			return item
		})
		return results
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

func (o *omniInterpreter) interpret(edge *Edge, items []Item) iter.Seq[Item] {
	var results iter.Seq[Item]

	if edge == nil {
		results = o.hndNil(edge, items)
		return results
	}

	switch edge.GetEdgeType() {
	case edgeTypeDirect:
		results = o.hndDirect(edge, items)
	case edgeTypeTTU:
		results = o.hndTTU(edge, items)
	case edgeTypeComputed:
		results = o.hndComputed(edge, items)
	case edgeTypeRewrite:
		results = o.hndRewrite(edge, items)
	case edgeTypeDirectLogical:
		results = o.hndDirectLogical(edge, items)
	case edgeTypeTTULogical:
		results = o.hndTTULogical(edge, items)
	default:
		panic("unexpected edge type")
	}
	return results
}

type intersectionResolver struct {
	ctx         context.Context
	interpreter interpreter
	done        atomic.Bool
	trk         tracker
}

func (r *intersectionResolver) ready() bool {
	return !r.done.Load()
}

func (r *intersectionResolver) resolve(senders []*sender, listeners []*listener) {
	defer func() {
		r.trk.Add(-1)
		r.done.Store(true)
	}()

	r.trk.Add(1)

	var wg sync.WaitGroup

	objects := make(map[string]struct{})
	buffers := make([]map[string]struct{}, len(senders))
	output := make([]map[string]struct{}, len(senders))

	for i := range senders {
		buffers[i] = make(map[string]struct{})
		output[i] = make(map[string]struct{})
	}

	errs := make([][]Item, len(senders))

	for i, snd := range senders {
		wg.Add(1)

		go func(i int, snd *sender) {
			defer wg.Done()

			for snd.more() {
				msg, ok := snd.recv()
				if !ok {
					runtime.Gosched()
					continue
				}

				var unseen []Item

				// Deduplicate items within this group based on the buffer for this sender
				for _, item := range msg.Value.Items {
					if item.Err != nil {
						unseen = append(unseen, item)
						continue
					}

					if _, ok := buffers[i][item.Value]; ok {
						continue
					}
					unseen = append(unseen, item)
					buffers[i][item.Value] = struct{}{}
				}

				// If there are no unseen items, skip processing
				if len(unseen) == 0 {
					msg.done()
					continue
				}

				results := r.interpreter.interpret(snd.edge, unseen)

				for item := range results {
					if r.ctx.Err() != nil {
						break
					}

					if item.Err != nil {
						errs[i] = append(errs[i], item)
					}
					output[i][item.Value] = struct{}{}
				}
				msg.done()
			}
		}(i, snd)
	}
	wg.Wait()

OutputLoop:
	for obj := range output[0] {
		for i := 1; i < len(output); i++ {
			if _, ok := output[i][obj]; !ok {
				continue OutputLoop
			}
		}
		objects[obj] = struct{}{}
	}

	var allErrs []Item

	for _, errList := range errs {
		allErrs = append(allErrs, errList...)
	}

	seq := seq.Flatten(seq.Sequence(allErrs...), seq.Transform(maps.Keys(objects), strToItem))

	var items []Item

	for item := range seq {
		items = append(items, item)
	}

	outGroup := group{
		Items: items,
	}

	for _, lst := range listeners {
		lst.send(outGroup)
	}
}

type exclusionResolver struct {
	ctx         context.Context
	interpreter interpreter
	done        atomic.Bool
	trk         tracker
}

func (r *exclusionResolver) ready() bool {
	return !r.done.Load()
}

func (r *exclusionResolver) resolve(senders []*sender, listeners []*listener) {
	defer func() {
		r.trk.Add(-1)
		r.done.Store(true)
	}()

	r.trk.Add(1)

	if len(senders) != 2 {
		panic("exclusion resolver requires two senders")
	}
	var wg sync.WaitGroup

	included := make(map[string]struct{})
	excluded := make(map[string]struct{})

	var includedErrs []Item
	var excludedErrs []Item

	var procIncluded func(*sender)
	var procExcluded func(*sender)

	procIncluded = func(snd *sender) {
		defer wg.Done()

		for snd.more() {
			msg, ok := snd.recv()
			if !ok {
				runtime.Gosched()
				continue
			}

			results := r.interpreter.interpret(snd.edge, msg.Value.Items)

			for item := range results {
				if r.ctx.Err() != nil {
					break
				}

				if item.Err != nil {
					includedErrs = append(includedErrs, item)
					continue
				}
				included[item.Value] = struct{}{}
			}
			msg.done()
		}
	}

	procExcluded = func(snd *sender) {
		defer wg.Done()

		for snd.more() {
			msg, ok := snd.recv()
			if !ok {
				runtime.Gosched()
				continue
			}

			results := r.interpreter.interpret(snd.edge, msg.Value.Items)

			for item := range results {
				if item.Err != nil {
					excludedErrs = append(excludedErrs, item)
					continue
				}
				excluded[item.Value] = struct{}{}
			}
			msg.done()
		}
	}

	wg.Add(1)
	go procIncluded(senders[0])

	wg.Add(1)
	go procExcluded(senders[1])

	wg.Wait()

	var allErrs []Item

	allErrs = append(allErrs, includedErrs...)
	allErrs = append(allErrs, excludedErrs...)

	filteredSeq := seq.Filter(maps.Keys(included), func(v string) bool {
		_, ok := excluded[v]
		return !ok
	})

	flattenedSeq := seq.Flatten(seq.Sequence(allErrs...), seq.Transform(filteredSeq, strToItem))

	var items []Item

	for item := range flattenedSeq {
		items = append(items, item)
	}

	outGroup := group{
		Items: items,
	}

	for _, lst := range listeners {
		lst.send(outGroup)
	}
}

type worker struct {
	status    *StatusPool
	senders   []*sender
	listeners []*listener
	resolver  resolver
	trk       tracker
	wg        sync.WaitGroup
}

func (w *worker) active() bool {
	return w.resolver.ready() || w.trk.Load() != 0
}

func (w *worker) start() {
	w.wg.Add(1)

	go func() {
		defer w.wg.Done()

		defer func() {
			for _, lst := range w.listeners {
				lst.close()
			}
		}()

		w.resolver.resolve(w.senders, w.listeners)
	}()
}

func (w *worker) close() {
	for _, lst := range w.listeners {
		lst.close()
	}
}

func (w *worker) wait() {
	w.wg.Wait()
}

// listener is a struct that contains fields relevant to the listening
// end of a pipeline connection.
type listener struct {
	cons consumer[group]

	// node is the weighted graph node that is listening.
	node *Node
}

func (lst *listener) send(g group) {
	lst.cons.send(g)
}

func (lst *listener) close() {
	lst.cons.close()
}

// sender is a struct that contains fields relevant to the producing
// end of a pipeline connection.
type sender struct {
	// edge is the weighted graph edge that is producing.
	edge *Edge

	prod producer[group]

	// chunkSize is the target number of items to include in each
	// outbound message. A value less than 1 indicates an unlimited
	// number of items per message.
	chunkSize int

	numProcs int
}

func (snd *sender) recv() (message[group], bool) {
	return snd.prod.recv()
}

func (snd *sender) more() bool {
	return !snd.prod.done()
}

func (w *worker) listen(edge *Edge, p producer[group], chunkSize int, numProcs int) {
	w.senders = append(w.senders, &sender{
		edge:      edge,
		prod:      p,
		chunkSize: chunkSize,
		numProcs:  numProcs,
	})
}

func (w *worker) subscribe(node *Node) *pipe {
	p := newPipe(w.trk)

	w.listeners = append(w.listeners, &listener{
		cons: p,
		node: node,
	})

	return p
}

type Pipeline struct {
	backend   *Backend
	chunkSize int
	numProcs  int
}

func NewPipeline(backend *Backend, options ...PipelineOption) *Pipeline {
	p := &Pipeline{
		backend:   backend,
		chunkSize: 100,
		numProcs:  3,
	}

	for _, option := range options {
		option(p)
	}
	return p
}

type Target struct {
	node *Node
	id   string
}

type Source *Node

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

func (p *Pipeline) Source(name, relation string) (Source, bool) {
	sourceNode, ok := p.backend.Graph.GetNodeByID(name + "#" + relation)
	return (Source)(sourceNode), ok
}

type PipelineOption func(*Pipeline)

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

type path struct {
	ctx     context.Context
	pipe    *Pipeline
	workers map[*Node]*worker
	trk     atomic.Int64
}

func (p *path) worker(node *Node, trk tracker, status *StatusPool) *worker {
	var w worker

	w.status = status
	w.trk = trk

	var r resolver

	id := status.Register()
	status.Set(id, true)

	switch node.GetNodeType() {
	case nodeTypeSpecificTypeAndRelation:
		omni := &omniInterpreter{
			hndNil:           handleLeafNode(node),
			hndDirect:        p.pipe.backend.handleDirectEdge,
			hndTTU:           p.pipe.backend.handleTTUEdge,
			hndComputed:      handleIdentity,
			hndRewrite:       handleIdentity,
			hndDirectLogical: handleUnsupported,
			hndTTULogical:    handleUnsupported,
		}

		r = &baseResolver{
			id:          id,
			ctx:         p.ctx,
			interpreter: omni,
			status:      status,
		}
	case nodeTypeSpecificType:
		omni := &omniInterpreter{
			hndNil:           handleLeafNode(node),
			hndDirect:        handleUnsupported,
			hndTTU:           handleUnsupported,
			hndComputed:      handleUnsupported,
			hndRewrite:       handleUnsupported,
			hndDirectLogical: handleUnsupported,
			hndTTULogical:    handleUnsupported,
		}

		r = &baseResolver{
			id:          id,
			ctx:         p.ctx,
			interpreter: omni,
			status:      status,
		}
	case nodeTypeSpecificTypeWildcard:
		omni := &omniInterpreter{
			hndNil:           handleLeafNode(node),
			hndDirect:        handleUnsupported,
			hndTTU:           handleUnsupported,
			hndComputed:      handleUnsupported,
			hndRewrite:       handleUnsupported,
			hndDirectLogical: handleUnsupported,
			hndTTULogical:    handleUnsupported,
		}

		r = &baseResolver{
			id:          id,
			ctx:         p.ctx,
			interpreter: omni,
			status:      status,
		}
	case nodeTypeOperator:
		switch node.GetLabel() {
		case weightedGraph.IntersectionOperator:
			omni := &omniInterpreter{
				hndNil:           handleUnsupported,
				hndDirect:        p.pipe.backend.handleDirectEdge,
				hndTTU:           p.pipe.backend.handleTTUEdge,
				hndComputed:      handleIdentity,
				hndRewrite:       handleIdentity,
				hndDirectLogical: handleIdentity,
				hndTTULogical:    handleIdentity,
			}

			r = &intersectionResolver{
				ctx:         p.ctx,
				interpreter: omni,
				trk:         trk,
			}
		case weightedGraph.UnionOperator:
			omni := &omniInterpreter{
				hndNil:           handleUnsupported,
				hndDirect:        p.pipe.backend.handleDirectEdge,
				hndTTU:           p.pipe.backend.handleTTUEdge,
				hndComputed:      handleIdentity,
				hndRewrite:       handleIdentity,
				hndDirectLogical: handleIdentity,
				hndTTULogical:    handleIdentity,
			}

			r = &baseResolver{
				id:          id,
				ctx:         p.ctx,
				interpreter: omni,
				status:      status,
			}
		case weightedGraph.ExclusionOperator:
			omni := &omniInterpreter{
				hndNil:           handleUnsupported,
				hndDirect:        p.pipe.backend.handleDirectEdge,
				hndTTU:           p.pipe.backend.handleTTUEdge,
				hndComputed:      handleIdentity,
				hndRewrite:       handleIdentity,
				hndDirectLogical: handleIdentity,
				hndTTULogical:    handleIdentity,
			}

			r = &exclusionResolver{
				ctx:         p.ctx,
				interpreter: omni,
				trk:         trk,
			}
		default:
			panic("unsupported operator node for reverse expand worker")
		}
	case nodeTypeLogicalDirectGrouping:
		omni := &omniInterpreter{
			hndNil:           handleUnsupported,
			hndDirect:        p.pipe.backend.handleDirectEdge,
			hndTTU:           handleUnsupported,
			hndComputed:      handleUnsupported,
			hndRewrite:       handleUnsupported,
			hndDirectLogical: handleUnsupported,
			hndTTULogical:    handleUnsupported,
		}

		r = &baseResolver{
			id:          id,
			ctx:         p.ctx,
			interpreter: omni,
			status:      status,
		}
	case nodeTypeLogicalTTUGrouping:
		omni := &omniInterpreter{
			hndNil:           handleUnsupported,
			hndDirect:        handleUnsupported,
			hndTTU:           p.pipe.backend.handleTTUEdge,
			hndComputed:      handleUnsupported,
			hndRewrite:       handleUnsupported,
			hndDirectLogical: handleUnsupported,
			hndTTULogical:    handleUnsupported,
		}

		r = &baseResolver{
			id:          id,
			ctx:         p.ctx,
			interpreter: omni,
			status:      status,
		}
	default:
		panic("unsupported node type for reverse expand worker")
	}

	w.resolver = r

	pw := &w

	p.workers[node] = pw

	return pw
}

func (p *path) resolve(source *Node, target Target, trk tracker, status *StatusPool) {
	if _, ok := p.workers[source]; ok {
		return
	}

	if trk == nil {
		trk = newEchoTracker(&p.trk)
	}

	if status == nil {
		status = new(StatusPool)
	}

	w := p.worker(source, trk, status)

	p.workers[source] = w

	switch source.GetNodeType() {
	case nodeTypeSpecificType, nodeTypeSpecificTypeAndRelation:
		if source == target.node {
			// source node is the target node.
			var grp group
			grp.Items = []Item{{Value: target.id}}
			w.listen(nil, newStaticProducer(&p.trk, grp), p.pipe.chunkSize, 1) // only one value to consume, so only one processor necessary.
		}
	case nodeTypeSpecificTypeWildcard:
		label := source.GetLabel()
		typePart := strings.Split(label, ":")[0]

		if source == target.node || typePart == target.node.GetLabel() {
			// source node is the target node or has the same type as the target.
			var grp group
			grp.Items = []Item{{Value: "*"}}
			w.listen(nil, newStaticProducer(&p.trk, grp), p.pipe.chunkSize, 1) // only one value to consume, so only one processor necessary.
		}
	}

	edges, ok := p.pipe.backend.Graph.GetEdgesFromNode(source)
	if !ok {
		return
	}

	for _, edge := range edges {
		var track tracker
		var stat *StatusPool

		isRecursive := len(edge.GetRecursiveRelation()) > 0

		if isRecursive {
			track = w.trk
			stat = w.status
		}

		p.resolve(edge.GetTo(), target, track, stat)

		numProcs := p.pipe.numProcs

		if !isRecursive {
			switch edge.GetEdgeType() {
			case edgeTypeDirect, edgeTypeRewrite, edgeTypeComputed, edgeTypeDirectLogical:
				numProcs = 1
			}
		}

		w.listen(edge, p.workers[edge.GetTo()].subscribe(source), p.pipe.chunkSize, numProcs)
	}
}

func (p *Pipeline) Build(source Source, target Target) iter.Seq[Item] {
	ctx, cancel := context.WithCancel(context.Background())

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

	var wg sync.WaitGroup

	results := sourceWorker.subscribe(nil)

	for _, w := range pth.workers {
		w.start()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			var inactiveCount int

			for _, w := range pth.workers {
				if !w.active() {
					w.close()
					inactiveCount++
				}
			}

			if inactiveCount == len(pth.workers) {
				break
			}

			messageCount := pth.trk.Load()

			if messageCount < 1 || ctx.Err() != nil {
				// cancel all running workers
				for _, w := range pth.workers {
					w.close()
				}

				// wait for all workers to finish
				for _, w := range pth.workers {
					w.wait()
				}
				break
			}

			runtime.Gosched()
		}
	}()

	return func(yield func(Item) bool) {
		defer wg.Wait()
		defer results.close()
		defer cancel()

		for !results.done() {
			msg, ok := results.recv()
			if !ok {
				runtime.Gosched()
				continue
			}

			for _, item := range msg.Value.Items {
				if !yield(item) {
					msg.done()
					return
				}
			}
			msg.done()
		}
	}
}
