package reverseexpand

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"
	"strings"
	"sync"
	"time"

	aq "github.com/emirpasic/gods/queues/arrayqueue"
	"go.opentelemetry.io/otel/trace"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	weightedGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/checkutil"
	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/stack"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

const (
	listObjectsResultChannelLength = 100
)

var (
	ErrEmptyStack           = errors.New("unexpected empty stack")
	ErrLowestWeightFail     = errors.New("failed to get lowest weight edge")
	ErrConstructUsersetFail = errors.New("failed to construct userset")
)

type ExecutionError struct {
	operation string
	object    string
	relation  string
	user      string
	cause     error
}

func (e *ExecutionError) Error() string {
	return fmt.Sprintf("failed to execute: operation: %s: object: %s: relation: %s: user: %s: cause: %s",
		e.operation,
		e.object,
		e.relation,
		e.user,
		e.cause.Error(),
	)
}

// typeRelEntry represents a step in the path taken to reach a leaf node.
// As reverseExpand traverses from a requested type#rel to its leaf nodes, it stack.Pushes typeRelEntry structs to a stack.
// After reaching a leaf, this stack is consumed by the `queryForTuples` function to build the precise chain of
// database queries needed to find the resulting objects.
type typeRelEntry struct {
	typeRel string // e.g. "organization#admin"

	// Only present for userset relations. Will be the userset relation string itself.
	// For `rel admin: [team#member]`, usersetRelation is "member"
	usersetRelation string
}

// queryJob represents a single task in the reverse expansion process.
// It holds the `foundObject` from a previous step in the traversal
// and the `ReverseExpandRequest` containing the current state of the request.
type queryJob struct {
	foundObject string
	req         *ReverseExpandRequest
}

// jobQueue is a thread-safe queue for managing `queryJob` instances.
// It's used to hold jobs that need to be processed during the recursive
// `queryForTuples` operation, allowing concurrent processing of branches
// in the authorization graph.
type jobQueue struct {
	queue aq.Queue
	mu    sync.Mutex
}

func newJobQueue() *jobQueue {
	return &jobQueue{queue: *aq.New()}
}

func (q *jobQueue) Empty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.queue.Empty()
}

func (q *jobQueue) enqueue(value ...queryJob) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, item := range value {
		q.queue.Enqueue(item)
	}
}

func (q *jobQueue) dequeue() (queryJob, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	val, ok := q.queue.Dequeue()
	if !ok {
		return queryJob{}, false
	}
	job, ok := val.(queryJob)
	if !ok {
		return queryJob{}, false
	}

	return job, true
}

var emptySequence = func(yield func(Item) bool) {}
var emptyGroupSequence = func(yield func(Group) bool) {}

type (
	Graph = weightedGraph.WeightedAuthorizationModelGraph
	Node  = weightedGraph.WeightedAuthorizationModelNode
	Edge  = weightedGraph.WeightedAuthorizationModelEdge

	EdgeType = weightedGraph.EdgeType
)

var (
	EdgeTypeDirect   = weightedGraph.DirectEdge
	EdgeTypeComputed = weightedGraph.ComputedEdge
	EdgeTypeRewrite  = weightedGraph.RewriteEdge
	EdgeTypeTTU      = weightedGraph.TTUEdge

	NodeTypeSpecificType            = weightedGraph.SpecificType
	NodeTypeSpecificTypeWildcard    = weightedGraph.SpecificTypeWildcard
	NodeTypeSpecificTypeAndRelation = weightedGraph.SpecificTypeAndRelation
	NodeTypeOperator                = weightedGraph.OperatorNode

	ErrNoSuchNode         = errors.New("no such node in graph")
	ErrNoSuchPath         = errors.New("no path between source and target nodes")
	ErrConnectionOverflow = errors.New("too many connections")
	ErrTupleCycle         = errors.New("cycle detected in tuples")
)

type Item struct {
	Value string
	Err   error
}

type Group struct {
	Items []Item
}

func sequence[T any](items ...T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, item := range items {
			if !yield(item) {
				return
			}
		}
	}
}

func mergeOrdered[T any](seqs ...iter.Seq[T]) iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, seq := range seqs {
			for item := range seq {
				if !yield(item) {
					return
				}
			}
		}
	}
}

func mergeUnordered[T any](seqs ...iter.Seq[T]) iter.Seq[T] {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan T)

	var wg sync.WaitGroup

	for _, seq := range seqs {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for item := range seq {
				select {
				case ch <- item:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	var wg2 sync.WaitGroup

	wg2.Add(1)
	go func() {
		defer wg2.Done()
		defer close(ch)
		wg.Wait()
	}()

	return func(yield func(T) bool) {
		defer wg2.Wait()
		defer cancel()

		for {
			select {
			case <-ctx.Done():
				return
			case result, ok := <-ch:
				if !ok {
					return
				}

				if !yield(result) {
					return
				}
			}
		}
	}
}

func transform[T any, U any](seq iter.Seq[T], fn func(T) U) iter.Seq[U] {
	return func(yield func(U) bool) {
		for item := range seq {
			yield(fn(item))
		}
	}
}

func dedup[T comparable](seq iter.Seq[T]) iter.Seq[T] {
	seen := make(map[T]struct{})

	return func(yield func(T) bool) {
		for item := range seq {
			if _, ok := seen[item]; ok {
				continue
			}
			seen[item] = struct{}{}
			yield(item)
		}
	}
}

func unwrap[T any](seqs iter.Seq[iter.Seq[T]]) iter.Seq[T] {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan T)

	var wg sync.WaitGroup

	for seq := range seqs {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for item := range seq {
				select {
				case ch <- item:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	var wg2 sync.WaitGroup

	wg2.Add(1)
	go func() {
		defer wg2.Done()
		defer close(ch)
		wg.Wait()
	}()

	return func(yield func(T) bool) {
		defer wg2.Wait()
		defer cancel()

		for {
			select {
			case <-ctx.Done():
				return
			case result, ok := <-ch:
				if !ok {
					return
				}

				if !yield(result) {
					return
				}
			}
		}
	}
}

type canceler struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func tee[T any](seq iter.Seq[T], n int) []iter.Seq[T] {
	seqs := make([]iter.Seq[T], n)

	chans := make([]chan T, n)
	cancelers := make([]canceler, n)

	for i := 0; i < n; i++ {
		chans[i] = make(chan T)

		ctx, cancel := context.WithCancel(context.Background())

		cancelers[i] = canceler{
			ctx:    ctx,
			cancel: cancel,
		}
	}

	go func() {
		defer func() {
			for _, ch := range chans {
				close(ch)
			}
		}()

		var done int

		for value := range seq {
			for i, ch := range chans {
				select {
				case ch <- value:
				case <-cancelers[i].ctx.Done():
					done++
				}
			}

			if done == n {
				return
			}
		}
	}()

	for i := 0; i < n; i++ {
		seqs[i] = func(yield func(T) bool) {
			defer cancelers[i].cancel()

			for item := range seq {
				yield(item)
			}
		}
	}
	return seqs
}

type backend struct {
	datastore storage.RelationshipTupleReader
	storeId   string
}

func (b *backend) query(ctx context.Context, objectType, objectRelation string, userFilter []*openfgav1.ObjectRelation) iter.Seq[Item] {
	ctx, cancel := context.WithCancel(ctx)

	ch := make(chan Item)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer close(ch)
		defer wg.Done()

		it, err := b.datastore.ReadStartingWithUser(
			ctx,
			b.storeId,
			storage.ReadStartingWithUserFilter{
				ObjectType: objectType,
				Relation:   objectRelation,
				UserFilter: userFilter,
			},
			storage.ReadStartingWithUserOptions{},
		)

		if err != nil {
			select {
			case <-ctx.Done():
			case ch <- Item{Err: err}:
			}
			return
		}

		defer it.Stop()

		for {
			t, err := it.Next(ctx)

			if err != nil {
				if err == storage.ErrIteratorDone {
					break
				}

				select {
				case <-ctx.Done():
				case ch <- Item{Err: err}:
				}
				return
			}

			if t == nil {
				continue
			}

			select {
			case <-ctx.Done():
				return
			case ch <- Item{Value: t.GetKey().Object}:
			}
		}
	}()

	return func(yield func(Item) bool) {
		defer wg.Wait()
		defer cancel()

		for {
			select {
			case <-ctx.Done():
				return
			case result, ok := <-ch:
				if !ok {
					return
				}

				if !yield(result) {
					return
				}
			}
		}
	}
}

// coordinator is responsible for keeping and synchronizing the current
// state of a pipeline. The state of a pipeline is comprised of an active
// status as a boolean value, and a count of total in-flight messages within
// the pipeline. Each pipeline connection registered with the coordinator
// receives a unique identifier that must be provided when updating the status
// of a registered connection. A coordinator can support up to 256 registered
// connections. A coordinator must not be copied.
type coordinator struct {
	// mu protects the fields within the coordinator struct.
	mu sync.Mutex

	// top holds the next available identifier.
	top int

	// the following fields are bitpacked flags.
	// each bit corresponds to a boolean value; 1 = true, 0 = false.
	// each field has a size of 64 bits and holds 64 distinct boolan values.
	// the fields are used in order, until their bits have been assigned to capacity.
	//
	// identifiers 0 - 63 will be assigned a bit from field a.
	a uint64

	// identifiers 64 - 127 will be assigned a bit from field b.
	b uint64

	// identifiers 128 - 191 will be assigned a bit from field c.
	c uint64

	// identifiers 192 - 255 will be assigned a bit from field d.
	d uint64

	// more fields can be added to extend the total number of available bit flags.

	// messageCount holds the total number of in-flight messages within the pipeline.
	messageCount int64
}

// register is a function that returns a unique identifier for a pipeline
// connection. A coordinator can support up to 256 registered connections.
// If more than the supported maximum number of connections is registered,
// an ErrConnectionOverflow error is returned. The identifier returned by
// the register function must be provided when calling the setActive function
// for a connection.
func (c *coordinator) register() (int, error) {
	if c.top >= 256 {
		return 0, ErrConnectionOverflow
	}
	id := c.top
	c.top++
	return id, nil
}

// setActive is a function that accepts a registered connection identifier
// and an active status as a boolean value. The status value provided must
// reflect the state of the pipeline connection associated with the provided
// identifier. When active is true, the associated connection is assumed to
// be processing a message. When active is false, the associated connection
// is assumed to be waiting for a new message to arrive.
func (c *coordinator) setActive(id int, active bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	pos := uint64(1 << (id % 64))

	if id < 64 {
		if active {
			c.a |= pos
			return
		}
		c.a &^= pos
		return
	}

	if id < 128 {
		if active {
			c.b |= pos
			return
		}
		c.b &^= pos
		return
	}

	if id < 192 {
		if active {
			c.c |= pos
			return
		}
		c.c &^= pos
		return
	}

	if id < 256 {
		if active {
			c.d |= pos
			return
		}
		c.d &^= pos
		return
	}
	panic("too many connections")
}

// getState is a function that returns the pipeline state as a sum
// of all of its registered connections. The first return value indicates
// the cummulative status of all registered connections; a value of true
// indicates that at least one connection is processing a message. A
// value of false indicates that all connections are in a waiting state.
// The second return value indicates the number of currently in-flight
// messages across all connections; a value greater than 0 indicates that
// at least one message is waiting to be processed by a connection.
func (c *coordinator) getState() (bool, int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.a|c.b|c.c|c.d != 0, c.messageCount
}

// addMessages accepts a positive or negative value indicating the amount
// to increment the value of the coordinator's messageCount value by.
func (c *coordinator) addMessages(n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.messageCount += n
}

// listener is a struct that contains fields relevant to the listening
// end of a pipeline connection.
type listener struct {
	// ch is input channel of the listener.
	ch chan Group

	// ctx manages the lifetime of the listener.
	ctx context.Context

	// cancel will end the listeners lifetime when invoked.
	cancel context.CancelFunc

	// node is the weighted graph node that is listening.
	node *Node
}

// sender is a struct that contains fields relevant to the producing
// end of a pipeline connection.
type sender struct {
	// edge is the weighted graph edge that is producing.
	edge *Edge

	// seq is the continuous sequence of messages that will yield
	// message values until the consumer stops iterating, or the
	// mechanism feeding the seqence signals that it is done.
	seq iter.Seq[Group]
}

// resolver is an interface that is consumed by a Worker struct.
// a resolver is responsible for consuming messages from a Worker's
// senders and broadcasting the result of processing the consumed
// messages to the Worker's listeners.
type resolver interface {
	// Resolve is a function that consumes messages from the
	// provided senders, and broadcasts the results of processing
	// the consumed messages to the provided listeners.
	Resolve(senders []*sender, listeners []*listener)
}

// specificTypeResolver is a struct that implements the resolver interface
// for a specific type weighted graph node type.
type specificTypeResolver struct {
	// id is the identifier obtained when registering this connection with
	// the coordinator for the associated pipeline.
	id int

	// coord is an instance of a coordinator for the associated pipeline.
	coord *coordinator

	// backend is an instance containing the datastore and store identifier.
	backend *backend

	// node is a weighted graph node that is assocated with this resovler.
	node *Node
}

// Resolve is a function that consumes messages from the provided senders,
// processes the messages, and broadcasts the results to the provided listeners.
// Resolve expects messages of Items having values of typeless identifiers. The
// recieved identifiers will have the type of the node appended to them before
// broadcasting the identifiers to  the provided listeners.
func (r *specificTypeResolver) Resolve(senders []*sender, listeners []*listener) {
	var mu sync.Mutex

	var active uint64

	var wg sync.WaitGroup

	for i, snd := range senders {
		wg.Add(1)

		go func(i int, snd *sender) {
			defer wg.Done()

			pos := uint64(1 << i)

			for group := range snd.seq {
				var items []Item

				mu.Lock()
				active |= pos
				r.coord.setActive(r.id, active != 0)
				mu.Unlock()

				r.coord.addMessages(-1)

				for _, item := range group.Items {
					if item.Err != nil {
						items = append(items, item)
						continue
					}
					item.Value = r.node.GetLabel() + ":" + item.Value
					items = append(items, item)
				}

				mappedGroup := Group{
					Items: items,
				}

				for _, lst := range listeners {
					r.coord.addMessages(1)
					select {
					case lst.ch <- mappedGroup:
						// println("SENT", r.node.GetUniqueLabel(), "->", lst.node.GetUniqueLabel())
					case <-lst.ctx.Done():
						r.coord.addMessages(-1)
					}
				}
				mu.Lock()
				active &^= pos
				r.coord.setActive(r.id, active != 0)
				mu.Unlock()
			}
		}(i, snd)
	}
	wg.Wait()
	// println("RESOLVER DONE", r.node.GetUniqueLabel())
}

type specificTypeAndRelationResolver struct {
	id      int
	coord   *coordinator
	backend *backend
	node    *Node
}

func (r *specificTypeAndRelationResolver) Resolve(senders []*sender, listeners []*listener) {
	defer func() {
		// println("RESOLVER DONE", r.node.GetUniqueLabel())
	}()

	if len(senders) == 0 {
		// nothing to do
		return
	}

	// nexts holds the next function for each sender sequence
	nexts := make([]func() (Group, bool), len(senders))

	// stops holds the stop function for each sender sequence
	stops := make([]func(), len(senders))

	// stopped keeps track of which sender sequences have been fully consumed
	stopped := make([]bool, len(senders))

	// buffers holds a set of seen item values for each sender to avoid processing duplicates
	buffers := make([]map[string]struct{}, len(senders))

	for i, snd := range senders {
		next, stop := iter.Pull(snd.seq)
		nexts[i] = next
		stops[i] = stop
		buffers[i] = make(map[string]struct{})
	}

	var mu sync.Mutex

	// active is a bitfield representing which senders are currently processing messages
	var active uint64

	// ctr is used to assign a unique bit position to each sender
	var ctr uint64

	// wg is used to wait for all sender goroutines to finish
	var wg sync.WaitGroup

	// Start a goroutine for each sender to process incoming groups concurrently
	for ctr < uint64(len(senders)) {
		ndx := ctr
		pos := uint64(1 << ndx)

		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				if stopped[ndx] {
					// This sender has been fully consumed, exit the loop
					break
				}

				timer := time.AfterFunc(500*time.Millisecond, func() {
					// println("STUCK?", senders[ndx].edge.GetTo().GetUniqueLabel(), "->", r.node.GetUniqueLabel())
				})

				inGroup, ok := nexts[ndx]()
				if !ok {
					timer.Stop()
					stops[ndx]()
					stopped[ndx] = true
					// println("CLOSING", senders[ndx].edge.GetTo().GetUniqueLabel(), "->", r.node.GetUniqueLabel())
					break
				}

				timer.Stop()

				mu.Lock()
				active |= pos
				r.coord.setActive(r.id, active != 0)
				mu.Unlock()

				r.coord.addMessages(-1)

				var unseen []Item

				// Deduplicate items within this group based on the buffer for this sender
				for _, item := range inGroup.Items {
					if item.Err != nil {
						continue
					}

					if _, ok := buffers[ndx][item.Value]; ok {
						continue
					}
					unseen = append(unseen, item)
					buffers[ndx][item.Value] = struct{}{}
				}

				// If there are no unseen items, skip processing
				if len(unseen) == 0 {
					mu.Lock()
					active &^= pos
					r.coord.setActive(r.id, active != 0)
					mu.Unlock()
					continue
				}

				// println("RECIEVED", senders[ndx].edge.GetTo().GetUniqueLabel(), "->", r.node.GetUniqueLabel(), fmt.Sprintf("%+v", unseen))

				var results iter.Seq[Item]

				switch senders[ndx].edge.GetEdgeType() {
				case EdgeTypeDirect:
					// For direct edges, we need to query the backend to find all objects of the specified type

					parts := strings.Split(r.node.GetLabel(), "#")
					nodeType := parts[0]
					nodeRelation := parts[1]

					userParts := strings.Split(senders[ndx].edge.GetTo().GetLabel(), "#")

					var userRelation string

					if len(userParts) > 1 {
						userRelation = userParts[1]
					}

					var userFilter []*openfgav1.ObjectRelation

					var errs []Item

					for _, item := range unseen {
						if item.Err != nil {
							errs = append(errs, item)
							continue
						}

						userFilter = append(userFilter, &openfgav1.ObjectRelation{
							Object:   item.Value,
							Relation: userRelation,
						})
					}

					if len(userFilter) > 0 {
						results = r.backend.query(context.Background(), nodeType, nodeRelation, userFilter)
					} else {
						results = emptySequence
					}

					if len(errs) > 0 {
						results = mergeOrdered(sequence(errs...), results)
					}
				case EdgeTypeTTU:
					if r.node == senders[ndx].edge.GetFrom() {
						// We are on the specific type and relation that begins the TTU.
						results = sequence(inGroup.Items...)
						break
					}
					// We are on the tupleset node.
					parts := strings.Split(r.node.GetLabel(), "#")
					nodeType := parts[0]
					nodeRelation := parts[1]

					var userFilter []*openfgav1.ObjectRelation

					var errs []Item

					for _, item := range unseen {
						if item.Err != nil {
							errs = append(errs, item)
							continue
						}

						userFilter = append(userFilter, &openfgav1.ObjectRelation{
							Object:   item.Value,
							Relation: "",
						})
					}

					if len(userFilter) > 0 {
						results = r.backend.query(context.Background(), nodeType, nodeRelation, userFilter)
					} else {
						results = emptySequence
					}

					if len(errs) > 0 {
						results = mergeOrdered(sequence(errs...), results)
					}
				case EdgeTypeComputed, EdgeTypeRewrite:
					results = sequence(inGroup.Items...)
				}

				// collect all items produced in results
				// and send them as a single group to listeners
				// this ensures that we maintain some semblance of ordering
				// from the original input stream
				// while still allowing concurrent processing of multiple input streams
				// and deduplication within each stream
				//
				// NOTE: this does mean that a single slow listener
				// can hold up the processing of other input streams
				// but this is a tradeoff we're willing to make for now.
				// This is specifically a problem when a listener is the current producer,
				// as it can lead to a deadlock. This is always true for recursive edges.
				var items []Item

				for item := range results {
					items = append(items, item)
				}

				outGroup := Group{
					Items: items,
				}

				// if no items were produced, skip sending to listeners
				if len(outGroup.Items) == 0 {
					mu.Lock()
					active &^= pos
					r.coord.setActive(r.id, active != 0)
					mu.Unlock()
					continue
				}

				// send a copy of the produced group to all listeners
				for _, lst := range listeners {
					r.coord.addMessages(1)
					select {
					case lst.ch <- outGroup:
						if lst.node != nil {
							// println("SENT", r.node.GetUniqueLabel(), "->", lst.node.GetUniqueLabel(), fmt.Sprintf("%+v", outGroup))
						}
					case <-lst.ctx.Done():
						r.coord.addMessages(-1)
					}
				}
				mu.Lock()
				active &^= pos
				r.coord.setActive(r.id, active != 0)
				mu.Unlock()
			}
		}()

		ctr++
	}
	wg.Wait()

	for _, stop := range stops {
		stop()
	}
}

type unionResolver struct {
	id      int
	coord   *coordinator
	backend *backend
	node    *Node
}

func (r *unionResolver) Resolve(senders []*sender, listeners []*listener) {
	var wg sync.WaitGroup

	var muIn sync.Mutex

	inBuffer := make(map[string]struct{})

	var muOut sync.Mutex

	outBuffer := make(map[string]struct{})

	errs := make([][]Item, len(senders))

	var mu sync.Mutex

	var active uint64

	for i, snd := range senders {
		wg.Add(1)

		go func(i int, snd *sender) {
			defer wg.Done()

			pos := uint64(1 << i)

			for inGroup := range snd.seq {
				mu.Lock()
				active |= pos
				r.coord.setActive(r.id, active != 0)
				mu.Unlock()

				r.coord.addMessages(-1)

				output := make(map[string]struct{})

				var unseen []Item

				// Deduplicate items within this group based on the buffer for this sender
				for _, item := range inGroup.Items {
					if item.Err != nil {
						continue
					}

					var seen bool

					muIn.Lock()
					if _, ok := inBuffer[item.Value]; ok {
						seen = true
					} else {
						inBuffer[item.Value] = struct{}{}
					}
					muIn.Unlock()

					if !seen {
						unseen = append(unseen, item)
					}
				}

				// If there are no unseen items, skip processing
				if len(unseen) == 0 {
					mu.Lock()
					active &^= pos
					r.coord.setActive(r.id, active != 0)
					mu.Unlock()
					continue
				}

				var results iter.Seq[Item]

				switch snd.edge.GetEdgeType() {
				case EdgeTypeDirect:
					parts := strings.Split(snd.edge.GetRelationDefinition(), "#")
					nodeType := parts[0]
					nodeRelation := parts[1]

					userParts := strings.Split(snd.edge.GetTo().GetLabel(), "#")

					var userRelation string

					if len(userParts) > 1 {
						userRelation = userParts[1]
					}

					var userFilter []*openfgav1.ObjectRelation

					var errs []Item

					for _, item := range unseen {
						if item.Err != nil {
							errs = append(errs, item)
							continue
						}

						userFilter = append(userFilter, &openfgav1.ObjectRelation{
							Object:   item.Value,
							Relation: userRelation,
						})
					}

					if len(userFilter) > 0 {
						results = r.backend.query(context.Background(), nodeType, nodeRelation, userFilter)
					} else {
						results = emptySequence
					}

					if len(errs) > 0 {
						results = mergeOrdered(sequence(errs...), results)
					}
				case EdgeTypeComputed, EdgeTypeRewrite, EdgeTypeTTU:
					results = sequence(inGroup.Items...)

				}

				for item := range results {
					if item.Err != nil {
						errs[i] = append(errs[i], item)
					}

					muOut.Lock()
					if _, ok := outBuffer[item.Value]; !ok {
						output[item.Value] = struct{}{}
						outBuffer[item.Value] = struct{}{}
					}
					muOut.Unlock()

				}

				objects := make(map[string]struct{})

				for obj := range output {
					objects[obj] = struct{}{}
				}

				var allErrs []Item

				for _, errList := range errs {
					allErrs = append(allErrs, errList...)
				}

				seq := mergeOrdered(
					sequence(allErrs...),
					transform(maps.Keys(objects), func(o string) Item { return Item{Value: o} }),
				)

				var items []Item

				for item := range seq {
					items = append(items, item)
				}

				outGroup := Group{
					Items: items,
				}

				for _, lst := range listeners {
					r.coord.addMessages(1)
					select {
					case lst.ch <- outGroup:
						// println("SENT", r.node.GetLabel(), "->", lst.node.GetLabel())
					case <-lst.ctx.Done():
						r.coord.addMessages(-1)
					}
				}
				mu.Lock()
				active &^= pos
				r.coord.setActive(r.id, active != 0)
				mu.Unlock()
			}
		}(i, snd)
	}
	wg.Wait()
}

type intersectionResolver struct {
	id      int
	coord   *coordinator
	backend *backend
	node    *Node
}

func (r *intersectionResolver) Resolve(senders []*sender, listeners []*listener) {
	var wg sync.WaitGroup

	objects := make(map[string]struct{})

	buffers := make([]map[string]struct{}, len(senders))

	output := make([]map[string]struct{}, len(senders))

	for i := range senders {
		buffers[i] = make(map[string]struct{})
		output[i] = make(map[string]struct{})
	}

	errs := make([][]Item, len(senders))

	r.coord.setActive(r.id, true)

	for i, snd := range senders {
		wg.Add(1)

		go func(i int, snd *sender) {
			defer wg.Done()

			for inGroup := range snd.seq {
				r.coord.addMessages(-1)

				var unseen []Item

				// Deduplicate items within this group based on the buffer for this sender
				for _, item := range inGroup.Items {
					if item.Err != nil {
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
					continue
				}

				var results iter.Seq[Item]

				switch snd.edge.GetEdgeType() {
				case EdgeTypeDirect:
					parts := strings.Split(snd.edge.GetRelationDefinition(), "#")
					nodeType := parts[0]
					nodeRelation := parts[1]

					userParts := strings.Split(snd.edge.GetTo().GetLabel(), "#")

					var userRelation string

					if len(userParts) > 1 {
						userRelation = userParts[1]
					}

					var userFilter []*openfgav1.ObjectRelation

					var errs []Item

					for _, item := range unseen {
						if item.Err != nil {
							errs = append(errs, item)
							continue
						}

						userFilter = append(userFilter, &openfgav1.ObjectRelation{
							Object:   item.Value,
							Relation: userRelation,
						})
					}

					if len(userFilter) > 0 {
						results = r.backend.query(context.Background(), nodeType, nodeRelation, userFilter)
					} else {
						results = emptySequence
					}

					if len(errs) > 0 {
						results = mergeOrdered(sequence(errs...), results)
					}
				case EdgeTypeComputed, EdgeTypeRewrite, EdgeTypeTTU:
					results = sequence(inGroup.Items...)

				}

				for item := range results {
					if item.Err != nil {
						errs[i] = append(errs[i], item)
					}
					output[i][item.Value] = struct{}{}
				}
			}
		}(i, snd)
	}
	wg.Wait()

	for obj := range output[0] {
		objects[obj] = struct{}{}
	}

	for i := 1; i < len(output); i++ {
		found := make(map[string]struct{})

		for obj := range output[i] {
			if _, ok := objects[obj]; ok {
				found[obj] = struct{}{}
			}
		}
		objects = found
	}

	var allErrs []Item

	for _, errList := range errs {
		allErrs = append(allErrs, errList...)
	}

	seq := mergeOrdered(sequence(allErrs...), transform(maps.Keys(objects), func(o string) Item { return Item{Value: o} }))

	var items []Item

	for item := range seq {
		items = append(items, item)
	}

	outGroup := Group{
		Items: items,
	}

	for _, sub := range listeners {
		r.coord.addMessages(1)
		select {
		case sub.ch <- outGroup:
		case <-sub.ctx.Done():
			r.coord.addMessages(-1)
		}
	}
	r.coord.setActive(r.id, false)
}

type exclusionResolver struct {
	id      int
	coord   *coordinator
	backend *backend
	node    *Node
}

func (r *exclusionResolver) Resolve(senders []*sender, listeners []*listener) {
	if len(senders) < 2 {
		panic("exclusion resolver requires at least two senders")
	}
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		included := make(map[string]struct{})
		excluded := make(map[string]struct{})

		errs := make([][]Item, len(senders))

		var mu1 sync.Mutex
		var mu2 sync.Mutex

		var wg sync.WaitGroup

		r.coord.setActive(r.id, true)

		for i, snd := range senders {
			wg.Add(1)

			go func(i int, snd *sender) {
				defer wg.Done()

				for group := range snd.seq {
					r.coord.addMessages(-1)

					if snd.edge.GetEdgeType() == EdgeTypeDirect {
						// For direct edges, we need to query the backend to find all objects of the specified type

						parts := strings.Split(snd.edge.GetRelationDefinition(), "#")
						nodeType := parts[0]
						nodeRelation := parts[1]

						userParts := strings.Split(snd.edge.GetTo().GetLabel(), "#")

						var userRelation string

						if len(userParts) > 1 {
							userRelation = userParts[1]
						}

						var userFilter []*openfgav1.ObjectRelation

						for _, item := range group.Items {
							if item.Err != nil {
								errs[i] = append(errs[i], item)
								continue
							}

							userFilter = append(userFilter, &openfgav1.ObjectRelation{
								Object:   item.Value,
								Relation: userRelation,
							})
						}

						var results iter.Seq[Item]

						if len(userFilter) > 0 {
							results = r.backend.query(context.Background(), nodeType, nodeRelation, userFilter)
						} else {
							results = emptySequence
						}

						for item := range results {
							if item.Err != nil {
								errs[i] = append(errs[i], item)
								continue
							}

							mu1.Lock()
							included[item.Value] = struct{}{}
							mu1.Unlock()
						}
					} else {
						// TODO: handle TTU inclusion/exclusion cases
						for _, item := range group.Items {
							if item.Err != nil {
								errs[i] = append(errs[i], item)
								continue
							}
							if i == len(senders)-1 {
								mu2.Lock()
								excluded[item.Value] = struct{}{}
								mu2.Unlock()
							} else {
								mu1.Lock()
								included[item.Value] = struct{}{}
								mu1.Unlock()
							}
						}
					}
				}
			}(i, snd)
		}
		wg.Wait()

		for obj := range excluded {
			delete(included, obj)
		}

		var allErrs []Item

		for _, errList := range errs {
			allErrs = append(allErrs, errList...)
		}

		seq := mergeOrdered(sequence(allErrs...), transform(maps.Keys(included), func(o string) Item { return Item{Value: o} }))

		var items []Item

		for item := range seq {
			items = append(items, item)
		}

		outGroup := Group{
			Items: items,
		}

		for _, sub := range listeners {
			r.coord.addMessages(1)
			select {
			case sub.ch <- outGroup:
			case <-sub.ctx.Done():
				r.coord.addMessages(-1)
			}
		}
		r.coord.setActive(r.id, false)
	}()
	wg.Wait()
}

type Worker struct {
	senders   []*sender
	listeners []*listener
	resolver  resolver
	wg        sync.WaitGroup
}

func NewWorker(backend *backend, node *Node, coord *coordinator) *Worker {
	var r resolver

	id, err := coord.register()
	if err != nil {
		panic(err)
	}

	switch node.GetNodeType() {
	case NodeTypeSpecificTypeAndRelation:
		r = &specificTypeAndRelationResolver{
			id:      id,
			coord:   coord,
			backend: backend,
			node:    node,
		}
	case NodeTypeSpecificType:
		r = &specificTypeResolver{
			id:      id,
			coord:   coord,
			backend: backend,
			node:    node,
		}
	case NodeTypeOperator:
		switch node.GetLabel() {
		case weightedGraph.IntersectionOperator:
			r = &intersectionResolver{
				id:      id,
				coord:   coord,
				backend: backend,
				node:    node,
			}
		case weightedGraph.UnionOperator:
			r = &unionResolver{
				id:      id,
				coord:   coord,
				backend: backend,
				node:    node,
			}
		case weightedGraph.ExclusionOperator:
			r = &exclusionResolver{
				id:      id,
				coord:   coord,
				backend: backend,
				node:    node,
			}
		default:
			panic("unsupported operator node for reverse expand worker")
		}
	default:
		panic("unsupported node type for reverse expand worker")
	}

	return &Worker{
		resolver: r,
	}
}

func (w *Worker) Start() {
	w.wg.Add(1)

	go func() {
		defer w.wg.Done()

		defer func() {
			for _, lst := range w.listeners {
				close(lst.ch)
			}
		}()

		w.resolver.Resolve(w.senders, w.listeners)
	}()
}

func (w *Worker) Cancel() {
	for _, lst := range w.listeners {
		lst.cancel()
	}
}

func (w *Worker) Wait() {
	w.wg.Wait()
}

func (w *Worker) Listen(edge *Edge, i iter.Seq[Group]) {
	w.senders = append(w.senders, &sender{
		edge: edge,
		seq:  i,
	})
}

func (w *Worker) Subscribe(ctx context.Context, node *Node) iter.Seq[Group] {
	ctx, cancel := context.WithCancel(ctx)
	ch := make(chan Group, 100)

	w.listeners = append(w.listeners, &listener{
		ch:     ch,
		ctx:    ctx,
		cancel: cancel,
		node:   node,
	})

	return func(yield func(Group) bool) {
		defer cancel()

		for {
			select {
			case <-ctx.Done():
				return
			case items, ok := <-ch:
				if !ok {
					return
				}

				if !yield(items) {
					return
				}
			}
		}
	}
}

func output(cancel context.CancelFunc, seq iter.Seq[Group], outWg *sync.WaitGroup, coord *coordinator) iter.Seq[Item] {
	ch := make(chan Item, 1)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(ch)

		for group := range seq {
			coord.addMessages(-1)
			for _, item := range group.Items {
				ch <- item
			}
		}
	}()

	return func(yield func(Item) bool) {
		defer outWg.Wait()
		defer wg.Wait()
		defer cancel()

		for item := range ch {
			if !yield(item) {
				return
			}
		}
	}
}

type Traversal struct {
	graph    *Graph
	backend  *backend
	pipeline map[*Node]*Worker
}

type Target *Node
type Source *Node

func (t *Traversal) Target(name string) (Target, bool) {
	targetNode, ok := t.graph.GetNodeByID(name)
	return (Target)(targetNode), ok
}

func (t *Traversal) Source(name, relation string) (Source, bool) {
	sourceNode, ok := t.graph.GetNodeByID(name + "#" + relation)
	return (Source)(sourceNode), ok
}

func (t *Traversal) Traverse(source Source, target Target, targetIdentifiers ...string) Path {
	return Path{
		traversal:         t,
		source:            (*Node)(source),
		target:            (*Node)(target),
		targetIdentifiers: targetIdentifiers,
	}
}

type Path struct {
	source            *Node
	traversal         *Traversal
	target            *Node
	targetIdentifiers []string
}

func (p *Path) Objects(ctx context.Context) iter.Seq[Item] {
	ctx, cancel := context.WithCancel(ctx)

	var coord coordinator

	p.resolve(ctx, p.source, &coord)

	targetWorker, ok := p.traversal.pipeline[p.target]
	if !ok {
		panic("no such source worker")
	}

	var items []Item

	for _, id := range p.targetIdentifiers {
		items = append(items, Item{Value: id})
	}

	outGroup := Group{
		Items: items,
	}

	seq := sequence(outGroup)

	coord.addMessages(1)

	targetWorker.Listen(nil, seq)

	sourceWorker, ok := p.traversal.pipeline[p.source]
	if !ok {
		panic("no such target worker")
	}

	var wg sync.WaitGroup

	results := output(cancel, sourceWorker.Subscribe(context.Background(), nil), &wg, &coord)

	for _, worker := range p.traversal.pipeline {
		worker.Start()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			var busy bool

			busy, messageCount := coord.getState()

			// println("busy: ", busy, " message count: ", messageCount)

			if !busy && messageCount < 1 {
				for _, worker := range p.traversal.pipeline {
					/*
						var node *Node
						switch worker.resolver.(type) {
						case *specificTypeResolver:
							node = worker.resolver.(*specificTypeResolver).node
						case *specificTypeAndRelationResolver:
							node = worker.resolver.(*specificTypeAndRelationResolver).node
						case *unionResolver:
							node = worker.resolver.(*unionResolver).node
						case *intersectionResolver:
							node = worker.resolver.(*intersectionResolver).node
						}
						println("CLOSING WORKER", node.GetUniqueLabel())
					*/
					worker.Cancel()
				}

				for _, worker := range p.traversal.pipeline {
					worker.Wait()
				}
				break
			}
		}
	}()

	return results
}

func (p *Path) resolve(ctx context.Context, source *Node, coord *coordinator) {
	if p.traversal.pipeline == nil {
		p.traversal.pipeline = make(map[*Node]*Worker)
	}

	_, ok := source.GetWeight(p.target.GetLabel())
	if !ok {
		// println(source.GetLabel())
		return
	}

	if _, ok := p.traversal.pipeline[source]; ok {
		return
	}
	p.traversal.pipeline[source] = NewWorker(p.traversal.backend, source, coord)

	edges, ok := p.traversal.graph.GetEdgesFromNode(source)
	if !ok {
		panic("given node not a member of graph")
	}

	for _, edge := range edges {
		if edge.GetEdgeType() == EdgeTypeTTU {
			tupleset, ok := p.traversal.graph.GetNodeByID(edge.GetTuplesetRelation())
			if !ok {
				panic("tupleset relation not in graph")
			}

			if _, ok := p.traversal.pipeline[tupleset]; !ok {
				p.traversal.pipeline[tupleset] = NewWorker(p.traversal.backend, tupleset, coord)
				p.traversal.pipeline[source].Listen(edge, p.traversal.pipeline[tupleset].Subscribe(ctx, source))
			}
			p.resolve(ctx, edge.GetTo(), coord)
			p.traversal.pipeline[tupleset].Listen(edge, p.traversal.pipeline[edge.GetTo()].Subscribe(ctx, tupleset))
			continue
		}

		if edge.GetEdgeType() == EdgeTypeDirect {
			if edge.GetTo().GetNodeType() == NodeTypeSpecificType {
				if _, ok := p.traversal.pipeline[edge.GetTo()]; !ok {
					p.traversal.pipeline[edge.GetTo()] = NewWorker(p.traversal.backend, edge.GetTo(), coord)
					p.traversal.pipeline[edge.GetTo()].Listen(edge, emptyGroupSequence)
				}
				p.traversal.pipeline[source].Listen(edge, p.traversal.pipeline[edge.GetTo()].Subscribe(ctx, source))
				continue
			}
		}

		p.resolve(ctx, edge.GetTo(), coord)
		p.traversal.pipeline[source].Listen(edge, p.traversal.pipeline[edge.GetTo()].Subscribe(ctx, source))
	}
}

// loopOverEdges iterates over a set of weightedGraphEdges and acts as a dispatcher,
// processing each edge according to its type to continue the reverse expansion process.
//
// While traversing, loopOverEdges appends relation entries to a stack for use in querying after traversal is complete.
// It will continue to dispatch and traverse the graph until it reaches a DirectEdge, which
// leads to a leaf node in the authorization graph. Once a DirectEdge is found, loopOverEdges invokes
// queryForTuples, passing it the stack of relations it constructed on the way to that particular leaf.
//
// For each edge, it creates a new ReverseExpandRequest, preserving the context of the overall query
// but updating the traversal state (the 'stack') based on the edge being processed.
//
// The behavior is determined by the edge type:
//
//   - DirectEdge: This represents a direct path to data. Here we initiate a call to
//     `queryForTuples` to query the datastore for tuples that match the relationship path
//     accumulated in the stack. This is the end of the traversal.
//
//   - ComputedEdge, RewriteEdge, and TTUEdge: These represent indirections in the authorization model.
//     The function modifies the traversal 'stack' to reflect the next relationship that needs to be resolved.
//     It then calls `dispatch` to continue traversing the graph with this new state until it reaches a DirectEdge.
func (c *ReverseExpandQuery) loopOverEdges(
	ctx context.Context,
	req *ReverseExpandRequest,
	edges []*weightedGraph.WeightedAuthorizationModelEdge,
	needsCheck bool,
	resolutionMetadata *ResolutionMetadata,
	resultChan chan<- *ReverseExpandResult,
	sourceUserType string,
) error {
	pool := concurrency.NewPool(ctx, int(c.resolveNodeBreadthLimit))

	for _, edge := range edges {
		newReq := req.clone()
		newReq.weightedEdge = edge

		toNode := edge.GetTo()
		goingToUserset := toNode.GetNodeType() == weightedGraph.SpecificTypeAndRelation

		// Going to a userset presents risk of infinite loop. Checking the edge and the traversal stack
		// ensures we don't perform the same traversal multiple times.
		if goingToUserset {
			key := edge.GetFrom().GetUniqueLabel() + toNode.GetUniqueLabel() + edge.GetTuplesetRelation() + stack.String(newReq.relationStack)
			_, loaded := c.visitedUsersetsMap.LoadOrStore(key, struct{}{})
			if loaded {
				// we've already visited this userset through this edge, exit to avoid an infinite cycle
				continue
			}
		}

		switch edge.GetEdgeType() {
		case weightedGraph.DirectEdge:
			if goingToUserset {
				// Attach the userset relation to the previous stack entry
				//  type team:
				//		define member: [user]
				//	type org:
				//		define teammate: [team#member]
				// A direct edge here is org#teammate --> team#member
				// so if we find team:fga for this user, we need to know to check for
				// team:fga#member when we check org#teammate
				if newReq.relationStack == nil {
					return ErrEmptyStack
				}
				entry, newStack := stack.Pop(newReq.relationStack)
				entry.usersetRelation = tuple.GetRelation(toNode.GetUniqueLabel())

				newStack = stack.Push(newStack, entry)
				newStack = stack.Push(newStack, typeRelEntry{typeRel: toNode.GetUniqueLabel()})
				newReq.relationStack = newStack

				// Now continue traversing
				pool.Go(func(ctx context.Context) error {
					return c.dispatch(ctx, newReq, resultChan, needsCheck, resolutionMetadata)
				})
				continue
			}

			// We have reached a leaf node in the graph (e.g. `user` or `user:*`),
			// and the traversal for this path is complete. Now we use the stack of relations
			// we've built to query the datastore for matching tuples.
			pool.Go(func(ctx context.Context) error {
				return c.queryForTuples(
					ctx,
					newReq,
					needsCheck,
					resultChan,
					"",
				)
			})
		case weightedGraph.ComputedEdge:
			// A computed edge is an alias (e.g., `define viewer: editor`).
			// We replace the current relation on the stack (`viewer`) with the computed one (`editor`),
			// as tuples are only written against `editor`.
			if toNode.GetNodeType() != weightedGraph.OperatorNode {
				if newReq.relationStack == nil {
					return ErrEmptyStack
				}
				_, newStack := stack.Pop(newReq.relationStack)
				newStack = stack.Push(newStack, typeRelEntry{typeRel: toNode.GetUniqueLabel()})
				newReq.relationStack = newStack
			}

			pool.Go(func(ctx context.Context) error {
				return c.dispatch(ctx, newReq, resultChan, needsCheck, resolutionMetadata)
			})
		case weightedGraph.TTUEdge:
			// Replace the existing type#rel on the stack with the tuple-to-userset relation:
			//
			// 	type document
			//		define parent: [folder]
			//		define viewer: admin from parent
			//
			// We need to remove document#viewer from the stack and replace it with the tupleset relation (`document#parent`).
			// Then we have to add the .To() relation `folder#admin`.
			// The stack becomes `[document#parent, folder#admin]`, and on evaluation we will first
			// query for folder#admin, then if folders exist we will see if they are related to
			// any documents as #parent.
			if newReq.relationStack == nil {
				return ErrEmptyStack
			}
			_, newStack := stack.Pop(newReq.relationStack)

			// stack.Push tupleset relation (`document#parent`)
			tuplesetRel := typeRelEntry{typeRel: edge.GetTuplesetRelation()}
			newStack = stack.Push(newStack, tuplesetRel)

			// stack.Push target type#rel (`folder#admin`)
			newStack = stack.Push(newStack, typeRelEntry{typeRel: toNode.GetUniqueLabel()})
			newReq.relationStack = newStack

			pool.Go(func(ctx context.Context) error {
				return c.dispatch(ctx, newReq, resultChan, needsCheck, resolutionMetadata)
			})
		case weightedGraph.RewriteEdge:
			// Behaves just like ComputedEdge above
			// Operator nodes (union, intersection, exclusion) are not real types, they never get added
			// to the stack.
			if toNode.GetNodeType() != weightedGraph.OperatorNode {
				if newReq.relationStack == nil {
					return ErrEmptyStack
				}
				_, newStack := stack.Pop(newReq.relationStack)
				newStack = stack.Push(newStack, typeRelEntry{typeRel: toNode.GetUniqueLabel()})
				newReq.relationStack = newStack

				pool.Go(func(ctx context.Context) error {
					return c.dispatch(ctx, newReq, resultChan, needsCheck, resolutionMetadata)
				})
				// continue to the next edge
				break
			}

			// If the edge is an operator node, we need to handle it differently.
			switch toNode.GetLabel() {
			case weightedGraph.IntersectionOperator:
				err := c.intersectionHandler(pool, newReq, resultChan, toNode, sourceUserType, resolutionMetadata)
				if err != nil {
					return err
				}
			case weightedGraph.ExclusionOperator:
				err := c.exclusionHandler(ctx, pool, newReq, resultChan, toNode, sourceUserType, resolutionMetadata)
				if err != nil {
					return err
				}
			case weightedGraph.UnionOperator:
				pool.Go(func(ctx context.Context) error {
					return c.dispatch(ctx, newReq, resultChan, needsCheck, resolutionMetadata)
				})
			default:
				return fmt.Errorf("unsupported operator node: %s", toNode.GetLabel())
			}
		default:
			return fmt.Errorf("unsupported edge type: %v", edge.GetEdgeType())
		}
	}

	// In order to maintain the current ListObjects behavior, in the case of timeout in reverse_expand_weighted
	// we will return partial results.
	// For more detail, see here: https://openfga.dev/api/service#/Relationship%20Queries/ListObjects
	err := pool.Wait()
	if err != nil {
		var executionError *ExecutionError
		if errors.As(err, &executionError) {
			if errors.Is(executionError.cause, context.Canceled) || errors.Is(executionError.cause, context.DeadlineExceeded) {
				return nil
			}
		}
	}
	return err
}

// queryForTuples performs all datastore-related reverse expansion logic. After a leaf node has been found in loopOverEdges,
// this function works backwards from a specified user (using the stack created in loopOverEdges)
// and an initial relationship edge to find all the objects that the given user has the given relationship with.
//
// This function orchestrates the concurrent execution of individual query jobs. It initializes a memoization
// map (`jobDedupeMap`) to prevent redundant database queries and a job queue to manage pending tasks.
// It kicks off the initial query and then continuously processes jobs from the queue using a concurrency pool
// until all branches leading up from the leaf have been explored.
func (c *ReverseExpandQuery) queryForTuples(
	ctx context.Context,
	req *ReverseExpandRequest,
	needsCheck bool,
	resultChan chan<- *ReverseExpandResult,
	foundObject string,
) error {
	span := trace.SpanFromContext(ctx)

	queryJobQueue := newJobQueue()

	// Now kick off the chain of queries
	items, err := c.executeQueryJob(ctx, queryJob{req: req, foundObject: foundObject}, resultChan, needsCheck)
	if err != nil {
		telemetry.TraceError(span, err)
		return err
	}

	// Populate the jobQueue with the initial jobs
	queryJobQueue.enqueue(items...)

	// We could potentially have c.resolveNodeBreadthLimit active routines reaching this point.
	// Limit querying routines to avoid explosion of routines.
	pool := concurrency.NewPool(ctx, int(c.resolveNodeBreadthLimit))

	for !queryJobQueue.Empty() {
		job, ok := queryJobQueue.dequeue()
		if !ok {
			// this shouldn't be possible
			return nil
		}

		// Each goroutine will take its first job from the original queue above
		// and then continue generating and processing jobs until there are no more.
		pool.Go(func(ctx context.Context) error {
			localQueue := newJobQueue()
			localQueue.enqueue(job)

			// While this goroutine's queue has items, keep looking for more
			for !localQueue.Empty() {
				nextJob, ok := localQueue.dequeue()
				if !ok {
					break
				}
				newItems, err := c.executeQueryJob(ctx, nextJob, resultChan, needsCheck)
				if err != nil {
					return err
				}
				localQueue.enqueue(newItems...)
			}

			return nil
		})
	}

	err = pool.Wait()
	if err != nil {
		telemetry.TraceError(span, err)
		return err
	}

	return nil
}

// executeQueryJob represents a single recursive step in the reverse expansion query process.
// It takes a `queryJob`, which encapsulates the current state of the traversal (found object,
// and the reverse expand request with its relation stack).
// The method constructs a database query based on the current relation at the top of the stack
// and the `foundObject` from the previous step. It queries the datastore, and for each result:
//   - If the relation stack is empty, it means a candidate object has been found, which is then sent to `resultChan`.
//   - If matching tuples are found, it prepares new `queryJob` instances to continue the traversal further up the graph,
//     using the newly found object as the `foundObject` for the next step.
//   - If no matching objects are found in the datastore, this branch of reverse expand is a dead end, and no more jobs are needed.
func (c *ReverseExpandQuery) executeQueryJob(
	ctx context.Context,
	job queryJob,
	resultChan chan<- *ReverseExpandResult,
	needsCheck bool,
) ([]queryJob, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Ensure we're always working with a copy
	currentReq := job.req.clone()

	userFilter, err := buildUserFilter(currentReq, job.foundObject)
	if err != nil {
		return nil, err
	}

	if currentReq.relationStack == nil {
		return nil, ErrEmptyStack
	}

	// Now pop the top relation off of the stack for querying
	entry, newStack := stack.Pop(currentReq.relationStack)
	typeRel := entry.typeRel

	currentReq.relationStack = newStack

	objectType, relation := tuple.SplitObjectRelation(typeRel)

	filteredIter, err := c.buildFilteredIterator(ctx, currentReq, objectType, relation, userFilter)
	if err != nil {
		return nil, err
	}
	defer filteredIter.Stop()

	var nextJobs []queryJob

	for {
		tupleKey, err := filteredIter.Next(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}
			return nil, err
		}

		// This will be a "type:id" e.g. "document:roadmap"
		foundObject := tupleKey.GetObject()

		// If there are no more type#rel to look for in the stack that means we have hit the base case
		// and this object is a candidate for return to the user.
		if currentReq.relationStack == nil {
			c.trySendCandidate(ctx, needsCheck, foundObject, resultChan)
			continue
		}

		// For non-recursive relations (majority of cases), if there are more items on the stack, we continue
		// the evaluation one level higher up the tree with the `foundObject`.
		nextJobs = append(nextJobs, queryJob{foundObject: foundObject, req: currentReq})
	}

	return nextJobs, err
}

func buildUserFilter(
	req *ReverseExpandRequest,
	object string,
) ([]*openfgav1.ObjectRelation, error) {
	var filter *openfgav1.ObjectRelation
	// This is true on every call to queryFunc except the first, since we only trigger subsequent
	// calls if we successfully found an object.
	if object != "" {
		if req.relationStack == nil {
			return nil, ErrEmptyStack
		}

		entry := stack.Peek(req.relationStack)
		filter = &openfgav1.ObjectRelation{Object: object}
		if entry.usersetRelation != "" {
			filter.Relation = entry.usersetRelation
		}
	} else {
		// This else block ONLY hits on the first call to queryFunc.
		toNode := req.weightedEdge.GetTo()

		switch toNode.GetNodeType() {
		case weightedGraph.SpecificType: // Direct User Reference. To() -> "user"
			// req.User will always be either a UserRefObject or UserRefTypedWildcard here. Queries that come in for
			// pure usersets do not take this code path. e.g. ListObjects(team:fga#member, document, viewer) will not make it here.
			var userID string
			val, ok := req.User.(*UserRefObject)
			if ok {
				userID = val.Object.GetId()
			} else {
				// It might be a wildcard user, which is ok
				_, ok = req.User.(*UserRefTypedWildcard)
				if !ok {
					return nil, fmt.Errorf("unexpected user type when building User filter: %T", val)
				}
				return []*openfgav1.ObjectRelation{}, nil
			}

			filter = &openfgav1.ObjectRelation{Object: tuple.BuildObject(toNode.GetUniqueLabel(), userID)}

		case weightedGraph.SpecificTypeWildcard: // Wildcard Referece To() -> "user:*"
			filter = &openfgav1.ObjectRelation{Object: toNode.GetUniqueLabel()}
		}
	}

	return []*openfgav1.ObjectRelation{filter}, nil
}

// buildFilteredIterator constructs the iterator used when reverse_expand queries for tuples.
// The returned iterator MUST have .Stop() called on it.
func (c *ReverseExpandQuery) buildFilteredIterator(
	ctx context.Context,
	req *ReverseExpandRequest,
	objectType string,
	relation string,
	userFilter []*openfgav1.ObjectRelation,
) (storage.TupleKeyIterator, error) {
	iter, err := c.datastore.ReadStartingWithUser(ctx, req.StoreID, storage.ReadStartingWithUserFilter{
		ObjectType: objectType,
		Relation:   relation,
		UserFilter: userFilter,
	}, storage.ReadStartingWithUserOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: req.Consistency,
		},
	})
	if err != nil {
		return nil, err
	}

	// filter out invalid tuples yielded by the database iterator
	return storage.NewConditionsFilteredTupleKeyIterator(
		storage.NewFilteredTupleKeyIterator(
			storage.NewTupleKeyIteratorFromTupleIterator(iter),
			validation.FilterInvalidTuples(c.typesystem),
		),
		checkutil.BuildTupleKeyConditionFilter(ctx, req.Context, c.typesystem),
	), nil
}

// findCandidatesForLowestWeightEdge finds the candidate objects for the lowest weight edge for intersection or exclusion.
func (c *ReverseExpandQuery) findCandidatesForLowestWeightEdge(
	pool *concurrency.Pool,
	req *ReverseExpandRequest,
	tmpResultChan chan<- *ReverseExpandResult,
	edges []*weightedGraph.WeightedAuthorizationModelEdge,
	sourceUserType string,
	resolutionMetadata *ResolutionMetadata,
) {
	// We need to create a new stack with the top item from the original request's stack
	// and use it to get the candidates for the lowest weight edge.
	// If the edge is a tuple to userset edge, we need to later check the candidates against the
	// original relationStack with the top item removed.
	var topItemStack stack.Stack[typeRelEntry]
	if req.relationStack != nil {
		topItem, newStack := stack.Pop(req.relationStack)
		req.relationStack = newStack
		topItemStack = stack.Push(nil, topItem)
	}

	// getting list object candidates from the lowest weight edge and have its result
	// pass through tmpResultChan.
	pool.Go(func(ctx context.Context) error {
		defer close(tmpResultChan)
		// stack with only the top item in it
		newReq := req.clone()
		newReq.relationStack = topItemStack
		err := c.shallowClone().loopOverEdges(
			ctx,
			newReq,
			edges,
			false,
			resolutionMetadata,
			tmpResultChan,
			sourceUserType,
		)
		return err
	})
}

// callCheckForCandidates calls check on the list objects candidate against non lowest weight edges.
func (c *ReverseExpandQuery) callCheckForCandidate(
	ctx context.Context,
	req *ReverseExpandRequest,
	tmpResult *ReverseExpandResult,
	resultChan chan<- *ReverseExpandResult,
	userset *openfgav1.Userset,
	isAllowed bool,
	resolutionMetadata *ResolutionMetadata,
) error {
	resolutionMetadata.CheckCounter.Add(1)
	handlerFunc := c.localCheckResolver.CheckRewrite(ctx,
		&graph.ResolveCheckRequest{
			StoreID:              req.StoreID,
			AuthorizationModelID: c.typesystem.GetAuthorizationModelID(),
			TupleKey:             tuple.NewTupleKey(tmpResult.Object, req.Relation, req.User.String()),
			ContextualTuples:     req.ContextualTuples,
			Context:              req.Context,
			Consistency:          req.Consistency,
			RequestMetadata:      graph.NewCheckRequestMetadata(),
		}, userset)
	tmpCheckResult, err := handlerFunc(ctx)
	if err != nil {
		operation := "intersection"
		if !isAllowed {
			operation = "exclusion"
		}

		return &ExecutionError{
			operation: operation,
			object:    tmpResult.Object,
			relation:  req.Relation,
			user:      req.User.String(),
			cause:     err,
		}
	}

	// If the allowed value does not match what we expect, we skip this candidate.
	// eg, for intersection we expect the check result to be true
	// and for exclusion we expect the check result to be false.
	if tmpCheckResult.GetAllowed() != isAllowed {
		return nil
	}

	// If the original stack only had 1 value, we can trySendCandidate right away (nothing more to check)
	if stack.Len(req.relationStack) == 0 {
		c.trySendCandidate(ctx, false, tmpResult.Object, resultChan)
		return nil
	}

	// If the original stack had more than 1 value, we need to query the parent values
	// new stack with top item in stack
	err = c.queryForTuples(ctx, req, false, resultChan, tmpResult.Object)
	if err != nil {
		return err
	}
	return nil
}

// callCheckForCandidates calls check on the list objects candidates against non lowest weight edges.
func (c *ReverseExpandQuery) callCheckForCandidates(
	pool *concurrency.Pool,
	req *ReverseExpandRequest,
	tmpResultChan <-chan *ReverseExpandResult,
	resultChan chan<- *ReverseExpandResult,
	userset *openfgav1.Userset,
	isAllowed bool,
	resolutionMetadata *ResolutionMetadata,
) {
	pool.Go(func(ctx context.Context) error {
		// note that we create a separate goroutine pool instead of the main pool
		// to avoid starvation on the main pool as there could be many candidates
		// arriving concurrently.
		tmpResultPool := concurrency.NewPool(ctx, int(c.resolveNodeBreadthLimit))

		for tmpResult := range tmpResultChan {
			tmpResultPool.Go(func(ctx context.Context) error {
				return c.callCheckForCandidate(ctx, req, tmpResult, resultChan, userset, isAllowed, resolutionMetadata)
			})
		}
		return tmpResultPool.Wait()
	})
}

// invoke loopOverWeightedEdges to get list objects candidate. Check
// will then be invoked on the non-lowest weight edges against these
// list objects candidates. If check returns true, then the list
// object candidates are true candidates and will be returned via
// resultChan. If check returns false, then these list object candidates
// are invalid because it does not satisfy all paths for intersection.
func (c *ReverseExpandQuery) intersectionHandler(
	pool *concurrency.Pool,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	intersectionNode *weightedGraph.WeightedAuthorizationModelNode,
	sourceUserType string,
	resolutionMetadata *ResolutionMetadata,
) error {
	if intersectionNode == nil || intersectionNode.GetNodeType() != weightedGraph.OperatorNode || intersectionNode.GetLabel() != weightedGraph.IntersectionOperator {
		return fmt.Errorf("%w: operation: intersection: %s", errors.ErrUnsupported, "invalid intersection node")
	}

	// verify if the node has weight to the sourceUserType
	edges, err := c.typesystem.GetEdgesFromNode(intersectionNode, sourceUserType)
	if err != nil {
		return err
	}

	// when the intersection node has a weight to the sourceUserType then it means all the group edges has weight to the sourceUserType
	intersectionEdges, err := typesystem.GetEdgesForIntersection(edges, sourceUserType)
	if err != nil {
		return fmt.Errorf("%w: operation: intersection: %s", ErrLowestWeightFail, err.Error())
	}

	// note that we should never see a case where no edges to call LO
	// i.e., len(intersectionEdges.LowestEdges) == 0 or we cannot call check (i.e., len(intersectionEdges.SiblingEdges) == 0)
	// because typesystem.GetEdgesFromNode should have returned an error

	tmpResultChan := make(chan *ReverseExpandResult, listObjectsResultChannelLength)
	intersectEdges := intersectionEdges.SiblingEdges
	usersets := make([]*openfgav1.Userset, 0, len(intersectEdges))
	for _, intersectEdge := range intersectEdges {
		// no matter how many direct edges we have, or ttu edges  they for typesystem only required this
		// no matter how many parent types have for the same ttu rel from parent will be only one created in the typesystem
		// for any other case, does not have more than one edge, the groupings only occur in direct edges or ttu edges
		userset, err := c.typesystem.ConstructUserset(intersectEdge[0], sourceUserType)
		if err != nil {
			// this should never happen
			return fmt.Errorf("%w: operation: intersection: %s", ErrConstructUsersetFail, err.Error())
		}
		usersets = append(usersets, userset)
	}

	var userset *openfgav1.Userset
	switch len(usersets) {
	case 0:
		return fmt.Errorf("%w: empty connected edges", ErrConstructUsersetFail) // defensive; should be handled by the early return above
	case 1:
		userset = usersets[0]
	default:
		userset = typesystem.Intersection(usersets...)
	}

	// Concurrently find candidates and call check on them as they are found
	c.findCandidatesForLowestWeightEdge(pool, req, tmpResultChan, intersectionEdges.LowestEdges, sourceUserType, resolutionMetadata)
	c.callCheckForCandidates(pool, req, tmpResultChan, resultChan, userset, true, resolutionMetadata)

	return nil
}

// invoke loopOverWeightedEdges to get list objects candidate. Check
// will then be invoked on the excluded edge against these
// list objects candidates. If check returns false, then the list
// object candidates are true candidates and will be returned via
// resultChan. If check returns true, then these list object candidates
// are invalid because it does not satisfy all paths for exclusion.
func (c *ReverseExpandQuery) exclusionHandler(
	ctx context.Context,
	pool *concurrency.Pool,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	exclusionNode *weightedGraph.WeightedAuthorizationModelNode,
	sourceUserType string,
	resolutionMetadata *ResolutionMetadata,
) error {
	if exclusionNode == nil || exclusionNode.GetNodeType() != weightedGraph.OperatorNode || exclusionNode.GetLabel() != weightedGraph.ExclusionOperator {
		return fmt.Errorf("%w: operation: exclusion: %s", errors.ErrUnsupported, "invalid exclusion node")
	}

	// verify if the node has weight to the sourceUserType
	exclusionEdges, err := c.typesystem.GetEdgesFromNode(exclusionNode, sourceUserType)
	if err != nil {
		return err
	}
	edges, err := typesystem.GetEdgesForExclusion(exclusionEdges, sourceUserType)
	if err != nil {
		return fmt.Errorf("%w: operation: exclusion: %s", ErrLowestWeightFail, err.Error())
	}

	// This means the exclusion edge does not have a path to the terminal type.
	// e.g. `B` in `A but not B` is not relevant to this query.
	if edges.ExcludedEdges == nil {
		newReq := req.clone()

		return c.shallowClone().loopOverEdges(
			ctx,
			newReq,
			edges.BaseEdges,
			false,
			resolutionMetadata,
			resultChan,
			sourceUserType,
		)
	}

	tmpResultChan := make(chan *ReverseExpandResult, listObjectsResultChannelLength)

	userset, err := c.typesystem.ConstructUserset(edges.ExcludedEdges[0], sourceUserType)
	if err != nil {
		// This should never happen.
		return fmt.Errorf("%w: operation: exclusion: %s", ErrConstructUsersetFail, err.Error())
	}

	// Concurrently find candidates and call check on them as they are found
	c.findCandidatesForLowestWeightEdge(pool, req, tmpResultChan, edges.BaseEdges, sourceUserType, resolutionMetadata)
	c.callCheckForCandidates(pool, req, tmpResultChan, resultChan, userset, false, resolutionMetadata)

	return nil
}
