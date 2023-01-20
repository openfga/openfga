package commands

import (
	"context"
	"fmt"
	"sync"

	"github.com/openfga/openfga/internal/contextualtuples"
	"github.com/openfga/openfga/internal/utils"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

// Keeping the interface simple for the time being
// we could make it Append* where * are tupleset, computedset, etc.
// especially if we want to generate other representations for the trace (e.g. a tree)
type resolutionTracer interface {
	AppendComputed() resolutionTracer
	AppendDirect() resolutionTracer
	AppendIndex(i int) resolutionTracer
	AppendIntersection(t intersectionTracer) resolutionTracer
	AppendString(s string) resolutionTracer
	AppendTupleToUserset() resolutionTracer
	AppendUnion() resolutionTracer
	CreateIntersectionTracer() intersectionTracer
	GetResolution() string
}

type intersectionTracer interface {
	AppendTrace(rt resolutionTracer)
	GetResolution() string
}

// noopResolutionTracer is thread safe as current implementation is immutable
type noopResolutionTracer struct{}

func (t *noopResolutionTracer) AppendComputed() resolutionTracer {
	return t
}

func (t *noopResolutionTracer) AppendDirect() resolutionTracer {
	return t
}

func (t *noopResolutionTracer) AppendIndex(_ int) resolutionTracer {
	return t
}

func (t *noopResolutionTracer) AppendIntersection(_ intersectionTracer) resolutionTracer {
	return t
}

func (t *noopResolutionTracer) AppendString(_ string) resolutionTracer {
	return t
}

func (t *noopResolutionTracer) AppendTupleToUserset() resolutionTracer {
	return t
}

func (t *noopResolutionTracer) AppendUnion() resolutionTracer {
	return t
}

var nit = &noopIntersectionTracer{}

func (t *noopResolutionTracer) CreateIntersectionTracer() intersectionTracer {
	return nit
}

func (t *noopResolutionTracer) GetResolution() string {
	return ""
}

type noopIntersectionTracer struct{}

func (t *noopIntersectionTracer) AppendTrace(_ resolutionTracer) {}

func (t *noopIntersectionTracer) GetResolution() string {
	return ""
}

// stringResolutionTracer is thread safe as current implementation is immutable
type stringResolutionTracer struct {
	trace string
}

func newStringResolutionTracer() resolutionTracer {
	return &stringResolutionTracer{
		trace: ".",
	}
}

func (t *stringResolutionTracer) AppendComputed() resolutionTracer {
	return t.AppendString("(computed-userset)")
}

func (t *stringResolutionTracer) AppendDirect() resolutionTracer {
	return t.AppendString("(direct)")
}

// AppendIndex We create separate append functions so no casting happens externally
// This aim to minimize overhead added by the no-op implementation
func (t *stringResolutionTracer) AppendIndex(n int) resolutionTracer {
	return &stringResolutionTracer{
		trace: fmt.Sprintf("%s%d", t.trace, n),
	}
}

func (t *stringResolutionTracer) AppendIntersection(it intersectionTracer) resolutionTracer {
	return &stringResolutionTracer{
		trace: fmt.Sprintf("%s[%s]", t.trace, it.GetResolution()),
	}
}

func (t *stringResolutionTracer) AppendString(subTrace string) resolutionTracer {
	return &stringResolutionTracer{
		trace: fmt.Sprintf("%s%s.", t.trace, subTrace),
	}
}

func (t *stringResolutionTracer) AppendTupleToUserset() resolutionTracer {
	return t.AppendString("(tuple-to-userset)")
}

func (t *stringResolutionTracer) AppendUnion() resolutionTracer {
	return t.AppendString("union")
}

func (t *stringResolutionTracer) CreateIntersectionTracer() intersectionTracer {
	return &stringIntersectionTracer{}
}

func (t *stringResolutionTracer) GetResolution() string {
	return t.trace
}

// stringIntersectionTracer is NOT thread safe. do not call from multiple threads
type stringIntersectionTracer struct {
	trace string
}

func (t *stringIntersectionTracer) AppendTrace(rt resolutionTracer) {
	if len(t.trace) != 0 {
		t.trace = fmt.Sprintf("%s,%s", t.trace, rt.GetResolution())
		return
	}

	t.trace = rt.GetResolution()
}

func (t *stringIntersectionTracer) GetResolution() string {
	return t.trace
}

type userSet struct {
	m sync.Mutex
	u map[string]resolutionTracer
}

type userWithTracer struct {
	u string
	r resolutionTracer
}

func (u *userSet) Add(r resolutionTracer, values ...string) {
	u.m.Lock()
	for _, v := range values {
		u.u[v] = r
	}
	u.m.Unlock()
}

func (u *userSet) AddFrom(other *userSet) {
	u.m.Lock()
	for _, uwr := range other.AsSlice() {
		u.u[uwr.u] = uwr.r
	}
	u.m.Unlock()
}

func (u *userSet) DeleteFrom(other *userSet) {
	u.m.Lock()
	for _, uwr := range other.AsSlice() {
		delete(u.u, uwr.u)
	}
	u.m.Unlock()
}

func (u *userSet) Get(value string) (resolutionTracer, bool) {
	u.m.Lock()
	defer u.m.Unlock()

	var found bool
	var rt resolutionTracer
	if rt, found = u.u[value]; !found {
		if rt, found = u.u[tupleUtils.Wildcard]; !found {
			return nil, false
		}
	}
	return rt, found
}

func (u *userSet) AsSlice() []userWithTracer {
	u.m.Lock()
	out := make([]userWithTracer, 0, len(u.u))
	for u, rt := range u.u {
		out = append(out, userWithTracer{
			u: u,
			r: rt,
		})
	}
	u.m.Unlock()
	return out
}

func newUserSet() *userSet {
	return &userSet{u: make(map[string]resolutionTracer)}
}

type userSets struct {
	mu  sync.Mutex
	usm map[int]*userSet
}

func newUserSets() *userSets {
	return &userSets{usm: make(map[int]*userSet, 0)}
}

func (u *userSets) Set(idx int, us *userSet) {
	u.mu.Lock()
	u.usm[idx] = us
	u.mu.Unlock()
}

func (u *userSets) Get(idx int) (*userSet, bool) {
	u.mu.Lock()
	us, ok := u.usm[idx]
	u.mu.Unlock()
	return us, ok
}

func (u *userSets) AsMap() map[int]*userSet {
	return u.usm
}

type chanResolveResult struct {
	err   error
	found bool
}

type circuitBreaker struct {
	mu           sync.Mutex
	breakerState bool
}

func (sc *circuitBreaker) Open() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.breakerState = true
}

func (sc *circuitBreaker) IsOpen() bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.breakerState
}

type resolutionContext struct {
	store            string
	model            *openfgapb.AuthorizationModel
	users            *userSet
	targetUser       string
	tk               *openfgapb.TupleKey
	contextualTuples *contextualtuples.ContextualTuples
	tracer           resolutionTracer
	metadata         *utils.ResolutionMetadata
	internalCB       *circuitBreaker // Opens if the user is found, controlled internally. Primarily used for UNION.
	externalCB       *circuitBreaker // Open is controlled from caller, Used for Difference and Intersection.
}

func newResolutionContext(store string, model *openfgapb.AuthorizationModel, tk *openfgapb.TupleKey, contextualTuples *contextualtuples.ContextualTuples, tracer resolutionTracer, metadata *utils.ResolutionMetadata, externalBreaker *circuitBreaker) *resolutionContext {
	return &resolutionContext{
		store:            store,
		model:            model,
		users:            newUserSet(),
		targetUser:       tk.GetUser(),
		tk:               tk,
		contextualTuples: contextualTuples,
		tracer:           tracer,
		metadata:         metadata,
		internalCB:       &circuitBreaker{breakerState: false},
		externalCB:       externalBreaker,
	}
}

func (rc *resolutionContext) shouldShortCircuit() bool {
	if rc.internalCB.IsOpen() || rc.externalCB.IsOpen() {
		return true
	}
	return rc.userFound()
}

func (rc *resolutionContext) shortCircuit() {
	rc.internalCB.Open()
}

func (rc *resolutionContext) userFound() bool {
	_, ok := rc.users.Get(rc.targetUser)
	if ok {
		rc.shortCircuit()
	}
	return ok
}

func (rc *resolutionContext) fork(tk *openfgapb.TupleKey, tracer resolutionTracer, resetResolveCounter bool) *resolutionContext {
	metadata := rc.metadata
	if resetResolveCounter {
		metadata = rc.metadata.Fork()
	}

	return &resolutionContext{
		store:            rc.store,
		model:            rc.model,
		users:            rc.users,
		targetUser:       rc.targetUser,
		tk:               tk,
		contextualTuples: rc.contextualTuples,
		tracer:           tracer,
		metadata:         metadata,
		internalCB:       rc.internalCB,
		externalCB:       rc.externalCB,
	}
}

func (rc *resolutionContext) readUserTuple(ctx context.Context, backend storage.TupleBackend) (*openfgapb.TupleKey, error) {

	typesys := typesystem.New(rc.model)

	tk, ok := rc.contextualTuples.ReadUserTuple(rc.tk)

	if tk != nil {
		err := validation.ValidateTuple(typesys, tk)
		if err == nil && ok {
			return tk, nil
		}
	}

	tuple, err := backend.ReadUserTuple(ctx, rc.store, rc.tk)
	if err != nil {
		return nil, err
	}

	tk = tuple.GetKey()
	if err := validation.ValidateTuple(typesys, tk); err != nil {
		return nil, nil
	}

	return tk, nil
}

func (rc *resolutionContext) readUsersetTuples(ctx context.Context, backend storage.TupleBackend) (storage.TupleKeyIterator, error) {
	cUsersetTuples := rc.contextualTuples.ReadUsersetTuples(rc.tk)
	usersetTuples, err := backend.ReadUsersetTuples(ctx, rc.store, rc.tk)
	if err != nil {
		return nil, err
	}

	iter1 := storage.NewStaticTupleKeyIterator(cUsersetTuples)
	iter2 := storage.NewTupleKeyIteratorFromTupleIterator(usersetTuples)

	return storage.NewFilteredTupleKeyIterator(
		storage.NewCombinedIterator(iter1, iter2),
		validation.FilterInvalidTuples(rc.model),
	), nil
}

func (rc *resolutionContext) read(ctx context.Context, backend storage.TupleBackend, tk *openfgapb.TupleKey) (storage.TupleKeyIterator, error) {
	cTuples := rc.contextualTuples.Read(tk)
	tuples, err := backend.Read(ctx, rc.store, tk)
	if err != nil {
		return nil, err
	}

	iter1 := storage.NewStaticTupleKeyIterator(cTuples)
	iter2 := storage.NewTupleKeyIteratorFromTupleIterator(tuples)

	return storage.NewFilteredTupleKeyIterator(
		storage.NewCombinedIterator(iter1, iter2),
		validation.FilterInvalidTuples(rc.model),
	), nil
}
