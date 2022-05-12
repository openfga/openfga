package queries

import (
	"context"
	"fmt"
	"sync"

	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/utils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	"go.buf.build/openfga/go/openfga/api/openfga"
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
		if rt, found = u.u[AllUsers]; !found {
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
	store                string
	authorizationModelId string
	users                *userSet
	targetUser           string
	tk                   *openfga.TupleKey
	contextualTuples     *contextualTuples
	tracer               resolutionTracer
	metadata             *utils.ResolutionMetadata
	internalCB           *circuitBreaker // Opens if the user is found, controlled internally. Primarily used for UNION.
	externalCB           *circuitBreaker // Open is controlled from caller, Used for Difference and Intersection.
}

func newResolutionContext(store, authorizationModelId string, tk *openfga.TupleKey, contextualTuples *contextualTuples, tracer resolutionTracer, metadata *utils.ResolutionMetadata, externalBreaker *circuitBreaker) *resolutionContext {
	return &resolutionContext{
		store:                store,
		authorizationModelId: authorizationModelId,
		users:                newUserSet(),
		targetUser:           tk.GetUser(),
		tk:                   tk,
		contextualTuples:     contextualTuples,
		tracer:               tracer,
		metadata:             metadata,
		internalCB:           &circuitBreaker{breakerState: false},
		externalCB:           externalBreaker,
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

func (rc *resolutionContext) fork(tk *openfga.TupleKey, tracer resolutionTracer, resetResolveCounter bool) *resolutionContext {
	metadata := rc.metadata
	if resetResolveCounter {
		metadata = rc.metadata.Fork()
	}

	return &resolutionContext{
		store:                rc.store,
		authorizationModelId: rc.authorizationModelId,
		users:                rc.users,
		targetUser:           rc.targetUser,
		tk:                   tk,
		contextualTuples:     rc.contextualTuples,
		tracer:               tracer,
		metadata:             metadata,
		internalCB:           rc.internalCB,
		externalCB:           rc.externalCB,
	}
}

func (rc *resolutionContext) readUserTuple(ctx context.Context, backend storage.TupleBackend) (*openfga.TupleKey, error) {
	tk, ok := rc.contextualTuples.readUserTuple(rc.tk)
	if ok {
		return tk, nil
	}

	rc.metadata.AddReadCall()
	tuple, err := backend.ReadUserTuple(ctx, rc.store, rc.tk)
	if err != nil {
		return nil, err
	}
	return tuple.GetKey(), nil
}

func (rc *resolutionContext) readUsersetTuples(ctx context.Context, backend storage.TupleBackend) (*tupleKeyIterator, error) {
	cUsersetTuples := rc.contextualTuples.readUsersetTuples(rc.tk)
	rc.metadata.AddReadCall()
	usersetTuples, err := backend.ReadUsersetTuples(ctx, rc.store, rc.tk)
	if err != nil {
		return nil, err
	}
	return newTupleKeyIterator(cUsersetTuples, usersetTuples), nil
}

func (rc *resolutionContext) read(ctx context.Context, backend storage.TupleBackend, tk *openfga.TupleKey) (*tupleKeyIterator, error) {
	cTuples := rc.contextualTuples.read(tk)
	rc.metadata.AddReadCall()
	tuples, err := backend.Read(ctx, rc.store, tk)
	if err != nil {
		return nil, err
	}
	return newTupleKeyIterator(cTuples, tuples), nil
}

type contextualTuples struct {
	usersets map[string][]*openfga.TupleKey
}

func (c *contextualTuples) read(tk *openfga.TupleKey) []*openfga.TupleKey {
	return c.usersets[tupleUtils.ToObjectRelationString(tk.GetObject(), tk.GetRelation())]
}

func (c *contextualTuples) readUserTuple(tk *openfga.TupleKey) (*openfga.TupleKey, bool) {
	tuples := c.usersets[tupleUtils.ToObjectRelationString(tk.GetObject(), tk.GetRelation())]
	for _, t := range tuples {
		if t.GetUser() == tk.GetUser() {
			return t, true
		}
	}
	return nil, false
}

func (c *contextualTuples) readUsersetTuples(tk *openfga.TupleKey) []*openfga.TupleKey {
	tuples := c.usersets[tupleUtils.ToObjectRelationString(tk.GetObject(), tk.GetRelation())]

	var res []*openfga.TupleKey
	for _, t := range tuples {
		if tupleUtils.GetUserTypeFromUser(t.GetUser()) == tupleUtils.UserSet {
			res = append(res, t)
		}
	}
	return res
}

type tupleKeyIterator struct {
	contextualTuples []*openfga.TupleKey
	tIterator        storage.TupleIterator
}

func newTupleKeyIterator(contextualTuples []*openfga.TupleKey, iter storage.TupleIterator) *tupleKeyIterator {
	return &tupleKeyIterator{
		contextualTuples: contextualTuples,
		tIterator:        iter,
	}
}

func (t *tupleKeyIterator) next() (*openfga.TupleKey, error) {
	if len(t.contextualTuples) > 0 {
		res, rest := t.contextualTuples[0], t.contextualTuples[1:]
		t.contextualTuples = rest
		return res, nil
	}

	r, err := t.tIterator.Next()
	if err != nil {
		return nil, err
	}

	return r.GetKey(), nil
}

func (t *tupleKeyIterator) stop() {
	// Close shouldn't error if called multiple times
	t.tIterator.Stop()
}

func validateAndPreprocessTuples(keyToCheck *openfga.TupleKey, tupleKeys []*openfga.TupleKey) (*contextualTuples, error) {
	if keyToCheck.GetUser() == "" || keyToCheck.GetRelation() == "" || keyToCheck.GetObject() == "" {
		return nil, serverErrors.InvalidCheckInput
	}
	if !tupleUtils.IsValidUser(keyToCheck.GetUser()) {
		return nil, serverErrors.InvalidUser(keyToCheck.GetUser())
	}

	tupleMap := map[string]struct{}{}
	usersets := map[string][]*openfga.TupleKey{}
	for _, tk := range tupleKeys {
		if _, ok := tupleMap[tk.String()]; ok {
			return nil, serverErrors.DuplicateContextualTuple(tk)
		}
		tupleMap[tk.String()] = struct{}{}

		if tk.GetUser() == "" || tk.GetRelation() == "" || tk.GetObject() == "" {
			return nil, serverErrors.InvalidContextualTuple(tk)
		}
		if !tupleUtils.IsValidUser(tk.GetUser()) {
			return nil, serverErrors.InvalidUser(tk.GetUser())
		}

		key := tupleUtils.ToObjectRelationString(tk.GetObject(), tk.GetRelation())
		usersets[key] = append(usersets[key], tk)
	}

	return &contextualTuples{usersets: usersets}, nil
}
