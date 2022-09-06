package memory

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-errors/errors"
	openfgaerrors "github.com/openfga/openfga/pkg/errors"
	"github.com/openfga/openfga/pkg/telemetry"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var errObjectOrUserMustBeSpecified = errors.New("either object or user must be specified")

type staticIterator struct {
	tuples            []*openfgapb.Tuple
	continuationToken []byte
}

func match(key *openfgapb.TupleKey, target *openfgapb.TupleKey) bool {
	if key.Object != "" {
		td, objectid := tupleUtils.SplitObject(key.Object)
		if objectid == "" {
			if td != tupleUtils.GetType(target.Object) {
				return false
			}
		} else {
			if key.Object != target.Object {
				return false
			}
		}
	}
	if key.Relation != "" && key.Relation != target.Relation {
		return false
	}
	if key.User != "" && key.User != target.User {
		return false
	}
	return true
}

func (s *staticIterator) Next() (*openfgapb.Tuple, error) {
	if len(s.tuples) == 0 {
		return nil, storage.ErrIteratorDone
	}
	next, rest := s.tuples[0], s.tuples[1:]
	s.tuples = rest
	return next, nil
}

func (s *staticIterator) Stop() {}

// A MemoryBackend provides an ephemeral memory-backed implementation of TupleBackend and AuthorizationModelBackend.
// MemoryBackend instances may be safely shared by multiple go-routines.
type MemoryBackend struct {
	tracer                    trace.Tracer
	maxTuplesInWriteOperation int
	maxTypesInTypeDefinition  int
	mu                        sync.Mutex

	// TupleBackend
	// map: store => set of tuples
	tuples map[string][]*openfgapb.Tuple /* GUARDED_BY(mu) */

	// ChangelogBackend
	// map: store => set of changes
	changes map[string][]*openfgapb.TupleChange

	// AuthorizationModelBackend
	// map: store = > map: type definition id => type definition
	authorizationModels map[string]map[string]*AuthorizationModelEntry /* GUARDED_BY(mu_) */

	// map: store id => store data
	stores map[string]*openfgapb.Store

	// map: store id | authz model id => assertions
	assertions map[string][]*openfgapb.Assertion
}

type AuthorizationModelEntry struct {
	model  *openfgapb.AuthorizationModel
	latest bool
}

var _ storage.OpenFGADatastore = (*MemoryBackend)(nil)

// New creates a new empty MemoryBackend.
func New(tracer trace.Tracer, maxTuplesInOneWrite int, maxTypesInAuthorizationModel int) *MemoryBackend {
	return &MemoryBackend{
		tracer:                    tracer,
		maxTuplesInWriteOperation: maxTuplesInOneWrite,
		maxTypesInTypeDefinition:  maxTypesInAuthorizationModel,
		tuples:                    make(map[string][]*openfgapb.Tuple, 0),
		changes:                   make(map[string][]*openfgapb.TupleChange, 0),
		authorizationModels:       make(map[string]map[string]*AuthorizationModelEntry),
		stores:                    make(map[string]*openfgapb.Store, 0),
		assertions:                make(map[string][]*openfgapb.Assertion, 0),
	}
}

// Close closes any open connections and cleans up residual resources
// used by this storage adapter instance.
func (s *MemoryBackend) Close(ctx context.Context) error {
	return nil
}

func (s *MemoryBackend) ListObjectsByType(ctx context.Context, store string, objectType string) (storage.ObjectIterator, error) {
	_, span := s.tracer.Start(ctx, "memory.ListObjectsByType")
	defer span.End()

	uniqueObjects := make(map[string]bool, 0)
	matches := make([]*openfgapb.Object, 0)
	for _, t := range s.tuples[store] {
		if objectType == "" || !strings.HasPrefix(t.Key.Object, objectType+":") {
			continue
		}
		_, found := uniqueObjects[t.Key.Object]
		if !found {
			uniqueObjects[t.Key.Object] = true
			objectType, objectID := tupleUtils.SplitObject(t.Key.Object)
			matches = append(matches, &openfgapb.Object{
				Type: objectType,
				Id:   objectID,
			})
		}
	}

	return storage.NewStaticObjectIterator(matches), nil
}

// Read See storage.TupleBackend.Read
func (s *MemoryBackend) Read(ctx context.Context, store string, key *openfgapb.TupleKey) (storage.TupleIterator, error) {
	ctx, span := s.tracer.Start(ctx, "memory.Read")
	defer span.End()

	return s.read(ctx, store, key, storage.PaginationOptions{})
}

func (s *MemoryBackend) ReadPage(ctx context.Context, store string, key *openfgapb.TupleKey, paginationOptions storage.PaginationOptions) ([]*openfgapb.Tuple, []byte, error) {
	ctx, span := s.tracer.Start(ctx, "memory.ReadPage")
	defer span.End()

	it, err := s.read(ctx, store, key, paginationOptions)
	if err != nil {
		return nil, nil, openfgaerrors.ErrorWithStack(err)
	}

	return it.tuples, it.continuationToken, nil
}

func (s *MemoryBackend) ReadChanges(ctx context.Context, store, objectType string, paginationOptions storage.PaginationOptions, horizonOffset time.Duration) ([]*openfgapb.TupleChange, []byte, error) {
	_, span := s.tracer.Start(ctx, "memory.ReadChanges")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	var err error
	var from int64
	var typeInToken string
	var continuationToken string
	if paginationOptions.From != "" {
		tokens := strings.Split(paginationOptions.From, "|")
		if len(tokens) == 2 {
			concreteToken := tokens[0]
			typeInToken = tokens[1]
			from, err = strconv.ParseInt(concreteToken, 10, 32)
			if err != nil {
				return nil, nil, openfgaerrors.ErrorWithStack(err)
			}
		}
	}

	if typeInToken != "" && typeInToken != objectType {
		return nil, nil, openfgaerrors.ErrorWithStack(storage.ErrMismatchObjectType)
	}

	var allChanges []*openfgapb.TupleChange
	now := time.Now().UTC()
	for _, change := range s.changes[store] {
		if objectType == "" || (objectType != "" && strings.HasPrefix(change.TupleKey.Object, objectType+":")) {
			if change.Timestamp.AsTime().After(now.Add(-horizonOffset)) {
				break
			}
			allChanges = append(allChanges, change)
		}
	}
	if len(allChanges) == 0 {
		return nil, nil, openfgaerrors.ErrorWithStack(storage.ErrNotFound)
	}

	pageSize := storage.DefaultPageSize
	if paginationOptions.PageSize > 0 {
		pageSize = paginationOptions.PageSize
	}
	to := int(from) + pageSize
	if len(allChanges) < to {
		to = len(allChanges)
	}
	res := allChanges[from:to]
	if len(res) == 0 {
		return nil, nil, openfgaerrors.ErrorWithStack(storage.ErrNotFound)
	}

	continuationToken = strconv.Itoa(len(allChanges))
	if to != len(allChanges) {
		continuationToken = strconv.Itoa(to)
	}
	continuationToken = continuationToken + fmt.Sprintf("|%s", objectType)

	return res, []byte(continuationToken), nil
}

func (s *MemoryBackend) read(ctx context.Context, store string, key *openfgapb.TupleKey, paginationOptions storage.PaginationOptions) (*staticIterator, error) {
	_, span := s.tracer.Start(ctx, "memory.read")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	if key.Object == "" && key.User == "" {
		err := errObjectOrUserMustBeSpecified
		telemetry.TraceError(span, err)
		return nil, openfgaerrors.ErrorWithStack(err)
	}
	var matches []*openfgapb.Tuple
	for _, t := range s.tuples[store] {
		if match(key, t.Key) {
			matches = append(matches, t)
		}
	}

	var err error
	var from int
	if paginationOptions.From != "" {
		from, err = strconv.Atoi(paginationOptions.From)
		if err != nil {
			telemetry.TraceError(span, err)
			return nil, openfgaerrors.ErrorWithStack(err)
		}
	}

	if from <= len(matches) {
		matches = matches[from:]
	}

	to := paginationOptions.PageSize
	if to != 0 && to < len(matches) {
		return &staticIterator{tuples: matches[:to], continuationToken: []byte(strconv.Itoa(from + to))}, nil
	}

	return &staticIterator{tuples: matches}, nil
}

// Write See storage.TupleBackend.Write
func (s *MemoryBackend) Write(ctx context.Context, store string, deletes storage.Deletes, writes storage.Writes) error {
	_, span := s.tracer.Start(ctx, "memory.Write")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	now := timestamppb.Now()

	if err := validateTuples(s.tuples[store], deletes, writes); err != nil {
		return openfgaerrors.ErrorWithStack(err)
	}

	var tuples []*openfgapb.Tuple
Delete:
	for _, t := range s.tuples[store] {
		for _, k := range deletes {
			if match(k, t.Key) {
				s.changes[store] = append(s.changes[store], &openfgapb.TupleChange{TupleKey: t.Key, Operation: openfgapb.TupleOperation_TUPLE_OPERATION_DELETE, Timestamp: now})
				continue Delete
			}
		}
		tuples = append(tuples, t)
	}

Write:
	for _, t := range writes {
		for _, et := range tuples {
			if match(t, et.Key) {
				continue Write
			}
		}
		tuples = append(tuples, &openfgapb.Tuple{Key: t, Timestamp: now})
		s.changes[store] = append(s.changes[store], &openfgapb.TupleChange{TupleKey: t, Operation: openfgapb.TupleOperation_TUPLE_OPERATION_WRITE, Timestamp: now})
	}
	s.tuples[store] = tuples
	return nil
}

func validateTuples(tuples []*openfgapb.Tuple, deletes, writes []*openfgapb.TupleKey) error {
	for _, tk := range deletes {
		if !find(tuples, tk) {
			return openfgaerrors.ErrorWithStack(storage.InvalidWriteInputError(tk, openfgapb.TupleOperation_TUPLE_OPERATION_DELETE))
		}
	}
	for _, tk := range writes {
		if find(tuples, tk) {
			return openfgaerrors.ErrorWithStack(storage.InvalidWriteInputError(tk, openfgapb.TupleOperation_TUPLE_OPERATION_WRITE))
		}
	}
	return nil
}

func find(tuples []*openfgapb.Tuple, tupleKey *openfgapb.TupleKey) bool {
	for _, tuple := range tuples {
		if match(tuple.Key, tupleKey) {
			return true
		}
	}
	return false
}

// ReadUserTuple See storage.TupleBackend.ReadUserTuple
func (s *MemoryBackend) ReadUserTuple(ctx context.Context, store string, key *openfgapb.TupleKey) (*openfgapb.Tuple, error) {
	_, span := s.tracer.Start(ctx, "memory.ReadUserTuple")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	if key.Object == "" && key.User == "" {
		err := errObjectOrUserMustBeSpecified
		telemetry.TraceError(span, err)
		return nil, openfgaerrors.ErrorWithStack(err)
	}
	for _, t := range s.tuples[store] {
		if match(key, t.Key) {
			return t, nil
		}
	}
	telemetry.TraceError(span, storage.ErrNotFound)
	return nil, openfgaerrors.ErrorWithStack(storage.ErrNotFound)
}

// ReadUsersetTuples See storage.TupleBackend.ReadUsersetTuples
func (s *MemoryBackend) ReadUsersetTuples(ctx context.Context, store string, key *openfgapb.TupleKey) (storage.TupleIterator, error) {
	_, span := s.tracer.Start(ctx, "memory.ReadUsersetTuples")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	if key.Object == "" && key.User == "" {
		err := errObjectOrUserMustBeSpecified
		telemetry.TraceError(span, err)
		return nil, openfgaerrors.ErrorWithStack(err)
	}
	var matches []*openfgapb.Tuple
	for _, t := range s.tuples[store] {
		if match(&openfgapb.TupleKey{
			Object:   key.GetObject(),
			Relation: key.GetRelation(),
		}, t.Key) && tupleUtils.GetUserTypeFromUser(t.GetKey().GetUser()) == tupleUtils.UserSet {
			matches = append(matches, t)
		}
	}
	return &staticIterator{tuples: matches}, nil
}

// ReadByStore See storage.TupleBackend.ReadByStore
func (s *MemoryBackend) ReadByStore(ctx context.Context, store string, options storage.PaginationOptions) ([]*openfgapb.Tuple, []byte, error) {
	_, span := s.tracer.Start(ctx, "memory.ReadByStore")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	matches := make([]*openfgapb.Tuple, len(s.tuples[store]))
	copy(matches, s.tuples[store])

	var from int64 = 0
	var err error

	pageSize := storage.DefaultPageSize
	if options.PageSize > 0 {
		pageSize = options.PageSize
	}

	if options.From != "" {
		from, err = strconv.ParseInt(options.From, 10, 32)
		if err != nil {
			return nil, make([]byte, 0), openfgaerrors.ErrorWithStack(err)
		}
	}
	to := int(from) + pageSize
	if len(matches) < to {
		to = len(matches)
	}

	partition := matches[from:to]
	continuationToken := ""
	if to != len(matches) {
		continuationToken = strconv.Itoa(to)
	}

	return partition, []byte(continuationToken), nil
}

func findAuthorizationModelByID(id string, configurations map[string]*AuthorizationModelEntry) (*openfgapb.AuthorizationModel, bool) {
	var nsc *openfgapb.AuthorizationModel

	if id == "" {
		// find latest
		for _, entry := range configurations {
			if entry.latest {
				nsc = entry.model
				break
			}
		}

		if nsc == nil {
			return nil, false
		}
	} else {
		if entry, ok := configurations[id]; !ok {
			return nil, false
		} else {
			nsc = entry.model
		}
	}
	return nsc, true
}

// definitionByType returns the definition of the objectType if it exists in the authorization model
func definitionByType(authorizationModel *openfgapb.AuthorizationModel, objectType string) (*openfgapb.TypeDefinition, bool) {
	for _, td := range authorizationModel.GetTypeDefinitions() {
		if td.GetType() == objectType {
			return td, true
		}
	}

	return nil, false
}

// ReadAuthorizationModel See storage.AuthorizationModelBackend.ReadAuthorizationModel
func (s *MemoryBackend) ReadAuthorizationModel(ctx context.Context, store string, id string) (*openfgapb.AuthorizationModel, error) {
	_, span := s.tracer.Start(ctx, "memory.ReadAuthorizationModel")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	tm, ok := s.authorizationModels[store]
	if !ok {
		telemetry.TraceError(span, storage.ErrNotFound)
		return nil, openfgaerrors.ErrorWithStack(storage.ErrNotFound)
	}

	if nsc, ok := findAuthorizationModelByID(id, tm); ok {
		if nsc.GetTypeDefinitions() == nil || len(nsc.GetTypeDefinitions()) == 0 {
			return nil, openfgaerrors.ErrorWithStack(storage.ErrNotFound)
		}
		return nsc, nil
	}

	telemetry.TraceError(span, storage.ErrNotFound)
	return nil, openfgaerrors.ErrorWithStack(storage.ErrNotFound)
}

// ReadAuthorizationModels See storage.AuthorizationModelBackend.ReadAuthorizationModels
// options.From is expected to be a number
func (s *MemoryBackend) ReadAuthorizationModels(ctx context.Context, store string, options storage.PaginationOptions) ([]*openfgapb.AuthorizationModel, []byte, error) {
	_, span := s.tracer.Start(ctx, "memory.ReadAuthorizationModels")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	models := make([]*openfgapb.AuthorizationModel, 0, len(s.authorizationModels[store]))
	for _, entry := range s.authorizationModels[store] {
		models = append(models, entry.model)
	}

	// from newest to oldest
	sort.Slice(models, func(i, j int) bool {
		return models[i].Id > models[j].Id
	})

	var from int64 = 0
	continuationToken := ""
	var err error

	pageSize := storage.DefaultPageSize
	if options.PageSize > 0 {
		pageSize = options.PageSize
	}

	if options.From != "" {
		from, err = strconv.ParseInt(options.From, 10, 32)
		if err != nil {
			return nil, nil, openfgaerrors.ErrorWithStack(err)
		}
	}

	to := int(from) + pageSize
	if len(models) < to {
		to = len(models)
	}
	res := models[from:to]

	if to != len(models) {
		continuationToken = strconv.Itoa(to)
	}

	return res, []byte(continuationToken), nil
}

// FindLatestAuthorizationModelID See storage.AuthorizationModelBackend.FindLatestAuthorizationModelID
func (s *MemoryBackend) FindLatestAuthorizationModelID(ctx context.Context, store string) (string, error) {
	_, span := s.tracer.Start(ctx, "memory.FindLatestAuthorizationModelID")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	tm, ok := s.authorizationModels[store]
	if !ok {
		telemetry.TraceError(span, storage.ErrNotFound)
		return "", openfgaerrors.ErrorWithStack(storage.ErrNotFound)
	}
	// find latest model
	nsc, ok := findAuthorizationModelByID("", tm)
	if !ok {
		telemetry.TraceError(span, storage.ErrNotFound)
		return "", openfgaerrors.ErrorWithStack(storage.ErrNotFound)
	}
	return nsc.Id, nil
}

// ReadTypeDefinition See storage.TypeDefinitionReadBackend.ReadTypeDefinition
func (s *MemoryBackend) ReadTypeDefinition(ctx context.Context, store, id, objectType string) (*openfgapb.TypeDefinition, error) {
	_, span := s.tracer.Start(ctx, "memory.ReadTypeDefinition")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	tm, ok := s.authorizationModels[store]
	if !ok {
		telemetry.TraceError(span, storage.ErrNotFound)
		return nil, openfgaerrors.ErrorWithStack(storage.ErrNotFound)
	}

	if nsc, ok := findAuthorizationModelByID(id, tm); ok {
		if ns, ok := definitionByType(nsc, objectType); ok {
			return ns, nil
		}
	}

	telemetry.TraceError(span, storage.ErrNotFound)
	return nil, openfgaerrors.ErrorWithStack(storage.ErrNotFound)
}

// WriteAuthorizationModel See storage.TypeDefinitionWriteBackend.WriteAuthorizationModel
func (s *MemoryBackend) WriteAuthorizationModel(ctx context.Context, store, id string, tds []*openfgapb.TypeDefinition) error {
	_, span := s.tracer.Start(ctx, "memory.WriteAuthorizationModel")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.authorizationModels[store]; !ok {
		s.authorizationModels[store] = make(map[string]*AuthorizationModelEntry)
	}

	for _, entry := range s.authorizationModels[store] {
		entry.latest = false
	}

	s.authorizationModels[store][id] = &AuthorizationModelEntry{
		model: &openfgapb.AuthorizationModel{
			Id:              id,
			TypeDefinitions: tds,
		},
		latest: true,
	}

	return nil
}

func (s *MemoryBackend) CreateStore(ctx context.Context, newStore *openfgapb.Store) (*openfgapb.Store, error) {
	_, span := s.tracer.Start(ctx, "memory.CreateStore")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.stores[newStore.Id]; ok {
		return nil, storage.ErrCollision
	}

	now := timestamppb.New(time.Now().UTC())
	s.stores[newStore.Id] = &openfgapb.Store{
		Id:        newStore.Id,
		Name:      newStore.Name,
		CreatedAt: now,
		UpdatedAt: now,
	}

	return s.stores[newStore.Id], nil
}

func (s *MemoryBackend) DeleteStore(ctx context.Context, id string) error {
	_, span := s.tracer.Start(ctx, "memory.DeleteStore")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.stores, id)
	return nil
}

func (s *MemoryBackend) WriteAssertions(ctx context.Context, store, modelID string, assertions []*openfgapb.Assertion) error {
	_, span := s.tracer.Start(ctx, "memory.WriteAssertions")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	assertionsID := fmt.Sprintf("%s|%s", store, modelID)
	s.assertions[assertionsID] = assertions

	return nil
}

func (s *MemoryBackend) ReadAssertions(ctx context.Context, store, modelID string) ([]*openfgapb.Assertion, error) {
	_, span := s.tracer.Start(ctx, "memory.ReadAssertions")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	assertionsID := fmt.Sprintf("%s|%s", store, modelID)
	assertions, ok := s.assertions[assertionsID]
	if !ok {
		return []*openfgapb.Assertion{}, nil
	}
	return assertions, nil
}

// MaxTuplesInWriteOperation returns the maximum number of tuples allowed in one write operation
func (s *MemoryBackend) MaxTuplesInWriteOperation() int {
	return s.maxTuplesInWriteOperation
}

// MaxTypesInTypeDefinition returns the maximum number of types allowed in a type definition
func (s *MemoryBackend) MaxTypesInTypeDefinition() int {
	return s.maxTypesInTypeDefinition
}

func (s *MemoryBackend) GetStore(ctx context.Context, storeID string) (*openfgapb.Store, error) {
	_, span := s.tracer.Start(ctx, "memory.GetStore")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stores[storeID] == nil {
		return nil, openfgaerrors.ErrorWithStack(storage.ErrNotFound)
	}

	return s.stores[storeID], nil
}

func (s *MemoryBackend) ListStores(ctx context.Context, paginationOptions storage.PaginationOptions) ([]*openfgapb.Store, []byte, error) {
	_, span := s.tracer.Start(ctx, "memory.ListStores")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	stores := make([]*openfgapb.Store, 0, len(s.stores))
	for _, t := range s.stores {
		stores = append(stores, t)
	}

	// from oldest to newest
	sort.SliceStable(stores, func(i, j int) bool {
		return stores[i].Id < stores[j].Id
	})

	var err error
	var from int64 = 0
	if paginationOptions.From != "" {
		from, err = strconv.ParseInt(paginationOptions.From, 10, 32)
		if err != nil {
			return nil, nil, openfgaerrors.ErrorWithStack(err)
		}
	}
	pageSize := storage.DefaultPageSize
	if paginationOptions.PageSize > 0 {
		pageSize = paginationOptions.PageSize
	}
	to := int(from) + pageSize
	if len(stores) < to {
		to = len(stores)
	}
	res := stores[from:to]
	if len(res) == 0 {
		return nil, nil, nil
	}

	continuationToken := ""
	if to != len(stores) {
		continuationToken = strconv.Itoa(to)
	}

	return res, []byte(continuationToken), nil
}

func (s *MemoryBackend) IsReady(ctx context.Context) (bool, error) {
	return true, nil
}
