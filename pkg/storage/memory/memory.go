// Package memory contains an implementation of the storage interface that lives in memory.
package memory

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/telemetry"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"go.opentelemetry.io/otel"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var tracer = otel.Tracer("openfga/pkg/storage/memory")

type staticIterator struct {
	records           []*storage.TupleRecord
	continuationToken []byte
	mu                sync.Mutex
}

// match returns true if all the fields in *TupleRecord are equal to the same field in the target *TupleKey.
// If the input Object doesn't specify an ID, only the Object Types are compared.
// If a field in the input parameter is empty, it is ignored in the comparison.
func match(t *storage.TupleRecord, target *openfgav1.TupleKey) bool {
	if target.Object != "" {
		td, objectid := tupleUtils.SplitObject(target.Object)
		if objectid == "" {
			if td != t.ObjectType {
				return false
			}
		} else {
			if td != t.ObjectType || objectid != t.ObjectID {
				return false
			}
		}
	}
	if target.Relation != "" && t.Relation != target.Relation {
		return false
	}
	if target.User != "" && t.User != target.User {
		return false
	}
	return true
}

func (s *staticIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.records) == 0 {
		return nil, storage.ErrIteratorDone
	}

	next, rest := s.records[0], s.records[1:]
	s.records = rest
	return next.AsTuple(), nil
}

func (s *staticIterator) Stop() {}

func (s *staticIterator) ToArray(ctx context.Context) ([]*openfgav1.Tuple, []byte, error) {
	var res []*openfgav1.Tuple
	for range s.records {
		t, err := s.Next(ctx)
		if err != nil {
			return nil, nil, err
		}

		res = append(res, t)
	}

	return res, s.continuationToken, nil
}

type StorageOption func(ds *MemoryBackend)

const (
	defaultMaxTuplesPerWrite             = 100
	defaultMaxTypesPerAuthorizationModel = 100
)

// A MemoryBackend provides an ephemeral memory-backed implementation of storage.OpenFGADatastore.
// MemoryBackend instances may be safely shared by multiple go-routines.
type MemoryBackend struct {
	maxTuplesPerWrite             int
	maxTypesPerAuthorizationModel int
	mu                            sync.Mutex

	// TupleBackend
	// map: store => set of tuples
	tuples map[string][]*storage.TupleRecord /* GUARDED_BY(mu) */

	// ChangelogBackend
	// map: store => set of changes
	changes map[string][]*openfgav1.TupleChange

	// AuthorizationModelBackend
	// map: store = > map: type definition id => type definition
	authorizationModels map[string]map[string]*AuthorizationModelEntry /* GUARDED_BY(mu_) */

	// map: store id => store data
	stores map[string]*openfgav1.Store

	// map: store id | authz model id => assertions
	assertions map[string][]*openfgav1.Assertion
}

var _ storage.OpenFGADatastore = (*MemoryBackend)(nil)

type AuthorizationModelEntry struct {
	model  *openfgav1.AuthorizationModel
	latest bool
}

// New creates a new empty MemoryBackend.
func New(opts ...StorageOption) storage.OpenFGADatastore {
	ds := &MemoryBackend{
		maxTuplesPerWrite:             defaultMaxTuplesPerWrite,
		maxTypesPerAuthorizationModel: defaultMaxTypesPerAuthorizationModel,
		tuples:                        make(map[string][]*storage.TupleRecord, 0),
		changes:                       make(map[string][]*openfgav1.TupleChange, 0),
		authorizationModels:           make(map[string]map[string]*AuthorizationModelEntry),
		stores:                        make(map[string]*openfgav1.Store, 0),
		assertions:                    make(map[string][]*openfgav1.Assertion, 0),
	}

	for _, opt := range opts {
		opt(ds)
	}

	return ds
}

func WithMaxTuplesPerWrite(n int) StorageOption {
	return func(ds *MemoryBackend) { ds.maxTuplesPerWrite = n }
}

func WithMaxTypesPerAuthorizationModel(n int) StorageOption {
	return func(ds *MemoryBackend) { ds.maxTypesPerAuthorizationModel = n }
}

// Close closes any open connections and cleans up residual resources
// used by this storage adapter instance.
func (s *MemoryBackend) Close() {
}

// Read See storage.TupleBackend.Read
func (s *MemoryBackend) Read(ctx context.Context, store string, key *openfgav1.TupleKey) (storage.TupleIterator, error) {
	ctx, span := tracer.Start(ctx, "memory.Read")
	defer span.End()

	return s.read(ctx, store, key, storage.PaginationOptions{})
}

func (s *MemoryBackend) ReadPage(ctx context.Context, store string, key *openfgav1.TupleKey, paginationOptions storage.PaginationOptions) ([]*openfgav1.Tuple, []byte, error) {
	ctx, span := tracer.Start(ctx, "memory.ReadPage")
	defer span.End()

	it, err := s.read(ctx, store, key, paginationOptions)
	if err != nil {
		return nil, nil, err
	}

	return it.ToArray(ctx)
}

func (s *MemoryBackend) ReadChanges(ctx context.Context, store, objectType string, paginationOptions storage.PaginationOptions, horizonOffset time.Duration) ([]*openfgav1.TupleChange, []byte, error) {
	_, span := tracer.Start(ctx, "memory.ReadChanges")
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
				return nil, nil, err
			}
		}
	}

	if typeInToken != "" && typeInToken != objectType {
		return nil, nil, storage.ErrMismatchObjectType
	}

	var allChanges []*openfgav1.TupleChange
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
		return nil, nil, storage.ErrNotFound
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
		return nil, nil, storage.ErrNotFound
	}

	continuationToken = strconv.Itoa(len(allChanges))
	if to != len(allChanges) {
		continuationToken = strconv.Itoa(to)
	}
	continuationToken = continuationToken + fmt.Sprintf("|%s", objectType)

	return res, []byte(continuationToken), nil
}

func (s *MemoryBackend) read(ctx context.Context, store string, tk *openfgav1.TupleKey, paginationOptions storage.PaginationOptions) (*staticIterator, error) {
	_, span := tracer.Start(ctx, "memory.read")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	var matches []*storage.TupleRecord
	if tk.GetObject() == "" && tk.GetRelation() == "" && tk.GetUser() == "" {
		matches = make([]*storage.TupleRecord, len(s.tuples[store]))
		copy(matches, s.tuples[store])
	} else {
		for _, t := range s.tuples[store] {
			if match(t, tk) {
				matches = append(matches, t)
			}
		}
	}

	var err error
	var from int
	if paginationOptions.From != "" {
		from, err = strconv.Atoi(paginationOptions.From)
		if err != nil {
			telemetry.TraceError(span, err)
			return nil, err
		}
	}

	if from <= len(matches) {
		matches = matches[from:]
	}

	to := paginationOptions.PageSize
	if to != 0 && to < len(matches) {
		return &staticIterator{records: matches[:to], continuationToken: []byte(strconv.Itoa(from + to))}, nil
	}

	return &staticIterator{records: matches}, nil
}

// Write See storage.TupleBackend.Write
func (s *MemoryBackend) Write(ctx context.Context, store string, deletes storage.Deletes, writes storage.Writes) error {
	_, span := tracer.Start(ctx, "memory.Write")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	now := timestamppb.Now()

	if err := validateTuples(s.tuples[store], deletes, writes); err != nil {
		return err
	}

	var records []*storage.TupleRecord
Delete:
	for _, tr := range s.tuples[store] {
		t := tr.AsTuple()
		tk := t.GetKey()
		for _, k := range deletes {
			if match(tr, tupleUtils.TupleKeyWithoutConditionToTupleKey(k)) {
				s.changes[store] = append(
					s.changes[store],
					&openfgav1.TupleChange{
						TupleKey:  tupleUtils.NewTupleKey(tk.GetObject(), tk.GetRelation(), tk.GetUser()), // redact the condition info
						Operation: openfgav1.TupleOperation_TUPLE_OPERATION_DELETE,
						Timestamp: now,
					},
				)
				continue Delete
			}
		}
		records = append(records, tr)
	}

Write:
	for _, t := range writes {
		for _, et := range records {
			if match(et, t) {
				continue Write
			}
		}

		var conditionName string
		var conditionContext *structpb.Struct
		if condition := t.GetCondition(); condition != nil {
			conditionName = condition.Name
			conditionContext = condition.Context
		}

		objectType, objectID := tupleUtils.SplitObject(t.Object)

		records = append(records, &storage.TupleRecord{
			Store:            store,
			ObjectType:       objectType,
			ObjectID:         objectID,
			Relation:         t.Relation,
			User:             t.User,
			ConditionName:    conditionName,
			ConditionContext: conditionContext,
			Ulid:             ulid.MustNew(ulid.Timestamp(now.AsTime()), ulid.DefaultEntropy()).String(),
			InsertedAt:       now.AsTime(),
		})

		tk := tupleUtils.NewTupleKeyWithCondition(
			tupleUtils.BuildObject(objectType, objectID),
			t.Relation,
			t.User,
			conditionName,
			conditionContext,
		)

		s.changes[store] = append(s.changes[store], &openfgav1.TupleChange{
			TupleKey:  tk,
			Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
			Timestamp: now,
		})
	}
	s.tuples[store] = records
	return nil
}

func validateTuples(
	records []*storage.TupleRecord,
	deletes []*openfgav1.TupleKeyWithoutCondition,
	writes []*openfgav1.TupleKey,
) error {
	for _, tk := range deletes {
		if !find(records, tupleUtils.TupleKeyWithoutConditionToTupleKey(tk)) {
			return storage.InvalidWriteInputError(tk, openfgav1.TupleOperation_TUPLE_OPERATION_DELETE)
		}
	}
	for _, tk := range writes {
		if find(records, tk) {
			return storage.InvalidWriteInputError(tk, openfgav1.TupleOperation_TUPLE_OPERATION_WRITE)
		}
	}
	return nil
}

// find returns true if there is any *TupleRecord for which storage.match returns true
func find(records []*storage.TupleRecord, tupleKey *openfgav1.TupleKey) bool {
	for _, tr := range records {
		if match(tr, tupleKey) {
			return true
		}
	}
	return false
}

// ReadUserTuple See storage.TupleBackend.ReadUserTuple
func (s *MemoryBackend) ReadUserTuple(ctx context.Context, store string, key *openfgav1.TupleKey) (*openfgav1.Tuple, error) {
	_, span := tracer.Start(ctx, "memory.ReadUserTuple")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, t := range s.tuples[store] {
		if match(t, key) {
			return t.AsTuple(), nil
		}
	}

	telemetry.TraceError(span, storage.ErrNotFound)
	return nil, storage.ErrNotFound
}

// ReadUsersetTuples See storage.TupleBackend.ReadUsersetTuples
func (s *MemoryBackend) ReadUsersetTuples(ctx context.Context, store string, filter storage.ReadUsersetTuplesFilter) (storage.TupleIterator, error) {
	_, span := tracer.Start(ctx, "memory.ReadUsersetTuples")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	var matches []*storage.TupleRecord
	for _, t := range s.tuples[store] {
		if match(t, &openfgav1.TupleKey{
			Object:   filter.Object,
			Relation: filter.Relation,
		}) && tupleUtils.GetUserTypeFromUser(t.User) == tupleUtils.UserSet {
			if len(filter.AllowedUserTypeRestrictions) == 0 { // 1.0 model
				matches = append(matches, t)
				continue
			}

			// 1.1 model: see if the tuple found is of an allowed type
			userType := tupleUtils.GetType(t.User)
			_, userRelation := tupleUtils.SplitObjectRelation(t.User)
			for _, allowedType := range filter.AllowedUserTypeRestrictions {
				if allowedType.Type == userType && allowedType.GetRelation() == userRelation {
					matches = append(matches, t)
					continue
				}
			}
		}
	}

	return &staticIterator{records: matches}, nil
}

func (s *MemoryBackend) ReadStartingWithUser(
	ctx context.Context,
	store string,
	filter storage.ReadStartingWithUserFilter,
) (storage.TupleIterator, error) {
	_, span := tracer.Start(ctx, "memory.ReadStartingWithUser")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	var matches []*storage.TupleRecord
	for _, t := range s.tuples[store] {
		if t.ObjectType != filter.ObjectType {
			continue
		}

		if t.Relation != filter.Relation {
			continue
		}

		for _, userFilter := range filter.UserFilter {
			targetUser := userFilter.GetObject()
			if userFilter.GetRelation() != "" {
				targetUser = tupleUtils.GetObjectRelationAsString(userFilter)
			}

			if targetUser == t.User {
				matches = append(matches, t)
			}
		}
	}
	return &staticIterator{records: matches}, nil
}

func findAuthorizationModelByID(id string, configurations map[string]*AuthorizationModelEntry) (*openfgav1.AuthorizationModel, bool) {
	var nsc *openfgav1.AuthorizationModel

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

// ReadAuthorizationModel See storage.AuthorizationModelBackend.ReadAuthorizationModel
func (s *MemoryBackend) ReadAuthorizationModel(ctx context.Context, store string, id string) (*openfgav1.AuthorizationModel, error) {
	_, span := tracer.Start(ctx, "memory.ReadAuthorizationModel")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	tm, ok := s.authorizationModels[store]
	if !ok {
		telemetry.TraceError(span, storage.ErrNotFound)
		return nil, storage.ErrNotFound
	}

	if model, ok := findAuthorizationModelByID(id, tm); ok {
		if model.GetTypeDefinitions() == nil || len(model.GetTypeDefinitions()) == 0 {
			return nil, storage.ErrNotFound
		}
		return model, nil
	}

	telemetry.TraceError(span, storage.ErrNotFound)
	return nil, storage.ErrNotFound
}

// ReadAuthorizationModels See storage.AuthorizationModelBackend.ReadAuthorizationModels
// options.From is expected to be a number
func (s *MemoryBackend) ReadAuthorizationModels(ctx context.Context, store string, options storage.PaginationOptions) ([]*openfgav1.AuthorizationModel, []byte, error) {
	_, span := tracer.Start(ctx, "memory.ReadAuthorizationModels")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	models := make([]*openfgav1.AuthorizationModel, 0, len(s.authorizationModels[store]))
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
			return nil, nil, err
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
	_, span := tracer.Start(ctx, "memory.FindLatestAuthorizationModelID")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	tm, ok := s.authorizationModels[store]
	if !ok {
		telemetry.TraceError(span, storage.ErrNotFound)
		return "", storage.ErrNotFound
	}
	// find latest model
	nsc, ok := findAuthorizationModelByID("", tm)
	if !ok {
		telemetry.TraceError(span, storage.ErrNotFound)
		return "", storage.ErrNotFound
	}
	return nsc.Id, nil
}

// WriteAuthorizationModel See storage.TypeDefinitionWriteBackend.WriteAuthorizationModel
func (s *MemoryBackend) WriteAuthorizationModel(ctx context.Context, store string, model *openfgav1.AuthorizationModel) error {
	_, span := tracer.Start(ctx, "memory.WriteAuthorizationModel")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.authorizationModels[store]; !ok {
		s.authorizationModels[store] = make(map[string]*AuthorizationModelEntry)
	}

	for _, entry := range s.authorizationModels[store] {
		entry.latest = false
	}

	s.authorizationModels[store][model.Id] = &AuthorizationModelEntry{
		model:  model,
		latest: true,
	}

	return nil
}

func (s *MemoryBackend) CreateStore(ctx context.Context, newStore *openfgav1.Store) (*openfgav1.Store, error) {
	_, span := tracer.Start(ctx, "memory.CreateStore")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.stores[newStore.Id]; ok {
		return nil, storage.ErrCollision
	}

	now := timestamppb.New(time.Now().UTC())
	s.stores[newStore.Id] = &openfgav1.Store{
		Id:        newStore.Id,
		Name:      newStore.Name,
		CreatedAt: now,
		UpdatedAt: now,
	}

	return s.stores[newStore.Id], nil
}

func (s *MemoryBackend) DeleteStore(ctx context.Context, id string) error {
	_, span := tracer.Start(ctx, "memory.DeleteStore")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.stores, id)
	return nil
}

func (s *MemoryBackend) WriteAssertions(ctx context.Context, store, modelID string, assertions []*openfgav1.Assertion) error {
	_, span := tracer.Start(ctx, "memory.WriteAssertions")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	assertionsID := fmt.Sprintf("%s|%s", store, modelID)
	s.assertions[assertionsID] = assertions

	return nil
}

func (s *MemoryBackend) ReadAssertions(ctx context.Context, store, modelID string) ([]*openfgav1.Assertion, error) {
	_, span := tracer.Start(ctx, "memory.ReadAssertions")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	assertionsID := fmt.Sprintf("%s|%s", store, modelID)
	assertions, ok := s.assertions[assertionsID]
	if !ok {
		return []*openfgav1.Assertion{}, nil
	}
	return assertions, nil
}

// MaxTuplesPerWrite returns the maximum number of tuples allowed in one write operation
func (s *MemoryBackend) MaxTuplesPerWrite() int {
	return s.maxTuplesPerWrite
}

// MaxTypesPerAuthorizationModel returns the maximum number of types allowed in a type definition
func (s *MemoryBackend) MaxTypesPerAuthorizationModel() int {
	return s.maxTypesPerAuthorizationModel
}

func (s *MemoryBackend) GetStore(ctx context.Context, storeID string) (*openfgav1.Store, error) {
	_, span := tracer.Start(ctx, "memory.GetStore")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stores[storeID] == nil {
		return nil, storage.ErrNotFound
	}

	return s.stores[storeID], nil
}

func (s *MemoryBackend) ListStores(ctx context.Context, paginationOptions storage.PaginationOptions) ([]*openfgav1.Store, []byte, error) {
	_, span := tracer.Start(ctx, "memory.ListStores")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	stores := make([]*openfgav1.Store, 0, len(s.stores))
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
			return nil, nil, err
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

func (s *MemoryBackend) IsReady(ctx context.Context) (storage.ReadinessStatus, error) {
	return storage.ReadinessStatus{IsReady: true}, nil
}
