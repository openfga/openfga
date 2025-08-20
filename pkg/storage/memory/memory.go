package memory

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"go.opentelemetry.io/otel"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/telemetry"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
)

var tracer = otel.Tracer("openfga/pkg/storage/memory")

type staticIterator struct {
	records           []*storage.TupleRecord
	continuationToken string
	mu                sync.Mutex
}

// match returns true if all the fields in t [*storage.TupleRecord] are equal to
// the same field in the target [*openfgav1.TupleKey]. If the input Object
// doesn't specify an ID, only the Object Types are compared. If a field
// in the input parameter is empty, it is ignored in the comparison.
func match(t *storage.TupleRecord, target *openfgav1.TupleKey) bool {
	if target.GetObject() != "" {
		td, objectid := tupleUtils.SplitObject(target.GetObject())
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
	if target.GetRelation() != "" && t.Relation != target.GetRelation() {
		return false
	}
	if target.GetUser() != "" && t.User != target.GetUser() {
		return false
	}
	return true
}

// Next see [storage.Iterator].Next.
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

// Stop does not do anything for staticIterator.
func (s *staticIterator) Stop() {}

// Head see [storage.Iterator].Next.
func (s *staticIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.records) == 0 {
		return nil, storage.ErrIteratorDone
	}

	rec := s.records[0]
	return rec.AsTuple(), nil
}

// ToArray converts the entire sequence of tuples in the staticIterator to an array format.
func (s *staticIterator) ToArray(ctx context.Context) ([]*openfgav1.Tuple, string, error) {
	var res []*openfgav1.Tuple
	for range s.records {
		t, err := s.Next(ctx)
		if err != nil {
			return nil, "", err
		}

		res = append(res, t)
	}

	return res, s.continuationToken, nil
}

// StorageOption defines a function type used for configuring a [MemoryBackend] instance.
type StorageOption func(dataStore *MemoryBackend)

const (
	defaultMaxTuplesPerWrite             = 100
	defaultMaxTypesPerAuthorizationModel = 100
)

// MemoryBackend provides an ephemeral memory-backed implementation of [storage.OpenFGADatastore].
// These instances may be safely shared by multiple go-routines.
type MemoryBackend struct {
	maxTuplesPerWrite             int
	maxTypesPerAuthorizationModel int

	// TupleBackend
	// map: store => set of tuples
	tuples      map[string][]*storage.TupleRecord // GUARDED_BY(mutexTuples).
	mutexTuples sync.RWMutex

	// ChangelogBackend
	// map: store => set of changes
	changes map[string][]*tupleChangeRec // GUARDED_BY(mutexTuples).

	// AuthorizationModelBackend
	// map: store = > map: type definition id => type definition
	authorizationModels map[string]map[string]*AuthorizationModelEntry // GUARDED_BY(mutexModels).
	mutexModels         sync.RWMutex

	// map: store id => store data
	stores      map[string]*openfgav1.Store // GUARDED_BY(mutexStores).
	mutexStores sync.RWMutex

	// map: store id | authz model id => assertions
	assertions      map[string][]*openfgav1.Assertion // GUARDED_BY(mutexAssertions).
	mutexAssertions sync.RWMutex
}

// Ensures that [MemoryBackend] implements the [storage.OpenFGADatastore] interface.
var _ storage.OpenFGADatastore = (*MemoryBackend)(nil)

// AuthorizationModelEntry represents an entry in a storage system
// that holds information about an authorization model.
type AuthorizationModelEntry struct {
	model  *openfgav1.AuthorizationModel
	latest bool
}

// New creates a new [MemoryBackend] given the options.
func New(opts ...StorageOption) storage.OpenFGADatastore {
	ds := &MemoryBackend{
		maxTuplesPerWrite:             defaultMaxTuplesPerWrite,
		maxTypesPerAuthorizationModel: defaultMaxTypesPerAuthorizationModel,
		tuples:                        make(map[string][]*storage.TupleRecord, 0),
		changes:                       make(map[string][]*tupleChangeRec, 0),
		authorizationModels:           make(map[string]map[string]*AuthorizationModelEntry),
		stores:                        make(map[string]*openfgav1.Store, 0),
		assertions:                    make(map[string][]*openfgav1.Assertion, 0),
	}

	for _, opt := range opts {
		opt(ds)
	}

	return ds
}

// WithMaxTuplesPerWrite returns a [StorageOption] that sets the maximum number of tuples allowed in a single write operation.
// This option is used to configure a [MemoryBackend] instance, providing a limit to the number of tuples that can be written at once.
// This helps in managing and controlling the load and performance of the memory storage during bulk write operations.
func WithMaxTuplesPerWrite(n int) StorageOption {
	return func(ds *MemoryBackend) { ds.maxTuplesPerWrite = n }
}

// WithMaxTypesPerAuthorizationModel returns a [StorageOption] that sets the maximum number of types allowed per authorization model.
// This configuration is particularly useful for limiting the complexity or size of an authorization model in a MemoryBackend instance,
// ensuring that models remain manageable and within predefined resource constraints.
func WithMaxTypesPerAuthorizationModel(n int) StorageOption {
	return func(ds *MemoryBackend) { ds.maxTypesPerAuthorizationModel = n }
}

// Close does not do anything for [MemoryBackend].
func (s *MemoryBackend) Close() {}

// Read see [storage.RelationshipTupleReader].Read.
func (s *MemoryBackend) Read(ctx context.Context, store string, key *openfgav1.TupleKey, _ storage.ReadOptions) (storage.TupleIterator, error) {
	ctx, span := tracer.Start(ctx, "memory.Read")
	defer span.End()

	return s.read(ctx, store, key, nil)
}

// ReadPage see [storage.RelationshipTupleReader].ReadPage.
func (s *MemoryBackend) ReadPage(ctx context.Context, store string, key *openfgav1.TupleKey, options storage.ReadPageOptions) ([]*openfgav1.Tuple, string, error) {
	ctx, span := tracer.Start(ctx, "memory.ReadPage")
	defer span.End()

	it, err := s.read(ctx, store, key, &options)
	if err != nil {
		return nil, "", err
	}

	return it.ToArray(ctx)
}

// ReadChanges see [storage.ChangelogBackend].ReadChanges.
func (s *MemoryBackend) ReadChanges(ctx context.Context, store string, filter storage.ReadChangesFilter, options storage.ReadChangesOptions) ([]*openfgav1.TupleChange, string, error) {
	_, span := tracer.Start(ctx, "memory.ReadChanges")
	defer span.End()

	s.mutexTuples.RLock()
	defer s.mutexTuples.RUnlock()

	var from *ulid.ULID
	if options.Pagination.From != "" {
		parsed, err := ulid.Parse(options.Pagination.From)
		if err != nil {
			return nil, "", storage.ErrInvalidContinuationToken
		}
		from = &parsed
	}

	objectType := filter.ObjectType
	horizonOffset := filter.HorizonOffset

	var allChanges []*tupleChangeRec
	now := time.Now().UTC()
	for _, changeRec := range s.changes[store] {
		if objectType == "" || (strings.HasPrefix(changeRec.Change.GetTupleKey().GetObject(), objectType+":")) {
			if changeRec.Change.GetTimestamp().AsTime().After(now.Add(-horizonOffset)) {
				break
			}
			if from != nil {
				if !options.SortDesc && changeRec.Ulid.Compare(*from) <= 0 {
					continue
				} else if options.SortDesc && changeRec.Ulid.Compare(*from) >= 0 {
					continue
				}
			}
			allChanges = append(allChanges, changeRec)
		}
	}
	if len(allChanges) == 0 {
		return nil, "", storage.ErrNotFound
	}

	pageSize := storage.DefaultPageSize
	if options.Pagination.PageSize > 0 {
		pageSize = options.Pagination.PageSize
	}
	if options.SortDesc {
		slices.Reverse(allChanges)
	}

	to := pageSize
	if len(allChanges) < to {
		to = len(allChanges)
	}
	if to == 0 {
		return nil, "", storage.ErrNotFound
	}

	res := make([]*openfgav1.TupleChange, 0, to)

	var last ulid.ULID
	for _, change := range allChanges[:to] {
		res = append(res, change.Change)
		last = change.Ulid
	}

	return res, last.String(), nil
}

// read returns an iterator of a store's tuples with a given tuple as filter.
// A nil paginationOptions input means the returned iterator will iterate through all values.
func (s *MemoryBackend) read(ctx context.Context, store string, tk *openfgav1.TupleKey, options *storage.ReadPageOptions) (*staticIterator, error) {
	_, span := tracer.Start(ctx, "memory.read")
	defer span.End()

	s.mutexTuples.RLock()
	defer s.mutexTuples.RUnlock()

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
	if options != nil && options.Pagination.From != "" {
		from, err = strconv.Atoi(options.Pagination.From)
		if err != nil {
			telemetry.TraceError(span, err)
			return nil, err
		}
	}

	if from <= len(matches) {
		matches = matches[from:]
	}

	to := 0 // fetch everything
	if options != nil {
		to = options.Pagination.PageSize
	}
	if to != 0 && to < len(matches) {
		return &staticIterator{records: matches[:to], continuationToken: strconv.Itoa(from + to)}, nil
	}

	return &staticIterator{records: matches}, nil
}

type tupleChangeRec struct {
	Change *openfgav1.TupleChange
	Ulid   ulid.ULID
}

// Write see [storage.RelationshipTupleWriter].Write.
func (s *MemoryBackend) Write(ctx context.Context, store string, deletes storage.Deletes, writes storage.Writes, _ ...storage.TupleWriteOptions) error {
	_, span := tracer.Start(ctx, "memory.Write")
	defer span.End()

	s.mutexTuples.Lock()
	defer s.mutexTuples.Unlock()

	now := timestamppb.Now()

	if err := validateTuples(s.tuples[store], deletes, writes); err != nil {
		return err
	}

	var records []*storage.TupleRecord
	entropy := ulid.DefaultEntropy()
Delete:
	for _, tr := range s.tuples[store] {
		t := tr.AsTuple()
		tk := t.GetKey()
		for _, k := range deletes {
			if match(tr, tupleUtils.TupleKeyWithoutConditionToTupleKey(k)) {
				s.changes[store] = append(
					s.changes[store],
					&tupleChangeRec{
						Change: &openfgav1.TupleChange{
							TupleKey:  tupleUtils.NewTupleKey(tk.GetObject(), tk.GetRelation(), tk.GetUser()), // Redact the condition info.
							Operation: openfgav1.TupleOperation_TUPLE_OPERATION_DELETE,
							Timestamp: now,
						},
						Ulid: ulid.MustNew(ulid.Timestamp(now.AsTime()), entropy),
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
			conditionName = condition.GetName()
			conditionContext = condition.GetContext()
		}

		objectType, objectID := tupleUtils.SplitObject(t.GetObject())

		records = append(records, &storage.TupleRecord{
			Store:            store,
			ObjectType:       objectType,
			ObjectID:         objectID,
			Relation:         t.GetRelation(),
			User:             t.GetUser(),
			ConditionName:    conditionName,
			ConditionContext: conditionContext,
			Ulid:             ulid.MustNew(ulid.Timestamp(now.AsTime()), ulid.DefaultEntropy()).String(),
			InsertedAt:       now.AsTime(),
		})

		tk := tupleUtils.NewTupleKeyWithCondition(
			tupleUtils.BuildObject(objectType, objectID),
			t.GetRelation(),
			t.GetUser(),
			conditionName,
			conditionContext,
		)

		s.changes[store] = append(s.changes[store], &tupleChangeRec{
			Change: &openfgav1.TupleChange{
				TupleKey:  tk,
				Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
				Timestamp: now,
			},
			Ulid: ulid.MustNew(ulid.Timestamp(now.AsTime()), entropy),
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

// find returns true if there is any [*storage.TupleRecord] for which match returns true.
func find(records []*storage.TupleRecord, tupleKey *openfgav1.TupleKey) bool {
	for _, tr := range records {
		if match(tr, tupleKey) {
			return true
		}
	}
	return false
}

// ReadUserTuple see [storage.RelationshipTupleReader].ReadUserTuple.
func (s *MemoryBackend) ReadUserTuple(ctx context.Context, store string, key *openfgav1.TupleKey, _ storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
	_, span := tracer.Start(ctx, "memory.ReadUserTuple")
	defer span.End()

	s.mutexTuples.RLock()
	defer s.mutexTuples.RUnlock()

	for _, t := range s.tuples[store] {
		if match(t, key) {
			return t.AsTuple(), nil
		}
	}

	telemetry.TraceError(span, storage.ErrNotFound)
	return nil, storage.ErrNotFound
}

// ReadUsersetTuples see [storage.RelationshipTupleReader].ReadUsersetTuples.
func (s *MemoryBackend) ReadUsersetTuples(
	ctx context.Context,
	store string,
	filter storage.ReadUsersetTuplesFilter,
	_ storage.ReadUsersetTuplesOptions,
) (storage.TupleIterator, error) {
	_, span := tracer.Start(ctx, "memory.ReadUsersetTuples")
	defer span.End()

	s.mutexTuples.RLock()
	defer s.mutexTuples.RUnlock()

	var matches []*storage.TupleRecord
	for _, t := range s.tuples[store] {
		if match(t, &openfgav1.TupleKey{
			Object:   filter.Object,
			Relation: filter.Relation,
		}) && tupleUtils.GetUserTypeFromUser(t.User) == tupleUtils.UserSet {
			if len(filter.AllowedUserTypeRestrictions) == 0 { // 1.0 model.
				matches = append(matches, t)
				continue
			}

			// 1.1 model: see if the tuple found is of an allowed type.
			userType := tupleUtils.GetType(t.User)
			_, userRelation := tupleUtils.SplitObjectRelation(t.User)
			for _, allowedType := range filter.AllowedUserTypeRestrictions {
				if allowedType.GetType() == userType && allowedType.GetRelation() == userRelation {
					matches = append(matches, t)
					continue
				}
			}
		}
	}

	return &staticIterator{records: matches}, nil
}

// ReadStartingWithUser see [storage.RelationshipTupleReader].ReadStartingWithUser.
func (s *MemoryBackend) ReadStartingWithUser(
	ctx context.Context,
	store string,
	filter storage.ReadStartingWithUserFilter,
	options storage.ReadStartingWithUserOptions,
) (storage.TupleIterator, error) {
	_, span := tracer.Start(ctx, "memory.ReadStartingWithUser")
	defer span.End()

	s.mutexTuples.RLock()
	defer s.mutexTuples.RUnlock()

	var matches []*storage.TupleRecord
	for _, t := range s.tuples[store] {
		if t.ObjectType != filter.ObjectType {
			continue
		}

		if t.Relation != filter.Relation {
			continue
		}

		if filter.ObjectIDs != nil && !filter.ObjectIDs.Exists(t.ObjectID) {
			continue
		}

		for _, userFilter := range filter.UserFilter {
			targetUser := userFilter.GetObject()
			if userFilter.GetRelation() != "" {
				targetUser = tupleUtils.GetObjectRelationAsString(userFilter)
			}

			if targetUser != t.User {
				continue
			}

			matches = append(matches, t)
		}
	}
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].ObjectID < matches[j].ObjectID
	})

	return &staticIterator{records: matches}, nil
}

func findAuthorizationModelByID(
	id string,
	configurations map[string]*AuthorizationModelEntry,
) (*openfgav1.AuthorizationModel, bool) {
	if id != "" {
		if entry, ok := configurations[id]; ok {
			return entry.model, true
		}

		return nil, false
	}

	for _, entry := range configurations {
		if entry.latest {
			return entry.model, true
		}
	}

	return nil, false
}

// ReadAuthorizationModel see [storage.AuthorizationModelReadBackend].ReadAuthorizationModel.
func (s *MemoryBackend) ReadAuthorizationModel(
	ctx context.Context,
	store string,
	id string,
) (*openfgav1.AuthorizationModel, error) {
	_, span := tracer.Start(ctx, "memory.ReadAuthorizationModel")
	defer span.End()

	s.mutexModels.RLock()
	defer s.mutexModels.RUnlock()

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

// ReadAuthorizationModels see [storage.AuthorizationModelReadBackend].ReadAuthorizationModels.
func (s *MemoryBackend) ReadAuthorizationModels(ctx context.Context, store string, options storage.ReadAuthorizationModelsOptions) ([]*openfgav1.AuthorizationModel, string, error) {
	_, span := tracer.Start(ctx, "memory.ReadAuthorizationModels")
	defer span.End()

	s.mutexModels.RLock()
	defer s.mutexModels.RUnlock()

	models := make([]*openfgav1.AuthorizationModel, 0, len(s.authorizationModels[store]))
	for _, entry := range s.authorizationModels[store] {
		models = append(models, entry.model)
	}

	// From newest to oldest.
	sort.Slice(models, func(i, j int) bool {
		return models[i].GetId() > models[j].GetId()
	})

	var from int64
	continuationToken := ""
	var err error

	pageSize := storage.DefaultPageSize
	if options.Pagination.PageSize > 0 {
		pageSize = options.Pagination.PageSize
	}

	if options.Pagination.From != "" {
		from, err = strconv.ParseInt(options.Pagination.From, 10, 32)
		if err != nil {
			return nil, "", err
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

	return res, continuationToken, nil
}

// FindLatestAuthorizationModel see [storage.AuthorizationModelReadBackend].FindLatestAuthorizationModel.
func (s *MemoryBackend) FindLatestAuthorizationModel(ctx context.Context, store string) (*openfgav1.AuthorizationModel, error) {
	_, span := tracer.Start(ctx, "memory.FindLatestAuthorizationModel")
	defer span.End()

	s.mutexModels.RLock()
	defer s.mutexModels.RUnlock()

	tm, ok := s.authorizationModels[store]
	if !ok {
		telemetry.TraceError(span, storage.ErrNotFound)
		return nil, storage.ErrNotFound
	}

	// Find latest model.
	nsc, ok := findAuthorizationModelByID("", tm)
	if !ok {
		telemetry.TraceError(span, storage.ErrNotFound)
		return nil, storage.ErrNotFound
	}
	return nsc, nil
}

// WriteAuthorizationModel see [storage.TypeDefinitionWriteBackend].WriteAuthorizationModel.
func (s *MemoryBackend) WriteAuthorizationModel(ctx context.Context, store string, model *openfgav1.AuthorizationModel) error {
	_, span := tracer.Start(ctx, "memory.WriteAuthorizationModel")
	defer span.End()

	s.mutexModels.Lock()
	defer s.mutexModels.Unlock()

	if _, ok := s.authorizationModels[store]; !ok {
		s.authorizationModels[store] = make(map[string]*AuthorizationModelEntry)
	}

	for _, entry := range s.authorizationModels[store] {
		entry.latest = false
	}

	s.authorizationModels[store][model.GetId()] = &AuthorizationModelEntry{
		model:  model,
		latest: true,
	}

	return nil
}

// CreateStore adds a new store to the [MemoryBackend].
func (s *MemoryBackend) CreateStore(ctx context.Context, newStore *openfgav1.Store) (*openfgav1.Store, error) {
	_, span := tracer.Start(ctx, "memory.CreateStore")
	defer span.End()

	s.mutexStores.Lock()
	defer s.mutexStores.Unlock()

	if _, ok := s.stores[newStore.GetId()]; ok {
		return nil, storage.ErrCollision
	}

	now := timestamppb.New(time.Now().UTC())
	s.stores[newStore.GetId()] = &openfgav1.Store{
		Id:        newStore.GetId(),
		Name:      newStore.GetName(),
		CreatedAt: now,
		UpdatedAt: now,
	}

	return s.stores[newStore.GetId()], nil
}

// DeleteStore removes a store from the [MemoryBackend].
func (s *MemoryBackend) DeleteStore(ctx context.Context, id string) error {
	_, span := tracer.Start(ctx, "memory.DeleteStore")
	defer span.End()

	s.mutexStores.Lock()
	defer s.mutexStores.Unlock()

	delete(s.stores, id)
	return nil
}

// WriteAssertions see [storage.AssertionsBackend].WriteAssertions.
func (s *MemoryBackend) WriteAssertions(ctx context.Context, store, modelID string, assertions []*openfgav1.Assertion) error {
	_, span := tracer.Start(ctx, "memory.WriteAssertions")
	defer span.End()

	s.mutexAssertions.Lock()
	defer s.mutexAssertions.Unlock()

	assertionsID := fmt.Sprintf("%s|%s", store, modelID)
	s.assertions[assertionsID] = assertions

	return nil
}

// ReadAssertions see [storage.AssertionsBackend].ReadAssertions.
func (s *MemoryBackend) ReadAssertions(ctx context.Context, store, modelID string) ([]*openfgav1.Assertion, error) {
	_, span := tracer.Start(ctx, "memory.ReadAssertions")
	defer span.End()

	s.mutexAssertions.RLock()
	defer s.mutexAssertions.RUnlock()

	assertionsID := fmt.Sprintf("%s|%s", store, modelID)
	assertions, ok := s.assertions[assertionsID]
	if !ok {
		return []*openfgav1.Assertion{}, nil
	}
	return assertions, nil
}

// MaxTuplesPerWrite see [storage.RelationshipTupleWriter].MaxTuplesPerWrite.
func (s *MemoryBackend) MaxTuplesPerWrite() int {
	return s.maxTuplesPerWrite
}

// MaxTypesPerAuthorizationModel see [storage.TypeDefinitionWriteBackend].MaxTypesPerAuthorizationModel.
func (s *MemoryBackend) MaxTypesPerAuthorizationModel() int {
	return s.maxTypesPerAuthorizationModel
}

// GetStore retrieves the details of a specific store from the MemoryBackend using its storeID.
func (s *MemoryBackend) GetStore(ctx context.Context, storeID string) (*openfgav1.Store, error) {
	_, span := tracer.Start(ctx, "memory.GetStore")
	defer span.End()

	s.mutexStores.RLock()
	defer s.mutexStores.RUnlock()

	if s.stores[storeID] == nil {
		return nil, storage.ErrNotFound
	}

	return s.stores[storeID], nil
}

// ListStores provides a paginated list of all stores present in the MemoryBackend.
func (s *MemoryBackend) ListStores(ctx context.Context, options storage.ListStoresOptions) ([]*openfgav1.Store, string, error) {
	_, span := tracer.Start(ctx, "memory.ListStores")
	defer span.End()

	s.mutexStores.RLock()
	defer s.mutexStores.RUnlock()

	stores := make([]*openfgav1.Store, 0, len(s.stores))
	for _, t := range s.stores {
		stores = append(stores, t)
	}

	if len(options.IDs) > 0 {
		filteredStores := make([]*openfgav1.Store, 0, len(stores))
		for _, storeID := range options.IDs {
			for _, store := range stores {
				if store.GetId() == storeID {
					filteredStores = append(filteredStores, store)
				}
			}
		}
		stores = filteredStores
	}

	if options.Name != "" {
		filteredStores := make([]*openfgav1.Store, 0, len(stores))
		for _, store := range stores {
			if store.GetName() == options.Name {
				filteredStores = append(filteredStores, store)
			}
		}
		stores = filteredStores
	}

	// From oldest to newest.
	sort.SliceStable(stores, func(i, j int) bool {
		return stores[i].GetId() < stores[j].GetId()
	})

	var err error
	var from int64
	if options.Pagination.From != "" {
		from, err = strconv.ParseInt(options.Pagination.From, 10, 32)
		if err != nil {
			return nil, "", err
		}
	}
	pageSize := storage.DefaultPageSize
	if options.Pagination.PageSize > 0 {
		pageSize = options.Pagination.PageSize
	}
	to := int(from) + pageSize
	if len(stores) < to {
		to = len(stores)
	}
	res := stores[from:to]
	if len(res) == 0 {
		return nil, "", nil
	}

	continuationToken := ""
	if to != len(stores) {
		continuationToken = strconv.Itoa(to)
	}

	return res, continuationToken, nil
}

// IsReady see [storage.OpenFGADatastore].IsReady.
func (s *MemoryBackend) IsReady(context.Context) (storage.ReadinessStatus, error) {
	return storage.ReadinessStatus{IsReady: true}, nil
}
