package grpc

import (
	"context"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	storagev1 "github.com/openfga/api/proto/storage/v1beta1"
	"github.com/openfga/openfga/pkg/storage"
)

// Server wraps a storage.OpenFGADatastore and exposes it via gRPC.
type Server struct {
	storagev1.UnimplementedStorageServiceServer
	datastore storage.OpenFGADatastore
}

func NewServer(datastore storage.OpenFGADatastore) *Server {
	return &Server{
		datastore: datastore,
	}
}

func (s *Server) Read(req *storagev1.ReadRequest, stream storagev1.StorageService_ReadServer) error {
	filter := storage.ReadFilter{
		Object:     req.GetFilter().GetObject(),
		Relation:   req.GetFilter().GetRelation(),
		User:       req.GetFilter().GetUser(),
		Conditions: req.GetFilter().GetConditions(),
	}

	options := storage.ReadOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: openfgav1.ConsistencyPreference(req.GetConsistency().GetPreference()),
		},
	}

	iter, err := s.datastore.Read(stream.Context(), req.GetStore(), filter, options)
	if err != nil {
		return toGRPCError(err)
	}
	defer iter.Stop()

	for {
		tuple, err := iter.Next(stream.Context())
		if err != nil {
			if err == storage.ErrIteratorDone {
				return nil
			}
			return toGRPCError(err)
		}

		if err := stream.Send(&storagev1.ReadResponse{Tuple: tuple}); err != nil {
			return err
		}
	}
}

func (s *Server) ReadPage(ctx context.Context, req *storagev1.ReadPageRequest) (*storagev1.ReadPageResponse, error) {
	filter := storage.ReadFilter{
		Object:     req.GetFilter().GetObject(),
		Relation:   req.GetFilter().GetRelation(),
		User:       req.GetFilter().GetUser(),
		Conditions: req.GetFilter().GetConditions(),
	}

	options := storage.ReadPageOptions{
		Pagination: storage.PaginationOptions{
			PageSize: int(req.GetPagination().GetPageSize()),
			From:     req.GetPagination().GetFrom(),
		},
		Consistency: storage.ConsistencyOptions{
			Preference: openfgav1.ConsistencyPreference(req.GetConsistency().GetPreference()),
		},
	}

	tuples, token, err := s.datastore.ReadPage(ctx, req.GetStore(), filter, options)
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &storagev1.ReadPageResponse{
		Tuples:            tuples,
		ContinuationToken: token,
	}, nil
}

func (s *Server) ReadUserTuple(ctx context.Context, req *storagev1.ReadUserTupleRequest) (*storagev1.ReadResponse, error) {
	options := storage.ReadUserTupleOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: openfgav1.ConsistencyPreference(req.GetConsistency().GetPreference()),
		},
	}

	reqFilter := req.GetFilter()
	filter := storage.ReadUserTupleFilter{
		Object:     reqFilter.GetObject(),
		Relation:   reqFilter.GetRelation(),
		User:       reqFilter.GetUser(),
		Conditions: reqFilter.GetConditions(),
	}

	tuple, err := s.datastore.ReadUserTuple(ctx, req.GetStore(), filter, options)
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &storagev1.ReadResponse{Tuple: tuple}, nil
}

func (s *Server) ReadUsersetTuples(req *storagev1.ReadUsersetTuplesRequest, stream storagev1.StorageService_ReadUsersetTuplesServer) error {

	filter := storage.ReadUsersetTuplesFilter{
		Object:                      req.GetFilter().GetObject(),
		Relation:                    req.GetFilter().GetRelation(),
		AllowedUserTypeRestrictions: req.GetFilter().GetAllowedUserTypeRestrictions(),
		Conditions:                  req.GetFilter().GetConditions(),
	}

	options := storage.ReadUsersetTuplesOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: openfgav1.ConsistencyPreference(req.GetConsistency().GetPreference()),
		},
	}

	iter, err := s.datastore.ReadUsersetTuples(stream.Context(), req.GetStore(), filter, options)
	if err != nil {
		return toGRPCError(err)
	}
	defer iter.Stop()

	for {
		tuple, err := iter.Next(stream.Context())
		if err != nil {
			if err == storage.ErrIteratorDone {
				return nil
			}
			return toGRPCError(err)
		}

		if err := stream.Send(&storagev1.ReadResponse{Tuple: tuple}); err != nil {
			return err
		}
	}
}

func (s *Server) ReadStartingWithUser(req *storagev1.ReadStartingWithUserRequest, stream storagev1.StorageService_ReadStartingWithUserServer) error {
	var userFilter []*openfgav1.ObjectRelation = nil
	if req.GetFilter() != nil && req.GetFilter().GetUserFilter() != nil {
		userFilter = req.GetFilter().GetUserFilter()
	}

	var objectIDs storage.SortedSet = nil
	if req.GetFilter() != nil && req.GetFilter().GetObjectIds() != nil {
		objectIDs = storage.NewSortedSet()
		for _, id := range req.GetFilter().GetObjectIds() {
			objectIDs.Add(id)
		}
	}

	filter := storage.ReadStartingWithUserFilter{
		ObjectType: req.GetFilter().GetObjectType(),
		Relation:   req.GetFilter().GetRelation(),
		UserFilter: userFilter,
		ObjectIDs:  objectIDs,
		Conditions: req.GetFilter().GetConditions(),
	}

	options := storage.ReadStartingWithUserOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: openfgav1.ConsistencyPreference(req.GetConsistency().GetPreference()),
		},
		WithResultsSortedAscending: req.GetWithResultsSortedAscending(),
	}

	iter, err := s.datastore.ReadStartingWithUser(stream.Context(), req.GetStore(), filter, options)
	if err != nil {
		return toGRPCError(err)
	}
	defer iter.Stop()

	for {
		tuple, err := iter.Next(stream.Context())
		if err != nil {
			if err == storage.ErrIteratorDone {
				return nil
			}
			return toGRPCError(err)
		}

		if err := stream.Send(&storagev1.ReadResponse{Tuple: tuple}); err != nil {
			return err
		}
	}
}

func (s *Server) Write(ctx context.Context, req *storagev1.WriteRequest) (*storagev1.WriteResponse, error) {
	// Note: condition field is intentionally ignored for deletes
	deletes := make([]*openfgav1.TupleKeyWithoutCondition, len(req.GetDeletes()))
	for i, d := range req.GetDeletes() {
		deletes[i] = &openfgav1.TupleKeyWithoutCondition{
			User:     d.GetUser(),
			Relation: d.GetRelation(),
			Object:   d.GetObject(),
		}
	}

	writes := req.GetWrites()

	var opts []storage.TupleWriteOption
	if reqOpts := req.GetOptions(); reqOpts != nil {
		switch reqOpts.GetOnMissingDelete() {
		case storagev1.OnMissingDelete_ON_MISSING_DELETE_IGNORE:
			opts = append(opts, storage.WithOnMissingDelete(storage.OnMissingDeleteIgnore))
		case storagev1.OnMissingDelete_ON_MISSING_DELETE_ERROR:
			opts = append(opts, storage.WithOnMissingDelete(storage.OnMissingDeleteError))
		default:
			opts = append(opts, storage.WithOnMissingDelete(storage.OnMissingDeleteError))
		}

		switch reqOpts.GetOnDuplicateInsert() {
		case storagev1.OnDuplicateInsert_ON_DUPLICATE_INSERT_IGNORE:
			opts = append(opts, storage.WithOnDuplicateInsert(storage.OnDuplicateInsertIgnore))
		case storagev1.OnDuplicateInsert_ON_DUPLICATE_INSERT_ERROR:
			opts = append(opts, storage.WithOnDuplicateInsert(storage.OnDuplicateInsertError))
		default:
			opts = append(opts, storage.WithOnDuplicateInsert(storage.OnDuplicateInsertError))
		}
	}

	err := s.datastore.Write(ctx, req.GetStore(), deletes, writes, opts...)
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &storagev1.WriteResponse{}, nil
}

func (s *Server) ReadAuthorizationModel(ctx context.Context, req *storagev1.ReadAuthorizationModelRequest) (*storagev1.ReadAuthorizationModelResponse, error) {
	model, err := s.datastore.ReadAuthorizationModel(ctx, req.GetStore(), req.GetId())
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &storagev1.ReadAuthorizationModelResponse{
		Model: model,
	}, nil
}

func (s *Server) ReadAuthorizationModels(ctx context.Context, req *storagev1.ReadAuthorizationModelsRequest) (*storagev1.ReadAuthorizationModelsResponse, error) {
	options := storage.ReadAuthorizationModelsOptions{
		Pagination: storage.PaginationOptions{
			PageSize: int(req.GetPagination().GetPageSize()),
			From:     req.GetPagination().GetFrom(),
		},
	}

	models, continuationToken, err := s.datastore.ReadAuthorizationModels(ctx, req.GetStore(), options)
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &storagev1.ReadAuthorizationModelsResponse{
		Models:            models,
		ContinuationToken: continuationToken,
	}, nil
}

func (s *Server) FindLatestAuthorizationModel(ctx context.Context, req *storagev1.FindLatestAuthorizationModelRequest) (*storagev1.FindLatestAuthorizationModelResponse, error) {
	model, err := s.datastore.FindLatestAuthorizationModel(ctx, req.GetStore())
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &storagev1.FindLatestAuthorizationModelResponse{
		Model: model,
	}, nil
}

func (s *Server) WriteAuthorizationModel(ctx context.Context, req *storagev1.WriteAuthorizationModelRequest) (*storagev1.WriteAuthorizationModelResponse, error) {
	err := s.datastore.WriteAuthorizationModel(ctx, req.GetStore(), req.GetModel())
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &storagev1.WriteAuthorizationModelResponse{}, nil
}

func (s *Server) CreateStore(ctx context.Context, req *storagev1.CreateStoreRequest) (*storagev1.CreateStoreResponse, error) {
	store, err := s.datastore.CreateStore(ctx, req.GetStore())
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &storagev1.CreateStoreResponse{
		Store: store,
	}, nil
}

func (s *Server) DeleteStore(ctx context.Context, req *storagev1.DeleteStoreRequest) (*storagev1.DeleteStoreResponse, error) {
	err := s.datastore.DeleteStore(ctx, req.GetId())
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &storagev1.DeleteStoreResponse{}, nil
}

func (s *Server) GetStore(ctx context.Context, req *storagev1.GetStoreRequest) (*storagev1.GetStoreResponse, error) {
	store, err := s.datastore.GetStore(ctx, req.GetId())
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &storagev1.GetStoreResponse{
		Store: store,
	}, nil
}

func (s *Server) ListStores(ctx context.Context, req *storagev1.ListStoresRequest) (*storagev1.ListStoresResponse, error) {
	options := storage.ListStoresOptions{
		IDs:  req.GetIds(),
		Name: req.GetName(),
		Pagination: storage.PaginationOptions{
			PageSize: int(req.GetPagination().GetPageSize()),
			From:     req.GetPagination().GetFrom(),
		},
	}

	stores, continuationToken, err := s.datastore.ListStores(ctx, options)
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &storagev1.ListStoresResponse{
		Stores:            stores,
		ContinuationToken: continuationToken,
	}, nil
}

func (s *Server) WriteAssertions(ctx context.Context, req *storagev1.WriteAssertionsRequest) (*storagev1.WriteAssertionsResponse, error) {
	err := s.datastore.WriteAssertions(ctx, req.GetStore(), req.GetModelId(), req.GetAssertions())
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &storagev1.WriteAssertionsResponse{}, nil
}

func (s *Server) ReadAssertions(ctx context.Context, req *storagev1.ReadAssertionsRequest) (*storagev1.ReadAssertionsResponse, error) {
	assertions, err := s.datastore.ReadAssertions(ctx, req.GetStore(), req.GetModelId())
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &storagev1.ReadAssertionsResponse{
		Assertions: assertions,
	}, nil
}

func (s *Server) ReadChanges(ctx context.Context, req *storagev1.ReadChangesRequest) (*storagev1.ReadChangesResponse, error) {
	filter := storage.ReadChangesFilter{
		ObjectType:    req.GetFilter().GetObjectType(),
		HorizonOffset: time.Duration(req.GetFilter().GetHorizonOffsetMs()) * time.Millisecond,
	}

	options := storage.ReadChangesOptions{
		Pagination: storage.PaginationOptions{
			PageSize: int(req.GetPagination().GetPageSize()),
			From:     req.GetPagination().GetFrom(),
		},
		SortDesc: req.GetSortDesc(),
	}

	changes, continuationToken, err := s.datastore.ReadChanges(ctx, req.GetStore(), filter, options)
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &storagev1.ReadChangesResponse{
		Changes:           changes,
		ContinuationToken: continuationToken,
	}, nil
}

func (s *Server) IsReady(ctx context.Context, req *storagev1.IsReadyRequest) (*storagev1.IsReadyResponse, error) {
	readinessStatus, err := s.datastore.IsReady(ctx)
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &storagev1.IsReadyResponse{
		Message: readinessStatus.Message,
		IsReady: readinessStatus.IsReady,
	}, nil
}
