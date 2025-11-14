package grpc

import (
	"context"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	storagev1 "github.com/openfga/openfga/pkg/storage/grpc/proto/storage/v1"
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

		if err := stream.Send(&storagev1.ReadResponse{Tuple: toStorageTuple(tuple)}); err != nil {
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

	storageTuples := make([]*storagev1.Tuple, len(tuples))
	for i, t := range tuples {
		storageTuples[i] = toStorageTuple(t)
	}

	return &storagev1.ReadPageResponse{
		Tuples:            storageTuples,
		ContinuationToken: token,
	}, nil
}

func (s *Server) ReadUserTuple(ctx context.Context, req *storagev1.ReadUserTupleRequest) (*storagev1.ReadResponse, error) {
	options := storage.ReadUserTupleOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: openfgav1.ConsistencyPreference(req.GetConsistency().GetPreference()),
		},
	}

	tuple, err := s.datastore.ReadUserTuple(ctx, req.GetStore(), fromStorageTupleKey(req.GetTupleKey()), options)
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &storagev1.ReadResponse{Tuple: toStorageTuple(tuple)}, nil
}

func (s *Server) ReadUsersetTuples(req *storagev1.ReadUsersetTuplesRequest, stream storagev1.StorageService_ReadUsersetTuplesServer) error {
	allowedRefs := make([]*openfgav1.RelationReference, len(req.GetFilter().GetAllowedUserTypeRestrictions()))
	for i, ref := range req.GetFilter().GetAllowedUserTypeRestrictions() {
		allowedRefs[i] = fromStorageRelationReference(ref)
	}

	filter := storage.ReadUsersetTuplesFilter{
		Object:                      req.GetFilter().GetObject(),
		Relation:                    req.GetFilter().GetRelation(),
		AllowedUserTypeRestrictions: allowedRefs,
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

		if err := stream.Send(&storagev1.ReadResponse{Tuple: toStorageTuple(tuple)}); err != nil {
			return err
		}
	}
}

func (s *Server) ReadStartingWithUser(req *storagev1.ReadStartingWithUserRequest, stream storagev1.StorageService_ReadStartingWithUserServer) error {
	userFilter := make([]*openfgav1.ObjectRelation, len(req.GetFilter().GetUserFilter()))
	for i, obj := range req.GetFilter().GetUserFilter() {
		userFilter[i] = fromStorageObjectRelation(obj)
	}

	objectIDs := storage.NewSortedSet()
	for _, id := range req.GetFilter().GetObjectIds() {
		objectIDs.Add(id)
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

		if err := stream.Send(&storagev1.ReadResponse{Tuple: toStorageTuple(tuple)}); err != nil {
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

	writes := make([]*openfgav1.TupleKey, len(req.GetWrites()))
	for i, w := range req.GetWrites() {
		writes[i] = fromStorageTupleKey(w)
	}

	opts := fromStorageTupleWriteOptions(req.GetOptions())

	err := s.datastore.Write(ctx, req.GetStore(), deletes, writes, opts...)
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &storagev1.WriteResponse{}, nil
}

// IsReady implements StorageService.IsReady.
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
