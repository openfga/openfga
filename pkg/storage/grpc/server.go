package grpc

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	storagev1 "github.com/openfga/openfga/pkg/storage/grpc/proto/storage/v1"
)

// Server wraps a storage.OpenFGADatastore and exposes it via gRPC.
type Server struct {
	storagev1.UnimplementedStorageServiceServer
	datastore storage.OpenFGADatastore
}

// NewServer creates a new gRPC storage server.
func NewServer(datastore storage.OpenFGADatastore) *Server {
	return &Server{
		datastore: datastore,
	}
}

// Read implements StorageService.Read.
func (s *Server) Read(req *storagev1.ReadRequest, stream storagev1.StorageService_ReadServer) error {
	filter := storage.ReadFilter{
		Object:     req.Filter.Object,
		Relation:   req.Filter.Relation,
		User:       req.Filter.User,
		Conditions: req.Filter.Conditions,
	}

	options := storage.ReadOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: openfgav1.ConsistencyPreference(req.Consistency.Preference),
		},
	}

	iter, err := s.datastore.Read(stream.Context(), req.Store, filter, options)
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("read failed: %v", err))
	}
	defer iter.Stop()

	for {
		tuple, err := iter.Next(stream.Context())
		if err != nil {
			if err == storage.ErrIteratorDone {
				return nil
			}
			return status.Error(codes.Internal, fmt.Sprintf("iterator error: %v", err))
		}

		if err := stream.Send(&storagev1.ReadResponse{Tuple: toStorageTuple(tuple)}); err != nil {
			return err
		}
	}
}

// ReadPage implements StorageService.ReadPage.
func (s *Server) ReadPage(ctx context.Context, req *storagev1.ReadPageRequest) (*storagev1.ReadPageResponse, error) {
	filter := storage.ReadFilter{
		Object:     req.Filter.Object,
		Relation:   req.Filter.Relation,
		User:       req.Filter.User,
		Conditions: req.Filter.Conditions,
	}

	options := storage.ReadPageOptions{
		Pagination: storage.PaginationOptions{
			PageSize: int(req.Pagination.PageSize),
			From:     req.Pagination.From,
		},
		Consistency: storage.ConsistencyOptions{
			Preference: openfgav1.ConsistencyPreference(req.Consistency.Preference),
		},
	}

	tuples, token, err := s.datastore.ReadPage(ctx, req.Store, filter, options)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("read page failed: %v", err))
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

// ReadUserTuple implements StorageService.ReadUserTuple.
func (s *Server) ReadUserTuple(ctx context.Context, req *storagev1.ReadUserTupleRequest) (*storagev1.ReadResponse, error) {
	options := storage.ReadUserTupleOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: openfgav1.ConsistencyPreference(req.Consistency.Preference),
		},
	}

	tuple, err := s.datastore.ReadUserTuple(ctx, req.Store, fromStorageTupleKey(req.TupleKey), options)
	if err != nil {
		if err == storage.ErrNotFound {
			return nil, status.Error(codes.NotFound, "tuple not found")
		}
		return nil, status.Error(codes.Internal, fmt.Sprintf("read user tuple failed: %v", err))
	}

	return &storagev1.ReadResponse{Tuple: toStorageTuple(tuple)}, nil
}

// ReadUsersetTuples implements StorageService.ReadUsersetTuples.
func (s *Server) ReadUsersetTuples(req *storagev1.ReadUsersetTuplesRequest, stream storagev1.StorageService_ReadUsersetTuplesServer) error {
	allowedRefs := make([]*openfgav1.RelationReference, len(req.Filter.AllowedUserTypeRestrictions))
	for i, ref := range req.Filter.AllowedUserTypeRestrictions {
		allowedRefs[i] = fromStorageRelationReference(ref)
	}

	filter := storage.ReadUsersetTuplesFilter{
		Object:                      req.Filter.Object,
		Relation:                    req.Filter.Relation,
		AllowedUserTypeRestrictions: allowedRefs,
		Conditions:                  req.Filter.Conditions,
	}

	options := storage.ReadUsersetTuplesOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: openfgav1.ConsistencyPreference(req.Consistency.Preference),
		},
	}

	iter, err := s.datastore.ReadUsersetTuples(stream.Context(), req.Store, filter, options)
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("read userset tuples failed: %v", err))
	}
	defer iter.Stop()

	for {
		tuple, err := iter.Next(stream.Context())
		if err != nil {
			if err == storage.ErrIteratorDone {
				return nil
			}
			return status.Error(codes.Internal, fmt.Sprintf("iterator error: %v", err))
		}

		if err := stream.Send(&storagev1.ReadResponse{Tuple: toStorageTuple(tuple)}); err != nil {
			return err
		}
	}
}

// ReadStartingWithUser implements StorageService.ReadStartingWithUser.
func (s *Server) ReadStartingWithUser(req *storagev1.ReadStartingWithUserRequest, stream storagev1.StorageService_ReadStartingWithUserServer) error {
	userFilter := make([]*openfgav1.ObjectRelation, len(req.Filter.UserFilter))
	for i, obj := range req.Filter.UserFilter {
		userFilter[i] = fromStorageObjectRelation(obj)
	}

	objectIDs := storage.NewSortedSet()
	for _, id := range req.Filter.ObjectIds {
		objectIDs.Add(id)
	}

	filter := storage.ReadStartingWithUserFilter{
		ObjectType: req.Filter.ObjectType,
		Relation:   req.Filter.Relation,
		UserFilter: userFilter,
		ObjectIDs:  objectIDs,
		Conditions: req.Filter.Conditions,
	}

	options := storage.ReadStartingWithUserOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: openfgav1.ConsistencyPreference(req.Consistency.Preference),
		},
		WithResultsSortedAscending: req.WithResultsSortedAscending,
	}

	iter, err := s.datastore.ReadStartingWithUser(stream.Context(), req.Store, filter, options)
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("read starting with user failed: %v", err))
	}
	defer iter.Stop()

	for {
		tuple, err := iter.Next(stream.Context())
		if err != nil {
			if err == storage.ErrIteratorDone {
				return nil
			}
			return status.Error(codes.Internal, fmt.Sprintf("iterator error: %v", err))
		}

		if err := stream.Send(&storagev1.ReadResponse{Tuple: toStorageTuple(tuple)}); err != nil {
			return err
		}
	}
}

// IsReady implements StorageService.IsReady.
func (s *Server) IsReady(ctx context.Context, req *storagev1.IsReadyRequest) (*storagev1.IsReadyResponse, error) {
	readinessStatus, err := s.datastore.IsReady(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("is ready failed: %v", err))
	}

	return &storagev1.IsReadyResponse{
		Message: readinessStatus.Message,
		IsReady: readinessStatus.IsReady,
	}, nil
}
