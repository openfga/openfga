package server

import (
	"context"
	"errors"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (s *Server) Check(ctx context.Context, req *openfgapb.CheckRequest) (*openfgapb.CheckResponse, error) {
	tk := req.GetTupleKey()
	ctx, span := tracer.Start(ctx, "Check", trace.WithAttributes(
		attribute.KeyValue{Key: "object", Value: attribute.StringValue(tk.GetObject())},
		attribute.KeyValue{Key: "relation", Value: attribute.StringValue(tk.GetRelation())},
		attribute.KeyValue{Key: "user", Value: attribute.StringValue(tk.GetUser())},
	))
	defer span.End()

	if tk.GetUser() == "" || tk.GetRelation() == "" || tk.GetObject() == "" {
		return nil, serverErrors.InvalidCheckInput
	}

	storeID := req.GetStoreId()
	modelID := req.GetAuthorizationModelId()

	typesys, err := s.typesystemResolver(ctx, storeID, modelID)
	if err != nil {
		if errors.Is(err, typesystem.ErrModelNotFound) {
			return nil, serverErrors.AuthorizationModelNotFound(modelID)
		}
	}

	if commands.ProhibitModel1_0(typesys.GetSchemaVersion(), s.config.AllowEvaluating1_0Models) {
		return nil, serverErrors.ValidationError(commands.ErrObsoleteAuthorizationModel)
	}

	if err := validation.ValidateUserObjectRelation(typesys, tk); err != nil {
		return nil, serverErrors.ValidationError(err)
	}

	for _, ctxTuple := range req.GetContextualTuples().GetTupleKeys() {
		if err := validation.ValidateTuple(typesys, ctxTuple); err != nil {
			return nil, serverErrors.HandleTupleValidateError(err)
		}
	}

	checkResolver := graph.NewLocalChecker(
		storage.NewCombinedTupleReader(s.datastore, req.ContextualTuples.GetTupleKeys()),
		s.typesystemResolver,
		checkConcurrencyLimit,
	)

	resp, err := checkResolver.ResolveCheck(ctx, &graph.ResolveCheckRequest{
		StoreID:              req.GetStoreId(),
		AuthorizationModelID: req.GetAuthorizationModelId(),
		TupleKey:             req.GetTupleKey(),
		ContextualTuples:     req.ContextualTuples.GetTupleKeys(),
		ResolutionMetadata: &graph.ResolutionMetadata{
			Depth: s.config.ResolveNodeLimit,
		},
	})
	if err != nil {
		if errors.Is(err, graph.ErrResolutionDepthExceeded) {
			return nil, serverErrors.AuthorizationModelResolutionTooComplex
		}

		return nil, serverErrors.HandleError("", err)
	}

	res := &openfgapb.CheckResponse{
		Allowed: resp.Allowed,
	}

	span.SetAttributes(attribute.KeyValue{Key: "allowed", Value: attribute.BoolValue(res.GetAllowed())})
	return res, nil
}

type DecoratedCheckResolver struct {
	checker graph.CheckResolver
	ds      storage.OpenFGADatastore
}

func (s *Server) Expand(ctx context.Context, req *openfgapb.ExpandRequest) (*openfgapb.ExpandResponse, error) {
	tk := req.GetTupleKey()
	ctx, span := tracer.Start(ctx, "Expand", trace.WithAttributes(
		attribute.KeyValue{Key: "object", Value: attribute.StringValue(tk.GetObject())},
		attribute.KeyValue{Key: "relation", Value: attribute.StringValue(tk.GetRelation())},
		attribute.KeyValue{Key: "user", Value: attribute.StringValue(tk.GetUser())},
	))
	defer span.End()

	storeID := req.GetStoreId()

	modelID, err := s.resolveAuthorizationModelID(ctx, storeID, req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}

	q := commands.NewExpandQuery(s.datastore, s.logger, s.typesystemResolver, s.config.AllowEvaluating1_0Models)
	return q.Execute(ctx, &openfgapb.ExpandRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		TupleKey:             tk,
	})
}

func (s *Server) ListObjects(ctx context.Context, req *openfgapb.ListObjectsRequest) (*openfgapb.ListObjectsResponse, error) {

	targetObjectType := req.GetType()

	ctx, span := tracer.Start(ctx, "ListObjects", trace.WithAttributes(
		attribute.String("object_type", targetObjectType),
		attribute.String("relation", req.GetRelation()),
		attribute.String("user", req.GetUser()),
	))
	defer span.End()

	storeID := req.GetStoreId()
	modelID := req.GetAuthorizationModelId()

	typesys, err := s.typesystemResolver(ctx, storeID, modelID)
	if err != nil {
		if errors.Is(err, typesystem.ErrModelNotFound) {
			return nil, serverErrors.AuthorizationModelNotFound(modelID)
		}
	}

	if commands.ProhibitModel1_0(typesys.GetSchemaVersion(), s.config.AllowEvaluating1_0Models) {
		return nil, serverErrors.ValidationError(commands.ErrObsoleteAuthorizationModel)
	}

	q := &commands.ListObjectsQuery{
		Datastore:             s.datastore,
		Logger:                s.logger,
		ListObjectsDeadline:   s.config.ListObjectsDeadline,
		ListObjectsMaxResults: s.config.ListObjectsMaxResults,
		ResolveNodeLimit:      s.config.ResolveNodeLimit,
		TypesystemResolver:    s.typesystemResolver,
	}

	return q.Execute(ctx, &openfgapb.ListObjectsRequest{
		StoreId:              storeID,
		ContextualTuples:     req.GetContextualTuples(),
		AuthorizationModelId: modelID,
		Type:                 targetObjectType,
		Relation:             req.Relation,
		User:                 req.User,
	})
}

func (s *Server) StreamedListObjects(req *openfgapb.StreamedListObjectsRequest, srv openfgapb.OpenFGAService_StreamedListObjectsServer) error {
	ctx := srv.Context()
	ctx, span := tracer.Start(ctx, "StreamedListObjects", trace.WithAttributes(
		attribute.String("object_type", req.GetType()),
		attribute.String("relation", req.GetRelation()),
		attribute.String("user", req.GetUser()),
	))
	defer span.End()

	storeID := req.GetStoreId()

	modelID, err := s.resolveAuthorizationModelID(ctx, storeID, req.GetAuthorizationModelId())
	if err != nil {
		return err
	}

	model, err := s.datastore.ReadAuthorizationModel(ctx, storeID, modelID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return serverErrors.AuthorizationModelNotFound(req.GetAuthorizationModelId())
		}

		return serverErrors.HandleError("", err)
	}

	typesys := typesystem.New(model)

	for _, ctxTuple := range req.GetContextualTuples().GetTupleKeys() {
		if err := validation.ValidateTuple(typesys, ctxTuple); err != nil {
			return serverErrors.HandleTupleValidateError(err)
		}
	}

	q := &commands.ListObjectsQuery{
		Datastore:             s.datastore,
		Logger:                s.logger,
		ListObjectsDeadline:   s.config.ListObjectsDeadline,
		ListObjectsMaxResults: s.config.ListObjectsMaxResults,
		ResolveNodeLimit:      s.config.ResolveNodeLimit,
		TypesystemResolver:    s.typesystemResolver,
	}

	req.AuthorizationModelId = modelID
	return q.ExecuteStreamed(ctx, req, srv)
}
