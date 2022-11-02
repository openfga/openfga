package commands

import (
	"context"
	"fmt"
	"reflect"

	"github.com/openfga/openfga/pkg/logger"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/pkg/utils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/server/validation"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// ExpandQuery resolves a target TupleKey into a UsersetTree by expanding type definitions.
type ExpandQuery struct {
	logger    logger.Logger
	tracer    trace.Tracer
	datastore storage.OpenFGADatastore
}

// NewExpandQuery creates a new ExpandQuery using the supplied backends for retrieving data.
func NewExpandQuery(datastore storage.OpenFGADatastore, tracer trace.Tracer, logger logger.Logger) *ExpandQuery {
	return &ExpandQuery{logger: logger, tracer: tracer, datastore: datastore}
}

func (query *ExpandQuery) Execute(ctx context.Context, req *openfgapb.ExpandRequest) (*openfgapb.ExpandResponse, error) {
	store := req.GetStoreId()
	modelID := req.GetAuthorizationModelId()
	tupleKey := req.GetTupleKey()
	object := tupleKey.GetObject()
	relation := tupleKey.GetRelation()

	if object == "" || relation == "" {
		return nil, serverErrors.InvalidExpandInput
	}

	tk := tupleUtils.NewTupleKey(object, relation, "")
	metadata := utils.NewResolutionMetadata()

	userset, err := query.getUserset(ctx, store, modelID, tk, metadata)
	if err != nil {
		utils.LogDBStats(ctx, query.logger, "Expand", metadata.GetReadCalls(), 0)
		return nil, err
	}

	model, err := query.datastore.ReadAuthorizationModel(ctx, req.GetStoreId(), req.GetAuthorizationModelId())
	if err != nil {
		if err == storage.ErrNotFound {
			return nil, serverErrors.AuthorizationModelNotFound(req.GetAuthorizationModelId())
		}

		return nil, serverErrors.HandleError("", err)
	}

	typesys := typesystem.New(model)

	root, err := query.resolveUserset(ctx, store, modelID, userset, tk, metadata, typesys)
	if err != nil {
		utils.LogDBStats(ctx, query.logger, "Expand", metadata.GetReadCalls(), 0)
		return nil, err
	}
	utils.LogDBStats(ctx, query.logger, "Expand", metadata.GetReadCalls(), 0)

	return &openfgapb.ExpandResponse{
		Tree: &openfgapb.UsersetTree{
			Root: root,
		},
	}, nil
}

func (query *ExpandQuery) resolveUserset(
	ctx context.Context,
	store, modelID string,
	userset *openfgapb.Userset,
	tk *openfgapb.TupleKey,
	metadata *utils.ResolutionMetadata,
	typesys *typesystem.TypeSystem,
) (*openfgapb.UsersetTree_Node, error) {
	ctx, span := query.tracer.Start(ctx, "resolveUserset")
	defer span.End()

	switch us := userset.Userset.(type) {
	case nil, *openfgapb.Userset_This:
		return query.resolveThis(ctx, store, tk, metadata)
	case *openfgapb.Userset_ComputedUserset:
		return query.resolveComputedUserset(ctx, us.ComputedUserset, tk, metadata)
	case *openfgapb.Userset_TupleToUserset:
		return query.resolveTupleToUserset(ctx, store, modelID, us.TupleToUserset, tk, metadata, typesys)
	case *openfgapb.Userset_Union:
		return query.resolveUnionUserset(ctx, store, modelID, us.Union, tk, metadata, typesys)
	case *openfgapb.Userset_Difference:
		return query.resolveDifferenceUserset(ctx, store, modelID, us.Difference, tk, metadata, typesys)
	case *openfgapb.Userset_Intersection:
		return query.resolveIntersectionUserset(ctx, store, modelID, us.Intersection, tk, metadata, typesys)
	default:
		return nil, serverErrors.UnsupportedUserSet
	}
}

// resolveThis resolves a DirectUserset into a leaf node containing a distinct set of users with that relation.
func (query *ExpandQuery) resolveThis(ctx context.Context, store string, tk *openfgapb.TupleKey, metadata *utils.ResolutionMetadata) (*openfgapb.UsersetTree_Node, error) {
	ctx, span := query.tracer.Start(ctx, "resolveThis")
	defer span.End()

	metadata.AddReadCall()
	iter, err := query.datastore.Read(ctx, store, tk)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}
	defer iter.Stop()
	distinctUsers := make(map[string]bool)
	for {
		tuple, err := iter.Next()
		if err != nil {
			if err == storage.ErrIteratorDone {
				break
			}
			return nil, serverErrors.HandleError("", err)
		}
		distinctUsers[tuple.GetKey().GetUser()] = true
	}
	users := make([]string, 0, len(distinctUsers))
	for u := range distinctUsers {
		users = append(users, u)
	}
	return &openfgapb.UsersetTree_Node{
		Name: toObjectRelation(tk),
		Value: &openfgapb.UsersetTree_Node_Leaf{
			Leaf: &openfgapb.UsersetTree_Leaf{
				Value: &openfgapb.UsersetTree_Leaf_Users{
					Users: &openfgapb.UsersetTree_Users{
						Users: users,
					},
				},
			},
		},
	}, nil
}

// resolveComputedUserset builds a leaf node containing the result of resolving a ComputedUserset rewrite.
func (query *ExpandQuery) resolveComputedUserset(ctx context.Context, userset *openfgapb.ObjectRelation, tk *openfgapb.TupleKey, metadata *utils.ResolutionMetadata) (*openfgapb.UsersetTree_Node, error) {

	var span trace.Span
	_, span = query.tracer.Start(ctx, "resolveComputedUserset")
	defer span.End()

	computed := &openfgapb.TupleKey{
		Object:   userset.GetObject(),
		Relation: userset.GetRelation(),
	}
	if len(computed.Object) == 0 {
		computed.Object = tk.Object
	}
	if len(computed.Relation) == 0 {
		computed.Relation = tk.Relation
	}
	return &openfgapb.UsersetTree_Node{
		Name: toObjectRelation(tk),
		Value: &openfgapb.UsersetTree_Node_Leaf{
			Leaf: &openfgapb.UsersetTree_Leaf{
				Value: &openfgapb.UsersetTree_Leaf_Computed{
					Computed: &openfgapb.UsersetTree_Computed{
						Userset: toObjectRelation(computed),
					},
				},
			},
		},
	}, nil
}

// resolveTupleToUserset creates a new leaf node containing the result of expanding a TupleToUserset rewrite.
func (query *ExpandQuery) resolveTupleToUserset(
	ctx context.Context,
	store, modelID string,
	userset *openfgapb.TupleToUserset,
	tk *openfgapb.TupleKey,
	metadata *utils.ResolutionMetadata,
	typesys *typesystem.TypeSystem,
) (*openfgapb.UsersetTree_Node, error) {
	ctx, span := query.tracer.Start(ctx, "resolveTupleToUserset")
	defer span.End()

	targetObject := tk.GetObject()

	tupleset := userset.GetTupleset().GetRelation()

	objectType, _ := tupleUtils.SplitObject(targetObject)
	relation, ok := typesys.GetRelation(objectType, tupleset)
	if !ok {
		return nil, serverErrors.RelationNotFound(tupleset, objectType, tupleUtils.NewTupleKey(targetObject, tupleset, ""))
	}

	tuplesetRewrite := relation.GetRewrite().GetUserset()
	if tuplesetRewrite != nil && reflect.TypeOf(tuplesetRewrite) != reflect.TypeOf(&openfgapb.Userset_This{}) {
		query.logger.Warn(
			fmt.Sprintf("unexpected rewrite on tupleset relation '%s#%s'", objectType, tupleset),
			zap.String("store_id", store),
			zap.String("authorization_model_id", modelID),
			zap.String("object_type", objectType),
			zap.String("relation", tupleset),
		)

		return nil, serverErrors.InvalidAuthorizationModelInput(
			fmt.Errorf("unexpected rewrite on relation '%s#%s'", objectType, tupleset),
		)
	}

	tsKey := &openfgapb.TupleKey{
		Object:   targetObject,
		Relation: tupleset,
	}

	if tsKey.GetRelation() == "" {
		tsKey.Relation = tk.GetRelation()
	}

	metadata.AddReadCall()
	iter, err := query.datastore.Read(ctx, store, tsKey)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}
	defer iter.Stop()

	var computed []*openfgapb.UsersetTree_Computed
	seen := make(map[string]bool)
	for {
		tuple, err := iter.Next()
		if err != nil {
			if err == storage.ErrIteratorDone {
				break
			}
			return nil, serverErrors.HandleError("", err)
		}
		user := tuple.GetKey().GetUser()

		if user == Wildcard {
			objectType, _ := tupleUtils.SplitObject(targetObject)

			query.logger.WarnWithContext(
				ctx,
				fmt.Sprintf("unexpected wildcard evaluated on tupleset relation '%s'", tupleset),
				zap.String("store_id", store),
				zap.String("authorization_model_id", modelID),
				zap.String("object_type", objectType),
			)

			return nil, serverErrors.InvalidTuple(
				fmt.Sprintf("unexpected wildcard evaluated on relation '%s#%s'", objectType, tupleset),
				tupleUtils.NewTupleKey(targetObject, tupleset, Wildcard),
			)
		}

		// user must contain a type (i.e., be an object or userset)
		if tupleUtils.GetType(user) == "" {
			continue
		}

		tObject, tRelation := tupleUtils.SplitObjectRelation(user)
		// We only proceed in the case that tRelation == userset.GetComputedUserset().GetRelation().
		// tRelation may be empty, and in this case, we set it to userset.GetComputedUserset().GetRelation().
		if tRelation == "" {
			tRelation = userset.GetComputedUserset().GetRelation()
		}

		if tRelation != userset.GetComputedUserset().GetRelation() {
			continue
		}

		cs := &openfgapb.TupleKey{
			Object:   tObject,
			Relation: tRelation,
		}

		computedRelation := toObjectRelation(cs)
		if !seen[computedRelation] {
			computed = append(computed, &openfgapb.UsersetTree_Computed{Userset: computedRelation})
			seen[computedRelation] = true
		}
	}

	return &openfgapb.UsersetTree_Node{
		Name: toObjectRelation(tk),
		Value: &openfgapb.UsersetTree_Node_Leaf{
			Leaf: &openfgapb.UsersetTree_Leaf{
				Value: &openfgapb.UsersetTree_Leaf_TupleToUserset{
					TupleToUserset: &openfgapb.UsersetTree_TupleToUserset{
						Tupleset: toObjectRelation(tsKey),
						Computed: computed,
					},
				},
			},
		},
	}, nil
}

// resolveUnionUserset creates an intermediate Usertree node containing the union of its children.
func (query *ExpandQuery) resolveUnionUserset(
	ctx context.Context,
	store, modelID string,
	usersets *openfgapb.Usersets,
	tk *openfgapb.TupleKey,
	metadata *utils.ResolutionMetadata,
	typesys *typesystem.TypeSystem,
) (*openfgapb.UsersetTree_Node, error) {
	ctx, span := query.tracer.Start(ctx, "resolveUnionUserset")
	defer span.End()

	nodes, err := query.resolveUsersets(ctx, store, modelID, usersets.Child, tk, metadata, typesys)
	if err != nil {
		return nil, err
	}
	return &openfgapb.UsersetTree_Node{
		Name: toObjectRelation(tk),
		Value: &openfgapb.UsersetTree_Node_Union{
			Union: &openfgapb.UsersetTree_Nodes{
				Nodes: nodes,
			},
		},
	}, nil
}

// resolveIntersectionUserset create an intermediate Usertree node containing the intersection of its children
func (query *ExpandQuery) resolveIntersectionUserset(
	ctx context.Context,
	store, modelID string,
	usersets *openfgapb.Usersets,
	tk *openfgapb.TupleKey,
	metadata *utils.ResolutionMetadata,
	typesys *typesystem.TypeSystem,
) (*openfgapb.UsersetTree_Node, error) {
	ctx, span := query.tracer.Start(ctx, "resolveIntersectionUserset")
	defer span.End()

	nodes, err := query.resolveUsersets(ctx, store, modelID, usersets.Child, tk, metadata, typesys)
	if err != nil {
		return nil, err
	}
	return &openfgapb.UsersetTree_Node{
		Name: toObjectRelation(tk),
		Value: &openfgapb.UsersetTree_Node_Intersection{
			Intersection: &openfgapb.UsersetTree_Nodes{
				Nodes: nodes,
			},
		},
	}, nil
}

// resolveDifferenceUserset creates and intermediate Usertree node containing the difference of its children
func (query *ExpandQuery) resolveDifferenceUserset(
	ctx context.Context,
	store, modelID string,
	userset *openfgapb.Difference,
	tk *openfgapb.TupleKey,
	metadata *utils.ResolutionMetadata,
	typesys *typesystem.TypeSystem,
) (*openfgapb.UsersetTree_Node, error) {
	ctx, span := query.tracer.Start(ctx, "resolveDifferenceUserset")
	defer span.End()

	nodes, err := query.resolveUsersets(ctx, store, modelID, []*openfgapb.Userset{userset.Base, userset.Subtract}, tk, metadata, typesys)
	if err != nil {
		return nil, err
	}
	base := nodes[0]
	subtract := nodes[1]
	return &openfgapb.UsersetTree_Node{
		Name: toObjectRelation(tk),
		Value: &openfgapb.UsersetTree_Node_Difference{
			Difference: &openfgapb.UsersetTree_Difference{
				Base:     base,
				Subtract: subtract,
			},
		},
	}, nil
}

// resolveUsersets creates Usertree nodes for multiple Usersets
func (query *ExpandQuery) resolveUsersets(
	ctx context.Context,
	store, modelID string,
	usersets []*openfgapb.Userset,
	tk *openfgapb.TupleKey,
	metadata *utils.ResolutionMetadata,
	typesys *typesystem.TypeSystem,
) ([]*openfgapb.UsersetTree_Node, error) {
	ctx, span := query.tracer.Start(ctx, "resolveUsersets")
	defer span.End()

	out := make([]*openfgapb.UsersetTree_Node, len(usersets))
	grp, ctx := errgroup.WithContext(ctx)
	for i, us := range usersets {
		// https://golang.org/doc/faq#closures_and_goroutines
		i, us := i, us
		grp.Go(func() error {
			node, err := query.resolveUserset(ctx, store, modelID, us, tk, metadata, typesys)
			if err != nil {
				return err
			}
			out[i] = node
			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}

// getUserset retrieves the authorizationModel configuration for a supplied TupleKey.
func (query *ExpandQuery) getUserset(ctx context.Context, store, modelID string, tk *openfgapb.TupleKey, metadata *utils.ResolutionMetadata) (*openfgapb.Userset, error) {
	ctx, span := query.tracer.Start(ctx, "getUserset")
	defer span.End()

	userset, err := validation.ValidateObjectsRelations(ctx, query.datastore, store, modelID, tk, metadata)
	if err != nil {
		return nil, serverErrors.HandleTupleValidateError(err)
	}
	return userset, nil
}

func toObjectRelation(tk *openfgapb.TupleKey) string {
	return tupleUtils.ToObjectRelationString(tk.GetObject(), tk.GetRelation())
}
