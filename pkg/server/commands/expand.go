package commands

import (
	"context"
	"errors"

	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"golang.org/x/sync/errgroup"
)

// ExpandQuery resolves a target TupleKey into a UsersetTree by expanding type definitions.
type ExpandQuery struct {
	logger    logger.Logger
	datastore storage.OpenFGADatastore
}

// NewExpandQuery creates a new ExpandQuery using the supplied backends for retrieving data.
func NewExpandQuery(datastore storage.OpenFGADatastore, logger logger.Logger) *ExpandQuery {
	return &ExpandQuery{logger: logger, datastore: datastore}
}

func (q *ExpandQuery) Execute(ctx context.Context, req *openfgapb.ExpandRequest) (*openfgapb.ExpandResponse, error) {
	store := req.GetStoreId()
	modelID := req.GetAuthorizationModelId()
	tupleKey := req.GetTupleKey()
	object := tupleKey.GetObject()
	relation := tupleKey.GetRelation()

	if object == "" || relation == "" {
		return nil, serverErrors.InvalidExpandInput
	}

	tk := tupleUtils.NewTupleKey(object, relation, "")

	model, err := q.datastore.ReadAuthorizationModel(ctx, store, modelID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, serverErrors.AuthorizationModelNotFound(modelID)
		}

		return nil, serverErrors.HandleError("", err)
	}

	if !typesystem.IsSchemaVersionSupported(model.GetSchemaVersion()) {
		return nil, serverErrors.ValidationError(typesystem.ErrInvalidSchemaVersion)
	}

	typesys, err := typesystem.NewAndValidate(ctx, model)
	if err != nil {
		return nil, serverErrors.ValidationError(typesystem.ErrInvalidModel)
	}

	if err = validation.ValidateObject(typesys, tk); err != nil {
		return nil, serverErrors.ValidationError(err)
	}

	err = validation.ValidateRelation(typesys, tk)
	if err != nil {
		return nil, serverErrors.ValidationError(err)
	}

	objectType := tupleUtils.GetType(object)
	rel, err := typesys.GetRelation(objectType, relation)
	if err != nil {
		if errors.Is(err, typesystem.ErrObjectTypeUndefined) {
			return nil, serverErrors.TypeNotFound(objectType)
		}

		if errors.Is(err, typesystem.ErrRelationUndefined) {
			return nil, serverErrors.RelationNotFound(relation, objectType, tk)
		}

		return nil, serverErrors.HandleError("", err)
	}

	userset := rel.GetRewrite()

	root, err := q.resolveUserset(ctx, store, userset, tk, typesys)
	if err != nil {
		return nil, err
	}

	return &openfgapb.ExpandResponse{
		Tree: &openfgapb.UsersetTree{
			Root: root,
		},
	}, nil
}

func (q *ExpandQuery) resolveUserset(
	ctx context.Context,
	store string,
	userset *openfgapb.Userset,
	tk *openfgapb.TupleKey,
	typesys *typesystem.TypeSystem,
) (*openfgapb.UsersetTree_Node, error) {
	ctx, span := tracer.Start(ctx, "resolveUserset")
	defer span.End()

	switch us := userset.Userset.(type) {
	case nil, *openfgapb.Userset_This:
		return q.resolveThis(ctx, store, tk, typesys)
	case *openfgapb.Userset_ComputedUserset:
		return q.resolveComputedUserset(ctx, us.ComputedUserset, tk)
	case *openfgapb.Userset_TupleToUserset:
		return q.resolveTupleToUserset(ctx, store, us.TupleToUserset, tk, typesys)
	case *openfgapb.Userset_Union:
		return q.resolveUnionUserset(ctx, store, us.Union, tk, typesys)
	case *openfgapb.Userset_Difference:
		return q.resolveDifferenceUserset(ctx, store, us.Difference, tk, typesys)
	case *openfgapb.Userset_Intersection:
		return q.resolveIntersectionUserset(ctx, store, us.Intersection, tk, typesys)
	default:
		return nil, serverErrors.UnsupportedUserSet
	}
}

// resolveThis resolves a DirectUserset into a leaf node containing a distinct set of users with that relation.
func (q *ExpandQuery) resolveThis(ctx context.Context, store string, tk *openfgapb.TupleKey, typesys *typesystem.TypeSystem) (*openfgapb.UsersetTree_Node, error) {
	ctx, span := tracer.Start(ctx, "resolveThis")
	defer span.End()

	tupleIter, err := q.datastore.Read(ctx, store, tk)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	filteredIter := storage.NewFilteredTupleKeyIterator(
		storage.NewTupleKeyIteratorFromTupleIterator(tupleIter),
		validation.FilterInvalidTuples(typesys),
	)
	defer filteredIter.Stop()

	distinctUsers := make(map[string]bool)
	for {
		tk, err := filteredIter.Next()
		if err != nil {
			if err == storage.ErrIteratorDone {
				break
			}
			return nil, serverErrors.HandleError("", err)
		}
		distinctUsers[tk.GetUser()] = true
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
func (q *ExpandQuery) resolveComputedUserset(ctx context.Context, userset *openfgapb.ObjectRelation, tk *openfgapb.TupleKey) (*openfgapb.UsersetTree_Node, error) {
	_, span := tracer.Start(ctx, "resolveComputedUserset")
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
func (q *ExpandQuery) resolveTupleToUserset(
	ctx context.Context,
	store string,
	userset *openfgapb.TupleToUserset,
	tk *openfgapb.TupleKey,
	typesys *typesystem.TypeSystem,
) (*openfgapb.UsersetTree_Node, error) {
	ctx, span := tracer.Start(ctx, "resolveTupleToUserset")
	defer span.End()

	targetObject := tk.GetObject()

	tupleset := userset.GetTupleset().GetRelation()

	objectType := tupleUtils.GetType(targetObject)
	_, err := typesys.GetRelation(objectType, tupleset)
	if err != nil {
		if errors.Is(err, typesystem.ErrObjectTypeUndefined) {
			return nil, serverErrors.TypeNotFound(objectType)
		}

		if errors.Is(err, typesystem.ErrRelationUndefined) {
			return nil, serverErrors.RelationNotFound(tupleset, objectType, tupleUtils.NewTupleKey(tk.Object, tupleset, tk.User))
		}
	}

	tsKey := &openfgapb.TupleKey{
		Object:   targetObject,
		Relation: tupleset,
	}

	if tsKey.GetRelation() == "" {
		tsKey.Relation = tk.GetRelation()
	}

	tupleIter, err := q.datastore.Read(ctx, store, tsKey)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	filteredIter := storage.NewFilteredTupleKeyIterator(
		storage.NewTupleKeyIteratorFromTupleIterator(tupleIter),
		validation.FilterInvalidTuples(typesys),
	)
	defer filteredIter.Stop()

	var computed []*openfgapb.UsersetTree_Computed
	seen := make(map[string]bool)
	for {
		tk, err := filteredIter.Next()
		if err != nil {
			if err == storage.ErrIteratorDone {
				break
			}
			return nil, serverErrors.HandleError("", err)
		}
		user := tk.GetUser()

		tObject, tRelation := tupleUtils.SplitObjectRelation(user)
		// We only proceed in the case that tRelation == userset.GetComputedUserset().GetRelation().
		// tRelation may be empty, and in this case, we set it to userset.GetComputedUserset().GetRelation().
		if tRelation == "" {
			tRelation = userset.GetComputedUserset().GetRelation()
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
func (q *ExpandQuery) resolveUnionUserset(
	ctx context.Context,
	store string,
	usersets *openfgapb.Usersets,
	tk *openfgapb.TupleKey,
	typesys *typesystem.TypeSystem,
) (*openfgapb.UsersetTree_Node, error) {
	ctx, span := tracer.Start(ctx, "resolveUnionUserset")
	defer span.End()

	nodes, err := q.resolveUsersets(ctx, store, usersets.Child, tk, typesys)
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
func (q *ExpandQuery) resolveIntersectionUserset(
	ctx context.Context,
	store string,
	usersets *openfgapb.Usersets,
	tk *openfgapb.TupleKey,
	typesys *typesystem.TypeSystem,
) (*openfgapb.UsersetTree_Node, error) {
	ctx, span := tracer.Start(ctx, "resolveIntersectionUserset")
	defer span.End()

	nodes, err := q.resolveUsersets(ctx, store, usersets.Child, tk, typesys)
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
func (q *ExpandQuery) resolveDifferenceUserset(
	ctx context.Context,
	store string,
	userset *openfgapb.Difference,
	tk *openfgapb.TupleKey,
	typesys *typesystem.TypeSystem,
) (*openfgapb.UsersetTree_Node, error) {
	ctx, span := tracer.Start(ctx, "resolveDifferenceUserset")
	defer span.End()

	nodes, err := q.resolveUsersets(ctx, store, []*openfgapb.Userset{userset.Base, userset.Subtract}, tk, typesys)
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
func (q *ExpandQuery) resolveUsersets(
	ctx context.Context,
	store string,
	usersets []*openfgapb.Userset,
	tk *openfgapb.TupleKey,
	typesys *typesystem.TypeSystem,
) ([]*openfgapb.UsersetTree_Node, error) {
	ctx, span := tracer.Start(ctx, "resolveUsersets")
	defer span.End()

	out := make([]*openfgapb.UsersetTree_Node, len(usersets))
	grp, ctx := errgroup.WithContext(ctx)
	for i, us := range usersets {
		// https://golang.org/doc/faq#closures_and_goroutines
		i, us := i, us
		grp.Go(func() error {
			node, err := q.resolveUserset(ctx, store, us, tk, typesys)
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

func toObjectRelation(tk *openfgapb.TupleKey) string {
	return tupleUtils.ToObjectRelationString(tk.GetObject(), tk.GetRelation())
}
