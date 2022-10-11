package graph

import (
	"fmt"

	"github.com/go-errors/errors"
	"github.com/openfga/openfga/pkg/typesystem"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

var (
	ErrTargetError    = errors.New("graph: target incorrectly specified")
	ErrNotImplemented = errors.New("graph: intersection and exclusion are not yet implemented")
)

// RelationshipIngressType is used to define an enum of the type of ingresses between
// source object references and target user references that exist in the graph of
// relationships.
type RelationshipIngressType int

const (
	// DirectIngress defines a direct ingress connection between a source object reference
	// and some target user reference.
	DirectIngress RelationshipIngressType = iota
	TupleToUsersetIngress
)

// RelationshipIngress represents a possible ingress point between some source object reference
// and a target user reference.
type RelationshipIngress struct {

	// The type of the relationship ingress
	Type RelationshipIngressType

	// The relationship reference that defines the ingress to some target relation
	Ingress *openfgapb.RelationReference

	// TuplesetRelation defines the tupleset relation reference that relates a source
	// object reference with a target if the type of the relationship ingress is that
	// of a TupleToUserset
	TuplesetRelation *openfgapb.RelationReference
}

// ConnectedObjectGraph represents a graph of relationships and the connectivity between
// object and relation references within the graph through direct or indirect relationships.
// The ConnectedObjectGraph should be used to introspect what kind of relationships between
// object types can exist.
type ConnectedObjectGraph struct {
	typesystem *typesystem.TypeSystem
}

// BuildConnectedObjectGraph builds an object graph representing the graph of relationships between connected
// object types either through direct or indirect relationships.
func BuildConnectedObjectGraph(typesystem *typesystem.TypeSystem) *ConnectedObjectGraph {
	return &ConnectedObjectGraph{
		typesystem: typesystem,
	}
}

// RelationshipIngresses computes the incoming edges (ingresses) that are possible between the source object and
// relation and the target user (user or userset).
//
// To look up Ingresses(`document#viewer`, `source`), where `source` is an object type with no relation, find the rewrites and types of viewer in the document type:
// 1. If `source` is a relational type then add `source, direct` to the result.
// 2. If `objectType#relation` is a type and `objectType#relation` can be a `source` then add `objectType#relation, direct` to the result.
// 3. If computed userset, say `define viewer as writer`, then recurse on `document#writer, source`.
// 4. If tuple-to-userset, say, viewer from parent. Go to parent and find its types. In this case, `folder`. Go to `folder` and see if it has a `viewer` relation. If so, recurse on `folder#viewer, source`.
//
// To look up Ingresses(`document#viewer`, `folder#viewer`), find the rewrites and relations of viewer in the document type:
// 1. If `folder#viewer` is a relational type then add `folder#viewer, direct` to the result.
// 2. If computed userset, say `define viewer as writer`, then recurse on `document#writer, folder#viewer`.
// 3. If tuple-to-userset, say, viewer from parent. Go to parent and find its related types.
//  1. If parent's types includes `folder` type, and `folder` contains `viewer` relation then this is exactly a ttu rewrite.
//  2. Otherwise, suppose the types contains `objectType` which has a relation `viewer`, then recurse on `objectType#viewer, folder#viewer`
func (g *ConnectedObjectGraph) RelationshipIngresses(target *openfgapb.RelationReference, source *openfgapb.RelationReference) ([]*RelationshipIngress, error) {
	return g.relationshipIngressesHelper(target, source, map[string]struct{}{})
}

func (g *ConnectedObjectGraph) relationshipIngressesHelper(target *openfgapb.RelationReference, source *openfgapb.RelationReference, visited map[string]struct{}) ([]*RelationshipIngress, error) {
	key := fmt.Sprintf("%s#%s", target.GetType(), target.GetRelation())
	if _, ok := visited[key]; ok {
		// We've already visited the target so no need to do so again.
		return nil, nil
	}
	visited[key] = struct{}{}

	var res []*RelationshipIngress

	if ok := g.typesystem.IsDirectlyRelated(target, source); ok {
		res = append(res, &RelationshipIngress{
			Type: DirectIngress,
			Ingress: &openfgapb.RelationReference{
				Type:     target.GetType(),
				Relation: target.GetRelation(),
			},
		})
	}

	relation, ok := g.typesystem.GetRelation(target.GetType(), target.GetRelation())
	if !ok {
		return nil, ErrTargetError
	}

	for _, relatedUserType := range relation.GetTypeInfo().GetDirectlyRelatedUserTypes() {
		if relatedUserType.GetRelation() != "" {
			if ok := g.typesystem.IsDirectlyRelated(relatedUserType, source); ok {
				key := fmt.Sprintf("%s#%s", relatedUserType.GetType(), relatedUserType.GetRelation())
				if _, ok := visited[key]; ok {
					continue
				}
				visited[key] = struct{}{}

				res = append(res, &RelationshipIngress{
					Type: DirectIngress,
					Ingress: &openfgapb.RelationReference{
						Type:     relatedUserType.GetType(),
						Relation: relatedUserType.GetRelation(),
					},
				})
			}
		}
	}

	ingresses, err := g.relationshipIngressesResolveRewrite(target, source, relation.GetRewrite(), visited)
	if err != nil {
		return nil, err
	}

	return append(res, ingresses...), nil
}

// relationshipIngressesResolveRewrite is what we use for recursive calls on the rewrites
func (g *ConnectedObjectGraph) relationshipIngressesResolveRewrite(target *openfgapb.RelationReference, source *openfgapb.RelationReference, rewrite *openfgapb.Userset, visited map[string]struct{}) ([]*RelationshipIngress, error) {
	switch t := rewrite.GetUserset().(type) {
	case *openfgapb.Userset_This:
		return []*RelationshipIngress{}, nil
	case *openfgapb.Userset_ComputedUserset:
		return g.relationshipIngressesHelper(&openfgapb.RelationReference{
			Type:     target.GetType(),
			Relation: t.ComputedUserset.GetRelation(),
		},
			source, visited)
	case *openfgapb.Userset_TupleToUserset:
		tupleset := t.TupleToUserset.GetTupleset().GetRelation()
		computedUserset := t.TupleToUserset.GetComputedUserset().GetRelation()
		tuplesetRelation, ok := g.typesystem.GetRelation(target.GetType(), tupleset)
		if !ok {
			return nil, ErrTargetError
		}

		var res []*RelationshipIngress

		// We need to check if this is a tuple-to-userset rewrite
		// parent: [folder#viewer] or parent: [folder]... I need to make better comments
		relationReference := &openfgapb.RelationReference{
			Type:     target.GetType(),
			Relation: tupleset,
		}
		if g.typesystem.IsDirectlyRelated(relationReference, source) || g.typesystem.IsDirectlyRelated(relationReference, &openfgapb.RelationReference{Type: source.GetType()}) {
			res = append(res, &RelationshipIngress{
				Type: TupleToUsersetIngress,
				Ingress: &openfgapb.RelationReference{
					Type:     target.GetType(),
					Relation: target.GetRelation(),
				},
				TuplesetRelation: &openfgapb.RelationReference{
					Type:     target.GetType(),
					Relation: tupleset,
				},
			})
		}

		for _, relatedUserType := range tuplesetRelation.GetTypeInfo().GetDirectlyRelatedUserTypes() {
			if _, ok := g.typesystem.GetRelation(relatedUserType.GetType(), computedUserset); ok {
				subResults, err := g.relationshipIngressesHelper(&openfgapb.RelationReference{
					Type:     relatedUserType.GetType(),
					Relation: computedUserset,
				}, source, visited)
				if err != nil {
					return nil, err
				}
				res = append(res, subResults...)
			}
		}

		return res, nil
	case *openfgapb.Userset_Union:
		var res []*RelationshipIngress
		for _, child := range t.Union.GetChild() {
			childResults, err := g.relationshipIngressesResolveRewrite(target, source, child, visited)
			if err != nil {
				return nil, err
			}
			res = append(res, childResults...)
		}
		return res, nil
	}

	return nil, ErrNotImplemented
}
