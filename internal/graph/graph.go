package graph

import (
	"github.com/openfga/openfga/pkg/typesystem"

	"github.com/go-errors/errors"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

var (
	ErrToFix          = errors.New("TODO: this should never actually fail so what to say")
	ErrNotImplemented = errors.New("intersection and exclusion are not yet implemented")
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

// RelationshipIngresses computes the incoming edges (ingresses) that are possible
// between the source object and relation and the target user (user or userset).
func (g *ConnectedObjectGraph) RelationshipIngresses(target *openfgapb.RelationReference, source *openfgapb.RelationReference) ([]*RelationshipIngress, error) {
	relation, ok := g.typesystem.GetRelation(target.GetType(), target.GetRelation())
	if !ok {
		return nil, ErrToFix
	}
	relatedUserTypes := relation.GetTypeInfo().GetDirectlyRelatedUserTypes()

	var res []*RelationshipIngress

	if contains(relatedUserTypes, source) {
		res = append(res, &RelationshipIngress{
			Type: DirectIngress,
			Ingress: &openfgapb.RelationReference{
				Type:     target.GetType(),
				Relation: target.GetRelation(),
			},
		})
	}

	for _, relatedUserType := range relatedUserTypes {
		if relatedUserType.GetRelation() != "" {
			if r, ok := g.typesystem.GetRelation(relatedUserType.GetType(), relatedUserType.GetRelation()); ok {
				if contains(r.GetTypeInfo().GetDirectlyRelatedUserTypes(), source) {
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
	}

	ingresses, err := g.relationshipIngressesHelper(target, source, relation.GetRewrite())
	if err != nil {
		return nil, err
	}

	return append(res, ingresses...), nil
}

// relationshipIngressesHelper is what we use for recursive calls on the rewrites
func (g *ConnectedObjectGraph) relationshipIngressesHelper(target *openfgapb.RelationReference, source *openfgapb.RelationReference, rewrite *openfgapb.Userset) ([]*RelationshipIngress, error) {
	switch t := rewrite.GetUserset().(type) {
	case *openfgapb.Userset_This:
		return []*RelationshipIngress{}, nil
	case *openfgapb.Userset_ComputedUserset:
		return g.RelationshipIngresses(&openfgapb.RelationReference{
			Type:     target.GetType(),
			Relation: t.ComputedUserset.GetRelation(),
		},
			source)
	case *openfgapb.Userset_TupleToUserset:
		tupleset := t.TupleToUserset.GetTupleset().GetRelation()
		computedUserset := t.TupleToUserset.GetComputedUserset().GetRelation()
		tuplesetRelation, ok := g.typesystem.GetRelation(target.GetType(), tupleset)
		if !ok {
			return nil, ErrToFix
		}

		relatedUserTypes := tuplesetRelation.GetTypeInfo().GetDirectlyRelatedUserTypes()
		var res []*RelationshipIngress

		// We need to check if this is a tuple-to-userset rewrite
		// parent: [folder#viewer] or parent: [folder]... I need to make better comments
		if contains(tuplesetRelation.GetTypeInfo().GetDirectlyRelatedUserTypes(), source) || contains(tuplesetRelation.GetTypeInfo().GetDirectlyRelatedUserTypes(), &openfgapb.RelationReference{Type: source.GetType()}) {
			res = append(res, &RelationshipIngress{
				Type: TupleToUsersetIngress,
				Ingress: &openfgapb.RelationReference{
					Type:     target.GetType(),
					Relation: target.GetRelation(),
				},
				TuplesetRelation: &openfgapb.RelationReference{
					Type:     target.GetType(),
					Relation: tuplesetRelation.GetName(),
				},
			})
		}

		for _, relatedUserType := range relatedUserTypes {
			if _, ok := g.typesystem.GetRelation(relatedUserType.GetType(), computedUserset); ok {
				subResults, err := g.RelationshipIngresses(&openfgapb.RelationReference{
					Type:     relatedUserType.GetType(),
					Relation: computedUserset,
				}, source)
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
			childResults, err := g.relationshipIngressesHelper(target, source, child)
			if err != nil {
				return nil, err
			}
			res = append(res, childResults...)
		}
		return res, nil
	}

	return nil, ErrNotImplemented
}

func contains(relationReferences []*openfgapb.RelationReference, target *openfgapb.RelationReference) bool {
	for _, relationReference := range relationReferences {
		if target.GetType() == relationReference.GetType() && target.GetRelation() == relationReference.GetRelation() {
			return true
		}
	}

	return false
}
