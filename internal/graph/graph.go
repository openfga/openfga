package graph

import (
	"context"
	"errors"
	"fmt"

	"github.com/openfga/openfga/pkg/typesystem"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type ctxKey string

const (
	resolutionDepthCtxKey ctxKey = "resolution-depth"
)

var (
	ErrTargetError    = errors.New("graph: target incorrectly specified")
	ErrNotImplemented = errors.New("graph: intersection and exclusion are not yet implemented")
)

// ContextWithResolutionDepth attaches the provided graph resolution depth to the parent context.
func ContextWithResolutionDepth(parent context.Context, depth uint32) context.Context {
	return context.WithValue(parent, resolutionDepthCtxKey, depth)
}

// ResolutionDepthFromContext returns the current graph resolution depth from the provided context (if any).
func ResolutionDepthFromContext(ctx context.Context) (uint32, bool) {
	depth, ok := ctx.Value(resolutionDepthCtxKey).(uint32)
	return depth, ok
}

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

// RelationshipIngresses computes the incoming edges (ingresses) that are possible between the target relation reference
// and the source relational reference.
//
// To look up Ingresses(`document#viewer`, `source`), where `source` is an object type with no relation, find the rewrite and types of viewer in the document type:
// 1. If `source` is a directly related type then add `source, direct` to the result.
// 2. If `objectType#relation` is a type and `objectType#relation` can be a `source` then add `objectType#relation, direct` to the result.
// 3. If computed userset, say `define viewer as writer`, then recurse on `document#writer, source`.
// 4. If tuple-to-userset, say, viewer from parent. Go to parent and find its types. In this case, `folder`. Go to `folder` and see if it has a `viewer` relation. If so, recurse on `folder#viewer, source`.
//
// To look up Ingresses(`document#viewer`, `folder#viewer`), find the rewrite and relations of viewer in the document type:
// 1. If `folder#viewer` is a relational type then add `folder#viewer, direct` to the result.
// 2. If computed userset, say `define viewer as writer`, then recurse on `document#writer, folder#viewer`.
// 3. If tuple-to-userset, say, viewer from parent. Go to parent and find its related types.
//  1. If parent's types includes `folder` type, and `folder` contains `viewer` relation then this is exactly a ttu rewrite.
//  2. Otherwise, suppose the types contains `objectType` which has a relation `viewer`, then recurse on `objectType#viewer, folder#viewer`
func (g *ConnectedObjectGraph) RelationshipIngresses(target *openfgapb.RelationReference, source *openfgapb.RelationReference) ([]*RelationshipIngress, error) {
	return g.findIngresses(target, source, map[string]struct{}{})
}

func (g *ConnectedObjectGraph) findIngresses(target *openfgapb.RelationReference, source *openfgapb.RelationReference, visited map[string]struct{}) ([]*RelationshipIngress, error) {
	key := fmt.Sprintf("%s#%s", target.GetType(), target.GetRelation())
	if _, ok := visited[key]; ok {
		// We've already visited the target so no need to do so again.
		return nil, nil
	}
	visited[key] = struct{}{}

	relation, err := g.typesystem.GetRelation(target.GetType(), target.GetRelation())
	if err != nil {
		return nil, err
	}

	return g.findIngressesWithRewrite(target, source, relation.GetRewrite(), visited)
}

// findIngressesWithRewrite is what we use for recursive calls on the rewrites, particularly union where we don't
// update the target and source, and only the rewrite.
func (g *ConnectedObjectGraph) findIngressesWithRewrite(
	target *openfgapb.RelationReference,
	source *openfgapb.RelationReference,
	rewrite *openfgapb.Userset,
	visited map[string]struct{},
) ([]*RelationshipIngress, error) {
	switch t := rewrite.GetUserset().(type) {
	case *openfgapb.Userset_This:
		var res []*RelationshipIngress

		directlyRelated, _ := g.typesystem.IsDirectlyRelated(target, source)
		publiclyAssignable, _ := g.typesystem.IsPubliclyAssignable(target, source.GetType())

		if directlyRelated || publiclyAssignable {
			res = append(res, &RelationshipIngress{
				Type:    DirectIngress,
				Ingress: typesystem.DirectRelationReference(target.GetType(), target.GetRelation()),
			})
		}

		typeRestrictions, _ := g.typesystem.GetDirectlyRelatedUserTypes(target.GetType(), target.GetRelation())

		for _, typeRestriction := range typeRestrictions {
			if typeRestriction.GetRelation() != "" {
				// recursively sub-collect any ingresses for the provided type (or any
				// descendant derived from it) that has an ingress with the 'source'
				ingresses, err := g.findIngresses(typeRestriction, source, visited)
				if err != nil {
					return nil, err
				}

				res = append(res, ingresses...)
			}
		}

		return res, nil
	case *openfgapb.Userset_ComputedUserset:
		return g.findIngresses(
			typesystem.DirectRelationReference(target.GetType(), t.ComputedUserset.GetRelation()),
			source,
			visited,
		)
	case *openfgapb.Userset_TupleToUserset:
		tupleset := t.TupleToUserset.GetTupleset().GetRelation()
		computedUserset := t.TupleToUserset.GetComputedUserset().GetRelation()

		var res []*RelationshipIngress

		tuplesetTypeRestrictions, _ := g.typesystem.GetDirectlyRelatedUserTypes(target.GetType(), tupleset)

		for _, typeRestriction := range tuplesetTypeRestrictions {

			r, err := g.typesystem.GetRelation(typeRestriction.GetType(), computedUserset)

			var directlyAssignable bool
			if typeRestriction.GetType() == source.GetType() {
				directlyAssignable = g.typesystem.IsDirectlyAssignable(r)
			}

			if directlyAssignable {
				res = append(res, &RelationshipIngress{
					Type:             TupleToUsersetIngress,
					Ingress:          typesystem.DirectRelationReference(target.GetType(), target.GetRelation()),
					TuplesetRelation: typesystem.DirectRelationReference(target.GetType(), tupleset),
				})
			}

			if err == nil {
				subResults, err := g.findIngresses(
					typesystem.DirectRelationReference(typeRestriction.GetType(), computedUserset),
					source,
					visited,
				)
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
			childResults, err := g.findIngressesWithRewrite(target, source, child, visited)
			if err != nil {
				return nil, err
			}
			res = append(res, childResults...)
		}
		return res, nil
	case *openfgapb.Userset_Intersection, *openfgapb.Userset_Difference:
		return nil, ErrNotImplemented
	default:
		panic("unexpected userset rewrite encountered")
	}
}
