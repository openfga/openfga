package graph

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type ctxKey string

const (
	resolutionDepthCtxKey ctxKey = "resolution-depth"
)

var (
	ErrResolutionDepthExceeded = errors.New("resolution depth exceeded")
	ErrTargetError             = errors.New("graph: target incorrectly specified")
	ErrNotImplemented          = errors.New("graph: intersection and exclusion are not yet implemented")
)

type findIngressOption int

const (
	resolveAllIngresses findIngressOption = iota
	resolveAnyIngress
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

type ResolutionMetadata struct {
	Depth uint32
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
	ComputedUsersetIngress
)

func (r RelationshipIngressType) String() string {
	switch r {
	case DirectIngress:
		return "direct"
	case ComputedUsersetIngress:
		return "computed_userset"
	case TupleToUsersetIngress:
		return "ttu"
	default:
		return "undefined"
	}
}

type IngressCondition int

const (

	// RequiresFurtherEvalCondition indicates an ingress condition whereby results expanded
	// under such an ingress require further Check evaluation before a determination of the
	// outcome can be made.
	//
	// Relationships involving intersection ('and') and/or exclusion ('but not') fall under
	// ingresses with this condition.
	RequiresFurtherEvalCondition IngressCondition = iota

	// NoFurtherEvalCondition indicates an ingress condition whereby results are factual and
	// known to be true and require no further evaluation before a determination of the outcome
	// can be made.
	NoFurtherEvalCondition
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

	Condition IngressCondition
}

func (r RelationshipIngress) String() string {
	val := ""
	if r.TuplesetRelation != nil {
		val = fmt.Sprintf("ingress %s, type %s, tupleset %s", r.Ingress.String(), r.Type.String(), r.TuplesetRelation.String())
	} else {
		val = fmt.Sprintf("ingress %s, type %s", r.Ingress.String(), r.Type.String())
	}
	return strings.ReplaceAll(val, "  ", " ")
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
// To look up Ingresses(`document#viewer`, `source`), where `source` is a type with no relation, look up the definition of relation `document#viewer` and:
// 1. If `source` is a directly related type then add `source, direct` to the result.
// 2. If `objectType#relation` is a type and `objectType#relation` can be a `source` then add `objectType#relation, direct` to the result.
// 3. If computed userset, say `define viewer as writer`, then recurse on `document#writer, source`.
// 4. If tuple-to-userset, say, `define viewer as viewer from parent`. Go to parent and find its types. In this case, `folder`. Go to `folder` and see if it has a `viewer` relation. If so, recurse on `folder#viewer, source`.
//
// To look up Ingresses(`document#viewer`, `folder#viewer`), look up the definition of relation `document#viewer` and:
// 1. If `folder#viewer` is a directly related type then add `folder#viewer, direct` to the result.
// 2. If computed userset, say `define viewer as writer`, then recurse on `document#writer, folder#viewer`.
// 3. If tuple-to-userset, say, `define viewer as viewer from parent`. Go to parent and find its related types.
//  1. If parent's types includes `folder` type, and `folder` contains `viewer` relation then this is exactly a ttu rewrite and....?
//  2. Otherwise, suppose the types contains `objectType` which has a relation `viewer`, then recurse on `objectType#viewer, folder#viewer`
func (g *ConnectedObjectGraph) RelationshipIngresses(target *openfgapb.RelationReference, source *openfgapb.RelationReference) ([]*RelationshipIngress, error) {
	return g.findIngresses(target, source, map[string]struct{}{}, resolveAllIngresses)
}

// PrunedRelationshipIngresses compute the minimum incoming endges (ingresses) that are possible between the target and the source.
//
// PrunedRelationshipIngresses is primarily used as the driver behind ingresses that work for models including intersection and exclusion.
// If the ingresses from the source to the target pass through a relationship involving intersection or exclusion (directly or indirectly),
// then the PrunedRelationshipIngresses will just return the first-most ingress involved in the rewrite.
//
// Consider the following model:
//
// type user
// type document
//
//	relations
//	  define allowed: [user]
//	  define viewer: [user] and allowed
//
// The pruned relationship ingresses from the 'user' type to 'document#viewer' returns only the `document#viewer` ingress but with a 'RequiresFurtherEvalCondition' ingress condition.
// This is because when evaluating relationships involving intersection or exclusion we choose to only evaluate one operand of the rewrite rule, and for each result found
// we call Check on the result to evaluate the sub-condition on the 'and allowed' bit.
func (g *ConnectedObjectGraph) PrunedRelationshipIngresses(target *openfgapb.RelationReference, source *openfgapb.RelationReference) ([]*RelationshipIngress, error) {
	return g.findIngresses(target, source, map[string]struct{}{}, resolveAnyIngress)
}

func (g *ConnectedObjectGraph) findIngresses(
	target *openfgapb.RelationReference,
	source *openfgapb.RelationReference,
	visited map[string]struct{},
	findIngressOption findIngressOption,
) ([]*RelationshipIngress, error) {
	key := tuple.ToObjectRelationString(target.GetType(), target.GetRelation())
	if _, ok := visited[key]; ok {
		// We've already visited the target so no need to do so again.
		return nil, nil
	}
	visited[key] = struct{}{}

	relation, err := g.typesystem.GetRelation(target.GetType(), target.GetRelation())
	if err != nil {
		return nil, err
	}

	return g.findIngressesWithTargetRewrite(
		target,
		source,
		relation.GetRewrite(),
		visited,
		findIngressOption,
	)
}

// findIngressesWithTargetRewrite is what we use for recursive calls on the targetRewrite, particularly union where we don't
// update the target and source, and only the targetRewrite.
func (g *ConnectedObjectGraph) findIngressesWithTargetRewrite(
	target *openfgapb.RelationReference,
	source *openfgapb.RelationReference,
	targetRewrite *openfgapb.Userset,
	visited map[string]struct{},
	findIngressOption findIngressOption,
) ([]*RelationshipIngress, error) {
	switch t := targetRewrite.GetUserset().(type) {
	case *openfgapb.Userset_This: // e.g. define viewer:[user] as self
		var res []*RelationshipIngress
		directlyRelated, _ := g.typesystem.IsDirectlyRelated(target, source)
		publiclyAssignable, _ := g.typesystem.IsPubliclyAssignable(target, source.GetType())

		if directlyRelated || publiclyAssignable {
			// if source=user, or define viewer:[user:*] as self
			res = append(res, &RelationshipIngress{
				Type:      DirectIngress,
				Ingress:   typesystem.DirectRelationReference(target.GetType(), target.GetRelation()),
				Condition: NoFurtherEvalCondition,
			})
		}

		typeRestrictions, _ := g.typesystem.GetDirectlyRelatedUserTypes(target.GetType(), target.GetRelation())

		for _, typeRestriction := range typeRestrictions {
			if typeRestriction.GetRelation() != "" { // e.g. define viewer:[team#member] as self
				// recursively sub-collect any ingresses for (team#member, source)
				ingresses, err := g.findIngresses(typeRestriction, source, visited, findIngressOption)
				if err != nil {
					return nil, err
				}

				res = append(res, ingresses...)
			}
		}

		return res, nil
	case *openfgapb.Userset_ComputedUserset: // e.g. target = define viewer as writer

		var ingresses []*RelationshipIngress

		// if source=document#writer
		sourceRelMatchesRewritten := target.GetType() == source.GetType() && t.ComputedUserset.GetRelation() == source.GetRelation()

		if sourceRelMatchesRewritten {
			ingresses = append(ingresses, &RelationshipIngress{
				Type:      ComputedUsersetIngress,
				Ingress:   typesystem.DirectRelationReference(target.GetType(), target.GetRelation()),
				Condition: NoFurtherEvalCondition,
			})
		}

		collected, err := g.findIngresses(
			typesystem.DirectRelationReference(target.GetType(), t.ComputedUserset.GetRelation()),
			source,
			visited,
			findIngressOption,
		)
		if err != nil {
			return nil, err
		}

		ingresses = append(
			ingresses,
			collected...,
		)
		return ingresses, nil
	case *openfgapb.Userset_TupleToUserset: // e.g. type document, define viewer as writer from parent
		tupleset := t.TupleToUserset.GetTupleset().GetRelation()               //parent
		computedUserset := t.TupleToUserset.GetComputedUserset().GetRelation() //writer

		var res []*RelationshipIngress
		// e.g. type document, define parent:[user, group] as self
		tuplesetTypeRestrictions, _ := g.typesystem.GetDirectlyRelatedUserTypes(target.GetType(), tupleset)

		for _, typeRestriction := range tuplesetTypeRestrictions {
			r, err := g.typesystem.GetRelation(typeRestriction.GetType(), computedUserset)
			if err != nil {
				if errors.Is(err, typesystem.ErrRelationUndefined) {
					continue
				}

				return nil, err
			}

			// if the computed relation on the tupleset is directly related to the source and computed relation,
			// then it must be an ingress
			//
			// e.g. target=document#viewer, source=folder#viewer
			//
			// type folder
			//   relations
			//     define viewer: [user]
			//
			// type document
			//   relations
			//     define parent: [folder]
			//     define viewer: viewer from parent
			if typeRestriction.GetType() == source.GetType() && computedUserset == source.GetRelation() {

				condition := NoFurtherEvalCondition

				involvesIntersection, err := g.typesystem.RelationInvolvesIntersection(typeRestriction.GetType(), r.GetName())
				if err != nil {
					return nil, err
				}

				involvesExclusion, err := g.typesystem.RelationInvolvesExclusion(typeRestriction.GetType(), r.GetName())
				if err != nil {
					return nil, err
				}

				if involvesIntersection || involvesExclusion {
					condition = RequiresFurtherEvalCondition
				}

				res = append(res, &RelationshipIngress{
					Type:             TupleToUsersetIngress,
					Ingress:          typesystem.DirectRelationReference(target.GetType(), target.GetRelation()),
					TuplesetRelation: typesystem.DirectRelationReference(target.GetType(), tupleset),
					Condition:        condition,
				})
			} else {
				matchesSourceType := typeRestriction.GetType() == source.GetType()
				directlyAssignable := g.typesystem.IsDirectlyAssignable(r)

				// if any of the inner type restrictions of the computed relation are related to the source, then there
				// must be an ingress
				//
				// e.g. target=document#viewer, source=folder#viewer
				//
				// type folder
				//   relations
				//     define viewer: [folder]
				//
				// type document
				//   relations
				//     define parent: [folder]
				//     define viewer: viewer from parent
				if directlyAssignable && matchesSourceType {
					innerRelatedTypes, _ := g.typesystem.GetDirectlyRelatedUserTypes(typeRestriction.GetType(), computedUserset)
					for _, innerRelatedTypeRestriction := range innerRelatedTypes {

						if innerRelatedTypeRestriction.GetType() == source.GetType() {
							res = append(res, &RelationshipIngress{
								Type:             TupleToUsersetIngress,
								Ingress:          typesystem.DirectRelationReference(target.GetType(), target.GetRelation()),
								TuplesetRelation: typesystem.DirectRelationReference(target.GetType(), tupleset),
								Condition:        NoFurtherEvalCondition,
							})
						}
					}
				}
			}

			subResults, err := g.findIngresses(
				typesystem.DirectRelationReference(typeRestriction.GetType(), computedUserset),
				source,
				visited,
				findIngressOption,
			)
			if err != nil {
				return nil, err
			}

			res = append(res, subResults...)

		}

		return res, nil
	case *openfgapb.Userset_Union: // e.g. target = define viewer as self or writer
		var res []*RelationshipIngress
		for _, child := range t.Union.GetChild() {
			// we recurse through each child rewrite
			childResults, err := g.findIngressesWithTargetRewrite(target, source, child, visited, findIngressOption)
			if err != nil {
				return nil, err
			}
			res = append(res, childResults...)
		}
		return res, nil
	case *openfgapb.Userset_Intersection:

		if findIngressOption == resolveAnyIngress {
			child := t.Intersection.GetChild()[0]

			childresults, err := g.findIngressesWithTargetRewrite(target, source, child, visited, findIngressOption)
			if err != nil {
				return nil, err
			}

			for _, childresult := range childresults {
				childresult.Condition = RequiresFurtherEvalCondition
			}

			return childresults, nil
		}

		var ingresses []*RelationshipIngress
		for _, child := range t.Intersection.GetChild() {

			res, err := g.findIngressesWithTargetRewrite(target, source, child, visited, findIngressOption)
			if err != nil {
				return nil, err
			}

			ingresses = append(ingresses, res...)
		}

		if len(ingresses) > 0 {
			ingresses[0].Condition = RequiresFurtherEvalCondition
		}

		return ingresses, nil
	case *openfgapb.Userset_Difference:

		if findIngressOption == resolveAnyIngress {
			child := t.Difference.GetBase()

			childresults, err := g.findIngressesWithTargetRewrite(target, source, child, visited, findIngressOption)
			if err != nil {
				return nil, err
			}

			for _, childresult := range childresults {
				childresult.Condition = RequiresFurtherEvalCondition
			}

			return childresults, nil
		}

		var ingresses []*RelationshipIngress

		baseRewrite := t.Difference.GetBase()

		baseIngresses, err := g.findIngressesWithTargetRewrite(target, source, baseRewrite, visited, findIngressOption)
		if err != nil {
			return nil, err
		}

		if len(baseIngresses) > 0 {
			baseIngresses[0].Condition = RequiresFurtherEvalCondition
		}

		ingresses = append(ingresses, baseIngresses...)

		subtractRewrite := t.Difference.GetSubtract()

		subIngresses, err := g.findIngressesWithTargetRewrite(target, source, subtractRewrite, visited, findIngressOption)
		if err != nil {
			return nil, err
		}
		ingresses = append(ingresses, subIngresses...)

		return ingresses, nil
	default:
		panic("unexpected userset rewrite encountered")
	}
}
