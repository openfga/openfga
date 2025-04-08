package graph

import (
	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/encoding"
)

type EdgeType int64

const (
	DirectEdge   EdgeType = 0
	RewriteEdge  EdgeType = 1
	TTUEdge      EdgeType = 2
	ComputedEdge EdgeType = 3
	// When an edge does not have cond in the model, it will have a condition with value none.
	// This is required to differentiate when an edge need to support condition and no condition
	// like define rel1: [user, user with condX], in this case the edge will have [none, condX]
	// or an edge needs to support only condition like define rel1: [user with condX], the edge will have [condX]
	// in the case the edge does not have any condition like define rel1: [user], the edge will have [none].
	NoCond string = "none"
)

type AuthorizationModelEdge struct {
	graph.Line

	// custom attributes
	edgeType EdgeType

	// only when edgeType == TTUEdge
	tuplesetRelation string

	// conditions on the edge. This is a flattened graph with dedupx edges,
	// if you have a node with multiple edges to another node will be deduplicate and instead
	// only one edge but with multiple conditions,
	// define rel1: [user, user with condX]
	// then the node rel1 will have an edge pointing to the node user and with two conditions
	// one that will be none and another one that will be condX
	conditions []string
}

var _ encoding.Attributer = (*AuthorizationModelEdge)(nil)

func (n *AuthorizationModelEdge) EdgeType() EdgeType {
	return n.edgeType
}

// TuplesetRelation returns the TTU relation. For example, relation
// define viewer: viewer from parent
// gives the graph "document#viewer" -> "document#viewer" and the edge
// is conditioned on "document#parent".
func (n *AuthorizationModelEdge) TuplesetRelation() string {
	return n.tuplesetRelation
}

func (n *AuthorizationModelEdge) Attributes() []encoding.Attribute {
	switch n.edgeType {
	case DirectEdge:
		return []encoding.Attribute{
			{
				Key:   "label",
				Value: "direct",
			},
		}
	case ComputedEdge:
		return []encoding.Attribute{
			{
				Key:   "style",
				Value: "dashed",
			},
		}
	case TTUEdge:
		headLabelAttrValue := n.tuplesetRelation
		if headLabelAttrValue == "" {
			headLabelAttrValue = "missing"
		}

		return []encoding.Attribute{
			{
				Key:   "headlabel",
				Value: "(" + headLabelAttrValue + ")",
			},
		}
	case RewriteEdge:
		return []encoding.Attribute{}
	default:
		return []encoding.Attribute{}
	}
}
