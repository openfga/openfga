package typesystem

import (
	"errors"
	"fmt"
	"strconv"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/encoding"
	"gonum.org/v1/gonum/graph/encoding/dot"
	"gonum.org/v1/gonum/graph/multi"
	"gonum.org/v1/gonum/graph/traverse"
)

// DotEncodedGraph represents an authorization model in graph form.
// For example, types such as `group`, usersets such as `group#member` and wildcards `group:*` are encoded as nodes.
//
// The edges are defined by the relations, e.g.
// `define viewer: [group#member]` defines an edge from group#member to document#viewer.
// `define viewer: viewer from parent` defines a conditional edge from folder#viewer to document#viewer.
// `define viewer: [user:*]` defines two edges, from user to document#viewer and from user:* to document#viewer
//
// Note that model conditions are not encoded, and the operands of intersection, union and exclusion
// are treated equally.
type DotEncodedGraph struct {
	*multi.DirectedGraph // models are multigraphs because there can be more than one edge between two nodes

	// Used to add unique labels to edges
	edgeCounter uint32

	// node labels to node IDs. Used to find nodes
	mapping map[string]int64

	// "fromNodeID-toNodeID-lineID" to line attrs. Used to prevent adding duplicate edges
	lines map[string]*dotEdge
}

var _ dot.Attributers = (*DotEncodedGraph)(nil)

func (g *DotEncodedGraph) DOTAttributers() (graph, node, edge encoding.Attributer) {
	return g, nil, nil
}

func (g *DotEncodedGraph) Attributes() []encoding.Attribute {
	// https://graphviz.org/docs/attrs/rankdir/ - bottom to top
	return []encoding.Attribute{{
		Key:   "rankdir",
		Value: "BT",
	}}
}

type dotEdge struct {
	graph.Line
	attrs map[string]string
}

var _ encoding.Attributer = (*dotEdge)(nil)

func (d *dotEdge) Attributes() []encoding.Attribute {
	var attrs []encoding.Attribute

	for k, val := range d.attrs {
		attrs = append(attrs, encoding.Attribute{
			Key:   k,
			Value: val,
		})
	}
	return attrs
}

type dotNode struct {
	graph.Node
	attrs map[string]string
}

var _ encoding.Attributer = (*dotNode)(nil)

func (d *dotNode) Attributes() []encoding.Attribute {
	var attrs []encoding.Attribute

	for k, val := range d.attrs {
		attrs = append(attrs, encoding.Attribute{
			Key:   k,
			Value: val,
		})
	}
	return attrs
}

func NewDotEncodedGraph(ts *TypeSystem) (*DotEncodedGraph, error) {
	g, err := buildGraph(ts)
	if err != nil {
		return nil, err
	}
	return g, nil
}

// GetDOT returns the DOT visualization. The output text is stable.
// It should only be used for debugging.
func (g *DotEncodedGraph) GetDOT() string {
	dotRepresentation, err := dot.MarshalMulti(g, "", "", "")
	if err != nil {
		return ""
	}
	return string(dotRepresentation)
}

// AreConnected returns true if it's possible to reach target node from source node using breadth first search.
func (g *DotEncodedGraph) AreConnected(sourceLabel, targetLabel string) (bool, error) {
	sourceNode := g.getNode(sourceLabel)
	if sourceNode == nil {
		return false, fmt.Errorf("source node %s not found", sourceLabel)
	}
	targetNode := g.getNode(targetLabel)
	if targetNode == nil {
		return false, fmt.Errorf("target node %s not found", targetLabel)
	}

	bfs := traverse.BreadthFirst{}

	res := false
	bfs.Walk(g.DirectedGraph, sourceNode, func(n graph.Node, d int) bool {
		if n.ID() == targetNode.ID() {
			res = true
			return true
		}
		return false
	})
	return res, nil
}

func buildGraph(ts *TypeSystem) (*DotEncodedGraph, error) {
	g := &DotEncodedGraph{multi.NewDirectedGraph(), 0, make(map[string]int64), make(map[string]*dotEdge)}
	var errs error
	for _, typeName := range ts.GetSortedTypeDefinitions() {
		_ = g.addAndGetNode(typeName)
		_ = g.addAndGetNode(typeName + ":*")

		for _, relation := range ts.GetSortedRelations(typeName) {
			_ = g.addAndGetNode(fmt.Sprintf("%s#%s", typeName, relation))

			rewrite, err := ts.GetRelation(typeName, relation)
			if err != nil {
				errs = errors.Join(errs, err)
				break
			}
			if _, err := WalkUsersetRewrite(rewrite.GetRewrite(), rewriteHandler(ts, g, typeName, relation)); err != nil {
				errs = errors.Join(errs, err)
				break
			}
		}
	}
	if errs != nil {
		return nil, errs
	}
	return g, nil
}

func rewriteHandler(typesys *TypeSystem, g *DotEncodedGraph, typeName, relation string) WalkUsersetRewriteHandler {
	relationNodeName := fmt.Sprintf("%s#%s", typeName, relation)

	return func(r *openfgav1.Userset) interface{} {
		switch rw := r.GetUserset().(type) {
		case *openfgav1.Userset_This:
			assignableRelations, err := typesys.GetDirectlyRelatedUserTypes(typeName, relation)
			if err != nil {
				return err
			}

			for _, assignableRelation := range assignableRelations {
				assignableType := assignableRelation.GetType()

				if assignableRelation.GetRelationOrWildcard() != nil {
					assignableRelationRef := assignableRelation.GetRelation()
					if assignableRelationRef != "" {
						assignableRelationNodeName := fmt.Sprintf("%s#%s", assignableType, assignableRelationRef)

						g.addEdge(assignableRelationNodeName, relationNodeName, "", "")
					}

					wildcardRelationRef := assignableRelation.GetWildcard()
					if wildcardRelationRef != nil {
						wildcardRelationNodeName := fmt.Sprintf("%s:*", assignableType)

						// define viewer: [user:*]
						// should create one edge from user to document#viewer and one from user:* to document#viewer
						g.addEdge(wildcardRelationNodeName, relationNodeName, "", "")
						g.addEdge(assignableType, relationNodeName, "", "")
					}
				} else {
					g.addEdge(assignableType, relationNodeName, "", "")
				}
			}
		case *openfgav1.Userset_ComputedUserset:
			rewrittenRelation := rw.ComputedUserset.GetRelation()
			rewritten, err := typesys.GetRelation(typeName, rewrittenRelation)
			if err != nil {
				return err
			}

			rewrittenNodeName := fmt.Sprintf("%s#%s", typeName, rewritten.GetName())
			g.addEdge(rewrittenNodeName, relationNodeName, "", "dashed")
		case *openfgav1.Userset_TupleToUserset:
			tupleset := rw.TupleToUserset.GetTupleset().GetRelation()
			rewrittenRelation := rw.TupleToUserset.GetComputedUserset().GetRelation()

			tuplesetRel, err := typesys.GetRelation(typeName, tupleset)
			if err != nil {
				return err
			}

			directlyRelatedTypes := tuplesetRel.GetTypeInfo().GetDirectlyRelatedUserTypes()
			for _, relatedType := range directlyRelatedTypes {
				assignableType := relatedType.GetType()
				rewrittenNodeName := fmt.Sprintf("%s#%s", assignableType, rewrittenRelation)
				conditionedOnNodeName := fmt.Sprintf("(%s#%s)", typeName, tuplesetRel.GetName())

				g.addEdge(rewrittenNodeName, relationNodeName, conditionedOnNodeName, "")
			}
		case *openfgav1.Userset_Union:
		case *openfgav1.Userset_Intersection:
		case *openfgav1.Userset_Difference:
		default:
			return nil
		}
		return nil
	}
}

func (g *DotEncodedGraph) getNode(label string) graph.Node {
	if id, ok := g.mapping[label]; ok {
		return g.Node(id)
	}
	return nil
}

func (g *DotEncodedGraph) addAndGetNode(label string) graph.Node {
	if n := g.getNode(label); n != nil {
		return g.Node(n.ID())
	}
	n := &dotNode{Node: g.DirectedGraph.NewNode(), attrs: make(map[string]string)}
	g.DirectedGraph.AddNode(n)
	g.mapping[label] = n.ID()
	n.attrs["label"] = label
	return n
}

func (g *DotEncodedGraph) addEdge(from, to, optionalHeadLabel, optionalStyle string) {
	n1 := g.addAndGetNode(from)
	n2 := g.addAndGetNode(to)
	if g.edgeExists(n1.ID(), n2.ID(), optionalHeadLabel) {
		return
	}
	line := g.DirectedGraph.NewLine(n1, n2)
	newEdge := &dotEdge{Line: line, attrs: make(map[string]string)}
	g.lines[fmt.Sprintf("%v-%v-%v", n1.ID(), n2.ID(), line.ID())] = newEdge
	g.DirectedGraph.SetLine(newEdge)
	g.edgeCounter++
	newEdge.attrs["label"] = strconv.Itoa(int(g.edgeCounter))
	if optionalHeadLabel != "" {
		newEdge.attrs["headlabel"] = optionalHeadLabel
	}
	if optionalStyle != "" {
		newEdge.attrs["style"] = optionalStyle
	}
}

func (g *DotEncodedGraph) edgeExists(n1 int64, n2 int64, optionalHeadLabel string) bool {
	existingEdgesIter := g.Lines(n1, n2)
	for {
		if !existingEdgesIter.Next() {
			break
		}
		e := existingEdgesIter.Line()
		if g.lines[fmt.Sprintf("%v-%v-%v", n1, n2, e.ID())].attrs["headlabel"] == optionalHeadLabel {
			// duplicate edge
			return true
		}
	}
	return false
}
