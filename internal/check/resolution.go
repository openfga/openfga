package check

import (
	"fmt"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/tuple"
)

// ResolutionTree represents the complete resolution result.
type ResolutionTree struct {
	// Check is the original check being performed (e.g., "document:1#view@user:alice")
	Check string `json:"check"`

	// Result is the final result of the check
	Result bool `json:"result"`

	// Tree shows how resolution happened
	Tree *ResolutionNode `json:"tree"`
}

// ResolutionNode represents a node in the resolution tree.
type ResolutionNode struct {
	// Type is the object#relation being evaluated at this node (e.g., "document:1#view")
	Type string `json:"type"`

	// Result is the result of this node's evaluation (true/false)
	Result bool `json:"result"`

	// Duration is the time taken to evaluate this node (includes children), e.g., "11.2ms", "99.84µs"
	Duration string `json:"duration"`

	// ItemCount is the number of tuples found at this node (for leaf nodes)
	ItemCount int `json:"item_count,omitempty"`

	// TuplesRead is the number of tuples read from the database at this node
	TuplesRead int `json:"tuples_read,omitempty"`

	// Union is set for union operations (relation defined as: a or b or c)
	Union *UnionNode `json:"union,omitempty"`

	// Intersection is set for intersection operations (relation defined as: a and b)
	Intersection *IntersectionNode `json:"intersection,omitempty"`

	// Exclusion is set for exclusion operations (relation defined as: a but not b)
	Exclusion *ExclusionNode `json:"exclusion,omitempty"`

	// Tuples is set for leaf nodes (direct relation check) - the tuples found
	Tuples []*TupleNode `json:"tuples,omitempty"`

	// startTime is used internally for duration calculation (not serialized)
	startTime time.Time
}

// UnionNode represents a union of multiple branches.
type UnionNode struct {
	Branches []*ResolutionNode `json:"branches"`
}

// IntersectionNode represents an intersection of multiple branches.
type IntersectionNode struct {
	Branches []*ResolutionNode `json:"branches"`
}

// ExclusionNode represents base minus subtracted set.
type ExclusionNode struct {
	Base     *ResolutionNode `json:"base"`
	Subtract *ResolutionNode `json:"subtract"`
}

// TupleNode represents a tuple found during resolution.
type TupleNode struct {
	// Tuple is the tuple that was found (e.g., "document:1#viewer@group:engineering#member")
	Tuple string `json:"tuple"`

	// Computed is the computed resolution of the userset if the tuple points to a userset
	// (e.g., group:engineering#member)
	Computed *ResolutionNode `json:"computed,omitempty"`
}

// NewResolutionNode creates a new resolution node for the given object and relation.
func NewResolutionNode(object, relation string) *ResolutionNode {
	return &ResolutionNode{
		Type:      fmt.Sprintf("%s#%s", object, relation),
		startTime: time.Now(),
	}
}

// NewResolutionNodeFromType creates a new resolution node from a type string (e.g., "document:1#view").
func NewResolutionNodeFromType(typeStr string) *ResolutionNode {
	return &ResolutionNode{
		Type:      typeStr,
		startTime: time.Now(),
	}
}

// NewTupleNode creates a new tuple node from a TupleKey.
func NewTupleNode(tk *openfgav1.TupleKey) *TupleNode {
	return &TupleNode{
		Tuple: tuple.TupleKeyToString(tk),
	}
}

// NewTupleNodeFromString creates a new tuple node from a tuple string.
func NewTupleNodeFromString(tupleStr string) *TupleNode {
	return &TupleNode{
		Tuple: tupleStr,
	}
}

// Complete marks the node as done and calculates duration.
func (n *ResolutionNode) Complete(result bool, itemCount int) {
	n.Result = result
	n.ItemCount = itemCount
	n.Duration = time.Since(n.startTime).String()
}

// CompleteWithTuplesRead marks the node as done with tuples read tracking.
func (n *ResolutionNode) CompleteWithTuplesRead(result bool, itemCount, tuplesRead int) {
	n.Result = result
	n.ItemCount = itemCount
	n.TuplesRead = tuplesRead
	n.Duration = time.Since(n.startTime).String()
}

// SetUnion sets the union node with the given branches.
func (n *ResolutionNode) SetUnion(branches []*ResolutionNode) {
	n.Union = &UnionNode{Branches: branches}
}

// SetIntersection sets the intersection node with the given branches.
func (n *ResolutionNode) SetIntersection(branches []*ResolutionNode) {
	n.Intersection = &IntersectionNode{Branches: branches}
}

// SetExclusion sets the exclusion node with the base and subtract nodes.
func (n *ResolutionNode) SetExclusion(base, subtract *ResolutionNode) {
	n.Exclusion = &ExclusionNode{Base: base, Subtract: subtract}
}

// AddTuple adds a tuple node to the resolution node.
func (n *ResolutionNode) AddTuple(tupleNode *TupleNode) {
	n.Tuples = append(n.Tuples, tupleNode)
}

// CountTuples returns the total count of tuples in this node and all its children.
func (n *ResolutionNode) CountTuples() int {
	if n == nil {
		return 0
	}

	count := len(n.Tuples)

	// Count tuples in child resolution nodes
	for _, t := range n.Tuples {
		if t.Computed != nil {
			count += t.Computed.CountTuples()
		}
	}

	// Count tuples in union branches
	if n.Union != nil {
		for _, branch := range n.Union.Branches {
			count += branch.CountTuples()
		}
	}

	// Count tuples in intersection branches
	if n.Intersection != nil {
		for _, branch := range n.Intersection.Branches {
			count += branch.CountTuples()
		}
	}

	// Count tuples in exclusion
	if n.Exclusion != nil {
		count += n.Exclusion.Base.CountTuples()
		count += n.Exclusion.Subtract.CountTuples()
	}

	return count
}

// MergeResolutionNodes merges multiple resolution responses into a union node.
func MergeResolutionNodes(nodes []*ResolutionNode) *ResolutionNode {
	if len(nodes) == 0 {
		return nil
	}
	if len(nodes) == 1 {
		return nodes[0]
	}

	// Find the first non-nil node to use as a base
	var result *ResolutionNode
	for _, n := range nodes {
		if n != nil {
			result = n
			break
		}
	}

	if result == nil {
		return nil
	}

	// If there are multiple nodes, create a union
	result.SetUnion(nodes)
	return result
}

// countTuplesInBranches counts the total number of tuples found in branches that returned true.
func countTuplesInBranches(branches []*ResolutionNode) int {
	count := 0
	for _, branch := range branches {
		if branch != nil && branch.Result {
			count += branch.ItemCount
		}
	}
	return count
}

// countTuplesReadInBranches counts the total number of tuples read from all branches.
func countTuplesReadInBranches(branches []*ResolutionNode) int {
	count := 0
	for _, branch := range branches {
		if branch != nil {
			count += branch.TuplesRead
		}
	}
	return count
}
