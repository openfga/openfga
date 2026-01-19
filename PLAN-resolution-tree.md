# Plan: Add Resolution Tree to Weighted Graph Check Implementation

## Overview

This plan outlines how to add a `resolution` field to the check response in the new weighted graph-based implementation (`internal/check/`). The resolution will return a tree structure showing how the permission was resolved, including the path through the model and the tuples found at each step.

## Desired Output Format

For a check using the **Google Drive sample model**: `doc:2021-roadmap#can_read@user:charles`

The output should show timing, item count, and result at each node, like:
```
✓ doc:2021-roadmap#can_read (5.8ms, 1 item)
├── ⨉ doc:2021-roadmap#viewer (0.6ms, 0 items)
├── ⨉ doc:2021-roadmap#owner (0.5ms, 0 items)
└── ✓ doc:2021-roadmap#can_read(viewer from parent) (4.2ms, 1 item)
    └── doc:2021-roadmap#parent@folder:product-2021
        └── ✓ folder:product-2021#viewer (3.1ms, 1 item)
            ├── ✓ folder:product-2021#viewer(direct) (2.3ms, 1 item)
            │   └── folder:product-2021#viewer@group:fabrikam#member
            │       └── ✓ group:fabrikam#member (1.1ms, 1 item)
            │           └── group:fabrikam#member@user:charles
            ├── ⨉ folder:product-2021#owner (0.4ms, 0 items)
            └── ⨉ folder:product-2021#viewer(viewer from parent) (0.3ms, 0 items)
```

JSON format:
```json
{
  "allowed": true,
  "resolution": {
    "check": "doc:2021-roadmap#can_read@user:charles",
    "result": true,
    "tree": {
      "type": "doc:2021-roadmap#can_read",
      "result": true,
      "duration": "5.8ms",
      "item_count": 1,
      "union": {
        "branches": [
          {
            "type": "doc:2021-roadmap#viewer",
            "result": false,
            "duration": "0.6ms",
            "item_count": 0,
            "tuples": []
          },
          {
            "type": "doc:2021-roadmap#owner",
            "result": false,
            "duration": "0.5ms",
            "item_count": 0,
            "tuples": []
          },
          {
            "type": "doc:2021-roadmap#can_read(viewer from parent)",
            "result": true,
            "duration": "4.2ms",
            "item_count": 1,
            "tuples": [
              {
                "tuple": "doc:2021-roadmap#parent@folder:product-2021",
                "computed": {
                  "type": "folder:product-2021#viewer",
                  "result": true,
                  "duration": "3.1ms",
                  "item_count": 1,
                  "union": {
                    "branches": [
                      {
                        "type": "folder:product-2021#viewer(direct)",
                        "result": true,
                        "duration": "2.3ms",
                        "item_count": 1,
                        "tuples": [
                          {
                            "tuple": "folder:product-2021#viewer@group:fabrikam#member",
                            "computed": {
                              "type": "group:fabrikam#member",
                              "result": true,
                              "duration": "1.1ms",
                              "item_count": 1,
                              "tuples": [
                                {"tuple": "group:fabrikam#member@user:charles"}
                              ]
                            }
                          }
                        ]
                      },
                      {
                        "type": "folder:product-2021#owner",
                        "result": false,
                        "duration": "0.4ms",
                        "item_count": 0,
                        "tuples": []
                      },
                      {
                        "type": "folder:product-2021#viewer(viewer from parent)",
                        "result": false,
                        "duration": "0.3ms",
                        "item_count": 0,
                        "tuples": []
                      }
                    ]
                  }
                }
              }
            ]
          }
        ]
      }
    }
  }
}
```

This shows how Charles can read `doc:2021-roadmap` via TTU (`viewer from parent`):
1. Charles is NOT a direct viewer of the doc
2. Charles is NOT the owner of the doc
3. BUT via `viewer from parent`:
   - doc:2021-roadmap has parent folder:product-2021
   - folder:product-2021 has viewer group:fabrikam#member
   - Charles is a member of group:fabrikam

---

## Resolution Tree Data Structures

### Core Types

```go
// ResolutionTree represents the complete resolution result
type ResolutionTree struct {
    // The original check being performed
    Check  string `json:"check"`  // "document:1#view@user:alice"

    // Final result
    Result bool `json:"result"`

    // The tree showing how resolution happened
    Tree *ResolutionNode `json:"tree"`
}

// ResolutionNode represents a node in the resolution tree
type ResolutionNode struct {
    // The object#relation being evaluated at this node
    Type string `json:"type"` // e.g., "document:1#view"

    // Result of this node's evaluation (true/false)
    Result bool `json:"result"`

    // Time taken to evaluate this node (includes children)
    Duration string `json:"duration"` // e.g., "11.2ms", "99.84µs"

    // Number of tuples found at this node (for leaf nodes)
    ItemCount int `json:"item_count,omitempty"`

    // Only one of the following will be set depending on the node type:

    // For union operations (relation defined as: a or b or c)
    Union *UnionNode `json:"union,omitempty"`

    // For intersection operations (relation defined as: a and b)
    Intersection *IntersectionNode `json:"intersection,omitempty"`

    // For exclusion operations (relation defined as: a but not b)
    Exclusion *ExclusionNode `json:"exclusion,omitempty"`

    // For leaf nodes (direct relation check) - the tuples found
    Tuples []*TupleNode `json:"tuples,omitempty"`
}

// UnionNode represents a union of multiple branches
type UnionNode struct {
    Branches []*ResolutionNode `json:"branches"`
}

// IntersectionNode represents an intersection of multiple branches
type IntersectionNode struct {
    Branches []*ResolutionNode `json:"branches"`
}

// ExclusionNode represents a base minus subtracted set
type ExclusionNode struct {
    Base     *ResolutionNode `json:"base"`
    Subtract *ResolutionNode `json:"subtract"`
}

// TupleNode represents a tuple found during resolution
type TupleNode struct {
    // The tuple that was found (e.g., "document:1#viewer@group:engineering#member")
    Tuple string `json:"tuple"`

    // If the tuple points to a userset (group:engineering#member), this shows
    // the computed resolution of that userset
    Computed *ResolutionNode `json:"computed,omitempty"`
}
```

### Updated Response

```go
type Response struct {
    Allowed    bool
    Resolution *ResolutionTree // nil if resolution tracking is disabled
}
```

### Updated Request

```go
type Request struct {
    // ... existing fields ...

    // TraceResolution enables building the resolution tree
    TraceResolution bool
}
```

---

## Current State Analysis

### Current Response Structure
```go
// internal/check/response.go
type Response struct {
    Allowed bool
}
```

### Where Tuples Are Found (Key Instrumentation Points)

| Method | File:Line | What it finds |
|--------|-----------|---------------|
| `specificType` | check.go:659 | Direct tuple (user assigned to relation) |
| `specificTypeWildcard` | check.go:707 | Wildcard tuple (type:* assigned) |
| `specificTypeAndRelation` | check.go:769 | Userset tuples (object#relation assigned) |
| `ttu` | check.go:843 | TTU intermediate tuples |
| `resolveRecursiveUserset` | check.go:277 | Recursive userset tuples |
| `resolveRecursiveTTU` | check.go:328 | Recursive TTU tuples |

### Resolution Flow Points

| Method | What it represents |
|--------|-------------------|
| `ResolveCheck` | Root of resolution |
| `ResolveUnion` | Union operation |
| `ResolveUnionEdges` | Multiple union branches |
| `ResolveIntersection` | Intersection operation |
| `ResolveExclusion` | Exclusion operation |
| `ResolveEdge` | Edge traversal |

---

## Implementation Plan

### Phase 1: Define Data Structures

**File: `internal/check/resolution.go` (new)**

```go
package check

import (
    "fmt"
    "time"
    openfgav1 "github.com/openfga/api/proto/openfga/v1"
    "github.com/openfga/openfga/pkg/tuple"
)

// ResolutionTree represents the complete resolution result
type ResolutionTree struct {
    Check  string          `json:"check"`
    Result bool            `json:"result"`
    Tree   *ResolutionNode `json:"tree"`
}

// ResolutionNode represents a node in the resolution tree
type ResolutionNode struct {
    Type         string             `json:"type"`
    Result       bool               `json:"result"`
    Duration     string             `json:"duration"`              // e.g., "11.2ms", "99.84µs"
    ItemCount    int                `json:"item_count,omitempty"`  // number of tuples found
    Union        *UnionNode         `json:"union,omitempty"`
    Intersection *IntersectionNode  `json:"intersection,omitempty"`
    Exclusion    *ExclusionNode     `json:"exclusion,omitempty"`
    Tuples       []*TupleNode       `json:"tuples,omitempty"`

    // Internal: start time for duration calculation (not serialized)
    startTime    time.Time          `json:"-"`
}

// UnionNode represents a union of multiple branches
type UnionNode struct {
    Branches []*ResolutionNode `json:"branches"`
}

// IntersectionNode represents an intersection of multiple branches
type IntersectionNode struct {
    Branches []*ResolutionNode `json:"branches"`
}

// ExclusionNode represents base minus subtracted
type ExclusionNode struct {
    Base     *ResolutionNode `json:"base"`
    Subtract *ResolutionNode `json:"subtract"`
}

// TupleNode represents a found tuple
type TupleNode struct {
    Tuple    string          `json:"tuple"`
    Computed *ResolutionNode `json:"computed,omitempty"`
}

// Helper functions

func NewResolutionNode(object, relation string) *ResolutionNode {
    return &ResolutionNode{
        Type:      fmt.Sprintf("%s#%s", object, relation),
        startTime: time.Now(),
    }
}

func NewTupleNode(tk *openfgav1.TupleKey) *TupleNode {
    return &TupleNode{
        Tuple: tuple.TupleKeyToString(tk),
    }
}

// Complete marks the node as done and calculates duration
func (n *ResolutionNode) Complete(result bool, itemCount int) {
    n.Result = result
    n.ItemCount = itemCount
    n.Duration = time.Since(n.startTime).String()
}

func (n *ResolutionNode) SetUnion(branches []*ResolutionNode) {
    n.Union = &UnionNode{Branches: branches}
}

func (n *ResolutionNode) SetIntersection(branches []*ResolutionNode) {
    n.Intersection = &IntersectionNode{Branches: branches}
}

func (n *ResolutionNode) SetExclusion(base, subtract *ResolutionNode) {
    n.Exclusion = &ExclusionNode{Base: base, Subtract: subtract}
}

func (n *ResolutionNode) AddTuple(tuple *TupleNode) {
    n.Tuples = append(n.Tuples, tuple)
}
```

### Phase 2: Update Response Structure

**File: `internal/check/response.go`**

Add Resolution field to Response:
```go
type Response struct {
    Allowed    bool
    Resolution *ResolutionTree
}
```

### Phase 3: Update Request Structure

**File: `internal/check/request.go`**

Add TraceResolution field:
```go
type Request struct {
    // ... existing fields ...
    TraceResolution bool
}

type RequestParams struct {
    // ... existing fields ...
    TraceResolution bool
}
```

Update `NewRequest` to copy TraceResolution.
Update `cloneWithTupleKey` to propagate TraceResolution.

### Phase 4: Instrument Main Check Flow

**File: `internal/check/check.go`**

#### 4.1 Update `ResolveCheck`

```go
func (r *Resolver) ResolveCheck(ctx context.Context, req *Request) (*Response, error) {
    // ... existing code ...

    res, err := r.ResolveUnion(ctx, req, node, nil)
    if err != nil {
        return nil, err
    }

    // Build resolution tree if tracing enabled
    if req.TraceResolution && res.Resolution != nil {
        res.Resolution.Check = req.GetTupleString()
        res.Resolution.Result = res.Allowed
    }

    return res, nil
}
```

#### 4.2 Update `ResolveUnion`

When tracing, collect resolution nodes from all branches and build union node:

```go
func (r *Resolver) ResolveUnion(ctx context.Context, req *Request, node *authzGraph.WeightedAuthorizationModelNode, visited *sync.Map) (*Response, error) {
    // ... existing code ...

    res, err := r.ResolveUnionEdges(ctx, req, terminalEdges, visited)

    // If tracing, wrap in union node
    if req.TraceResolution && res != nil {
        resNode := NewResolutionNode(req.GetTupleKey().GetObject(), req.GetTupleKey().GetRelation())
        // The branches come from ResolveUnionEdges
        resNode.SetUnion(res.resolutionBranches) // Need to collect these
        res.Resolution = &ResolutionTree{Tree: resNode}
    }

    return res, nil
}
```

#### 4.3 Update `ResolveUnionEdges`

Track resolution from each edge:
- Collect resolution nodes from each branch
- Even if result is false, include the branch (showing empty tuples)
- Return all branches in response metadata

#### 4.4 Update `ResolveIntersection`

```go
func (r *Resolver) ResolveIntersection(ctx context.Context, req *Request, node *authzGraph.WeightedAuthorizationModelNode) (*Response, error) {
    // ... existing logic ...

    if req.TraceResolution {
        resNode := NewResolutionNode(req.GetTupleKey().GetObject(), req.GetTupleKey().GetRelation())
        resNode.SetIntersection(branchNodes) // Collect from each branch
        // ... attach to response
    }
}
```

#### 4.5 Update `ResolveExclusion`

```go
func (r *Resolver) ResolveExclusion(ctx context.Context, req *Request, node *authzGraph.WeightedAuthorizationModelNode) (*Response, error) {
    // ... existing logic ...

    if req.TraceResolution {
        resNode := NewResolutionNode(req.GetTupleKey().GetObject(), req.GetTupleKey().GetRelation())
        resNode.SetExclusion(baseNode, subtractNode)
        // ... attach to response
    }
}
```

### Phase 5: Instrument Leaf Nodes (Tuple Discovery)

#### 5.1 Update `specificType`

This finds direct user assignments. When a tuple is found:

```go
func (r *Resolver) specificType(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge) (*Response, error) {
    // ... existing code to find tuple t ...

    res := &Response{Allowed: allowed}

    if req.TraceResolution {
        resNode := NewResolutionNode(req.GetTupleKey().GetObject(), edge.GetRelation())
        if t != nil && allowed {
            resNode.AddTuple(NewTupleNode(t))
        }
        res.Resolution = &ResolutionTree{Tree: resNode}
    }

    return res, nil
}
```

#### 5.2 Update `specificTypeAndRelation`

This finds userset assignments and needs to recurse:

```go
func (r *Resolver) specificTypeAndRelation(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, visited *sync.Map) (*Response, error) {
    // ... existing code ...

    // When iterating through tuples and resolving usersets:
    // For each tuple found that leads to allowed=true,
    // create TupleNode with computed resolution from child check

    if req.TraceResolution {
        resNode := NewResolutionNode(req.GetTupleKey().GetObject(), relation)
        for _, foundTuple := range matchingTuples {
            tupleNode := NewTupleNode(foundTuple.key)
            if foundTuple.childRes != nil && foundTuple.childRes.Resolution != nil {
                tupleNode.Computed = foundTuple.childRes.Resolution.Tree
            }
            resNode.AddTuple(tupleNode)
        }
        res.Resolution = &ResolutionTree{Tree: resNode}
    }
}
```

#### 5.3 Update `ttu`

Similar to userset, but for tuple-to-userset:

```go
func (r *Resolver) ttu(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, visited *sync.Map) (*Response, error) {
    // ... existing code ...

    // Track tuples found and their computed resolutions
    if req.TraceResolution {
        resNode := NewResolutionNode(req.GetTupleKey().GetObject(), req.GetTupleKey().GetRelation())
        for _, foundTuple := range matchingTuples {
            tupleNode := NewTupleNode(foundTuple.key)
            tupleNode.Computed = foundTuple.childRes.Resolution.Tree
            resNode.AddTuple(tupleNode)
        }
        res.Resolution = &ResolutionTree{Tree: resNode}
    }
}
```

### Phase 6: Update Strategies

**Files: `default.go`, `weight2.go`, `recursive.go`**

The strategies need to:
1. Propagate TraceResolution to child requests
2. Collect resolution nodes from child responses
3. Build resolution tree showing the tuples that led to the result

#### 6.1 DefaultStrategy

Update `Userset` and `TTU` to collect resolution from dispatched checks.

#### 6.2 Weight2Strategy

Update to track which tuples matched in the intersection.

#### 6.3 RecursiveStrategy

Update to show the recursive path traversed.

### Phase 7: Update Interface

**File: `internal/check/interface.go`**

No changes needed - Strategy interface returns `*Response` which now includes Resolution.

### Phase 8: Wire to API

**File: `pkg/server/commands/check_command.go`**

Add parameter to enable tracing:

```go
type CheckCommandParams struct {
    // ... existing fields ...
    TraceResolution bool
}
```

Map to proto response (may need proto changes or JSON field).

---

## Thread Safety Considerations

Since resolution tracking happens during concurrent execution:

1. Each branch creates its own `ResolutionNode`
2. Parent nodes collect child nodes only after children complete
3. No shared mutable state during resolution building
4. Final tree assembly happens in the parent after all children return

---

## Example Scenarios

All examples use the **Google Drive sample model** from [openfga/sample-stores](https://github.com/openfga/sample-stores/tree/main/stores/gdrive).

### Model Reference

```
model
  schema 1.1

type user

type group
  relations
    define member: [user]

type folder
  relations
    define can_create_file: owner
    define owner: [user]
    define parent: [folder]
    define viewer: [user, user:*, group#member] or owner or viewer from parent

type doc
  relations
    define can_change_owner: owner
    define can_read: viewer or owner or viewer from parent
    define can_share: owner or owner from parent
    define can_write: owner or owner from parent
    define owner: [user]
    define parent: [folder]
    define viewer: [user, user:*, group#member]
```

### Tuples Reference

```yaml
tuples:
  - user: user:anne
    relation: member
    object: group:contoso
  - user: user:beth
    relation: member
    object: group:contoso
  - user: user:charles
    relation: member
    object: group:fabrikam
  - user: folder:product-2021
    relation: parent
    object: doc:public-roadmap
  - user: folder:product-2021
    relation: parent
    object: doc:2021-roadmap
  - user: group:fabrikam#member
    relation: viewer
    object: folder:product-2021
  - user: user:anne
    relation: owner
    object: folder:product-2021
  - user: user:beth
    relation: viewer
    object: doc:2021-roadmap
  - user: user:*
    relation: viewer
    object: doc:public-roadmap
```

---

### Scenario 1: Direct Assignment

Check: `doc:2021-roadmap#viewer@user:beth`

Beth is directly assigned as viewer of doc:2021-roadmap via tuple `doc:2021-roadmap#viewer@user:beth`.

```json
{
  "check": "doc:2021-roadmap#viewer@user:beth",
  "result": true,
  "tree": {
    "type": "doc:2021-roadmap#viewer",
    "result": true,
    "duration": "150µs",
    "item_count": 1,
    "tuples": [
      {"tuple": "doc:2021-roadmap#viewer@user:beth"}
    ]
  }
}
```

---

### Scenario 2: Wildcard Assignment

Check: `doc:public-roadmap#viewer@user:anyone`

The public-roadmap document has a wildcard viewer assignment: `doc:public-roadmap#viewer@user:*`.

```json
{
  "check": "doc:public-roadmap#viewer@user:anyone",
  "result": true,
  "tree": {
    "type": "doc:public-roadmap#viewer",
    "result": true,
    "duration": "120µs",
    "item_count": 1,
    "tuples": [
      {"tuple": "doc:public-roadmap#viewer@user:*"}
    ]
  }
}
```

---

### Scenario 3: Userset (Group Membership)

Check: `folder:product-2021#viewer@user:charles`

Charles is a member of group:fabrikam, and group:fabrikam#member is a viewer of folder:product-2021.

Tuples involved:
- `folder:product-2021#viewer@group:fabrikam#member`
- `group:fabrikam#member@user:charles`

```json
{
  "check": "folder:product-2021#viewer@user:charles",
  "result": true,
  "tree": {
    "type": "folder:product-2021#viewer",
    "result": true,
    "duration": "2.3ms",
    "item_count": 1,
    "tuples": [
      {
        "tuple": "folder:product-2021#viewer@group:fabrikam#member",
        "computed": {
          "type": "group:fabrikam#member",
          "result": true,
          "duration": "1.1ms",
          "item_count": 1,
          "tuples": [
            {"tuple": "group:fabrikam#member@user:charles"}
          ]
        }
      }
    ]
  }
}
```

---

### Scenario 4: Union (can_read = viewer or owner or viewer from parent)

Check: `doc:2021-roadmap#can_read@user:beth`

Model: `define can_read: viewer or owner or viewer from parent`

Beth can read because she is a direct viewer. The union evaluates all branches.

```json
{
  "check": "doc:2021-roadmap#can_read@user:beth",
  "result": true,
  "tree": {
    "type": "doc:2021-roadmap#can_read",
    "result": true,
    "duration": "3.2ms",
    "item_count": 1,
    "union": {
      "branches": [
        {
          "type": "doc:2021-roadmap#viewer",
          "result": true,
          "duration": "0.8ms",
          "item_count": 1,
          "tuples": [
            {"tuple": "doc:2021-roadmap#viewer@user:beth"}
          ]
        },
        {
          "type": "doc:2021-roadmap#owner",
          "result": false,
          "duration": "0.5ms",
          "item_count": 0,
          "tuples": []
        },
        {
          "type": "doc:2021-roadmap#can_read(viewer from parent)",
          "result": false,
          "duration": "1.2ms",
          "item_count": 0,
          "tuples": []
        }
      ]
    }
  }
}
```

---

### Scenario 5: TTU (Tuple-to-Userset) - viewer from parent

Check: `doc:2021-roadmap#can_read@user:charles`

Charles can read doc:2021-roadmap through the TTU path: `viewer from parent`.
- doc:2021-roadmap has parent folder:product-2021
- folder:product-2021 has viewer group:fabrikam#member
- Charles is member of group:fabrikam

```json
{
  "check": "doc:2021-roadmap#can_read@user:charles",
  "result": true,
  "tree": {
    "type": "doc:2021-roadmap#can_read",
    "result": true,
    "duration": "5.8ms",
    "item_count": 1,
    "union": {
      "branches": [
        {
          "type": "doc:2021-roadmap#viewer",
          "result": false,
          "duration": "0.6ms",
          "item_count": 0,
          "tuples": []
        },
        {
          "type": "doc:2021-roadmap#owner",
          "result": false,
          "duration": "0.5ms",
          "item_count": 0,
          "tuples": []
        },
        {
          "type": "doc:2021-roadmap#can_read(viewer from parent)",
          "result": true,
          "duration": "4.2ms",
          "item_count": 1,
          "tuples": [
            {
              "tuple": "doc:2021-roadmap#parent@folder:product-2021",
              "computed": {
                "type": "folder:product-2021#viewer",
                "result": true,
                "duration": "3.1ms",
                "item_count": 1,
                "union": {
                  "branches": [
                    {
                      "type": "folder:product-2021#viewer(direct)",
                      "result": true,
                      "duration": "2.3ms",
                      "item_count": 1,
                      "tuples": [
                        {
                          "tuple": "folder:product-2021#viewer@group:fabrikam#member",
                          "computed": {
                            "type": "group:fabrikam#member",
                            "result": true,
                            "duration": "1.1ms",
                            "item_count": 1,
                            "tuples": [
                              {"tuple": "group:fabrikam#member@user:charles"}
                            ]
                          }
                        }
                      ]
                    },
                    {
                      "type": "folder:product-2021#owner",
                      "result": false,
                      "duration": "0.4ms",
                      "item_count": 0,
                      "tuples": []
                    },
                    {
                      "type": "folder:product-2021#viewer(viewer from parent)",
                      "result": false,
                      "duration": "0.3ms",
                      "item_count": 0,
                      "tuples": []
                    }
                  ]
                }
              }
            }
          ]
        }
      ]
    }
  }
}
```

---

### Scenario 6: Owner path (can_write = owner or owner from parent)

Check: `doc:2021-roadmap#can_write@user:anne`

Anne can write to doc:2021-roadmap because she is the owner of the parent folder:product-2021.

Model: `define can_write: owner or owner from parent`

```json
{
  "check": "doc:2021-roadmap#can_write@user:anne",
  "result": true,
  "tree": {
    "type": "doc:2021-roadmap#can_write",
    "result": true,
    "duration": "4.1ms",
    "item_count": 1,
    "union": {
      "branches": [
        {
          "type": "doc:2021-roadmap#owner",
          "result": false,
          "duration": "0.5ms",
          "item_count": 0,
          "tuples": []
        },
        {
          "type": "doc:2021-roadmap#can_write(owner from parent)",
          "result": true,
          "duration": "3.2ms",
          "item_count": 1,
          "tuples": [
            {
              "tuple": "doc:2021-roadmap#parent@folder:product-2021",
              "computed": {
                "type": "folder:product-2021#owner",
                "result": true,
                "duration": "1.8ms",
                "item_count": 1,
                "tuples": [
                  {"tuple": "folder:product-2021#owner@user:anne"}
                ]
              }
            }
          ]
        }
      ]
    }
  }
}
```

---

### Scenario 7: Denied access (no matching path)

Check: `doc:2021-roadmap#can_change_owner@user:beth`

Beth cannot change the owner because she is not the owner. Model: `define can_change_owner: owner`

```json
{
  "check": "doc:2021-roadmap#can_change_owner@user:beth",
  "result": false,
  "tree": {
    "type": "doc:2021-roadmap#can_change_owner",
    "result": false,
    "duration": "0.8ms",
    "item_count": 0,
    "tuples": []
  }
}
```

---

### Scenario 8: Complex path - can_read via owner from parent

Check: `doc:2021-roadmap#can_read@user:anne`

Anne can read through multiple paths - as folder owner she inherits viewer access.

Model: `define can_read: viewer or owner or viewer from parent`
`define viewer: [user, user:*, group#member] or owner or viewer from parent` (on folder)

```json
{
  "check": "doc:2021-roadmap#can_read@user:anne",
  "result": true,
  "tree": {
    "type": "doc:2021-roadmap#can_read",
    "result": true,
    "duration": "6.5ms",
    "item_count": 1,
    "union": {
      "branches": [
        {
          "type": "doc:2021-roadmap#viewer",
          "result": false,
          "duration": "0.6ms",
          "item_count": 0,
          "tuples": []
        },
        {
          "type": "doc:2021-roadmap#owner",
          "result": false,
          "duration": "0.5ms",
          "item_count": 0,
          "tuples": []
        },
        {
          "type": "doc:2021-roadmap#can_read(viewer from parent)",
          "result": true,
          "duration": "5.0ms",
          "item_count": 1,
          "tuples": [
            {
              "tuple": "doc:2021-roadmap#parent@folder:product-2021",
              "computed": {
                "type": "folder:product-2021#viewer",
                "result": true,
                "duration": "3.8ms",
                "item_count": 1,
                "union": {
                  "branches": [
                    {
                      "type": "folder:product-2021#viewer(direct)",
                      "result": false,
                      "duration": "0.9ms",
                      "item_count": 0,
                      "tuples": []
                    },
                    {
                      "type": "folder:product-2021#owner",
                      "result": true,
                      "duration": "1.2ms",
                      "item_count": 1,
                      "tuples": [
                        {"tuple": "folder:product-2021#owner@user:anne"}
                      ]
                    },
                    {
                      "type": "folder:product-2021#viewer(viewer from parent)",
                      "result": false,
                      "duration": "0.8ms",
                      "item_count": 0,
                      "tuples": []
                    }
                  ]
                }
              }
            }
          ]
        }
      ]
    }
  }
}
```

---

### Scenario 9: can_create_file (owner only)

Check: `folder:product-2021#can_create_file@user:anne`

Anne can create files because she is the folder owner.

Model: `define can_create_file: owner`

```json
{
  "check": "folder:product-2021#can_create_file@user:anne",
  "result": true,
  "tree": {
    "type": "folder:product-2021#can_create_file",
    "result": true,
    "duration": "1.5ms",
    "item_count": 1,
    "tuples": [
      {"tuple": "folder:product-2021#owner@user:anne"}
    ]
  }
}
```

---

### Scenario 10: can_create_file denied for viewer

Check: `folder:product-2021#can_create_file@user:charles`

Charles cannot create files - he is only a viewer (via group membership), not an owner.

```json
{
  "check": "folder:product-2021#can_create_file@user:charles",
  "result": false,
  "tree": {
    "type": "folder:product-2021#can_create_file",
    "result": false,
    "duration": "0.6ms",
    "item_count": 0,
    "tuples": []
  }
}
```

---

## Files Summary

### New Files
- `internal/check/resolution.go` - Resolution tree types and helpers
- `internal/check/resolution_test.go` - Tests

### Modified Files
- `internal/check/response.go` - Add Resolution field
- `internal/check/request.go` - Add TraceResolution flag
- `internal/check/check.go` - Instrument resolution flow
- `internal/check/default.go` - Track strategy resolution
- `internal/check/weight2.go` - Track strategy resolution
- `internal/check/recursive.go` - Track strategy resolution
- `pkg/server/commands/check_command.go` - Wire to API

---

## Implementation Order

1. **Phase 1-2**: Define types in `resolution.go` and update `response.go`
2. **Phase 3**: Update `request.go` for TraceResolution flag
3. **Phase 5**: Instrument leaf nodes (`specificType`, `specificTypeAndRelation`, `ttu`) - this gives us the basic tuple tracking
4. **Phase 4**: Instrument main flow (`ResolveUnion`, `ResolveIntersection`, `ResolveExclusion`) - combines branches
5. **Phase 6**: Update strategies to propagate resolution
6. **Phase 8**: Wire to API
7. **Testing**: Add comprehensive tests
