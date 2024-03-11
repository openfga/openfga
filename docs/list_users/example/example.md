# ListUsers API implementation

An authorization model can be represented as a directed, possibly cyclical, graph. This graph shows the possible relationships between types (e.g. `user`) and usersets (eg `user:*`, `group:fga#member`).

At a high level, answering ListUsers queries involves two phases:

- Phase 1: we draw the model and we do something similar to Breadth First Search. Starting at a source and trying to reach a target node, we explore (forward expand) all the paths that can lead to the target object type. During this expansion, we read tuples and we include in the response all the subjects that we find that are of the target type. Some of those objects will require further evaluation; we mark them as "candidates".
- Phase 2: all the "candidate" objects that require further evaluation, we call Check upon. If that Check returns `allowed=true`, we include them in the response.

> NOTE: In this example, phase 2 isn't necessary because the model has no intersections or exclusions, so we'll exclude it. Please see `example_with_intersection_or_exclusion` to see how phase 2 works.

## Example model
Consider a store with the following authorization model:

```
model
    schema 1.1
type user

type group
    relations
    define member: [user, group#member]

type folder
    relations
        define viewer: [user]

type document
    relations
        define parent: [folder]
        define editor: [user]
        define viewer: [user, user:*, group#member] or editor or viewer from parent
```

We can represent this with a graph:

![model](model.svg)

Consider the following 8 tuples: 

```html
document:1#viewer@user:andres
document:2#viewer@group:eng#member
document:3#editor@user:andres
document:4#parent@folder:1
document:5#viewer@user:*
folder:1#viewer@user:andres
group:eng#member@group:fga#member
group:fga#member@user:andres
```

## Example 1: ListUsers(object= document:1, relation= viewer, filter=[user])

In phase 1, we do the following:

1. using the directed graph above, determine the edges between a source node (`document#viewer`) and target node (`user`),
2. using the tuples in the store to traverse those edges, do a BFS of the graph. Along the way, we add to the response the objects that we find, or we recursively examine the usersets that we find.

### Iteration 1

We determine all the possible paths between the source `document#viewer` (in cyan) and the targets `user` and `user:*` type (in magenta). And then we look at the edges at distance 0 or 1 in those paths (from left to right, edges 6,3,4,5,7) and look at their tail node.

![example1-iter1](1.svg)

```go
// compute all paths, grab edges at distance 0 or 1, and grab their tails
Edges(document#viewer, user) → [document#editor, user, user:*, group#member, folder#viewer]
```

We have to explore each neighbor node separately. To do that, we iterate over tuples and find all of the objects connected from source userset `document:1#viewer` to each of those nodes. We do this using the ForwardExpand internal API: `ForwardExpand(source Userset, neighbor Userset) -> [Object or Usersets]`

```go
// find all tuples of form `document:1#viewer@document:...#editor`
a. ForwardExpand(document:1#viewer, document#editor) → []

// find all tuples of form `document:1#viewer@user:...`
b. ForwardExpand(document:1#viewer, user) → [ user:andres ]

// find 1 tuple `document:1#viewer@user:*`
c. ForwardExpand(document:1#viewer, user:*) → []

// find all tuples of form `document:1#viewer@group:...#member`
d. ForwardExpand(document:1#viewer, group#member) → []

// find all tuples of form `document:1#viewer@folder:...#viewer`
e. ForwardExpand(document:1#viewer, folder#viewer) → []
```

Next, we apply recursion on each result we got.

### Iteration 2

#### 2b. ForwardExpand(user:andres, user)

Here, both the source and target parameters are of the same type, which is the type of the original request. So, we can say we found a result `user:andres`. We can immediately add it to the list of results and terminate the recursion in this branch.

### Result
Since there were no leftover usersets that were pending further recursion, we can return a final response:

```go
ListUsers(object= document:1, relation= viewer, filter=[user]) → [user:andres]
```


## Example 2: ListUsers(object= document:2, relation= viewer, filter=[user])

### Iteration 1

It's a similar starting point as example 1, iteration 1:

```go
// find all tuples of form `document:2#viewer@document:...#editor`
a. ForwardExpand(document:2#viewer, document#editor) → []

// find all tuples of form `document:2#viewer@user:...`
b. ForwardExpand(document:2#viewer, user) →  []

// find 1 tuple `document:2#viewer@user:*`
c. ForwardExpand(document:2#viewer, user:*) → []

// find all tuples of form `document:2#viewer@group:...#member`
d. ForwardExpand(document:2#viewer, group#member) → [ group:eng#member ]

// find all tuples of form `document:2#viewer@folder:...#viewer`
e. ForwardExpand(document:2#viewer, folder#viewer) → []
```

### Iteration 2

#### 2d. ForwardExpand(group:eng#member, user)

![example2-iter2](2_d.svg)

```go
// compute all paths, grab edges at distance 0 or 1, and grab their tails
Edges(group#member, user) → [group#member, user]
```

We examine each neighbor node separately:

```go
// find all tuples of form `group:eng#member@group:...#member`
a. ForwardExpand(group:eng#member, group#member) → [group:fga#member]

// find all tuples of form `group:eng#member@user:...`
b. ForwardExpand(group:eng#member, user) → []
```

### Iteration 3

#### 3a. ForwardExpand(group:fga#member, user)

We examine each neighbor node separately:

```go
// find all tuples of form `group:fga#member@group:...#member`
a. ForwardExpand(group:fga#member, group#member) → []

// find all tuples of form `group:fga#member@user:...`
b. ForwardExpand(group:fga#member, user) → [user:andres]
```

### Iteration 4

#### 4a. ForwardExpand(user:andres, user)

Here, both the source and target parameters are of the same type, which is the type of the original request. So, we can say we found a result `user:andres`. We can immediately add it to the list of results and terminate the recursion in this branch.

### Result
Since there were no leftover usersets that were pending further recursion, we can return a final response:

```go
ListUsers(object= document:2, relation= viewer, filter=[user]) → [user:andres]
```