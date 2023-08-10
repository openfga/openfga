# ListObjects API implementation

At a high level, answering ListObjects queries involves a reverse expansion algorithm. Thinking of an authorization model as a directed graph and the tuples as the way of "moving" through that graph, we start the search from a specific object and explore (reverse expand)  all the paths that can lead to the target object type and relation. During this expansion, we add to the final response all the concrete objects that we find that are of the target type. And if we discover usersets that don't match the target type and relation, we process those further.

## Example
Consider the following model:

```
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

We can represent this with a graph of types (e.g. `user`) and usersets (eg `user:*`):

<!--
digraph G {
    
  rankdir=BT

  document
  
  group
  
  user -> "document#viewer"
  
  user -> "group#member"
  
  "group#member" -> "group#member"
  
  "group#member" -> "document#viewer"
  
  user -> "document#editor"
  
  "document#editor" -> "document#viewer" [style=dotted]
  
  user -> "folder#viewer"
  
  "folder#viewer" -> "document#viewer"  [style=dashed]
  
  folder -> "document#parent"
  
  "folder#viewer" -> "document#parent"
  
  "user:*" -> "document#viewer"
}
-->

![model](model.svg)

(Solid lines represent direct relationships. Dotted lines represent relationships via computed usersets. Dashed lines represent relationships via tuple-to-usersets)

And consider a store with the following 8 tuples: 

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

Answering a ListObjects query like `ListObjects(type= document, relation= viewer, user= user:andres)` consists of applying the following steps:

1. using the directed graph above, determine the edges between a source node and target node,
2. using the tuples in the store to traverse those edges, explore the graph. Here, we either add found objects to the response or we add usersets to a list of that needs further expansion.

### Iteration 1

We determine all the possible paths (in red) between the source `user:andres` (in light blue) and the target `document#viewer` userset (in magenta).

<!--
digraph G {
    
    rankdir=BT
    
  user [style=filled,fillcolor=lightblue]

  document
  
  group
  
  "user:*" -> "document#viewer"
  
  "document#viewer" [style=filled,fillcolor=magenta]

  user -> "group#member" [color=red]
  
  "group#member" -> "group#member" [color=red]
  
  "group#member" -> "document#viewer" [color=red]
  
  user -> "document#editor" [color=red]
  
  user -> "document#viewer" [color=red]
  
  "document#editor" -> "document#viewer" [style=dotted, color=red]
  
  user -> "folder#viewer" [color=red]
  
  "folder#viewer" -> "document#viewer"  [style=dashed, color=red]
  
  folder -> "document#parent" 
  
  "folder#viewer" -> "document#parent"
}
-->

![step1](step1.svg)

Thinking of it as a breadth-first search, there are 4 edges leaving `user` and arriving to the following usersets:

- `group#member`
- `document#editor`
- `folder#viewer`
- `document#viewer`

So, we have to iterate over tuples and find all of the objects connected from source object `user:andres` to each of those four usersets. We do this using the ConnectedObjects internal API: `ConnectedObjects(source Object or Userset, target Userset) -> [Objects or Usersets]`

Since the four edges mentioned are solid lines (i.e. direct relations), they are straightforward to compute:

```go
// find all tuples of form `group:...#member@user:andres`
a. ConnectedObjects(user:andres, group#member) → [group:fga#member]

// find all tuples of form `document:...#viewer@user:andres`
b. ConnectedObjects(user:andres, document#viewer) → [document:1#viewer]

// find all tuples of form `document:...#editor@user:andres`
c. ConnectedObjects(user:andres, document#editor) → [document:3#editor]

// find all tuples of form `folder:...#viewer@user:andres`
d. ConnectedObjects(user:andres, folder#viewer) → [folder:1#viewer]
```

Notice how there is also an edge from `user:*` (remember our source user was `user:andres`) to `document#viewer`.

<!--
digraph G {
    
    rankdir=BT
    
  user
  
  document
  
  group
  
  "user:*" -> "document#viewer" [color=red]
  
  "document#viewer" [style=filled,fillcolor=magenta]

  user -> "group#member"
  
  "group#member" -> "group#member"
  
  "group#member" -> "document#viewer" 
  
  user -> "document#editor"
  
  user -> "document#viewer" 
  
  "document#editor" -> "document#viewer" [style=dotted]
  
  user -> "folder#viewer"
  
  "folder#viewer" -> "document#viewer"  [style=dashed]
  
  folder -> "document#parent" 
  
  "folder#viewer" -> "document#parent"
  
  "user:*"  [style=filled,fillcolor=lightblue]
}
-->

![step1_a](step1_a.svg)

So we have to examine this too, which is simple because it's a direct relation:

```go
// find all tuples of form `document:...#viewer@user:*`
e. ConnectedObjects(user:*, document#viewer) → [document:5#viewer]
```

Next, we apply recursion on each result we got.

### Iteration 2

#### 2a. ConnectedObjects(group:fga#member, document#viewer)

<!-- 
digraph G {
    
    rankdir=BT
    
  user
  
  "group#member" [style=filled,fillcolor=lightblue]

  document
  
  group
  
  "user:*" -> "document#viewer"
  
  "document#viewer" [style=filled,fillcolor=magenta]

  user -> "group#member"
  
  "group#member" -> "group#member" [color=red]
  
  "group#member" -> "document#viewer" [color=red]
  
  user -> "document#editor"
  
  user -> "document#viewer"
  
  "document#editor" -> "document#viewer" [style=dotted]
  
  user -> "folder#viewer"
  
  "folder#viewer" -> "document#viewer"  [style=dashed]
  
  folder -> "document#parent" 
  
  "folder#viewer" -> "document#parent"
}
-->

![step2_a](step2_a.svg)

The paths from specific object `group:fga#member` to userset `document#viewer` can be via tuples of the form 

1. `document:...#viewer@group:fga#member` (i.e. a direct relation), or
2. `document:...#viewer@group:X#member` and `group:X#member@group:fga#member` (i.e. an indirect relation via nesting of groups)

There are no tuples that satisfy (1). But for (2), the following tuples exist: `group:eng#member@group:fga#member` and `document:2#viewer@group:eng#member`. So,

```go
ConnectedObjects(group:fga#member, document#viewer) → [group:eng#member]
```

We will have to recurse through userset `group:eng#member` in a next iteration because its type and relation don't match the target `document#viewer`.

#### 2b. ConnectedObjects(document:1#viewer, document#viewer)

Here, both the source and target parameters are usersets of the same type and relation. So, we can say we found an object `document:1`. We can immediately add it to the list of results and terminate the recursion in this branch.

```go
ConnectedObjects(document:1#viewer, document#viewer) → [document:1]
```

#### 2c. ConnectedObjects(document:3#editor, document#viewer)

<!--
digraph G {
    
    rankdir=BT
    
  user
  
  "group#member"

  document
  
  "document#editor"  [style=filled,fillcolor=lightblue]
  
  group
  
  "document#viewer" [style=filled,fillcolor=magenta]

  user -> "group#member"
  
  "group#member" -> "group#member" 
  
  "group#member" -> "document#viewer"
  
  user -> "document#editor"
  
  user -> "document#viewer"
  
  "document#editor" -> "document#viewer" [style=dotted, color=red]
  
  user -> "folder#viewer"
  
  "folder#viewer" -> "document#viewer"  [style=dashed]
  
  folder -> "document#parent" 
  
  "folder#viewer" -> "document#parent"
  
  "user:*" -> "document#viewer"
}
-->

![step2_c](step2_c.svg)

The edge from `document#editor` to `document#viewer` is a computed userset relation (dotted red line). 

This path is special. It means that all editors are also viewers. So we need to find tuples of the form `document:..#editor@user:andres`.

Since we have a tuple `document:3#editor@user:andres` we can add `document:3` to the response and recursion in this branch stops here.

```go
ConnectedObjects(document:3#editor, document#viewer) → [document:3]
```

#### 2d. ConnectedObjects(folder:1#viewer, document#viewer)

<!--

digraph G {
    
    rankdir=BT
    
  "folder#viewer"   [style=filled,fillcolor=lightblue]
  
  "document#viewer" [style=filled,fillcolor=magenta]

  user -> "group#member"
  
  "group#member" -> "group#member" 
  
  "group#member" -> "document#viewer"
  
  user -> "document#editor"
  
  user -> "document#viewer"
  
  "document#editor" -> "document#viewer" [style=dotted]
  
  user -> "folder#viewer"
  
  "folder#viewer" -> "document#viewer"  [style=dashed, color=red]
  
  folder -> "document#parent" 
  
  "folder#viewer" -> "document#parent"
  
  "document#parent"  [style=filled,fillcolor=gray]
  
  document
    
  group
}

--> 

![step2_d](step2_d.svg)

The edge from the source `folder#viewer` (light blue) and the target `document#viewer` (magenta) is a tuple-to-userset rewrite (dashed red line) through the `document#parent` tupleset (gray). This means that if someone can view a folder, and the document's parent is that folder, they can also view the document. 

In other words: for any `folder:...#viewer@user:andres` tuple we find, if that `folder` also has a `parent` relation with some `document`, then we can deduce that the `document#viewer` relation exists. 

So, we search for tuples and find `folder:1#viewer@user:andres`. Then we search for tuples of the form `document:...#parent@folder:1`. We find tuple `document:4#parent@folder:1`. So we can add `document:4` to the response and recursion in this branch stops here.

```go
 ConnectedObjects(folder:1#viewer, document#viewer) → [document:4]
```

#### 2e. ConnectedObjects(document:5#viewer, document#viewer)

Here, similar to step 2b, both the source and target parameters are usersets of the same type and relation. So, we can say we found an object `document:5`. We can immediately add it to the list of results and terminate the recursion in this branch.

```go
ConnectedObjects(document:5#viewer, document#viewer) → [document:5]
```

### Iteration 3

In this iteration we examine usersets left from the previous iteration: `group:eng#member`.

#### 3a. ConnectedObjects(group:eng#member, document#viewer)

<!-- 
digraph G {
    
    rankdir=BT
    
  "group#member"  [style=filled,fillcolor=lightblue]
    
  "folder#viewer" 
  
  "document#viewer" [style=filled,fillcolor=magenta]

  user -> "group#member"
  
  "group#member" -> "group#member" [color=red]
  
  "group#member" -> "document#viewer"  [color=red]
  
  user -> "document#editor"
  
  user -> "document#viewer"
  
  "document#editor" -> "document#viewer" [style=dotted]
  
  user -> "folder#viewer"
  
  "folder#viewer" -> "document#viewer"  [style=dashed]
  
  folder -> "document#parent" 
  
  "folder#viewer" -> "document#parent"
  
  "user:*" -> "document#viewer"
}
-->

![step3_a](step3_a.svg)

Similar to step 2a, the paths from specific object `group:eng#member` to userset `document#viewer` can be via tuples of the form

1. `document:...#viewer@group:eng#member` (i.e. a direct relation), or
2. `document:...#viewer@group:X#member` and `group:X#member@group:eng#member` (i.e. an indirect relation via nesting of groups)

For (2), we lookup tuples of the form `group:...#member@group:eng#member`. There aren't any.

For (1), we lookup tuples of the form `document:...#viewer@group:eng#member`. We find one: `document:2#viewer@group:eng#member`. Therefore, we add `document:2` to the response and stop recursion of this branch.

```go
ConnectedObjects(group:eng#member, document#viewer) → [document:2]
```

## Conclusion

Since there were no leftover usersets that were pending further recursion, we can return a final response:

```go
ListObjects(type= document, relation= viewer, user= user:andres) → [document:1, document:2, document:3, document:4, document:5]
```