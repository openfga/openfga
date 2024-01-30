# Check
The [Check API](https://openfga.dev/api/service#/Relationship%20Queries/Check) looks up if a particular user/subject has specific relationship with a given object. 

It is a forward expansion algorithm that starts at a given `object#relation` and iteratively and recursively expands relationships until a particular user/subject is found or until all paths of the expansion have been visited and no such paths were found. It can be viewed as a tree traversal or a directed graph traversal. 

As relationships are expanded the Check algorithm follows relationship rewrite rules, and these rewrite rules define one or more paths that must be evaluated. For example, given the following FGA model:
```
model
  schema 1.1

type user

type folder
 relations
   define viewer: [user]

type document
  relations
    define parent: [folder]
    define editor: [user] or editor from parent
    define viewer: editor
```
The `document#viewer` relation is "rewritten" indirectly as `document#editor`, which means that in order to evaluate a specific relationship of the form `Check(document:1#viewer@user:jon)` we must actually rewrite that lookup as `Check(document:1#editor@user:jon)`. Similarly, the `document#editor` relationship is rewritten using a set union (e.g. `or` keyword) such that if a user is either directly an editor of the document or an editor on the parent folder, then the user can edit the document. This kind of rewritten behavior allows us to model hierarchical semantics. These "rewrites" make up the base-line behavior behind the semantics of the FGA graph traversal, and we explain each of them with examples in further detail below.

## Direct Rewrites
Direct rewrites establish the most basic form of FGA relationship rewrite definitions. These direct rewrites allow developers to express the most basic of authorization model semantics, including direct permissions, indirect or computed permissions, and/or hierarchical permissions.

> ℹ️ The term "permission" is used above, but from here forward we will formally refer to permissions simply as relationships. A users/subject has a permission if they have the relationship, and a relationship can be realized through one or more relationship rewrites.

### Direct Relationships
> e.g. define viewer: [user]
> e.g. define viewer: [group#member]

If a relation can be directly assigned or given/shared with a specific user/subject, then we refer to it as a "direct relationship". Direct relationships are any of those relationships that, for a particular authorization model, can be written to an [FGA Store](https://openfga.dev/docs/concepts#what-is-a-store) using the [Write API](https://openfga.dev/api/service#/Relationship%20Tuples/Write). Looking at the model below, the `document#owner` relationship is a direct relationship, because `user` objects can be directly written/assigned to document owners. This is evident by the defined `[user]` type restriction on the owner relation. Similarly, the `document#viewer` relationship is a direct relationship, because any set of users/subjects of the form `group#member` can be directly written/assigned to document viewer.

```
model
  schema 1.1

type user

type group
  relations
    define member: [user, group#member]

type document
  relations
    define owner: [user]
    define viewer: [group#member]
```

Evaluation of direct relationships involves a direct database lookup. If the user/subject is a set of subjects, then further expansion must occur. For example, consider the following tuples:

| object     | relation | user             |
|------------|----------|------------------|
| document:1 | owner    | user:jon         |
| document:1 | viewer   | group:fga#member |
| group:fga  | member   | user:andres      |

Evaluation of the query `Check(document:1#owner@user:jon)` would involve a direct lookup involving the following callstack:

```
Server#Check(document:1#owner@user:jon)
|--> LocalChecker#ResolveCheck(document:1#owner@user:jon)
|-----> LocalChecker#checkDirect(document:1#owner@user:jon)
|--------> storage#ReadUserTuple(document:1#owner@user:jon) --> ["document:1#owner@user:jon"]
|--------> return {allowed: true} // found a direct resolution path 
```

Evaluation of the query `Check(document:1#owner@user:bob)` would involve the following callstack:

```
Server#Check(document:1#owner@user:bob)
|--> LocalChecker#ResolveCheck(document:1#owner@user:bob)
|-----> LocalChecker#checkDirect(document:1#owner@user:bob)
|--------> storage#ReadUserTuple(document:1#owner@user:bob) --> []
|--------> return {allowed: false} // no direct resolution path found
```

Evaluation of `Check(document:1#viewer@user:andres)` would involve a little further expansion, for example:
```
Server#Check(document:1#viewer@user:andres)
|--> LocalChecker#ResolveCheck(document:1#viewer@user:andres)
|-----> LocalChecker#checkDirect(document:1#viewer@user:andres)
|--------> storage#ReadUsersetTuples(document:1#viewer) --> ["document:1#viewer@group:fga#member"]
|--------> LocalChecker#ResolveCheck(group:fga#member@user:andres) <-- dispatch a new Check subproblem
|-----------> LocalChecker#checkDirect(group:fga#member@user:andres)
|--------------> storage#ReadUserTuple(group:fga#member@user:andres) --> ["group:fga#member@user:andres"]
|--------------> return {allowed: true} // found resolution path through group#member relationship
|--------------> storage#ReadUsersetTuples(group:fga#member) --> []
```
In the example immediately above we had to follow an intermediate relationship through `group:fga#member` in order to find `user:andres`. When we have to start evaluation of a new subproblem to determine graph reachability, we refer to this process as "dispatching". Dispatching is when we have to dispatch resolution of a new subproblem in order to evaluate another subproblem.

> ℹ️ We'll use the callstack format above to demonstrate resolution of various Check queries from here forward. These callstacks are defined using the format `package#method` so that you can follow along in the code more easily if you want to learn more by looking at the code.

### Computed Relationships (e.g. computed_userset)
> e.g. define viewer: editor

Evaluation of computed relationships involves an indirect evaluation of the computed relation, and the computed relation may involve one or more other rewritten relationships not just direct relationships. For example, consider the following model and tuples:

```
model
  schema 1.1

type user

type document
  relations
    define owner: [user]
    define editor: [user] or owner
    define viewer: editor
```

| object     | relation | user        |
|------------|----------|-------------|
| document:1 | owner    | user:jon    |
| document:1 | editor   | user:andres |

In this model the `document#viewer` relation is purely rewritten as `document#editor`, which means that a user/subject *must* be an editor to also be a viewer, and the evaluation of the `document#editor` relation involves a union (see [union](#union) below for more info) of either a direct relationship `document#editor` or an owner relationship `document#owner`. That is, if a user/subject is either a direct editor of a document or the owner of it, then they have the editor relationship and therefore the viewer relationship as well.

Evaluation of the query `Check(document:1#viewer@user:jon)` would involve the following callstack:

```
Server#Check(document:1#viewer@user:jon)
|--> LocalChecker#ResolveCheck(document:1#viewer@user:jon)
|-----> LocalChecker#checkComputedUserset(document:1#viewer@user:jon)
|--------> LocalChecker#ResolveCheck(document:1#editor@user:jon) <-- notice the rewritten ResolveCheck
|-----------> LocalChecker#union
|--------------> LocalChecker#checkDirect(document:1#editor@user:jon)
|-----------------> storage#ReadUserTuple(document:1#editor@user:jon) --> []
|--------------> LocalChecker#checkDirect(document:1#owner@user:jon)
|-----------------> storage#ReadUserTuple(document:1#owner@user:jon) --> ["document:1#owner@user:jon"]
|-----------------> return {allowed: true} // found a resolution path  through 'document:1#owner'
```
Notice that since `user:jon` is the owner of the document he is also a viewer indirectly through the computed relationship.

Evaluation of the query `Check(document:1#viewer@user:andres)` would involve the following callstack:

```
Server#Check(document:1#viewer@user:andres)
|--> LocalChecker#ResolveCheck(document:1#viewer@user:andres)
|-----> LocalChecker#checkComputedUserset(document:1#viewer@user:andres)
|--------> LocalChecker#ResolveCheck(document:1#editor@user:andres) <-- notice the rewritten ResolveCheck
|-----------> LocalChecker#union
|--------------> LocalChecker#checkDirect(document:1#editor@user:andres)
|-----------------> storage#ReadUserTuple(document:1#editor@user:andres) --> ["document:1#editor@user:andres"]
|-----------------> return {allowed: true} // found a resolution path  through 'document:1#editor'
|--------------> LocalChecker#checkDirect(document:1#owner@user:andres)
|-----------------> storage#ReadUserTuple(document:1#owner@user:andres) --> []
```
Notice that since `user:andres` is an editor of the document he is also a viewer indirectly through the computed relationship.

### Hierarchical Relationships (e.g. tuple_to_userset or TTU)
> e.g. define viewer: viewer from parent

Hierarchical relationships can be expressed in the FGA modeling language 


## Set Rewrites
Set rewrites allow developers to compose one or more of the direct or basic rewrites mentioned above using set operations including union, intersection, and exclusion. These set rewrites allow developers to model more complex model semantics such as multiple relationships granting access and/or blacklists and the like. 

### Union
> e.g. define viewer: editor or owner

### Intersection
> e.g. define viewer: [user] and allowed

### Exclusion (Difference)
> e.g. define viewer: [user] but not restricted

## Advanced Behavior

### Cycle Detection

### Concurrency Control (Depth and Breadth)

## Code References

* [Server#Check](https://github.com/openfga/openfga/blob/ad04038afbd58890cb65b409780b0cbbf85d5103/pkg/server/server.go#L582) - main OpenFGA server's entrypoint for the Check API, but quickly delegates to `LocalChecker#ResolveCheck`

* [LocalChecker#ResolveCheck](https://github.com/openfga/openfga/blob/ad04038afbd58890cb65b409780b0cbbf85d5103/internal/graph/check.go#L444) - internal implementation of the Check resolution algorithm.

* [LocalChecker#CheckHandlerFunc](https://github.com/openfga/openfga/blob/ad04038afbd58890cb65b409780b0cbbf85d5103/internal/graph/check.go#L179) - Check rewrite handler function signature.

* [LocalChecker#checkDirect](https://github.com/openfga/openfga/blob/ad04038afbd58890cb65b409780b0cbbf85d5103/internal/graph/check.go#L501) - Check rewrite handler for direct relationships.

* [LocalChecker#checkComputedUserset](https://github.com/openfga/openfga/blob/ad04038afbd58890cb65b409780b0cbbf85d5103/internal/graph/check.go#L717) - Check rewrite handler for computed relationships.

* [Storage#ReadUserTuple](https://github.com/openfga/openfga/blob/ad04038afbd58890cb65b409780b0cbbf85d5103/pkg/storage/storage.go#L91) - direct database/storage tuple lookup 

* [Storage#ReadUsersetTuples](https://github.com/openfga/openfga/blob/ad04038afbd58890cb65b409780b0cbbf85d5103/pkg/storage/storage.go#L106) - lookup all user/subject sets related to a particular object and relation (e.g. userset lookup)