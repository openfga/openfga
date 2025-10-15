<!-- Use the following information to analyze test gaps-->

# **Authoring OpenFGA Models**

This guide provides a comprehensive overview of authoring OpenFGA authorization models. It covers core concepts, the modeling language, relationship definitions, and testing methodologies, drawing insights from the openfga/sample-stores repository for practical examples.

## **1. Introduction to OpenFGA and Authorization Modeling**

OpenFGA is an open-source authorization solution that empowers developers to implement fine-grained access control within their applications through an intuitive modeling language.

It functions as a flexible authorization engine, simplifying the process of defining application permissions.

Inspired by Google's Zanzibar paper, OpenFGA primarily champions Relationship-Based Access Control (ReBAC), while also effectively addressing use cases for Role-Based Access Control (RBAC) and Attribute-Based Access Control (ABAC). Its "developer-first" philosophy is evident in its Domain Specific Language (DSL) and supporting tools, which lower the barrier to entry for developers.

The core purpose of an authorization model is to define a system's permission structure, answering questions like, "Can user U perform action A on object O?". By externalizing authorization logic from application code, OpenFGA provides a robust mechanism for managing complex access policies, especially in large-scale systems. The modeling language is designed to be powerful for engineers yet accessible to other team stakeholders, fostering collaborative policy development.

## **2. OpenFGA Core Concepts: The Building Blocks of Your Model**

To effectively model authorization in OpenFGA, it is essential to understand its core building blocks:

* **Authorization Model:** A static blueprint that combines one or more type definitions to precisely define the permission structure of a system. It represents the  
  *possible* relations between users and objects. Models are immutable; each modification creates a new version with a unique ID. Models aren't expected change often - only when new product features or change in functionality is introduced. They're also generally expected to be backward compatible, but can break backward compatibility once the system has completely moved off the older relations in it.
* **Type:** A string that defines a class of objects sharing similar characteristics (e.g., user, document, folder, organization, team, repo).
* **Object:** A specific instance of a defined Type (e.g., `document:roadmap`, `user:anne`, `organization:acme`). An object's relationships are defined through relationship tuples and the authorization model.
* **User:** An entity that can be related to an object. A user can be a specific individual (e.g., user:anne), a wildcard representing everyone of a certain type (e.g. `user:*` which means all users), or a userset (e.g., `team:product#member`), which denotes a group of users.
* **Relation:** A string defined within a type definition in the authorization model. It specifies the *possibility* of a relationship existing between an object of that type and a user. Relation names are arbitrary (e.g., owner, editor, viewer, member, admin), but must be defined on the object type in the model.
* **Relationship Tuple:** Dynamic data elements representing the *facts* about relationships between users and objects (e.g., {"user": "user:anne", "relation": "editor", "object": "document:new-roadmap"}).3 Without these, authorization checks will fail, as the model only defines what is *possible*, not what *currently exists*.

The clear separation between the static authorization model (schema) and dynamic relationship tuples (data) is a fundamental design principle. This enables efficient permission evaluation and decouples core logic changes from specific user permission modifications. The immutability of models supports robust versioning, allowing for controlled rollouts and managing complex migrations.

The following table summarizes these core concepts:

| Concept | Description | Example |
| :---- | :---- | :---- |
| **Type** | A class of objects that share similar characteristics. | user, document, folder, organization |
| **Object** | A specific instance of a defined type, an entity in the system. | `user:anne`, `document:report_2023`, `folder:marketing_docs` |
| **User** | An entity that can be related to an object. Can be a specific object, a wildcard, or a userset. | `user:bob`, `user:*` (everyone), `team:product#member` |
| **Relation** | A string defined within a type definition that specifies the possibility of a relationship between an object of that type and a user. | owner, editor, viewer, member, admin |
| **Relationship Tuple** | A grouping of a user, a relation, and an object, representing a factual relationship in the system. | `{"user": "user:anne", "relation": "viewer", "object": "document:roadmap"}` |
| **Authorization Model** | A static definition combining type definitions to define the entire permission structure of a system. | ```type document relations define viewer: [user]``` |

## **3. The OpenFGA Modeling Language: DSL**

OpenFGA's Configuration Language is fundamental to constructing a system's authorization model, informing OpenFGA about object types and their relationships. It describes all possible relations for an object of a given type and the conditions under which one entity is related to that object. The language is primarily expressed in DSL (Domain Specific Language).

### **DSL: Developer-Friendly Syntax for Readability**

The DSL provides syntactic sugar over the underlying JSON, designed for ease of use and improved readability.10 It is the preferred syntax for developers using the Playground, CLI, and IDE extensions (like Visual Studio Code), offering features like syntax highlighting and validation.5 DSL models are compiled to JSON before being sent to OpenFGA's API.5

An example of an OpenFGA model in DSL:

```dsl.openfga
model  
  schema 1.1  
type user  
type document  
  relations  
  define viewer: [user] or editor  
  define editor: [user]
```

## **4  Defining Relationships: Crafting Your Authorization Logic**

OpenFGA provides a rich set of constructs for defining relationships, enabling the modeling of complex authorization policies.

### **Direct Relationships: Explicit Access Grants**

A direct relationship is established when a specific relationship tuple (e.g., user=X, relation=R, object=Y) is explicitly stored. The authorization model must explicitly permit this through direct relationship type restrictions. These restrictions define which types of users can be directly associated with an object for a given relation, using formats like

`[<type>`], `[<type:*>]`, or `[<type>#<relation>]`.

For example,

```
define owner: [user] 
```

means only individual users can be directly assigned as owners.

A tuple like:

```
{"user": "user:anne", "relation": "owner", "object": "document:1"} 
```

is a direct relationship.

### **Concentric Relationships: Inheriting Permissions**

Concentric relationships represent nested or implied relations, where one relation automatically confers another (e.g., "all editors are viewers").This is implemented using the or keyword within a relation definition.

For example,

```
define viewer: [user] or editor
```

means a user is a viewer if directly assigned OR if they are an editor. `user:anne` is an editor of `document:new-roadmap`, she implicitly has viewer access, reducing the number of required tuples.

### **Indirect Relationships with 'X from Y': Scalable Hierarchical and Group-Based Access**

The X from Y syntax is crucial for scalability, allowing a user to acquire a relation (X) to an object through an intermediary object (Y) and a defined relation on Y. This avoids individual tuple creation for every permission, enabling higher-level abstraction. It is highly effective for hierarchical and group-based access control. For example,

```
define admin: [user] or repo_admin from organization 
```

on a repo type means a user is an admin if directly assigned, or if they have the repo_admin relation to an org that owns the repo.

This simplifies management; revoking access can be done by deleting a single tuple linking the intermediary.

### **Contextual Authorization with Conditions: Dynamic Permissions**

Conditions introduce dynamic, contextual authorization. A condition is a function using Google's Common Expression Language (CEL), with parameters and a boolean expression.

Example:
```
condition less_than_hundred(x: int) { 
  x < 100 
}
```

Conditions are required to be defined at the end of the model (after the type definitions), and are instantiated using conditional relationship tuples.

### **Leveraging Usersets for Group-Based Access Control**

A userset represents a set or collection of users, denoted by object#relation (e.g., `company:xy#employee`). Usersets are fundamental for assigning permissions to groups, reducing tuple count and providing flexibility for bulk access management. They can be used in direct relationship type restrictions, such as:

```
define editor: [user, team#member]
```

OpenFGA computes implied relationships based on userset membership, and usersets are integral to defining complex access rules involving union, intersection, or exclusion of groups.

Note that specifying `team#member` means "all members from a specific team". It does not mean that "you need to be a team member to be an editor". Only use it when you need to assign a relation to a set of users from specific object.

OpenFGA's strength lies in its capacity to construct complex authorization logic from foundational elements: direct relationships, concentric relationships (or), and indirect relationships (X from Y). This compositional approach 10 enables modeling intricate real-world permission structures efficiently.

The following table summarizes the key relationship definition patterns in OpenFGA:

| Pattern Name                           | Description                                                                                                                                                                         | DSL Syntax Example                              | Explanation of Effect                                                                                  |
|:---------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:------------------------------------------------|:-------------------------------------------------------------------------------------------------------|
| **Direct Relationship**                | Explicitly grants a user a relation to an object via a stored tuple, subject to type restrictions.                                                                                  | `define owner: [user]`                          | Only individual users can be directly assigned as owner.                                               |
| **Concentric Relationship**            | Defines that having one relation implies having another relation to the same object (e.g., editors are viewers).                                                                    | `define viewer: [user] or editor`               | A user is a viewer if directly assigned as viewer OR if they are an editor.                            |
| **Indirect Relationship ('X from Y')** | A user gains a relation (X) to an object through another object (Y) and a specific relation on Y. This is known as tuple to userset.                                                | `define admin: [user] or repo_admin from owner` | A user is admin of a repo if directly assigned OR if they are repo_admin of an org that owns the repo. |
| **Conditional Relationship**           | A relationship is permissible only if a specified condition, evaluated at runtime, is true.                                                                                         | `define admin: [user with non_expired_grant]`   | A user is admin only if the non_expired_grant condition evaluates to true for their context.           |
| **Usersets**                           | Represents a collection of users (e.g., a group or a set of users related by a specific relation).                                                                                  | `define editor: [user, team#member]`            | An editor can be a direct user OR any member of a specified team.                                      |
| **Union**                              | The union operator (or in the DSL, union in the JSON syntax) indicates that a relationship exists if the user is in any of the sets of users (union).                               | `define viewer: [user] or editor`               | A user is a viewer if they are directly assigned as a viewer OR if they are an editor.                 |
| **Intersection**                       | The intersection operator (and in the DSL, intersection in the JSON syntax) indicates that a relationship exists if the user is in all the sets of users.                           | `define viewer: authorized_user and editor`     | A user is a viewer if they are an authorized_user AND they are an editor.                              |
| **Exclusion**                          | The exclusion operator (but not in the DSL, difference in the JSON syntax) indicates that a relationship exists if the user is in the base userset but not in the excluded userset. | `define viewer: [user] but not blocked`         | A user is a viewer if they are directly assigned as a viewer AND they are NOT blocked.                 |

In this specific example,
```dsl.openfga
define admin: [user with non_expired_grant]
```
Admin is a direct relationship with a condition. A user can be an admin only if they have a direct relationship with the object and the condition `non_expired_grant` evaluates to true for their context.


### Special Syntax: Type Bound Public Access ###

In OpenFGA, type bound public access, also known as public wildcard, (represented by <type>:*) is a special OpenFGA syntax meaning "every object of [type]" when invoked as a user within a relationship tuple. For example, any user of type `user` can be a viewer document if the relationship tuple is defined as:

```dsl.openfga
model  
  schema 1.1  
type user
type document  
  relations  
  define viewer: [user:*] or editor  
  define editor: [user]
```


### DSL Requirement ###
1. Specify all direct relationship and usersets before all other relations.
2. Tuple to userset parent cannot be a userset. For example, the following is invalid:

```dsl.openfga
model  
  schema 1.1  
type user
type team
  relations
    define member: [user]
type organization
 relations
   define member: [team]
type document
  relations  
    define badParent : [organization#member] # An organization owns a document.
    define relation3: member from badParent  # Invalid because badParent is a userset.
```
3. A relationship can have multiple union, intersection and exclusion operator. They MUST have bracket if there is a mixture of types. For example, the following is valid:

```dsl.openfga
model
    schema 1.1
type user
type group
    relations
        define member: [user]
type document
    relations
        define viewer: ([user] or commentor) and not banned # valid because there is a bracket.
        define banned: [user]
        define owner: [user, group#member]
        define commentor: [user] or owner or editor # valid because there is no mixture of types.
        define editor: [user]
```
The following is invalid because there is no bracket:

```dsl.openfga
model
    schema 1.1
type user
type group
    relations
        define member: [user]
type document
    relations
        define viewer: [user] or owner and not banned  # Invalid because there is no bracket.
        define banned: [user]
        define owner: [user, group#member]
```
4. Parent of tuple to userset cannot be a public wildcard. For example, the following is invalid:

```dsl.openfga
model  
  schema 1.1  
type group
    relations
      define member: [user]
type document
  relations  
    define badParent : [group:*] 
    define relation3: member from badParent  # Invalid because badParent is a public wildcard
```
5. Direct Relationship, Usersets and Conditional Relationship must be the first items in union, intersection and exclusion operator. For example, the following is valid:

```dsl.openfga
model
    schema 1.1
type user
type group
    relations
        define member: [user]
type document
    relations
        define viewer: ([user] or commentor) and not banned # valid because direct relationship and userset are the first items in union, intersection and exclusion operator.
        define banned: [user]
        define owner: [user, group#member]
``
The following is invalid because direct relationship and userset are not the first items in union, intersection and exclusion operator:

```dsl.openfga
model
    schema 1.1
type user
type group
    relations
        define member: [user]
type document
    relations
        define viewer: (commentor or [user]) and not banned # invalid because direct relationship and userset are not the first items in union, intersection and exclusion operator.
        define banned: [user]
        define owner: [user] but not [group#member] # invalid because group#member are not the first items in union, intersection and exclusion operator.
```
In the above example, `viewer` is not valid because directly assignable type `user` is not specified first.
In the above example, `owner` is not valid because userset `group#member` is not specified first.


## **5  Defining Model Complexity: Using Weighted Graph**

OpenFGA models can vary in complexity, which can impact performance and maintainability. To help developers assess and manage this complexity, OpenFGA introduces a weighted graph approach to model complexity analysis.
For a check or list object requests, there needs to be a path from the object's relationship to the user type.

For example, in the following model:

```dsl.openfga
model  
  schema 1.1  
type user
type employee
type organization
  relations
    define member: [user]
type team
  relations
    define member: [employee]
type document
  relations  
    define parent : [organization] # An organization owns a document.

    define relation1: [user]  # A direct user 
    define relation2: [organization#member]  
    define relation3: member from parent
    define relation4: [employee]
    define relation5: member from team
```

In the above example, type `user` has a path to the following relations with object `document`
- `relation1` because it has a direct path to `user` type, so it has a weight of 1.
- `relation2` because it has a path to `organization` type, which has a direct path to `user` type, so it has a weight of 2.
- `relation3` because it has a path to `organization` type, which has a direct path to `user` type, so it has a weight of 2.

It does not have a path to the following relations with object `document`
- `relation4` because it has a path to `employee` type, which does not have a path to `user` type.
- `relation5` because it has a path to `team` type, which has a path to `employee` type, which does not have a path to `user` type.

### Calculating Weights ###
Weights are calculated based on the longest path from the object's relationship to the user type. The weights are assigned as follows:
- **Weight 1:** Direct relationship to the user type (e.g., `define relation1: [user]`).
- **Weight 2:** One intermediary type between the object's relationship and the user type (e.g., `define relation2: [organization#member]`).
- **Weight 3:** Two intermediary types between the object's relationship and the user type.
- **Weight Infinite:** Any userset or indirect relationship that refers back to itself and forms a loop.
  Adding weight of for each intermediary type in the path. This is also known as recursion model.

### What makes a good test ###
Ideally, the model will have different complexities.  This requires types and relations with varying weights and varying orders. For example:
- Different weight: create models with weights 1, 2, 3, 4, and infinite.  You can add more weights after infinite, but they will all be treated as infinite.
- Different order: create models with the same weight but different orders of relations. For example,
    - Model A: `relation1`, `relation2`, `relation3`
    - Model B: `relation3`, `relation2`, `relation1`
    - Model C: `relation2`, `relation1`, `relation3`
- Different number of relations: create models with the same weight and order but different number of relations. For example,
    - Model A: `relation1`, `relation2`, `relation3`
    - Model B: `relation1`, `relation2`, `relation3`, `relation4`
- Different number of parents for tuple to userset. For example,

```dsl.openfga
model  
  schema 1.1  
type user
type organization
  relations
    define member: [user]
type team
  relations
    define member: [user]
type document
  relations  
    define parent1 : [organization]
    define parent2 : [team, organization]
    define relation1: member from parent1
    define relation2: member from parent2
```
In the above example, `relation1` has 1 parent while `relation2` has 2 parents. Both have a weight of 2 because they both have a path to `organization` type, which has a direct path to `user` type.
- Different combination of userset and tuple to userset.
- Different combination of union, intersection and exclusion operators.
- With and without conditions
- With and without type bound public access
- With and without Concentric Relationship
- With and without Direct Relationship
- With and without Indirect Relationship
- With and without Usersets
- With and without Conditional Relationship
- With and without Exclusion
- With and without Intersection
- With and without Union
- Different combination of weight for each child of union, intersection and exclusion operators for each weight. This is especially important. Make sure there are test cases for union, intersection and exclusion in addition to the mixes of these conditions.
- Different order of weight for direct relationship and userset. For example, create models with the same weight but different orders of direct relationship and userset. For example,
    - Model A: `define viewer: [user, organization#member]`
    - Model B: `define viewer: [organization#member, user]`
    - Model C: `define viewer: [organization#member, team#owner]` where `team#owner` is weight 3 and `organization#member` is weight 2.
    - Model D: `define viewer: [team#owner, organization#member]` where `team#owner` is weight 3 and `organization#member` is weight 2.
- Different number of parents in a tuple to userset. Add different combination of weight for each parent. Test this for both union, intersection and exclusion. For example, create models with the same weight but different number of parents in a tuple to userset. For example,
    - Model A: `define relation1: member from parent1` where `parent1` has 1 parent.
    - Model B: `define relation2: member from parent2` where `parent2` has 2 parents.
    - Model C: `define relation3: member from parent3` where `parent3` has 3 parents.
    - Model D: `define relation4: member from parent4` where `parent4` has 4 parents.
- Different order of parents in a tuple to userset. For example, different order of parents in a tuple to userset where each parent has different weight. For example,
    - Model A: `define relation1: member from parent1` where `parent1` has parents `organization` and `team`.
    - Model B: `define relation2: member from parent2` where `parent2` has parents `team` and `organization`.
