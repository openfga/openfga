# Walking the query AST

This package is the query AST. If you are writing a datastore adapter for a backend this
repository does not ship, this document tells you how to consume the tree correctly: what it
guarantees, what it leaves to you, and the specific mistakes that produce queries which run and
return the wrong rows.

## What this package is not

Nothing here parses anything. There is no lexer, no `Parse`, no source positions, no error
recovery. The tree is a *build target*: a typed builder constructs it, and your adapter walks
the result. "Parsing the AST" means writing a correct total walker over a closed node set.

This package also does not render, execute, or validate. It is data and nothing else. Every
decision about how a construct is expressed against a real datastore is left to you.

## The tree is relational

Be aware of what you are adopting. The node algebra is relational: a projection list, aliased
table occurrences, joins, filters, grouping, aggregates, ordering, row limits. If your backend
is not relational, you are not "spelling these differently" — you are translating between models,
and several nodes may have no counterpart at all. That is the one assumption the tree does make,
and it is structural: no amount of reinterpretation removes it.

What the tree does *not* assume is a target language. Nothing here holds query text, in any
notation. Every operator, connective, quantifier, join flavour, column, cast target, function,
and aggregate is a closed enum carrying no spelling of its own, and value-bearing nodes carry
values rather than rendered fragments. Producing text — if your backend even wants text — is
entirely your job, and nothing in the tree has pre-empted a decision about it.

Everything below is written for any adapter. Where a rule exists because of how text-emitting
backends behave, it says so.

## The tree

**One statement type.** `Select` is the only statement. There is no set-operation node (union,
intersection, difference) and no insert, update, or delete. Subqueries are reached through
exactly two nodes, `ExistsNode` and `SubqueryNode`, each holding a nested `*Select`.

**Every table is the `tuple` table.** `Table` carries only an alias, so every join is a
self-join. What that table is called, or whether it is a table at all, is yours to decide.

### Position is part of the type

Read this before the node table, because it determines the shape of your walker.

The node set is divided into four **position categories**, each a sealed sub-interface of `Node`,
and **every field in the tree is typed with the category it accepts**:

| Category | Legal positions |
| --- | --- |
| `Predicate` | `Select.Where`, `Select.Having`, `JoinClause.On`, a searched `CASE`'s `When`, an aggregate's `Filter`, a connective's parts |
| `ScalarValue` | anywhere one value is wanted: a comparison operand, a function argument, a cast input, `Select.GroupBy`, `OrderTerm.Expr`, a `THEN` or `ELSE` |
| `SetValue` | `QuantifiedNode.Set`, and nowhere else |
| `Projection` | `Select.Columns`, and `AliasNode.Inner` |

`ScalarValue` **embeds** `Projection`, so every single-valued expression can also stand as an
output column and you never have to assert to project one. `SetValue` does not embed it: whether
a set is projectable depends on the node, so each set type says so individually.

Two consequences for you, and they are the reason the categories exist:

- **Write one walk function per category**, not one per node kind. You will not need a
  "am I in value or in predicate position?" parameter threaded through the recursion, because the
  compiler already knows: it is the static type of the field you are visiting. That parameter is
  unavoidable when a tree erases the distinction, and it is the largest cost a target which
  separates the two syntactically pays to consume such a tree. Oracle before 23c is the case that
  forced this design — it has no boolean value type at all, so `SELECT (a.store = :1)` is not
  merely unusual there, it is unwriteable.
- **You may rely on position; you may not rely on provenance.** Every field is exported, so a
  hand-written struct literal is as valid a tree as anything the builder produces, and nothing
  records where a tree came from. What the categories guarantee is that the trees which can be
  built *at all* are well-positioned — a mis-positioned one does not compile, for the builder and
  the forger alike. This is a stronger guarantee than "the builder is careful", which you would
  have no way to verify.

There is **no boolean value** in the tree. `Predicate` is not a `ScalarValue`, there is no boolean
`CastType`, and no `LitNode` should carry a `bool`. A caller who wants a boolean *column* writes
the `CASE` that produces one — which is what every engine would have made them do anyway, except
that now the lowering is visible in the tree instead of being each adapter's invention.

**24 node types**, all satisfying `Node`:

| Category | Types |
| --- | --- |
| `Predicate` | `CompareNode`, `LikeNode`, `BetweenNode`, `IsNullNode`, `InNode`, `QuantifiedNode`, `ExistsNode`, `LogicalNode`, `NotNode`, `ConstPredNode` |
| `ScalarValue` (hence also `Projection`) | `ColNode`, `BindNode`, `LitNode`, `CastNode`, `FuncNode`, `JSONObjectNode`, `AggNode`, `CaseSearchedNode`, `CaseSimpleNode`, `SubqueryNode` |
| `SetValue` | `SetBindNode`, `SubqueryNode` |
| `Projection` only | `AliasNode` |
| no category | `JSONPairNode`, `*Select` |

`SubqueryNode` is the one type spanning three categories, which is correct: a scalar subquery, a
set subquery, and a projected subquery are the same construct. Crossing between categories still
takes an explicit type assertion, so it stays visible in your code.

The two entries with **no category** are the ones worth pausing on. `JSONPairNode` is not an
expression, and typing it out of every category is what confines it to `JSONObjectNode.Pairs`.
Before the categories existed, a bare pair could be dropped into a projection list, and a renderer
would dutifully emit a fragment like `KEY 'k' VALUE x` as though it were a column. `*Select` is
discussed in rule 2.

### Sealed but open

Every category interface carries an unexported marker method, so no type outside this package can
join a category and the set of node kinds is closed. Two consequences:

- You **cannot** add a node kind. If your backend needs a construct the tree cannot express,
  that is a change to this package, not something to work around locally.
- Your type switch is **total and stays total**. The node set cannot grow behind your back, so
  a `default` clause is unreachable for a well-formed tree — which is why panicking there is
  the right choice. Note that per-category switches are much smaller than a 27-way one, and each
  category's `default` is genuinely unreachable rather than a catch-all for mis-positioning.

Every concrete type and every field is exported, so you can walk the tree fully without any
privileged access. Sealed to construct, open to inspect.

### `Validate` checks what the types cannot

`Node` has one method beyond its marker: `Validate() error`. Every node checks its own rules —
arity, required fields, cross-field relationships, enum range — then calls `Validate` on each direct
child. So:

```go
if err := stmt.Validate(); err != nil {
    return fmt.Errorf("malformed query tree: %w", err)
}
```

**One call at your boundary validates the whole tree.** It returns the *first* violation it reaches
and stops; fixing that one reveals the next. The finding carries the path it was found at, which is
what makes a single error actionable:

```text
Where: Parts[1]: Inner: Right: required field is nil
```

Because only the first fault is reported, check order is part of the behaviour: each node checks its
own local rules — enum range, arity, alias, cross-field — before recursing into children in field
order. That surfaces the shallowest fault, which is usually the cause rather than a consequence.

Each error wraps one of six sentinels — `ErrMissing`, `ErrArity`, `ErrEnum`, `ErrCrossField`,
`ErrValue`, `ErrAlias` — so you can branch on the *kind* of violation with `errors.Is` without
parsing text. The path prefix is for humans.

This changes what the rules below are for, so read them with it in mind. They were previously
things you had to check yourself or trust; now you check them once, mechanically, and the rules
tell you **what a violation means and how your renderer would fail without the check**. Every rule
marked *checked by `Validate`* is one you no longer have to defend against node by node — though a
panicking `default` is still correct, because nothing forces a caller to validate.

Three things `Validate` deliberately does not check, because a node cannot see far enough to:
**alias scope** (rule 8b — it does catch a *duplicate* alias within one `Select`, which is local),
**whether a bound value is encodable by your driver**, and **grouping correctness**.

## Rules a walker must get right

Each of these is a real trap.

### 1. Nodes are values, not pointers

Every node kind satisfies its categories as a value — except `Select`, which satisfies `Node` as
`*Select`. Switch on `ColNode`, never `*ColNode`. A pointer case compiles fine and silently never
matches, so the node falls through to your `default` and panics, or is skipped.

### 2. `*Select` is a `Node`, but belongs to no category

`ExistsNode.Stmt` and `SubqueryNode.Stmt` are typed concretely as `*Select`, and `*Select`
satisfies none of the four categories, so a bare statement can never appear in an operand
position. Your category walkers therefore never see one, and you recurse into a subquery through
your statement-level function. This is now enforced rather than merely true.

### 3. Two classes of enum, though both carry only tags

Every enum here is textless, so you owe a mapping for all of them. They still divide by one
question worth knowing before you start, because it predicts how much of that mapping is
genuinely yours to invent.

**Convergent** — `Op`, `LogicalOp`, `Quantifier`, `JoinType`, `SortDirection`. Backends of a given
family tend to agree on these, so a mapping is often shareable across several adapters rather than
written per-adapter. One caution: `Ascending` is the conventional default, and most targets express
it by emitting nothing at all. If your mapping returns an empty string for it, guard before writing
a separator or you emit a stray one. `JoinType` carries only `JoinInner`, `JoinLeftOuter`, and
`JoinCross` — the three every target renders identically; `RIGHT`/`FULL OUTER` are absent, because
they are not universally supported.

**Divergent** — `Column`, `CastType`, `ScalarFunc`, `AggFunc`. Expect to write these yourself,
because the divergence is real even within one family: a "text" cast target is named differently on
nearly every engine. `AggFunc` is deliberately minimal — it holds only `AggCount`, the one aggregate
every target renders; the collecting and truth-valued aggregates were left out because they do not
exist portably across Postgres, MySQL, and SQLite.

`Column` deserves emphasis, because it is the one that will bite you. It is a **logical** column
with **no default physical mapping to inherit**. Nothing in the tree says a column is stored as
a column at all. The three subject columns in particular may have to be *computed*: under a
layout that packs the subject into a single `_user` value holding `"type:id"` or
`"type:id#relation"`, obtaining `ColSubjectID` means decomposing that value, not naming a field.
Your adapter must state how every logical column is obtained.

`CastType` has one omission to know about, and one limitation. The omission: there is **no boolean
target**, because a cast produces a `ScalarValue` and truth values are a disjoint category — so
you never owe a "cannot cast to BOOLEAN" rejection. The limitation: a target carries no **length
or precision**, which is real — Oracle's `VARCHAR2` and `RAW` require one — so an adapter needing
a size must choose a maximum and document it.

Every divergent enum has a `String` method. It is **for diagnostics only** — panic messages,
test failures, `%v` in a debug dump — and must never reach your output. `Column.String` returns
`"ObjectType"`, not a stored field name.

### 4. Prove exhaustiveness using the `Count` sentinels

Each divergent enum ends with a count member — `ColCount`, `TypeCount`, `FuncCount`, and
`AggCount_` (trailing underscore, because `AggCount` is already the counting aggregate). These are
**not members of the enum**; they exist so you can prove you cover it. Two working techniques:

- *Table sized by the sentinel.* Declare your mapping as `[ColCount]func(alias string) string`,
  keying every element by its `Col…` constant, then assert in your package's `init` that no
  element is still the zero value. Add a member to the enum and the literal no longer fills the
  array, so the gap fails at startup.
- *Exhaustive switch with a panicking default.* Cheaper to write; catches the gap at run time
  rather than at startup.

Pick one deliberately. Falling back to a zero value for an unmapped column is the worst failure
mode available to you: it yields a well-formed query that returns wrong rows.

Two caveats on the sentinels themselves:

- **A sentinel is assignable to the field it counts.** `ColNode{Name: ColCount}` compiles, because
  `ColCount` is a `Column`; Go cannot prevent this. Such a tree is malformed and will reach your
  exhaustive switch's `default` — one more reason that `default` should panic rather than return a
  fallback. Same for `TypeCount`, `FuncCount`, and `AggCount_`. `Validate` reports all four as
  `ErrEnum`, which is the only place the assignment can be caught.
- **The position categories have no equivalent.** They are closed sets too, but there is no
  `Count` you can size a table by, so a category walker's exhaustiveness rests on the membership
  lists documented on the `Projection` and `SetValue` interfaces. A `Projection` walker in
  particular handles `AliasNode` and `SubqueryNode`, then asserts the rest to `ScalarValue`. Make
  that assertion fail loudly: it is total today, and a future non-scalar `Projection` is exactly the
  change that would make a silent skip return wrong columns.

### 5. Arity is not typed, and it is PER-FUNCTION — *checked by `Validate`*

The categories type *what kind* of node fills a slot. Nothing types *how many*. Every slice field
can legally be empty as far as Go is concerned, and several of them have no rendering when they
are: an empty `LogicalNode.Parts` is `WHERE ()`, an empty `CaseSearchedNode.Branches` is
`CASE END`, an empty `InNode.Elems` is `IN ()`. Each is a **malformed tree**, so panicking is
correct — see [Failure modes](#failure-modes-distinguish-the-two) — and `Validate` reports each of
them as `ErrArity` before your renderer ever sees it.

The required minimum arities, all of which are also stated on the nodes themselves:

| Field | Requirement |
| --- | --- |
| `LogicalNode.Parts` | ≥ 1 (a single part is legal and renders as its one operand) |
| `InNode.Elems` | ≥ 1 — for "matches nothing", use `ConstPredNode{Value: false}` |
| `CaseSearchedNode.Branches`, `CaseSimpleNode.Branches` | ≥ 1 |
| `JSONObjectNode.Pairs` | ≥ 1 (the empty object is excluded as unportable) |
| `FuncNode.Args` | **per `Fn`** — see each `ScalarFunc` member |
| `AggNode.Args` | **per `Fn`** — see each `AggFunc` member |
| `SetBindNode.Elems` | **may be empty**; the one exception, and rule 7 says what it means |

**The two per-function rows are the trap.** `FuncNode.Args` varies by `ScalarFunc` — `Lower`/`Upper`
take one argument, `Coalesce` takes one or more — so an arity check keyed above the switch on `Fn`
is wrong for one of them. `AggNode.Args` is the sharper case even though `AggCount` is the only
aggregate: empty `Args` means **count rows** (`COUNT(*)`) and one argument means count that
expression's values, so the `*` rendering belongs *inside* the `AggCount` case, keyed on
`len(Args) == 0` there — not above the switch, where a future aggregate that requires an argument
would inherit a spurious `*`. `Validate` rejects a malformed tree, but not a walker that would
mis-render a valid one, so this remains yours to get right.

That `AggCount`'s row-counting form is an *absent argument* rather than a flag is load-bearing.
Because the distinction lives in the arity, a backend that must restructure an aggregate — say,
one with no way to filter an aggregate directly, which has to push the condition into the
aggregated expression instead — can branch on it and substitute a constant for the absent
argument. Had the row-counting form been modelled as a fabricated `*` operand, that rewrite would
have produced nonsense.

### 6. `SetBindNode.Elems` is `[]any`; `InNode.Elems` is `[]ScalarValue`

Easy to conflate. Set elements are raw Go values to supply as parameters. `InNode` elements are
subtrees to recurse into.

### 7. `QuantifiedNode.Set` is a `SetValue`, of which there are two

Type-switch it: `SetBindNode` or `SubqueryNode`. The latter keeps the general "compare left against
the result of a nested query" form.

Lowering a bound set is genuinely yours to decide — one collection-valued parameter where your
backend supports it, an expanded per-element form where it does not. If you expand, two cases
must be handled explicitly or you will emit an invalid query:

- **Empty set.** `All` is vacuously true and `Any` vacuously false. Emit a constant predicate;
  most backends have no representation for an empty set operand. This is exactly what
  `ConstPredNode` exists for — see rule 15 — and if you route your constant through the same
  helper that renders that node, the two agree by construction.
- **`OpEq` with `Any`** is exactly a membership test, which many backends express more directly
  than a chain. Other operator/quantifier combinations become a disjunction for `Any` and a
  conjunction for `All`.

### 8. Nil-able fields are a closed list; everything else is required — *checked by `Validate`*

**The rule: an interface-typed or pointer field is required unless its doc says it is nil-able.**
The complete nil-able set is every `Filter` (on all four aggregate nodes), every `Else` (on both
CASE nodes), `JoinClause.On` (nil for `JoinCross`, and *required* for every other `JoinType`),
`Select.Where`, `Select.Having`, `Select.Limit`, and `Select.Offset`.

Everything else must be populated, including `ExistsNode.Stmt`, `SubqueryNode.Stmt`,
`AliasNode.Inner`, `CaseSimpleNode.Base`, and every operand of `CompareNode`, `LikeNode`,
`BetweenNode`, and `IsNullNode`. The two `Stmt` pointers are worth a glance even though you may
rely on them: getting it wrong costs you a nil dereference rather than the clean panic an unhandled
node kind gives you — which is exactly why `Validate` reports a nil `Stmt` as `ErrMissing` instead
of dereferencing it, and why validating at your boundary converts that dereference into a message.

Note what is *not* nil-able any more: `CaseNode.Base`. See rule 9.

`Where` and `Having` are each a **single** predicate node, nil when the clause is absent — not a
list you have to combine. A caller wanting several conditions composes them into one `LogicalNode`,
so the connective is part of the tree rather than a convention you are expected to know. A nil
check is therefore the whole contract: emit the keyword and walk the one node, or emit nothing.

`Limit` and `Offset` are pointers **specifically so that "no limit" stays distinct from "limit
zero"**. Compare against nil; never test the pointed-to value for truthiness.

### 8a. Relationships between fields, typed by nothing — *checked by `Validate`*

- **`Select.Having` without `Select.GroupBy`** is legal and means "over the whole result as one
  group", exactly as in SQL. No special handling, and `Validate` permits it.
- **`AggNode.Distinct` requires an argument.** `COUNT(DISTINCT *)` is not a construct.
- **`JoinClause.On` is required for every flavour but `JoinCross`, and forbidden for that one.** A
  nil `On` with `JoinInner` is not an implicit cross join, and dropping a condition on a cross join
  changes which rows return.
- **No two of a `Select`'s tables may share an alias**, since a `ColNode` naming it would be
  ambiguous. This is the one scope-ish rule that is local enough to check.

Each violation above reports as `ErrCrossField`, except a missing `On`, which is `ErrMissing`.

### 8b. Alias *scope* is still unchecked

This is the one rule `Validate` cannot take off your hands. A `ColNode.Alias` should name a `Table`
in `From` or `Joins` of the `Select` it appears in, or of an enclosing one for a correlated
subquery — and a node validating its own subtree can see neither. Checking it would mean collecting
column references from every clause and threading enclosing scopes through subqueries: a resolver,
not a shape-checker. A `ColNode` naming an alias that is nowhere in scope renders as a plausible
qualified column and fails at the engine, which is the acceptable end of the failure spectrum.

What *is* checked: an **empty** alias on `ColNode`, `Table`, or `AliasNode` (`ErrAlias`), and a
**duplicate** table alias within one `Select` (`ErrCrossField`). Beyond emptiness, `Table.Alias` and
`AliasNode.Alias` remain the tree's only free strings and are otherwise unvalidated — no length
limit, no character-set check, no collision check among output names. Quoting is yours. See rule
14's closing note.

### 9. The two conditional forms are two node types

`CaseSearchedNode` and `CaseSimpleNode`, distinguished by the node type rather than by a nil
`Base`:

- `CaseSearchedNode` — each `SearchedBranch.When` is a full **predicate**, evaluated on its own.
- `CaseSimpleNode` — each `SimpleBranch.When` is a **value**, compared for equality against
  `Base`, which is never nil.

They are separate types because the forms differ in the *category* of their `When`. One node could
only have typed that slot as the union of the two, which would put the discrimination back in your
hands — and a nil check is a weaker thing to rely on than a type. Both nodes are `ScalarValue`s:
branches are evaluated in order, the first match wins, and `Else` is the nil-able fallback.

### 10. Some children are not in any category

`OrderTerm`, `SearchedBranch`, `SimpleBranch`, `Table`, and `JoinClause` are clause items,
deliberately not nodes at all, and `JSONPairNode` is a `Node` in no category. None can appear as
an operand, so the type system keeps them out of operand position. The cost is that a walk driven
purely off the four categories will never reach them. Descend explicitly into:

- `Select.From`, `Select.Joins`, `Select.OrderBy`,
- `CaseSearchedNode.Branches`, `CaseSimpleNode.Branches`,
- `JSONObjectNode.Pairs`.

### 11. `Filter` restricts which rows reach an aggregate

`AggNode.Filter` narrows the aggregate's input to the rows the predicate accepts; `AggNode.Distinct`
is the other modifier, and the two are independent. Several engines have no native `FILTER` clause,
so if you emulate it by pushing the condition into the aggregated expression, mind the three-way
case — a filtered-out row must yield **NULL**, not a substitute value that the aggregate would still
count — or you change the result rather than just its formatting. `COUNT(*)` has no argument to
wrap, so a filtered one becomes `COUNT(CASE WHEN <filter> THEN 1 END)`.

### 12. `JSONPairNode` appears only under the object constructor

Specifically in `JSONObjectNode.Pairs` — and now that is enforced by its absence from every category
rather than merely being true. The key/value pairing survives as a node rather than being flattened
at construction time because backends consume it in different shapes: the ANSI form is `k VALUE v`,
Oracle spells it `KEY k VALUE v`, and MySQL and Postgres both take a flat `k, v` argument list. Had
it been flattened earlier, you would have no way to recover the pairing.

### 13. Parameter ordering is traversal order

An adapter accumulates parameter values in one flat list as it walks. If your backend identifies
parameters *positionally*, that list's order is fixed by your traversal, so you must visit
clauses in output order and left-to-right within each clause. Reordering a clause for
convenience silently misnumbers every subsequent parameter — a query that still runs, with wrong
results. Named or otherwise self-describing parameters are immune, which is worth knowing before
you choose.

### 14. `LitNode` is a request, not a capability — and escaping is yours

`LitNode.Value` and `BindNode.Value` hold the same thing: a raw Go value, `any`. The nodes
differ only in what the builder asked for. A `BindNode` asks you to supply the value out of band
as a parameter; a `LitNode` asks you to write it directly into the query instead.

That request is a preference — plan-cache behaviour, readability of a logged query — and never a
capability. **Every value a `LitNode` can carry, a `BindNode` can carry too.** So if you cannot
inline safely, or your target has no notion of inlining at all, treat a `LitNode` exactly as you
treat a `BindNode`. That is always correct, and it is the right default.

If you do inline, **the escaping is yours, and it is the security-critical part of your adapter.**
The value arrives raw precisely because escaping is a property of your target and the builder
cannot know it — a builder that pre-escaped would be guessing at your rules. Three specific traps:

- **Dispatch on the underlying kind, not the concrete type.** A caller's named type
  (`type storeID string`) is *not* `string` in a Go type switch. A `case string` arm misses it,
  and whatever your fallback does to it — formatting it unquoted, say — is an injection hole.
- **Not every value has a portable inline form.** A timestamp is the clear case: its spelling
  varies by engine and several require an explicit type or format. If you cannot render a value
  faithfully, bind it rather than approximating it.
- **A `bool` should not reach you at all**, since the tree has no boolean value; a constant truth
  value is `ConstPredNode`. Treat one as the tree corruption it is rather than guessing at an
  encoding — `TRUE` is unportable, and `1` presumes a numeric encoding chosen elsewhere in your
  renderer.

**One asymmetry to know before you follow the "treat a `LitNode` like a `BindNode`" advice.** It is
about the *rendering* being interchangeable, not the *validation* — because there is none on the
bind path. A `LitNode`'s value passes through your inlining logic, which rejects what it cannot
spell, so a bad value fails loudly. A `BindNode`'s value goes to your driver essentially
unexamined: `BindNode{Value: ColNode{}}` renders a clean placeholder and hands your driver an AST
node, and `BindNode{Value: true}` binds a Go `bool` to an engine that may have no boolean type.
Neither errors. **This is the widest hole left in the tree** — the one place a malformed tree still
yields a query that runs and is wrong rather than one that fails — and `Value` stays `any` because
the set of types a driver accepts is your driver's fact, not this package's. If you want the
guarantee, check it at your bind boundary.

Nothing else in the tree is free text. The only caller-supplied strings are a table alias and an
output alias (`AliasNode.Alias`); everything else is a closed enum, so no arbitrary operator,
function, or type name can reach you.

### 15. `ConstPredNode` is the constant predicate, and you owe it a spelling

Unconditionally true or unconditionally false, and it is in the tree because no target spells a
bare truth value the same way — Oracle has none to spell, and renders the two as `1 = 1` and
`1 = 0`. You need this node's rendering anyway for the empty-set lowering in rule 7, so decide it
once and use it in both places.

### Type erasure: there is no Go type to recover

The tree records no Go type information about an expression. A builder may be generically typed,
but that type parameter is *phantom* — it leaves no trace in the node, so two expressions of
different Go types produce identical nodes. A type switch cannot recover it, because the
information was never written.

This is why position had to be encoded as a category and could not be encoded as a Go type. A
`Predicate` is not "an expression whose phantom type is `bool`" — nothing in the node would say
so — it is a member of a distinct interface, which is a fact the compiler can check and a walker
can read.

What *does* survive is narrower and per-node:

| Node | Recoverable | How |
| --- | --- | --- |
| `BindNode` | the **runtime** type of the value | `Value.(type)` — `Value` is `any`, holding a real `string`, `int32`, `time.Time`, … |
| `SetBindNode` | the same, per element | `Elems[i].(type)` |
| `LitNode` | the **runtime** type of the value | `Value.(type)` — identical to `BindNode`; see [rule 14](#14-litnode-is-a-request-not-a-capability--and-escaping-is-yours) |
| `CastNode` | the requested **target** type | `Type` — a `CastType`, not a Go type |
| `ColNode` | nothing | only the `Column` enum tag |
| every node | its position **category** | the static type of the field it was reached through |
| all others | nothing further | structure and enums only |

Drive your walker off structure and position, never off inferred Go types. Where a consumer
genuinely needs type information, the tree records it explicitly — that is exactly why
`CastNode` carries a `Type` and `ColNode` a `Name`.

Type-switching on `BindNode.Value` is legitimate if you need it for lowering, but it is not
validation: the builder already established that the value is one your driver can accept. Note
that a *named* type reaches you as itself — a `type storeID string` arrives as `storeID`, not
`string` — so what you recover is what your driver will see, not what the builder declared.

## Failure modes: distinguish the two

**Tree corruption** — an unknown node kind, an unmapped enum member, a `bool` in a `LitNode`, a
required field left nil, a slice below its minimum arity (rule 5), or a violated cross-field rule
(rule 8a). None of these can happen for a well-formed tree: the node set is sealed, the enums are
closed, and the arity and field rules are part of this package's contract. Reaching one means a
programming error upstream. Panic. Do not paper over it with a zero-value fallback, which converts
a loud failure into wrong query results — and do not paper over it by emitting the degenerate
construct either, since `WHERE ()` and `CASE END` are not queries.

`Validate` is how you turn most of this class into a message with a path in it rather than a panic
from deep inside a renderer. Call it once at your boundary and the whole class collapses to one
error return, which also gives you somewhere to name the offending field. Keep the panicking
`default`s regardless: validation is a caller's choice, not a precondition the types enforce, so an
unvalidated tree can still reach you.

**Legitimately unsupported construct** — valid input your backend cannot express. Return an
error, ideally a distinct error type naming the construct, so a caller building statements
dynamically can fall back. The tree has been deliberately tightened to the intersection of what
Postgres, MySQL, and SQLite render, so a *shipped* SQL backend should have nothing left to reject —
the constructs that once diverged (`RIGHT`/`FULL OUTER JOIN`, `DISTINCT ON`, `NULLS FIRST`/`LAST`,
ordered-input and collecting aggregates, the boolean aggregates) were removed rather than left for
each adapter to reject. A backend further from this trio may still find one: a bound-set expansion
it cannot lower, or `Filter` if it cannot push the condition inward. Those are the residue to return
an error for.

Note that this list is shorter than it would be without the categories. A projected predicate, a
grouped predicate, a cast to boolean, a boolean literal, and a projected bound parameter set are
all things an adapter used to have to reject at render time, and are now compile errors at the
call site — so they are not failure modes you need to write code for.

`LitNode` is deliberately *not* on the unsupported list. It asks for something you may be unable
to do, but never for something you cannot express: bind the value instead and the query is still
exactly the one that was requested.

Silently dropping such a construct is the mistake to avoid. Ignoring a `Filter` you cannot emulate,
or discarding a join condition, changes which rows come back without saying so.

That the shared tree can carry a construct your backend rejects is the honest outcome of an
engine-neutral AST. The tree models a relational query; each adapter states what it supports.

## Recommended order of work

1. Map every `Column`. Start here — it is the largest source of silently wrong results, and
   there is no default to inherit.
2. Map `CastType`, `ScalarFunc`, and `AggFunc`, with a coverage assertion driven by the `Count`
   sentinels.
3. Walk `Select` clause by clause. Traversal order fixes positional parameter numbering, so
   settle it before anything else.
4. Write **four** node functions — one per position category — each with a panicking default.
   Do not write a single function over `Node`: you would be discarding the position information
   the field types just handed you, and you would find yourself passing it back in as a
   parameter.
5. Call `Validate` once, at the boundary where a tree enters your adapter, and return its error.
   This is cheap and it retires most of the tree-corruption class before your walkers run — do it
   before writing defensive checks of your own, since almost all of them are now redundant.
6. Decide your unsupported-construct set and return errors for it. Note that this is a *different*
   set from anything `Validate` reports: those trees are well-formed and your backend simply cannot
   express them.
7. Test by asserting both the query you produce and the parameter list — including parameter
   *order*, which rule 13 makes part of your contract.
8. Test the **malformed** trees too, with hand-written struct literals rather than through a
   builder — and test them *without* your `Validate` call, so you learn how your walker itself
   fails. You are looking for a panic; a rendered string means you have a silent-wrong-results path
   that a caller who skips validation would hit. `ast`'s own `validate_test.go` is a worked list of
   which literals violate which rule.
