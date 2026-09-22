# Walking the query AST

This package is the query AST. If you are writing a datastore adapter for a backend this
repository does not ship, this document tells you how to consume the tree correctly.

Nothing here parses, renders, executes, or validates against a schema. The tree is a *build
target*: a typed builder (package `query`) constructs it, and your adapter walks the result. It is
data and nothing else — every decision about how a construct maps to a real datastore is yours.

The package doc comment in `ast.go` covers the type model in full; this file covers the traps a
walker hits and the order to attack them in.

## The two assumptions the tree makes

**It is relational.** The node algebra is projections, aliased table occurrences, joins, filters,
grouping, aggregates, ordering, row limits. A non-relational backend is translating between models,
not respelling constructs — some nodes may have no counterpart. No reinterpretation removes this.

**Every table is the `tuple` table.** `Table` carries only an alias, so every join is a self-join.
What that table is actually called is yours to decide.

It makes no assumption about a *target language*: every operator, connective, quantifier, join
flavour, column, cast target, function, and aggregate is a closed, textless enum, and value-bearing
nodes carry values, not rendered fragments. Producing text is entirely your job.

## Position is part of the type

The node set divides into four **position categories**, each a sealed sub-interface of `Node`, and
**every field is typed with the category it accepts** — so a mis-positioned tree does not compile.

| Category | Legal positions |
| --- | --- |
| `Predicate` | `Select.Where`/`Having`, `JoinClause.On`, a searched `CASE`'s `When`, `AggNode.Filter`, `NotNode.Inner`, a connective's parts |
| `ScalarValue` | any single-value slot: a comparison operand, function argument, cast input, `GroupBy`, `OrderTerm.Expr`, a `THEN`/`ELSE` |
| `SetValue` | `QuantifiedNode.Set`, and nowhere else |
| `Projection` | `Select.Columns`, `AliasNode.Inner` |

`ScalarValue` **embeds** `Projection`, so every single-valued expression can also project without a
cast. `SetValue` does not: whether a set is projectable depends on the node.

The payoff: **write one walk function per category, not one per node kind.** You never thread an
"am I in value or predicate position?" parameter — the compiler already knows it as the static type
of the field you are visiting. (Oracle before 23c forced this design: with no boolean value type,
`SELECT (a.store = :1)` is unwriteable there, so predicates and values must stay separate worlds.)

There is **no boolean value** in the tree: `Predicate` is not a `ScalarValue`, there is no boolean
`CastType`, and no `LitNode` carries a `bool`. A caller wanting a boolean *column* writes the `CASE`
that produces one.

You may rely on **position** but not **provenance**: every field is exported, so a hand-written
struct literal is as valid as anything the builder produces, and nothing records where a tree came
from.

### The 24 node types

| Category | Types |
| --- | --- |
| `Predicate` | `CompareNode`, `LikeNode`, `BetweenNode`, `IsNullNode`, `InNode`, `QuantifiedNode`, `ExistsNode`, `LogicalNode`, `NotNode`, `ConstPredNode` |
| `ScalarValue` (hence `Projection`) | `ColNode`, `BindNode`, `LitNode`, `CastNode`, `FuncNode`, `JSONObjectNode`, `AggNode`, `CaseSearchedNode`, `CaseSimpleNode`, `SubqueryNode` |
| `SetValue` | `SetBindNode`, `SubqueryNode` |
| `Projection` only | `AliasNode` |
| no category | `JSONPairNode`, `*Select` |

`SubqueryNode` spans three categories — a scalar, set, and projected subquery are the same
construct — and crossing between them still takes an explicit assertion, so it stays visible.

The two **no-category** entries matter. `JSONPairNode` is not an expression; keeping it out of every
category confines it to `JSONObjectNode.Pairs` (otherwise a bare pair could be dropped into a
projection list). `*Select` is discussed in rule 2.

### Sealed to construct, open to inspect

Every category interface carries an unexported marker method, so the node set is **closed**: you
cannot add a kind — a construct the tree can't express is a change to this package, not a local
workaround. Because it can't grow behind your back, your per-category type switch is **total**, and
a `default` clause is unreachable for a well-formed tree — which is why panicking there is correct.
Per-category switches are far smaller than one 24-way switch over `Node`, and each category's
`default` is genuinely unreachable rather than a catch-all for mis-positioning.

Every concrete type and field is exported, so you can walk the tree with no privileged access.

## `Validate` checks what the types cannot

`Node` has one method beyond its marker: `Validate() error`. Each node checks its own rules — arity,
required fields, cross-field relationships, enum range — then recurses into its children, so **one
call at your boundary validates the whole tree**:

```go
if err := stmt.Validate(); err != nil {
    return fmt.Errorf("malformed query tree: %w", err)
}
```

It returns the **first** violation and stops; fixing it reveals the next. Because only the first is
reported, check order is deliberate: each node checks its own local rules before recursing in field
order, surfacing the shallowest fault. The error carries the path it was found at:

```text
Where: Parts[1]: Inner: Right: required field is nil
```

Each error wraps one of six sentinels — `ErrMissing`, `ErrArity`, `ErrEnum`, `ErrCrossField`,
`ErrValue`, `ErrAlias` — so you can branch on the *kind* with `errors.Is`. The path prefix is for
humans.

So the rules below are no longer things you check by hand: you check them once, mechanically, and
each rule tells you **what a violation means**. Keep a panicking `default` regardless — validation
is a caller's choice, not a precondition the types enforce.

Three things `Validate` deliberately does **not** check: **alias scope** (rule 8b — though it does
catch a *duplicate* alias within one `Select`), **whether a bound value is encodable by your
driver** (rule 14), and **grouping correctness**.

## Rules a walker must get right

### 1. Nodes are values, not pointers

Every node satisfies its categories as a value — except `Select`, which satisfies `Node` as
`*Select`. Switch on `ColNode`, never `*ColNode`: a pointer case compiles and silently never
matches, falling through to your `default`.

### 2. `*Select` is a `Node` but belongs to no category

`ExistsNode.Stmt` and `SubqueryNode.Stmt` are typed concretely as `*Select`, so a bare statement
never appears in an operand position. Your category walkers never see one — recurse into a subquery
through your statement-level function.

### 3. Two classes of enum, both textless

Every enum owes you a mapping. They divide by how much of it is yours to invent:

- **Convergent** — `Op`, `LogicalOp`, `Quantifier`, `JoinType`, `SortDirection`. Backends of a
  family tend to agree, so a mapping is often shareable across adapters rather than written per
  one. Caution: `Ascending` is the default and most targets emit *nothing* for it — guard against
  emitting a stray separator.
  `JoinType` holds only `JoinInner`, `JoinLeftOuter`, `JoinCross`; `RIGHT`/`FULL OUTER` are absent.
- **Divergent** — `Column`, `CastType`, `ScalarFunc`, `AggFunc`. Write these yourself; a "text" cast
  is named differently on nearly every engine. `AggFunc` holds only `AggCount` — collecting and
  truth-valued aggregates were left out as non-portable.

`Column` is the one that bites: it is a **logical** column with **no default physical mapping**.
Under a layout packing the subject into a single `_user` value (`"type:id"` or `"type:id#relation"`),
obtaining `ColSubjectID` means *decomposing* that value, not naming a field. State how every logical
column is obtained.

`CastType` carries **no boolean target** (a cast yields a `ScalarValue`; truth values are disjoint)
and **no length/precision** — an adapter needing a size (Oracle `VARCHAR2`, `RAW`) must choose a
maximum and document it.

Every divergent enum has a `String` method for **diagnostics only** — it must never reach output.
`Column.String` returns `"ObjectType"`, not a stored field name.

### 4. Prove exhaustiveness with the `Count` sentinels

Each divergent enum ends with a count member — `ColCount`, `TypeCount`, `FuncCount`, `AggCount_`
(trailing underscore, since `AggCount` is the counting aggregate). These are **not enum members**;
they let you prove coverage two ways:

- *Table sized by the sentinel*: `[ColCount]func(alias string) string`, keyed by each `Col…`
  constant, with an `init` assertion that no element is the zero value. Adding an enum member breaks
  the literal at startup.
- *Exhaustive switch with a panicking default*: cheaper, catches gaps at run time.

Falling back to a zero value for an unmapped column is the **worst** failure available: a well-formed
query that returns wrong rows.

Two caveats: a sentinel is **assignable** to the field it counts (`ColNode{Name: ColCount}`
compiles), so it can reach your `default` — one more reason it should panic; `Validate` reports all
four as `ErrEnum`, the only place this is caught. And the **position categories have no `Count`** —
a category walker's exhaustiveness rests on the membership lists in the interface docs. A
`Projection` walker handles `AliasNode` and `SubqueryNode`, then asserts the rest to `ScalarValue`;
make that assertion fail loudly.

### 5. Arity is not typed, and it is PER-FUNCTION — *checked by `Validate`*

Nothing types *how many*. Several empty slices have no rendering — `WHERE ()`, `CASE END`, `IN ()` —
so each is a malformed tree. `Validate` reports them as `ErrArity`.

| Field | Requirement |
| --- | --- |
| `LogicalNode.Parts` | ≥ 1 (a single part renders as its one operand) |
| `InNode.Elems` | ≥ 1 — for "matches nothing", use `ConstPredNode{Value: false}` |
| `CaseSearchedNode.Branches`, `CaseSimpleNode.Branches` | ≥ 1 |
| `JSONObjectNode.Pairs` | ≥ 1 (the empty object is unportable) |
| `FuncNode.Args` | **per `Fn`** — `Lower`/`Upper` take one, `Coalesce` one or more, `JSONArray` zero or more |
| `AggNode.Args` | **per `Fn`** |
| `SetBindNode.Elems` | **may be empty** — the one exception; rule 7 |

The two per-function rows are the trap: an arity check keyed *above* the switch on `Fn` is wrong.
For `AggCount`, empty `Args` means **count rows** (`COUNT(*)`) and one argument counts that
expression — so the `*` rendering belongs *inside* the `AggCount` case, keyed on `len(Args) == 0`,
never above the switch where a future aggregate would inherit a spurious `*`. `Validate` rejects a
malformed tree but not a walker that mis-renders a valid one, so this stays yours.

That the row-counting form is an *absent argument* rather than a flag is load-bearing: a backend
that must push a filter into the aggregated expression can substitute a constant for the absent
argument — a rewrite that would produce nonsense if the form were a fabricated `*` operand.

### 6. `SetBindNode.Elems` is `[]any`; `InNode.Elems` is `[]ScalarValue`

Easy to conflate. Set elements are raw Go values to supply as parameters; `InNode` elements are
subtrees to recurse into.

### 7. `QuantifiedNode.Set` is a `SetValue` — `SetBindNode` or `SubqueryNode`

Lowering a bound set is yours: one collection-valued parameter where supported, an expanded
per-element form where not. If you expand, two cases must be explicit:

- **Empty set.** `All` is vacuously true, `Any` vacuously false — emit a `ConstPredNode` (rule 15);
  most backends can't represent an empty set operand. Route it through the same helper as rule 15 so
  the two agree.
- **`OpEq` with `Any`** is a membership test, often expressible more directly. Other
  operator/quantifier combinations become a disjunction (`Any`) or conjunction (`All`).

### 8. Nil-able fields are a closed list; everything else is required — *checked by `Validate`*

An interface-typed or pointer field is required **unless its doc says it is nil-able**. The complete
nil-able set: `AggNode.Filter`, every `Else` (both CASE nodes), `JoinClause.On` (nil for
`JoinCross`, required otherwise), `Select.Where`, `Select.Having`, `Select.Limit`, `Select.Offset`.

Everything else must be populated, including `ExistsNode.Stmt`, `SubqueryNode.Stmt`,
`AliasNode.Inner`, `CaseSimpleNode.Base`, and every operand of `CompareNode`, `LikeNode`,
`BetweenNode`, `IsNullNode`. Getting a `Stmt` wrong costs a nil dereference rather than a clean
panic — which is why `Validate` reports a nil `Stmt` as `ErrMissing`.

`Where` and `Having` are each a **single** predicate node, nil when absent — not a list to combine.
Several conditions are composed into one `LogicalNode`, so the connective is in the tree. A nil check
is the whole contract.

`Limit`/`Offset` are pointers **so "no limit" stays distinct from "limit zero"** — compare against
nil, never test the pointed-to value for truthiness.

### 8a. Relationships between fields, typed by nothing — *checked by `Validate`*

- **`Having` without `GroupBy`** is legal ("over the whole result as one group").
- **`AggNode.Distinct` requires an argument** — `COUNT(DISTINCT *)` is not a construct.
- **`JoinClause.On` is required for every flavour but `JoinCross`, and forbidden for that one.** A
  nil `On` with `JoinInner` is not an implicit cross join; dropping a condition changes which rows
  return.
- **No two of a `Select`'s tables may share an alias**, since a `ColNode` naming it is ambiguous.

Each reports as `ErrCrossField`, except a missing `On`, which is `ErrMissing`.

### 8b. Alias *scope* is still unchecked

The one rule `Validate` can't take off your hands. A `ColNode.Alias` should name a `Table` in the
`Select` it appears in (or an enclosing one, for a correlated subquery) — which a node validating its
own subtree cannot see. An out-of-scope alias renders as a plausible qualified column and fails at
the engine. What *is* checked: an **empty** alias on `ColNode`/`Table`/`AliasNode` (`ErrAlias`), and
a **duplicate** table alias within one `Select` (`ErrCrossField`). Beyond emptiness, aliases are free
strings — quoting and collision among output names are yours (rule 14's closing note).

### 9. The two conditional forms are two node types

Distinguished by node type, not by a nil `Base`:

- `CaseSearchedNode` — each `When` is a full **predicate**.
- `CaseSimpleNode` — each `When` is a **value**, compared for equality against `Base` (never nil).

They are separate types because their `When` differs in *category*; one node could only type that
slot as the union, putting the discrimination back on your walker. Both are `ScalarValue`s;
branches evaluate in order, first match wins, `Else` is the nil-able fallback.

### 10. Some children are not in any category

`OrderTerm`, `SearchedBranch`, `SimpleBranch`, `Table`, `JoinClause` are clause items — not nodes —
and `JSONPairNode` is a `Node` in no category. A walk driven purely off the four categories never
reaches them. Descend explicitly into `Select.From`/`Joins`/`OrderBy`, the two `…Branches`, and
`JSONObjectNode.Pairs`.

### 11. `Filter` restricts which rows reach an aggregate

`AggNode.Filter` narrows the aggregate's input; `Distinct` is an independent modifier. If your engine
has no native `FILTER` and you push the condition into the aggregated expression, a filtered-out row
must yield **NULL**, not a substitute the aggregate would still count. `COUNT(*)` becomes
`COUNT(CASE WHEN <filter> THEN 1 END)`.

### 12. `JSONPairNode` appears only under `JSONObjectNode.Pairs`

The key/value pairing survives as a node rather than being flattened because backends consume it in
different shapes: ANSI `k VALUE v`, Oracle `KEY k VALUE v`, MySQL and Postgres a flat `k, v` list.

### 13. Parameter ordering is traversal order

If your backend identifies parameters *positionally*, the order of the flat parameter list you
accumulate is fixed by your traversal — visit clauses in output order, left-to-right. Reordering for
convenience silently misnumbers every subsequent parameter. Named parameters are immune.

### 14. `LitNode` is a request, not a capability — and escaping is yours

`LitNode.Value` and `BindNode.Value` hold the same thing: a raw Go value (`any`). A `BindNode` asks
you to supply it out of band as a parameter; a `LitNode` asks you to write it directly into the
query. That request is a preference (plan-cache behaviour, log readability), never a capability —
**every value a `LitNode` can carry, a `BindNode` can too**, so if you cannot inline safely, treat a
`LitNode` exactly as a `BindNode`. That is always correct.

If you do inline, **escaping is yours and is the security-critical part of your adapter.** Three
traps:

- **Dispatch on the underlying kind, not the concrete type.** A named type (`type storeID string`)
  is *not* `string` in a Go type switch; a `case string` arm misses it, and an unquoted fallback is
  an injection hole.
- **Not every value has a portable inline form.** A timestamp's spelling varies by engine; if you
  cannot render a value faithfully, bind it.
- **A `bool` should not reach you at all** — a constant truth value is `ConstPredNode`.

**On validation:** `Validate` rejects the two Go values the tree's own model excludes from a bound or
inlined slot — a `bool` and an AST node (`ErrValue`) — for both `LitNode` and `BindNode`. What it
**cannot** check is whether your driver can *encode* a value, because that is your driver's fact, not
this package's: `BindNode{Value: someUnencodableStruct}` passes `Validate` and surfaces (if at all)
as a driver error at execution. That residual gap is **the widest hole left in the tree** — the one
place a tree that passes validation can still produce a query that runs and is wrong. If you want the
guarantee, check the value at your bind boundary. (The inline path has a second line of defence: a
`LitNode`'s value also runs through your escaping logic, which rejects what it cannot spell.)

The only caller-supplied strings anywhere in the tree are a table alias and an output alias
(`AliasNode.Alias`); everything else is a closed enum.

### 15. `ConstPredNode` is the constant predicate, and you owe it a spelling

Unconditionally true or false. It exists because no target spells a bare truth value the same way —
Oracle renders the two as `1 = 1` and `1 = 0`. You need this rendering anyway for the empty-set
lowering in rule 7, so decide it once and use it in both places.

## Type erasure: there is no Go type to recover

The tree records no Go type for an expression. A builder may be generically typed, but that parameter
is *phantom* — two expressions of different Go types produce identical nodes. This is why position is
a category, not a phantom `bool`: a category is a fact the compiler checks and a walker reads.

What survives is narrower and per-node:

| Node | Recoverable | How |
| --- | --- | --- |
| `BindNode`, `LitNode` | the **runtime** type of the value | `Value.(type)` — a real `string`, `int32`, `time.Time`, … |
| `SetBindNode` | the same, per element | `Elems[i].(type)` |
| `CastNode` | the requested **target** type | `Type` — a `CastType`, not a Go type |
| `ColNode` | nothing | only the `Column` enum tag |
| every node | its position **category** | the static type of the field it was reached through |

Drive your walker off structure and position, never off inferred Go types. A *named* type reaches you
as itself — `type storeID string` arrives as `storeID`, not `string` — so what you recover is what
your driver will see.

## Failure modes: distinguish the two

**Tree corruption** — an unknown node kind, an unmapped enum member, a `bool` or AST node in a
`LitNode`/`BindNode`, a required field left nil, a slice below its minimum arity, a violated
cross-field rule. None can happen for a well-formed tree, so reaching one is a programming error
upstream: **panic**. Do not paper over it with a zero-value fallback (wrong rows) or by emitting the
degenerate construct (`WHERE ()` is not a query). `Validate` turns most of this class into one error
with a path, but keep the panicking `default`s — an unvalidated tree can still reach you.

**Legitimately unsupported construct** — valid input your backend cannot express: return a distinct
error naming the construct so a caller can fall back. The tree is deliberately tightened to the
intersection of what Postgres, MySQL, and SQLite render (`RIGHT`/`FULL OUTER JOIN`, `DISTINCT ON`,
`NULLS FIRST`/`LAST`, ordered-input and collecting aggregates were *removed*, not left for each
adapter to reject), so a shipped SQL backend should have little to reject. A backend further afield
might still hit one: a bound-set expansion it cannot lower, or a `Filter` it cannot push inward.
Silently dropping such a construct — ignoring a `Filter`, discarding a join condition — changes which
rows return without saying so, and is the mistake to avoid.

`LitNode` is *not* on the unsupported list: bind the value instead and the query is unchanged.

The categories shorten this list — a projected predicate, a grouped predicate, a cast to boolean, a
boolean literal, and a projected bound set are all compile errors at the call site now, not failure
modes you write code for.

## Recommended order of work

1. **Map every `Column`.** The largest source of silently wrong results, with no default to inherit.
2. **Map `CastType`, `ScalarFunc`, `AggFunc`**, with a coverage assertion driven by the `Count`
   sentinels.
3. **Walk `Select` clause by clause.** Traversal order fixes positional parameter numbering — settle
   it first.
4. **Write four node functions**, one per category, each with a panicking default. Not one function
   over `Node` — that discards the position information the field types hand you.
5. **Call `Validate` once**, at the boundary where a tree enters your adapter, before writing
   defensive checks of your own — almost all become redundant.
6. **Decide your unsupported-construct set** and return errors for it. This is a *different* set from
   anything `Validate` reports.
7. **Test both the query and the parameter list** — including parameter *order* (rule 13).
8. **Test malformed trees too**, with hand-written struct literals, *without* your `Validate` call,
   so you learn how your walker itself fails: you want a panic, not a rendered string. `ast`'s own
   `validate_test.go` is a worked list of which literals violate which rule.
