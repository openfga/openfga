# Walking the query AST

This package holds a SELECT query as a tree of plain Go structs. Package `query` builds the tree; a
backend walks it and writes SQL for its own database. This file is for someone writing that walker:
the mistakes a walker can make, and the order to do the work in. The type docs in `ast.go` cover
each node.

The tree holds no SQL text. Every operator, column, cast type, function, and aggregate is an enum
with no spelling, and values are raw Go values. Writing the SQL, and escaping it, is your job.

## Two assumptions

- **The query is relational**: result columns, tables, joins, filters, grouping, aggregates, ordering,
  and limits. A backend that is not relational has to translate, and some nodes may have no match.
- **Every table is the `tuple` table.** `Table` holds only an alias, so every join is a self-join.
  You choose the real table name.

## Four kinds of node

A node holds its children in slots (its typed fields). Each slot accepts one of four kinds of node,
and each kind is an interface, so a node in the wrong slot does not compile.

| Kind | Allowed in |
| --- | --- |
| `Predicate` (a true/false test) | `Select.Where`/`Having`, `JoinClause.On`, a searched CASE's `When`, `AggNode.Filter`, `NotNode.Inner`, `LogicalNode.Parts` |
| `ScalarValue` (one value) | a comparison side, function argument, cast input, `GroupBy`, `OrderTerm.Expr`, a CASE `Then`/`Else` |
| `SetValue` (a group of values) | `QuantifiedNode.Set` only |
| `Projection` (a result column) | `Select.Columns`, `AliasNode.Inner` |

Every `ScalarValue` is also a `Projection`. A `SetValue` is not.

| Kind | Node types |
| --- | --- |
| `Predicate` | `CompareNode`, `LikeNode`, `BetweenNode`, `IsNullNode`, `InNode`, `QuantifiedNode`, `ExistsNode`, `LogicalNode`, `NotNode`, `ConstPredNode` |
| `ScalarValue` (so also `Projection`) | `ColNode`, `BindNode`, `LitNode`, `CastNode`, `FuncNode`, `JSONObjectNode`, `AggNode`, `CaseSearchedNode`, `CaseSimpleNode`, `SubqueryNode` |
| `SetValue` | `SetBindNode`, `SubqueryNode` |
| `Projection` only | `AliasNode` |
| none | `JSONPairNode`, `*Select` |

`SubqueryNode` is the only node in three kinds, since a subquery can be a value, a set, or a result
column.

**Write one walk function per kind**, not one switch over `Node`. The type of the field you are
reading tells you which function to call, so you never need an "is this a value or a test?" flag.

**There is no boolean value.** A `Predicate` is not a `ScalarValue`, there is no boolean `CastType`,
and `LitNode`/`BindNode` must not hold a `bool`. Some databases have no boolean type (e.g., Oracle
before 23c). For a true/false result column, the caller writes a CASE.

Each kind interface has an unexported method, so only this package can add node types, and a switch
per kind can list every case. Still panic in `default`: every type and field is exported, so a
hand-built tree can hold anything.

## `Validate` checks what the types cannot

`Validate` checks each node's own rules (list lengths, required fields, rules across fields, enum
ranges), then its children. One call on the statement checks the whole tree:

```go
if err := stmt.Validate(); err != nil {
    return fmt.Errorf("malformed query tree: %w", err)
}
```

It returns the first problem only. Parents are checked before children, so that problem is usually
the cause. The error starts with the path to the bad field:

```text
Where: Parts[1]: Inner: Right: required field is nil
```

Each error wraps one sentinel (`ErrMissing`, `ErrArity`, `ErrEnum`, `ErrCrossField`, `ErrValue`,
`ErrAlias`), so you can check its kind with `errors.Is`. The path is for people.

`Validate` does **not** check alias scope (rule 9), whether your driver accepts a value (rule 15),
or whether non-aggregated columns are in GROUP BY.

Rules marked *(Validate)* below are checked for you; the rule says what a failure means. Calling
`Validate` is optional, so keep a panicking `default` anyway.

## Rules for a walker

### 1. Nodes are values, not pointers

Switch on `ColNode`, never `*ColNode`. `query` never builds pointer nodes; the one exception is
`*Select`. Because the methods have value receivers, a hand-built `*ColNode` still fits a slot and
passes `Validate`, so it ends up in your `default`, which should panic.

### 2. `*Select` is in no kind

`ExistsNode.Stmt` and `SubqueryNode.Stmt` are typed `*Select`, so a statement never appears where a
value or test is expected. Your per-kind functions never see one; handle a subquery with your
statement function.

### 3. Enums: some are the same everywhere, some are not

- **Same on every SQL database**: `Op`, `LogicalOp`, `Quantifier`, `JoinType`, `SortDirection`. One
  mapping can often serve several backends. `Ascending` is the zero value; most databases write
  nothing for it. `JoinType` has only inner, left outer, and cross joins.
- **Different between databases**: `Column`, `CastType`, `ScalarFunc`, `AggFunc`. Write these
  yourself; for example, the text cast type has a different name on almost every database.
  `AggFunc` has only `AggCount`.

**`Column` is the easy one to get wrong.** It names a logical column, with no default stored
column. If your table stores the subject as one `_user` string (`type:id` or `type:id#relation`),
`ColSubjectID` means taking that string apart, not naming a column. Decide how you get every column.

`CastType` has no boolean target and no length or precision. If your database needs a size, pick a
maximum and document it.

The `String` methods on these enums are for error messages only. Never write them into SQL:
`Column.String` returns `"ObjectType"`, not a real column name.

### 4. Use the `Count` values to cover every case

Each enum that differs between databases ends with a count value: `ColCount`, `TypeCount`,
`FuncCount`, `AggCount_` (with an underscore, since `AggCount` is COUNT). It is not a real member.
Use it in one of two ways:

- An array `[ColCount]func(alias string) string` keyed by each `Col…` constant, plus an `init` check
  that no element is empty. A new column then fails at startup.
- A switch over every member with a panicking `default`. Simpler, but a gap shows up only when that
  case runs.

Never fall back to a default for an unknown column. The query still runs and returns wrong rows.

A count value can still be assigned (`ColNode{Name: ColCount}` compiles), so it can reach your
`default`; `Validate` reports it as `ErrEnum`. Node kinds have no count value, so for them, rely on
the member lists above. A `Projection` walker handles `AliasNode` and `SubqueryNode`, then asserts
the rest to `ScalarValue`; make that assertion panic if it fails.

### 5. List lengths *(Validate)*

The types do not say how many elements a list needs. An empty list often has no SQL form (`WHERE ()`,
`CASE END`, `IN ()`), so `Validate` reports it as `ErrArity`.

| Field | Needs |
| --- | --- |
| `LogicalNode.Parts` | 1 or more (one part is written as just that part) |
| `InNode.Elems` | 1 or more; for "matches nothing", use `ConstPredNode{Value: false}` |
| `CaseSearchedNode.Branches`, `CaseSimpleNode.Branches` | 1 or more |
| `JSONObjectNode.Pairs` | 1 or more (an empty JSON object does not work everywhere) |
| `FuncNode.Args` | depends on `Fn`: `Lower`/`Upper` 1, `Coalesce` 1 or more, `JSONArray` 0 or more |
| `AggNode.Args` | depends on `Fn`: `AggCount` 0 or 1 |
| `SetBindNode.Elems` | may be empty (rule 7) |

For `AggCount`, no argument means `COUNT(*)`, and one argument counts that value. Write the `*` inside
your `AggCount` case, not before the switch on `Fn`, or a future aggregate would get a `*` it should
not have. Because `COUNT(*)` is an empty list rather than a `*` argument, a backend emulating a filter
(rule 11) can put a constant in the empty place.

### 6. `SetBindNode.Elems` is `[]any`; `InNode.Elems` is `[]ScalarValue`

Set elements are raw Go values to send as parameters. `InNode` elements are nodes to walk.

### 7. `QuantifiedNode.Set` is a `SetBindNode` or a `SubqueryNode`

You choose how to write a bound set: one array parameter where your database supports it, or one
parameter per element. If you expand it, handle two cases:

- **Empty set.** `ALL` of an empty set is true, `ANY` is false. Write a `ConstPredNode` (rule 16);
  most databases cannot write an empty set.
- **`OpEq` with `Any`** is the same as IN. Other combinations become an OR (`Any`) or AND (`All`) of
  comparisons.

### 8. Which fields may be nil *(Validate)*

An interface or pointer field is required unless listed here. The fields that can be nil: `AggNode.Filter`, `Else` on both CASE nodes, `JoinClause.On` (nil only for
`JoinCross`), `Select.Where`, `Select.Having`, `Select.Limit`, `Select.Offset`. A nil required field
is `ErrMissing`.

`Where` and `Having` each hold one predicate, not a list. Several conditions are one `LogicalNode`.

An empty `Select.Columns` means all columns (`SELECT *`); an empty `Select.From` means no table.

`Limit` and `Offset` are pointers so that "no limit" is different from "limit 0". Check for nil, not
for zero.

### 9. Rules across fields *(Validate)*, and alias scope (not checked)

- `AggNode.Distinct` needs an argument: `COUNT(DISTINCT *)` does not exist.
- `JoinClause.On` is required for every join except `JoinCross`, and not allowed for `JoinCross`. A
  nil `On` on an inner join is not a cross join; dropping a condition changes which rows return.
- Two tables in one `Select` must not share an alias, since a column naming it would be ambiguous.

These are `ErrCrossField`, except a missing `On`, which is `ErrMissing`. `Having` without `GroupBy`
is allowed (the whole result is one group).

**Alias scope is not checked.** A `ColNode.Alias` should name a `Table` in its `Select`, or in an
outer one for a correlated subquery. A node cannot see that. A wrong alias becomes SQL that looks
fine and fails at the database. `Validate` only rejects an empty alias (`ErrAlias`) and a duplicate
table alias. Quoting aliases and clashes between result-column names are your job.

### 10. The two CASE forms are two node types

- `CaseSearchedNode`: each `When` is a test.
- `CaseSimpleNode`: each `When` is a value, compared for equality with `Base` (never nil).

Branches are checked in order and the first match wins. `Else` may be nil.

### 11. `AggNode.Filter` limits which rows the aggregate sees

If your database has no `FILTER` clause and you move the test into the counted value, a skipped row
must become NULL, not some other value the aggregate would still count. `COUNT(*)` with a filter
becomes `COUNT(CASE WHEN <filter> THEN 1 END)`.

### 12. Some children are in no kind

`OrderTerm`, `SearchedBranch`, `SimpleBranch`, `Table`, and `JoinClause` are parts of a clause, not
nodes. `JSONPairNode` is a node in no kind. A walk that only follows the four kinds never reaches
them, so walk `Select.From`/`Joins`/`OrderBy`, both CASE `Branches`, and `JSONObjectNode.Pairs`
yourself. `JSONPairNode` keeps key and value together so you can write either ANSI `k VALUE v` or the
flat `k, v` list MySQL and Postgres use.

### 13. Parameter order is walk order

With positional parameters, collect them in the order you write the SQL: clauses in output order,
left to right. Walking out of order misnumbers every later parameter.

### 14. `BindNode` and `LitNode` hold the same values

Both hold a raw Go value. `BindNode` asks you to send it as a parameter; `LitNode` asks you to write
it into the SQL text. Inlining is only a preference (for plan caching or readable logs). Every
`LitNode` value could be a parameter, so treating a `LitNode` as a `BindNode` is always correct.

### 15. If you inline a value, escaping is yours

This is the security-critical part of a backend.

- **Switch on the value's underlying kind (`reflect.Kind`), not its Go type.** A named type
  (`type storeID string`) does not match `case string`, and an unquoted fallback lets SQL in.
- **Some values have no inline form that works everywhere**, such as a timestamp. If you cannot write
  a value exactly, bind it.
- **A `bool` should never reach you**; a constant true/false is `ConstPredNode`.

`Validate` rejects a `bool` or a node as a `LitNode`/`BindNode` value (`ErrValue`). It cannot tell
whether your driver accepts a value: `BindNode{Value: someStruct}` passes `Validate` and fails only
when the query runs. If you need that checked, check it where you bind.

Aliases are the only free-text identifiers (`Table.Alias`, `ColNode.Alias`, `AliasNode.Alias`).
Quote them.

### 16. `ConstPredNode` needs a spelling

A constant true or false. Databases write it differently (e.g., Oracle has to write `1 = 1` and
`1 = 0`). You also need it for the empty set in rule 7, so write one helper for both.

### 17. `LikeNode` means ANSI `LIKE`, which no database does by default

A `LikeNode` is case-sensitive, and only `%` and `_` are special. Each database departs from that
differently, so the same pattern can match different rows:

- **Case**: Postgres `LIKE` is case-sensitive. MySQL's depends on the column collation and is
  usually case-insensitive. SQLite's is case-insensitive for ASCII.
- **Backslash**: Postgres and MySQL treat `\` as an escape by default; SQLite does not.

Write SQL that gives the ANSI meaning (e.g., Postgres `ESCAPE ''` turns off the backslash escape). If
you cannot, return an error rather than writing a plain `LIKE`.

## The tree has no Go types

`query` builds expressions with Go generics, but the type is gone in the tree: two expressions of
different Go types give the same nodes. What you can still find:

| Node | What you can find | How |
| --- | --- | --- |
| `BindNode`, `LitNode` | the value's runtime type | `Value.(type)` |
| `SetBindNode` | the same, per element | `Elems[i].(type)` |
| `CastNode` | the target type | `Type`, a `CastType` |
| `ColNode` | nothing | only the `Column` enum |
| any node | its kind | the type of the field you reached it through |

Walk by structure and kind, never by guessed Go types. A named type stays itself: `type storeID
string` arrives as `storeID`, which is also what your driver sees.

## Two kinds of failure

**A broken tree**: an unknown node type, an unmapped enum value, a `bool` or node as a value, a
missing required field, a list that is too short, or a broken rule across fields. A correct builder
never makes one, so this is a bug upstream: **panic**. Never fall back to a default (wrong rows) or
write the broken SQL (`WHERE ()` is not a query). `Validate` turns most of these into one error with
a path; keep the panicking `default`s for trees that skip it.

**A valid construct your database cannot write**: return an error that names the construct, so the
caller can fall back. The tree only has what Postgres, MySQL, and SQLite can all write, so a backend
for one of them should have little to reject. A backend for another database might hit some, such as
a bound set it cannot expand or a `Filter` it cannot move inward. Never drop a construct silently:
ignoring a `Filter` or a join condition changes which rows return without any error.

A `LitNode` is never unsupported: bind the value instead and the query means the same thing.

## Order of work

1. **Map every `Column`.** This is the largest source of wrong results, and there is no default.
2. **Map `CastType`, `ScalarFunc`, and `AggFunc`**, with a coverage check using the `Count` values.
3. **Walk `Select` clause by clause.** Fix the walk order first, since it sets parameter numbers.
4. **Write four node functions**, one per kind, each with a panicking `default`.
5. **Call `Validate` once**, where a tree enters your backend, before writing checks of your own.
   Most of them will not be needed.
6. **Decide which constructs you do not support**, and return errors for them. This is a different
   list from what `Validate` reports.
7. **Test the SQL and the parameter list**, including parameter order (rule 13).
8. **Test broken trees without calling `Validate`**, using hand-built struct literals, to see how
   your walker fails: you want a panic, not SQL. `validate_test.go` has broken literals for each
   rule.
