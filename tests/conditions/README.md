# Test matrix for dynamic conditions

This package tests dynamic conditions: relationship tuples whose condition is
the reserved name `$expression` and which carry their own CEL expression. The
feature is experimental and is enabled with `--experimentals inline_expressions`.

The matrix answers one question for every case: does the server return the
correct result, where the correct result is one of **allowed**, **denied** or
**error** (the server cannot decide)?

This document describes how the matrix is built, why it is built that way,
what it found, and what did not behave as expected.

## Running it

```bash
go test -race -count=1 ./tests/conditions/
```

`make test-matrix` runs `./tests/...` and includes this package.

Each test logs one summary line per layer and engine:

```
layer 2: passed=3092 provisional=428 (...) known_gaps=0 () failures=0
```

## Architecture

### Four layers

The full cross-product of types, values, model shapes and lifecycle events is
too large to run or to read. The problem splits into four layers that do not
depend on each other, so the matrix tests their sum, not their product.

| Layer | Question | File | Size |
| --- | --- | --- | --- |
| 0. Write | Is this tuple accepted or rejected? | `layer0.go` | 133 tuples, each as a stored tuple and as a contextual tuple |
| 1. One tuple | Given one expression and one value in the request, what is the result? | `layer1.go`, `layer1_values.go` | 40 expressions × 43 values, each as a grant and as a deny rule: 3,440 cells |
| 2. Composition | Given where the conditioned tuple sits in the model, what is the final answer? | `layer2.go` | 5 slots × 10 wrappers × 4 outcomes, plus second-tuple variants: 880 cells |
| 3. Lifecycle | Does the answer stay right when something changes? | `layer3.go` | 13 scenarios |

Layer 1 does not depend on the shape of the model, so it uses the simplest
one. Layer 2 does not depend on parameter types, so it uses one string
parameter and one expression.

### Cells, groups and outcomes

`matrix.go` holds the pieces every layer shares.

- A **`Cell`** is one authorization question (user, relation, object, request
  context) and the result it must give.
- A **`Group`** is a model, its tuples and the cells that query them. Each
  group runs in its own store. Tuples are written one request at a time, in the
  order listed, so write order is under the test's control.
- **`RunGroup`** runs every cell of a group against four APIs: Check,
  BatchCheck, ListObjects and ListUsers.
- An **`Outcome`** counts what happened: passed, provisional, known gaps and
  failures.

The list APIs are judged through the same cell as Check. ListObjects is asked
for the cell's object type and the cell is **allowed** if its object is in the
list. ListUsers is asked for the cell's object and the cell is **allowed** if
its user, or a typed wildcard of its type, is in the list. A request that
returns an error makes the cell an **error**.

### Expected results come from rules

No expected value in this package was copied from the server's output. Each
layer has a rule, written independently of the server code.

| Layer | Rule |
| --- | --- |
| 0. Write | A list of tuples that must be accepted or rejected. |
| 1. One tuple | `ConvertValue` says which JSON values fit each declared type. Each expression has its logic written a second time as a Go function. The result is: missing value or value that does not fit → error; otherwise the Go function's answer. |
| 2. Composition | Three-valued logic. A tuple is true, false or unknown (cannot be evaluated). Union, intersection and exclusion follow the usual rules: a true operand decides a union, a false operand decides an intersection, and otherwise unknown stays unknown. Unknown is reported as an error and never as an allow. |
| 3. Lifecycle | Written per scenario. There are few enough to review by hand. |

`rules_test.go` tests the rules themselves, without a server.

### Three kinds of non-failure

A result that differs from the expected one is a failure, with two exceptions
that are declared in `gaps.go`.

- **Known gap.** The server's behavior today differs from an agreed target,
  and a proposed improvement closes the difference. The generator attaches the
  gap and the result the server gives today. The cell passes only while the
  server still gives that result. When the improvement lands, the run fails
  with a `GAP CLOSED` message and the gap is removed. The list can only shrink.
- **Provisional.** The expected result waits on a decision. The cell accepts
  an error in addition to the expected answer. It never accepts an allow the
  rule does not give.

A failure message says how much harm the wrong result can do:

| Label | Meaning |
| --- | --- |
| `CRITICAL allowed-should-not-be` | The server allowed, and the rule says denied or error. |
| `HIGH denied-should-not-be` | The server denied, and the rule says allowed. |
| `HIGH answer-where-error-expected` | The server denied, and the rule says error. |
| `MEDIUM error-where-answer-exists` | The server returned an error, and the rule gives an answer. |

### Server configurations

Every cell of Layers 2 and 3 runs on five configurations (`Engines` in
`matrix_test.go`). Layer 1 runs in full on the default configuration and on
one cell in seven on the others, because type conversion happens before the
configurations differ. Layer 0 runs on the default configuration.

| Name | Settings |
| --- | --- |
| `default` | `inline_expressions` only |
| `check-optimizations` | plus `enable-check-optimizations`, `enable-list-objects-optimizations` |
| `weighted-graph` | plus `weighted_graph_check` |
| `pipeline` | plus `pipeline_list_objects`, `pipeline_optimizations` |
| `caches` | plus the Check query cache, Check iterator cache, ListObjects iterator cache and shared iterator |

### Named conditions as a baseline

Where a case can be written both ways, it runs as a `$expression` tuple and as
a named condition declared in the model, and both must give the expected
result.

Separately, the existing matrices in `tests/check` and `tests/listobjects` are
run a second time with every use of their named condition (`xcond`) replaced
by the equivalent `$expression` tuple. The rewrite is in
`tests/check/check_inline.go`. The expected results are unchanged: the two
kinds of condition must agree on every existing assertion.

## Approach

- **Generate, do not hand-write.** Cases come from lists: types, values,
  expressions, slots, wrappers, outcomes. Adding a value adds a row to every
  expression. Changing a rule changes every cell it governs.
- **Isolate each case for the list APIs.** ListObjects evaluates every object
  of a type, so one tuple that cannot be evaluated fails the request for all of
  them. Every Layer 1 expression and every Layer 2 position therefore has its
  own object type. A list request then reads exactly the tuples of one case.
- **Run every case as a grant and as a deny rule.** A value that silently
  evaluates to false is harmless in a grant and is a wrong allow in a deny
  rule (`base but not blocked`). Layer 1 runs every cell both ways.
- **Control write order.** A second tuple on the same relation is written
  before the user's tuple in one group and after it in another.
- **Keep the rules small.** The value rule is about 150 lines and the logic
  rule about 40. Three independent comparisons back them up: named condition
  against `$expression`, configuration against configuration, and Check against
  the list APIs.
- **Check that the harness can fail.** One rule was broken on purpose
  (`p == 'A'` evaluated as `p == 'B'`). The run reported 16 failures with the
  right severity labels, and the rule was restored.

## Results

Run on the in-memory datastore with the race detector, on `main` at `8e4909c`
(after v1.22.1), November 2026.

| Part | Results | Passed | Known gaps | Provisional | Failures |
| --- | --- | --- | --- | --- | --- |
| Layer 0, Write | 265 | 253 | 12 | 0 | 0 |
| Layer 1, `$expression`, default configuration | 13,760 | 13,224 | 536 | 0 | 0 |
| Layer 1, named conditions, default configuration | 12,536 | 12,536 | 0 | 0 | 0 |
| Layer 1, sampled, each of the other four configurations | 3,764 | 3,680 | 84 | 0 | 0 |
| Layer 2, each of five configurations | 3,520 | 3,092 to 3,120 | 0 | 400 to 428 | 0 |
| Layer 3, 13 scenarios on five configurations | 65 | 65 | 0 | 0 | 0 |
| Existing Check matrix rerun as `$expression`, three configurations | 6,660 subtests | all | | | 0 |
| Existing Check matrix as contextual tuples, rerun as `$expression` | all subtests | all | | | 0 |
| Existing ListObjects matrix rerun as `$expression`, two configurations | 412 subtests | all | | | 0 |

A "result" is one cell on one API. The package takes about 135 seconds with the
race detector.

**No `$expression` case returned an allow or a deny that the rules do not give,
other than the known gaps below.** Named conditions and `$expression` agree on
every assertion of the existing matrices.

## What did not behave as expected

### Server behavior that differs from the target: known gaps

Each gap is tied to a proposed improvement. The numbers are the ones used in
`gaps.go`.

| Improvement | What the server does today | Where the matrix sees it | Results affected |
| --- | --- | --- | --- |
| 1. Strict numeric strings | A numeric parameter accepts any decimal string. `"007"`, `"7.0"`, `"7e0"` and `"+7"` all become `7`, also inside `list<int>` and `map<int>`. A string is accepted for a `double`. | Layer 1: 116 cells | 464 |
| 1. Reject unsafe integers | A JSON number at or above 2^53 has already been rounded when the server reads it. A policy `p == 9007199254740992` allows a request that sent `9007199254740993`. | Layer 1: 18 cells | 72 |
| 2. Reject `any` types | A `$expression` parameter can be declared `any`, `map<any>` or `list<any>`. With those types a comparison across types is not an error: `==` is false and `!=` is true. | Layer 0: 4 tuples | 8 |
| 3. Reject extra context keys | Keys other than `expression` and `parameters` in the tuple context are accepted and ignored. A value stored there is not bound; the caller supplies it. | Layer 0: 1 tuple | 2 |
| 4. Estimate cost at write | An expression that exceeds the evaluation cost limit for every input is accepted, and then every request that reaches it returns an error. | Layer 0: 1 tuple | 2 |
| 6. Replace in one request | A Write request that deletes a tuple and writes it again with a new expression is rejected (`duplicate tuple in write`). An edit needs two requests, with a moment in between where the tuple does not exist. | Layer 3: 1 scenario | 5 |

The first two matter most for correctness. In both, a value that is not the
one the policy names is accepted as a match. In a deny rule that would be a
wrong allow; the matrix records it as a known gap in both directions.

### Results that depend on a decision: provisional cells

In Layer 2, 400 to 428 of 3,520 results per configuration are an error where
the three-valued rule gives an answer. They fall in two groups.

| Group | Situation | Results per configuration |
| --- | --- | --- |
| A true tuple beside one that cannot be evaluated (improvement 7) | The rest of the relation decides the result, for example `base or slot` where `base` is true, or the user's own tuple is true and a tuple for another group is missing its parameter. The rule gives the answer; the server may return an error. | 304 to 332 |
| A tuple that does not apply to the user cannot be evaluated | No tuple that applies to the user is true, and a tuple for a group the user is not in is missing its parameter. The rule says denied; the server returns an error. | 96 |

None of these is a wrong allow. They are listed because the same request can
get an answer or an error, and a caller cannot tell which to expect.

### The weighted graph configuration is not deterministic on provisional cells

On `default`, `check-optimizations`, `pipeline` and `caches`, the number of
provisional results was the same in every run compared (428, 428, 424, 428).

On `weighted-graph` it changed from run to run: 400, 401, 403, 405, 406 and
409 in six runs. The same request, on the same store, sometimes returns the
answer and sometimes returns an error. Every observed result was acceptable
under the provisional rule, so the tests pass, but the behavior is not stable.

### The result depends on the order tuples were written

Two groups in Layer 2 differ only in whether the second tuple was written
before or after the user's tuple. On the in-memory datastore tuples are read
in write order, and during the earlier exploration the non-weighted
configurations returned an error when the tuple that cannot be evaluated was
read first and the answer when it was read last. The matrix fixes the order in
each group so these cells are stable. The existing matrices in `tests/check`
shuffle tuples before writing; cases of this kind would be flaky there.

### ListObjects and ListUsers fail where Check answers

For a union such as `base or slot` where `base` is true and the conditioned
tuple cannot be evaluated, Check returns allowed. ListObjects and ListUsers
return an error for the same request. These are counted in the first
provisional group.

### Things that were different from what was assumed while building

- **A model holds at most 25 conditions.** The named-condition form of Layer 1
  needs one condition per expression, so the 40 expressions are split over two
  groups.
- **A contextual tuple that does not validate returns error code 2027,** not
  the 2000 that a stored tuple returns. Layer 0 uses that code to tell a
  rejected contextual tuple from one that was accepted and then failed to
  evaluate.
- **The context size limit is not applied to contextual tuples.** A tuple
  whose condition context is 36 KB is rejected when written (`condition context
  size limit exceeded: 36029 bytes exceeds 32768 bytes`). The same tuple sent
  as a contextual tuple of a Check is accepted and evaluated. The
  oversized-expression case is therefore run as a stored tuple only. This is
  not tied to a proposed improvement.
- **Every declared parameter must be present, even one the expression does not
  use.** `true` with a declared parameter returns an error when the parameter
  is missing. Layer 0 accepts the tuple; Layer 1 always sends or omits the one
  parameter it declares.
- **`any` cannot be written in the DSL.** `any`, `map<any>` and `list<any>` are
  syntax errors in a DSL model and are accepted in a JSON model. The matrix
  only exercises them through `$expression` parameters in Layer 0.
- **Nothing failed on the first full run.** Because that is also what a harness
  that cannot fail looks like, a rule was broken on purpose to confirm that
  failures are reported.

## Not covered

- **SQL datastores.** The matrix has only run on the in-memory datastore.
  Tuple read order may differ on Postgres, MySQL and SQLite, which matters for
  the order-dependent cells.
- **A second tuple with named conditions.** The second-tuple variants of
  Layer 2 are generated for `$expression` only.
- **Layer 1 on the non-default configurations is sampled,** one cell in seven.
- **ListUsers is judged leniently** when a second tuple cannot be evaluated: an
  error and the correct answer for the user both pass.
- **Turning the experimental flag off** after `$expression` tuples exist.
- **A tuple that fails at runtime** inside the expression (for example a
  division by zero) as a Layer 2 outcome. Layer 2 has true, false, missing
  parameter and wrong type.
- **AuthZen endpoints and StreamedListObjects.**

## Adding a case

- **A new value:** add a `SentValue` to `SentValues` in `layer1_values.go`. It
  is checked against every expression. If the target rule and today's rule
  disagree on it, the generator attaches the numeric gap.
- **A new expression:** add an `Expression` to `Expressions` in `layer1.go`,
  with the CEL text and the same logic as a Go function.
- **A new model position:** add a `slot` or a `wrapper` in `layer2.go`. A
  wrapper needs its DSL and its three-valued formula.
- **A new write case:** add it in `Layer0` in `layer0.go`.
- **A new scenario:** add a subtest in `RunLayer3` in `layer3.go`.
- **A gap that closed:** remove the `Gap` from the generator that sets it, and
  from `gaps.go` when nothing uses it.
