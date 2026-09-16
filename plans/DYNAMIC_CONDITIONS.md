# Dynamic Conditions

## Background

Currently, the only way to apply ABAC (attribute based access control) rules
within an OpenFGA authorization model is to define a condition within the
model and apply it to a direct type on a relation.

In order to support AI agentic use-cases, such as MCP (model context protocol)
gateways, we need a more dynamic approach to defining ABAC rules. Ideally,
the design for this more dynamic approach will change as little places within
the OpenFGA project as possible by reusing existing type schemas.

## Implementation

We have designed the initial implementation of this new feature and now need
to implement it within the OpenFGA code. The design works as follows:

### The $expression Syntax

A new reserved symbol has been added to the OpenFGA language project. The
symbol is represented as `$expression` and can be used in place of a condition
name within an authorization model. For instance, the following model shows
how to use this new symbol:

#### Model Example

```
model
	schema 1.1

type user

type document
	relations
		define editor: [user with $expression]
		define viewer: [user] or editor
```

The model above defines the now reserved symbol `$expression` in place of a
condition. This expression applies the same way that a normal condition would,
as an indicator that a condition exists on the relation "document#editor" for 
the user type. 

Many of the existing rules around a condition apply to the $expression:
- Only one $expression can be defined per object/relation/type.
- When a type has an attached $expression, the same type can exist on the 
relation without an $expression or condition.
- When a type has an attached $expression, the same type can exist on the 
relation with a condition that is defined within the model.
- When writing a tuple for the type with an $expression, the given
"condition_name" field value must be set to "$expression". (The context of
such a tuple will also have a different schema than a standard condition.
I will expand more on that later.)

## Context of $expression

The context schema of a tuple written to include an $expression differs from
that of a standard condition. As a yaml example, a standard condition's schema
might look like the following:

```
condition:
  name: expired
  context:
    grant_time: 2023-01-01T00:00:00Z
	grant_duration: 10m
```

Note that the condition's name is indicated along with the values of its
parameters. All other information about the condition's CEL expression and
parameter types is contained within the authorization model definition.

However, an $expression is not defined within the authorization model; this
allows nearly any expression to be applied where a static condition would be
constrained to its definition in the model. To support this capability for
an $expression, the schema must define the parameter names and types used by
it's CEL expression, as well as the CEL expression itself. Such a schema will
be defined as follows, using yaml example again:

```
condition:
  name: $expression
  context:
    parameters:
	  channel_id: string
	expression: "channel_id == 'X123456'"
```

Some rules around defining this context structure:

- The `name` attribute of `condition` is required have a value exactly equal to
"$expression".
- The `expression` attribute of `context` is required and must be a valid CEL
expression.
- The `parameters` attribute of `context` is optional, but when defined, may
contain parameters used within the CEL expression, with their data types
defined as values.
- When a parameter is not defined within the context schema, the parameter is
still permitted for use within the CEL expression, and its data type will be
inferred as `string` implicitly.

### Protobuf Struct Encoding

The `context` field of a `TupleCondition` is `google.protobuf.Struct` — no new
proto types are needed. For $expression tuples the Struct uses two reserved
top-level keys:

- `"expression"` — a string value containing the CEL expression.
- `"parameters"` — a nested Struct value whose keys are parameter names and
  whose values are the type name strings (e.g. `"string"`, `"int"`, `"bool"`,
  `"duration"`, `"timestamp"`, `"double"`, `"uint"`, `"any"`, `"map"`,
  `"list"`, `"ipaddress"`). These names correspond to the `paramTypeString`
  reverse-lookup table in `internal/condition/types/types.go`.

Example JSON representation:
```json
{
  "expression": "channel_id == 'X123456'",
  "parameters": {
    "channel_id": "string"
  }
}
```

## Requirements

### Language Dependency

The `$expression` reserved symbol (`DOLLAR_EXPRESSION` token) is already merged
and vendored via the language package at
`github.com/openfga/language/pkg/go v0.3.2-0.20260818192608-0d2ad7fb7c40`.
The parser handles `$expression` in model DSL and encodes it as the condition
name on the corresponding `RelationReference`. No further language changes are
needed.

### Experimental Flag

This new feature must be gated behind an experimental flag of:
`inline_expressions`. The constant should be added to
`pkg/server/config/config.go` alongside the existing experimental constants,
following the underscore naming convention.

When the flag is **off**:
- `WriteAuthorizationModel` rejects models containing `$expression` — the
  check lives in the **server handler layer** (`pkg/server/write_authzmodel.go`)
  before the command is invoked, to keep the command flag-agnostic.
- `Write` rejects tuples whose `condition_name` is `"$expression"` — the
  check lives in `pkg/server/write.go` before the write command is invoked.

When the flag is **on**, everything proceeds normally; stores not using
`$expression` behave exactly as they do today.

### TypeSystem Validation (Model Level)

The TypeSystem validates that condition names referenced in relations exist in
the model's `conditions` map. `$expression` is a reserved keyword and must
be whitelisted at this validation step (`typesystem.go` around line 1461) so
that it is not rejected as an unknown condition. No `EvaluableCondition` entry
for `$expression` is ever stored in `t.conditions`.

### Write-Time Tuple Validation

`validateCondition` in `internal/validation/validation.go` currently:
1. Checks the condition name against `typesys.GetConditions()`.
2. Verifies the type restriction references the same condition name.
3. Validates the context Struct and casts parameters.

For `$expression` tuples, a parallel path must be added:

1. Detect `tk.GetCondition().GetName() == "$expression"` early (before the
   `ContainsForbiddenChars` check applies — note: `$` is not a control
   character so this check already passes).
2. Verify the type restriction list includes a `RelationReference` whose
   condition is `"$expression"` for the user type (same logic as existing).
3. Parse the `"expression"` key from the context Struct — it is required;
   absence is a validation error.
4. Parse the `"parameters"` key (optional nested Struct) and reverse-map the
   string type names to `ConditionParamTypeRef` values.
5. Build a `*openfgav1.Condition` from the parsed expression and parameters,
   then call `condition.NewCompiled(...)` to **fully compile** the CEL
   expression against the declared parameter types (type-checked compilation,
   matching the depth used for model conditions).
6. Return a `tuple.InvalidConditionalTupleError` on any error.

### Read-Time Evaluation

At evaluation time (`internal/checkutil/checkutil.go`) the call chain is:

```
typesys.GetCondition(name) → eval.EvaluateTupleCondition(ctx, t, cond, reqCtx)
```

When `name == "$expression"`:
- Skip the `typesys.GetCondition` lookup (it will return `(nil, false)`).
- Instead, build an `EvaluableCondition` directly from the tuple's context
  Struct using the same parse logic as write-time validation.
- Call `condition.NewCompiled(...)` (no caching — compile on every eval for
  the initial implementation; revisit if benchmarks show a problem).
- Evaluate against the **request context only** (the requester's context
  struct, not the tuple context fields) after casting to typed parameters via
  `CastContextToTypedParameters`.

#### Request Context Rules

- Extra request context keys are ignored.
- Any parameter declared in `parameters` (or used in the expression and
  therefore inferred as `string`) that is **absent** from the request context
  is a **hard error** surfaced as an HTTP 4xx to the caller — not a silent
  deny. This differs from the existing `MissingParameters` soft-deny path.

### `EvaluateTupleCondition` Changes

`eval.EvaluateTupleCondition` currently returns an error when
`evaluableCondition == nil`. For `$expression` this nil check must be
bypassed — the condition is built from the tuple, not passed in from the
model. One approach: build the `EvaluableCondition` before calling
`EvaluateTupleCondition` and pass it in normally; the evaluation function
itself needs no change.

### ListObjects and ListUsers

$expression tuples are evaluated inline the same way as Check — build the
`EvaluableCondition` from each tuple at evaluation time and evaluate against
the request context. No special-casing for list operations beyond what the
existing condition evaluation path already does.

### Test Coverage

- New entries in `assets/tests/abac_tests.yaml` covering:
  - Basic $expression check (hit and miss)
  - Missing parameter hard error
  - Unknown parameter ignored
  - $expression alongside a named condition on the same relation
  - ListObjects with $expression
- Unit tests in `internal/validation/validation_test.go` for write-time
  validation of $expression tuples (valid, missing expression, bad CEL,
  bad type name, flag-off rejection).
- Unit tests in `internal/condition/eval/eval_test.go` for read-time
  evaluation paths.
