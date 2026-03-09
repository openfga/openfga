# AuthZen Implementation Guide

This document describes how the [AuthZEN Authorization API 1.0](https://openid.net/specs/authorization-api-1_0.html) is implemented inside OpenFGA. It is aimed at engineers who are already familiar with the OpenFGA codebase and want to understand the AuthZen layer.

## Architecture Overview

The AuthZen layer is a thin translation layer on top of existing OpenFGA APIs. It does **not** introduce new authorization logic. Every AuthZen endpoint delegates to one of the native APIs. All translation logic lives directly in the handler methods — there are no intermediate command wrappers.

| AuthZen Endpoint | Delegates to |
|---|---|
| Evaluation | `Server.Check()` |
| Evaluations (`execute_all`) | `Server.BatchCheck()` |
| Evaluations (short-circuit) | `Server.Check()` sequentially |
| SubjectSearch | `Server.ListUsers()` |
| ResourceSearch | `Server.StreamedListObjects()` |
| ActionSearch | `Server.BatchCheck()` (all relations) |
| GetConfiguration | Static response construction |

### File Layout

```
pkg/server/
  authzen.go                  # All AuthZen endpoint handlers (Evaluation, Evaluations, Search)
  authzen_configuration.go    # GetConfiguration handler
  authzen_test.go             # Server-level tests for all handlers
  commands/
    authzen_utils.go          # MergePropertiesToContext helper + provider interfaces
    authzen_utils_test.go     # Tests for property merging
tests/authzen/
    authzen_test.go           # Shared test harness (store/model/tuple helpers)
    evaluation_test.go        # Integration tests for Evaluation
    evaluations_test.go       # Integration tests for Evaluations (batch + semantics)
    search_test.go            # Integration tests for all Search endpoints
    store_id_validation_test.go # Store ID validation across endpoints
```

## Startup and Wiring

### Feature Flag

All AuthZen endpoints are gated behind the `authzen` experimental flag. The check happens at the top of every handler:

```go
if !s.featureFlagClient.Boolean(serverconfig.ExperimentalAuthZen, req.GetStoreId()) {
    return nil, status.Error(codes.Unimplemented, "AuthZEN endpoints are experimental...")
}
```

The flag is defined in `pkg/server/config/config.go` as `ExperimentalAuthZen = "authzen"` and flows through the standard experimentals configuration (`--experimentals=authzen` or `OPENFGA_EXPERIMENTALS`).

### gRPC and HTTP Registration

In `cmd/run/run.go`:
- gRPC: `authzenv1.RegisterAuthZenServiceServer(grpcServer, svr)` — the same `Server` struct implements both `OpenFGAServiceServer` and `AuthZenServiceServer`
- HTTP: `authzenv1.RegisterAuthZenServiceHandler(ctx, mux, grpcConn)` — registers grpc-gateway HTTP handlers that proxy to the gRPC service

### Authorization Model ID Header

AuthZen uses the `Openfga-Authorization-Model-Id` HTTP header (not a request body field) to pin requests to a specific model version. This requires explicit forwarding through grpc-gateway:

```go
// cmd/run/run.go — custom header matcher
runtime.WithIncomingHeaderMatcher(func(key string) (string, bool) {
    if key == server.AuthZenAuthorizationModelIDHeader {
        return key, true
    }
    return runtime.DefaultHeaderMatcher(key)
})
```

The header is extracted from gRPC metadata in `getAuthorizationModelIDFromHeader()`, validated against the ULID pattern (`^[ABCDEFGHJKMNPQRSTVWXYZ0-9]{26}$`), and passed to the underlying OpenFGA API calls. If absent or invalid, the store's latest model is used (standard OpenFGA behavior).

## Concept Mapping

AuthZen and OpenFGA use different terminology for the same concepts. The translation is done inline in each handler:

```
AuthZen Subject{type:"user", id:"anne"}  →  OpenFGA user = "user:anne"
AuthZen Resource{type:"doc", id:"roadmap"}  →  OpenFGA object = "doc:roadmap"
AuthZen Action{name:"reader"}  →  OpenFGA relation = "reader"
```

Type and ID are concatenated with `:` to form the OpenFGA `user` and `object` strings. The action name maps directly to a relation name. The shared helper `buildCheckRequest()` in `authzen.go` handles this translation for all evaluation paths.

### Properties and Context

AuthZen allows `properties` on Subject, Resource, and Action. OpenFGA has no direct equivalent — it only has a single `context` struct for ABAC conditions.

`MergePropertiesToContext()` in `commands/authzen_utils.go` merges properties into the OpenFGA context with namespaced prefixes:

| Source | Prefix | Example |
|---|---|---|
| `subject.properties.department` | `subject_` | `subject_department` |
| `resource.properties.classification` | `resource_` | `resource_classification` |
| `action.properties.severity` | `action_` | `action_severity` |
| `request.context.current_time` | (none) | `current_time` |

Precedence (lowest to highest): subject properties, resource properties, action properties, request context. Request context wins on conflicts.

Underscore is used as separator because OpenFGA does not allow condition parameters with `.` in their names.

Both `SubjectPropertiesProvider` and `ResourcePropertiesProvider` interfaces abstract over the full entity types and their filter variants (e.g., `Subject` vs `SubjectFilter`, `Resource` vs `ResourceFilter`), so the same merge function works for all endpoints.

## Endpoint Implementations

### Evaluation

The simplest endpoint. Uses `buildCheckRequest()` to translate the AuthZen request into a `CheckRequest` and returns the boolean decision.

Flow:
1. Feature flag check
2. Validate request
3. Extract model ID from header
4. `buildCheckRequest()` — formats user/object/relation, merges properties into context
5. Call `s.Check()`
6. Return `{decision: allowed}`

### Evaluations (Batch)

Supports three execution modes controlled by `options.evaluations_semantic`.

#### Empty evaluations list

If `evaluations` is empty or nil, the request is treated as a single Evaluation using the top-level subject/resource/action/context. This delegates to `Server.Evaluation()` directly and wraps the result in a single-item response.

#### `execute_all` (default)

The `evaluateAll()` method builds a single `BatchCheckRequest` inline:

1. Resolve effective values for each evaluation (per-item overrides fall back to top-level defaults)
2. Build `BatchCheckItem` per evaluation with correlation ID = string index
3. Call `s.BatchCheck()` with all items
4. Map results back to ordered response array

Each evaluation item inherits defaults from the top-level request fields:

```
effective_subject  = evaluation[i].subject  ?? request.subject
effective_resource = evaluation[i].resource ?? request.resource
effective_action   = evaluation[i].action   ?? request.action
effective_context  = evaluation[i].context  ?? request.context
```

Result mapping:
- Allowed results → `{decision: true}`
- Denied results → `{decision: false}`
- Missing results → `{decision: false, context: {error: {status: 500, message: "missing result..."}}}`
- Error results → `{decision: false, context: {error: {status: <http_code>, message: "..."}}}`

Error HTTP status codes are derived from OpenFGA error codes via `grpcErrorToHTTPStatus()`:
- `InputError` codes (e.g., `validation_error = 2000`) → mapped through `servererrors.NewEncodedError()` → HTTP 400
- `InternalError` codes (e.g., `deadline_exceeded`) → HTTP 500

#### `deny_on_first_deny` / `permit_on_first_permit`

These cannot use BatchCheck because evaluation order matters. The `evaluateWithShortCircuit()` method processes evaluations sequentially:

1. For each evaluation, resolve effective values (overrides + defaults)
2. Call `buildCheckRequest()` then `s.Check()` directly (using the pre-resolved model ID)
3. Check the short-circuit condition:
   - `deny_on_first_deny`: break if `decision == false`
   - `permit_on_first_permit`: break if `decision == true`
4. On error: set `decision = false`, wrap error in context, and evaluate the short-circuit condition (errors are treated as denials)

The authorization model ID is resolved once in `Evaluations()` and passed to `evaluateWithShortCircuit()`, ensuring all sequential checks use the same model version.

The response contains only the evaluations that were actually processed — fewer items than the request if short-circuiting occurred.

**Tradeoff**: Sequential execution cannot leverage BatchCheck parallelism.

### SubjectSearch

Answers "Who can perform action X on resource Y?" by delegating to ListUsers.

Key mapping:
- `resource{type, id}` → `ListUsersRequest.Object{Type, Id}`
- `action.name` → `ListUsersRequest.Relation`
- `subject.type` → `ListUsersRequest.UserTypeFilter[]{Type}` (single filter)

The subject `type` is required because ListUsers requires at least one `UserTypeFilter`. The subject `id`, if provided, is ignored per the AuthZen spec for search operations.

Response transformation:
- Object users → `Subject{type, id}`
- Wildcard users → `Subject{type, id: "*"}`

### ResourceSearch

Answers "What resources of type X can subject Y access?" by delegating to StreamedListObjects.

Key mapping:
- `subject{type, id}` → `StreamedListObjectsRequest.User = "type:id"`
- `action.name` → `StreamedListObjectsRequest.Relation`
- `resource.type` → `StreamedListObjectsRequest.Type`

The resource `type` is required because ListObjects requires an object type. The resource `id`, if provided, is ignored — if you know the ID, use Evaluation instead.

Uses an `objectCollector` (defined in `authzen.go`) that implements the `StreamedListObjectsServer` interface to accumulate all streamed results. Object IDs are returned as `"type:id"` strings and parsed by splitting on the first `:`.

### ActionSearch

Answers "What actions can subject Y perform on resource X?" OpenFGA has no native API for this, so it is synthesized from the authorization model and BatchCheck.

Flow:
1. Resolve the typesystem for the store/model to get the authorization model
2. Extract all relation names defined on `resource.type`
3. Build a `BatchCheckRequest` with one item per relation
4. Execute single BatchCheck call
5. Filter: keep only relations where `allowed == true`, skip errors
6. Sort alphabetically for deterministic output
7. Return as `Action{name}` list

**Tradeoff**: Uses a single BatchCheck call (parallel) rather than N individual checks. However, it checks ALL relations on the type, which may include relations the caller doesn't care about.

### GetConfiguration

Returns PDP metadata per [AuthZEN spec section 9](https://openid.net/specs/authorization-api-1_0.html). Implemented in `authzen_configuration.go`.

Constructs absolute endpoint URLs from the incoming request's `Host` header and store ID. Required fields per spec: `policy_decision_point` and `access_evaluation_endpoint`. All search and batch endpoints are optional.

The endpoint is scoped per store: `/.well-known/authzen-configuration/{store_id}`.

## Shared Helpers

All helpers are defined in `authzen.go` except `MergePropertiesToContext` which lives in `commands/authzen_utils.go` since it depends on the provider interfaces.

| Helper | Purpose |
|---|---|
| `buildCheckRequest()` | Translates AuthZen subject/resource/action/context into an OpenFGA `CheckRequest` |
| `grpcErrorToHTTPStatus()` | Maps gRPC error codes (standard and OpenFGA custom) to HTTP status codes |
| `errorContext()` | Builds the `{error: {status, message}}` JSON struct for error responses |
| `getAuthorizationModelIDFromHeader()` | Extracts and validates the model ID from gRPC metadata |
| `MergePropertiesToContext()` | Merges subject/resource/action properties into the OpenFGA context with prefixes |

## Error Context

The AuthZen spec defines `context` in evaluation responses as an arbitrary JSON object. OpenFGA uses it to communicate per-evaluation errors in batch responses.

The `errorContext()` helper builds:

```json
{
  "error": {
    "status": 400,
    "message": "type 'unknown_type' not found"
  }
}
```

This allows consumers to distinguish between "denied" (`decision: false`, no context) and "error" (`decision: false`, context with error details).

For successful evaluations, context is not populated — only the `decision` boolean is returned.

## Pagination

Pagination is not supported. Per the AuthZEN specification, pagination is optional ("a PDP MAY support pagination"). The `page` parameter in search requests is accepted but ignored. All results are returned in a single response. The `page` object is not included in responses.

## Proto Design Decisions

### Separate Filter Types

Search endpoints use `SubjectFilter` and `ResourceFilter` instead of `Subject` and `Resource`. The filter types only require `type` — the `id` field is optional and ignored if provided. This is consistent with the underlying OpenFGA APIs: ListUsers requires a type filter, and ListObjects requires an object type.

### Response Context as google.protobuf.Struct

`EvaluationResponse.context` is a `google.protobuf.Struct` (arbitrary JSON) rather than a structured message. This matches the AuthZen spec where context is implementation-defined, and avoids coupling the proto to specific error fields.

### Evaluations Field

`EvaluationsRequest.evaluations` is a repeated field that can be empty. When empty, the request is treated as a single evaluation using the top-level fields. This matches the AuthZen spec where the evaluations array is optional and an empty list means "evaluate the top-level request as a single evaluation."

### Authorization Model ID

The authorization model ID is passed via an HTTP header (`Openfga-Authorization-Model-Id`) rather than a request body field. This is an OpenFGA-specific extension — the AuthZen spec has no concept of model versioning. The header approach keeps the request body compliant with the AuthZen spec while allowing model pinning.

## Limitations

| Limitation | Reason |
|---|---|
| No pagination on search endpoints | AuthZen pagination is optional; not yet implemented |
| No multi-type subject/resource search | OpenFGA ListUsers and ListObjects require a type filter |
| No contextual tuples | AuthZen has no equivalent concept |
| No decision reasons/obligations in context | OpenFGA Check does not return explanations |
| ActionSearch checks ALL relations on the type | No way to filter which relations are "actions" vs structural |
| Short-circuit semantics are sequential | Order matters, so BatchCheck parallelism cannot be used |
| Properties require underscore-prefixed condition parameters | OpenFGA does not allow `.` in condition parameter names |
