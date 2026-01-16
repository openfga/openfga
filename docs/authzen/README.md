# OpenFGA AuthZEN Implementation and Interop scenarios

## AuthZEN Implementation

OpenFGA includes an experimental implementation of the [Authorization API 1.0](https://github.com/openid/authzen/blob/main/api/authorization-api-1_0.md).

---

## Implementation Decisions and Tradeoffs

This section documents key design decisions made when mapping AuthZEN concepts to OpenFGA's architecture.

### Concept Mapping

| AuthZEN Concept | OpenFGA Equivalent | Notes |
|-----------------|-------------------|-------|
| `subject.type` + `subject.id` | `user` (formatted as `type:id`) | OpenFGA uses a single `user` field with colon-separated type and ID |
| `resource.type` + `resource.id` | `object` (formatted as `type:id`) | OpenFGA uses a single `object` field with colon-separated type and ID |
| `action.name` | `relation` | AuthZEN "actions" map directly to OpenFGA "relations" |
| `context` | `context` | Direct passthrough to OpenFGA's ABAC context |
| `properties` | Merged into `context` | See Properties Handling below |

### Properties Handling

AuthZEN allows `properties` objects on `subject`, `resource`, and `action`. OpenFGA does not have a direct equivalent—it only has a single `context` object for ABAC conditions.

**Decision:** Properties are automatically merged into the OpenFGA `context` with namespaced keys using underscore (`_`) as the separator:

| AuthZEN Property | OpenFGA Context Key |
|------------------|---------------------|
| `subject.properties.department` | `subject_department` |
| `resource.properties.classification` | `resource_classification` |
| `action.properties.severity` | `action_severity` |

**Note:** Underscore is used as the separator because OpenFGA does not allow condition parameters with `.` in their names.

**Precedence:** If there's a key conflict between properties and the request-level `context`, the request-level `context` takes precedence. For example, if both `context.subject.department` and `subject.properties.department` are specified, the value from `context` is used.

**Tradeoff:** This approach requires authorization models to be aware of the namespacing convention. Models must reference properties as `context["subject_department"]` rather than `context["department"]`.

### Search API Pagination

**Current Status:** Pagination is not currently supported per AuthZEN specification (it's an optional feature).

**Behavior:**
- The `page` request parameter is accepted but ignored
- All results are returned in a single response
- The `page` object is not included in responses

**Rationale:** Per the AuthZEN specification, pagination is optional: "a PDP MAY support pagination." When a PDP does not support pagination, it should ignore the `page` request parameter, return all matching results, and not include the `page` object in the response.

**Future Consideration:** Pagination may be implemented in the future if needed for performance with very large result sets

### SubjectSearch and Subject Type Requirement

**Challenge:** OpenFGA's `ListUsers` API requires at least one `UserTypeFilter` specifying the type of users to search for. This is a fundamental requirement of the API.

**Decision:** `SubjectSearch` uses `SubjectFilter` (not `Subject`) where `type` is required but `id` is optional. This allows clients to search for all subjects of a type without specifying a specific ID.

**Tradeoff:** Clients must always specify a subject type when calling `SubjectSearch`. Searching across all subject types in a single request is not supported.

### ActionSearch Implementation

**Challenge:** AuthZEN `ActionSearch` asks "what actions can this subject perform on this resource?" OpenFGA does not have a native API for this—it only supports checking specific relations.

**Decision:** `ActionSearch` is implemented by:
1. Retrieving all relations defined for the resource type from the authorization model
2. Performing a `BatchCheck` call for all relations to determine if the subject has access
3. Returning only the permitted relations as "actions"

**Tradeoffs:**
- **Performance**: This uses a single `BatchCheck` call for all relations on the resource type, which is more efficient than individual checks.
- **Consistency**: All checks use the same authorization model version to ensure consistent results.
- **Ordering**: Actions are sorted alphabetically for consistent response ordering.

### Evaluations Semantic Options

AuthZEN's `evaluations` endpoint supports semantic options to control batch execution behavior:

| Semantic | Behavior | OpenFGA Implementation |
|----------|----------|----------------------|
| `execute_all` (default) | Execute all evaluations | Uses `BatchCheck` for all items |
| `deny_on_first_deny` | Stop on first denied | Short-circuits after first `decision: false` |
| `permit_on_first_permit` | Stop on first permitted | Short-circuits after first `decision: true` |

**Tradeoff:** Short-circuit semantics cannot leverage `BatchCheck` parallelism since order matters. Each evaluation is processed sequentially until the condition is met.

### Response Field Omission

**Decision:** Optional fields with `nil` values are omitted from JSON responses rather than being serialized as `null`. For example, `"properties": null` is never returned.

**Rationale:** This provides cleaner API responses and follows the principle of not including empty optional fields. Proto definitions use `optional` markers to enable this behavior.

### Store ID in URL Path and Discovery Endpoint

**Decision:** All AuthZEN endpoints include `store_id` in the URL path:
- Discovery endpoint: `/.well-known/authzen-configuration/{store_id}`
- Access endpoints: `/stores/{store_id}/access/v1/...`

**Rationale:** OpenFGA is multi-tenant with multiple stores. Following the AuthZEN spec's multi-tenant pattern (example: `https://pdp.example.com/.well-known/authzen-configuration/tenant1`), the discovery endpoint is scoped per store. This returns absolute endpoint URLs specific to that store, meeting the AuthZEN spec requirement for directly-usable URLs without templating.

### Error Mapping

| Condition | AuthZEN HTTP Status | Notes |
|-----------|-------------------|-------|
| Missing required fields | 400 Bad Request | Subject, resource, or action missing |
| Invalid store ID | 404 Not Found | Store does not exist |
| Invalid authorization model | 400 Bad Request | Model ID invalid or not found |
| Internal error | 500 Internal Server Error | Unexpected processing errors |

---

## Features Not Implemented

This section documents AuthZEN specification features that are **not currently implemented** in OpenFGA's AuthZEN layer.

### Response Context (Decision Context)

The AuthZEN specification allows PDPs to return a `context` object in evaluation responses containing:
- **Reason codes** explaining why a decision was made
- **Advices and obligations** tied to the access decision
- **UI rendering hints** for the PEP
- **Step-up authentication instructions** (e.g., required `acr`/`amr` values)
- **Environmental information** and metadata

**Current status:** OpenFGA returns only the `decision` boolean. The response `context` object is not populated with reasons, obligations, or hints.

**Rationale:** OpenFGA's Check API does not natively return decision explanations or obligations. Implementing this would require significant architectural changes.

### Contextual Tuples

AuthZEN does not have a direct equivalent to OpenFGA's contextual tuples feature, which allows passing temporary relationship tuples that exist only for the duration of the request.

**Current status:** There is no mapping from AuthZEN request fields to OpenFGA contextual tuples.

**Workaround:** Clients needing contextual tuples must use the native OpenFGA API directly.

### Search API: Pagination

The AuthZEN specification supports optional pagination for Search APIs with `page.limit`, `page.token`, `page.total`, and `page.properties` fields.

**Current status:** Pagination is not supported. The `page` request parameter is ignored, all results are returned, and the `page` object is not included in responses.

**Rationale:** Per the AuthZEN specification, pagination is optional: "a PDP MAY support pagination." This implementation returns all results in a single response.

### Search API: Multi-Type Subject Search

The AuthZEN specification allows searching for subjects without specifying a type, which would return subjects of all types.

**Current status:** `SubjectSearch` requires a subject `type` to be specified.

**Rationale:** OpenFGA's `ListUsers` API requires at least one `UserTypeFilter`. Searching across all types would require multiple API calls and result aggregation.

### Search API: Action Properties in Responses

The AuthZEN specification allows `Action` objects to include a `properties` field.

**Current status:** `ActionSearch` responses return actions with only the `name` field. Properties are not included.

### Evaluation Response: Properties Passthrough

When a request includes `properties` on subject, resource, or action, the AuthZEN specification does not require echoing them back in responses.

**Current status:** Properties from requests are not included in responses. This is compliant behavior but noted for clarity.

### Advanced Batch Semantics: Parallel Execution Hints

The AuthZEN specification notes that evaluations in a batch "may be executed sequentially or in parallel, left to the discretion of each implementation."

**Current status:**
- `execute_all` uses OpenFGA's `BatchCheck` which executes in parallel
- `deny_on_first_deny` and `permit_on_first_permit` execute sequentially (required for short-circuit semantics)

No mechanism exists for a PEP to hint at desired parallelism behavior.

### Error Reasons in Batch Evaluation Responses

The AuthZEN specification shows examples of individual evaluation errors being conveyed in the response `context` field (e.g., `"error": "resource not found"`).

**Current status:** Individual evaluation errors are not returned with detailed reason codes. Failed evaluations return `decision: false` without additional context.

### PDP Capabilities Registry

The AuthZEN specification references an "AuthZEN Policy Decision Point Capabilities Registry" for declaring supported optional features.

**Current status:** The `/.well-known/authzen-configuration` endpoint returns capabilities but does not reference URNs from a formal registry.

See [pdp-capabilities-registry.md](pdp-capabilities-registry.md) for sample capability definitions and OpenFGA's supported capabilities.

### Transport Bindings Beyond HTTPS

The AuthZEN specification is defined transport-agnostic, with HTTPS as the normative binding. Other bindings (e.g., gRPC) may be defined.

**Current status:** Only HTTPS (REST/JSON) is supported. gRPC bindings for AuthZEN are not implemented, though OpenFGA's native API supports gRPC.

---

### Enabling AuthZEN

AuthZEN endpoints are gated behind an experimental feature flag. To enable them, start OpenFGA with:

```bash
openfga run --experimentals enable_authzen
```

Or set the environment variable:

```bash
OPENFGA_EXPERIMENTALS=enable_authzen openfga run
```

### Specifying Authorization Model ID

By default, AuthZEN endpoints use the latest authorization model for the store. To use a specific model version, you can specify the authorization model ID using:

1. **HTTP Header** (recommended): `Openfga-Authorization-Model-Id`
2. **Request Field**: `authorization_model_id` in the request body

**Example using header:**
```bash
curl -X POST http://localhost:8080/stores/{store_id}/access/v1/evaluation \
  -H "Content-Type: application/json" \
  -H "Openfga-Authorization-Model-Id: 01ARZ3NDEKTSV4RRFFQ69G5FAV" \
  -d '{
    "subject": {"type": "user", "id": "alice"},
    "resource": {"type": "document", "id": "doc1"},
    "action": {"name": "reader"}
  }'
```

**Precedence:** If both the header and request field are specified, the header takes precedence.

**Supported Endpoints:** The header is supported on:
- `POST /stores/{store_id}/access/v1/evaluation`
- `POST /stores/{store_id}/access/v1/search/subject`
- `POST /stores/{store_id}/access/v1/search/resource`
- `POST /stores/{store_id}/access/v1/search/action`

Note: The `evaluations` (batch) endpoint does not support the `authorization_model_id` parameter—it always uses the latest model.

### Supported Endpoints

| AuthZEN Endpoint | HTTP Path | Description |
|------------------|-----------|-------------|
| Evaluation | `POST /stores/{store_id}/access/v1/evaluation` | Single authorization check |
| Evaluations | `POST /stores/{store_id}/access/v1/evaluations` | Batch authorization checks |
| Subject Search | `POST /stores/{store_id}/access/v1/search/subject` | Find subjects with access to a resource |
| Resource Search | `POST /stores/{store_id}/access/v1/search/resource` | Find resources a subject can access |
| Action Search | `POST /stores/{store_id}/access/v1/search/action` | Find actions a subject can perform |
| PDP Metadata | `GET /.well-known/authzen-configuration/{store_id}` | Store-scoped PDP discovery and capabilities |

---

## Evaluation API

The AuthZen `evaluation` endpoint maps to an [OpenFGA `check`](https://openfga.dev/api/service#/Relationship%20Queries/Check) call:

```json
### AuthZen Evaluation
POST /stores/<store_id>/access/v1/evaluation
{
  "subject": {
    "type": "user",
    "id": "<user_id>"
  },
  "resource": {
    "type": "document",
    "id": "<document_id>"
  },
  "action": {
    "name": "can_read"
  },
  "context": {
    "current_time": "1985-10-26T01:22-07:00"
  }
}
```

```json
### OpenFGA Check
POST /stores/<store_id>/check
{
  "tuple_key": {
    "user": "user:<user_id>",
    "relation": "can_read",
    "object": "document:<document_id>",
    "context": {
      "current_time":"1985-10-26T01:22-07:00"
    }  
  }
}
```

---

## Evaluations API (Batch)

The AuthZen `evaluations` endpoint maps to an [OpenFGA `batch-check`](https://openfga.dev/api/service#/Relationship%20Queries/BatchCheck) call:

```json
### AuthZen Evaluations
POST /stores/<store_id>/access/v1/evaluations
{
  "subject": {
    "type": "user",
    "id": "<user_id>"
  },
  "context":{
    "time": "2024-05-31T15:22-07:00"
  },
  "evaluations": [
    {
      "action": {
        "name": "can_read"
      },
      "resource": {
        "type": "document",
        "id": "<document_id>"
      }
    },
    {
      "action": {
        "name": "can_edit"
      },
      "resource": {
        "type": "document",
        "id": "<document_id>"
      }
    }
  ]
}
```

```json
### OpenFGA Check
POST /stores/<store_id>/batch-check
{
  "checks": [
     {
       "tuple_key": {
         "object": "document:<document_id>"
         "relation": "can_edit",
         "user": "user:<user_id>",
       },
        "context":{
            "time": "2024-05-31T15:22-07:00"
        },
       "correlation_id": "1"
     },
    {
       "tuple_key": {
         "object": "document:<document_id>"
         "relation": "can_read",
         "user": "user:<user_id>",
       },
        "context":{
            "time": "2024-05-31T15:22-07:00"
        },
       "correlation_id": "2"
     }
   ]
}
```

### Evaluations Semantic Options

The `evaluations` endpoint supports semantic options to control execution behavior:

| Semantic | Description |
|----------|-------------|
| `execute_all` | (Default) Execute all evaluations and return all results |
| `deny_on_first_deny` | Stop processing on the first denied evaluation |
| `permit_on_first_permit` | Stop processing on the first permitted evaluation |

```json
POST /stores/<store_id>/access/v1/evaluations
{
  "subject": { "type": "user", "id": "alice" },
  "options": {
    "evaluations_semantic": "deny_on_first_deny"
  },
  "evaluations": [
    { "action": { "name": "read" }, "resource": { "type": "document", "id": "doc1" } },
    { "action": { "name": "write" }, "resource": { "type": "document", "id": "doc2" } }
  ]
}
```

### Properties to Context Mapping

AuthZEN `properties` on `subject`, `resource`, and `action` are automatically merged into the OpenFGA `context` with namespaced keys using underscore as the separator:

| AuthZEN Property | OpenFGA Context Key |
|------------------|---------------------|
| `subject.properties.department` | `subject_department` |
| `resource.properties.classification` | `resource_classification` |
| `action.properties.severity` | `action_severity` |

**Precedence:** Request-level `context` takes precedence over properties if there are key conflicts.

```json
{
  "subject": {
    "type": "user",
    "id": "alice",
    "properties": { "department": "engineering" }
  },
  "resource": {
    "type": "document",
    "id": "doc1",
    "properties": { "classification": "confidential" }
  },
  "action": { "name": "read" },
  "context": { "current_time": "2024-01-15T10:00:00Z" }
}
```

This results in a context passed to OpenFGA Check:
```json
{
  "subject_department": "engineering",
  "resource_classification": "confidential",
  "current_time": "2024-01-15T10:00:00Z"
}
```

---

## Search APIs

### Subject Search

Find all subjects that have access to a specific resource for a given action:

```json
POST /stores/<store_id>/access/v1/search/subject
{
  "subject": { "type": "user"},
  "resource": { "type": "document", "id": "doc1" },
  "action": { "name": "reader" }
}
```

Response:
```json
{
  "subjects": [
    { "type": "user", "id": "alice" },
    { "type": "user", "id": "bob" }
  ]
}
```

This maps to OpenFGA's [ListUsers](https://openfga.dev/api/service#/Relationship%20Queries/ListUsers) API.

### Resource Search

Find all resources of a type that a subject can access for a given action:

```json
POST /stores/<store_id>/access/v1/search/resource
{
  "subject": { "type": "user", "id": "alice" },
  "action": { "name": "reader" },
  "resource": { "type": "document" }
}
```

Response:
```json
{
  "resources": [
    { "type": "document", "id": "doc1" },
    { "type": "document", "id": "doc2" }
  ]
}
```

This maps to OpenFGA's [ListObjects](https://openfga.dev/api/service#/Relationship%20Queries/ListObjects) API (via StreamedListObjects).

### Action Search

Find all actions (relations) a subject can perform on a specific resource:

```json
POST /stores/<store_id>/access/v1/search/action
{
  "subject": { "type": "user", "id": "alice" },
  "resource": { "type": "document", "id": "doc1" }
}
```

Response:
```json
{
  "actions": [
    { "name": "reader" },
    { "name": "writer" }
  ]
}
```

This uses a BatchCheck call for all relations defined for the resource type to determine which actions are permitted.

### Pagination

**Note:** Pagination is not currently supported. The `page` request parameter is accepted but ignored—all results are returned in a single response and the `page` object is not included in responses.

Per the AuthZEN specification, pagination is optional: "a PDP MAY support pagination."

---

## PDP Metadata Discovery

The `/.well-known/authzen-configuration/{store_id}` endpoint provides store-scoped PDP discovery per [AuthZEN spec section 13](https://github.com/openid/authzen/blob/main/api/authorization-api-1_1_02.md#13-pdp-discovery).

Following the AuthZEN spec's multi-tenant pattern (example: `https://pdp.example.com/.well-known/authzen-configuration/tenant1`), OpenFGA provides a per-store discovery endpoint that returns absolute endpoint URLs specific to that store.

```json
GET /.well-known/authzen-configuration/01ARZ3NDEKTSV4RRFFQ69G5FAV
```

Response:
```json
{
  "policy_decision_point": {
    "name": "OpenFGA",
    "version": "1.8.0",
    "description": "OpenFGA is a high-performance authorization/permission engine"
  },
  "access_endpoints": {
    "evaluation": "/stores/01ARZ3NDEKTSV4RRFFQ69G5FAV/access/v1/evaluation",
    "evaluations": "/stores/01ARZ3NDEKTSV4RRFFQ69G5FAV/access/v1/evaluations",
    "subject_search": "/stores/01ARZ3NDEKTSV4RRFFQ69G5FAV/access/v1/search/subject",
    "resource_search": "/stores/01ARZ3NDEKTSV4RRFFQ69G5FAV/access/v1/search/resource",
    "action_search": "/stores/01ARZ3NDEKTSV4RRFFQ69G5FAV/access/v1/search/action"
  },
  "capabilities": [
    "evaluation",
    "evaluations",
    "subject_search",
    "resource_search",
    "action_search"
  ]
}
```

**Note:** The endpoint URLs are absolute paths specific to the requested store, meeting the AuthZEN spec requirement for directly-usable URLs without templating.

---

## AuthZEN Interop Scenarios

The [AuthZEN working group](https://openid.net/wg/authzen/) has defined two interoperability scenarios:

- [Todo App Interop Scenario](https://authzen-interop.net/docs/scenarios/todo-1.1/)
- [API Gateway Interop Scenario](https://authzen-interop.net/docs/category/api-gateway-10-draft-02)

These scenarios require their own OpenFGA store, with their model and tuples:

- [OpenFGA Todo App Interop Model](./authzen-todo.fga.yaml)
- [OpenFGA Gateway Interop Model](./authzen-gateway.fga.yaml)

The model files have inline documentation explaining the rationale for the design.

To run tests using the [FGA CLI](https://github.com/openfga/cli), use:

```bash
fga model test --test authzen-todo.fga.yaml
fga model test --test authzen-gateway.fga.yaml
```

There can also use [`authzen-todo.http`](./authzen-todo.http) and [`authzen-gateway.http`](./authzen-gateway.http) using [REST Client](https://marketplace.visualstudio.com/items?itemName=humao.rest-client). 


You can also try it live on the [AuthZen interop website](https://todo.authzen-interop.net/). Use the user credentials specified [here](https://github.com/openid/authzen/blob/main/interop/authzen-todo-application/README.md#identities).

## Running the AuthZEN Todo Interop test suite

- Clone https://github.com/openid/authzen
- Go to the `interop/authzen-todo-backend` folder and follow the instructions in the readme to build the project.
- Set the shared key as an environment variable. You can find it on the OpenFGA vault under the "OpenFGA AuthZeN shared key" name.

```
export AUTHZEN_PDP_API_KEY="<shared-key>"
```

- Run the tests:
```
yarn test https://authzen-interop.openfga.dev/stores/01JG9JGS4W0950VN17G8NNAH3C 
```

## Running the AuthZEN API Gateway Interop test suite

- Clone https://github.com/openid/authzen
- Go to the `interop/authzen-api-gateway` folder and follow the instructions in the readme to build the project.
- Set the shared key as an environment variable. You can find it on the OpenFGA vault under the "OpenFGA AuthZeN shared key" name.

```
export AUTHZEN_PDP_API_KEY="<shared-key>"
```

- Run the tests:
```
yarn test https://authzen-interop.openfga.dev/stores/01JG9JGS4W0950VN17G8NNAH3C 
```
## Running the AuthZEN Todo Application

- Set the shared key as an environment variable. You can find it on the OpenFGA vault under the "OpenFGA AuthZeN shared key" name.

```
export AUTHZEN_PDP_API_KEY='{"OpenFGA": "Bearer <shared-key>"}'
```

- Change the `gateways.--Pass Through--` entry in [pdps.json](https://github.com/openid/authzen/blob/main/interop/authzen-todo-backend/src/pdps.json) to point to "http://localhost:8080".

- Change the `VITE_API_ORIGIN` in the [.env]`https://github.com/openid/authzen/blob/main/interop/authzen-todo-application/.env` file to `http://localhost:8080`

- Follow the instructions in this [readme](https://github.com/openid/authzen/tree/main/interop/authzen-todo-backend) to build and run the back-end app.

- Follow the instructions in this [readme](https://github.com/openid/authzen/blob/main/interop/authzen-todo-application/README.md) to run the front-end app.

## Pointing to a local OpenFGA instance

To run the test suites or the interop application pointing to a local OpenFGA instance, you need to:

- Change the `AUTHZEN_PDP_API_KEY` values to match the ones used by OpenFGA locally.
- Change the [pdps.json](https://github.com/openid/authzen/blob/main/interop/authzen-todo-backend/src/pdps.json) OpenFGA entries and point to the local OpenFGA instance.
- You can run the test suites by pointing to the local OpenFGA instance too:

```
yarn test http://localhost:8080/stores/01JG9JGS4W0950VN17G8NNAH3C 
```
- If you want the Todo App pointing to a local OpenFGA instance, you'll need to change the port that OpenFGA uses, as it conflicts with the one used by the interop backend app:

```
dist/openfga run --http-addr 0.0.0.0:4000        
```
