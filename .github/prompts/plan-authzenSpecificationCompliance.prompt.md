## Plan: AuthZEN Specification Compliance Update

This plan makes OpenFGA's AuthZEN implementation fully spec-compliant by adding Search APIs, metadata discovery, evaluation options, and gating all endpoints behind an experimental flag. Changes span both `openfga/api` (protos, released first) and `openfga/openfga` (implementation) repositories.

### Instructions

- Never commit or push code to github directly.


### Steps

1. **Add experimental flag for AuthZEN** in `pkg/server/server.go`: Define `ExperimentalEnableAuthZen ExperimentalFeatureFlag = "enable-authzen"`, add to allowed values in `cmd/run/flags.go`, and gate existing `Evaluation`/`Evaluations` endpoints plus all new endpoints behind `IsExperimentallyEnabled()` checks.

2. **Add Search API proto definitions** in `openfga/api` at `authzen/v1/authzen_service.proto`: Define `SubjectSearch`, `ResourceSearch`, and `ActionSearch` RPC methods with request/response messages including pagination (`page` object with `token`, `limit`, `next_token`, `count`, `total`), mapping to `/stores/{store_id}/access/v1/search/subject`, `/search/resource`, and `/search/action` HTTP endpoints. **Note:** `SubjectSearchRequest` uses `SubjectFilter` (not `Subject`) where `id` is optional but `type` is required, allowing clients to filter by subject type without specifying an id. **Note:** All `properties` fields in `Subject`, `SubjectFilter`, `Resource`, and `Action` messages are marked as `optional` so that nil values are omitted from JSON responses (avoiding `"properties": null`).

3. **Add PDP Metadata proto definition** in `openfga/api`: Define `GetConfiguration` RPC at `GET /.well-known/authzen-configuration/{store_id}` returning `policy_decision_point`, absolute store-specific endpoint URLs, and `capabilities` array per spec section 13. Following the AuthZEN spec's multi-tenant pattern (example: `https://pdp.example.com/.well-known/authzen-configuration/tenant1`), the discovery endpoint is scoped per store and returns absolute endpoint URLs specific to that store, meeting the spec requirement for directly-usable URLs without templating.

4. **Add `evaluations_semantic` option** to `EvaluationsRequest` proto in `openfga/api`: Add `options.evaluations_semantic` enum field with values `EXECUTE_ALL` (default), `DENY_ON_FIRST_DENY`, `PERMIT_ON_FIRST_PERMIT`.

5. **Implement Subject Search handler** in `pkg/server/commands/`: Create `subject_search.go` that calls `ListUsers`, converts results to AuthZEN `Subject` format, and implements in-memory pagination (collect all results, slice by `limit`/`token`, generate `next_token`). **Note:** Subject type is required for SubjectSearch because `ListUsers` requires a `UserFilter` with at least one type. The `SubjectFilter` type (with optional `id`) is used instead of `Subject` (with required `id`) to allow filtering by type only.

6. **Implement Resource Search handler** in `pkg/server/commands/`: Create `resource_search.go` that calls `StreamedListObjects`, converts results to AuthZEN `Resource` format, and implements pagination with early termination - the streaming stops once enough objects are collected for the current page (offset + limit + 1). This is more efficient than fetching all results. Note that without stable ordering from the underlying API, pagination may have duplicates or gaps between pages.

7. **Implement Action Search handler** in `pkg/server/commands/`: Create `action_search.go` that uses `typesystem.GetRelations(resourceType)` to get all relations from the latest model, then performs a Check call for each relation to filter to only permitted actions, with in-memory pagination. **Note:** The server's `ActionSearch` method must resolve the typesystem once and create its own check function that uses the already-resolved typesystem directly. This prevents the `Openfga-Authorization-Model-Id` response header from being duplicated (once per Check call) by avoiding re-resolution of the typesystem in each Check.

8. **Implement PDP Metadata handler** in `pkg/server/`: Create `authzen_configuration.go` that extracts `store_id` from the request and returns configuration with `policy_decision_point`, absolute store-specific endpoint URLs (using `fmt.Sprintf` to inject the store ID), and capabilities. This meets the AuthZEN spec requirement for directly-usable URLs without templating.

9. **Implement `evaluations_semantic` options** in `pkg/server/commands/batch_evaluate.go`: Add short-circuit logic for `DENY_ON_FIRST_DENY` (stop on first `decision: false`, include `context.reason`) and `PERMIT_ON_FIRST_PERMIT` (stop on first `decision: true`).

10. **Map Subject/Resource/Action `properties`** to `context` in `pkg/server/commands/evaluate.go` and `pkg/server/commands/batch_evaluate.go`: Merge `subject.properties`, `resource.properties`, and `action.properties` into the `context` object passed to Check/ListObjects/ListUsers, using namespaced keys (e.g., `subject.department`, `resource.classification`). **Note:** The `MergePropertiesToContext` utility uses a `SubjectPropertiesProvider` interface to accept both `Subject` and `SubjectFilter` types.

11. **Add extensive unit tests for existing Evaluation endpoint** in `pkg/server/commands/evaluate_test.go`: Test AuthZEN-to-OpenFGA request transformation, `subject`/`resource`/`action` field mapping, `context` passthrough, properties-to-context merging (with precedence rules), error code mapping (400/401/403/500), missing required fields validation, and response `decision`/`context` formatting.

12. **Add extensive unit tests for existing Evaluations endpoint** in `pkg/server/commands/batch_evaluate_test.go`: Test default value inheritance (`subject`/`action`/`resource`/`context` from top-level), per-evaluation overrides, `evaluations_semantic` options (`execute_all`, `deny_on_first_deny` with short-circuit, `permit_on_first_permit` with short-circuit), response ordering matches request ordering, individual evaluation errors in `context.error`, and properties merging across default and per-evaluation levels.

13. **Add unit tests for new Search commands** in `pkg/server/commands/`: Create `subject_search_test.go`, `resource_search_test.go`, `action_search_test.go` testing transformation logic, pagination token generation/parsing/validation (invalid tokens, changed parameters mid-pagination), `limit` handling, properties-to-context merging, empty result sets, and error handling.

14. **Add unit tests for PDP Metadata** in `pkg/server/`: Create `authzen_configuration_test.go` testing correct endpoint URL generation, capabilities array, response format compliance with spec section 13.

15. **Add integration tests for existing AuthZEN endpoints** in `tests/authzen/`: Create `evaluation_test.go` and `evaluations_test.go` with end-to-end scenarios including: basic permit/deny decisions, ABAC conditions with context, X-Request-ID header propagation, authentication (401 for missing/invalid auth), error responses (400 for malformed requests, 404 for missing store), properties usage in conditions, and interoperability with existing OpenFGA Check/BatchCheck behavior.

16. **Add integration tests for new AuthZEN endpoints** in `tests/authzen/`: Create `search_test.go` covering Subject/Resource/Action Search with pagination flows (initial request, continuation with token, final page with empty `next_token`), transitive relationship traversal, dynamic Action Search filtering via Check calls, and `metadata_test.go` for `/.well-known/authzen-configuration` endpoint validation.

17. **Add AuthZEN interop test suite integration** in `tests/authzen/`: Create `interop_test.go` that runs the official AuthZEN Todo and Gateway interop scenarios from `docs/authzen/` models, verifying compliance with the published test vectors.

### Further Considerations

1. **Pagination and ordering**: Search APIs sort results by type and ID only when pagination is requested (i.e., when `page` is specified in the request). This ensures consistent results across pagination requests. When no pagination is specified, results are returned in the order provided by the underlying OpenFGA APIs (`ListUsers`, `StreamedListObjects`). For `ResourceSearch`, the implementation uses `StreamedListObjects` with early termination - it stops streaming once it has collected enough objects for the current page (offset + limit + 1), then sorts and paginates.

2. **Pagination performance**: Since OpenFGA's `ListObjects`/`ListUsers` don't support pagination natively, consider adding a configurable maximum result limit at the AuthZEN layer to prevent memory issues on large result sets. Should there be a hard cap? Recommendation: Add a server config option `authzen-max-search-results` defaulting to 1000.

3. **Action Search performance**: Iterating all relations and calling Check for each can be expensive for types with many relations. Should there be a limit on the number of relations checked, or should this be documented as a performance consideration?

4. **Properties namespace conflicts**: If `context` already has a key that conflicts with a property key (e.g., both `context.time` and `subject.properties.time`), which takes precedence? Recommendation: Request-level `context` takes precedence, document this behavior.

