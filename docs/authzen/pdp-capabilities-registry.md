# OpenFGA PDP Capabilities

This document describes the AuthZEN PDP capabilities implemented by OpenFGA. These capabilities follow the registration template defined in the AuthZEN specification for the IANA "AuthZEN Policy Decision Point Capabilities" registry.

---

## Implemented Capabilities

### :search-pagination

| Field | Value |
|-------|-------|
| **Capability Name** | `:search-pagination` |
| **Capability URN** | `urn:ietf:params:authzen:capability:search-pagination` |
| **Capability Description** | PDP supports pagination for Search APIs using `page.token` and `page.limit` request parameters, and returns `page.next_token` and `page.count` in responses. |
| **Change Controller** | OpenID Foundation AuthZEN Working Group |
| **Specification Document(s)** | Authorization API 1.0, Section "Pagination" |

**OpenFGA Implementation:**
- ✅ Supports `page.limit` (default: 50, max: 1000)
- ✅ Supports `page.token` for continuation
- ✅ Returns `page.next_token` when more results available
- ✅ Returns `page.count` with the number of results in current page
- ❌ Does not return `page.total` (would require expensive full enumeration)
- ❌ Does not support `page.properties` for sorting/filtering hints

### :properties-to-context

| Field | Value |
|-------|-------|
| **Capability Name** | `:properties-to-context` |
| **Capability URN** | `urn:ietf:params:authzen:capability:properties-to-context` |
| **Capability Description** | PDP merges `properties` from Subject, Resource, and Action into the evaluation context with namespaced keys (e.g., `subject.department`, `resource.classification`). |
| **Change Controller** | OpenID Foundation AuthZEN Working Group |
| **Specification Document(s)** | Authorization API 1.0, Section "Subject Properties" |

**OpenFGA Implementation:**
- ✅ Subject properties merged with `subject.` prefix (e.g., `subject.department`)
- ✅ Resource properties merged with `resource.` prefix (e.g., `resource.classification`)
- ✅ Action properties merged with `action.` prefix (e.g., `action.method`)
- ✅ Explicit `context` values take precedence over properties
- ✅ Properties available for use in OpenFGA conditions

### :evaluations-semantic

| Field | Value |
|-------|-------|
| **Capability Name** | `:evaluations-semantic` |
| **Capability URN** | `urn:ietf:params:authzen:capability:evaluations-semantic` |
| **Capability Description** | PDP supports `options.evaluations_semantic` in batch evaluation requests with values `execute_all`, `deny_on_first_deny`, and `permit_on_first_permit`. |
| **Change Controller** | OpenID Foundation AuthZEN Working Group |
| **Specification Document(s)** | Authorization API 1.0, Section "Evaluations semantics" |

**OpenFGA Implementation:**
- ✅ `execute_all` - Uses OpenFGA's BatchCheck with parallel execution
- ✅ `deny_on_first_deny` - Sequential execution, stops on first `false` decision
- ✅ `permit_on_first_permit` - Sequential execution, stops on first `true` decision
- Default semantic is `execute_all` when `options.evaluations_semantic` is omitted

### :transitive-search

| Field | Value |
|-------|-------|
| **Capability Name** | `:transitive-search` |
| **Capability URN** | `urn:ietf:params:authzen:capability:transitive-search` |
| **Capability Description** | PDP performs transitive searches, traversing intermediate relationships. For example, if user U is member of group G, and group G can view document D, a subject search for viewers of D includes user U. |
| **Change Controller** | OpenID Foundation AuthZEN Working Group |
| **Specification Document(s)** | Authorization API 1.0, Section "Search Semantics" |

**OpenFGA Implementation:**
- ✅ Full transitive relationship traversal for both SubjectSearch and ResourceSearch
- ✅ SubjectSearch uses OpenFGA's `ListUsers` API with transitive expansion
- ✅ ResourceSearch uses OpenFGA's `StreamedListObjects` API with transitive expansion
- ✅ Respects the authorization model's relation definitions for transitivity

### :abac-context

| Field | Value |
|-------|-------|
| **Capability Name** | `:abac-context` |
| **Capability URN** | `urn:ietf:params:authzen:capability:abac-context` |
| **Capability Description** | PDP supports Attribute-Based Access Control (ABAC) by evaluating the `context` object in authorization decisions. Context attributes can be used in policy conditions. |
| **Change Controller** | OpenID Foundation AuthZEN Working Group |
| **Specification Document(s)** | Authorization API 1.0, Section "Context" |

**OpenFGA Implementation:**
- ✅ Context passed directly to OpenFGA's condition evaluation
- ✅ Supports complex nested objects and arrays in context
- ✅ Merged properties from subject/resource/action available in conditions
- ✅ Conditions defined in authorization model using CEL-like syntax

---

## Capabilities Not Implemented

### :decision-context-reasons

| Field | Value |
|-------|-------|
| **Capability Name** | `:decision-context-reasons` |
| **Capability URN** | `urn:ietf:params:authzen:capability:decision-context-reasons` |
| **Capability Description** | PDP returns a `context` object in evaluation responses containing reasons for the decision, such as which policy matched or why access was denied. |
| **Change Controller** | OpenID Foundation AuthZEN Working Group |
| **Specification Document(s)** | Authorization API 1.0, Section "Decision Context" |

**OpenFGA Status:** ❌ Not implemented. OpenFGA does not currently return decision explanations in the AuthZEN response `context`. The response contains only the `decision` boolean.

### :signed-metadata

| Field | Value |
|-------|-------|
| **Capability Name** | `:signed-metadata` |
| **Capability URN** | `urn:ietf:params:authzen:capability:signed-metadata` |
| **Capability Description** | PDP provides signed metadata via a `signed_metadata` JWT in the discovery endpoint, enabling verification of metadata integrity and issuer authenticity. |
| **Change Controller** | OpenID Foundation AuthZEN Working Group |
| **Specification Document(s)** | Authorization API 1.0, Section "Signature Parameter" |

**OpenFGA Status:** ❌ Not implemented. The `/.well-known/authzen-configuration` endpoint returns unsigned JSON metadata.

---

## Summary

| Capability | Supported | Notes |
|------------|-----------|-------|
| `:search-pagination` | ✅ Yes | `limit`, `token`, `next_token`, `count` supported; `total` not supported |
| `:properties-to-context` | ✅ Yes | Properties merged with `subject.`, `resource.`, `action.` prefixes |
| `:evaluations-semantic` | ✅ Yes | All three semantics: `execute_all`, `deny_on_first_deny`, `permit_on_first_permit` |
| `:transitive-search` | ✅ Yes | Full transitive relationship traversal via ListUsers and StreamedListObjects |
| `:abac-context` | ✅ Yes | Context passed to OpenFGA conditions for ABAC evaluation |
| `:decision-context-reasons` | ❌ No | OpenFGA does not return decision explanations |
| `:signed-metadata` | ❌ No | Metadata endpoint returns unsigned JSON |
