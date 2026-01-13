---
stand_alone: true
ipr: none
cat: std # Check
submissiontype: IETF
wg: OpenID AuthZEN

docname: authorization-api-1_0

title: Authorization API 1.0
abbrev: azapi
lang: en
kw:
  - Authorization
  - Access Management
  - XACML
  - OPA
  - Topaz
  - Cedar
  - PDP
  - PEP
  - ALFA
# date: 2022-02-02 -- date is filled in automatically by xml2rfc if not given
author:
- role: editor # remove if not true
  ins: O. Gazitt
  name: Omri Gazitt
  org: Aserto
  email: ogazitt@gmail.com
- role: editor # remove if not true
  ins: D. Brossard
  name: David Brossard
  org: Axiomatics
  email: david.brossard@axiomatics.com  
- role: editor # remove if not true
  ins: A. Tulshibagwale
  name: Atul Tulshibagwale
  org: SGNL
  email: atul@sgnl.ai
contributor: # Same structure as author list, but goes into contributors
- name: Marc Jordan
  org: SGNL
  email: marc@sgnl.ai
- name: Erik Gustavson
  org: SGNL
  email: erik@sgnl.ai
- name: Alexandre Babeanu
  org: Indykite
  email: alex.babeanu@indykite.com
- name: David Hyland
  org: ID Partners
  email: dave@idpartners.com.au
- name: Jean-François Lombardo
  org: AWS
  email: jeffsec@amazon.com
- name: Alex Olivier
  org: Cerbos
  email: alex@cerbos.dev
- name: Michiel Trimpe
  org: VNG Realisatie
  email: michiel.trimpe@vng.nl
- name: Elie Azerad
  org: Independent Contributor
  email: elie.azerad@gmail.com

normative:
  RFC6749: # OAuth
  RFC8259: # JSON
  RFC8615: # well-known URIs
  RFC3553: # URN namespace for parameters
  RFC9110: # HTTP Semantics
  XACML:
    title: eXtensible Access Control Markup Language (XACML) Version 1.1
    target: https://www.oasis-open.org/committees/xacml/repository/cs-xacml-specification-1.1.pdf
    author:
    - name: Simon Godik
      role: editor
      org: Overxeer
    - name: Tim Moses (Ed.)
      role: editor
      org: Entrust
    date: 2006

informative:
  RFC7519: # JWT
  RFC7515: # JWS
  RFC8126: # Writing for IANA
  IANA.well-known-uris: # IANA well-known registry
  RFC9525: # Service Identity in TLS
  RFC7234: # HTTP caching
  RFC2617: # HTTP authentication
  NIST.SP.800-162: # ABAC
  RFC7493: # I-JSON

--- abstract

The Authorization API enables Policy Decision Points (PDPs) and Policy Enforcement Points (PEPs) to communicate authorization requests and decisions to each other without requiring knowledge of each other's inner workings. The Authorization API is served by the PDP and is called by the PEP. The Authorization API includes evaluation endpoints, which provide specific access decisions, and search endpoints for discovering permissible subjects, resources, or actions.

--- middle

# Introduction
Computational services often implement access control within their components by separating Policy Decision Points (PDPs) from Policy Enforcement Points (PEPs). PDPs and PEPs are defined in XACML ({{XACML}}) and NIST's ABAC SP 800-162 ({{NIST.SP.800-162}}). Communication between PDPs and PEPs follows similar patterns across different software and services that require or provide authorization information. The Authorization API described in this document enables different providers to offer PDP and PEP capabilities without having to bind themselves to one particular implementation of a PDP or PEP.

# Model
By convention, we refer to a service that implements this API as a Policy Decision Point, or PDP. The policy language, architecture, and state management aspects of a PDP are beyond the scope of this specification.

By convention, we refer to a client of the Authorization API as a Policy Enforcement Point, or PEP. Clients may consume the Authorization API for use cases that go beyond enforcement of authorization decisions; for example, the Resource Search API ({{resource-search-api}}) allows a caller to discover the resources on which a subject can perform an action. For consistency, we use the term PEP to describe a client of the API, regardless of the use case.

The Authorization API is defined in a transport-agnostic manner. A normative HTTPS binding is described in Transport ({{transport}}). Other bindings, such as gRPC, may be defined in other profiles of this specification.

Authentication for the Authorization API itself is out of scope for this document, since authentication for APIs is well-documented elsewhere. Support for OAuth 2.0 ({{RFC6749}}) is RECOMMENDED.

# Features
The core feature of the Authorization API is the Access Evaluation API ({{access-evaluation-api}}), which enables a PEP to determine whether a specific request can be permitted to access a specific resource. The following are non-normative examples:

- Can Alice view document #123?
- Can Alice view document #123 at 16:30 on Tuesday, June 11, 2024?
- Can a manager print?

The Access Evaluations API ({{access-evaluations-api}}) enables execution of multiple evaluations in a single request. The following are non-normative examples:

- Can Alice view documents 123, 234 and 345 on Tuesday, June 11, 2024?
- Can document 123 be viewed by Alice and Bob?

The Search APIs ({{search}}) provide lists of resources, subjects or actions that would be allowed access. The following are non-normative examples:

- Which documents can Alice view?
- Who can view document 123?
- What actions can Alice perform on document 123 on Tuesday, June 11, 2024?

# API Version
This document describes the API version 1.0. Any updates to this API through subsequent revisions of this document or other documents MAY augment this API, but MUST NOT modify the API described here. Augmentation MAY include additional API methods or additional parameters to existing API methods, additional authorization mechanisms, or additional optional headers in HTTPS transport bindings. Endpoints for version 1.0 SHOULD include `v1` in the endpoint identifier (e.g. `https://pdp.example.com/access/v1/`).

# Information Model {#information-model}
The information model for requests and responses include the following entities: Subject, Action, Resource, Context, and Decision. These are all defined below.

## Subject {#subject}
A Subject is the user or machine principal about whom the Authorization API is being invoked. The Subject may be requesting access at the time the Authorization API is invoked.

A Subject is an object that contains two REQUIRED keys, `type` and `id`, which have a string value, and an OPTIONAL key, `properties`, with a value of an object.

`type`:
: REQUIRED. A string value that specifies the type of the Subject.

`id`:
: REQUIRED. A string value containing the unique identifier of the Subject, scoped to the `type`.

`properties`:
: OPTIONAL. An object which can be used to express additional attributes of a Subject.


### Subject Properties {#subject-properties}
Many authorization systems are stateless, and expect the PEP to pass in all relevant attributes used in the evaluation of the authorization policy. To satisfy this requirement, Subjects MAY include additional attributes as key-value pairs, under the `git` object. A property can contain both simple values, such as strings, numbers, booleans and nulls, and complex values, such as arrays and objects.

Examples of subject attributes can include, but are not limited to:

- department,
- group memberships,
- device identifier,
- IP address.

### Examples (non-normative) {#subject-examples}

The following is a non-normative example of a minimal Subject:

~~~ json
{
  "type": "user",
  "id": "alice@example.com"
}
~~~
{: #subject-example title="Example Subject"}

The following is a non-normative example of a Subject which adds a string-valued `department` property:

~~~ json
{
  "type": "user",
  "id": "alice@example.com",
  "properties": {
    "department": "Sales"
  }
}
~~~
{: #subject-department-example title="Example Subject with Additional Property"}

The following is a non-normative example of a subject which adds IP address and device identifier properties:

~~~ json
{
  "type": "user",
  "id": "alice@example.com",
  "properties": {
    "ip_address": "172.217.22.14",
    "device_id": "8:65:ee:17:7e:0b"
  }
}
~~~
{: #subject-device-id-example title="Example Subject with IP Address and Device ID"}

## Resource {#resource}
A Resource is the target of an access request. It is an object that is constructed similar to a Subject entity. It has the following keys:

`type`:
: REQUIRED. A string value that specifies the type of the Resource.

`id`:
: REQUIRED. A string value containing the unique identifier of the Resource, scoped to the `type`.

`properties`:
: OPTIONAL. An object which can be used to express additional attributes of a Resource.

### Resource Properties {#resource-properties}

Similarly to the Subject properties, the PEP can also provide attributes for the Resource in the properties field.

Such attributes can include, but are not limited to, attributes of the resource used in access evaluations or metadata about the resource.

### Examples (non-normative) {#resource-examples}

The following is a non-normative example of a Resource with a `type` and a simple `id`:

~~~ json
{
  "type": "book",
  "id": "123"
}
~~~
{: #resource-example title="Example Resource"}

The following is a non-normative example of a Resource containing a `library_record` property, that is itself an object:

~~~ json
{
  "type": "book",
  "id": "123",
  "properties": {
    "library_record":{
      "title": "AuthZEN in Action",
      "isbn": "978-0593383322"
    }
  }
}
~~~
{: #resource-example-structured title="Example Resource with Additional Property"}

## Action {#action}
An Action is the type of access that the requester intends to perform.

Action is an object that contains a REQUIRED `name` key with a string value, and an OPTIONAL `properties` key with an object value.

`name`:
: REQUIRED. A string value containing the name of the Action.

`properties`:
: OPTIONAL. An object which can be used to express additional attributes of an Action.

### Action Properties {#action-properties}

Similarly to the Subject and Resource properties, the PEP can also provide attributes for the Action in the properties field.

Such attributes can include, but are not limited to, parameters of the action that is being requested.

### Examples (non-normative) {#action-examples}

The following is a non-normative example of an action:

~~~ json
{
  "name": "can_read"
}
~~~
{: #action-example title="Example Action"}

The following is a non-normative example of an action with additional properties:

~~~ json
{
  "name": "extend-loan",
  "properties": {
    "period": "2W"
  }
}
~~~
{: #action-extend-loan-example title="Example Action with properties for extending a book loan."}

## Context {#context}
The Context represents the environment of the access evaluation request.

Context is an object which can be used to express attributes of the environment. 

Examples of context attributes can include, but are not limited to:

- The time of day,
- Location from which the request was received,
- Capabilities of the PEP,
- JSON Schema or JSON-LD definitions for the request.

### Examples (non-normative) {#context-examples}

The following is a non-normative example of a Context:

~~~ json
{
  "time": "1985-10-26T01:22-07:00"
}
~~~
{: #context-example title="Example Context"}


The following example of a Context provides a JSON Schema definition which can be used to parse and validate the AuthZEN request:

~~~ json
{
  "time": "1985-10-26T01:22-07:00",
  "schema": "https://schema.example.com/access-request.schema.json"
}
~~~
{: #context-schema-example title="Example Context with a reference to a JSON schema"}


## Decision {#decision}
A Decision is the result of the evaluation of an access request. It provides the information required for the PEP to enforce the decision.

Decision is an object that contains a REQUIRED `decision` key with a `boolean` value, and an OPTIONAL `context` key with an object value.

`decision`:
: REQUIRED. A boolean value that specifies whether the Decision is to allow or deny the operation.

`context`:
: OPTIONAL. An object which can convey additional information that can be used by the PEP as part of the decision enforcement process.

In this specification, assuming the evaluation was successful, there are only two possible values for the `decision`:

- `true`: The access request is permitted to go forward. If the PEP does not understand information in the `context` response object, the PEP MAY choose to reject the decision.
- `false`: The access request is denied and MUST NOT be permitted to go forward.

The following is a non-normative example of a minimal Decision:

~~~ json
{
  "decision": true
}
~~~
{: #decision-example title="Example Decision"}

### Decision Context {#decision-context}
In addition to a `decision`, a response MAY contain a `context` field which contains an object. This context can convey additional information that can be used by the PEP as part of the decision enforcement process.

Examples include, but are not limited to:

- Reason(s) a decision was made,
- "Advices" and/or "Obligations" tied to the access decision,
- Hints for rendering UI state,
- Instructions for step-up authentication,
- Environmental information,
- etc.

### Examples (non-normative) {#decision-examples}
The following are all non-normative examples of possible and valid contexts, provided to illustrate possible usages. The actual semantics and format of the `context` object are an implementation concern and outside the scope of this specification. For example, implementations MAY use keys that correspond to concepts from other standards, such as HTTP status codes, to convey common reasons in an interoperable manner.

#### Non-normative Example 1: conveying decision Reasons
The PDP may provide reasons to explain a decision. In the non-normative example below, an implementation might convey different reasons to administrators and end-users, using keys that could correspond to HTTP status codes:

~~~ json
{
  "decision": false,
  "context": {
    "reason_admin": {
      "403": "Request failed policy C076E82F"
    },
    "reason_user": {
      "403": "Insufficient privileges. Contact your administrator"
    }
  }
}
~~~
{: #response-with-reason-context-example title="Non-normative Example Response with reason Context"}

#### Non-normative Example 2: conveying metadata and environmental elements
In the following non-normative example, the PDP justifies its decision by including environmental conditions that did not meet its policies. Metadata pertaining to the decision response times is also provided:

~~~ json
{
  "decision": false,
  "context": {
    "metadata": {
      "response_time": 60,
      "response_time_unit": "ms"
    },
    "environment": {
      "ip": "10.10.0.1",
      "datetime": "2025-06-27T18:03-07:00",
      "os": "ubuntu24.04.2LTS-AMDx64"
    }
  }
}
~~~
{: #response-with-environment-context-example title="Non-normative Example Response with Environment and Metadata Context"}

#### Non-normative Example 3: requesting step-up authentication
In the following non-normative example, the PDP requests a step-up authentication of the requesting subject, by signalling the required `acr` and `amr` access token claim values it expects to see in order to approve the request:

~~~ json
{
  "decision": false,
  "context": {
    "acr_values": "urn:com:example:loa:3",
    "amr_values": "mfa hwk"
  }
}
~~~
{: #response-with-step-up-example title="Non-normative Example Response with a step-up request Context"}

# Access Evaluation API {#access-evaluation-api}

The Access Evaluation API defines the message exchange pattern between a PEP and a PDP for executing a single access evaluation.

## The Access Evaluation API Request {#access-evaluation-request}
The Access Evaluation request is an object consisting of four entities previously defined in the Information Model ({{information-model}}):

`subject`:
: REQUIRED. The subject (or principal) of type Subject

`action`:
: REQUIRED. The action (or verb) of type Action.

`resource`:
: REQUIRED. The resource of type Resource.

`context`:
: OPTIONAL. The context (or environment) of type Context.

### Example (non-normative)

~~~ json
{
  "subject": {
    "type": "user",
    "id": "alice@example.com"
  },
  "resource": {
    "type": "account",
    "id": "123"
  },
  "action": {
    "name": "can_read",
    "properties": {
      "method": "GET"
    }
  },
  "context": {
    "time": "1985-10-26T01:22-07:00"
  }
}
~~~
{: #request-example title="Example Request"}

## The Access Evaluation API Response {#access-evaluation-response}

The response of the Access Evaluation API consists of the Decision entity as defined in the Information Model ({{information-model}}).

# Access Evaluations API {#access-evaluations-api}

The Access Evaluations API defines the message exchange pattern between a PEP and a PDP for evaluating multiple access evaluations within the scope of a single message exchange (also known as "boxcarring" requests).

## The Access Evaluations API Request {#access-evaluations-request}

The Access Evaluation API Request builds on the information model presented in {{information-model}} and the object defined in the Access Evaluation Request ({{access-evaluation-request}}).

To send multiple access evaluation requests in a single message, the PEP MAY add an `evaluations` key to the request. The `evaluations` key is an array which contains a list of objects, each typed as the object as defined in the Access Evaluation Request ({{access-evaluation-request}}), and specifying a discrete request.

If an `evaluations` array is NOT present or is empty, the Access Evaluations Request behaves in a backwards-compatible manner with the (single) Access Evaluation API Request ({{access-evaluation-request}}).

If an `evaluations` array IS present and contains one or more objects, these form distinct requests that the PDP will evaluate. These requests are independent from each other, and may be executed sequentially or in parallel, left to the discretion of each implementation.

The top-level `subject`, `action`, `resource`, and `context` keys provide default values for their respective fields in the `evaluations` array.  The top-level `subject`, `action` and `resource` keys MAY be omitted if the `evaluations` array is present, contains one or more objects, and every object in the `evaluations` array contains the respective top-level key. This behavior is described in {{default-values}}.

The following is a non-normative example for specifying three requests, with no default values:

~~~json
{
  "evaluations": [
    {
      "subject": {
        "type": "user",
        "id": "alice@example.com"
      },
      "action": {
        "name": "can_read"
      },
      "resource": {
        "type": "document",
        "id": "boxcarring.md"
      },
      "context": {
        "time": "2024-05-31T15:22-07:00"
      }
    },
    {
      "subject": {
        "type": "user",
        "id": "alice@example.com"
      },
      "action": {
        "name": "can_read"
      },
      "resource": {
        "type": "document",
        "id": "subject-search.md"
      },
      "context": {
        "time": "2024-05-31T15:22-07:00"
      }
    },
    {
      "subject": {
        "type": "user",
        "id": "alice@example.com"
      },
      "action": {
        "name": "can_read"
      },
      "resource": {
        "type": "document",
        "id": "resource-search.md"
      },
      "context": {
        "time": "2024-05-31T15:22-07:00"
      }
    }
  ]
}
~~~

### Default values {#default-values}

While the example above provides the most flexibility in specifying distinct values in each request for every evaluation, it is common for boxcarred requests to share one or more values of the evaluation request. For example, evaluations MAY all refer to a single subject, and/or have the same contextual (environmental) attributes.

Default values offer a more compact syntax that avoids unnecessary duplication of request data.

The top-level `subject`, `action`, `resource`, and `context` keys provide default values for each object in the evaluations array. Any of these keys specified within an individual evaluation object overrides the corresponding top-level default. Because `subject`, `action`, and `resource` are required for a valid evaluation, any of these keys omitted from an evaluation object MUST be provided as a top-level key.

The following is a non-normative example for specifying three requests that refer to a single subject and context:

~~~json
{
  "subject": {
    "type": "user",
    "id": "alice@example.com"
  },
  "context": {
    "time": "2024-05-31T15:22-07:00"
  },
  "evaluations": [
    {
      "action": {
        "name": "can_read"
      },
      "resource": {
        "type": "document",
        "id": "boxcarring.md"
      }
    },
    {
      "action": {
        "name": "can_read"
      },
      "resource": {
        "type": "document",
        "id": "subject-search.md"
      }
    },
    {
      "action": {
        "name": "can_read"
      },
      "resource": {
        "type": "document",
        "id": "resource-search.md"
      }
    }
  ]
}
~~~

The following is a non-normative example for specifying three requests that refer to a single `subject` and `context`, with a default value for `action`, that is overridden by the third request:

~~~json
{
  "subject": {
    "type": "user",
    "id": "alice@example.com"
  },
  "context": {
    "time": "2024-05-31T15:22-07:00"
  },
  "action": {
    "name": "can_read"
  },
  "evaluations": [
    {
      "resource": {
        "type": "document",
        "id": "boxcarring.md"
      }
    },
    {
      "resource": {
        "type": "document",
        "id": "subject-search.md"
      }
    },
    {
      "action": {
        "name": "can_edit"
      },
      "resource": {
        "type": "document",
        "id": "resource-search.md"
      }
    }
  ]
}
~~~

### Evaluations options

The `evaluations` request payload includes an OPTIONAL `options` key, with a value that is an object.

This provides a general-purpose mechanism for providing PEP-supplied metadata on how the request is to be executed.

One such option controls *evaluation semantics*, and is described in {{evaluations-semantics}}.

A non-normative example of the `options` field is shown below, following an `evaluations` array provided for the sake of completeness:

~~~json
{
  "evaluations": [{
    "resource": {
      "type": "doc",
      "id": "1"
    },
    "subject": {
      "type": "doc",
      "id": "2"
    }
  }],
  "options": {
    "evaluations_semantic": "execute_all",
    "another_option": "value"
  }
}
~~~

#### Evaluations semantics {#evaluations-semantics}

By default, every request in the `evaluations` array is executed and a response returned in the same array order. This is the most common use-case for boxcarring multiple evaluation requests in a single payload.

This specification supports three evaluation semantics:

1. *Execute all of the requests (potentially in parallel), return all of the results.* Any failure can be denoted by `"decision": false` and MAY provide a reason code in the context.
2. *Deny on first denial (or failure).* This semantic could be desired if a PEP wants to issue a few requests in a particular order, with any denial (error, or `"decision": false`) "short-circuiting" the evaluations call and returning on the first denial. This essentially works like the `&&` operator in programming languages.
3. *Permit on first permit.* This is the converse "short-circuiting" semantic, working like the `||` operator in programming languages.

To select the desired evaluation semantic, a PEP can pass in `options.evaluations_semantic` with exactly one of the following values:

  * `execute_all`
  * `deny_on_first_deny`
  * `permit_on_first_permit`

`execute_all` is the default semantic, so an `evaluations` request without the `options.evaluations_semantic` flag will execute using this semantic.

##### Example: Evaluate `read` action for three documents using all three semantics

Execute all requests:

~~~json
{
  "subject": {
    "type": "user",
    "id": "alice@example.com"
  },
  "action": {
    "name": "read"
  },
  "options": {
    "evaluations_semantic": "execute_all"
  },
  "evaluations": [
    {
      "resource": {
        "type": "document",
        "id": "1"
      }
    },
    {
      "resource": {
        "type": "document",
        "id": "2"
      }
    },
    {
      "resource": {
        "type": "document",
        "id": "3"
      }
    }
  ]
}
~~~

Response:

~~~json
{
  "evaluations": [
    {
      "decision": true
    },
    {
      "decision": false
    },
    {
      "decision": true
    }
  ]
}
~~~

Deny on first deny:

~~~json
{
  "subject": {
    "type": "user",
    "id": "alice@example.com"
  },
  "action": {
    "name": "read"
  },
  "options": {
    "evaluations_semantic": "deny_on_first_deny"
  },
  "evaluations": [
    {
      "resource": {
        "type": "document",
        "id": "1"
      }
    },
    {
      "resource": {
        "type": "document",
        "id": "2"
      }
    },
    {
      "resource": {
        "type": "document",
        "id": "3"
      }
    }
  ]
}
~~~

Response:

~~~json
{
  "evaluations": [
    {
      "decision": true
    },
    {
      "decision": false,
      "context": {
        "code": "200",
        "reason": "deny_on_first_deny"
      }
    }
  ]
}
~~~

Permit on first permit:

~~~json
{
  "subject": {
    "type": "user",
    "id": "alice@example.com"
  },
  "action": {
    "name": "read"
  },
  "options": {
    "evaluations_semantic": "permit_on_first_permit"
  },
  "evaluations": [
    {
      "resource": {
        "type": "document",
        "id": "1"
      }
    },
    {
      "resource": {
        "type": "document",
        "id": "2"
      }
    },
    {
      "resource": {
        "type": "document",
        "id": "3"
      }
    }
  ]
}
~~~

Response:

~~~json
{
  "evaluations": [
    {
      "decision": true
    }
  ]
}
~~~

## The Access Evaluations API Response {#access-evaluations-response}

Like the request format, the Access Evaluations Response format for an Access Evaluations Request adds an `evaluations` array that lists the decisions in the same order they were provided in the `evaluations` array in the request. Each value of the evaluations array is typed as a Decision as defined in the Information Model ({{information-model}}).

In case the `evaluations` array is present, it is RECOMMENDED that the `decision` key of the response be omitted. If present, it can be ignored by the PEP.

The following is a non-normative example of a Access Evaluations Response to an Access Evaluations Request containing three evaluation objects:

~~~json
{
  "evaluations": [
    {
      "decision": true
    },
    {
      "decision": false,
      "context": {
        "reason": "resource not found"
      }
    },
    {
      "decision": false,
      "context": {
        "reason": "Subject is a viewer of the resource"
      }
    }
  ]
}
~~~

### Errors

There are two types of errors, and they are handled differently:
1. Transport-level errors, or errors that pertain to the entire payload.
2. Errors in individual evaluations.

The first type of error is handled at the transport level. For example, for the HTTP binding, the 4XX and 5XX codes indicate a general error that pertains to the entire payload, as described in Transport ({{transport}}).

The second type of error is handled at the payload level. Decisions default to *closed* (i.e. `false`), but the `context` field can include errors that are specific to that request.

The following is a non-normative example of a response to an Access Evaluations Request containing three evaluation objects, two of them demonstrating how errors can be returned for two of the evaluation requests:

~~~json
{
  "evaluations": [
    {
      "decision": true
    },
    {
      "decision": false,
      "context": {
        "error": {
          "status": 404,
          "message": "Resource not found"
        }
      }
    },
    {
      "decision": false,
      "context": {
        "reason": "Subject is a viewer of the resource"
      }
    }
  ]
}
~~~


# Search APIs {#search}

The Search APIs enable a PEP to discover the set of subjects, resources, or actions that are permitted within a specific authorization context. Their purpose is to return a list of authorized entities, rather than verify a single access request.

To perform a search, the PEP provides the Subject, Resource, Action, and Context entities defined in the Information Model ({{information-model}}), but omits the unique identifier of the entity being queried. The PDP then responds with the set of authorized entities for the queried entity type which would be authorized according to the provided criteria.

## Semantics {#search-semantics}

A search is designed to return entities that would correspond to a permitted decision. Therefore, any result from a Search API, when subsequently used in an Access Evaluation API call, SHOULD result in a `"decision": true` response. However, because the evaluation is implementation-specific and may depend on other variables (such as time), this outcome is not guaranteed.

In addition, it is RECOMMENDED that a search be performed transitively, traversing intermediate attributes and/or relationships. For example, if user U is a member of group G, and group G is designated as a viewer on a document D, then a search for all subjects of type user that can view document D will include user U.

## Pagination {#search-pagination}

Search APIs can return large result sets. To manage this, a PDP MAY support pagination, allowing a PEP to navigate and retrieve subsets of the total result set.

Pagination does not guarantee an atomic snapshot of the result set. Consequently, if items are added or removed while paginating, results MAY be repeated or omitted between pages.

Pagination is based on the use of opaque tokens. A PEP makes an initial request for data by sending a query that does not contain a token. If the PDP determines that the result set contains too many results to fit in a single response, the PDP returns a partial result set and a token that the PEP can use to retrieve the next page of results.

A paginated response MUST be clearly identified by the inclusion of a `page` object containing a non-empty, opaque `next_token`. This token is the signal to the PEP that more results are available.

To retrieve the next page, the PEP sends a subsequent request containing a `page` object with the `token` field set to the `next_token` value from the previous response. This process is repeated until the PDP returns a `page` object in which the value of the `next_token` field is an empty string, signaling the end of the result set.

When a request contains a token, all entities (e.g., `subject`, `resource`, `action`, `context`) and pagination parameters (e.g., `limit`)  MUST be identical to the preceding request. PDPs SHOULD return an error when any entity or parameter has been changed.

PEPs that wish to sequentially iterate through the entire result set SHOULD use the core pagination mechanism described above, which is designed to work consistently across all PDPs that support the search APIs.

### Paginated Requests {#search-pagination-request}

A Search API Request MAY include a `page` object indicating which subset of the larger result set the PEP would like to receive.

The `page` object in a Search API Request consists of the following keys:

`token`:
: OPTIONAL. An opaque string value from the `next_token` of a previous response.

`limit`:
: OPTIONAL. A non-negative integer indicating the maximum number of results to return in the response.

`properties`:
: OPTIONAL. An object containing additional implementation-specific pagination request attributes, such as, but not limited to, sorting and filtering.

Apart from the `token`, all values from the initial request MUST remain identical for subsequent pages. If a different value is provided mid-pagination the PDP SHOULD return an error.

Additional keys MAY be included in the `page` object. If they are, they MUST be defined in a specification referenced in the AuthZEN Policy Decision Point Capabilities Registry ({{iana-pdp-capabilities-registry}}). Furthermore, the PDP MUST declare support for the corresponding capability URN in its `supported_capabilities` metadata ({{pdp-metadata-data-capabilities}}).

### Paginated Responses {#search-pagination-response}

Any Search API Response MAY include a `page` object, but if a response does not contain the entire result set, it MUST include this object.

The `page` object contains the following keys:

`next_token`: 
: REQUIRED. An opaque string value indicating the next page of results to return. If there are no more results after this page, its value MUST be an empty string.

`count`: 
: OPTIONAL. A non-negative integer indicating the number of results included in this response. When included at the start of a response, as described in the Search API Response ({{search-response}}), this enables a PEP to display a progress indicator when processing large or slow responses.

`total`:
: OPTIONAL. A non-negative integer indicating the total number of results matching the query criteria at the time of the request. This value is not guaranteed to equal the total number of items returned across all pages if the underlying data set changes during pagination.

`properties`:
: OPTIONAL. An object containing additional pagination response attributes. Examples include, but are not limited to, estimated totals or the number of remaining results.

### Examples (non-normative) {#search-pagination-examples}

The following is a non-normative example of a request-response cycle to retrieve a total of three results with a page size limit of two.

~~~ json
{
  "subject": {
    "type": "user",
    "id": "alice@example.com"
  },
  "action": {
    "name": "can_read"
  },
  "resource": {
    "type": "account"
  },
  "page": {
    "limit": 2
  }
}
~~~
{: #search-pagination-token-initial-request title="Example initial Search API Request"}

~~~ json
{
  "page": {
    "next_token": "a3M9NDU2O3N6PTI=",
    "count": 2,
    "total": 3
  },
  "results": [
    {
      "type": "account",
      "id": "123"
    },
    {
      "type": "account",
      "id": "456"
    }
  ]
}
~~~
{: #search-pagination-token-initial-response title="Example initial Search API Response"}

~~~ json
{
  "subject": {
    "type": "user",
    "id": "alice@example.com"
  },
  "action": {
    "name": "can_read"
  },
  "resource": {
    "type": "account"
  },
  "page": {
    "token": "a3M9NDU2O3N6PTI="
  }
}
~~~
{: #search-pagination-token-second-request title="Example second Search API Request"}

~~~ json
{
  "page": {
    "next_token": "",
    "count": 1,
    "total": 3
  },
  "results": [
    {
      "type": "account",
      "id": "789"
    }
  ]
}
~~~
{: #search-pagination-token-second-response title="Example second Search API Response"}

## The Search API Response {#search-response}

The response to a Search API Request always follows the same structure. Each Search API Response is a JSON object with the following keys:

`page`:
: OPTIONAL. An object providing pagination information, as defined in Paginated Responses ({{search-pagination-response}}). It is RECOMMENDED that the `page` object be the first key in the response, as this allows a PEP to use the `count` value to display a progress indicator when processing large or slow responses.

`context`:
: OPTIONAL. An object that can convey additional information that can be used by the PEP, similar to its function in the Access Evaluation Response (see {{access-evaluation-response}}).

`results`:
: REQUIRED. An array containing zero or more entities, as defined in the Information Model ({{information-model}}). It MUST contain only entities of the type being searched for (e.g., Subjects, Resources, or Actions).

The following is a non-normative example of a search response returning resources:

~~~ json
{
  "page": {
    "count": 2,
    "total": 102
  },
  "context": {
    "query_execution_time_ms": 42
  },
  "results": [
    {
      "type": "account",
      "id": "123"
    },
    {
      "type": "account",
      "id": "456"
    }
  ]
}
~~~
{: #search-response-example title="Example Resource Search API Response"}

## Subject Search API {#subject-search-api}

The Subject Search API returns all subjects of a given type that are permitted according to the provided Action ({{action}}), Resource ({{resource}}), and Context ({{context}}).

### The Subject Search API Request {#subject-search-request}

The Subject Search request is an object consisting of the following entities:

`subject`:
: REQUIRED. The subject (or principal) of type Subject. The Subject MUST contain a `type`, but the Subject `id` SHOULD be omitted, and if present, MUST be ignored.

`action`:
: REQUIRED. The action (or verb) of type Action.

`resource`:
: REQUIRED. The resource of type Resource.

`context`:
: OPTIONAL. Contextual data about the request.

`page`:
: OPTIONAL. A page object for paginated requests.

### Example (non-normative)

The following payload defines a request for the subjects of type `user` that can perform the `can_read` action on the resource of type `account` and ID `123`.

~~~ json
{
  "subject": {
    "type": "user"
  },
  "action": {
    "name": "can_read"
  },
  "resource": {
    "type": "account",
    "id": "123"
  },
  "context": {
    "time": "2024-10-26T01:22-07:00"
  }
}
~~~
{: #subject-search-request-example title="Example Subject Search API Request"}

The following payload defines a valid response to this request.

~~~ json
{
  "results": [
    {
      "type": "user",
      "id": "alice@example.com"
    },
    {
      "type": "user",
      "id": "bob@example.com"
    }
  ]
}
~~~
{: #subject-search-response-example title="Example Subject Search API Response"}

## Resource Search API {#resource-search-api}

The Resource Search API returns all resources of a given type that are permitted according to the provided Action ({{action}}), Subject ({{subject}}), and Context ({{context}}).

### The Resource Search API Request {#resource-search-request}

The Resource Search request is an object consisting of the following entities:

`subject`:
: REQUIRED. The subject (or principal) of type Subject.

`action`:
: REQUIRED. The action (or verb) of type Action.

`resource`:
: REQUIRED. The resource of type Resource. The Resource MUST contain a `type`, but the Resource `id` SHOULD be omitted, and if present, MUST be ignored.

`context`:
: OPTIONAL. Contextual data about the request.

`page`:
: OPTIONAL. A page object for paginated requests.

### Example (non-normative)

The following payload defines a request for the resources of type `account` on which the subject of type `user` and ID `alice@example.com` can perform the `can_read` action.

~~~ json
{
  "subject": {
    "type": "user",
    "id": "alice@example.com"
  },
  "action": {
    "name": "can_read"
  },
  "resource": {
    "type": "account"
  }
}
~~~
{: #resource-search-request-example title="Example Resource Search API Request"}

The following payload defines a valid response to this request.

~~~ json
{
  "results": [
    {
      "type": "account",
      "id": "123"
    },
    {
      "type": "account",
      "id": "456"
    }
  ]
}
~~~
{: #resource-search-response-example title="Example Resource Search API Response"}

## Action Search API {#action-search-api}

The Action Search API returns all actions that are permitted according to the provided Subject ({{subject}}), Resource ({{resource}}), and Context ({{context}}).

### The Action Search API Request {#action-search-request}

The Action Search request is an object consisting of the following entities:

`subject`:
: REQUIRED. The subject (or principal) of type Subject.

`resource`:
: REQUIRED. The resource of type Resource.

`context`:
: OPTIONAL. Contextual data about the request.

`page`:
: OPTIONAL. A page object for paginated requests.

Note: Unlike the Subject and Resource Search APIs, the `action` key is omitted from the Action Search request payload.

### Example (non-normative)

The following payload defines a request for the actions that the subject of type `user` with ID `123` may perform on the resource of type `account` and ID `123` at 01:22 AM.

~~~ json
{
  "subject": {
    "type": "user",
    "id": "alice@example.com"
  },
  "resource": {
    "type": "account",
    "id": "123"
  },
  "context": {
    "time": "2024-10-26T01:22-07:00"
  }
}
~~~
{: #action-search-request-example title="Example Action Search API Request"}

The following payload defines a valid response to this request.

~~~ json
{
  "results": [
    {
      "name": "can_read"
    },
    {
      "name": "can_write"
    }
  ]
}
~~~
{: #action-search-response-example title="Example Action Search API Response"}

# Policy Decision Point Metadata {#pdp-metadata}

It is RECOMMENDED that PDPs provide metadata describing their configuration.

## Data structure {#pdp-metadata-data}

The following Policy Decision Point metadata parameters are used by this specification and are registered in the IANA "AuthZEN Policy Decision Point Metadata" registry established in {{iana-pdp-metadata-registry}}.

### Endpoint Parameters {#pdp-metadata-data-endpoint}

`policy_decision_point`:
: REQUIRED. The Policy Decision Point identifier, which is a URL that uses the "https" scheme and has no query or fragment components. Policy Decision Point metadata is published at a location that is ".well-known" according to {{RFC8615}} derived from this Policy Decision Point identifier, as described in {{pdp-metadata-access}}. The Policy Decision Point identifier is used to prevent Policy Decision Point mix-up attacks.

`access_evaluation_endpoint`:
: REQUIRED. URL of Access Evaluation API endpoint

`access_evaluations_endpoint`:
: OPTIONAL. URL of Access Evaluations API endpoint

`search_subject_endpoint`:
: OPTIONAL. URL of Search API endpoint for subject entities

`search_action_endpoint`:
: OPTIONAL. URL of Search API endpoint for action entities

`search_resource_endpoint`:
: OPTIONAL. URL of Search API endpoint for resource entities

Note: the absence of any of these parameters is sufficient for the PEP to determine that the PDP is not capable and therefore will not return a result for the associated API.

### Capabilities Parameters {#pdp-metadata-data-capabilities}

`capabilities`:
: OPTIONAL. JSON array containing a list of registered IANA URNs referencing PDP specific capabilities.

### Signature Parameter {#pdp-metadata-data-sig}

In addition to JSON elements, metadata parameters MAY also be provided as a `signed_metadata` value, which is a JSON Web Token {{RFC7519}} that asserts metadata values about the PDP as a bundle. A set of metadata parameters that can be used in signed metadata as claims are defined in {{pdp-metadata-data-endpoint}}. The signed metadata MUST be digitally signed or MACed using JSON Web Signature {{RFC7515}} and MUST contain an `iss` (issuer) claim denoting the party attesting to the claims in the signed metadata.

A PEP MAY ignore the signed metadata if they do not support this feature. If the PEP supports signed metadata, metadata values conveyed in the signed metadata MUST take precedence over the corresponding values conveyed using plain JSON elements. Signed metadata is included in the Policy Decision Point metadata JSON object using this OPTIONAL metadata parameter:

`signed_metadata`:
: A JWT containing metadata parameters about the protected resource as claims. This is a string value consisting of the entire signed JWT. A `signed_metadata` parameter SHOULD NOT appear as a claim in the JWT; it is RECOMMENDED to reject any metadata in which this occurs.

## Obtaining Policy Decision Point Metadata {#pdp-metadata-access}

PDPs supporting metadata MUST make a JSON document containing metadata as specified in the AuthZEN Policy Decision Point Metadata Registry ({{iana-pdp-metadata-registry}}) available at a URL formed by inserting a well-known URI string between the host component and the path and/or query components, if any. The well-known URI string used is `/.well-known/authzen-configuration`.

The syntax and semantics of .well-known are defined in {{RFC8615}}. The well-known URI path suffix used is registered in the IANA "Well-Known URIs" registry {{IANA.well-known-uris}}.

An example of a PDP supporting multiple tenants will have a discovery endpoint as follows:

~~~
https://pdp.example.com/.well-known/authzen-configuration/tenant1
~~~

### Policy Decision Point Metadata Request {#pdp-metadata-access-request}

A Policy Decision Point metadata document MUST be queried using an HTTP GET request at the previously specified URL. The consumer of the metadata would make the following request when the resource identifier is https://pdp.example.com:

~~~ http
GET /.well-known/authzen-configuration HTTP/1.1
Host: pdp.example.com
~~~

### Policy Decision Point Metadata Response {#pdp-metadata-access-response}

The response is a set of metadata parameters about the protected resource's configuration. 

A successful response MUST use the HTTP status code `200` and a `Content-Type` of `application/json`. Its body MUST be a JSON object that contains a set of metadata parameters as defined in the AuthZEN Policy Decision Point Metadata Registry ({{iana-pdp-metadata-registry}}). 

Any metadata parameters in the response that are not understood by the PEP MUST be ignored.

Parameters that have multiple values are represented as JSON arrays. Parameters that have no values MUST be omitted from the response.

An error response uses the applicable HTTP status code value.

The following is a non-normative example response:

~~~ http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "policy_decision_point": "https://pdp.example.com",
  "access_evaluation_endpoint": "https://pdp.example.com/access/v1/evaluation",
  "search_subject_endpoint": "https://pdp.example.com/access/v1/search/subject",
  "search_resource_endpoint": "https://pdp.example.com/access/v1/search/resource"
}
~~~

### Policy Decision Point Metadata Validation {#pdp-metadata-data-endpoint-validation}

The `policy_decision_point` value returned MUST be identical to the Policy Decision Point identifier value into which the well-known URI string was inserted to create the URL used to retrieve the metadata.  If these values are not identical, the data contained in the response MUST NOT be used.

The recipient MUST validate that any signed metadata was signed by a key belonging to the issuer and that the signature is valid. If the signature does not validate or the issuer is not trusted, the recipient SHOULD treat this as an error condition.

# Transport {#transport}

This specification defines an HTTPS binding using JSON serialization which MUST be implemented by a compliant PDP.

Additional transport bindings (e.g. gRPC or CoAP) MAY be defined in the future in the form of profiles, and MAY be implemented by a PDP.

## HTTPS JSON Binding {#transport-https-json}

All API requests within this binding are made via an HTTPS `POST` request. 

Requests MUST include a `Content-Type` header with the value `application/json`, and the request body for each endpoint MUST be a JSON object that conforms to the corresponding request structure, as defined in {{table-api-endpoints}}.

A successful response is an HTTPS response with a status code of `200` and a `Content-Type` of `application/json`. Its body is a JSON object that conforms to the corresponding response structure, as defined in {{table-api-endpoints}}.

The request URL MUST be the value of the corresponding endpoint parameter, as defined in {{table-api-endpoints}}, if it is provided in the Policy Decision Point metadata ({{pdp-metadata-data-endpoint}}). If the parameter is not provided, the URL SHOULD be formed by appending the default path, as defined in {{table-api-endpoints}}, to the PDP's base URL (which is the `policy_decision_point` value from the Policy Decision Point metadata, if available.

The following table provides an overview of the API endpoints defined in this binding:

| API Endpoint       | Default Path               | Metadata Parameter          | Request Schema                 | Response Schema                 |
|--------------------|----------------------------|-----------------------------|--------------------------------|---------------------------------|
| Access Evaluation  | /access/v1/evaluation      | access_evaluation_endpoint  | {{access-evaluation-request}}  | {{access-evaluation-response}}  |
| Access Evaluations | /access/v1/evaluations     | access_evaluations_endpoint | {{access-evaluations-request}} | {{access-evaluations-response}} |
| Subject Search     | /access/v1/search/subject  | search_subject_endpoint     | {{subject-search-request}}     | {{search-response}}     |
| Resource Search    | /access/v1/search/resource | search_resource_endpoint    | {{resource-search-request}}    | {{search-response}}    |
| Action Search      | /access/v1/search/action   | search_action_endpoint      | {{action-search-request}}      | {{search-response}}      |
{: #table-api-endpoints title="API Endpoint Overview"}      

### JSON Serialization {#transport-https-json-serialization}

This section specifies the serialization of the information model entities and API schemas defined in this document to the JSON format {{RFC8259}}. The top-level element of all request and response bodies MUST be a JSON object ({{Section 4 of RFC8259}}). Implementations SHOULD also adhere to the security recommendations in JSON Payload Considerations ({{security-json}}).

The data types defined in this specification are mapped to JSON types as follows:

Object:
: Represented as a JSON object ({{Section 4 of RFC8259}}). The values of its members can be any valid JSON value as defined in {{Section 3 of RFC8259}}, including other objects and arrays, unless specified otherwise.

Array:
: Represented as a JSON array ({{Section 5 of RFC8259}}).

String:
: Represented as a JSON string ({{Section 7 of RFC8259}}).

Integer:
: Represented as a JSON number ({{Section 6 of RFC8259}}). Note the recommendation in {{security-json}} to not encode values that exceed IEEE 754 double-precision.

Boolean:
: Represented as the JSON literals `true` or `false` ({{Section 3 of RFC8259}}).

If a required attribute in the information model is omitted, the server MUST return a "Bad Request" error, as defined in {{error-responses}}.

To ensure forward compatibility, receivers MUST ignore unknown fields present in request or response bodies. Implementations MUST NOT assume a particular ordering of JSON object members.

### Error Responses {#error-responses}
The following error responses are common to all methods of the Authorization API. The error response is indicated by an HTTPS status code ({{Section 15 of RFC9110}}) that indicates error.

The following errors are indicated by the status codes defined below:

| Code | Description  | HTTPS Body Content |
|------|--------------|-------------------|
| 400  | Bad Request  | An error message string |
| 401  | Unauthorized | An error message string |
| 403  | Forbidden    | An error message string |
| 500  | Internal Error | An error message string |
{: #table-error-status-codes title="HTTPS Error status codes"}

Note: HTTPS errors are returned by the PDP to indicate an error condition relating to the request or its processing; they are unrelated to the outcome of an authorization decision and are distinct from it. A successful request that results in a "deny" is indicated by a 200 OK status code with a { "decision": false } payload.

To make this concrete:

- a `401` HTTPS status code indicates that the PEP did not properly authenticate to the PDP - for example, by omitting a required `Authorization` header, or using an invalid access token.
- the PDP indicates to the PEP that the authorization request is denied by sending a response with a `200` HTTPS status code, along with a payload of `{ "decision": false }`.

### Request Identification
All requests to the API MAY have request identifiers to uniquely identify them. The PEP is responsible for generating the request identifier. If present, it is RECOMMENDED to use the HTTPS Header `X-Request-ID` as the request identifier. The value of this header is an arbitrary string. The following non-normative example describes this header:

~~~ http
POST /access/v1/evaluation HTTP/1.1
Authorization: Bearer mF_9.B5f-4.1JqM
X-Request-ID: bfe9eb29-ab87-4ca3-be83-a1d5d8305716
~~~
{: #request-id-example title="Example HTTPS request with a Request Id Header"}

When an Authorization API request contains a request identifier the PDP MUST include a request identifier in the response. It is RECOMMENDED to specify the request identifier using the HTTPS Response header `X-Request-ID`. If the PEP specified a request identifier in the request, the PDP MUST include the same identifier in the response to that request.

The following is a non-normative example of an HTTPS Response with this header:

~~~ http
HTTP/1.1 OK
Content-Type: application/json
X-Request-ID: bfe9eb29-ab87-4ca3-be83-a1d5d8305716
~~~
{: #example-response-request-id title="Example HTTPS response with a Request Id Header"}

### Examples (non-normative)

The following is a non-normative example of the HTTPS binding of the Access Evaluation Request:

~~~ http
POST /access/v1/evaluation HTTP/1.1
Host: pdp.example.com
Content-Type: application/json
Authorization: Bearer <myoauthtoken>
X-Request-ID: bfe9eb29-ab87-4ca3-be83-a1d5d8305716

{
  "subject": {
    "type": "user",
    "id": "alice@example.com"
  },
  "resource": {
    "type": "todo",
    "id": "1"
  },
  "action": {
    "name": "can_read"
  },
  "context": {
    "time": "1985-10-26T01:22-07:00"
  }
}
~~~
{: #example-access-evaluation-request title="Example of an HTTPS Access Evaluation Request"}

The following is a non-normative example of an HTTPS Access Evaluation Response:

~~~ http
HTTP/1.1 OK
Content-Type: application/json
X-Request-ID: bfe9eb29-ab87-4ca3-be83-a1d5d8305716

{
  "decision": true
}
~~~
{: #example-access-evaluation-response title="Example of an HTTP Access Evaluation Response"}

The following is a non-normative example of a the HTTPS binding of the Access Evaluations Request:

~~~ http
POST /access/v1/evaluations HTTP/1.1
Host: pdp.example.com
Content-Type: application/json
Authorization: Bearer <myoauthtoken>
X-Request-ID: bfe9eb29-ab87-4ca3-be83-a1d5d8305716

{
  "subject": {
    "type": "user",
    "id": "alice@example.com"
  },
  "context": {
    "time": "2024-05-31T15:22-07:00"
  },
  "action": {
    "name": "can_read"
  },
  "evaluations": [
    {
      "resource": {
        "type": "document",
        "id": "boxcarring.md"
      }
    },
    {
      "resource": {
        "type": "document",
        "id": "subject-search.md"
      }
    },
    {
      "action": {
        "name": "can_edit"
      },
      "resource": {
        "type": "document",
        "id": "resource-search.md"
      }
    }
  ]
}
~~~
{: #example-access-evaluations-request title="Example of an HTTPS Access Evaluations Request"}

The following is a non-normative example of an HTTPS Access Evaluations Response:

~~~ http
HTTP/1.1 OK
Content-Type: application/json
X-Request-ID: bfe9eb29-ab87-4ca3-be83-a1d5d8305716

{
  "evaluations": [
    {
      "decision": true
    },
    {
      "decision": false,
      "context": {
        "error": {
          "status": 404,
          "message": "Resource not found"
        }
      }
    },
    {
      "decision": false,
      "context": {
        "reason": "Subject is a viewer of the resource"
      }
    }
  ]
}
~~~
{: #example-access-evaluations-response title="Example of an HTTPS Access Evaluations Response"}

The following is a non-normative example of the HTTPS binding of the Subject Search Request:

~~~ http
POST /access/v1/search/subject HTTP/1.1
Host: pdp.example.com
Content-Type: application/json
Authorization: Bearer <myoauthtoken>
X-Request-ID: bfe9eb29-ab87-4ca3-be83-a1d5d8305716

{
  "subject": {
    "type": "user"
  },
  "action": {
    "name": "can_read"
  },
  "resource": {
    "type": "account",
    "id": "123"
  }
}
~~~
{: #example-subject-search-request title="Example of an HTTPS Subject Search Request"}

The following is a non-normative example of an HTTPS Subject Search Response:

~~~ http
HTTP/1.1 OK
Content-Type: application/json
X-Request-ID: bfe9eb29-ab87-4ca3-be83-a1d5d8305716

{
  "page": {
    "next_token": "a3M9NDU2O3N6PTI="
  },
  "results": [
    {
      "type": "user",
      "id": "alice@example.com"
    },
    {
      "type": "user",
      "id": "bob@example.com"
    }
  ]
}
~~~
{: #example-subject-search-response title="Example of an HTTPS Subject Search Response"}

The following is a non-normative example of the HTTPS binding of the Resource Search Request:

~~~ http
POST /access/v1/search/resource HTTP/1.1
Host: pdp.example.com
Content-Type: application/json
Authorization: Bearer <myoauthtoken>
X-Request-ID: bfe9eb29-ab87-4ca3-be83-a1d5d8305716

{
  "subject": {
    "type": "user",
    "id": "alice@example.com"
  },
  "action": {
    "name": "can_read"
  },
  "resource": {
    "type": "account"
  }
}
~~~
{: #example-resource-search-request title="Example of an HTTPS Resource Search Request"}

The following is a non-normative example of an HTTPS Resource Search Response:

~~~ http
HTTP/1.1 OK
Content-Type: application/json
X-Request-ID: bfe9eb29-ab87-4ca3-be83-a1d5d8305716

{
  "page": {
    "next_token": "a3M9NDU2O3N6PTI="
  },
  "results": [
    {
      "type": "account",
      "id": "123"
    },
    {
      "type": "account",
      "id": "456"
    }
  ]
}
~~~
{: #example-resource-search-response title="Example of an HTTPS Resource Search Response"}

The following is a non-normative example of the HTTPS binding of the Action Search Request:

~~~ http
POST /access/v1/search/action HTTP/1.1
Host: pdp.example.com
Content-Type: application/json
Authorization: Bearer <myoauthtoken>
X-Request-ID: bfe9eb29-ab87-4ca3-be83-a1d5d8305716

{
  "subject": {
    "type": "user",
    "id": "alice@example.com"
  },
  "resource": {
    "type": "account",
    "id": "123"
  },
  "context": {
    "time": "2024-10-26T01:22-07:00"
  }
}
~~~
{: #example-action-search-request title="Example of an HTTPS Action Search Request"}

The following is a non-normative example of an HTTPS Action Search Response:

~~~ http
HTTP/1.1 OK
Content-Type: application/json
X-Request-ID: bfe9eb29-ab87-4ca3-be83-a1d5d8305716

{
  "page": {
    "next_token": "a3M9NDU2O3N6PTI="
  },
  "results": [
    {
      "name": "can_read"
    },
    {
      "name": "can_write"
    }
  ]
}
~~~
{: #example-action-search-response title="Example of an HTTPS Action Search Response"}


# Security Considerations {#Security}

## Communication Integrity and Confidentiality {#security-integrity-confidentiality}

In the ABAC architecture, the PEP-PDP connection is the most sensitive one and needs to be secured to guarantee:

 - Integrity
 - Confidentiality

As a result, the connection between the PEP and the PDP MUST be secured using the most adequate means given the choice of transport (e.g. TLS for HTTP REST).

## Policy Confidentiality and Sender Authentication {#security-confidentiality-authn}

Additionally, the PDP SHOULD authenticate the calling PEP. There are several ways authentication can be established. These ways are out of scope of this specification. They MAY include:

 - Mutual TLS
 - OAuth-based authentication
 - API key

The choice and strength of either mechanism is not in scope.

Authenticating the PEP allows the PDP to avoid common attacks (such as DoS - see below) and/or reveal its internal policies. A malicious actor could craft a large number of requests to try and understand what policies the PDP is configured with. Requesting a PEP be authenticated mitigates that risk.

## Sender Authentication Failure {#security-sender-authentn-fail}

If the protected resource request does not include the proper authentication credentials, or does not have a valid authentication scheme proof that enables access to the protected resource, the resource server MUST respond with a 401 HTTP status code and SHOULD include the HTTP "WWW-Authenticate" response header field; it MAY include it in response to other conditions as well. The "WWW-Authenticate" header field uses the framework defined by HTTP/1.1 {{RFC2617}} and indicates the expected authentication scheme as well as the realm that has authority for it.

The following is a non-normative example response:

```http
HTTP/1.1 401 Unauthorized
WWW-Authenticate: Bearer realm="https://as.example.com"
```

## Trust {#security-trust}

In ABAC, there are occasionally conversations around the trust between PEP and PDP: how can the PDP trust that the PEP is sending the correct values? The architecture of this model assumes the PDP must trust the PEP, as the PEP is ultimately responsible for enforcing the decision the PDP produces. 

## JSON Payload Considerations {#security-json}

To ensure the unambiguous interpretation of JSON payloads, implementations SHOULD process and generate JSON payloads in a manner consistent with the I-JSON profile ({{RFC7493}}). In particular, implementations SHOULD ensure that:

- JSON text is encoded as UTF-8, and strings do not contain invalid Unicode sequences such as unpaired surrogates ({{Section 2.1 of RFC7493}}).
- Numeric values do not exceed the magnitude or precision supported by IEEE 754 double-precision ({{Section 2.2 of RFC7493}}).  
- Member names within a JSON object are unique after processing escape characters ({{Section 2.3 of RFC7493}}).

To avoid ambiguity between a property that is absent and one that is present with a null value, properties with a value of null SHOULD be omitted from JSON objects.

## Authorization Response Integrity {#security-authorization-response-integrity}

The PDP MAY choose to sign its authorization response, ensuring the PEP can verify the integrity of the data it receives. This practice is valuable for maintaining trust in the authorization process.

The PEP can ensure that the authorization response is not tampered with by verifying the signature of the authorization decision if it is signed. It ensures response accuracy and completeness. 

TLS effectively protects data in transit for a direct, point-to-point connection but does not guarantee data integrity for the full connection path between the PEP and the PDP if there are intermediaries, such as proxies or gateways. 

Digital signatures offer important advantages in this context. They provide non-repudiation, allowing verification that the response genuinely originated from the PDP. Moreover, digital signatures ensure the integrity of the authorization response, confirming that its contents have not been altered in transit.

## Availability & Denial of Service {#security-avail-dos}

The PDP SHOULD apply reasonable protections to avoid common attacks tied to request payload size, the number of requests, invalid JSON, nested JSON attacks, or memory consumption. Rate limiting is one such way to address such issues.

## Differences between Unsigned and Signed Metadata {#security-metadata-sig}

Unsigned metadata is integrity protected by use of TLS at the site where it is hosted. This means that its security is dependent upon the Internet Public Key Infrastructure (PKI) {{RFC9525}}. Signed metadata is additionally integrity protected by the JWS signature applied by the issuer, which is not dependent upon the Internet PKI.
When using unsigned metadata, the party issuing the metadata is the PDP itself. Whereas, when using signed metadata, the party issuing the metadata is represented by the `iss` (issuer) claim in the signed metadata. When using signed metadata, applications can make trust decisions based on the issuer that performed the signing -- information that is not available when using unsigned metadata. How these trust decisions are made is out of scope for this specification.

## Metadata Caching {#security-metadata-caching}

Policy Decision Point metadata is retrieved using an HTTP GET request, as specified in {{pdp-metadata-access-request}}. Normal HTTP caching behaviors apply, meaning that the GET may retrieve a cached copy of the content, rather than the latest copy. Implementations should utilize HTTP caching directives such as Cache-Control with max-age, as defined in {{RFC7234}}, to enable caching of retrieved metadata for appropriate time periods.

# IANA Considerations {#iana}

This specification requests IANA to take four actions: the creation of a new protocol registry group named 'AuthZEN', the establishment of two new registries within this group ('AuthZEN Policy Decision Point Metadata' and 'AuthZEN Policy Decision Point Capabilities'), the registration of a new Well-Known URI ('authzen-configuration'), and the registration of a new URN sub-namespace ('authzen').

The following registration procedure is used for the registries established by this specification.

Values are registered on a Specification Required {{RFC8126}} basis after a two-week review period on the openid-specs-authzen@lists.openid.net mailing list, following review and approval by one or more Designated Experts. However, to allow for the allocation of values prior to publication of the final version of a specification, the Designated Experts may approve registration once they are satisfied that the specification will be completed and published. However, if the specification is not completed and published in a timely manner, as determined by the Designated Experts, the Designated Experts may request that IANA withdraw the registration.

Registration requests sent to the mailing list for review should use an appropriate subject (e.g., "Request to register AuthZEN Policy Decision Point Metadata: example").

Within the review period, the Designated Experts will either approve or deny the registration request, communicating this decision to the review list and IANA. Denials should include an explanation and, if applicable, suggestions as to how to make the request successful. The IANA escalation process is followed when the Designated Experts are not responsive within 14 days.

Criteria that should be applied by the Designated Experts includes determining whether the proposed registration duplicates existing functionality, determining whether it is likely to be of general applicability or whether it is useful only for a single application, and whether the registration makes sense.

IANA must only accept registry updates from the Designated Experts and should direct all requests for registration to the review mailing list.

It is suggested that multiple Designated Experts be appointed who are able to represent the perspectives of different applications using this specification, in order to enable broadly-informed review of registration decisions. In cases where a registration decision could be perceived as creating a conflict of interest for a particular Expert, that Expert should defer to the judgment of the other Experts.

The reason for the use of the mailing list is to enable public review of registration requests, enabling both Designated Experts and other interested parties to provide feedback on proposed registrations. The reason to allow the Designated Experts to allocate values prior to publication as a final specification is to enable giving authors of specifications proposing registrations the benefit of review by the Designated Experts before the specification is completely done, so that if problems are identified, the authors can iterate and fix them before publication of the final specification.

## AuthZEN Policy Decision Point Metadata Registry {#iana-pdp-metadata-registry}

This specification asks IANA to establish the "AuthZEN Policy Decision Point Metadata" registry under the registry group "AuthZEN Parameters". The registry records the Policy Decision Point metadata parameter and a reference to the specification that defines it.

### Registry Definition

Registry Name: AuthZEN Policy Decision Point Metadata

Registration Policy: Specification Required per {{RFC8126}}

Reference: \[This Document\]

### Registration Template {#iana-pdp-metadata-template}

Metadata Name:
: The name requested (e.g., "resource"). This name is case-sensitive. Names may not match other registered names in a case-insensitive manner unless the Designated Experts state that there is a compelling reason to allow an exception.

Metadata Description:
: Brief description of the metadata (e.g., "Resource identifier URL").

Change Controller:
: For IETF stream RFCs, list the "IETF". For others, give the name of the responsible party. Other details (e.g., postal address, email address, home page URI) may also be included.

Specification Document(s):
: Reference to the document or documents that specify the parameter, preferably including URIs that can be used to retrieve copies of the documents. An indication of the relevant sections may also be included but is not required.

### Initial Registrations {#iana-pdp-metadata-initial}

Metadata Name:
: `policy_decision_point`

Metadata Description:
: Base URL of the Policy Decision Point

Change Controller:
: OpenID Foundation AuthZEN Working Group
: mailto:openid-specs-authzen@lists.openid.net

Specification Document(s):
: {{pdp-metadata-data-endpoint}} of \[This Document\]



Metadata Name:
: `access_evaluation_endpoint`

Metadata Description:
: URL of the Policy Decision Point's Access Evaluation API endpoint

Change Controller:
: OpenID Foundation AuthZEN Working Group
: mailto:openid-specs-authzen@lists.openid.net

Specification Document(s):
: {{pdp-metadata-data-endpoint}} of \[This Document\]



Metadata Name:
: `access_evaluations_endpoint`

Metadata Description:
: URL of the Policy Decision Point's Access Evaluations API endpoint

Change Controller:
: OpenID Foundation AuthZEN Working Group
: mailto:openid-specs-authzen@lists.openid.net

Specification Document(s):
: {{pdp-metadata-data-endpoint}} of \[This Document\]



Metadata Name:
: `search_subject_endpoint`

Metadata Description:
: URL of the Policy Decision Point's Search API endpoint for Subject entities

Change Controller:
: OpenID Foundation AuthZEN Working Group
: mailto:openid-specs-authzen@lists.openid.net

Specification Document(s):
: {{pdp-metadata-data-endpoint}} of \[This Document\]



Metadata Name:
: `search_resource_endpoint`

Metadata Description:
: URL of the Policy Decision Point's Search API endpoint for Resource entities

Change Controller:
: OpenID Foundation AuthZEN Working Group
: mailto:openid-specs-authzen@lists.openid.net

Specification Document(s):
: {{pdp-metadata-data-endpoint}} of \[This Document\]



Metadata Name:
: `search_action_endpoint`

Metadata Description:
: URL of the Policy Decision Point's Search API endpoint for Action entities

Change Controller:
: OpenID Foundation AuthZEN Working Group
: mailto:openid-specs-authzen@lists.openid.net

Specification Document(s):
: {{pdp-metadata-data-endpoint}} of \[This Document\]



Metadata Name:
: `capabilities`

Metadata Description:
: Array of URNs describing specific Policy Decision Point capabilities

Change Controller:
: OpenID Foundation AuthZEN Working Group
: mailto:openid-specs-authzen@lists.openid.net

Specification Document(s):
: {{pdp-metadata-data-capabilities}} of \[This Document\]


Metadata Name:
: `signed_metadata`

Metadata Description:
: JWT containing metadata parameters about the protected resource as claims.

Change Controller:
: OpenID Foundation AuthZEN Working Group
: mailto:openid-specs-authzen@lists.openid.net

Specification Document(s):
: {{pdp-metadata-data-sig}} of \[This Document\]


## Well-Known URI Registry {#iana-wk-registry}

This specification asks IANA to register the well-known URI defined in {{pdp-metadata-access}} in the IANA "Well-Known URIs" registry {{IANA.well-known-uris}}.

### Registry Contents {#iana-wk-contents}

URI Suffix:
: authzen-configuration

Reference:
: \[This Document\]

Status:
: permanent

Change Controller:
: OpenID Foundation AuthZEN Working Group
: mailto:openid-specs-authzen@lists.openid.net

Related Information:
: (none)

## AuthZEN Policy Decision Point Capabilities Registry {#iana-pdp-capabilities-registry}

This specification asks IANA to establish the "AuthZEN Policy Decision Point Capabilities" registry under the registry group "AuthZEN Parameters". The registry contains PDP-specific capabilities or features. These URNs are intended to be used in Policy Decision Point metadata discovery documents (as described in {{pdp-metadata}}) to allow a PEP to determine the supported functionality of a given PDP instance. The content of this registry will be specified by AuthZEN-compliant PDP vendors that want to declare interoperable capabilities.

### Registry Definition {#iana-pdp-capabilities-definition}

Registry Name: AuthZEN Policy Decision Point Capabilities

Registration Policy: Specification Required per {{RFC8126}}

Reference: \[This Document\]

### Registration Template {#iana-pdp-capabilities-template}

Capability Name:
: The name of the capability. This name MUST begin with the colon (":") character. This name is case-sensitive. Names may not match other registered names in a case-insensitive manner unless the Designated Experts state that there is a compelling reason to allow an exception.

Capability URN: The URN of the AuthZEN Policy Decision Point Capability.

Capability Description:
: Brief description of the capability.

Change Controller:
: OpenID Foundation AuthZEN Working Group
: mailto:openid-specs-authzen@lists.openid.net

Specification Document(s):
: Reference to the document or documents that specify the parameter, preferably including URIs that can be used to retrieve copies of the documents. An indication of the relevant sections may also be included but is not required.

## Registration of "authzen" URN Sub-namespace {#iana-urn-namespace}

This specification asks IANA to register a new URN sub-namespace within the "IETF URN Sub-namespace for Registered Protocol Parameter Identifiers" registry defined in {{RFC3553}}.

Registry Name: authzen

Specification: \[This Document\]

Repository: "AuthZEN Policy Decision Point Capabilities" registry ({{iana-pdp-capabilities-registry}} of \[This Document\])

Index value: Sub-parameters MUST be specified in UTF-8, using standard URI encoding where necessary.

--- back

# Terminology
Subject:
: The user or machine principal for whom an authorization decision is being requested.

Resource:
: The target of the request; the resource about which the authorization decision is being made.

Action:
: The operation the Subject is attempting on the Resource, in the context an authorization decision.

Context:
: When present in a request, the environmental or contextual attributes for this request. When present in a response, additional contextual information associated with a decision or search result.

Decision:
: The value of the evaluation decision made by the PDP: `true` for "allow", `false` for "deny".

PDP:
: Policy Decision Point. The component or system that provides authorization decisions over the network interface defined here as the Authorization API.

PEP:
: Policy Enforcement Point. The component or system that acts as a client to the PDP. The most common use case for a PEP is to request decisions and enforce access based on the decisions obtained from the PDP. It can also request decisions or search results for other purposes, such as determining which resources a subject may have access to.

# Acknowledgements {#Acknowledgements}

This template uses extracts from templates written by
{{{Pekka Savola}}}, {{{Elwyn Davies}}} and
{{{Henrik Levkowetz}}}.

# Notices {#Notices}
Copyright (c) 2026 The OpenID Foundation.

The OpenID Foundation (OIDF) grants to any Contributor, developer, implementer,
or other interested party a non-exclusive, royalty free, worldwide copyright license to
reproduce, prepare derivative works from, distribute, perform and display, this
Implementers Draft, Final Specification, or Final Specification Incorporating Errata
Corrections solely for the purposes of (i) developing specifications, and (ii)
implementing Implementers Drafts, Final Specifications, and Final Specification
Incorporating Errata Corrections based on such documents, provided that attribution
be made to the OIDF as the source of the material, but that such attribution does not
indicate an endorsement by the OIDF.

The technology described in this specification was made available from contributions
from various sources, including members of the OpenID Foundation and others.
Although the OpenID Foundation has taken steps to help ensure that the technology
is available for distribution, it takes no position regarding the validity or scope of any
intellectual property or other rights that might be claimed to pertain to the
implementation or use of the technology described in this specification or the extent
to which any license under such rights might or might not be available; neither does it
represent that it has made any independent effort to identify any such rights. The
OpenID Foundation and the contributors to this specification make no (and hereby
expressly disclaim any) warranties (express, implied, or otherwise), including implied
warranties of merchantability, non-infringement, fitness for a particular purpose, or
title, related to this specification, and the entire risk as to implementing this
specification is assumed by the implementer. The OpenID Intellectual Property
Rights policy (found at openid.net) requires contributors to offer a patent promise not
to assert certain patent claims against other contributors and against implementers.
OpenID invites any interested party to bring to its attention any copyrights, patents,
patent applications, or other proprietary rights that may cover technology that may be
required to practice this specification.