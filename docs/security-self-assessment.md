# OpenFGA Security Self Assessment

## Table of Contents


- [OpenFGA Security Self Assessment](#openfga-security-self-assessment)
  - [Table of Contents](#table-of-contents)
  - [Metadata](#metadata)
    - [Security Links](#security-links)
  - [Overview](#overview)
    - [Background](#background)
    - [Actors](#actors)
    - [Actions](#actions)
    - [Goals](#goals)
    - [Non-Goals](#non-goals)
  - [Self-Assessment Use](#self-assessment-use)
  - [Security Functions and Features](#security-functions-and-features)
    - [Security Relevant](#security-relevant)
  - [Project Compliance](#project-compliance)
  - [Secure Development Practices](#secure-development-practices)
    - [Ecosystem](#ecosystem)
    - [Development Pipeline](#development-pipeline)
    - [Communication Channels](#communication-channels)
      - [Internal](#internal)
      - [Security Email Group](#security-email-group)
  - [Security Issue Resolution](#security-issue-resolution)
    - [Responsible Disclosure Process](#responsible-disclosure-process)
    - [Incident Response](#incident-response)
  - [Appendix](#appendix)
    - [Known Issues Over Time](#known-issues-over-time)
    - [Case Studies.](#case-studies)
    - [Related Projects/Vendors](#related-projectsvendors)

## Metadata

|   |  |
| -- | -- |
| Assessment Stage | Incomplete |
| Software | https://github.com/openfga |
| Security Provider | Yes. OpenFGA is used to decide if a subject (user, application) user can perform a specific action on a resource or not.|
| Languages | Go, Java, Javascript, Python, C# |
| SBOM | The Software Bill of Materials is not publicly available, but is included in each GitHub release using Syft, which is a CLI tool, and Go library for generating an SBOM from container images and filesystems. https://github.com/openfga/openfga/pull/683 |

### Security Links

| Doc | url |
| -- | -- |
| Security Policy | [OpenFGA Security Policy](https://github.com/openfga/openfga/security/policy) |
| Security Insights | [OpenFGA Security Insights](https://github.com/openfga/openfga/blob/main/SECURITY-INSIGHTS.yml) |
| Security risks | [OpenFGA Security risks](https://github.com/orgs/openfga/security/risk) |
| -- | -- |

## Overview

Implementing access control is a very common requirement when developing applications, where different subjects can perform different actions on different resources. 

OpenFGA is a high performance and flexible authorization/permission engine that can be used to implement fine grained access control in any application component. 

Developers can use OpenFGA to easily craft authorization and permission methods based on the policies they require that are specific to their own projects.

### Background

OpenFGA is an authorization/permission engine that incorporates Relationship-Based Access Control (ReBAC) and Attribute Based Access Control (ABAC) concepts with a domain-specific language that enables crafting authorizations solutions that can grow and evolve to any use case.

It's inspired on the idea described in the [Google Zanzibar paper](https://research.google/pubs/pub48190/).

Fine-Grained Authorization refers to individual users having access to specific objects and resources within a system. Google Drive is an example of this, as owners of resources can grant different users to have different levels of access to their resources.

OpenFGA makes helps developers make authorization decisions by combining two concepts:

- An Authorization Model, where developers define their authorization policies

- A set of relationship tuples that instantiate the model and OpenFGA uses to answer access control queries.

An authorization model looks like:

```python
model
  schema 1.1

type user
type group
  relations 
    define member: [user]
type folder
  relations
    define owner: [user]
    define parent: [folder]
    define viewer: [user, group#member] or owner or viewer from parent

type document
  relations 
    define parent: [folder]
    define owner: [user]
    define viewer: [user, group#member] or owner or viewer from parent
```

Relationship tuples look like:

| Subject | Relation | Object |
| --- | --- | --- |
| user:alice | member | group:engineering |
| folder:root | parent | document:readme |
| group#engineering:member | viewer | folder:root |

With this information, OpenFGA can be queried in different ways:

- Using the [/check](https://openfga.dev/api/service#/Relationship%20Queries/Check) endpoint to ask questions like "Is `user:alice` a `viewer` for `document:readme`?". With the data provided above, OpenFGA will return `{allowed : "true"}`, as Alice is a member of the engineering team, which has viewer access on the 'readme' document's parent folder.

- Using the [/list-objects](https://openfga.dev/api/service#/Relationship%20Queries/ListObjects) endpoint to ask questions like "What are all the documents for which `user:alice` is a `viewer`. With the data provided above, OpenFGA will return `{object_ids { "document:readme" }`

### Actors

The actors within the system are the OpenFGA server, Database server, and the CLI/API clients. 

**OpenFGA Server**

The OpenFGA server is responsible for:

- Storing and retrieving relationship tuples
- Storing and retrieving and authorization models.
- Evaluating the inferred permissions for a given subject and object.

**Database Server**

Stores the relationship tuples and authorization models, as well as a changelog. Currently support Postgres and MySQL.

**CLI/API Clients**

Make API requests to the OpenFGA server, i.e. creating/querying relationship tuples, updating authorization models, checking for access, or listing objects a user has access to. 

Clients can use either no authentication, shared key, or OAuth client credentials as a method for authentication.

### Actions

**Invoking the OpenFGA APIs**

Every time a server endpoint is invoked, OpenFGA validates that:

  - The credentials provided in the API call match the ones configured in the server.

  - Ensures that the input is a semantically valid, e.g. that a tuple is valid according to the authorization model or that the model does not have disallowed cyclical or problematic definitions

  - Payload Verification: 
  
    - Confirm that API payloads adhere to Protobuf API definitions.  
    - Validate parameters for proper structure, e.g. ensuring users are written in the correct format which is 'user:<userid>'
  
**Writing an Authorization Model**

  - Semantic Model Verification: OpenFGA validates that Authorization Models are semantically valid, avoiding cyclical or problematic definitions and other disallowed criteria.

  - Version and Configuration: When writing a model, a new version is created (models are immutable). Applications must be configured to use the new version after validation and confirmation of expected behavior.

**Calling the Authorization Query endpoints**

When the [/check](https://openfga.dev/api/service#/Relationship%20Queries/Check) and [/list-objects](https://openfga.dev/api/service#/Relationship%20Queries/ListObjects) endpoints are called, OpenFGA limits the number of simultaneous paths explored and enforces depth limitations on the graph traversal. 

To protect against DoS attacks, OpenFGA restricts both the number of simultaneous paths explored and the depth of paths traversed in the graph.

**Upgrading OpenFGA Database Schema**

Database Migration Planning: When installing new versions of OpenFGA, database migrations can be executed in a way that minimizes downtime and ensure a smooth transition in the target system.

### Goals

* Simplify and standardize authorization processes, making them more consistent across various applications and systems.

* Establish patterns and standards for externalized authorization.

* Create architectural patterns, terminologies, and protocols that enable interoperability among different authorization systems.

* Deliver an authorization service for any application component.

* Enable centralized authorization decisions and permits diverse teams to implement authorization using a shared framework across various application components.

### Non-Goals

* Tools for management of groups/roles/permissions not inherently provided to the end-users.
* Does not intend to serve as a comprehensive data repository for non-authorization related data.
* Does not aim to provide a complete authentication and Access Control Solution.

## Self-Assessment Use

This self-assessment is created by the OpenFGA team to perform an internal analysis of the project's security. It is not intended to provide a security audit of OpenFGA, or function as an independent assessment or attestation of OpenFGA's security health.

This document serves to provide OpenFGA users with an initial understanding of OpenFGA's security, where to find existing security documentation, OpenFGA plans for security, and general overview of OpenFGA security practices, both for development of OpenFGA as well as security of OpenFGA.

This document provides the CNCF TAG-Security with an initial understanding of OpenFGA to assist in a joint-assessment, necessary for projects under incubation. Taken together, this document and the joint-assessment serve as a cornerstone for if and when OpenFGA seeks graduation and is preparing for a security audit.

## Security Functions and Features

OpenFGA, being an Open-Source project, allows for a more robust security implementation by following the Principle of Open Design.

OpenFGA models authorization systems by providing the security features such as Role-based Access Control and Atrribute-based Access Control.

OpenFGA boasts exceptional speed in processing secure authorization check call. This swift authorization mechanism not only enhances efficiency but also reinforces the security posture, assuring robust protection for applications and platforms for diverse scales.

OpenFGA provides a wide variety of SDK's, as well as easy integration for new SDK's. This reduces the chance of critical vulnerabilities due to compatibility issues.

### Security Relevant

**Basic Threat Landscape**
The Basic Threat Landscape presents a general overview of technologies and actors specific to the security of integrating openFGA in a broader system. The list of introductory threats stands to orient future comprehensive threat models.  
```
non-goals:
  - Manipulate groups/roles/permissions 
  - Store non-authorization data and PII data
  - Provide complete authentication/authorization solution  

goals:  
  - Establish consistent authorization standards and processes 
  - Be a centralized authorization engine for systems sharing common software components  
  - Enable interoperability among different authorization systems

technologies:
  openfga.server:
    - language.go
    - container.docker
    - image.chainguard
    - protocols.grpc
    - protocols.http
    - openapi
    - authentication.oidc
    - authentication.psk
    - observability.opentelemetry
    - observability.prometheus
    
  openfga.datastore:
    - in-memory
    - mysql
    - postgres
  
  openfga.sdks:
    - openapi
    - language.java
    - language.go
    - language.python
    - language.donet
    - language.js

  openfga.deployment:
    - helms.chart

actors:
  openfga:
    - server
    - datastore
    - clients
    - configuration.language

  system:
    - users
    - applications
    - resources
    - developers
    - operators
    - external.idp

actions: 
  system.users:
    - Request access to [system.resources] through [openfga.clients|system.applications]  

  system.developers:
    - Integrate [openfga.sdks] in [openfga.clients|system.applications]
    - Validate and verify semantically [opengfa.authz_models] 

  system.operators:
    - Migrate [openfga.datastore]  
    - Deploy [openfga.server]

  system.external.idp:
    - Provide [jwks_uri] through oidc /.well-known/openid-configuration 
    - Sign [token] with [rs256] algorithm 

  configuration.language:
    - Provide a domain specific language to describe authorization policies
    - Describe the authorization model with [entities], [types] and [relations] against a [schema] 
    
  openfga.datastore:
    - Store authorization models [opengfa.authz_models] 
    - Store authorization data [opengfa.relationships.tupples]
    - Support for [MySQL, Postgres] database

  openfga.clients|system.applications:
    - Authenticate against [openfga.server] with [openfga.psk] secret or through [external.idp]    
    - Execute authorization checks with [openfga.relationships.queries]
    - Manage the authorization model [opengfa.authz_models] 

  openfga.server:
    - Write authorization model [opengfa.authz_models] to [openfga.datastore]
    - Write authorization data [opengfa.relationships.tupples] to [openfga.datastore]
    - Provide [grpc|http] messaging protocol 
    - Authenticate trusted [opengfa.clients] with 3 options [none|psk|oidc] 
    - Validate and verify [payload]   
    - Evaluate access control decisions [opengfa.relationships.queries]
  
  openfga.server.api:
    stores:
      - list
      - create
      - get
      - delete 
      - assertions.read
      - assertions.upsert
    authz-models:
      - list
      - create
      - get
    relationships.tupples:
      - read
      - write
      - list.changes
    relationships.queries:
      - check
      - expand
      - list-objects
      - streamed-list-objects

sdlcAssessment:
  - technologies:
      sca: snyk
      sast: semgrep, codeql
      dast: n/a
      

threats: 
    summary: | 
      authenticated clients can both execute authorization checks (read) and update the authorization model (write)
      clients point to a specific version of the authz-model 
    weakness: improper authorization
    attack: elevation of privilege
    component: openfga.server 
    actors: compromised:openfga.client
    control: authorization:least_privilege, authorization:scopes
    impact: high
    likelihood: high

    summary: | 
      external.idp issues an id_token to a malicious party    
    weakness: improper authentication
    attack: spoofing 
    component: openfga.server
    actors: malicious:system.external.idp
    control: authentication:trusted_issuer
    impact: high
    likelihood: low

    summary: | 
      the preshared key used for openfga.server/clients authentication is leaked/stolen     
    weakness: improper secrets handling/storing
    attack: information disclosure 
    component: openfga.server, openfga.clients
    actors: compromised:openfga.clients, malicious:system.operators
    control: secret:safe_mannipulation
    impact: high
    likelihood: low

    summary: |
      openfga.server availability is disrupted through extensive network calls      
    weakness: uncontrolled resource consumption
    attack: denial of service
    component: openfga.server
    actors: malicious:openfga.clients, misconfigured:system.applications
    control: request:limiting, request:throttling
    impact: medium
    likelihood: medium

    summary: |
      openfga.server availability is disrupted through authorization checks with extensive graph traversal queries 
    weakness: improper restriction of input
    attack: denial of service
    component: openfga.server
    actors: malicious:openfga.clients, misconfigured:system.applications
    control: input:sanization, input:normalization,  
    impact: medium
    likelihood: low

    summary: |
      opengfa.authz_model is flawed or too permissive 
    weakness: business logic
    attack: elevation of privilege
    component: system.applications
    actors: malicious:system.users
    control: code:review, code:testing
    impact: high
    likelihood: low

    summary: |
      opengfa.authz_model is updated, but openfga.clients are not updated to match versions
    weakness: improper authorization
    attack: elevation of privilege
    component: openfga.server
    actors: compromised:openfga.clients
    control: software:update
    impact: high
    likelihood: low

    summary: |
      system.applications execute a check without specifying the opengfa.authz_model version
    weakness: server configuration
    attack: elevation of privilege
    component: system.applications
    actors: malicious:system.users
    control: code:review, code:testing
    impact: medium
    likelihood: high

    summary: |
      opengfa.datastore exhibit eventual consistency leading to inconsistent authorization checks amongst openfga.clients   
    weakness: improper authorization
    attack: elevation of privilege
    component: system.applications
    actors: malicious:system.users
    control: software:versioning, software:timestamping
    impact: low
    likelihood: low

    summary: |
      opengfa.configuration.language vulnerabilities leads to authorization bypass 
    weakness: business logic
    attack: authorization bypass
    component: system.applications
    actors: malicious:system.users
    control: software:update
    impact: high
    likelihood: low

```

## Project Compliance

We strongly advise against storing Personal Identifiable Information (PII) such as email addresses in any of the relationship tuples to ensure compliance with GDPR and other privacy regulations.

- ❌ `{user: alice@email.com, relation: can_view, object: document:readme }`
- ✅ `{user: abcd1234, relation: can_view, object: document:readme }`

By refraining from including PII in relationship tuples, users can simplify their compliance efforts and mitigate potential privacy risks. This practice aligns with data protection principles and safeguards user privacy, contributing to a more secure and regulatory-compliant implementation of OpenFGA.

## Secure Development Practices

### Development Pipeline

| Stage | Status |
| - | - |
| Build |  [![main](https://github.com/openfga/openfga/actions/workflows/main.yaml/badge.svg)](https://github.com/openfga/openfga/actions/workflows/main.yaml) [![pr](https://github.com/openfga/openfga/actions/workflows/pull_request.yaml/badge.svg)](https://github.com/openfga/openfga/actions/workflows/pull_request.yaml)  [![codecov](https://codecov.io/gh/openfga/openfga/branch/main/graph/badge.svg)](https://codecov.io/gh/openfga/openfga) |
| Release| [![release.yaml](https://github.com/openfga/openfga/actions/workflows/release.yaml/badge.svg)](https://github.com/openfga/openfga/actions/workflows/release.yaml) |
| Scanning | [![CodeQL](https://github.com/openfga/openfga/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/openfga/openfga/actions/workflows/github-code-scanning/codeql) [![Semgrep](https://github.com/openfga/openfga/actions/workflows/semgrep.yaml/badge.svg)](https://github.com/openfga/openfga/actions/workflows/semgrep.yaml) [![Snyk](https://snyk.io/test/github/openfga/openfga/main/badge.svg)](https://snyk.io/test/github/openfga/openfga)  |
| License| [![FOSSA](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fopenfga%2Fopenfga.svg?type=shield&issueType=license)](https://app.fossa.com/projects/git%2Bgithub.com%2Fopenfga%2Fopenfga?ref=badge_shield&issueType=license) [![FOSSA](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fopenfga%2Fopenfga.svg?type=shield&issueType=security)](https://app.fossa.com/projects/git%2Bgithub.com%2Fopenfga%2Fopenfga?ref=badge_shield&issueType=security)|
| OpenSSF | [![OpenSSF Best Practices](https://www.bestpractices.dev/projects/6374/badge)](https://www.bestpractices.dev/projects/6374) [![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/openfga/openfga/badge)](https://securityscorecards.dev/viewer/?uri=github.com/openfga/openfga) |
| CLOMonitor | [![openfga](https://img.shields.io/endpoint?url=https://clomonitor.io/api/projects/cncf/openfga/badge)](https://clomonitor.io/projects/cncf/openfga) |
| | |

### Communication Channels
#### Internal
[![github](https://img.shields.io/badge/github-discussions-black.svg?logo=github)](https://github.com/orgs/openfga/discussions)
[![github](https://img.shields.io/badge/github-issues-black.svg?logo=github)](https://github.com/orgs/openfga/discussions)
[![github](https://img.shields.io/badge/github-pulls-black.svg?logo=github)](https://github.com/orgs/openfga/discussions)
[![slack](https://img.shields.io/badge/slack-okta_%23external%82okta%82openfga-black.svg?logo=slack)](https://cloud-native.slack.com/archives/C06G1NNH47N)

#### Inbound & Outbound
[![email](https://img.shields.io/badge/email-security@openfga.dev-openfga?color=25c2a0&logo=mail.ru)](mailto:security@openfga.dev)
[![community](https://img.shields.io/badge/openfga-community-25c2a0.svg?logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAAAAABWESUoAAABY0lEQVR4AYWSIYyDQBBFn1eVNWdR9QJ1Co+qD6o+J+pFVdX51NfVB4tCYiowaASCJrg5dj6Xbgi5++a18BhmZ8D+CbakrRx17ajatXCD3Ywj5DP2cFsJvNPaI2BDyM5FQG/lhlBDMeOm2ieoI0EZBWVanaLpVGZ0Uw93zVtIvfCAF66B3l+W/gpLT5eAJAG4qOtyESoJ14DTCeAqoVoEK1ye9tCadfAxedni3eRg6kvoBV3EhqduthalmYTBKOFgZmdVVF5AMzOBkg/gYT2ASpna/TS7A3sSFwZtack3kJnvLKGeZV37MkXneWqAFTZ16rm3KN2CydCPl6O1CKNLEvLQhk07L/yE3egN5PEuyHJC8oDVqCWkGe5lq2VF6x41ngYYfQhZ9MH00YCnRsdqV5+cnNeoP67GQgVHzeuqY5UrQT31AXmRBmwID+sIObAh3LX1Qls/wH0lWK/emwVd3OSf+QEqxdwXzzaUTwAAAABJRU5ErkJggg==)](https://openfga.dev/docs/community )
[![slack](https://img.shields.io/badge/slack-cncf_%23openfga-25c2a0.svg?logo=slack)](https://cloud-native.slack.com/archives/C06G1NNH47N)

 ### Ecosystem

#### Artifacts

[![chainguard](https://img.shields.io/badge/Chainguard-images-openfga?color=25c2a0&logo=chainguard)](https://images.chainguard.dev/directory/image/go/versions)
[![helmchart](https://img.shields.io/badge/Helm_-charts-openfga?color=25c2a0&logo=helm)](https://github.com/openfga/helm-charts) 
[![artifact hub](https://img.shields.io/badge/Artifact_-hub-openfga?color=25c2a0&logo=artifacthub)](https://artifacthub.io/packages/helm/openfga/openfga) 

#### Observability
OpenFGA can be integrated with and monitored through the following technologies:

[![opentelemetry](https://img.shields.io/badge/Opentelemetry--openfga?color=25c2a0&logo=opentelemetry)](https://github.com/open-telemetry)
[![grafana](https://img.shields.io/badge/Grafana--openfga?color=25c2a0&logo=grafana)](https://grafana.com/)
[![prometheus](https://img.shields.io/badge/Prometheus--openfga?color=25c2a0&logo=prometheus)](https://prometheus.io/)
[![jaeger](https://img.shields.io/badge/Jaeger--openfga?color=25c2a0&logo=jaeger)](https://jaegertracing.io/)
[![dynatrace](https://img.shields.io/badge/Dynatrace--openfga?color=25c2a0&logo=Dynatrace)](https://dynatrace.io/)


## Security Issue Resolution
### Responsible Disclosure

OpenFGA vulnerability management is described in the official project security documentation [SECURITY.md](https://github.com/openfga/.github/blob/main/SECURITY.md). 

### Incident Response
The OpenFGA maintainers bear the responsibility of monitoring and addressing reported vulnerabilities. Identified issues undergo prioritized triage, with immediate escalation upon confirmation. The triage process is conducted in private channels.

Adhering to the GitHub security advisory process, OpenFGA initiates the CVE (Common Vulnerabilities and Exposures) request upon issue identification. The resolution is developed in a private branch associated with the CVE.

Upon confirmation of the fix's effectiveness, it is released through a new patch for each major supported version of OpenFGA.

The changelog will link to the CVE, which will describe the vulnerability and its mitigation. Any public announcements sent for these fixes will be linked to [the release notes](https://github.com/openfga/openfga/releases/tag/v1.3.2).

All OpenFGA security issues can be found on the [Github advisories page](https://github.com/openfga/openfga/security/advisories).

## Appendix

### Case Studies. 

The [list](https://github.com/openfga/community/blob/main/ADOPTERS.md) of projects that utilize OpenFGA include Okta FGA, Twintag, Mapped, Procure Ai,Canonical (Juju & LFX), Wolt, Italarchivi, Read AI, Virtool, Configu, Fianu Labs, and ExcID.

### Related Projects/Vendors

[OPA (Open Policy Agent)](https://github.com/open-policy-agent):
 - Part of CNCF projects for externalizing authorization.
 - Uses Rego as its policy language.
 - Differs from OpenFGA by storing required data as relationship tuples.
 - Requires data provision during policy invocation or querying during evaluation.

 [Kyverno](https://github.com/kyverno):

 - CNCF project focusing on implementing security policies for Kubernetes deployments.
