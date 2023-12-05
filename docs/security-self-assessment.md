# OpenFGA Security Self Assessment

## Table of Contents

* [Metadata](#metadata)
  * [Security links](#security-links)
* [Overview](#overview)
  * [Actors](#actors)
  * [Actions](#actions)
  * [Background](#background)
  * [Goals](#goals)
  * [Non-goals](#non-goals)
* [Self-assessment use](#self-assessment-use)
* [Security functions and features](#security-functions-and-features)
* [Project compliance](#project-compliance)
* [Secure development practices](#secure-development-practices)
* [Security issue resolution](#security-issue-resolution)
* [Appendix](#appendix)

## Metadata

|   |  |
| -- | -- |
| Software | https://github.com/openfga |
| Security Provider | Yes. OpenFGA is used to decide if a subject (user, application) user can perform a specific action on a resource or not.|
| Languages | Go, Java, Javascript, Python, C# |
| SBOM | It's generated as part of every release, for example in [v.1.3.4](https://github.com/openfga/openfga/releases/download/v1.3.4/openfga_1.3.4_linux_386.tar.gz.sbom)|   |

### Security Links

  - https://github.com/openfga/openfga/security/policy
  - https://github.com/orgs/openfga/security/risk

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

See [Actors](#actors) and [Actions](#actions) for more detailed description of the critical actors, actions, and potential threats.

### Security Relevant

Applications track and point to specific versions of authorization models.

## Project Compliance

When utilizing OpenFGA, it's required to store relationship tuples like `{user: user: alice, relation: can_view, object: document:readme }`. We strongly advise users against storing Personal Identifiable Information (PII) such as email addresses in any of the relationship tuples. 

This precaution is recommended to ensure compliance with GDPR and other privacy regulations.

By refraining from including PII in relationship tuples, users can simplify their compliance efforts and mitigate potential privacy risks. This practice aligns with data protection principles and safeguards user privacy, contributing to a more secure and regulatory-compliant implementation of OpenFGA.

## Secure Development Practices

The OpenFGA project include the test cases as per the CNCF standard. It passes the [OpenSSF](https://bestpractices.coreinfrastructure.org/projects/6374) Best Practices. SonarScan tells that the OpenFGA's code coverage is around 83% with A+ Go rating.

The OpenFGA project follows established CNCF and OSS best practices for code development and delivery. OpenFGA [passes OpenSSF Best Practices](https://bestpractices.coreinfrastructure.org/projects/6374), has an [OpenSSF scorecard of](https://api.securityscorecards.dev/projects/github.com/openfga/openfga) 9.1 and a [CLO Monitor score of 100%](https://clomonitor.io/projects/openfga).

CodeCov [reports a code coverage of 82.10%](https://app.codecov.io/gh/openfga/openfga, and it has a [A+ Go rating](https://goreportcard.com/report/github.com/openfga/openfga).

### Ecosystem

OpenFGA uses [Chainguard images](https://www.chainguard.dev/chainguard-images), it supports [OpenTelemetry](https://github.com/open-telemetry), and can be monitored with tools like [Grafana](https://grafana.com/), [Prometheus](https://prometheus.io/) and [Jaeger](https://www.jaegertracing.io/). 

To monitor the logs and enabling the tracing mechanisms, OpenFGA can be integrated with [Jaeger](https://www.jaegertracing.io/). Jaeger adds the tracing headers to the logs, making easy to track the request flow. 

It supports [OpenTelemetry](https://github.com/open-telemetry), and can be monitored with tools like [Grafana](https://grafana.com/), [Prometheus](https://prometheus.io/), and [Dynatrace](https://www.dynatrace.com/) which it delivers analytics and automation for unified observability and security.

It provides [Helm Charts](https://github.com/openfga/helm-charts) that are available in [Artifact Hub](https://artifacthub.io/packages/helm/openfga/openfga).

### Development Pipeline

All code is maintained on [Github](https://github.com/openfga). Changes must be reviewed and merged by the project maintainers. Before changes are merged, all the changes must pass static checks, license checks, [multiple linters](https://github.com/openfga/openfga/blob/main/.golangci.yaml) including `gofmt` and `govet`, and pass all unit tests and e2e tests. 

Changes are scanned by Snyk, FOSSA, semgrep and CodeQL. Code changes are submitted via Pull Requests and contributors need to sign a CLA through [EasyCLA](https://easycla.lfx.linuxfoundation.org). Commits to the main branch directly are not allowed.

OpenFGA published container images are based on Chainguard's and are scanned using Snyk container scanning.

### Communication Channels

#### Internal

Team and users members communicate with each other through the [OpenFGA Discord](https://discord.gg/8naAwJfWN6), a Okta internal Slack channel, and discuss in Github [discussions](https://github.com/orgs/openfga/discussions), Github [issues](https://github.com/openfga/openfga/issues) or [pull requests](https://github.com/openfga/openfga/pulls).

#### Security Email Group

Any kind of security related issues, vulnerabilities can be reported to OpenFGA team at security@openfga.dev. This email is given in [OpenFGA repository](https://github.com/openfga/.github/blob/main/SECURITY.md)

## Security Issue Resolution
### Responsible Disclosure Process

OpenFGA project vulnerability handling related processes are recorded in the [OpenFGA Security Doc](https://github.com/openfga/.github/blob/main/SECURITY.md). Related security vulnerabilities can be reported and communicated via email to security@openfga.dev.

The OpenFGA maintainers are responsible for responding within 5 working days. It is the maintainers’ duties to triage the severity of the issue and determine how to address the issue.

### Incident Response

See [OpenFGA Security Doc](https://github.com/openfga/.github/blob/main/SECURITY.md) for a description for how incidents should be communicated. 

The OpenFGA maintainers bear the responsibility of monitoring and addressing reported vulnerabilities. Identified issues undergo prioritized triage, with immediate escalation upon confirmation. The triage process is conducted in private channels.

Adhering to the GitHub security advisory process, OpenFGA initiates the CVE (Common Vulnerabilities and Exposures) request upon issue identification. The resolution is developed in a private branch associated with the CVE.

Upon confirmation of the fix's effectiveness, it is released through a new patch for each major supported version of OpenFGA.

The changelog will link to the CVE, which will describe the vulnerability and its mitigation. Any public announcements sent for these fixes will be linked to [the release notes](https://github.com/openfga/openfga/releases/tag/v1.3.2).

## Appendix

### Known Issues Over Time

All OpenFGA security issues can be found on the [Github advisories page](https://github.com/openfga/openfga/security/advisories).

OpenFGA occasionally responded incorrectly to authorization queries, which is a security vulnerability. This is usually due to problems in the way relationships are defined in the relationship tuples. Known issues have been fixed.

There have also been issues in responsiveness when a certain number of ListObjects are executed. This has also been fixed with updates.

### Case Studies. 

The list of projects that utilize OpenFGA include Okta FGA, Twintag, Mapped, Procure Ai,Canonical (Juju & LFX), Wolt, Italarchivi, Read AI, Virtool, Configu, Fianu Labs, and ExcID.

An up to date list of companies that publicly acknowledged using OpenFGA can be found [here](https://github.com/openfga/community/blob/main/ADOPTERS.md).

### Related Projects/Vendors

[OPA (Open Policy Agent)](https://github.com/open-policy-agent):
 - Part of CNCF projects for externalizing authorization.
 - Uses Rego as its policy language.
 - Differs from OpenFGA by storing required data as relationship tuples.
 - Requires data provision during policy invocation or querying during evaluation.

 [Kyverno](https://github.com/kyverno):

 - CNCF project focusing on implementing security policies for Kubernetes deployments.
