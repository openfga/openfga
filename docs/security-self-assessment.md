# OpenFGA Security Self Assessment

## Table of Contents
s
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

OpenFGA is a high performance and flexible authorization/permission engine. It incorporates Relationship-Based Access Control (ReBAC) and Attribute Based Access Control (ABAC) concepts with a domain-specific language that makes it easy to craft authorization and permission solutions that can grow and evolve to any use case, at any scale. 

It's inspired on the idea described in the [Google Zanzibar paper](https://research.google/pubs/pub48190/).

### Background

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

OpenFGA was accepted as a CNCF sandbox project in June 2022. Since our inclusion in the CNCF, it has been promising community contribution, growth, and adoption. Companies such as [Canonical](https://canonical.com) or [Okta](https://fga.dev) are building products and integrations on top of the OpenFGA, while others, like [Wolt](https://wolt.com) and [Read.ai](https://www.read.ai/) are using OpenFGA to implement authorization for their products.

OpenFGA has an active community, with 500+ Discord members, 40 unique contributors, and an average of [850 commits per month](https://openfga.devstats.cncf.io/d/2/commits-repository-groups?orgId=1&var-period=m&var-repogroups=All&from=now-1y%2Fy&to=now-1y%2Fy). 

### Actors

**OpenFGA Server**

The OpenFGA server is responsible for storing and querying relationship tuples and authorization models.

Given that it's going to be used to know if a user can perform an action on a resource, any potential bug in the OpenFGA server logic can have security implications. 

The server can be configured without authentication, with a shared key, or using the OAuth client credentials flow.

**Database Server**

The database server stores relationship tuples, authorization models and a changelog. OpenFGA currently supports Postgres and MySQL, and it can be extended to support other databases.

Database credentials can be provided as a parameter to the OpenFGA server, via environment variables or a configuration file.

**CLI/API Clients**

The CLI/API clients are used to make API requests to the OpenFGA server. This includes functionality such as creating and querying relationship tuples, updating the authorization model and running authorization queries such as checking for access or listing objects a user has access to.

They authenticate using any of the mechanisms supported by the server (no authentication, shared key, OAuth client credentials).

If the credentials were compromised, the attacker would be able to perform any action granted to those credentials. This would include: 

* Changing the authorization model.
* Adding tuples to give the attacker access to a resource.
* Removing tuples to prevent a user from accessing a resource.
* Querying authorization data, giving the attacker access to privileged information.

To achieve this the attacker would need to be able to connect directly to the OpenFGA service, which is not recommended to be accessible from outside the services’ internal network.

OpenFGA provides versioned authorization models, and applications should point to a specific version. This implies that even if the attacker updates the authorization model, it should not have any effect unless they can also update the application configuration to make it point to the new version.

### Actions

**Invoking the OpenFGA APIs**

Every time a server endpoint is invoked, OpenFGA validates that:

  - The credentials provided in the API call match the ones configured in the server.

  - Ensures that the input is a semantically valid, e.g. that a tuple is valid according to the authorization model or that the model does not have disallowed cyclical or problematic definitions

  - The payload of the API call:
  
    - Matches the Protobuf API definitions.
    - Validates the parameters have the proper structure, e.g. users need to be written this way 'user:<userid>'
  
**Writing an Authorization Model**

OpenFGA validates that the Authorization Models are semantically valid from the server standpoint, as in they do not have cyclical or problematic definitions, or other disallowed criteria.

When a model is written a new version is created (Authorization Models in OpenFGA are immutable). The application needs to be configured to use the new version when needed, and when after it has been validated and confirmed to be working as expected.

**Calling the Authorization Query endpoints**

When the [/check](https://openfga.dev/api/service#/Relationship%20Queries/Check) and [/list-objects](https://openfga.dev/api/service#/Relationship%20Queries/ListObjects) endpoints are called, OpenFGA needs to traverse the graph defined by the model and instated by the tuples. The graph can be very wide and deep. To protect the service from a DoSS, OpenFGA needs to limit both the number of simultaneous paths it explores, as well as the depth of the paths it traverses.

**Upgrading OpenFGA Database Schema**

When new version of OpenFGA are installed, it might imply running a database migration in the target system. To avoid downtime, migrations need to be carefully written and planned.

### Goals

* Provide developers a flexible way to implement authorization in their applications.

* Provide a high performance and scalable authorization service that can be used in any application component.

* Allow centralized authorization decisions, decoupling policy enforcement from decision making. Allows different teams to implement authorization using a common framework that can be used in any application component.

* Not to be coupled to any authentication technology. It only requires a way to identify the subject of the authorization (user, application, device, etc).

### Non-Goals

* Does not provide tools for end-users to manage groups, roles, permissions.
* Does not aim to be a general purpose data store for non-authorization related data

## Self-Assessment Use

This self-assessment is created by the OpenFGA team to perform an internal analysis of the project's security. It is not intended to provide a security audit of OpenFGA, or function as an independent assessment or attestation of OpenFGA's security health.

This document serves to provide OpenFGA users with an initial understanding of OpenFGA's security, where to find existing security documentation, OpenFGA plans for security, and general overview of OpenFGA security practices, both for development of OpenFGA as well as security of OpenFGA.

This document provides OpenFGA maintainers and stakeholders with additional context to help inform the roadmap creation process, so that security and feature improvements can be prioritized accordingly.

## Security Functions and Features

See [Actors](#actors) and [Actions](#actions) for more detailed description of the critical actors, actions, and potential threats.

### Critical

TBD

### Security Relevant

TBD

## Project Compliance

When using OpenFGA you need to store relationship tuples like `{user: user: alice, relation: can_view, object: document:readme }`. We recommend users to not to store  Personal Identifiable Information like email addresses in any of the relationship tuples. This will make it simpler to comply with GDPR and other privacy regulations.

## Secure Development Practices

The OpenFGA project follows established CNCF and OSS best practices for code development and delivery. OpenFGA [passes OpenSSF Best Practices](https://bestpractices.coreinfrastructure.org/projects/6374), has an [OpenSSF scorecard of](https://api.securityscorecards.dev/projects/github.com/openfga/openfga) 9 and a [CLO Monitor score of 99%](https://clomonitor.io/projects/openfga).

OpenFGA's code coverage is 83%, and it has a [A+ Go rating](https://goreportcard.com/report/github.com/openfga/openfga).

### Ecosystem

OpenFGA uses [Chainguard images](https://www.chainguard.dev/chainguard-images), it supports [OpenTelemetry](https://github.com/open-telemetry), and can be monitored with tools like [Grafana](https://grafana.com/), [Prometheus](https://prometheus.io/) and [Jaeger](https://www.jaegertracing.io/). 

It provides [Helm Charts](https://github.com/openfga/helm-charts) that are available in [Artifact Hub](https://artifacthub.io/packages/helm/openfga/openfga).

### Development Pipeline

All code is maintained on [Github](https://github.com/openfga). Changes must be reviewed and merged by the project maintainers. Before changes are merged, all the changes must pass static checks, license checks, [multiple linters](https://github.com/openfga/openfga/blob/main/.golangci.yaml) including `gofmt` and `govet`, and pass all unit tests and e2e tests. 

Changes are scanned by Snyk, FOSSA, semgrep and CodeQL. Code changes are submitted via Pull Requests and contributors need to sign a CLA through [EasyCLA](https://easycla.lfx.linuxfoundation.org). Commits to the main branch directly are not allowed.

OpenFGA published container images are based on Chainguard's and are scanned using Snyk container scanning.

### Communication Channels

#### Internal

Team and users members communicate with each other through the [OpenFGA Discord](https://discord.gg/8naAwJfWN6), a Okta internal Slack channel, and discuss in Github [discussions](https://github.com/orgs/openfga/discussions), Github [issues](https://github.com/openfga/openfga/issues) or [pull requests](https://github.com/openfga/openfga/pulls).

#### Security Email Group

To report a security problem in OpenFGA, users should contact the Maintainers Team at security@openfga.dev. The security email group is also listed in the security document in [our repository](https://github.com/openfga/.github/blob/main/SECURITY.md).

## Security Issue Resolution
### Responsible Disclosure Process

OpenFGA project vulnerability handling related processes are recorded in the [OpenFGA Security Doc](https://github.com/openfga/.github/blob/main/SECURITY.md). Related security vulnerabilities can be reported and communicated via email to security@openfga.dev.

The OpenFGA maintainers are responsible for responding within 5 working days. It is the maintainers’ duties to triage the severity of the issue and determine how to address the issue.

### Incident Response

See [OpenFGA Security Doc](https://github.com/openfga/.github/blob/main/SECURITY.md) for a description for how incidents should be communicated. 

OpenFGA maintainers are responsible for tracking any vulnerabilities reported. 

Issues are triaged with high priority, and are escalated if confirmed. Issues are triaged in private channels. 

OpenFGA follows the Github security advisory process. When an issue is identified, a CVE is requested, and the fix is worked out in a private branch linked to the CVE.

Once the fix is confirmed, it will be released in a new patch release for each OpenFGA major supported version.

The changelog will link to the CVE, which will describe the vulnerability and its mitigation. Any public announcements sent for these fixes will be linked to [the release notes](https://github.com/openfga/openfga/releases/tag/v1.3.2).

## Appendix

### Known Issues Over Time

All OpenFGA security issues can be found on the [Github advisories page](https://github.com/openfga/openfga/security/advisories).

Given OpenFGA makes authorization decisions, bugs in OpenFGA can cause security issues for OpenFGA's adopters. Each product bug where OpenFGA returns a positive authorization result when it should not is treated as a security vulnerability.

OpenFGA [passes OpenSSF Best Practices](https://bestpractices.coreinfrastructure.org/projects/6374). 

### Case Studies. 

- [Okta Fine Grained Authorization](https://fga.dev) is a SaaS product built on top of OpenFGA. It provisions and operates a set of OpenFGA nodes for their customers in a multi-tenant environment.

- Canonical uses OpenFGA as an authorization component for Juju and for LFX. Some relevant links discussing it:

  - [Commercial Systems OpenFGA](https://github.com/canonical/cs-openfga)
  - [OpenFGA Charm Relation Interfaces](https://github.com/canonical/charm-relation-interfaces/blob/main/interfaces/openfga/v0/README.md)
  - [OpenFGA Authorization Driver for LXD](https://discourse.ubuntu.com/t/openfga-authorization-driver/38979)

- [Read.AI](https://www.read.ai/) uses OpenFGA to implement authorization for their products. One of their features it to record/analyze meetings. They use OpenFGA to control the different actions different users can perform on recorded meetings.

The list of companies that publicly acknowledged using OpenFGA can be found [here](https://github.com/openfga/community/blob/main/ADOPTERS.md).

### Related Projects/Vendors

[OPA](https://github.com/open-policy-agent) is a CNCF project that can be used to externalize authorization. It uses [Rego](https://www.openpolicyagent.org/docs/latest/policy-language/) as its policy language. The main difference with OpenFGA is that OpenFGA stores the data it needs to use to make authorization decisions as relationship tuples. OPA needs to be provided with that data when invoking the policies, or needs to query the data when evaluating the policy.

[Kyverno](https://github.com/kyverno) is a CNCF project designed to implement security policies for Kubernetes deployments. 
