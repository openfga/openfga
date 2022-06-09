# OpenFGA
![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/openfga/openfga?sort=semver&color=green) [![Container Image](https://img.shields.io/github/v/release/openfga/openfga?color=blueviolet&label=container&logo=docker "Container Image")](https://hub.docker.com/r/openfga/openfga/tags) ![Downloads](https://img.shields.io/github/downloads/openfga/openfga/total.svg?style=flat&color=lightgrey) [![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](./LICENSE)
![Codecov](https://img.shields.io/codecov/c/github/openfga/openfga) ![Snyk Vulnerabilities for GitHub Repo](https://img.shields.io/snyk/vulnerabilities/github/openfga/openfga?color=orange)
[![Discord Server](https://img.shields.io/discord/844600078504951838?color=7289da&logo=discord "Discord Server")](https://discord.com/channels/759188666072825867/930524706854031421) [![Twitter](https://img.shields.io/twitter/follow/openfga?color=%23179CF0&logo=twitter&style=flat-square "@openfga on Twitter")](https://twitter.com/openfga)

A high performance and flexible authorization/permission engine built for developers and inspired by [Google Zanzibar](https://research.google/pubs/pub48190/).

OpenFGA is designed to make it easy for developers to model their application permissions, and to add and integrate fine-grained authorization into their applications.

# Getting Started
The following section is intended to help you get started quickly. For more in-depth information, please take a look at our official [Documentation](https://openfga.dev/).

## Setup and Installation
> ℹ️ The following sections setup an OpenFGA server using the default configuration values. These are intended for rapid development and not for a production environment.
>
> For more information on how to configure the OpenFGA server, please take a look at our official documentation on [Configuring OpenFGA](https://openfga.dev/intro/setup-openfga#configuring-the-server) or our [Production Checklist](https://openfga.dev/intro/setup-openfga#production-checklist).

### Docker Compose
[`docker-compose.yaml`](./docker-compose.yaml) provides an example of how to setup OpenFGA using Docker, and it's a great way to get started quickly.

```
➜ docker-compose up openfga
```

### Pre-compiled Binaries
Download the [latest release](https://github.com/openfga/openfga/releases/latest) for your platform and extract it. Then run the binary
with the command:
```
➜ ./bin/openfga
```

### Source
> Make sure you have Go 1.18 or later installed. See the [Go downloads](https://go.dev/dl/) page.

You can install from source using Go modules (make sure `$GOBIN` is on your shell `$PATH`).

```
➜ export PATH=$PATH:$(go env GOBIN)
➜ go install github.com/openfga/openfga/cmd/openfga
```

Or you can build it with the source by cloning the project first and then building it.

```
➜ git clone https://github.com/openfga/openfga.git && cd openfga
➜ go build cmd/openfga/openfga.go

➜ ./openfga
```

## Verifying the Installation
Now that you have [Setup and Installed](#setup-and-installation) OpenFGA, you can test your installation by [creating an OpenFGA Store](https://openfga.dev/integration/create-store/). 

```
curl -X POST 'localhost:8080/stores' \
--header 'Content-Type: application/json' \
--data-raw '{
    "name": "openfga-demo"
}'
```
If everything is running correctly you should get a response with information about the newly created store, for example:

```
{
    "id": "01G3EMTKQRKJ93PFVDA1SJHWD2",
    "name": "openfga-demo",
    "created_at": "2022-05-19T17:11:12.888680Z",
    "updated_at": "2022-05-19T17:11:12.888680Z"
}
```
## Next Steps
Take a look at examples of how to:

* [Write an Authorization Model](https://openfga.dev/api/service/#/Authorization%20Models/WriteAuthorizationModel)
* [Write Relationship Tuples](https://openfga.dev/api/service/#/Relationship%20Tuples/Write)
* [Check Relationships](https://openfga.dev/api/service/#/Relationship%20Queries/Check)
* [Add authentication](https://openfga.dev/intro/setup-openfga#configuring-authentication)

Don't hesitate to browse the official [Documentation][https://openfga.dev/], [API Reference][https://openfga.dev/api/service], and [Examples][examples].

# Production Readiness
The core [OpenFGA](https://github.com/openfga/openfga) service is used by [Auth0 FGA](https://fga.dev), and it has been used in production since December 2021. 

OpenFGA's PostgreSQL Storage Adapter was built specifically for OpenFGA and does not have production usage yet. 

The OpenFGA team will do its best to address all production issues with high priority.

# Contributing

See [CONTRIBUTING](https://github.com/openfga/.github/blob/main/CONTRIBUTING.md).

[doc]: https://docs.openfga.dev
[config-doc]: https://docs.openfga.dev/configuration
[examples]: https://docs.openfga.dev/examples
[api]: https://docs.openfga.dev/api
[prod-checklist]: https://docs.openfga.dev/production-checklist
