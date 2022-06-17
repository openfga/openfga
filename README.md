# OpenFGA

[![Go Reference](https://pkg.go.dev/badge/github.com/openfga/openfga.svg)](https://pkg.go.dev/github.com/openfga/openfga)
![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/openfga/openfga?sort=semver&color=green)
[![Container Image](https://img.shields.io/github/v/release/openfga/openfga?color=blueviolet&label=container&logo=docker "Container Image")](https://hub.docker.com/r/openfga/openfga/tags)
![Downloads](https://img.shields.io/github/downloads/openfga/openfga/total.svg?style=flat&color=lightgrey)  <!-- render new line with double space here -->  
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](./LICENSE)
![Codecov](https://img.shields.io/codecov/c/github/openfga/openfga)
[![Go Report](https://goreportcard.com/badge/github.com/openfga/openfga)](https://goreportcard.com/report/github.com/openfga/openfga)
![Snyk Vulnerabilities for GitHub Repo](https://img.shields.io/snyk/vulnerabilities/github/openfga/openfga?color=orange)  <!-- render new line with double space here -->  
[![Discord Server](https://img.shields.io/discord/844600078504951838?color=7289da&logo=discord "Discord Server")](https://discord.com/channels/759188666072825867/930524706854031421)
[![Twitter](https://img.shields.io/twitter/follow/openfga?color=%23179CF0&logo=twitter&style=flat-square "@openfga on Twitter")](https://twitter.com/openfga)

A high-performance and flexible authorization/permission engine built for developers and inspired by [Google Zanzibar](https://research.google/pubs/pub48190/).

OpenFGA is designed to make it easy for developers to model their application permissions and add and integrate fine-grained authorization into their applications.

# Getting Started

The following section aims to help you get started quickly. Please look at our official [documentation](https://openfga.dev/) for in-depth information.

## Setup and Installation

> ℹ️ The following sections setup an OpenFGA server using the default configuration values. These are for rapid development and not for a production environment.
>
> For more information on how to configure the OpenFGA server, please take a look at our official documentation on [Configuring OpenFGA](https://openfga.dev/docs/getting-started/setup-openfga#configuring-the-server) or our [Production Checklist](https://openfga.dev/docs/getting-started/setup-openfga#production-checklist).

### Docker

Given OpenFGA is available on [Dockerhub](https://hub.docker.com/r/openfga/openfga) you can quickly start it using the in-memory datastore by running the following command:

```bash
docker run -p 8080:8080 openfga/openfga:latest
```

### Docker Compose

[`docker-compose.yaml`](./docker-compose.yaml) provides an example of how to launch OpenFGA using `docker compose`. It launches PostgreSQL too, but it's not wired up to use it as a datastore yet:

```bash
docker compose up openfga
```

If you haven't cloned the repository you can get the `docker-compose.yaml` file with the following command:

```bash
curl -LO https://openfga.dev/docker-compose.yaml

```

### Pre-compiled Binaries

Download your platform's [latest release](https://github.com/openfga/openfga/releases/latest) and extract it. Then run the binary
with the command:

```bash
./bin/openfga run
```

### Source

> Make sure you have Go 1.18 or later installed. See the [Go downloads](https://go.dev/dl/) page.

You can install from source using Go modules (make sure `$GOBIN` is on your shell `$PATH`).

```bash
export PATH=$PATH:$(go env GOBIN)
```

Then:

```bash
go install github.com/openfga/openfga/cmd/openfga
```

Or you can build it with the source by cloning the project and then building it.

```bash
git clone https://github.com/openfga/openfga.git && cd openfga
go build cmd/openfga/openfga.go

./openfga run
```

## Verifying the Installation

Now that you have [Setup and Installed](#setup-and-installation) OpenFGA, you can test your installation by [creating an OpenFGA Store](https://openfga.dev/docs/getting-started/create-store).

```bash
curl -X POST 'localhost:8080/stores' \
--header 'Content-Type: application/json' \
--data-raw '{
    "name": "openfga-demo"
}'
```

If everything is running correctly, you should get a response with information about the newly created store, for example:

```json
{
  "id": "01G3EMTKQRKJ93PFVDA1SJHWD2",
  "name": "openfga-demo",
  "created_at": "2022-05-19T17:11:12.888680Z",
  "updated_at": "2022-05-19T17:11:12.888680Z"
}
```

## Next Steps

Take a look at examples of how to:

- [Write an Authorization Model](https://openfga.dev/api/service#/Authorization%20Models/WriteAuthorizationModel)
- [Write Relationship Tuples](https://openfga.dev/api/service#/Relationship%20Tuples/Write)
- [Perform Authorization Checks](https://openfga.dev/api/service#/Relationship%20Queries/Check)
- [Add Authentication to your OpenFGA server](https://openfga.dev/docs/getting-started/setup-openfga#configuring-authentication)

Don't hesitate to browse the official [Documentation](https://openfga.dev/), [API Reference](https://openfga.dev/api/service).

# Production Readiness

The core [OpenFGA](https://github.com/openfga/openfga) service has been in use by [Auth0 FGA](https://fga.dev) in production since December 2021.

OpenFGA's PostgreSQL Storage Adapter was purposely built for OpenFGA and does not have production usage yet.

The OpenFGA team will do its best to address all production issues with high priority.

# Contributing

See [CONTRIBUTING](https://github.com/openfga/.github/blob/main/CONTRIBUTING.md).

[doc]: https://openfga.dev/docs
[config-doc]: https://openfga.dev/docs/getting-started/setup-openfga
[api]: https://openfga.dev/api/service
[prod-checklist]: https://openfga.dev/docs/getting-started/setup-openfga#production-checklist
