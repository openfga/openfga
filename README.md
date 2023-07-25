# OpenFGA
[![Go Reference](https://pkg.go.dev/badge/github.com/openfga/openfga.svg)](https://pkg.go.dev/github.com/openfga/openfga)
![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/openfga/openfga?sort=semver&color=green)
[![Container Image](https://img.shields.io/github/v/release/openfga/openfga?color=blueviolet&label=container&logo=docker "Container Image")](https://hub.docker.com/r/openfga/openfga/tags)
![Codecov](https://img.shields.io/codecov/c/github/openfga/openfga)
[![Go Report](https://goreportcard.com/badge/github.com/openfga/openfga)](https://goreportcard.com/report/github.com/openfga/openfga)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/6374/badge)](https://bestpractices.coreinfrastructure.org/projects/6374)
[![Discord Server](https://img.shields.io/discord/759188666072825867?color=7289da&logo=discord "Discord Server")](https://discord.gg/8naAwJfWN6)
[![Twitter](https://img.shields.io/twitter/follow/openfga?color=%23179CF0&logo=twitter&style=flat-square "@openfga on Twitter")](https://twitter.com/openfga)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fopenfga%2Fopenfga.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fopenfga%2Fopenfga?ref=badge_shield)
[![Artifact HUB](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/openfga)](https://artifacthub.io/packages/helm/openfga/openfga)
[![OpenSSF
Scorecard](https://api.securityscorecards.dev/projects/github.com/openfga/openfga/badge)](https://api.securityscorecards.dev/projects/github.com/openfga/openfga)

A high-performance and flexible authorization/permission engine built for developers and inspired by [Google Zanzibar](https://research.google/pubs/pub48190/).

OpenFGA is designed to make it easy for developers to model their application permissions and add and integrate fine-grained authorization into their applications.

It allows in-memory data storage for quick development, as well as pluggable database modules. It currently supports PostgreSQL 14 and MySQL 8.

It offers an [HTTP API](https://openfga.dev/api/service) and a [gRPC API](https://buf.build/openfga/api/file/main:openfga/v1/openfga_service.proto). It has SDKs for [Node.js/JavaScript](https://www.npmjs.com/package/@openfga/sdk), [GoLang](https://github.com/openfga/go-sdk), [Python](https://github.com/openfga/python-sdk) and [.NET](https://www.nuget.org/packages/OpenFga.Sdk). Look in our [Community section](https://github.com/openfga/community#community-projects) for third-party SDKs and tools.

## Getting Started

The following section aims to help you get started quickly. Please look at our official [documentation](https://openfga.dev/) for in-depth information.

### Setup and Installation

> ℹ️ The following sections setup an OpenFGA server using the default configuration values. These are for rapid development and not for a production environment. Data written to an OpenFGA instance using the default configuration with the memory storage engine will *not* persist after the service is stopped.
>
> For more information on how to configure the OpenFGA server, please take a look at our official documentation on [Running in Production](https://openfga.dev/docs/getting-started/running-in-production).

#### Docker

OpenFGA is available on [Dockerhub](https://hub.docker.com/r/openfga/openfga), so you can quickly start it using the in-memory datastore by running the following commands:

```bash
docker pull openfga/openfga
docker run -p 8080:8080 -p 3000:3000 openfga/openfga run
```

#### Docker Compose

[`docker-compose.yaml`](./docker-compose.yaml) provides an example of how to launch OpenFGA with Postgres using `docker compose`.

1. First, either clone this repo or curl the `docker-compose.yaml` file with the following command:

   ```bash
   curl -LO https://openfga.dev/docker-compose.yaml
   ```

2. Then, run the following command:

   ```bash
   docker compose up
   ```

### Package Managers

If you are a [Homebrew](https://brew.sh/) user, you can install [OpenFGA](https://formulae.brew.sh/formula/openfga) with the following command:

```bash
brew install openfga
```

#### Pre-compiled Binaries

Download your platform's [latest release](https://github.com/openfga/openfga/releases/latest) and extract it. Then run the binary
with the command:

```bash
./openfga run
```

### Building from Source

There are two recommended options for building OpenFGA from source code:

#### Building from source with `go install`

> Make sure you have Go 1.20 or later installed. See the [Go downloads](https://go.dev/dl/) page.

You can install from source using Go modules:

1. First, make sure `$GOBIN` is on your shell `$PATH`:

   ```bash
   export PATH=$PATH:$(go env GOBIN)
   ```

2. Then use the install command:

   ```bash
   go install github.com/openfga/openfga/cmd/openfga
   ```

3. Run the server with:

   ```bash
   ./openfga run
   ```

#### Building from source with `go build`

Alternatively you can build OpenFGA by cloning the project from this Github repo, and then building it with the `go build` command:

1. Clone the repo to a local directory, and navigate to that directory:

   ```bash
   git clone https://github.com/openfga/openfga.git && cd openfga
   ```

2. Then use the build command:

   ```bash
   go build -o ./openfga ./cmd/openfga
   ```

3. Run the server with:

   ```bash
   ./openfga run
   ```

### Verifying the Installation

Now that you have [Set up and Installed](#setup-and-installation) OpenFGA, you can test your installation by [creating an OpenFGA Store](https://openfga.dev/docs/getting-started/create-store).

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

## Playground
The Playground facilitates rapid development by allowing you to visualize and model your application's authorization model(s) and manage relationship tuples with a locally running OpenFGA instance.

To run OpenFGA with the Playground disabled, provide the `--playground-enabled=false` flag.

```
./openfga run --playground-enabled=false
```
Once OpenFGA is running, by default, the Playground can be accessed at [http://localhost:3000/playground](http://localhost:3000/playground).

In the event that a port other than the default port is required, the `--playground-port` flag can be set to change it. For example,

```sh
./openfga run --playground-enabled --playground-port 3001
```

## Profiler (pprof)
Profiling through [pprof](https://github.com/google/pprof) can be enabled on the OpenFGA server by providing the `--profiler-enabled` flag.

```sh
./openfga run --profiler-enabled
```

This will start serving profiling data on port `3001`. You can see that data by visiting `http://localhost:3001/debug/pprof`.

If you need to serve the profiler on a different address, you can do so by specifying the `--profiler-addr` flag. For example,

```sh
./openfga run --profiler-enabled --profiler-addr :3002
```

Once the OpenFGA server is running, in another window you can run the following command to generate a compressed CPU profile:

```sh
go tool pprof -proto -seconds 60 http://localhost:3001/debug/pprof/profile
# will collect data for 60 seconds and generate a file like pprof.samples.cpu.001.pb.gz
```

That file can be analyzed visually by running the following command and then visiting `http://localhost:8084`:

```shell
go tool pprof -http=localhost:8084 pprof.samples.cpu.001.pb.gz
```

## Next Steps

Take a look at examples of how to:

- [Write an Authorization Model](https://openfga.dev/api/service#/Authorization%20Models/WriteAuthorizationModel)
- [Write Relationship Tuples](https://openfga.dev/api/service#/Relationship%20Tuples/Write)
- [Perform Authorization Checks](https://openfga.dev/api/service#/Relationship%20Queries/Check)
- [Add Authentication to your OpenFGA server](https://openfga.dev/docs/getting-started/setup-openfga#configuring-authentication)

Don't hesitate to browse the official [Documentation](https://openfga.dev/), [API Reference](https://openfga.dev/api/service).

## Limitations
### MySQL Storage engine
The MySQL storage engine has a lower length limit for some properties of a tuple compared with other storage backends. For more information see [the docs](https://openfga.dev/docs/getting-started/setup-openfga/docker#configuring-data-storage).

## Production Readiness

The core [OpenFGA](https://github.com/openfga/openfga) service has been in use by [Auth0 FGA](https://fga.dev) in production since December 2021.

OpenFGA's Memory Storage Adapter was built for development purposes only and is not recommended for a production environment, because it is not designed for scalable queries and has no support for persistence.

OpenFGA's PostgreSQL Storage Adapter was purposely built for OpenFGA. Auth0 is not using it in a production environment.

OpenFGA's MySQL Storage Adapter was contributed to OpenFGA by [@twintag](https://github.com/twintag), who are using it in a production environment. Auth0 is not using it in a production environment.

The OpenFGA team will do its best to address all production issues with high priority.

You can learn about more organizations using OpenFGA in production [here](https://github.com/openfga/community/blob/main/ADOPTERS.md). If your organization is using OpenFGA in production please consider adding it to the list.

## Contributing

See [CONTRIBUTING](https://github.com/openfga/.github/blob/main/CONTRIBUTING.md).

[doc]: https://openfga.dev/docs
[config-doc]: https://openfga.dev/docs/getting-started/setup-openfga
[api]: https://openfga.dev/api/service
[prod-checklist]: https://openfga.dev/docs/getting-started/running-in-production

## Community Meetings

We hold a monthly meeting to interact with the community, collaborate and receive/provide feedback. You can find more details, including the time, our agenda, and the meeting minutes [here](https://github.com/openfga/community/blob/main/community-meetings.md).
