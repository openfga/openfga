# Scripts

Maintenance and release helper scripts for OpenFGA.

- [`create-release-pr.sh`](#release-scripts) — open a release PR.
- [`upgrade-go.sh`](#upgrade-go) — upgrade the Go toolchain and pinned container images.

## Release Scripts

`create-release-pr.sh` performs the following steps:
- Checkout the base branch
- Creates the release branch off of the base
- Creates the pull request against the base branch
- Adds the body to the pull request
- Adds the pull request labels

After the release PR merges, two GitHub Actions are triggered:
1. Create the git tag + release for the new version.
2. Create a PR against [openfga/helm-charts](https://github.com/openfga/helm-charts) to bump the chart versions.

## Setup

These scripts use two important packages, `gh` and `jq`. The scripts were written on a Mac in bash.

### Installing gh

Go to [gh install](https://github.com/cli/cli#installation) to install.

Once it is installed, you will need to do some work to allow it to authenticate. Follow [the manual](https://cli.github.com/manual/) to run the `gh auth login` command.

- Select "github.com"
- Select your preferred protocol
- Either upload your SSH public key, or choose "Skip" and proceed to browser login

### Installing jq

Go to [jq](https://stedolan.github.io/jq/download/)

Once it is installed, you can then use it on the command line to parse out a JSON object. For example, to see that both gh and jq are working, try running:

```sh
gh api repos/:owner/:repo/releases | jq --arg packageVersion "v1.9.2" '.[] | select(.tag_name | contains($packageVersion))'
```

The above command will find the release notes that include `v1.9.2` in the `tag_name`, thereby finding the release for the OpenFGA v1.9.2 package release.

### Installing npm

To install `npm` in order to use `npx`, if you plan on using `node`, it is recommended that you use [nvm](https://github.com/nvm-sh/nvm?tab=readme-ov-file#installing-and-updating). However, if you just need to run this script, you can use:

```sh
brew install npm
```

## Usage

Check if you are already logged in using `gh auth status`. If not, follow the browser login for `gh`. To login with `ssh` using a browser, use:

```sh
gh auth login --hostname github.com --git-protocol ssh --skip-ssh-key --web
```

Next, run the script by passing the tag version:

```sh
./scripts/create-release-pr.sh -t <version>

# eg.,
./scripts/create-release-pr.sh -t 1.9.2
```

If the tag already exists, or the branch was already used, the script will be cancelled and you will need to proceed further manually.

## Upgrade Go

`upgrade-go.sh` replaces the manual Go-upgrade playbook. It auto-detects the latest stable Go release and rewrites, in one step:

- `go.mod` — the `toolchain` line only (the `go 1.X` language directive is never touched).
- `Dockerfile` — the pinned `chainguard/go` builder, `chainguard/static`, and `grpc-health-probe` image digests.
- `Dockerfile.goreleaser` — the pinned `chainguard/static` and `grpc-health-probe` image digests (it has no builder stage).
- `CHANGELOG.md` — a `### Security` entry under `[Unreleased]` with a `#PLACEHOLDER` PR number to fill in.

The script only edits files; it never runs `git`. Review the diff, replace the CHANGELOG `#PLACEHOLDER` with the real PR number, commit, and open the PR.

### Requirements

`docker` (with a running daemon and the `buildx` plugin), `curl`, `jq`, `perl`, and `awk`. Image digests are read from the registry without downloading layers; the `chainguard/go` builder image is pulled once to verify its Go version matches the target.

### Usage

```sh
# Preview the planned changes without editing any files:
make upgrade-go ARGS="--dry-run"

# Apply the upgrade:
make upgrade-go

# Or run the script directly:
./scripts/upgrade-go.sh --dry-run
./scripts/upgrade-go.sh
```

The script is idempotent: if everything is already at the latest versions it prints `already up to date` and makes no changes. If Chainguard has not yet published the latest Go release, it aborts with a clear message so the toolchain and builder image never drift out of sync — retry once Chainguard catches up.