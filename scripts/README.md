# Release Scripts

This performs the following steps:
- Checkout the base branch
- Creates the release branch off of the base
- Creates the pull request against the base branch
- Adds the body to the pull request
- Adds the pull request labels
- Creates a PR against openfga/helm-charts to bump Chart.yaml versions
 
## Setup

These scripts use two important packages, `gh` and `jq`. The scripts were written on a Mac in bash.

### openfga/helm-charts repo

The script also updates the versions in [openfga/helm-charts](https://github.com/openfga/helm-charts) by default, and as such expects it to be cloned as a sibling directory to this repo (e.g., if this repo is in `~/repos/openfga/openfga`, then the helm-charts repo should be in `~/repos/openfga/helm-charts`):

```sh
git clone git@github.com:openfga/helm-charts.git ../helm-charts
```

If desired, you can skip this step by providing an argument to the script, see [Usage](#usage) below.

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

Next, run the script by passing the tag version. This will create a release PR for both openfga and helm-charts:

```sh
./scripts/create-release-pr.sh -t <version>

# e.g.,
./scripts/create-release-pr.sh -t 1.9.2
```

To skip the helm-charts PR, pass `-H`:

```sh
./scripts/create-release-pr.sh -t 1.9.2 -H
```

If the tag already exists, or the branch was already used, the script will be cancelled and you will need to proceed further manually.