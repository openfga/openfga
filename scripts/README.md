# Release Scripts
 
## Setup

These scripts use two important packages, `gh` and `jq`. The scripts were written on a Mac in bash.

## Installing gh

Go to [gh install](https://github.com/cli/cli#installation) to install.

Once it is installed, you will need to do some work to allow it to authenticate. Follow [the manual](https://cli.github.com/manual/) to run the `gh auth login` command.

- Select "github.com"
- Select your preferred protocol
- Either upload your SSH public key, or choose "Skip" and proceed to browser login

## Installing jq

Go to [jq](https://stedolan.github.io/jq/download/)

Once it is installed, you can then use it on the command line to parse out a JSON object. For example, to see that both gh and jq are working, try running:

```
gh api repos/:owner/:repo/releases | jq --arg packageVersion "v1.9.2" '.[] | select(.tag_name | contains($packageVersion))'
```

The above command will find the release notes that include `v1.9.2` in the `tag_name`, thereby finding the release for the OpenFGA v1.9.2 package release.

## Usage

```
gh auth login
./scripts/create-release-pr.sh -t <version>
```