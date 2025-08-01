#!/usr/bin/env bash

set -eo pipefail
 
tag=
base="main"
 
die() {
local exit_code=1
local OPTIND=1
local opt

while getopts "c:" opt; do
    case "$opt" in
    c)
        exit_code="$OPTARG"
        ;;
    esac
done

shift $((OPTIND - 1))

echo "ERROR:" "$@" >&2
exit $exit_code
}
 
print_usage() {
cat <<EOF
Creates a release pull request.

$(echo -e "\033[1mUSAGE\033[0m")
    ${0##*/} [OPTIONS]

This performs the following steps:
- Checkout the base branch
- Creates the release branch off of the base
- Creates the pull request against the base branch
- Adds the body to the pull request
- Adds the pull request labels

$(echo -e "\033[1mOPTIONS\033[0m")
EOF
sed -n '/^[[:space:]]*###/ s//   /p' "$BASH_SOURCE"

cat <<EOF

$(echo -e "\033[1mEXAMPLE\033[0m")
    \$ ${0##*/} -t 1.9.3

$(echo -e "\033[1mEXIT CODES\033[0m")
- 0: Success
- 1: General error
- 2: Usage error
EOF
}

info() {
echo "===>" "$@"
}

while getopts ":t:h" opt; do
case "$opt" in
    t)
    ### -t    The version of the release to use for the tag.
    tag="$OPTARG"
    ;;
    ### -h    Print this help.
    h)
    print_usage
    exit 0
    ;;
    :)
    die 2 "Option -$OPTARG requires an argument."
    ;;
    \?)
    die "Invalid option: -${OPTARG}"
    ;;
esac
done

shift "$((OPTIND-1))"

if [[ -z "$tag" ]]; then
  print_usage
  die -c 2 "Missing required -t <version> argument"
fi

labels="release"
branch_name="release/v${tag}"
title="Update changelog to prep for ${tag} release"

echo "Branch name: ${branch_name}"
echo "Base branch: ${base}"

git checkout $base && git pull || die "Could not checkout or pull ${base}."

info "Creating branch '${branch_name}'..."
git checkout -b $branch_name

curr_date=$(date +%Y-%m-%d)
changelog_file="CHANGELOG.md"
tmp_file="$(mktemp)"

awk -v tag="$tag" -v date="$curr_date" '
  BEGIN { unreleased_found=0 }
  {
    if (!unreleased_found && $0 ~ /^## \[Unreleased\]/) {
      unreleased_found=1
      print $0
      print "## [" tag "] - " date
    } else {
      print $0
    }
  }
' "$changelog_file" > "$tmp_file"

mv "$tmp_file" "$changelog_file"
echo "Promoted Unreleased section to [$tag] - $curr_date"

npx keep-a-changelog

info "Committing..."

git add "$changelog_file" && git commit -m "${title}"

git push --set-upstream origin $branch_name

body=$(cat << END
## Description
This PR updates the changelog in preparation for releasing \`v${tag}\`.
END
)

echo "$body"

info "Creating pull request..."

pull_request=$(gh pr create --base ${base} --title "Update changelog to prep for ${tag} release" --body "$body" --label "$labels")

echo "Pull Request: $pull_request"