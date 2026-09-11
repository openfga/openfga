#!/usr/bin/env bash
#
# upgrade-go.sh — auto-detect the latest stable Go release and refresh the Go
# toolchain across go.mod, both Dockerfiles, and CHANGELOG.md.
#
# Usage:
#   scripts/upgrade-go.sh [--dry-run] [--help]
#
# The script edits files only; it never runs git. After it finishes, review the
# diff, replace the CHANGELOG PR #PLACEHOLDER, commit, and open a PR.

set -euo pipefail

DRY_RUN=0

log()  { printf '%s\n' "$*"; }
warn() { printf 'WARNING: %s\n' "$*" >&2; }
die()  { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

usage() {
  cat <<'EOF'
Usage: scripts/upgrade-go.sh [--dry-run] [--help]

Auto-detects the latest stable Go release and refreshes:
  - go.mod            (toolchain line only; the `go 1.X` directive is untouched)
  - Dockerfile        (chainguard/go, chainguard/static, grpc-health-probe)
  - Dockerfile.goreleaser
  - CHANGELOG.md      (inserts a ### Security entry with a #PLACEHOLDER PR number)

Options:
  --dry-run   Resolve versions and print planned changes without editing files.
  --help      Show this help.
EOF
}

parse_args() {
  while (( $# > 0 )); do
    case "$1" in
      --dry-run) DRY_RUN=1 ;;
      --help|-h) usage; exit 0 ;;
      *) die "Unknown argument: $1 (try --help)" ;;
    esac
    shift
  done
}

preflight() {
  local missing=()
  local cmd
  for cmd in docker curl jq perl awk; do
    command -v "$cmd" >/dev/null 2>&1 || missing+=("$cmd")
  done
  if (( ${#missing[@]} > 0 )); then
    die "Missing required commands: ${missing[*]}"
  fi
  docker info >/dev/null 2>&1 || die "Docker daemon is not running or not reachable."
}

# Echo the latest STABLE Go version, e.g. "1.26.8".
latest_go_version() {
  curl -fsSL "https://go.dev/dl/?mode=json" \
    | jq -r 'map(select(.stable == true)) | .[0].version' \
    | sed 's/^go//'
}

# Pull an image ref and echo its "sha256:..." digest (empty on failure).
# $1 = full image reference to pull.
pull_digest() {
  docker pull "$1" 2>/dev/null | grep 'Digest:' | cut -d ' ' -f 2 || true
}

# Resolve the chainguard/go digest for a specific Go version and verify the tag
# pins to it. Aborts if Chainguard has not yet published that version.
# $1 = Go version (e.g. "1.26.8"). Echoes the digest on success.
resolve_chainguard_go() {
  local ver="$1" digest
  digest="$(pull_digest "cgr.dev/chainguard/go:latest")"
  [[ -n "$digest" ]] || die "Could not resolve cgr.dev/chainguard/go:latest digest."
  if ! docker pull "cgr.dev/chainguard/go:${ver}@${digest}" >/dev/null 2>&1; then
    die "Chainguard has not published go ${ver} yet (go.dev is ahead). Retry later."
  fi
  printf '%s' "$digest"
}

# Echo the latest grpc-health-probe release tag, e.g. "v0.4.57".
latest_probe_tag() {
  curl -fsSL "https://api.github.com/repos/grpc-ecosystem/grpc-health-probe/releases/latest" \
    | jq -r '.tag_name'
}

# Rewrite go.mod toolchain + both Dockerfiles to the resolved target refs.
# Requires GO_VERSION, GO_DIGEST, STATIC_DIGEST, PROBE_TAG, PROBE_DIGEST in scope.
apply_file_edits() {
  # go.mod: toolchain line only (never the `go 1.X` directive).
  perl -pi -e "s{^toolchain go\\S+}{toolchain go${GO_VERSION}}" go.mod

  # chainguard/go builder image (Dockerfile only).
  GO_IMAGE_REF="cgr.dev/chainguard/go:${GO_VERSION}@${GO_DIGEST}" \
    perl -pi -e 's{cgr\.dev/chainguard/go:\S+}{$ENV{GO_IMAGE_REF}}g' Dockerfile

  # chainguard/static (both Dockerfiles).
  STATIC_REF="cgr.dev/chainguard/static@${STATIC_DIGEST}" \
    perl -pi -e 's{cgr\.dev/chainguard/static\S*}{$ENV{STATIC_REF}}g' \
    Dockerfile Dockerfile.goreleaser

  # grpc-health-probe (both Dockerfiles).
  PROBE_REF="ghcr.io/grpc-ecosystem/grpc-health-probe:${PROBE_TAG}@${PROBE_DIGEST}" \
    perl -pi -e 's{ghcr\.io/grpc-ecosystem/grpc-health-probe:\S+}{$ENV{PROBE_REF}}g' \
    Dockerfile Dockerfile.goreleaser
}

# Insert a Security bullet under [Unreleased] in CHANGELOG.md.
# Requires GO_VERSION and PROBE_TAG in scope. Idempotent per Go version.
insert_changelog() {
  local bullet
  bullet="- Update Go toolchain to ${GO_VERSION}, align the \`chainguard/go\` builder image, refresh \`chainguard/static\`, and rebuild the embedded \`grpc-health-probe\` (${PROBE_TAG}). [#PLACEHOLDER](https://github.com/openfga/openfga/pull/PLACEHOLDER)"

  # Skip if an entry for this Go version already exists (idempotence).
  if grep -qF "Update Go toolchain to ${GO_VERSION}," CHANGELOG.md; then
    return 0
  fi

  local tmp
  tmp="$(mktemp)"
  awk -v bullet="$bullet" '
    BEGIN { in_unreleased=0; done=0 }
    /^## \[Unreleased\]/ { in_unreleased=1; print; next }
    /^## \[/ && in_unreleased==1 {
      if (!done) { print "### Security"; print bullet; print ""; done=1 }
      in_unreleased=0
    }
    in_unreleased==1 && /^### Security/ && done==0 {
      print; print bullet; done=1; next
    }
    { print }
    END { if (in_unreleased==1 && done==0) { print "### Security"; print bullet } }
  ' CHANGELOG.md > "$tmp" && mv "$tmp" CHANGELOG.md
}

main() {
  parse_args "$@"
  # Resolve the repo root so the script works from any CWD.
  local repo_root
  repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  cd "$repo_root"
  preflight
  log "preflight OK (repo root: $repo_root)"
  # Resolution + edits are added in later tasks.
}

# Only run main when executed directly, not when sourced (so functions are unit-testable).
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  main "$@"
fi
