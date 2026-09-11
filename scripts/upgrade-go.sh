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

# Echo the "sha256:..." manifest digest of an image ref, read from the registry
# WITHOUT downloading image layers (empty on failure). Matches the digest that
# `docker pull` reports. $1 = image reference (tag).
image_digest() {
  docker buildx imagetools inspect "$1" --format '{{.Manifest.Digest}}' 2>/dev/null || true
}

# Resolve the chainguard/go:latest digest and verify the image is actually the
# target Go version. Chainguard's public registry serves only :latest (versioned
# tags need a paid tier) and the image carries NO version label, so the only
# ground truth is running it. Aborts if Chainguard's latest lags the target.
# $1 = target Go version (e.g. "1.26.8"). Echoes the digest on success.
resolve_chainguard_go() {
  local ver="$1" latest_digest actual_version
  latest_digest="$(image_digest "cgr.dev/chainguard/go:latest")"
  [[ -n "$latest_digest" ]] || die "Could not resolve cgr.dev/chainguard/go:latest digest."
  # Pin to the digest we just resolved so the version we read is the version we pin.
  actual_version="$(docker run --rm --entrypoint go "cgr.dev/chainguard/go:latest@${latest_digest}" version 2>/dev/null \
    | awk '{print $3}' | sed 's/^go//')"
  [[ -n "$actual_version" ]] || die "Could not read Go version from cgr.dev/chainguard/go:latest."
  if [[ "$actual_version" != "$ver" ]]; then
    die "Chainguard go:latest is ${actual_version}, not ${ver} (go.dev is ahead). Retry later."
  fi
  printf '%s' "$latest_digest"
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
  local repo_root
  repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  cd "$repo_root"
  preflight

  log "Resolving latest versions..."
  GO_VERSION="$(latest_go_version)"
  [[ -n "$GO_VERSION" && "$GO_VERSION" != "null" ]] || die "Could not determine latest Go version."
  GO_DIGEST="$(resolve_chainguard_go "$GO_VERSION")"      # aborts on Chainguard lag
  STATIC_DIGEST="$(image_digest "cgr.dev/chainguard/static:latest")"
  [[ -n "$STATIC_DIGEST" ]] || die "Could not resolve cgr.dev/chainguard/static digest."
  PROBE_TAG="$(latest_probe_tag)"
  [[ -n "$PROBE_TAG" && "$PROBE_TAG" != "null" ]] || die "Could not determine grpc-health-probe release."
  PROBE_DIGEST="$(image_digest "ghcr.io/grpc-ecosystem/grpc-health-probe:${PROBE_TAG}")"
  [[ -n "$PROBE_DIGEST" ]] || die "Could not resolve grpc-health-probe digest for ${PROBE_TAG}."

  local target_toolchain="go${GO_VERSION}"
  local target_go_image="cgr.dev/chainguard/go:${GO_VERSION}@${GO_DIGEST}"
  local target_static="cgr.dev/chainguard/static@${STATIC_DIGEST}"
  local target_probe="ghcr.io/grpc-ecosystem/grpc-health-probe:${PROBE_TAG}@${PROBE_DIGEST}"

  # Current values (|| true so `set -e` + grep-miss doesn't abort).
  local cur_toolchain cur_go_image cur_static cur_probe
  cur_toolchain="$(grep -E '^toolchain ' go.mod | awk '{print $2}' || true)"
  cur_go_image="$(grep -oE 'cgr\.dev/chainguard/go:\S+' Dockerfile | head -1 || true)"
  cur_static="$(grep -oE 'cgr\.dev/chainguard/static\S*' Dockerfile | head -1 || true)"
  cur_probe="$(grep -oE 'ghcr\.io/grpc-ecosystem/grpc-health-probe:\S+' Dockerfile | head -1 || true)"

  local changed=0
  _row() {  # $1 label, $2 current, $3 target
    local mark="unchanged"
    if [[ "$2" != "$3" ]]; then mark="CHANGED"; changed=1; fi
    printf '  %-16s %s\n' "$1:" "$mark"
    printf '    from: %s\n' "$2"
    printf '    to:   %s\n' "$3"
  }

  log ""
  log "Planned changes:"
  _row "toolchain"  "$cur_toolchain" "$target_toolchain"
  _row "chainguard/go" "$cur_go_image" "$target_go_image"
  _row "chainguard/static" "$cur_static" "$target_static"
  _row "grpc-health-probe" "$cur_probe" "$target_probe"
  log ""

  if (( changed == 0 )); then
    log "already up to date"
    exit 0
  fi

  if (( DRY_RUN == 1 )); then
    log "(dry run — no files modified)"
    exit 0
  fi

  apply_file_edits
  insert_changelog

  log "Files updated. Next steps:"
  log "  1. Review the diff:  git diff"
  log "  2. Replace #PLACEHOLDER in CHANGELOG.md with the real PR number."
  log "  3. Commit and open a PR."
}

# Only run main when executed directly, not when sourced (so functions are unit-testable).
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  main "$@"
fi
