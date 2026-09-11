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
