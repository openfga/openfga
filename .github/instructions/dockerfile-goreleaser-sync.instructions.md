---
applyTo: "Dockerfile"
excludeAgent: "coding-agent"
---

# Dockerfile.goreleaser grpc-health-probe Sync Check

When reviewing changes to `Dockerfile`, check whether the `ghcr.io/grpc-ecosystem/grpc-health-probe` image reference (version tag and/or digest) was modified.

If it was changed, verify that `Dockerfile.goreleaser` was also updated in the same PR so that its `COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:...` line uses the identical image reference (same version tag and digest) as the `FROM ghcr.io/grpc-ecosystem/grpc-health-probe:...` line in `Dockerfile`.

If `Dockerfile.goreleaser` was not updated to match, flag this as a required change: "The grpc-health-probe image in `Dockerfile.goreleaser` must be updated to match the version in `Dockerfile`."
