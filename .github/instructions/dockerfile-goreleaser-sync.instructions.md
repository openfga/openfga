---
applyTo: "Dockerfile*"
excludeAgent: "coding-agent"
---

# Dockerfile.goreleaser / Dockerfile.goreleaser-fips grpc-health-probe Sync Check

When reviewing changes to `Dockerfile`, check whether the `ghcr.io/grpc-ecosystem/grpc-health-probe` image reference (version tag and/or digest) was modified.

If it was changed, verify that **all three** of the following files were also updated in the same PR so that their grpc-health-probe reference uses the identical image reference (same version tag and digest):

- `Dockerfile.goreleaser` — `COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:...` line
- `Dockerfile.goreleaser-fips` — `COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:...` line
- `Dockerfile.fips` — `FROM ghcr.io/grpc-ecosystem/grpc-health-probe:...` line

If any of these files were not updated to match, flag them as required changes.
