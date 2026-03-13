---
applyTo: "CHANGELOG.md"
excludeAgent: "coding-agent"
---

# Semver Breaking Change Version Bump Check

When reviewing a pull request whose title starts with "release: update changelog for release":

1. Look at the newly added version header (e.g., `## [X.Y.Z]`) in CHANGELOG.md.
2. Scan all changelog entries under that new version header for the word "Breaking" or "**Breaking**" (bold markdown).
3. If any breaking change entry is found, verify that the version bump includes at least a MINOR version increment compared to the previous release. For example:
   - `1.11.6` to `1.12.0` = correct (MINOR bump)
   - `1.11.6` to `1.11.7` = INCORRECT (only PATCH bump despite breaking changes)
4. If breaking changes exist but the version is only a PATCH bump, flag this as a critical issue: "This release contains breaking changes but only has a PATCH version bump. Per semver, breaking changes require at least a MINOR version bump."

This rule only applies to release PRs. For regular PRs adding entries to the `## [Unreleased]` section, no version bump validation is needed.
