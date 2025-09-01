# OpenFGA Deprecation Policy

This document outlines how the OpenFGA organization handles deprecations and breaking changes across all of our projects, including the OpenFGA server and SDK libraries.

## Overview

The OpenFGA organization consists of multiple components that may have different versioning strategies:

- **OpenFGA Server**: Follows [Semantic Versioning 2.0.0](https://semver.org/) and is currently in major version 1.x.x
- **OpenFGA SDKs**: Follow [Semantic Versioning 2.0.0](https://semver.org/) but are currently in major version 0.x.x

The different versioning approaches mean we handle breaking changes differently for each component type.

## SDK Deprecation Policy (v0.x.x)

### Current State
All OpenFGA SDKs are currently in major version 0.x.x, which according to Semantic Versioning rules allows for breaking changes in minor version releases.

### Approach
While we *can* introduce breaking changes in minor versions due to our 0.x.x status, **we actively try to avoid it** and follow these principles:

1. **Minimize Breaking Changes**: We make every effort to maintain backward compatibility
2. **Clear Communication**: When breaking changes are necessary, we:
   - Clearly mark them as `BREAKING CHANGE:` in changelogs
   - Provide detailed migration guides
   - Explain the rationale for the change
3. **Gradual Migration**: When possible, we provide migration paths:
   - Introduce new APIs alongside deprecated ones
   - Rename old methods (e.g., `batchCheck` → `clientBatchCheck`)
   - Provide clear documentation on moving to new patterns
4. **Ship breaking changes in minor features**: While SDKs are in 0.x.x versions, breaking changes should require a minor version upgrade.

### Recent Breaking Change Examples

#### Go SDK
- **v0.7.0 (April 2025)**: BatchCheck API changes
  - `BatchCheck` method renamed to `ClientBatchCheck`
  - `BatchCheckResponse` renamed to `ClientBatchCheckResponse`
  - Required OpenFGA v1.8.0+ server
  
- **v0.6.0 (August 2024)**: Enum handling changes
  - Added class name prefixes to avoid collisions (e.g., `INT` → `TYPENAME_INT`)

#### Python SDK
- **v0.9.0 (December 2024)**: BatchCheck method changes
  - `batch_check` renamed to `client_batch_check`
  - `BatchCheckResponse` renamed to `ClientBatchCheckClientResponse`

#### JavaScript/TypeScript SDK
- **v0.8.0 (January 2025)**: BatchCheck and Node.js requirements
  - Minimum Node.js version raised to v16.15.0
  - BatchCheck API changes similar to other SDKs

## OpenFGA Server Deprecation Policy (v1.x.x)

### Three-Phase Deprecation Process

For the OpenFGA server, we follow a structured three-phase approach that typically spans **~6 months**:

#### Phase 1: Announce Deprecation & Enable New Behavior
- Introduce the new behavior/feature
- Announce deprecation of the old behavior in changelog and documentation
- Both old and new behaviors work simultaneously
- Users are encouraged to migrate to the new approach

#### Phase 2: Require Flag to Enable Old Behavior
- New behavior becomes the default
- Old behavior requires an explicit flag to enable
- Clear warnings when using deprecated flags
- Documentation updated to reflect new defaults

#### Phase 3: Remove Old Behavior
- Remove deprecated flags and old implementation
- Update documentation to remove references to old behavior
- Provide migration guide for any remaining users

### Real Examples from OpenFGA Server

#### Schema 1.0 to 1.1 Migration (Primary Example)
This migration perfectly demonstrates our three-phase deprecation process:

1. **Phase 1 - Announce & Enable New ([v0.3.0](https://github.com/openfga/openfga/releases/tag/v0.3.0))**: 
   - Introduced support for Schema 1.1 alongside existing Schema 1.0
   - Both schema versions worked simultaneously
   - Users encouraged to migrate to Schema 1.1 for new features
   - Documented the [migration process](https://github.com/openfga/openfga.dev/pull/304/files#diff-fb73553a5788ebb568065b0d7d145541d1a324943585eefac4ca2ac8753e3c35)

2. **Phase 2 - Require Flag ([v0.4.1](https://github.com/openfga/openfga/releases/tag/v0.4.1))**:
   - Schema 1.1 became the default
   - Required explicit flag to continue using Schema 1.0:
     - `--allow-schema-1-0-type-definitions` flag needed for creating 1.0 models
     - `--allow-evaluating-schema-1-0-models` flag needed for evaluating 1.0 models
   - Clear warnings when using deprecated flags

3. **Phase 3 - Remove ([v1.1.0](https://github.com/openfga/openfga/releases/tag/v1.1.0))**:
   - Completely removed Schema 1.0 support
   - Removed both `--allow-schema-1-0-type-definitions` and `--allow-evaluating-schema-1-0-models` flags
   - Only Schema 1.1 supported going forward

*Timeline: ~10 months from introduction to removal*

## Communication Guidelines

### Changelog Requirements
- All breaking changes must be clearly marked with `BREAKING CHANGE:` or `[BREAKING]`
- Include rationale for the change
- Provide migration instructions when possible
- Reference related issues/PRs for more context

### Documentation Updates
- Update API documentation to reflect changes
- Provide migration guides for complex changes
- Update examples and tutorials
- Add deprecation notices to relevant sections

### Community Communication
- Announce major breaking changes in community channels
- Provide advance notice when possible (especially for server changes)
- Respond to user questions and concerns promptly
- Consider user feedback in deprecation timelines

## Migration to v1.0.0 SDKs

Once OpenFGA SDKs reach v1.0.0, they will follow strict semantic versioning:

- **Patch versions (1.0.x)**: Bug fixes only, no breaking changes
- **Minor versions (1.x.0)**: New features, no breaking changes
- **Major versions (x.0.0)**: Breaking changes allowed

At that point, SDK deprecations will follow a more formal process similar to the server's three-phase approach, with longer timelines to provide more stability for production users.

## Best Practices for Breaking Changes

### For SDK Maintainers
1. **Batch related changes**: Group related breaking changes into single releases when possible
2. **Provide clear migration paths**: Include code examples showing before/after patterns
3. **Consider backwards compatibility**: Use method overloading or optional parameters when possible
4. **Test thoroughly**: Validate changes against real-world usage patterns

### For OpenFGA Server Maintainers  
1. **Follow the three-phase process**: Don't skip phases unless critical security/performance issues require it
2. **Use experimental flags**: Test new features behind flags before making them default
3. **Monitor adoption**: Track usage of deprecated flags to understand migration progress
4. **Communicate early**: Announce deprecations as soon as decisions are made

### For Users
1. **Monitor changelogs**: Stay updated with deprecation announcements
2. **Test early**: Try new versions in non-production environments
3. **Plan migrations**: Account for deprecation timelines in project planning
4. **Provide feedback**: Report issues or concerns with deprecation timelines

## Conclusion

While OpenFGA SDKs in v0.x.x allow for more frequent breaking changes, we strive to minimize disruption to users. The OpenFGA server follows a structured deprecation process to ensure users have adequate time to migrate. As our ecosystem matures, we will continue to refine these processes to balance innovation with stability.

For questions about specific deprecations or this policy, please reach out through our [community channels](https://openfga.dev/community) or open an issue in the relevant repository.
