## [Unreleased]

### Added
- Refactored migration system for dependency injection and library integration. [#2502](https://github.com/openfga/openfga/pull/2502)
  - Introduced `MigrationProvider` interface for custom migration implementations
  - Moved per-database migration logic to individual drivers (`postgres/`, `mysql/`, `sqlite/`)
  - Added `MigratorRegistry` for flexible provider management
  - Maintains full backward compatibility with existing `RunMigrations` API
  - Enables applications embedding OpenFGA as a library to inject their own migration systems
  - Provides multiple integration patterns: default, custom provider, registry, and direct provider usage

## [1.8.15] - 2025-06-11
