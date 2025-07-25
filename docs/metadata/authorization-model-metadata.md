# Authorization Model Metadata

This document describes the metadata feature for OpenFGA authorization models, which allows users to attach key-value labels to authorization models for better organization, discovery, and management.

## Overview

Authorization model metadata provides a way to attach arbitrary key-value pairs to authorization models, similar to Kubernetes labels. This enables:

- **Organization**: Categorize models by environment, team, application, etc.
- **Discovery**: Find models based on metadata filters
- **Automation**: Enable tooling and automation based on metadata
- **Lifecycle Management**: Track model versions, ownership, and deployment status

## Metadata Format

Metadata is a map of string key-value pairs with the following constraints:

### Keys
- Maximum length: 63 characters
- Must be valid DNS-1123 labels (alphanumeric, dashes, dots)
- Must start and end with alphanumeric characters
- Cannot use reserved prefixes: `openfga.dev/`, `kubernetes.io/`, `k8s.io/`

### Values
- Maximum length: 253 characters
- Can contain any UTF-8 characters except control characters
- Can be empty strings

### Limits
- Maximum number of labels per model: 64
- Maximum total metadata size: 32KB

## Examples

### Valid Metadata
```json
{
  "environment": "production",
  "team": "platform",
  "version": "v1.2.3",
  "app.example.com/component": "auth-service",
  "release-date": "2024-01-15"
}
```

### Reserved System Labels
The following prefixes are reserved for system use:
- `openfga.dev/` - Reserved for OpenFGA internal labels
- `kubernetes.io/` - Reserved for Kubernetes integration
- `k8s.io/` - Reserved for Kubernetes integration

## API Usage

### Writing Models with Metadata
```proto
message WriteAuthorizationModelRequest {
  string store_id = 1;
  repeated TypeDefinition type_definitions = 2;
  string schema_version = 3;
  map<string, Condition> conditions = 4;
  map<string, string> metadata = 5; // New metadata field
}
```

### Reading Models with Metadata
```proto
message AuthorizationModel {
  string id = 1;
  string schema_version = 2;
  repeated TypeDefinition type_definitions = 3;
  map<string, Condition> conditions = 4;
  map<string, string> metadata = 5; // New metadata field
}
```

### Filtering by Metadata
```proto
message ReadAuthorizationModelsRequest {
  string store_id = 1;
  int32 page_size = 2;
  string continuation_token = 3;
  map<string, string> metadata_filter = 4; // New filter field
}
```

## Database Schema

The metadata is stored in the `authorization_model` table as a JSON column:

### PostgreSQL
```sql
ALTER TABLE authorization_model ADD COLUMN metadata JSONB;
CREATE INDEX idx_authorization_model_metadata ON authorization_model USING GIN (metadata);
```

### MySQL
```sql
ALTER TABLE authorization_model ADD COLUMN metadata JSON;
CREATE INDEX idx_authorization_model_metadata ON authorization_model (metadata(255));
```

### SQLite
```sql
ALTER TABLE authorization_model ADD COLUMN metadata TEXT CHECK(metadata IS NULL OR json_valid(metadata));
```

## Migration Path

1. **Phase 1**: Database schema migration to add metadata column
2. **Phase 2**: API changes to support metadata in requests/responses
3. **Phase 3**: Enhanced querying and filtering capabilities
4. **Phase 4**: Tooling and automation features

## Best Practices

### Naming Conventions
- Use lowercase with dashes: `app-name`, `environment`
- Use DNS notation for namespacing: `team.example.com/owner`
- Be consistent across your organization

### Common Labels
```json
{
  "environment": "production|staging|development",
  "team": "platform|security|product",
  "version": "v1.0.0",
  "application": "user-service",
  "component": "authorization",
  "owner": "team-email@company.com",
  "created-by": "deployment-system",
  "last-updated": "2024-01-15T10:30:00Z"
}
```

### Security Considerations
- Don't store sensitive information in metadata
- Be mindful of metadata size limits
- Use proper access controls for metadata management
- Consider compliance requirements for metadata retention

## Implementation Details

### Validation
- Metadata validation occurs during model write operations
- Invalid metadata results in a validation error
- Normalization removes empty keys and trims whitespace

### Performance
- Metadata is stored denormalized for query performance
- GIN indexes (PostgreSQL) and JSON indexes (MySQL) support efficient filtering
- Metadata doesn't affect authorization check performance

### Backward Compatibility
- Existing models without metadata continue to work
- New schema version may be required for full metadata support
- Gradual rollout strategy recommended
