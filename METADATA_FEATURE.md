# Authorization Model Metadata Feature

This pull request adds metadata support to OpenFGA authorization models, enabling users to attach key-value labels for better organization, discovery, and management.

## Changes Made

### 1. Database Schema Updates
- **PostgreSQL**: Added `metadata JSONB` column with GIN index for efficient querying
- **MySQL**: Added `metadata JSON` column with index for metadata searches  
- **SQLite**: Added `metadata TEXT` column with JSON validation constraint

### 2. Metadata Validation Package
- **Location**: `pkg/server/metadata/validation.go`
- **Features**:
  - Key-value pair validation following DNS-1123 label standards
  - Size limits (63 chars for keys, 253 chars for values, 32KB total)
  - Reserved prefix protection (`openfga.dev/`, `kubernetes.io/`, `k8s.io/`)
  - Comprehensive test coverage

### 3. API Integration (Prepared)
- Updated `WriteAuthorizationModelCommand` with metadata validation hooks
- Extended `ReadAuthorizationModelsOptions` with metadata filtering support
- Prepared integration points for when API protobuf is updated

### 4. Documentation
- Complete feature documentation in `docs/metadata/authorization-model-metadata.md`
- Migration guide and best practices
- API usage examples and security considerations

## Technical Details

### Metadata Constraints
```go
const (
    MaxLabelKeyLength   = 63    // Maximum key length
    MaxLabelValueLength = 253   // Maximum value length  
    MaxMetadataSize     = 32768 // Maximum total size (32KB)
    MaxLabelsCount      = 64    // Maximum number of labels
)
```

### Key Validation Rules
- Must be lowercase DNS-1123 labels
- Only alphanumeric characters, dashes, and dots allowed
- Must start and end with alphanumeric characters
- No consecutive dashes or dots
- Reserved prefixes are protected

### Database Schema
The metadata is stored as JSON in the `authorization_model` table:

**PostgreSQL**:
```sql
ALTER TABLE authorization_model ADD COLUMN metadata JSONB;
CREATE INDEX idx_authorization_model_metadata ON authorization_model USING GIN (metadata);
```

## API Changes Required

This implementation is prepared for the following API protobuf changes:

```proto
message WriteAuthorizationModelRequest {
  // ...existing fields...
  map<string, string> metadata = 5; // New field
}

message AuthorizationModel {
  // ...existing fields...
  map<string, string> metadata = 5; // New field
}

message ReadAuthorizationModelsRequest {
  // ...existing fields...
  map<string, string> metadata_filter = 4; // New filtering field
}
```

## Testing

All new functionality includes comprehensive tests:
- Metadata validation with various edge cases
- Key format validation (valid/invalid patterns)
- Size limit enforcement
- Reserved prefix protection

Run tests with:
```bash
go test ./pkg/server/metadata/...
```

## Migration Strategy

1. **Phase 1** (This PR): Database schema and validation infrastructure
2. **Phase 2**: API protobuf updates in `openfga/api` repository
3. **Phase 3**: Full integration and client library updates
4. **Phase 4**: Advanced querying and tooling features

## Usage Examples

### Writing Models with Metadata
```go
metadata := map[string]string{
    "environment": "production",
    "team": "platform", 
    "version": "v1.2.3",
    "app.example.com/component": "auth-service",
}

// Validate metadata
if err := metadata.ValidateMetadata(metadata); err != nil {
    return fmt.Errorf("invalid metadata: %w", err)
}
```

### Querying Models by Metadata
```go
options := storage.ReadAuthorizationModelsOptions{
    MetadataFilter: map[string]string{
        "environment": "production",
        "team": "platform",
    },
}
```

## Backward Compatibility

- Existing authorization models continue to work unchanged
- Metadata field is optional in all operations
- No breaking changes to existing APIs
- Gradual rollout is supported

## Performance Considerations

- Metadata is stored denormalized for query performance
- Database indexes enable efficient metadata-based filtering
- Metadata does not impact authorization check performance
- Size limits prevent excessive memory usage

## Security

- No sensitive data should be stored in metadata
- Proper access controls apply to metadata operations
- Reserved prefixes prevent system conflicts
- Validation prevents injection attacks

## Next Steps

1. Merge this infrastructure PR
2. Update API protobuf definitions in `openfga/api`
3. Complete integration in write/read operations
4. Add HTTP API endpoints for metadata filtering
5. Update client libraries with metadata support

## Related Issues

This feature addresses the need for better authorization model organization and discovery, enabling:
- Multi-tenant model management
- Environment-based model separation  
- Team and application categorization
- Automated deployment and lifecycle management
