# AuthZEN Specification Compliance - Detailed Implementation Plan

## Overview

This document provides a detailed implementation plan for making OpenFGA's AuthZEN implementation fully spec-compliant. The work spans two repositories:
1. **`openfga/api`** - Proto definitions (must be released first)
2. **`openfga/openfga`** - Server implementation

## Prerequisites

- Never commit or push code to GitHub directly
- Proto changes in `openfga/api` must be released before implementation in `openfga/openfga`
- All new endpoints must be gated behind the `enable-authzen` experimental flag

---

## Phase 1: Experimental Flag & Gating (openfga/openfga)

### Step 1.1: Define the Experimental Flag

**File:** [pkg/server/config/config.go](pkg/server/config/config.go)

Add a new experimental flag constant after the existing experimental flags (around line 105):

```go
const (
    // ... existing flags ...
    ExperimentalPipelineListObjects = "pipeline_list_objects"
    
    // AuthZEN experimental flag
    ExperimentalEnableAuthZen = "enable_authzen"
)
```

### Step 1.2: Register Flag in CLI

**File:** [cmd/run/run.go](cmd/run/run.go)

Update the `--experimentals` flag help text (around line 99) to include the new flag:

```go
flags.StringSlice("experimentals", defaultConfig.Experimentals, 
    fmt.Sprintf("a comma-separated list of experimental features to enable. Allowed values: %s, %s, %s, %s, %s, %s", 
        serverconfig.ExperimentalCheckOptimizations, 
        serverconfig.ExperimentalListObjectsOptimizations, 
        serverconfig.ExperimentalAccessControlParams, 
        serverconfig.ExperimentalPipelineListObjects, 
        serverconfig.ExperimentalDatastoreThrottling,
        serverconfig.ExperimentalEnableAuthZen))
```

### Step 1.3: Gate Existing AuthZEN Endpoints

**File:** [pkg/server/server.go](pkg/server/server.go)

Add experimental flag check at the beginning of `Evaluation` method (line ~1161):

```go
func (s Server) Evaluation(ctx context.Context, req *authzenv1.EvaluationRequest) (*authzenv1.EvaluationResponse, error) {
    // Gate behind experimental flag
    if !s.featureFlagClient.Boolean(serverconfig.ExperimentalEnableAuthZen, req.GetStoreId()) {
        return nil, status.Error(codes.Unimplemented, "AuthZEN endpoints are experimental. Enable with --experimentals=enable_authzen")
    }
    
    ctx, span := tracer.Start(ctx, "authzen.Evaluation")
    // ... rest of implementation
}
```

Same for `Evaluations` method (line ~1188):

```go
func (s Server) Evaluations(ctx context.Context, req *authzenv1.EvaluationsRequest) (*authzenv1.EvaluationsResponse, error) {
    // Gate behind experimental flag
    if !s.featureFlagClient.Boolean(serverconfig.ExperimentalEnableAuthZen, req.GetStoreId()) {
        return nil, status.Error(codes.Unimplemented, "AuthZEN endpoints are experimental. Enable with --experimentals=enable_authzen")
    }
    
    ctx, span := tracer.Start(ctx, "authzen.Evaluations")
    // ... rest of implementation
}
```

### Step 1.4: Add Helper Method for AuthZEN Enablement Check

**File:** [pkg/server/server.go](pkg/server/server.go)

Add a helper method similar to `IsAccessControlEnabled()`:

```go
// IsAuthZenEnabled returns true if the AuthZEN experimental feature is enabled.
func (s *Server) IsAuthZenEnabled(storeID string) bool {
    return s.featureFlagClient.Boolean(serverconfig.ExperimentalEnableAuthZen, storeID)
}
```

---

## Phase 2: Proto Definitions (openfga/api)

### Step 2.1: Add Pagination Messages

**File:** `authzen/v1/authzen_service.proto`

Add pagination-related messages after the existing messages:

```protobuf
// Pagination request parameters
message PageRequest {
  // Continuation token from previous response
  optional string token = 1;
  // Maximum number of results to return (default: 50, max: 1000)
  optional uint32 limit = 2 [(validate.rules).uint32 = {lte: 1000}];
}

// Pagination response parameters
message PageResponse {
  // Token to retrieve next page (empty if no more results)
  string next_token = 1;
  // Number of results in this page
  uint32 count = 2;
  // Total number of results (if known, otherwise 0)
  optional uint32 total = 3;
}
```

### Step 2.2: Add Search API Proto Definitions

**File:** `authzen/v1/authzen_service.proto`

Add new RPC methods to the `AuthZenService`:

```protobuf
service AuthZenService {
  // ... existing Evaluation and Evaluations RPCs ...

  // SubjectSearch returns subjects that have access to the specified resource
  rpc SubjectSearch(SubjectSearchRequest) returns (SubjectSearchResponse) {
    option (google.api.http) = {
      post: "/stores/{store_id}/access/v1/search/subject"
      body: "*"
    };
    option (grpc.gateway.protoc_gen_openapiv2.options.openapiv2_operation) = {
      summary: "Search for subjects with access to a resource"
      tags: ["AuthZen"]
      operation_id: "SubjectSearch"
    };
  }

  // ResourceSearch returns resources that a subject has access to
  rpc ResourceSearch(ResourceSearchRequest) returns (ResourceSearchResponse) {
    option (google.api.http) = {
      post: "/stores/{store_id}/access/v1/search/resource"
      body: "*"
    };
    option (grpc.gateway.protoc_gen_openapiv2.options.openapiv2_operation) = {
      summary: "Search for resources a subject has access to"
      tags: ["AuthZen"]
      operation_id: "ResourceSearch"
    };
  }

  // ActionSearch returns actions a subject can perform on a resource
  rpc ActionSearch(ActionSearchRequest) returns (ActionSearchResponse) {
    option (google.api.http) = {
      post: "/stores/{store_id}/access/v1/search/action"
      body: "*"
    };
    option (grpc.gateway.protoc_gen_openapiv2.options.openapiv2_operation) = {
      summary: "Search for actions a subject can perform on a resource"
      tags: ["AuthZen"]
      operation_id: "ActionSearch"
    };
  }

  // GetConfiguration returns PDP metadata and capabilities
  rpc GetConfiguration(GetConfigurationRequest) returns (GetConfigurationResponse) {
    option (google.api.http) = {
      get: "/.well-known/authzen-configuration"
    };
    option (grpc.gateway.protoc_gen_openapiv2.options.openapiv2_operation) = {
      summary: "Get AuthZEN PDP configuration and capabilities"
      tags: ["AuthZen"]
      operation_id: "GetConfiguration"
    };
  }
}
```

### Step 2.3: Add Search Request/Response Messages

**File:** `authzen/v1/authzen_service.proto`

```protobuf
// SubjectFilter is used for search operations where id is optional
// Unlike Subject (where both type and id are required), SubjectFilter
// allows filtering by type only, which is needed for SubjectSearch
message SubjectFilter {
  string type = 1 [
    (validate.rules).string = {
      pattern: "^[^:#@\\s]{1,50}$"
      ignore_empty: false
    },
    (google.api.field_behavior) = REQUIRED
  ];
  // Optional subject id filter
  optional string id = 2 [
    (validate.rules).string = {
      pattern: "^[^:#@\\s]{1,500}$"
      ignore_empty: true
    }
  ];
  // Optional so nil values are omitted from JSON (no "properties": null)
  optional google.protobuf.Struct properties = 3;
}

// SubjectSearch messages
message SubjectSearchRequest {
  Resource resource = 1 [(validate.rules).message.required = true, (google.api.field_behavior) = REQUIRED];
  Action action = 2 [(validate.rules).message.required = true, (google.api.field_behavior) = REQUIRED];
  // Filter by subject type (id is optional in SubjectFilter)
  // Subject type is REQUIRED for SubjectSearch because ListUsers requires a UserFilter
  optional SubjectFilter subject = 3;
  google.protobuf.Struct context = 4;
  PageRequest page = 5;
  string store_id = 6 [json_name = "store_id"];
  string authorization_model_id = 7 [json_name = "authorization_model_id"];
}

message SubjectSearchResponse {
  repeated Subject subjects = 1;
  PageResponse page = 2;
}

// ResourceSearch messages
message ResourceSearchRequest {
  Subject subject = 1 [(validate.rules).message.required = true, (google.api.field_behavior) = REQUIRED];
  Action action = 2 [(validate.rules).message.required = true, (google.api.field_behavior) = REQUIRED];
  optional Resource resource = 3;  // Optional filter by resource type
  google.protobuf.Struct context = 4;
  PageRequest page = 5;
  string store_id = 6 [json_name = "store_id"];
  string authorization_model_id = 7 [json_name = "authorization_model_id"];
}

message ResourceSearchResponse {
  repeated Resource resources = 1;
  PageResponse page = 2;
}

// ActionSearch messages
message ActionSearchRequest {
  Subject subject = 1 [(validate.rules).message.required = true, (google.api.field_behavior) = REQUIRED];
  Resource resource = 2 [(validate.rules).message.required = true, (google.api.field_behavior) = REQUIRED];
  google.protobuf.Struct context = 3;
  PageRequest page = 4;
  string store_id = 5 [json_name = "store_id"];
  string authorization_model_id = 6 [json_name = "authorization_model_id"];
}

message ActionSearchResponse {
  repeated Action actions = 1;
  PageResponse page = 2;
}
```

### Step 2.4: Add PDP Metadata Messages

**File:** `authzen/v1/authzen_service.proto`

**Important:** All `properties` fields in `Subject`, `SubjectFilter`, `Resource`, and `Action` messages MUST be marked as `optional` so that nil values are omitted from JSON responses (avoiding `"properties": null`). This is a protojson behavior where `optional` message fields are omitted when nil.

```protobuf
message GetConfigurationRequest {}

message GetConfigurationResponse {
  PolicyDecisionPoint policy_decision_point = 1 [json_name = "policy_decision_point"];
  Endpoints access_endpoints = 2 [json_name = "access_endpoints"];
  repeated string capabilities = 3;
}

message PolicyDecisionPoint {
  string name = 1;
  string version = 2;
  string description = 3;
}

message Endpoints {
  string evaluation = 1;
  string evaluations = 2;
  string subject_search = 3 [json_name = "subject_search"];
  string resource_search = 4 [json_name = "resource_search"];
  string action_search = 5 [json_name = "action_search"];
}
```

### Step 2.5: Add Evaluations Semantic Options

**File:** `authzen/v1/authzen_service.proto`

Add to `EvaluationsRequest` message:

```protobuf
message EvaluationsRequest {
  optional Subject subject = 1;
  optional Action action = 2;
  optional Resource resource = 3;
  optional google.protobuf.Struct context = 4;
  repeated EvaluationsItemRequest evaluations = 5;
  string store_id = 6 [json_name = "store_id"];
  
  // Options for batch evaluation semantics
  EvaluationsOptions options = 7;
}

message EvaluationsOptions {
  // Controls how batch evaluations are processed
  EvaluationsSemantic evaluations_semantic = 1 [json_name = "evaluations_semantic"];
}

enum EvaluationsSemantic {
  // Execute all evaluations (default behavior)
  EXECUTE_ALL = 0;
  // Stop on first deny decision
  DENY_ON_FIRST_DENY = 1;
  // Stop on first permit decision  
  PERMIT_ON_FIRST_PERMIT = 2;
}
```

### Step 2.6: Generate Proto Code

Run in `openfga/api` directory:
```bash
make generate
```

---

## Phase 3: Properties-to-Context Mapping (openfga/openfga)

### Step 3.1: Create Context Merger Utility

**File:** `pkg/server/commands/authzen_utils.go` (NEW)

```go
package commands

import (
    "google.golang.org/protobuf/types/known/structpb"
    authzenv1 "github.com/openfga/api/proto/authzen/v1"
)

// SubjectPropertiesProvider is an interface for types that provide subject properties.
// Both Subject and SubjectFilter implement this interface.
type SubjectPropertiesProvider interface {
    GetProperties() *structpb.Struct
}

// MergePropertiesToContext merges subject, resource, and action properties into 
// the context struct. Properties are namespaced with their source prefix.
// Precedence (lowest to highest): subject.properties, resource.properties, 
// action.properties, request context
func MergePropertiesToContext(
    requestContext *structpb.Struct,
    subject SubjectPropertiesProvider,  // Accepts both Subject and SubjectFilter
    resource *authzenv1.Resource,
    action *authzenv1.Action,
) (*structpb.Struct, error) {
    merged := make(map[string]interface{})
    
    // Add subject properties with "subject." prefix
    if subject != nil && subject.GetProperties() != nil {
        for k, v := range subject.GetProperties().AsMap() {
            merged["subject."+k] = v
        }
    }
    
    // Add resource properties with "resource." prefix
    if resource != nil && resource.GetProperties() != nil {
        for k, v := range resource.GetProperties().AsMap() {
            merged["resource."+k] = v
        }
    }
    
    // Add action properties with "action." prefix
    if action != nil && action.GetProperties() != nil {
        for k, v := range action.GetProperties().AsMap() {
            merged["action."+k] = v
        }
    }
    
    // Request context takes precedence (overrides properties)
    if requestContext != nil {
        for k, v := range requestContext.AsMap() {
            merged[k] = v
        }
    }
    
    if len(merged) == 0 {
        return nil, nil
    }
    
    return structpb.NewStruct(merged)
}
```

### Step 3.2: Update Evaluate Command

**File:** [pkg/server/commands/evaluate.go](pkg/server/commands/evaluate.go)

Update `NewEvaluateRequestCommand` to merge properties:

```go
func NewEvaluateRequestCommand(req *authzenv1.EvaluationRequest) (*EvaluateRequestCommand, error) {
    mergedContext, err := MergePropertiesToContext(
        req.GetContext(),
        req.GetSubject(),
        req.GetResource(),
        req.GetAction(),
    )
    if err != nil {
        return nil, err
    }
    
    cmd := &EvaluateRequestCommand{
        checkParams: openfgav1.CheckRequest{
            StoreId:              req.GetStoreId(),
            AuthorizationModelId: req.GetAuthorizationModelId(),
            TupleKey: &openfgav1.CheckRequestTupleKey{
                User:     fmt.Sprintf("%s:%s", req.GetSubject().GetType(), req.GetSubject().GetId()),
                Relation: req.GetAction().GetName(),
                Object:   fmt.Sprintf("%s:%s", req.GetResource().GetType(), req.GetResource().GetId()),
            },
            Context: mergedContext,
        },
    }
    return cmd, nil
}
```

### Step 3.3: Update Batch Evaluate Command

**File:** [pkg/server/commands/batch_evaluate.go](pkg/server/commands/batch_evaluate.go)

Update to merge properties for each evaluation item, handling default values.

---

## Phase 4: Search API Implementation (openfga/openfga)

### Step 4.1: Subject Search Handler

**File:** `pkg/server/commands/subject_search.go` (NEW)

```go
package commands

import (
    "context"
    "encoding/base64"
    "encoding/json"
    "fmt"
    
    authzenv1 "github.com/openfga/api/proto/authzen/v1"
    openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

const (
    DefaultSearchLimit = 50
    MaxSearchLimit     = 1000
)

type SubjectSearchCommand struct {
    listUsersFunc func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error)
}

type SubjectSearchCommandOption func(*SubjectSearchCommand)

func WithListUsersFunc(fn func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error)) SubjectSearchCommandOption {
    return func(c *SubjectSearchCommand) {
        c.listUsersFunc = fn
    }
}

func NewSubjectSearchCommand(opts ...SubjectSearchCommandOption) *SubjectSearchCommand {
    cmd := &SubjectSearchCommand{}
    for _, opt := range opts {
        opt(cmd)
    }
    return cmd
}

// PaginationToken stores pagination state
type PaginationToken struct {
    Offset     int    `json:"offset"`
    RequestHash string `json:"hash"`  // Hash of request params to detect changes
}

func (c *SubjectSearchCommand) Execute(
    ctx context.Context, 
    req *authzenv1.SubjectSearchRequest,
) (*authzenv1.SubjectSearchResponse, error) {
    // Merge properties to context
    // Note: req.GetSubject() returns *SubjectFilter which implements SubjectPropertiesProvider
    mergedContext, err := MergePropertiesToContext(
        req.GetContext(),
        req.GetSubject(),
        req.GetResource(),
        nil,
    )
    if err != nil {
        return nil, err
    }
    
    // Validate that subject type is specified (required for ListUsers)
    // This is because ListUsers requires at least one UserFilter
    if req.GetSubject() == nil || req.GetSubject().GetType() == "" {
        return nil, fmt.Errorf("subject type is required for subject search")
    }
    
    // Build ListUsers request
    listUsersReq := &openfgav1.ListUsersRequest{
        StoreId:              req.GetStoreId(),
        AuthorizationModelId: req.GetAuthorizationModelId(),
        Object: &openfgav1.Object{
            Type: req.GetResource().GetType(),
            Id:   req.GetResource().GetId(),
        },
        Relation: req.GetAction().GetName(),
        Context:  mergedContext,
        // Subject type is required, so we always have a UserFilter
        UserFilters: []*openfgav1.UserTypeFilter{
            {Type: req.GetSubject().GetType()},
        },
    }
    
    // Execute ListUsers (returns all results - no native pagination)
    resp, err := c.listUsersFunc(ctx, listUsersReq)
    if err != nil {
        return nil, err
    }
    
    // Convert to AuthZEN subjects
    var allSubjects []*authzenv1.Subject
    for _, user := range resp.GetUsers() {
        subject := userToSubject(user)
        if subject != nil {
            allSubjects = append(allSubjects, subject)
        }
    }
    
    // Apply pagination
    limit := getLimit(req.GetPage())
    offset := 0
    
    if req.GetPage() != nil && req.GetPage().GetToken() != "" {
        token, err := decodePaginationToken(req.GetPage().GetToken())
        if err != nil {
            return nil, fmt.Errorf("invalid pagination token: %w", err)
        }
        offset = token.Offset
    }
    
    // Slice results
    start := offset
    end := offset + int(limit)
    if start > len(allSubjects) {
        start = len(allSubjects)
    }
    if end > len(allSubjects) {
        end = len(allSubjects)
    }
    
    pagedSubjects := allSubjects[start:end]
    
    // Generate next token
    var nextToken string
    if end < len(allSubjects) {
        nextToken = encodePaginationToken(&PaginationToken{Offset: end})
    }
    
    return &authzenv1.SubjectSearchResponse{
        Subjects: pagedSubjects,
        Page: &authzenv1.PageResponse{
            NextToken: nextToken,
            Count:     uint32(len(pagedSubjects)),
            Total:     uint32Ptr(uint32(len(allSubjects))),
        },
    }, nil
}

func userToSubject(user *openfgav1.User) *authzenv1.Subject {
    if user.GetUser() != nil {
        obj := user.GetUser()
        return &authzenv1.Subject{
            Type: obj.GetType(),
            Id:   obj.GetId(),
        }
    }
    // Handle wildcard users
    if user.GetWildcard() != nil {
        return &authzenv1.Subject{
            Type: user.GetWildcard().GetType(),
            Id:   "*",
        }
    }
    return nil
}

func getLimit(page *authzenv1.PageRequest) uint32 {
    if page == nil || page.GetLimit() == 0 {
        return DefaultSearchLimit
    }
    if page.GetLimit() > MaxSearchLimit {
        return MaxSearchLimit
    }
    return page.GetLimit()
}

func encodePaginationToken(token *PaginationToken) string {
    data, _ := json.Marshal(token)
    return base64.StdEncoding.EncodeToString(data)
}

func decodePaginationToken(s string) (*PaginationToken, error) {
    data, err := base64.StdEncoding.DecodeString(s)
    if err != nil {
        return nil, err
    }
    var token PaginationToken
    if err := json.Unmarshal(data, &token); err != nil {
        return nil, err
    }
    return &token, nil
}

func uint32Ptr(v uint32) *uint32 {
    return &v
}
```

### Step 4.2: Resource Search Handler

**File:** `pkg/server/commands/resource_search.go` (NEW)

Similar pattern to Subject Search, but calls `ListObjects` instead of `ListUsers`.

```go
package commands

import (
    "context"
    
    authzenv1 "github.com/openfga/api/proto/authzen/v1"
    openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

type ResourceSearchCommand struct {
    listObjectsFunc func(ctx context.Context, req *openfgav1.ListObjectsRequest) (*openfgav1.ListObjectsResponse, error)
}

type ResourceSearchCommandOption func(*ResourceSearchCommand)

func WithListObjectsFunc(fn func(ctx context.Context, req *openfgav1.ListObjectsRequest) (*openfgav1.ListObjectsResponse, error)) ResourceSearchCommandOption {
    return func(c *ResourceSearchCommand) {
        c.listObjectsFunc = fn
    }
}

func NewResourceSearchCommand(opts ...ResourceSearchCommandOption) *ResourceSearchCommand {
    cmd := &ResourceSearchCommand{}
    for _, opt := range opts {
        opt(cmd)
    }
    return cmd
}

func (c *ResourceSearchCommand) Execute(
    ctx context.Context,
    req *authzenv1.ResourceSearchRequest,
) (*authzenv1.ResourceSearchResponse, error) {
    // Merge properties to context
    mergedContext, err := MergePropertiesToContext(
        req.GetContext(),
        req.GetSubject(),
        req.GetResource(),
        nil,
    )
    if err != nil {
        return nil, err
    }
    
    // Build ListObjects request
    listObjectsReq := &openfgav1.ListObjectsRequest{
        StoreId:              req.GetStoreId(),
        AuthorizationModelId: req.GetAuthorizationModelId(),
        User:                 fmt.Sprintf("%s:%s", req.GetSubject().GetType(), req.GetSubject().GetId()),
        Relation:             req.GetAction().GetName(),
        Type:                 req.GetResource().GetType(),
        Context:              mergedContext,
    }
    
    // Execute ListObjects
    resp, err := c.listObjectsFunc(ctx, listObjectsReq)
    if err != nil {
        return nil, err
    }
    
    // Convert to AuthZEN resources
    var allResources []*authzenv1.Resource
    for _, objID := range resp.GetObjects() {
        // objID format is "type:id"
        resource := objectIDToResource(objID, req.GetResource().GetType())
        if resource != nil {
            allResources = append(allResources, resource)
        }
    }
    
    // Apply pagination (same pattern as SubjectSearch)
    limit := getLimit(req.GetPage())
    offset := 0
    
    if req.GetPage() != nil && req.GetPage().GetToken() != "" {
        token, err := decodePaginationToken(req.GetPage().GetToken())
        if err != nil {
            return nil, fmt.Errorf("invalid pagination token: %w", err)
        }
        offset = token.Offset
    }
    
    start := offset
    end := offset + int(limit)
    if start > len(allResources) {
        start = len(allResources)
    }
    if end > len(allResources) {
        end = len(allResources)
    }
    
    pagedResources := allResources[start:end]
    
    var nextToken string
    if end < len(allResources) {
        nextToken = encodePaginationToken(&PaginationToken{Offset: end})
    }
    
    return &authzenv1.ResourceSearchResponse{
        Resources: pagedResources,
        Page: &authzenv1.PageResponse{
            NextToken: nextToken,
            Count:     uint32(len(pagedResources)),
            Total:     uint32Ptr(uint32(len(allResources))),
        },
    }, nil
}

func objectIDToResource(objID, resourceType string) *authzenv1.Resource {
    // Parse "type:id" format
    parts := strings.SplitN(objID, ":", 2)
    if len(parts) != 2 {
        return nil
    }
    return &authzenv1.Resource{
        Type: parts[0],
        Id:   parts[1],
    }
}
```

### Step 4.3: Action Search Handler

**File:** `pkg/server/commands/action_search.go` (NEW)

```go
package commands

import (
    "context"
    "fmt"
    
    authzenv1 "github.com/openfga/api/proto/authzen/v1"
    openfgav1 "github.com/openfga/api/proto/openfga/v1"
    "github.com/openfga/openfga/pkg/typesystem"
)

type ActionSearchCommand struct {
    typesystemResolver typesystem.TypesystemResolverFunc
    checkFunc          func(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error)
}

type ActionSearchCommandOption func(*ActionSearchCommand)

func WithTypesystemResolver(resolver typesystem.TypesystemResolverFunc) ActionSearchCommandOption {
    return func(c *ActionSearchCommand) {
        c.typesystemResolver = resolver
    }
}

func WithCheckFunc(fn func(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error)) ActionSearchCommandOption {
    return func(c *ActionSearchCommand) {
        c.checkFunc = fn
    }
}

func NewActionSearchCommand(opts ...ActionSearchCommandOption) *ActionSearchCommand {
    cmd := &ActionSearchCommand{}
    for _, opt := range opts {
        opt(cmd)
    }
    return cmd
}

func (c *ActionSearchCommand) Execute(
    ctx context.Context,
    req *authzenv1.ActionSearchRequest,
) (*authzenv1.ActionSearchResponse, error) {
    // Get typesystem for the model
    typesys, err := c.typesystemResolver(ctx, req.GetStoreId(), req.GetAuthorizationModelId())
    if err != nil {
        return nil, err
    }
    
    // Get all relations for the resource type
    relations, err := typesys.GetRelations(req.GetResource().GetType())
    if err != nil {
        return nil, err
    }
    
    // Merge properties to context
    mergedContext, err := MergePropertiesToContext(
        req.GetContext(),
        req.GetSubject(),
        req.GetResource(),
        nil,
    )
    if err != nil {
        return nil, err
    }
    
    // Check each relation to see if subject has access
    var permittedActions []*authzenv1.Action
    user := fmt.Sprintf("%s:%s", req.GetSubject().GetType(), req.GetSubject().GetId())
    object := fmt.Sprintf("%s:%s", req.GetResource().GetType(), req.GetResource().GetId())
    
    for relationName := range relations {
        checkReq := &openfgav1.CheckRequest{
            StoreId:              req.GetStoreId(),
            AuthorizationModelId: req.GetAuthorizationModelId(),
            TupleKey: &openfgav1.CheckRequestTupleKey{
                User:     user,
                Relation: relationName,
                Object:   object,
            },
            Context: mergedContext,
        }
        
        checkResp, err := c.checkFunc(ctx, checkReq)
        if err != nil {
            // Log error but continue with other relations
            continue
        }
        
        if checkResp.GetAllowed() {
            permittedActions = append(permittedActions, &authzenv1.Action{
                Name: relationName,
            })
        }
    }
    
    // Apply pagination
    limit := getLimit(req.GetPage())
    offset := 0
    
    if req.GetPage() != nil && req.GetPage().GetToken() != "" {
        token, err := decodePaginationToken(req.GetPage().GetToken())
        if err != nil {
            return nil, fmt.Errorf("invalid pagination token: %w", err)
        }
        offset = token.Offset
    }
    
    start := offset
    end := offset + int(limit)
    if start > len(permittedActions) {
        start = len(permittedActions)
    }
    if end > len(permittedActions) {
        end = len(permittedActions)
    }
    
    pagedActions := permittedActions[start:end]
    
    var nextToken string
    if end < len(permittedActions) {
        nextToken = encodePaginationToken(&PaginationToken{Offset: end})
    }
    
    return &authzenv1.ActionSearchResponse{
        Actions: pagedActions,
        Page: &authzenv1.PageResponse{
            NextToken: nextToken,
            Count:     uint32(len(pagedActions)),
            Total:     uint32Ptr(uint32(len(permittedActions))),
        },
    }, nil
}
```

### Step 4.4: Wire Search Endpoints in Server

**File:** [pkg/server/server.go](pkg/server/server.go)

Add implementations for the new RPC methods:

```go
func (s Server) SubjectSearch(ctx context.Context, req *authzenv1.SubjectSearchRequest) (*authzenv1.SubjectSearchResponse, error) {
    if !s.featureFlagClient.Boolean(serverconfig.ExperimentalEnableAuthZen, req.GetStoreId()) {
        return nil, status.Error(codes.Unimplemented, "AuthZEN endpoints are experimental. Enable with --experimentals=enable_authzen")
    }
    
    ctx, span := tracer.Start(ctx, "authzen.SubjectSearch")
    defer span.End()
    
    if !validator.RequestIsValidatedFromContext(ctx) {
        if err := req.Validate(); err != nil {
            return nil, status.Error(codes.InvalidArgument, err.Error())
        }
    }
    
    ctx = telemetry.ContextWithRPCInfo(ctx, telemetry.RPCInfo{
        Service: s.serviceName,
        Method:  "authzen.SubjectSearch",
    })
    
    cmd := commands.NewSubjectSearchCommand(
        commands.WithListUsersFunc(s.ListUsers),
    )
    
    return cmd.Execute(ctx, req)
}

func (s Server) ResourceSearch(ctx context.Context, req *authzenv1.ResourceSearchRequest) (*authzenv1.ResourceSearchResponse, error) {
    // Similar pattern...
}

func (s Server) ActionSearch(ctx context.Context, req *authzenv1.ActionSearchRequest) (*authzenv1.ActionSearchResponse, error) {
    // Gate behind experimental flag
    if !s.featureFlagClient.Boolean(serverconfig.ExperimentalEnableAuthZen, req.GetStoreId()) {
        return nil, status.Error(codes.Unimplemented, "AuthZEN endpoints are experimental. Enable with --experimentals=enable_authzen")
    }

    ctx, span := tracer.Start(ctx, "authzen.ActionSearch")
    defer span.End()

    // ... validation ...

    // IMPORTANT: Resolve typesystem ONCE to set the response header and get the model ID.
    // This prevents duplicate Openfga-Authorization-Model-Id headers from being set
    // by each Check call (which would otherwise each call resolveTypesystem).
    typesys, err := s.resolveTypesystem(ctx, req.GetStoreId(), req.GetAuthorizationModelId())
    if err != nil {
        return nil, err
    }
    resolvedModelID := typesys.GetAuthorizationModelID()

    // Build the check resolver for this request
    builder := s.getCheckResolverBuilder(req.GetStoreId())
    checkResolver, checkResolverCloser, err := builder.Build()
    if err != nil {
        return nil, err
    }
    defer checkResolverCloser()

    // Create a check function that uses the resolved typesystem directly
    // to avoid re-resolving and setting duplicate headers
    checkFunc := func(ctx context.Context, checkReq *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
        checkQuery := commands.NewCheckCommand(
            s.datastore,
            checkResolver,
            typesys,
            commands.WithCheckCommandLogger(s.logger),
            // ... other options ...
        )

        resp, _, err := checkQuery.Execute(ctx, &commands.CheckCommandParams{
            StoreID:     checkReq.GetStoreId(),
            TupleKey:    checkReq.GetTupleKey(),
            Context:     checkReq.GetContext(),
            Consistency: openfgav1.ConsistencyPreference_UNSPECIFIED,
        })
        if err != nil {
            return nil, err
        }
        return &openfgav1.CheckResponse{Allowed: resp.Allowed}, nil
    }

    // Use a cached typesystem resolver that returns the already-resolved typesystem
    cachedTypesystemResolver := func(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
        return typesys, nil
    }

    req.AuthorizationModelId = resolvedModelID

    query := commands.NewActionSearchQuery(
        commands.WithTypesystemResolver(cachedTypesystemResolver),
        commands.WithCheckFunc(checkFunc),
    )

    return query.Execute(ctx, req)
}
```

---

## Phase 5: PDP Metadata Endpoint (openfga/openfga)

### Step 5.1: Implement GetConfiguration Handler

**File:** `pkg/server/authzen_configuration.go` (NEW)

```go
package server

import (
    "context"
    
    authzenv1 "github.com/openfga/api/proto/authzen/v1"
    "github.com/openfga/openfga/internal/build"
)

func (s Server) GetConfiguration(ctx context.Context, req *authzenv1.GetConfigurationRequest) (*authzenv1.GetConfigurationResponse, error) {
    // Note: This endpoint is NOT gated by experimental flag as it's needed for discovery
    
    ctx, span := tracer.Start(ctx, "authzen.GetConfiguration")
    defer span.End()
    
    return &authzenv1.GetConfigurationResponse{
        PolicyDecisionPoint: &authzenv1.PolicyDecisionPoint{
            Name:        "OpenFGA",
            Version:     build.Version,
            Description: "OpenFGA is a high-performance authorization system implementing the AuthZEN specification",
        },
        AccessEndpoints: &authzenv1.Endpoints{
            Evaluation:     "/stores/{store_id}/access/v1/evaluation",
            Evaluations:    "/stores/{store_id}/access/v1/evaluations",
            SubjectSearch:  "/stores/{store_id}/access/v1/search/subject",
            ResourceSearch: "/stores/{store_id}/access/v1/search/resource",
            ActionSearch:   "/stores/{store_id}/access/v1/search/action",
        },
        Capabilities: []string{
            "evaluation",
            "evaluations",
            "subject_search",
            "resource_search", 
            "action_search",
        },
    }, nil
}
```

---

## Phase 6: Evaluations Semantic Options (openfga/openfga)

### Step 6.1: Update Batch Evaluate Command

**File:** [pkg/server/commands/batch_evaluate.go](pkg/server/commands/batch_evaluate.go)

Add short-circuit logic based on `evaluations_semantic`:

```go
type BatchEvaluateRequestCommand struct {
    batchCheckParams *openfgav1.BatchCheckRequest
    semantic         authzenv1.EvaluationsSemantic
    evaluations      []*authzenv1.EvaluationsItemRequest
    defaults         *EvaluationDefaults
}

type EvaluationDefaults struct {
    Subject  *authzenv1.Subject
    Resource *authzenv1.Resource
    Action   *authzenv1.Action
    Context  *structpb.Struct
}

// For short-circuit semantics, we need to process evaluations sequentially
func (cmd *BatchEvaluateRequestCommand) ShouldShortCircuit() bool {
    return cmd.semantic == authzenv1.EvaluationsSemantic_DENY_ON_FIRST_DENY ||
           cmd.semantic == authzenv1.EvaluationsSemantic_PERMIT_ON_FIRST_PERMIT
}

func (cmd *BatchEvaluateRequestCommand) GetSemantic() authzenv1.EvaluationsSemantic {
    return cmd.semantic
}
```

### Step 6.2: Update Server Evaluations Method

**File:** [pkg/server/server.go](pkg/server/server.go)

Update `Evaluations` to handle short-circuit semantics:

```go
func (s Server) Evaluations(ctx context.Context, req *authzenv1.EvaluationsRequest) (*authzenv1.EvaluationsResponse, error) {
    if !s.featureFlagClient.Boolean(serverconfig.ExperimentalEnableAuthZen, req.GetStoreId()) {
        return nil, status.Error(codes.Unimplemented, "AuthZEN endpoints are experimental")
    }
    
    ctx, span := tracer.Start(ctx, "authzen.Evaluations")
    defer span.End()
    
    // ... validation ...
    
    evalReqCmd := commands.NewBatchEvaluateRequestCommand(req)
    
    // Check if we need short-circuit evaluation
    if evalReqCmd.ShouldShortCircuit() {
        return s.evaluateWithShortCircuit(ctx, req, evalReqCmd)
    }
    
    // Default: batch all evaluations
    batchCheckResponse, err := s.BatchCheck(ctx, evalReqCmd.GetBatchCheckRequests())
    if err != nil {
        return nil, err
    }
    
    return commands.TransformResponse(batchCheckResponse)
}

func (s Server) evaluateWithShortCircuit(
    ctx context.Context, 
    req *authzenv1.EvaluationsRequest,
    cmd *commands.BatchEvaluateRequestCommand,
) (*authzenv1.EvaluationsResponse, error) {
    semantic := cmd.GetSemantic()
    responses := make([]*authzenv1.EvaluationResponse, len(req.GetEvaluations()))
    
    for i, eval := range req.GetEvaluations() {
        // Build single check request from evaluation + defaults
        checkReq := buildCheckRequestFromEvaluation(req, eval)
        
        checkResp, err := s.Check(ctx, checkReq)
        if err != nil {
            responses[i] = &authzenv1.EvaluationResponse{
                Decision: false,
                Context: &authzenv1.EvaluationResponseContext{
                    Error: &authzenv1.ResponseContextError{
                        Status:  500,
                        Message: err.Error(),
                    },
                },
            }
            continue
        }
        
        responses[i] = &authzenv1.EvaluationResponse{
            Decision: checkResp.GetAllowed(),
        }
        
        // Short-circuit logic
        if semantic == authzenv1.EvaluationsSemantic_DENY_ON_FIRST_DENY && !checkResp.GetAllowed() {
            responses[i].Context = &authzenv1.EvaluationResponseContext{
                ReasonAdmin: structpb.NewStringValue("Short-circuited on first deny"),
            }
            // Return only evaluations up to this point
            return &authzenv1.EvaluationsResponse{
                EvaluationResponses: responses[:i+1],
            }, nil
        }
        
        if semantic == authzenv1.EvaluationsSemantic_PERMIT_ON_FIRST_PERMIT && checkResp.GetAllowed() {
            responses[i].Context = &authzenv1.EvaluationResponseContext{
                ReasonAdmin: structpb.NewStringValue("Short-circuited on first permit"),
            }
            return &authzenv1.EvaluationsResponse{
                EvaluationResponses: responses[:i+1],
            }, nil
        }
    }
    
    return &authzenv1.EvaluationsResponse{
        EvaluationResponses: responses,
    }, nil
}
```

---

## Phase 7: Unit Tests (openfga/openfga)

### Step 7.1: Evaluate Command Tests

**File:** [pkg/server/commands/evaluate_test.go](pkg/server/commands/evaluate_test.go)

Expand with comprehensive tests:

```go
func TestEvaluateRequestCommand(t *testing.T) {
    t.Run("basic_transformation", func(t *testing.T) {
        // Test subject/resource/action mapping
    })
    
    t.Run("context_passthrough", func(t *testing.T) {
        // Test that context is passed correctly
    })
    
    t.Run("properties_to_context_merge", func(t *testing.T) {
        // Test subject.properties, resource.properties, action.properties merging
    })
    
    t.Run("properties_precedence", func(t *testing.T) {
        // Test that request context takes precedence over properties
    })
    
    t.Run("missing_required_fields", func(t *testing.T) {
        // Test validation of required fields
    })
    
    t.Run("special_characters_in_ids", func(t *testing.T) {
        // Test handling of special characters in subject/resource IDs
    })
}
```

### Step 7.2: Batch Evaluate Command Tests

**File:** [pkg/server/commands/batch_evaluate_test.go](pkg/server/commands/batch_evaluate_test.go)

Expand with comprehensive tests:

```go
func TestBatchEvaluateRequestCommand(t *testing.T) {
    t.Run("default_value_inheritance", func(t *testing.T) {
        // Test that top-level subject/action/resource/context are inherited
    })
    
    t.Run("per_evaluation_overrides", func(t *testing.T) {
        // Test that per-evaluation values override defaults
    })
    
    t.Run("evaluations_semantic_execute_all", func(t *testing.T) {
        // Test default EXECUTE_ALL behavior
    })
    
    t.Run("evaluations_semantic_deny_on_first_deny", func(t *testing.T) {
        // Test DENY_ON_FIRST_DENY short-circuit
    })
    
    t.Run("evaluations_semantic_permit_on_first_permit", func(t *testing.T) {
        // Test PERMIT_ON_FIRST_PERMIT short-circuit
    })
    
    t.Run("response_ordering", func(t *testing.T) {
        // Test that response order matches request order
    })
    
    t.Run("individual_evaluation_errors", func(t *testing.T) {
        // Test error handling per evaluation
    })
    
    t.Run("properties_merge_with_defaults", func(t *testing.T) {
        // Test properties merging across default and per-evaluation
    })
}
```

### Step 7.3: Search Command Tests

**Files:** (NEW)
- `pkg/server/commands/subject_search_test.go`
- `pkg/server/commands/resource_search_test.go`
- `pkg/server/commands/action_search_test.go`

```go
// subject_search_test.go
func TestSubjectSearchCommand(t *testing.T) {
    t.Run("basic_transformation", func(t *testing.T) { /* ... */ })
    t.Run("pagination_initial_request", func(t *testing.T) { /* ... */ })
    t.Run("pagination_continuation", func(t *testing.T) { /* ... */ })
    t.Run("pagination_invalid_token", func(t *testing.T) { /* ... */ })
    t.Run("limit_handling", func(t *testing.T) { /* ... */ })
    t.Run("properties_to_context", func(t *testing.T) { /* ... */ })
    t.Run("empty_result_set", func(t *testing.T) { /* ... */ })
    t.Run("subject_type_filter", func(t *testing.T) { /* ... */ })
}

// Similar patterns for resource_search_test.go and action_search_test.go
```

### Step 7.4: PDP Metadata Tests

**File:** `pkg/server/authzen_configuration_test.go` (NEW)

```go
func TestGetConfiguration(t *testing.T) {
    t.Run("returns_correct_endpoints", func(t *testing.T) { /* ... */ })
    t.Run("returns_version", func(t *testing.T) { /* ... */ })
    t.Run("returns_capabilities", func(t *testing.T) { /* ... */ })
    t.Run("format_compliance", func(t *testing.T) { /* ... */ })
}
```

---

## Phase 8: Integration Tests (openfga/openfga)

### Step 8.1: Create AuthZEN Test Directory

Create directory structure:
```
tests/
└── authzen/
    ├── authzen_test.go      # Shared test utilities
    ├── evaluation_test.go    # Evaluation endpoint tests
    ├── evaluations_test.go   # Evaluations endpoint tests
    ├── search_test.go        # Search endpoint tests
    ├── metadata_test.go      # /.well-known/authzen-configuration tests
    └── interop_test.go       # Official interop test vectors
```

### Step 8.2: Evaluation Integration Tests

**File:** `tests/authzen/evaluation_test.go` (NEW)

```go
package authzen

import (
    "testing"
    // ...
)

func TestEvaluation(t *testing.T) {
    engines := []string{"memory", "postgres", "mysql", "sqlite"}
    
    for _, engine := range engines {
        t.Run(engine, func(t *testing.T) {
            client := buildAuthZenClient(t, engine, []string{"enable_authzen"})
            
            t.Run("basic_permit", func(t *testing.T) { /* ... */ })
            t.Run("basic_deny", func(t *testing.T) { /* ... */ })
            t.Run("with_abac_conditions", func(t *testing.T) { /* ... */ })
            t.Run("x_request_id_propagation", func(t *testing.T) { /* ... */ })
            t.Run("authentication_required", func(t *testing.T) { /* ... */ })
            t.Run("invalid_request_400", func(t *testing.T) { /* ... */ })
            t.Run("missing_store_404", func(t *testing.T) { /* ... */ })
            t.Run("properties_in_conditions", func(t *testing.T) { /* ... */ })
        })
    }
}
```

### Step 8.3: Search Integration Tests

**File:** `tests/authzen/search_test.go` (NEW)

```go
func TestSearch(t *testing.T) {
    engines := []string{"memory", "postgres", "mysql", "sqlite"}
    
    for _, engine := range engines {
        t.Run(engine, func(t *testing.T) {
            client := buildAuthZenClient(t, engine, []string{"enable_authzen"})
            
            t.Run("SubjectSearch", func(t *testing.T) {
                t.Run("initial_page", func(t *testing.T) { /* ... */ })
                t.Run("pagination_flow", func(t *testing.T) { /* ... */ })
                t.Run("final_page_empty_token", func(t *testing.T) { /* ... */ })
                t.Run("transitive_relationships", func(t *testing.T) { /* ... */ })
            })
            
            t.Run("ResourceSearch", func(t *testing.T) { /* similar */ })
            t.Run("ActionSearch", func(t *testing.T) { /* similar */ })
        })
    }
}
```

### Step 8.4: Interop Tests

**File:** `tests/authzen/interop_test.go` (NEW)

```go
func TestAuthZenInterop(t *testing.T) {
    t.Run("TodoScenario", func(t *testing.T) {
        // Load model from docs/authzen/authzen-todo.fga
        // Load tuples from docs/authzen/authzen-todo.tuples.yaml
        // Run test vectors from docs/authzen/authzen-todo.tests.yaml
    })
    
    t.Run("GatewayScenario", func(t *testing.T) {
        // Load model from docs/authzen/authzen-gateway.fga
        // Load tuples from docs/authzen/authzen-gateway.tuples.yaml
        // Run test vectors from docs/authzen/authzen-gateway.tests.yaml
    })
}
```

---

## Phase 9: Configuration Options (openfga/openfga)

### Step 9.1: Add AuthZEN Configuration

**File:** [pkg/server/config/config.go](pkg/server/config/config.go)

```go
// AuthZenConfig defines configuration for AuthZEN endpoints
type AuthZenConfig struct {
    // MaxSearchResults limits the number of results for search operations
    // to prevent memory issues. Default: 1000
    MaxSearchResults uint32
    
    // MaxActionsPerSearch limits the number of relations checked in ActionSearch
    // Default: 100
    MaxActionsPerSearch uint32
}
```

### Step 9.2: Add CLI Flags

**File:** [cmd/run/run.go](cmd/run/run.go)

```go
flags.Uint32("authzen-max-search-results", 1000, "Maximum number of results for AuthZEN search operations")
flags.Uint32("authzen-max-actions-per-search", 100, "Maximum number of relations to check in AuthZEN ActionSearch")
```

---

## Implementation Order

### Week 1: Foundation (openfga/api + openfga/openfga)
1. ✅ Phase 1: Experimental Flag & Gating
2. ✅ Phase 2: Proto Definitions (all search, metadata, options)

### Week 2: Core Implementation (openfga/openfga)
3. ✅ Phase 3: Properties-to-Context Mapping
4. ✅ Phase 5: PDP Metadata Endpoint
5. ✅ Phase 6: Evaluations Semantic Options

### Week 3: Search APIs (openfga/openfga)
6. ✅ Phase 4: Search API Implementation (Subject, Resource, Action)

### Week 4: Testing (openfga/openfga)
7. ✅ Phase 7: Unit Tests
8. ✅ Phase 8: Integration Tests
9. ✅ Phase 9: Configuration Options

---

## Performance Considerations

### Pagination Memory Usage
Since `ListObjects`/`ListUsers` don't support native pagination, all results are collected in memory before slicing. Mitigations:
- Add `authzen-max-search-results` config option (default: 1000)
- Return error if result count exceeds limit
- Document pagination as "in-memory" in API docs

### Action Search Performance
Iterating all relations and calling Check for each can be expensive. Mitigations:
- Add `authzen-max-actions-per-search` limit (default: 100)
- Use concurrent Check calls with bounded concurrency
- Cache Check results within the request context
- Document performance implications

### Properties Namespace Conflicts
When `context` has a key that conflicts with a property key:
- **Decision:** Request-level `context` takes precedence
- **Implementation:** Merge properties first, then overlay context
- **Documentation:** Add to API docs explaining precedence rules

---

## Files Changed/Created Summary

### openfga/api Repository
| File | Action | Description |
|------|--------|-------------|
| `authzen/v1/authzen_service.proto` | Modify | Add Search RPCs, PDP metadata, pagination messages, evaluations_semantic |

### openfga/openfga Repository  
| File | Action | Description |
|------|--------|-------------|
| `pkg/server/config/config.go` | Modify | Add `ExperimentalEnableAuthZen` constant |
| `cmd/run/run.go` | Modify | Add flag to allowed experimentals list |
| `pkg/server/server.go` | Modify | Gate endpoints, add new RPC implementations |
| `pkg/server/commands/authzen_utils.go` | Create | Context merging utilities |
| `pkg/server/commands/evaluate.go` | Modify | Add properties-to-context merging |
| `pkg/server/commands/batch_evaluate.go` | Modify | Add semantic options, properties merging |
| `pkg/server/commands/subject_search.go` | Create | Subject search handler |
| `pkg/server/commands/resource_search.go` | Create | Resource search handler |
| `pkg/server/commands/action_search.go` | Create | Action search handler |
| `pkg/server/authzen_configuration.go` | Create | PDP metadata handler |
| `pkg/server/commands/evaluate_test.go` | Modify | Expand tests |
| `pkg/server/commands/batch_evaluate_test.go` | Modify | Expand tests |
| `pkg/server/commands/subject_search_test.go` | Create | Unit tests |
| `pkg/server/commands/resource_search_test.go` | Create | Unit tests |
| `pkg/server/commands/action_search_test.go` | Create | Unit tests |
| `pkg/server/authzen_configuration_test.go` | Create | Unit tests |
| `tests/authzen/authzen_test.go` | Create | Shared test utilities |
| `tests/authzen/evaluation_test.go` | Create | Integration tests |
| `tests/authzen/evaluations_test.go` | Create | Integration tests |
| `tests/authzen/search_test.go` | Create | Integration tests |
| `tests/authzen/metadata_test.go` | Create | Integration tests |
| `tests/authzen/interop_test.go` | Create | Interop test vectors |
