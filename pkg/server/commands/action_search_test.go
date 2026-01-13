package commands

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestActionSearchQuery(t *testing.T) {
	t.Run("basic_action_search", func(t *testing.T) {
		// Mock typesystem resolver that returns a type with 3 relations
		mockTypesystemResolver := func(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
			model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type document
					relations
						define reader: [user]
						define writer: [user]
						define owner: [user]
			`)
			ts, err := typesystem.New(model)
			require.NoError(t, err)
			return ts, nil
		}

		// Mock check function - allow read and owner, deny write
		mockCheck := func(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
			allowed := req.GetTupleKey().GetRelation() == "reader" || req.GetTupleKey().GetRelation() == "owner"
			return &openfgav1.CheckResponse{Allowed: allowed}, nil
		}

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithCheckFunc(mockCheck),
		)

		req := &authzenv1.ActionSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp.GetActions(), 2) // reader and owner

		// Actions should be sorted alphabetically
		actionNames := make([]string, len(resp.GetActions()))
		for i, a := range resp.GetActions() {
			actionNames[i] = a.GetName()
		}
		require.Contains(t, actionNames, "reader")
		require.Contains(t, actionNames, "owner")
		require.NotContains(t, actionNames, "writer")
	})

	t.Run("no_permitted_actions", func(t *testing.T) {
		mockTypesystemResolver := func(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
			model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type document
					relations
						define reader: [user]
						define writer: [user]
			`)
			ts, err := typesystem.New(model)
			require.NoError(t, err)
			return ts, nil
		}

		// Deny all checks
		mockCheck := func(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
			return &openfgav1.CheckResponse{Allowed: false}, nil
		}

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithCheckFunc(mockCheck),
		)

		req := &authzenv1.ActionSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Empty(t, resp.GetActions())
		require.Equal(t, uint32(0), resp.GetPage().GetCount())
		require.Equal(t, uint32(0), resp.GetPage().GetTotal())
	})

	t.Run("pagination_initial_request", func(t *testing.T) {
		// Create a type with many relations
		mockTypesystemResolver := func(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
			model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type document
					relations
						define action1: [user]
						define action2: [user]
						define action3: [user]
						define action4: [user]
						define action5: [user]
			`)
			ts, err := typesystem.New(model)
			require.NoError(t, err)
			return ts, nil
		}

		// Allow all checks
		mockCheck := func(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
			return &openfgav1.CheckResponse{Allowed: true}, nil
		}

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithCheckFunc(mockCheck),
		)

		limit := uint32(2)
		req := &authzenv1.ActionSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
			Page:     &authzenv1.PageRequest{Limit: &limit},
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp.GetActions(), 2)
		require.NotEmpty(t, resp.GetPage().GetNextToken())
		require.Equal(t, uint32(2), resp.GetPage().GetCount())
		require.Equal(t, uint32(5), resp.GetPage().GetTotal())
		// Verify first page actions are sorted (action1, action2)
		require.Equal(t, "action1", resp.GetActions()[0].GetName())
		require.Equal(t, "action2", resp.GetActions()[1].GetName())
	})

	t.Run("pagination_continuation", func(t *testing.T) {
		mockTypesystemResolver := func(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
			model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type document
					relations
						define action1: [user]
						define action2: [user]
						define action3: [user]
						define action4: [user]
						define action5: [user]
			`)
			ts, err := typesystem.New(model)
			require.NoError(t, err)
			return ts, nil
		}

		mockCheck := func(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
			return &openfgav1.CheckResponse{Allowed: true}, nil
		}

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithCheckFunc(mockCheck),
		)

		limit := uint32(2)
		// First request
		req := &authzenv1.ActionSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
			Page:     &authzenv1.PageRequest{Limit: &limit},
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.NotEmpty(t, resp.GetPage().GetNextToken())

		// Continue with token
		req.Page.Token = &resp.Page.NextToken
		resp2, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp2.GetActions(), 2)
		require.Equal(t, "action3", resp2.GetActions()[0].GetName())
		require.Equal(t, "action4", resp2.GetActions()[1].GetName())
	})

	t.Run("pagination_last_page", func(t *testing.T) {
		mockTypesystemResolver := func(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
			model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type document
					relations
						define action1: [user]
						define action2: [user]
						define action3: [user]
						define action4: [user]
						define action5: [user]
			`)
			ts, err := typesystem.New(model)
			require.NoError(t, err)
			return ts, nil
		}

		mockCheck := func(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
			return &openfgav1.CheckResponse{Allowed: true}, nil
		}

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithCheckFunc(mockCheck),
		)

		limit := uint32(2)
		// First request - get 2 of 5
		req := &authzenv1.ActionSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
			Page:     &authzenv1.PageRequest{Limit: &limit},
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp.GetActions(), 2)
		require.NotEmpty(t, resp.GetPage().GetNextToken())

		// Second request - get 2 more
		req.Page.Token = &resp.Page.NextToken
		resp2, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp2.GetActions(), 2)
		require.NotEmpty(t, resp2.GetPage().GetNextToken())

		// Third request - get remaining 1
		req.Page.Token = &resp2.Page.NextToken
		resp3, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp3.GetActions(), 1)
		require.Empty(t, resp3.GetPage().GetNextToken()) // No more pages
		require.Equal(t, "action5", resp3.GetActions()[0].GetName())
	})

	t.Run("pagination_invalid_token", func(t *testing.T) {
		mockTypesystemResolver := func(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
			model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type document
					relations
						define reader: [user]
			`)
			ts, err := typesystem.New(model)
			require.NoError(t, err)
			return ts, nil
		}

		mockCheck := func(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
			return &openfgav1.CheckResponse{Allowed: true}, nil
		}

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithCheckFunc(mockCheck),
		)

		invalidToken := "not-valid-base64-!@#"
		req := &authzenv1.ActionSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
			Page:     &authzenv1.PageRequest{Token: &invalidToken},
		}

		_, err := query.Execute(context.Background(), req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid pagination token")
	})

	t.Run("pagination_invalid_json_token", func(t *testing.T) {
		mockTypesystemResolver := func(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
			model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type document
					relations
						define reader: [user]
			`)
			ts, err := typesystem.New(model)
			require.NoError(t, err)
			return ts, nil
		}

		mockCheck := func(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
			return &openfgav1.CheckResponse{Allowed: true}, nil
		}

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithCheckFunc(mockCheck),
		)

		// Valid base64 but invalid JSON
		invalidToken := "bm90LXZhbGlkLWpzb24=" // "not-valid-json" in base64
		req := &authzenv1.ActionSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
			Page:     &authzenv1.PageRequest{Token: &invalidToken},
		}

		_, err := query.Execute(context.Background(), req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid pagination token")
	})

	t.Run("properties_to_context_subject", func(t *testing.T) {
		mockTypesystemResolver := func(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
			model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type document
					relations
						define reader: [user]
			`)
			ts, err := typesystem.New(model)
			require.NoError(t, err)
			return ts, nil
		}

		var capturedReq *openfgav1.CheckRequest
		mockCheck := func(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
			capturedReq = req
			return &openfgav1.CheckResponse{Allowed: true}, nil
		}

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithCheckFunc(mockCheck),
		)

		req := &authzenv1.ActionSearchRequest{
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"role": "admin"}),
			},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, capturedReq.GetContext())
		require.Equal(t, "admin", capturedReq.GetContext().AsMap()["subject_role"])
	})

	t.Run("properties_to_context_resource", func(t *testing.T) {
		mockTypesystemResolver := func(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
			model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type document
					relations
						define reader: [user]
			`)
			ts, err := typesystem.New(model)
			require.NoError(t, err)
			return ts, nil
		}

		var capturedReq *openfgav1.CheckRequest
		mockCheck := func(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
			capturedReq = req
			return &openfgav1.CheckResponse{Allowed: true}, nil
		}

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithCheckFunc(mockCheck),
		)

		req := &authzenv1.ActionSearchRequest{
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{
				Type:       "document",
				Id:         "doc1",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"classification": "secret"}),
			},
			StoreId: "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, capturedReq.GetContext())
		require.Equal(t, "secret", capturedReq.GetContext().AsMap()["resource_classification"])
	})

	t.Run("properties_to_context_combined", func(t *testing.T) {
		mockTypesystemResolver := func(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
			model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type document
					relations
						define reader: [user]
			`)
			ts, err := typesystem.New(model)
			require.NoError(t, err)
			return ts, nil
		}

		var capturedReq *openfgav1.CheckRequest
		mockCheck := func(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
			capturedReq = req
			return &openfgav1.CheckResponse{Allowed: true}, nil
		}

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithCheckFunc(mockCheck),
		)

		req := &authzenv1.ActionSearchRequest{
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"department": "engineering"}),
			},
			Resource: &authzenv1.Resource{
				Type:       "document",
				Id:         "doc1",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"owner_dept": "engineering"}),
			},
			StoreId: "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, capturedReq.GetContext())
		contextMap := capturedReq.GetContext().AsMap()
		require.Equal(t, "engineering", contextMap["subject_department"])
		require.Equal(t, "engineering", contextMap["resource_owner_dept"])
	})

	t.Run("typesystem_resolver_error", func(t *testing.T) {
		mockTypesystemResolver := func(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
			return nil, errors.New("failed to resolve typesystem")
		}

		mockCheck := func(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
			return &openfgav1.CheckResponse{Allowed: true}, nil
		}

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithCheckFunc(mockCheck),
		)

		req := &authzenv1.ActionSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to resolve typesystem")
	})

	t.Run("invalid_resource_type", func(t *testing.T) {
		mockTypesystemResolver := func(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
			model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type document
					relations
						define reader: [user]
			`)
			ts, err := typesystem.New(model)
			require.NoError(t, err)
			return ts, nil
		}

		mockCheck := func(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
			return &openfgav1.CheckResponse{Allowed: true}, nil
		}

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithCheckFunc(mockCheck),
		)

		req := &authzenv1.ActionSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "unknown_type", Id: "doc1"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to get relations for type unknown_type")
	})

	t.Run("check_function_error_continues", func(t *testing.T) {
		mockTypesystemResolver := func(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
			model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type document
					relations
						define reader: [user]
						define writer: [user]
						define owner: [user]
			`)
			ts, err := typesystem.New(model)
			require.NoError(t, err)
			return ts, nil
		}

		// Error on "writer" relation, allow others
		mockCheck := func(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
			if req.GetTupleKey().GetRelation() == "writer" {
				return nil, errors.New("check failed")
			}
			return &openfgav1.CheckResponse{Allowed: true}, nil
		}

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithCheckFunc(mockCheck),
		)

		req := &authzenv1.ActionSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		// Should still get 2 results (reader and owner), skipping writer which errored
		require.Len(t, resp.GetActions(), 2)

		actionNames := make([]string, len(resp.GetActions()))
		for i, a := range resp.GetActions() {
			actionNames[i] = a.GetName()
		}
		require.Contains(t, actionNames, "reader")
		require.Contains(t, actionNames, "owner")
		require.NotContains(t, actionNames, "writer")
	})

	t.Run("all_relations_sorted_alphabetically", func(t *testing.T) {
		mockTypesystemResolver := func(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
			model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type document
					relations
						define zulu: [user]
						define alpha: [user]
						define mike: [user]
						define bravo: [user]
			`)
			ts, err := typesystem.New(model)
			require.NoError(t, err)
			return ts, nil
		}

		mockCheck := func(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
			return &openfgav1.CheckResponse{Allowed: true}, nil
		}

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithCheckFunc(mockCheck),
		)

		req := &authzenv1.ActionSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp.GetActions(), 4)
		// Verify alphabetical order
		require.Equal(t, "alpha", resp.GetActions()[0].GetName())
		require.Equal(t, "bravo", resp.GetActions()[1].GetName())
		require.Equal(t, "mike", resp.GetActions()[2].GetName())
		require.Equal(t, "zulu", resp.GetActions()[3].GetName())
	})

	t.Run("authorization_model_id_forwarded", func(t *testing.T) {
		var capturedStoreID, capturedModelID string
		mockTypesystemResolver := func(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
			capturedStoreID = storeID
			capturedModelID = modelID
			model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type document
					relations
						define reader: [user]
			`)
			ts, err := typesystem.New(model)
			require.NoError(t, err)
			return ts, nil
		}

		var capturedCheckReq *openfgav1.CheckRequest
		mockCheck := func(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
			capturedCheckReq = req
			return &openfgav1.CheckResponse{Allowed: true}, nil
		}

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithCheckFunc(mockCheck),
		)

		req := &authzenv1.ActionSearchRequest{
			Subject:              &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource:             &authzenv1.Resource{Type: "document", Id: "doc1"},
			StoreId:              "01HVMMBCMGZNT3SED4CT2KA89Q",
			AuthorizationModelId: "01HVMMBCMGZNT3SED4CT2KA90X",
		}

		_, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, "01HVMMBCMGZNT3SED4CT2KA89Q", capturedStoreID)
		require.Equal(t, "01HVMMBCMGZNT3SED4CT2KA90X", capturedModelID)
		require.Equal(t, "01HVMMBCMGZNT3SED4CT2KA89Q", capturedCheckReq.GetStoreId())
		require.Equal(t, "01HVMMBCMGZNT3SED4CT2KA90X", capturedCheckReq.GetAuthorizationModelId())
	})

	t.Run("check_request_parameters", func(t *testing.T) {
		mockTypesystemResolver := func(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
			model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type document
					relations
						define reader: [user]
			`)
			ts, err := typesystem.New(model)
			require.NoError(t, err)
			return ts, nil
		}

		var capturedReq *openfgav1.CheckRequest
		mockCheck := func(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
			capturedReq = req
			return &openfgav1.CheckResponse{Allowed: true}, nil
		}

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithCheckFunc(mockCheck),
		)

		req := &authzenv1.ActionSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.NoError(t, err)

		// Verify check request parameters
		require.Equal(t, "user:alice", capturedReq.GetTupleKey().GetUser())
		require.Equal(t, "document:doc1", capturedReq.GetTupleKey().GetObject())
		require.Equal(t, "reader", capturedReq.GetTupleKey().GetRelation())
	})

	t.Run("large_number_of_relations", func(t *testing.T) {
		// Generate DSL with many relations
		dsl := `
			model
				schema 1.1
			type user
			type document
				relations
`
		for i := 0; i < 20; i++ {
			dsl += fmt.Sprintf("\t\t\t\t\tdefine relation%02d: [user]\n", i)
		}

		mockTypesystemResolver := func(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
			model := testutils.MustTransformDSLToProtoWithID(dsl)
			ts, err := typesystem.New(model)
			require.NoError(t, err)
			return ts, nil
		}

		// Allow half the relations
		mockCheck := func(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
			// Allow even-numbered relations
			relation := req.GetTupleKey().GetRelation()
			var num int
			fmt.Sscanf(relation, "relation%02d", &num)
			return &openfgav1.CheckResponse{Allowed: num%2 == 0}, nil
		}

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithCheckFunc(mockCheck),
		)

		req := &authzenv1.ActionSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp.GetActions(), 10) // Half of 20 relations
	})
}
