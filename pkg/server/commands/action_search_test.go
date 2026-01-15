package commands

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
)

// mockBatchCheckFunc creates a mock BatchCheck function that applies the given logic to each check.
func mockBatchCheckFunc(checkLogic func(tupleKey *openfgav1.CheckRequestTupleKey) (bool, error)) func(ctx context.Context, req *openfgav1.BatchCheckRequest) (*openfgav1.BatchCheckResponse, error) {
	return func(ctx context.Context, req *openfgav1.BatchCheckRequest) (*openfgav1.BatchCheckResponse, error) {
		results := make(map[string]*openfgav1.BatchCheckSingleResult)
		for _, check := range req.GetChecks() {
			allowed, err := checkLogic(check.GetTupleKey())
			if err != nil {
				results[check.GetCorrelationId()] = &openfgav1.BatchCheckSingleResult{
					CheckResult: &openfgav1.BatchCheckSingleResult_Error{
						Error: &openfgav1.CheckError{Message: err.Error()},
					},
				}
			} else {
				results[check.GetCorrelationId()] = &openfgav1.BatchCheckSingleResult{
					CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: allowed},
				}
			}
		}
		return &openfgav1.BatchCheckResponse{Result: results}, nil
	}
}

func TestActionSearchQuery(t *testing.T) {
	t.Run("basic_action_search", func(t *testing.T) {
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

		mockBatchCheck := mockBatchCheckFunc(func(tupleKey *openfgav1.CheckRequestTupleKey) (bool, error) {
			relation := tupleKey.GetRelation()
			return relation == "reader" || relation == "owner", nil
		})

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithBatchCheckFunc(mockBatchCheck),
		)

		req := &authzenv1.ActionSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp.GetActions(), 2)

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

		mockBatchCheck := mockBatchCheckFunc(func(tupleKey *openfgav1.CheckRequestTupleKey) (bool, error) {
			return false, nil
		})

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithBatchCheckFunc(mockBatchCheck),
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

		mockBatchCheck := mockBatchCheckFunc(func(tupleKey *openfgav1.CheckRequestTupleKey) (bool, error) {
			return true, nil
		})

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithBatchCheckFunc(mockBatchCheck),
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

		mockBatchCheck := mockBatchCheckFunc(func(tupleKey *openfgav1.CheckRequestTupleKey) (bool, error) {
			return true, nil
		})

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithBatchCheckFunc(mockBatchCheck),
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
		require.NotEmpty(t, resp.GetPage().GetNextToken())

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

		mockBatchCheck := mockBatchCheckFunc(func(tupleKey *openfgav1.CheckRequestTupleKey) (bool, error) {
			return true, nil
		})

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithBatchCheckFunc(mockBatchCheck),
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

		req.Page.Token = &resp.Page.NextToken
		resp2, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp2.GetActions(), 2)
		require.NotEmpty(t, resp2.GetPage().GetNextToken())

		req.Page.Token = &resp2.Page.NextToken
		resp3, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp3.GetActions(), 1)
		require.Empty(t, resp3.GetPage().GetNextToken())
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

		mockBatchCheck := mockBatchCheckFunc(func(tupleKey *openfgav1.CheckRequestTupleKey) (bool, error) {
			return true, nil
		})

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithBatchCheckFunc(mockBatchCheck),
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

		mockBatchCheck := mockBatchCheckFunc(func(tupleKey *openfgav1.CheckRequestTupleKey) (bool, error) {
			return true, nil
		})

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithBatchCheckFunc(mockBatchCheck),
		)

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

		var capturedReq *openfgav1.BatchCheckRequest
		mockBatchCheck := func(ctx context.Context, req *openfgav1.BatchCheckRequest) (*openfgav1.BatchCheckResponse, error) {
			capturedReq = req
			results := make(map[string]*openfgav1.BatchCheckSingleResult)
			for _, check := range req.GetChecks() {
				results[check.GetCorrelationId()] = &openfgav1.BatchCheckSingleResult{
					CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: true},
				}
			}
			return &openfgav1.BatchCheckResponse{Result: results}, nil
		}

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithBatchCheckFunc(mockBatchCheck),
		)

		req := &authzenv1.ActionSearchRequest{
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]any{"role": "admin"}),
			},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, capturedReq)
		require.Len(t, capturedReq.GetChecks(), 1)
		require.NotNil(t, capturedReq.GetChecks()[0].GetContext())
		require.Equal(t, "admin", capturedReq.GetChecks()[0].GetContext().AsMap()["subject_role"])
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

		var capturedReq *openfgav1.BatchCheckRequest
		mockBatchCheck := func(ctx context.Context, req *openfgav1.BatchCheckRequest) (*openfgav1.BatchCheckResponse, error) {
			capturedReq = req
			results := make(map[string]*openfgav1.BatchCheckSingleResult)
			for _, check := range req.GetChecks() {
				results[check.GetCorrelationId()] = &openfgav1.BatchCheckSingleResult{
					CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: true},
				}
			}
			return &openfgav1.BatchCheckResponse{Result: results}, nil
		}

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithBatchCheckFunc(mockBatchCheck),
		)

		req := &authzenv1.ActionSearchRequest{
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{
				Type:       "document",
				Id:         "doc1",
				Properties: testutils.MustNewStruct(t, map[string]any{"classification": "secret"}),
			},
			StoreId: "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, capturedReq)
		require.Len(t, capturedReq.GetChecks(), 1)
		require.NotNil(t, capturedReq.GetChecks()[0].GetContext())
		require.Equal(t, "secret", capturedReq.GetChecks()[0].GetContext().AsMap()["resource_classification"])
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

		var capturedReq *openfgav1.BatchCheckRequest
		mockBatchCheck := func(ctx context.Context, req *openfgav1.BatchCheckRequest) (*openfgav1.BatchCheckResponse, error) {
			capturedReq = req
			results := make(map[string]*openfgav1.BatchCheckSingleResult)
			for _, check := range req.GetChecks() {
				results[check.GetCorrelationId()] = &openfgav1.BatchCheckSingleResult{
					CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: true},
				}
			}
			return &openfgav1.BatchCheckResponse{Result: results}, nil
		}

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithBatchCheckFunc(mockBatchCheck),
		)

		req := &authzenv1.ActionSearchRequest{
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]any{"department": "engineering"}),
			},
			Resource: &authzenv1.Resource{
				Type:       "document",
				Id:         "doc1",
				Properties: testutils.MustNewStruct(t, map[string]any{"owner_dept": "engineering"}),
			},
			StoreId: "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, capturedReq)
		require.Len(t, capturedReq.GetChecks(), 1)
		require.NotNil(t, capturedReq.GetChecks()[0].GetContext())
		contextMap := capturedReq.GetChecks()[0].GetContext().AsMap()
		require.Equal(t, "engineering", contextMap["subject_department"])
		require.Equal(t, "engineering", contextMap["resource_owner_dept"])
	})

	t.Run("typesystem_resolver_error", func(t *testing.T) {
		mockTypesystemResolver := func(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
			return nil, errors.New("failed to resolve typesystem")
		}

		mockBatchCheck := mockBatchCheckFunc(func(tupleKey *openfgav1.CheckRequestTupleKey) (bool, error) {
			return true, nil
		})

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithBatchCheckFunc(mockBatchCheck),
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

		mockBatchCheck := mockBatchCheckFunc(func(tupleKey *openfgav1.CheckRequestTupleKey) (bool, error) {
			return true, nil
		})

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithBatchCheckFunc(mockBatchCheck),
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

		mockBatchCheck := mockBatchCheckFunc(func(tupleKey *openfgav1.CheckRequestTupleKey) (bool, error) {
			if tupleKey.GetRelation() == "writer" {
				return false, errors.New("check failed")
			}
			return true, nil
		})

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithBatchCheckFunc(mockBatchCheck),
		)

		req := &authzenv1.ActionSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
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

		mockBatchCheck := mockBatchCheckFunc(func(tupleKey *openfgav1.CheckRequestTupleKey) (bool, error) {
			return true, nil
		})

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithBatchCheckFunc(mockBatchCheck),
		)

		req := &authzenv1.ActionSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp.GetActions(), 4)
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

		var capturedBatchReq *openfgav1.BatchCheckRequest
		mockBatchCheck := func(ctx context.Context, req *openfgav1.BatchCheckRequest) (*openfgav1.BatchCheckResponse, error) {
			capturedBatchReq = req
			results := make(map[string]*openfgav1.BatchCheckSingleResult)
			for _, check := range req.GetChecks() {
				results[check.GetCorrelationId()] = &openfgav1.BatchCheckSingleResult{
					CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: true},
				}
			}
			return &openfgav1.BatchCheckResponse{Result: results}, nil
		}

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithBatchCheckFunc(mockBatchCheck),
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
		require.Equal(t, "01HVMMBCMGZNT3SED4CT2KA89Q", capturedBatchReq.GetStoreId())
		require.Equal(t, "01HVMMBCMGZNT3SED4CT2KA90X", capturedBatchReq.GetAuthorizationModelId())
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

		var capturedReq *openfgav1.BatchCheckRequest
		mockBatchCheck := func(ctx context.Context, req *openfgav1.BatchCheckRequest) (*openfgav1.BatchCheckResponse, error) {
			capturedReq = req
			results := make(map[string]*openfgav1.BatchCheckSingleResult)
			for _, check := range req.GetChecks() {
				results[check.GetCorrelationId()] = &openfgav1.BatchCheckSingleResult{
					CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: true},
				}
			}
			return &openfgav1.BatchCheckResponse{Result: results}, nil
		}

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithBatchCheckFunc(mockBatchCheck),
		)

		req := &authzenv1.ActionSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.NoError(t, err)

		require.Len(t, capturedReq.GetChecks(), 1)
		check := capturedReq.GetChecks()[0]
		require.Equal(t, "user:alice", check.GetTupleKey().GetUser())
		require.Equal(t, "document:doc1", check.GetTupleKey().GetObject())
		require.Equal(t, "reader", check.GetTupleKey().GetRelation())
	})

	t.Run("large_number_of_relations", func(t *testing.T) {
		var dslBuilder strings.Builder
		dslBuilder.WriteString(`
			model
				schema 1.1
			type user
			type document
				relations
`)
		for i := range 20 {
			fmt.Fprintf(&dslBuilder, "\t\t\t\t\tdefine relation%02d: [user]\n", i)
		}

		mockTypesystemResolver := func(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
			model := testutils.MustTransformDSLToProtoWithID(dslBuilder.String())
			ts, err := typesystem.New(model)
			require.NoError(t, err)
			return ts, nil
		}

		mockBatchCheck := mockBatchCheckFunc(func(tupleKey *openfgav1.CheckRequestTupleKey) (bool, error) {
			relation := tupleKey.GetRelation()
			var num int
			fmt.Sscanf(relation, "relation%02d", &num)
			return num%2 == 0, nil
		})

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithBatchCheckFunc(mockBatchCheck),
		)

		req := &authzenv1.ActionSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp.GetActions(), 10)
	})

	t.Run("batch_check_error_propagates", func(t *testing.T) {
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

		mockBatchCheck := func(ctx context.Context, req *openfgav1.BatchCheckRequest) (*openfgav1.BatchCheckResponse, error) {
			return nil, errors.New("batch check failed")
		}

		query := NewActionSearchQuery(
			WithTypesystemResolver(mockTypesystemResolver),
			WithBatchCheckFunc(mockBatchCheck),
		)

		req := &authzenv1.ActionSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "batch check failed")
	})
}
