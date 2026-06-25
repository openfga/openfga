package listusers

import (
	"context"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func newValidateTypesystem(t *testing.T) *typesystem.TypeSystem {
	t.Helper()
	model := parser.MustTransformDSLToProto(`
model
  schema 1.1
type user
type document
  relations
    define viewer: [user]
`)
	typesys, err := typesystem.New(model)
	require.NoError(t, err)
	return typesys
}

func baseRequest() *openfgav1.ListUsersRequest {
	return &openfgav1.ListUsersRequest{
		Object:      &openfgav1.Object{Type: "document", Id: "1"},
		Relation:    "viewer",
		UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
	}
}

func TestValidateListUsersRequest(t *testing.T) {
	ctx := context.Background()
	typesys := newValidateTypesystem(t)

	t.Run("valid_request", func(t *testing.T) {
		require.NoError(t, ValidateListUsersRequest(ctx, baseRequest(), typesys))
	})

	t.Run("target_relation_not_found", func(t *testing.T) {
		req := baseRequest()
		req.Relation = "nonexistent"
		err := ValidateListUsersRequest(ctx, req, typesys)
		require.Error(t, err)
	})

	t.Run("target_object_type_undefined", func(t *testing.T) {
		req := baseRequest()
		req.Object = &openfgav1.Object{Type: "folder", Id: "1"}
		err := ValidateListUsersRequest(ctx, req, typesys)
		require.Error(t, err)
	})

	t.Run("user_filter_type_not_found", func(t *testing.T) {
		req := baseRequest()
		req.UserFilters = []*openfgav1.UserTypeFilter{{Type: "group"}}
		err := ValidateListUsersRequest(ctx, req, typesys)
		require.Error(t, err)
	})

	t.Run("user_filter_relation_not_found", func(t *testing.T) {
		req := baseRequest()
		req.UserFilters = []*openfgav1.UserTypeFilter{{Type: "document", Relation: "nonexistent"}}
		err := ValidateListUsersRequest(ctx, req, typesys)
		require.Error(t, err)
	})

	t.Run("user_filter_with_valid_relation", func(t *testing.T) {
		req := baseRequest()
		req.UserFilters = []*openfgav1.UserTypeFilter{{Type: "document", Relation: "viewer"}}
		// Target relation must still resolve; viewer exists on document.
		require.NoError(t, ValidateListUsersRequest(ctx, req, typesys))
	})

	t.Run("invalid_contextual_tuple", func(t *testing.T) {
		req := baseRequest()
		req.ContextualTuples = []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "undefined_relation", "user:anne"),
		}
		err := ValidateListUsersRequest(ctx, req, typesys)
		require.Error(t, err)
	})

	t.Run("valid_contextual_tuple", func(t *testing.T) {
		req := baseRequest()
		req.ContextualTuples = []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "user:anne"),
		}
		require.NoError(t, ValidateListUsersRequest(ctx, req, typesys))
	})
}
