// Package writemodel contains integration tests for the WriteAuthorizationModel API.
package writemodel

import (
	"context"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/openfga/openfga/pkg/typesystem"
)

var testCases = map[string]struct {
	model string
	code  int
}{
	// implemented in Fails_If_Using_Self_As_Type_Name
	// "case1": {
	//	model: `
	//	type user
	//	type self
	//	  relations
	//		define member: [user]
	//	`,
	//	code: 2056,
	//},
	// implemented in Fails_If_Using_This_As_Type_Name
	//"case2": {
	//	model: `
	//	type user
	//	type this
	//	  relations
	//		define member: [user]
	//	`,
	//	code: 2056,
	//},
	// implemented in Fails_If_Using_Self_As_Relation_Name
	//"case3": {
	//	model: `
	//	type user
	//	type group
	//	  relations
	//		define self: [user]
	//	`,
	//	code: 2056,
	//},
	// implemented in Fails_If_Using_This_As_Relation_Name
	//"case4": {
	//	model: `
	//	type user
	//	type group
	//	  relations
	//		define this: [user]
	//	`,
	//	code: 2056,
	//},
	"case6": {
		model: `model
	schema 1.1
type user
type group
  relations
	define group: group from group`,
		code: 2056,
	},
	"case7": {
		model: `model
	schema 1.1
type user
type group
  relations
	define parent: [group]
	define viewer: viewer from parent`,
		code: 2056,
	},
	"case8": {
		model: `model
	schema 1.1
type group
  relations
	define viewer: [group#viewer]`,
		code: 2056,
	},
	"case9": {
		model: `model
	schema 1.1
type user
type org
  relations
	define member: [user]
type group
  relations
	define parent: [org]
	define viewer: viewer from parent`,
		code: 2056,
	},
	"case10": {
		model: `model
	schema 1.1
type user
type group
  relations
	define parent: [group]
	define viewer: reader from parent`,
		code: 2056,
	},
	"case11": {
		model: `model
	schema 1.1
type user
type org
type group
  relations
	define parent: [group]
	define viewer: viewer from org`,
		code: 2056,
	},
	"case12": {
		model: `model
	schema 1.1
type user
type org
type group
  relations
	define parent: [group]
	define viewer: org from parent`,
		code: 2056,
	},
	"case13": {
		model: `model
	schema 1.1
type user
type org
type group
  relations
	define parent: [group, group#org]`,
		code: 2056,
	},
	"case14": {
		model: `model
	schema 1.1
type user
type org
  relations
	define viewer: [user]
type group
  relations
	define parent: [group]
	define viewer: viewer from parent`,
		code: 2056,
	},
	"case16": {
		model: `model
	schema 1.1
type document
  relations
	define reader: writer
	define writer: reader`,
		code: 2056,
	},
	"case17": {
		model: `model
	schema 1.1
type user
type folder
  relations
	define parent: [folder] or parent from parent
	define viewer: [user] or viewer from parent`,
		code: 2056,
	},
	"case18": {
		model: `model
	schema 1.1
type user
type folder
  relations
	define root: [folder]
	define parent: [folder] or root
	define viewer: [user] or viewer from parent`,
		code: 2056,
	},
	"case19": {
		model: `model
	schema 1.1
type user
type folder
  relations
	define root: [folder]
	define parent: root
	define viewer: [user] or viewer from parent`,
		code: 2056,
	},
	"case20": {
		model: `model
	schema 1.1
type user
type folder
  relations
	define root: [folder]
	define parent: [folder, folder#parent]
	define viewer: [user] or viewer from parent`,
		code: 2056,
	},
	"case21": {
		model: `model
	schema 1.1
type user
type group
  relations
	define member: [user]
	define reader: member and allowed`,
		code: 2056,
	},
	"case22": {
		model: `model
	schema 1.1
type user
type group
  relations
	define member: [user]
	define reader: member or allowed`,
		code: 2056,
	},
	"case23": {
		model: `model
	schema 1.1
type user
type group
  relations
	define member: [user]
	define reader: allowed but not member`,
		code: 2056,
	},
	"case24": {
		model: `model
	schema 1.1
type user
type group
  relations
	define member: [user]
	define reader: member but not allowed`,
		code: 2056,
	},
	//	"case25": {
	//		model: `model
	//	schema 1.1
	//type user
	//type org
	//  relations
	//	define member`,
	//		code: 2056,
	//	},
	"same_type_fails": {
		model: `model
	schema 1.1
type user
type user`,
		code: 2056,
	},
	"difference_includes_itself_in_subtract_fails": {
		model: `model
	schema 1.1
type user
type document
  relations
	define viewer: [user] but not viewer`,
		code: 2056,
	},
	"union_includes_itself_fails": {
		model: `model
	schema 1.1
type user
type document
  relations
	define viewer: [user] or viewer`,
		code: 2056,
	},
	"intersection_includes_itself_fails": {
		model: `model
	schema 1.1
type user
type document
  relations
	define viewer: [user] and viewer`,
		code: 2056,
	},
	"simple_model_succeeds": {
		model: `model
	schema 1.1
type user
type folder
  relations
	define viewer: [user]
type document
  relations
	define parent: [folder]
	define viewer: viewer from parent`,
	},
	"no_relations_succeeds": {
		model: `model
	schema 1.1
type user`,
	},
	"union_may_contain_repeated_relations": {
		model: `model
	schema 1.1
type user
type document
  relations
	define editor: [user]
	define viewer: editor or editor`,
	},
	"intersection_may_contain_repeated_relations": {
		model: `model
	schema 1.1
type user
type document
  relations
	define editor: [user]
	define viewer: editor and editor`,
	},
	"exclusion_may_contain_repeated_relations": {
		model: `model
	schema 1.1
type user
type document
  relations
	define editor: [user]
	define viewer: editor but not editor`,
	},
	"as_long_as_one_computed_userset_type_is_valid": {
		model: `model
	schema 1.1
type user
type group
  relations
	define parent: [group, team]
	define viewer: reader from parent
type team
  relations
	define reader: [user]`,
	},
}

// ClientInterface defines interface for running WriteAuthorizationModel tests.
type ClientInterface interface {
	CreateStore(ctx context.Context, in *openfgav1.CreateStoreRequest, opts ...grpc.CallOption) (*openfgav1.CreateStoreResponse, error)
	WriteAuthorizationModel(ctx context.Context, in *openfgav1.WriteAuthorizationModelRequest, opts ...grpc.CallOption) (*openfgav1.WriteAuthorizationModelResponse, error)
}

// RunAllTests will run all write model tests.
func RunAllTests(t *testing.T, client ClientInterface) {
	t.Run("RunAllTests", func(t *testing.T) {
		t.Run("WriteTest", func(t *testing.T) {
			t.Parallel()
			runTests(t, client)
		})
	})
}

func runTests(t *testing.T, client ClientInterface) {
	ctx := context.Background()

	for name, test := range testCases {
		name := name
		test := test

		t.Run(name, func(t *testing.T) {
			t.Parallel()
			resp, err := client.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: "write_model_test"})
			require.NoError(t, err)

			storeID := resp.GetId()
			model := parser.MustTransformDSLToProto(test.model)
			_, err = client.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
				StoreId:         storeID,
				SchemaVersion:   typesystem.SchemaVersion1_1,
				TypeDefinitions: model.GetTypeDefinitions(),
				Conditions:      model.GetConditions(),
			})

			if test.code == 0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				e, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, test.code, int(e.Code()), err)
			}
		})
	}
}
