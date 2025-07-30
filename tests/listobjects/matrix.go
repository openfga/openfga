package listobjects

import (
	"context"
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/pkg/testutils"
)

// MustNewStruct returns a new *structpb.Struct or panics
// on error. The new *structpb.Struct value is built from
// the map m.
func MustNewStruct(m map[string]any) *structpb.Struct {
	s, err := structpb.NewStruct(m)
	if err == nil {
		return s
	}
	panic(err)
}

var matrix = matrixTests{
	Name: "matrix_test",
	Model: `model
  schema 1.1
type user
type employee
# yes, wildcard is a userset instead of a direct, makes it easier and tests the userset path
type directs
  relations
    define direct: [user]
    define direct_comb: [user, user with xcond, user:*, user:* with xcond]
    define direct_mult_types: [user, user:*, employee, employee:*]
    define other_rel: [user, user with xcond, user:*, user:* with xcond, employee]
    define computed: direct
    define computed_comb: direct_comb
    define computed_mult_types: direct_mult_types
    define computed_2_times: computed
    define computed_3_times: computed_2_times
    
    define or_computed: computed_3_times or computed_comb
    define or_computed_mult_types: or_computed or computed_mult_types
    define and_computed_mult_types: or_computed_mult_types and other_rel
    define butnot_computed: and_computed_mult_types but not computed_comb
    define alg_combined: butnot_computed but not computed_3_times
    define alg_combined_oneline: (computed_3_times or computed_comb) and (computed_mult_types or other_rel)
   
    define tuple_cycle_len2_userset: [user, employee, usersets-user#tuple_cycle_len2_userset]  
    define tuple_cycle_len2_ttu: [user, employee] or tuple_cycle_len2_ttu from cycle_len2_parent
    define cycle_len2_parent: [ttus]
    define tuple_cycle_len3: [user, employee, complexity3#tuple_cycle_len3]

type directs-employee
  relations
    define direct: [employee, employee:*, employee with xcond, employee:* with xcond]
    define other_rel: [employee]
    define direct_wild: [employee:*]
    define computed: direct
    define computed_2_times: computed
    define computed_3_times: computed_2_times
   
    define or_computed: computed_3_times or other_rel
    define and_computed: or_computed and direct_wild
    define alg_combined: and_computed but not other_rel
    define alg_combined_oneline: (computed_3_times or other_rel) and (computed or direct_wild)
    
    define tuple_cycle_len2_userset: [employee, usersets-user#tuple_cycle_len2_userset] 
    define tuple_cycle_len2_ttu: [user, employee] or tuple_cycle_len2_ttu from cycle_len2_parent
    define cycle_len2_parent: [ttus]
    define tuple_cycle_len3: [employee, complexity3#tuple_cycle_len3]
      
type usersets-user
  relations
    define userset: [directs#direct_comb, directs-employee#direct]
    define userset_other_rel: [directs#other_rel, directs-employee#other_rel]
    define userset_alg_combined: [directs#alg_combined, directs-employee#alg_combined]
    define userset_alg_combined_oneline: [directs#alg_combined_oneline, directs-employee#alg_combined_oneline]
    define userset_combined_cond: [directs#computed_mult_types with xcond, directs-employee#computed_3_times]
    define user_rel1: [user, user:*]
    define user_rel2: [user, user with xcond]
    define user_rel3: [user, user:* with xcond]
    define userset_computed: userset
    define userset_alg_combined_computed: userset_alg_combined
    
    define userset_recursive: [user, usersets-user#userset_recursive]
    define userset_recursive_public: [user:*, usersets-user#userset_recursive_public]
    define userset_recursive_combined_w3: [user, user:*, employee, usersets-user#userset_recursive_combined_w3, usersets-user#userset]
    define userset_recursive_alg_combined_oneline: ([user, usersets-user#userset_recursive_alg_combined_oneline] or user_rel1) or (user_rel2 and user_rel3)
    define userset_recursive_alg_combined: [usersets-user#userset_recursive_alg_combined] or user_rel4
    define userset_recursive_alg_combined_w2: ([user, usersets-user#userset_recursive_alg_combined_w2] or user_rel1) or (user_rel2 and userset)
    define user_rel4: user_rel1 or user_rel5
    define user_rel5: user_rel2 and user_rel3
    
    define or_userset: userset_computed or userset_alg_combined_computed
    define and_userset: or_userset and userset_combined_cond
    define alg_combined: and_userset but not userset_alg_combined_oneline
    define alg_combined_computed: alg_combined
     
    define tuple_cycle_len2_userset: [user, directs#tuple_cycle_len2_userset, directs-employee#tuple_cycle_len2_userset with xcond]
    define tuple_cycle_len3: [directs#tuple_cycle_len3, directs-employee#tuple_cycle_len3]

type ttus
  relations
    define direct_parent: [directs]
    define mult_parent_types: [directs, directs-employee, directs with xcond, directs-employee with xcond]
    define ttu_direct: direct from direct_parent
    define ttu_other_rel: other_rel from mult_parent_types
    define ttu_alg_combined: alg_combined from mult_parent_types
    define ttu_alg_combined_oneline: alg_combined_oneline from mult_parent_types
    define duplicate_ttu: direct from direct_parent or direct from mult_parent_types
    define ttu_computed: ttu_direct
    define ttu_alg_combined_computed: ttu_alg_combined
    define user_rel1: [user, user:*]
    define user_rel2: [user, user with xcond]
    define user_rel3: [user, user:* with xcond]
    
    define or_ttu: ttu_computed or ttu_alg_combined_computed
    define and_ttu: or_ttu and ttu_other_rel
    define alg_combined: and_ttu but not ttu_alg_combined_oneline
    define alg_combined_computed: alg_combined
    define user_rel4: user_rel1 or user_rel5
    define user_rel5: user_rel2 and user_rel3
    
    define ttu_parent: [ttus]
    define ttu_recursive: [user] or ttu_recursive from ttu_parent
    define ttu_recursive_public: [user:*] or ttu_recursive_public from ttu_parent
    define ttu_recursive_combined_w3: [user, user:*, employee] or ttu_recursive_combined_w3 from ttu_parent or direct from direct_parent
    define ttu_recursive_alg_combined_oneline: ([user] or ttu_recursive_alg_combined_oneline from ttu_parent) or (user_rel1 or (user_rel2 and user_rel3)) 
    define ttu_recursive_alg_combined_w2: ([user] or ttu_recursive_alg_combined from ttu_parent) or (user_rel1 or (user_rel2 and ttu_direct)) 
    define ttu_recursive_alg_combined: ttu_recursive_alg_combined from ttu_parent or user_rel4
    
    define tuple_cycle_len2_ttu: [user, employee] or tuple_cycle_len2_ttu from mult_parent_types
    
type complexity3
  relations
    define userset_parent: [usersets-user, usersets-user with xcond]
  
    define ttu_userset: userset from userset_parent
    define ttu_userset_public: [user:*] or ttu_userset
    define ttu_userset_other_rel: userset_other_rel from userset_parent
    define ttu_userset_inner_alg_combined: userset_alg_combined_computed from userset_parent
    define ttu_userset_alg_combined: alg_combined_computed from userset_parent
    define or_ttu_userset: ttu_userset or ttu_userset_other_rel
    define and_ttu_userset: or_ttu_userset and ttu_userset_inner_alg_combined
    define alg_combined_ttu_userset: and_ttu_userset but not ttu_userset_public
    
    define userset_ttu: [ttus#ttu_direct, ttus#ttu_direct with xcond]
    define userset_ttu_public: [user:*, ttus#ttu_direct]
    define userset_ttu_other_rel: [ttus#ttu_other_rel]
    define userset_ttu_inner_alg_combined: [ttus#ttu_alg_combined_computed]
    define userset_ttu_alg_combined: [ttus#alg_combined_computed]
    define or_userset_ttu: userset_ttu or userset_ttu_other_rel 
    define and_userset_ttu: or_userset_ttu and userset_ttu_inner_alg_combined

    define alg_combined_userset_ttu: and_userset_ttu but not userset_ttu_public
    
    define tuple_cycle_len3: [user, employee] or tuple_cycle_len3 from userset_parent
    
type complexity4
  relations
    define parent: [complexity3, complexity3 with xcond]
    define ttu_userset_ttu: userset_ttu from parent
    define userset_ttu_userset: [complexity3#ttu_userset, complexity3#ttu_userset_other_rel with xcond]
    define or_complex4: [complexity3#userset_ttu_public] or ttu_userset_ttu
    define alg_combined_complex4: or_complex4 and alg_combined_userset_ttu from parent

condition xcond(x: string) {
  x == '1'
}`,

	Tests: slices.Concat(directs, usersets, ttus),
}

func runTestMatrix(t *testing.T, params testParams) {
	schemaVersion := params.schemaVersion
	client := params.client
	name := matrix.Name

	ctx := context.Background()

	t.Run(name, func(t *testing.T) {
		tests := matrix.Tests

		for _, test := range tests {
			t.Run("test_"+test.Name, func(t *testing.T) {
				resp, err := client.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: test.Name})
				require.NoError(t, err)
				storeID := resp.GetId()
				modelProto := parser.MustTransformDSLToProto(matrix.Model)
				writeModelResponse, err := client.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
					StoreId:         storeID,
					SchemaVersion:   schemaVersion,
					TypeDefinitions: modelProto.GetTypeDefinitions(),
					Conditions:      modelProto.GetConditions(),
				})
				require.NoError(t, err)
				modelID := writeModelResponse.GetAuthorizationModelId()

				tuples := testutils.Shuffle(test.Tuples)
				tuplesLength := len(tuples)
				if tuplesLength > 0 {
					for i := 0; i < tuplesLength; i += writeMaxChunkSize {
						end := int(math.Min(float64(i+writeMaxChunkSize), float64(tuplesLength)))
						writeChunk := (tuples)[i:end]
						_, err = client.Write(ctx, &openfgav1.WriteRequest{
							StoreId:              storeID,
							AuthorizationModelId: modelID,
							Writes: &openfgav1.WriteRequestWrites{
								TupleKeys: writeChunk,
							},
						})
						require.NoError(t, err)
					}
				}

				if len(test.ListObjectAssertions) == 0 {
					t.Skipf("no list objects assertions defined")
				}

				listObjectsAssertion(ctx, t, client, storeID, writeModelResponse.GetAuthorizationModelId(), false, test.Tuples, test.ListObjectAssertions)
			})
		}
	})
}
