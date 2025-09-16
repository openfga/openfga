package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// to prevent overwhelming server on expensive queries.
const maxConcurrentReads = 50

// Some of the benchmark tests require context blocks to be built
// but many do not. This noop method is a placeholder for the non-context test cases.
func noopContextGenerator() *structpb.Struct {
	return &structpb.Struct{}
}

func BenchmarkCheck(b *testing.B, ds storage.OpenFGADatastore) {
	benchmarkScenarios := map[string]struct {
		inputModel       string
		tupleGenerator   func() []*openfgav1.TupleKey
		tupleKeyToCheck  *openfgav1.CheckRequestTupleKey
		contextGenerator func() *structpb.Struct
		expected         bool
	}{
		`race_between_direct_and_userset`: {
			inputModel: `
				model
					schema 1.1
				type user
				type team
					relations
						define member: [user,team#member]
				type repo
					relations
						define admin: [user,team#member] or member from owner
						define owner: [organization]
				type organization
					relations
						define member: [user]`,
			tupleGenerator: func() []*openfgav1.TupleKey {
				var tuples []*openfgav1.TupleKey
				for i := 0; i < 1000; i++ { // add user:anne to many teams
					tuples = append(tuples, &openfgav1.TupleKey{
						Object:   fmt.Sprintf("team:%d", i),
						Relation: "member",
						User:     "user:anne",
					})
				}

				// Now give anne direct admin and also give one of the teams admin
				tuples = append(tuples, []*openfgav1.TupleKey{
					{Object: "repo:openfga", Relation: "admin", User: "user:anne"},
					{Object: "repo:openfga", Relation: "admin", User: "team:123#member"},
				}...)
				return tuples
			},
			contextGenerator: noopContextGenerator,
			tupleKeyToCheck: &openfgav1.CheckRequestTupleKey{
				Object:   "repo:openfga",
				Relation: "admin",
				User:     "user:anne",
			},
			expected: true,
		},
		`userset_check_only`: {
			inputModel: `
				model
					schema 1.1
				type user
				type team
					relations
						define member: [user,team#member]
				type repo
					relations
						define admin: [user,team#member] or member from owner
						define owner: [organization]
				type organization
					relations
						define member: [user]`,
			tupleGenerator: func() []*openfgav1.TupleKey {
				var tuples []*openfgav1.TupleKey
				for i := 0; i < 1000; i++ { // add user:anne to many teams
					tuples = append(tuples, &openfgav1.TupleKey{
						Object:   fmt.Sprintf("team:%d", i),
						Relation: "member",
						User:     "user:anne",
					})
				}
				// Now give a team direct access
				tuples = append(tuples, &openfgav1.TupleKey{Object: "repo:openfga", Relation: "admin", User: "team:123#member"})
				return tuples
			},
			contextGenerator: noopContextGenerator,

			// user:bob has no direct access, so we must check if he's a member of a team
			tupleKeyToCheck: &openfgav1.CheckRequestTupleKey{
				Object:   "repo:openfga",
				Relation: "admin",
				User:     "user:bob",
			},
			expected: false,
		}, `with_intersection`: {
			inputModel: `
				model
					schema 1.1
				type user
				type group
					relations
						define a: [user]
						define b: [user]
						define intersect: a and b
						define exclude: a but not b
			`,
			tupleGenerator: func() []*openfgav1.TupleKey {
				return []*openfgav1.TupleKey{
					{Object: "group:1", Relation: "a", User: "user:anne"},
					{Object: "group:1", Relation: "b", User: "user:anne"},
				}
			},
			tupleKeyToCheck: &openfgav1.CheckRequestTupleKey{
				Object: "group:1", Relation: "intersect", User: "user:anne",
			},
			contextGenerator: noopContextGenerator,
			expected:         true,
		}, `with_exclusion`: {
			inputModel: `
				model
					schema 1.1
				type user
				type group
					relations
						define a: [user]
						define b: [user]
						define intersect: a and b
						define exclude: a but not b
			`,
			tupleGenerator: func() []*openfgav1.TupleKey {
				return []*openfgav1.TupleKey{
					{Object: "group:1", Relation: "a", User: "user:anne"},
					{Object: "group:1", Relation: "b", User: "user:anne"},
				}
			},
			contextGenerator: noopContextGenerator,
			tupleKeyToCheck: &openfgav1.CheckRequestTupleKey{
				Object: "group:1", Relation: "exclude", User: "user:anne",
			},
			expected: false,
		}, `with_computed`: {
			inputModel: `
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user]
						define computed_member: member
			`,
			tupleGenerator: func() []*openfgav1.TupleKey {
				return []*openfgav1.TupleKey{{Object: "group:x", Relation: "member", User: "user:anne"}}
			},
			contextGenerator: noopContextGenerator,
			tupleKeyToCheck:  &openfgav1.CheckRequestTupleKey{Object: "group:x", Relation: "computed_member", User: "user:anne"},
			expected:         true,
		}, `with_userset`: {
			inputModel: `
				model
					schema 1.1
				type user
				type user2
				type team
					relations
						define member: [user, user2]
				type document
					relations
						define viewer: [team#member]
			`,
			tupleGenerator: func() []*openfgav1.TupleKey {
				return []*openfgav1.TupleKey{
					{Object: "document:x", Relation: "viewer", User: "team:fga#member"},
					{Object: "team:fga", Relation: "member", User: "user:anne"},
				}
			},
			tupleKeyToCheck:  &openfgav1.CheckRequestTupleKey{Object: "document:x", Relation: "viewer", User: "user:anne"},
			contextGenerator: noopContextGenerator,
			expected:         true,
		}, `with_nested_usersets`: {
			inputModel: `
					model
						schema 1.1
					type user
					type team
						relations
							define member: [user,team#member]
				`,
			tupleGenerator: func() []*openfgav1.TupleKey {
				tuples := []*openfgav1.TupleKey{{Object: "team:24", Relation: "member", User: "user:maria"}}
				for i := 1; i < config.DefaultResolveNodeLimit; i++ {
					tuples = append(tuples, &openfgav1.TupleKey{
						Object:   fmt.Sprintf("team:%d", i),
						Relation: "member",
						User:     fmt.Sprintf("team:%d#member", i+1),
					})
				}
				return tuples
			},
			tupleKeyToCheck:  &openfgav1.CheckRequestTupleKey{Object: "team:1", Relation: "member", User: "user:maria"},
			contextGenerator: noopContextGenerator,
			expected:         true,
		}, `with_simple_ttu`: {
			inputModel: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
					define member_complex: [user, group#member_complex]
			type folder
				relations
					define parent: [group]
					define viewer: member from parent
					define viewer_complex: member_complex from parent
			`,
			tupleGenerator: func() []*openfgav1.TupleKey {
				tuplesToWrite := []*openfgav1.TupleKey{
					tuple.NewTupleKey("group:999", "member", "user:maria"),
					tuple.NewTupleKey("group:999", "member_complex", "user:maria"),
				}
				for i := 1; i < 1_000; i++ {
					tuplesToWrite = append(tuplesToWrite, tuple.NewTupleKey(
						"folder:x",
						"parent",
						fmt.Sprintf("group:%d", i)))
				}
				return tuplesToWrite
			},
			tupleKeyToCheck:  &openfgav1.CheckRequestTupleKey{Object: "folder:x", Relation: "viewer", User: "user:maria"},
			contextGenerator: noopContextGenerator,
			expected:         true,
		}, `with_complex_ttu`: {
			inputModel: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
					define member_complex: [user, group#member_complex]
			type folder
				relations
					define parent: [group]
					define viewer: member from parent
					define viewer_complex: member_complex from parent
			`,
			tupleGenerator: func() []*openfgav1.TupleKey {
				tuplesToWrite := []*openfgav1.TupleKey{
					tuple.NewTupleKey("group:999", "member", "user:maria"),
					tuple.NewTupleKey("group:999", "member_complex", "user:maria"),
				}
				for i := 1; i < 1_000; i++ {
					tuplesToWrite = append(tuplesToWrite, tuple.NewTupleKey(
						"folder:x",
						"parent",
						fmt.Sprintf("group:%d", i)))
				}
				return tuplesToWrite
			},
			tupleKeyToCheck:  &openfgav1.CheckRequestTupleKey{Object: "folder:x", Relation: "viewer_complex", User: "user:maria"},
			contextGenerator: noopContextGenerator,
			expected:         true,
		},
		`with_one_condition`: {
			inputModel: `
				model
					schema 1.1
				type user
				type doc
					relations
						define viewer: [user with password]
				condition password(p: string) {
					p == "secret"
				}
			`,
			tupleGenerator: func() []*openfgav1.TupleKey {
				return []*openfgav1.TupleKey{
					tuple.NewTupleKeyWithCondition(
						"doc:x", "viewer", "user:maria", "password", nil,
					),
				}
			},
			tupleKeyToCheck: &openfgav1.CheckRequestTupleKey{
				Object: "doc:x", Relation: "viewer", User: "user:maria",
			},
			contextGenerator: func() *structpb.Struct {
				s, err := structpb.NewStruct(map[string]interface{}{
					"p": "secret",
				})
				if err != nil {
					panic(err)
				}
				return s
			},
			expected: true,
		},
		`with_one_condition_with_many_parameters`: {
			inputModel: `
				model
					schema 1.1
				type user
				type doc
					relations
						define viewer: [user with complex]
				condition complex(b: bool, s:string, i: int, u: uint, d: double, du: duration, t:timestamp, ip:ipaddress) {
					b == true && s == "s" && i == 1 && u == uint(1) && d == 0.1 && du == duration("1h") && t == timestamp("1972-01-01T10:00:20.021Z") && ip == ipaddress("127.0.0.1")
				}
			`,
			tupleGenerator: func() []*openfgav1.TupleKey {
				return []*openfgav1.TupleKey{
					tuple.NewTupleKeyWithCondition(
						"doc:x", "viewer", "user:maria", "complex", nil,
					),
				}
			},
			contextGenerator: func() *structpb.Struct {
				s, err := structpb.NewStruct(map[string]interface{}{
					"b":  true,
					"s":  "s",
					"i":  1,
					"u":  1,
					"d":  0.1,
					"du": "1h",
					"t":  "1972-01-01T10:00:20.021Z",
					"ip": "127.0.0.1",
				})
				if err != nil {
					panic(err)
				}
				return s
			},
			tupleKeyToCheck: &openfgav1.CheckRequestTupleKey{
				Object: "doc:x", Relation: "viewer", User: "user:maria",
			},
			expected: true,
		},
	}

	for name, bm := range benchmarkScenarios {
		ctx := context.Background()
		storeID := ulid.Make().String()
		model := testutils.MustTransformDSLToProtoWithID(bm.inputModel)
		typeSystem, err := typesystem.NewAndValidate(context.Background(), model)
		require.NoError(b, err)

		err = ds.WriteAuthorizationModel(context.Background(), storeID, model)
		require.NoError(b, err)

		// create and write necessary tuples in deterministic order (to make runs comparable)
		tuples := bm.tupleGenerator()
		for i := 0; i < len(tuples); {
			var tuplesToWrite []*openfgav1.TupleKey
			for j := 0; j < ds.MaxTuplesPerWrite(); j++ {
				if i == len(tuples) {
					break
				}
				tuplesToWrite = append(tuplesToWrite, tuples[i])
				i++
			}
			err := ds.Write(context.Background(), storeID, nil, tuplesToWrite)
			require.NoError(b, err)
		}

		checkQuery := commands.NewCheckCommand(
			ds,
			graph.NewLocalChecker(graph.WithOptimizations(true)),
			commands.CheckCommandParams{
				StoreID:  storeID,
				TupleKey: bm.tupleKeyToCheck,
				Context:  bm.contextGenerator(),
				Typesys:  typeSystem,
			},
			commands.WithCheckCommandMaxConcurrentReads(maxConcurrentReads),
		)

		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				response, _, err := checkQuery.Execute(ctx)

				require.NoError(b, err)
				require.Equal(b, bm.expected, response.GetAllowed())
			}
		})
	}

	// Now run the one benchmark that doesn't fit the table pattern
	benchmarkCheckWithBypassUsersetReads(b, ds)
}

// This benchmark test creates multiple authorization models so it doesn't fit into
// the table pattern above.
func benchmarkCheckWithBypassUsersetReads(b *testing.B, ds storage.OpenFGADatastore) {
	schemaOne := `
		model
			schema 1.1
		type user
		type group
			relations
				define member: [user]
		type document
			relations
				define viewer: [user:*, group#member]
	`
	storeID := ulid.Make().String()
	modelOne := testutils.MustTransformDSLToProtoWithID(schemaOne)

	err := ds.WriteAuthorizationModel(context.Background(), storeID, modelOne)
	require.NoError(b, err)

	// add user to many usersets according to model A
	var tuples []*openfgav1.TupleKey
	for i := 0; i < 1000; i++ {
		tuples = append(tuples, &openfgav1.TupleKey{
			Object:   fmt.Sprintf("group:%d", i),
			Relation: "member",
			User:     "user:anne",
		})
	}

	// one userset gets access to document:budget
	tuples = append(tuples, &openfgav1.TupleKey{Object: "document:budget", Relation: "viewer", User: "group:999#member"})

	// now actually write the tuples
	for i := 0; i < len(tuples); {
		var tuplesToWrite []*openfgav1.TupleKey
		for j := 0; j < ds.MaxTuplesPerWrite(); j++ {
			if i == len(tuples) {
				break
			}
			tuplesToWrite = append(tuplesToWrite, tuples[i])
			i++
		}
		err := ds.Write(context.Background(), storeID, nil, tuplesToWrite)
		require.NoError(b, err)
	}

	schemaTwo := `
		model
			schema 1.1
		type user
		type user2
		type group
			relations
				define member: [user2]
		type document
			relations
				define viewer: [user:*, group#member]
	`
	modelTwo := testutils.MustTransformDSLToProtoWithID(schemaTwo)
	typeSystemTwo, err := typesystem.NewAndValidate(context.Background(), modelTwo)
	require.NoError(b, err)

	// all the usersets added above are now invalid and should be skipped!
	err = ds.WriteAuthorizationModel(context.Background(), storeID, modelTwo)
	require.NoError(b, err)

	checkQuery := commands.NewCheckCommand(
		ds,
		graph.NewLocalChecker(graph.WithOptimizations(true)),
		commands.CheckCommandParams{
			StoreID:  storeID,
			TupleKey: tuple.NewCheckRequestTupleKey("document:budget", "viewer", "user:anne"),
			Typesys:  typeSystemTwo,
		},
		commands.WithCheckCommandMaxConcurrentReads(maxConcurrentReads),
	)

	b.Run("benchmark_with_bypass_userset_read", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			response, _, err := checkQuery.Execute(context.Background())

			require.NoError(b, err)
			require.False(b, response.GetAllowed())
		}
	})
}
