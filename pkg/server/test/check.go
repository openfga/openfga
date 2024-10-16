package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/server/config"
	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
)

func BenchmarkCheck(b *testing.B, ds storage.OpenFGADatastore) {
	benchmarkScenarios := map[string]struct {
		inputModel       string
		tupleGenerator   func() []*openfgav1.TupleKey
		tupleKeyToCheck  *openfgav1.CheckRequestTupleKey
		contextStruct    *structpb.Struct
		contextualTuples *openfgav1.ContextualTupleKeys
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
			expected: true,
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
			tupleKeyToCheck: &openfgav1.CheckRequestTupleKey{Object: "group:x", Relation: "computed_member", User: "user:anne"},
			expected:        true,
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
			tupleKeyToCheck: &openfgav1.CheckRequestTupleKey{Object: "document:x", Relation: "viewer", User: "user:anne"},
			expected:        true,
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

		// create and write necessary tuples
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
			graph.NewLocalChecker(),
			typeSystem,
			commands.WithCheckCommandMaxConcurrentReads(config.DefaultMaxConcurrentReadsForCheck),
			commands.WithCheckCommandResolveNodeLimit(config.DefaultResolveNodeLimit),
		)

		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				response, _, err := checkQuery.Execute(ctx, &openfgav1.CheckRequest{
					StoreId:          storeID,
					TupleKey:         bm.tupleKeyToCheck,
					ContextualTuples: bm.contextualTuples,
					Context:          bm.contextStruct,
				})

				require.Equal(b, bm.expected, response.GetAllowed())
				require.NoError(b, err)
			}
		})
	}
}
