package listusers

import (
	"context"
	"fmt"
	"math"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	listuserstest "github.com/openfga/openfga/internal/test/listusers"
	"github.com/openfga/openfga/internal/test/matrixmodel"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/tests"
)

// ListUsers matrix over the shared matrix model. Check (weighted-graph resolver) is the specification: every response
// is verified against it (soundness, completeness, wildcard, no self). Assertions marked LegacyDiverges are skipped
// unless LU_MATRIX_RUN_LEGACY_DIVERGENCES=1.

const runLegacyDivergencesEnv = "LU_MATRIX_RUN_LEGACY_DIVERGENCES"

// unseenProbeID never appears in a tuple; it stands in for the wildcard, since Check rejects typed wildcards under and/but not.
const unseenProbeID = "matrix_unseen_probe"

type matrixAssertion struct {
	listuserstest.Assertion
	// LegacyDiverges, when set, explains why the current ListUsers implementation is known to differ from Check.
	LegacyDiverges string
}

type matrixTest struct {
	Name       string
	Tuples     []*openfgav1.TupleKey
	Assertions []matrixAssertion
}

type matrixTests struct {
	Name  string
	Model string
	Tests []matrixTest
}

var matrix = matrixTests{
	Name:  "matrix_test",
	Model: matrixmodel.Model,
	Tests: slices.Concat(directs, usersets, ttus, complexity),
}

var xCond = &openfgav1.RelationshipCondition{Name: "xcond"}

var (
	validConditionContext   = mustNewStruct(map[string]any{"x": "1"})
	invalidConditionContext = mustNewStruct(map[string]any{"x": "9"})
)

func mustNewStruct(m map[string]any) *structpb.Struct {
	s, err := structpb.NewStruct(m)
	if err != nil {
		panic(err)
	}
	return s
}

func t3(object, relation, user string) *openfgav1.TupleKey {
	return &openfgav1.TupleKey{Object: object, Relation: relation, User: user}
}

func t3c(object, relation, user string) *openfgav1.TupleKey {
	return &openfgav1.TupleKey{Object: object, Relation: relation, User: user, Condition: xCond}
}

// assertion builds a successful assertion: the users expected for (object, relation) under a single filter.
func assertion(object, relation, filter string, context *structpb.Struct, expected ...string) matrixAssertion {
	if expected == nil {
		expected = []string{}
	}
	return matrixAssertion{Assertion: listuserstest.Assertion{
		Request:     &listuserstest.TestListUsersRequest{Object: object, Relation: relation, Filters: []string{filter}},
		Context:     context,
		Expectation: expected,
	}}
}

// diverges marks an assertion as one the current implementation is known to get wrong.
func diverges(a matrixAssertion, reason string) matrixAssertion {
	a.LegacyDiverges = reason
	return a
}

// failing builds an assertion that must fail with the given gRPC code.
func failing(object, relation, filter string, context *structpb.Struct, code int) matrixAssertion {
	return matrixAssertion{Assertion: listuserstest.Assertion{
		Request:   &listuserstest.TestListUsersRequest{Object: object, Relation: relation, Filters: []string{filter}},
		Context:   context,
		ErrorCode: code,
	}}
}

// RunMatrixTests runs the ListUsers matrix twice: with the tuples stored, and with every tuple sent as a contextual tuple.
func RunMatrixTests(t *testing.T, engine string, desc string, client tests.ClientInterface) {
	t.Run("test matrix "+engine+" "+desc, func(t *testing.T) {
		t.Parallel()
		for _, contextual := range []bool{false, true} {
			runMatrix(t, client, contextual)
		}
	})
}

func runMatrix(t *testing.T, client tests.ClientInterface, contextual bool) {
	ctx := context.Background()
	modelProto := parser.MustTransformDSLToProto(matrix.Model)

	for ndx, test := range matrix.Tests {
		name := "test_" + test.Name
		storeName := fmt.Sprintf("lu-matrix-%d", ndx) // store names are limited to 64 characters
		if contextual {
			name += "_ctxTuples"
			storeName += "-ctx"
		}
		t.Run(name, func(t *testing.T) {
			if contextual && len(test.Tuples) > 100 {
				t.Skipf("cannot send more than 100 contextual tuples in one request")
			}
			t.Parallel()

			resp, err := client.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: storeName})
			require.NoError(t, err)
			storeID := resp.GetId()

			writeModelResponse, err := client.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
				StoreId:         storeID,
				SchemaVersion:   typesystem.SchemaVersion1_1,
				TypeDefinitions: modelProto.GetTypeDefinitions(),
				Conditions:      modelProto.GetConditions(),
			})
			require.NoError(t, err)
			modelID := writeModelResponse.GetAuthorizationModelId()

			if !contextual {
				tuples := testutils.Shuffle(test.Tuples)
				for i := 0; i < len(tuples); i += writeMaxChunkSize {
					end := int(math.Min(float64(i+writeMaxChunkSize), float64(len(tuples))))
					_, err = client.Write(ctx, &openfgav1.WriteRequest{
						StoreId:              storeID,
						AuthorizationModelId: modelID,
						Writes:               &openfgav1.WriteRequestWrites{TupleKeys: tuples[i:end]},
					})
					require.NoError(t, err)
				}
			}

			for i, a := range test.Assertions {
				t.Run(fmt.Sprintf("assertion_%d_%s_%s_%s", i, a.Request.Object, a.Request.Relation, strings.Join(a.Request.Filters, ",")), func(t *testing.T) {
					if a.LegacyDiverges != "" && os.Getenv(runLegacyDivergencesEnv) != "1" {
						t.Skipf("known divergence of the legacy implementation: %s", a.LegacyDiverges)
					}
					checkAssertion(ctx, t, client, storeID, modelID, test, a, contextual)
				})
			}
		})
	}
}

func checkAssertion(
	ctx context.Context,
	t *testing.T,
	client tests.ClientInterface,
	storeID, modelID string,
	test matrixTest,
	a matrixAssertion,
	contextual bool,
) {
	info := fmt.Sprintf("ListUsers request: %s. Tuples: %v. Contextual tuples: %v. Context: %v. Known legacy divergence: %q",
		a.Request.ToString(), test.Tuples, a.ContextualTuples, a.Context, a.LegacyDiverges)

	ctxTuples := testutils.Shuffle(a.ContextualTuples)
	if contextual {
		ctxTuples = append(ctxTuples, test.Tuples...)
	}

	req := a.Request.ToProtoRequest()
	resp, err := client.ListUsers(ctx, &openfgav1.ListUsersRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Object:               req.GetObject(),
		Relation:             req.GetRelation(),
		UserFilters:          req.GetUserFilters(),
		Context:              a.Context,
		ContextualTuples:     ctxTuples,
	})

	if a.ErrorCode != 0 {
		require.Error(t, err, info)
		e, ok := status.FromError(err)
		require.True(t, ok, info)
		require.Equal(t, a.ErrorCode, int(e.Code()), info)
		return
	}
	require.NoError(t, err, info)

	got := listuserstest.FromUsersProto(resp.GetUsers())

	// The expectation, exactly.
	assert.ElementsMatch(t, a.Expectation, got, "expectation mismatch. "+info)

	// The expectation, against Check.
	filterType, filterRelation := tuple.SplitObjectRelation(a.Request.Filters[0])
	wildcard := tuple.TypedPublicWildcard(filterType)

	check := func(user string) bool {
		r, err := client.Check(ctx, &openfgav1.CheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: modelID,
			TupleKey:             tuple.NewCheckRequestTupleKey(a.Request.Object, a.Request.Relation, user),
			ContextualTuples:     &openfgav1.ContextualTupleKeys{TupleKeys: ctxTuples},
			Context:              a.Context,
		})
		require.NoError(t, err, "check %s. %s", user, info)
		return r.GetAllowed()
	}

	returned := make(map[string]struct{}, len(got))
	for _, u := range got {
		returned[u] = struct{}{}
	}

	// soundness
	for _, u := range got {
		if tuple.IsTypedWildcard(u) {
			continue // typed wildcards cannot be checked under intersection or exclusion; see the wildcard check below.
		}
		assert.True(t, check(u), "soundness: %s was returned but Check denies it. %s", u, info)
	}

	// completeness
	covered := false
	_, covered = returned[wildcard]
	for _, u := range candidates(test, a, filterType, filterRelation) {
		if _, ok := returned[u]; ok {
			continue
		}
		if check(u) {
			assert.True(t, covered, "completeness: %s passes Check but was not returned, and %s was not returned either. %s", u, wildcard, info)
		}
	}

	// the wildcard
	if filterRelation == "" {
		granted := check(tuple.BuildObject(filterType, unseenProbeID))
		assert.Equal(t, granted, covered, "wildcard: %s returned=%v but Check grants an unseen %s user=%v. %s", wildcard, covered, filterType, granted, info)
	}

	// no self, unless a tuple names the object#relation as a subject (a data cycle makes it a legitimate member of itself)
	if filterRelation != "" && filterType == tuple.GetType(a.Request.Object) && filterRelation == a.Request.Relation {
		self := a.Request.Object + "#" + a.Request.Relation
		if !slices.Contains(candidates(test, a, filterType, filterRelation), self) {
			assert.NotContains(t, got, self, "no self: %s must not be returned. %s", self, info)
		}
	}
}

// candidates returns every subject that appears in the tuples with the filter's type (and relation), excluding wildcards.
func candidates(test matrixTest, a matrixAssertion, filterType, filterRelation string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, tuples := range [][]*openfgav1.TupleKey{test.Tuples, a.ContextualTuples} {
		for _, tk := range tuples {
			u := tk.GetUser()
			typ, id, rel := tuple.ToUserParts(u)
			if typ != filterType || rel != filterRelation || id == "*" {
				continue
			}
			if _, ok := seen[u]; ok {
				continue
			}
			seen[u] = struct{}{}
			out = append(out, u)
		}
	}
	return out
}
