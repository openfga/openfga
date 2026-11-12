// Package conditions holds the test matrix for dynamic conditions, the
// tuples whose condition is the reserved name $expression.
//
// The matrix has four layers:
//
//   - Layer 0, Write: is a tuple accepted or rejected.
//   - Layer 1, One tuple: a declared parameter type against the value sent.
//   - Layer 2, Composition: where the conditioned tuple sits in the model.
//   - Layer 3, Lifecycle: model changes, policy edits, contextual tuples.
//
// Cases are generated from lists of values. The expected result of every case
// comes from a rule in this package, never from the server's current output.
// A case whose current result differs from the agreed target is tied to a
// [Gap]; see gaps.go.
package conditions

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/pkg/tuple"
)

// Result is the outcome of one authorization question.
type Result int

const (
	// Allowed means the API answered that the user has the relation.
	Allowed Result = iota
	// Denied means the API answered that the user does not have the relation.
	Denied
	// Error means the API returned an error instead of an answer.
	Error
)

func (r Result) String() string {
	switch r {
	case Allowed:
		return "allowed"
	case Denied:
		return "denied"
	default:
		return "error"
	}
}

// Not inverts an answer. An error stays an error.
func (r Result) Not() Result {
	switch r {
	case Allowed:
		return Denied
	case Denied:
		return Allowed
	default:
		return Error
	}
}

// API names one of the query APIs a cell runs against.
type API string

const (
	APICheck       API = "Check"
	APIBatchCheck  API = "BatchCheck"
	APIListObjects API = "ListObjects"
	APIListUsers   API = "ListUsers"
)

// AllAPIs is every API a cell runs against.
var AllAPIs = []API{APICheck, APIBatchCheck, APIListObjects, APIListUsers}

// Kind says how a condition is expressed.
type Kind string

const (
	// KindExpression is a $expression tuple that carries its own CEL.
	KindExpression Kind = "expression"
	// KindNamed is a named condition declared in the model.
	KindNamed Kind = "named"
)

// InlineName is the reserved condition name for dynamic conditions.
const InlineName = "$expression"

// Cell is one authorization question and the result it must give.
type Cell struct {
	// ID identifies the cell in failure messages. It is stable across runs.
	ID string

	User     string
	Relation string
	Object   string
	// Context is the request context. A nil map sends no context.
	Context map[string]any

	// Want is the correct result.
	Want Result

	// ErrorAlsoOK lists the APIs for which an error is accepted besides Want,
	// because the expected result waits on a decision. Provisional says which.
	ErrorAlsoOK []API
	Provisional string

	// Gap marks a cell whose result today differs from Want for a known
	// reason. Today is the result the server gives until the gap is closed.
	Gap   *Gap
	Today Result
}

// Group is a model, its tuples, and the cells that query them. Each group
// runs in its own store.
type Group struct {
	Name string
	// ModelDSL is the authorization model in DSL form.
	ModelDSL string
	// Tuples are written one request at a time, in this order.
	Tuples []*openfgav1.TupleKey
	Cells  []Cell
}

// Client is the part of the OpenFGA API the matrix uses.
type Client = openfgav1.OpenFGAServiceClient

// Outcome counts what happened to the cells of a run.
type Outcome struct {
	// Passed counts results equal to the expected result.
	Passed int
	// Provisional counts errors accepted in place of the expected answer,
	// by the decision they wait on.
	Provisional map[string]int
	// KnownGaps counts results that match a known gap, by improvement number.
	KnownGaps map[int]int
	// Failures describes every result that is neither.
	Failures []string
}

func newOutcome() *Outcome {
	return &Outcome{KnownGaps: map[int]int{}, Provisional: map[string]int{}}
}

func (o *Outcome) merge(other *Outcome) {
	o.Passed += other.Passed
	for k, v := range other.Provisional {
		o.Provisional[k] += v
	}
	for k, v := range other.KnownGaps {
		o.KnownGaps[k] += v
	}
	o.Failures = append(o.Failures, other.Failures...)
}

// Summary describes the outcome in one line.
func (o *Outcome) Summary() string {
	keys := make([]int, 0, len(o.KnownGaps))
	for k := range o.KnownGaps {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	gaps := make([]string, 0, len(keys))
	total := 0
	for _, k := range keys {
		gaps = append(gaps, fmt.Sprintf("improvement %d: %d", k, o.KnownGaps[k]))
		total += o.KnownGaps[k]
	}
	reasons := make([]string, 0, len(o.Provisional))
	provisional := 0
	for reason, count := range o.Provisional {
		reasons = append(reasons, fmt.Sprintf("%d %s", count, reason))
		provisional += count
	}
	sort.Strings(reasons)
	return fmt.Sprintf("passed=%d provisional=%d (%s) known_gaps=%d (%s) failures=%d",
		o.Passed, provisional, strings.Join(reasons, "; "), total, strings.Join(gaps, ", "), len(o.Failures))
}

// judge compares one observed result with what the cell expects.
func (o *Outcome) judge(scope string, cell *Cell, api API, got Result) {
	if cell.Gap != nil && cell.Today != cell.Want {
		switch got {
		case cell.Today:
			o.KnownGaps[cell.Gap.Improvement]++
		case cell.Want:
			o.Failures = append(o.Failures, fmt.Sprintf(
				"[GAP CLOSED] %s %s %s: now %s as intended; remove the gap for improvement %d (%s)",
				scope, cell.ID, api, got, cell.Gap.Improvement, cell.Gap.Summary))
		default:
			o.Failures = append(o.Failures, fmt.Sprintf(
				"[%s] %s %s %s: got %s, want %s (known gap expects %s today)",
				severity(got, cell.Want), scope, cell.ID, api, got, cell.Want, cell.Today))
		}
		return
	}

	switch {
	case got == cell.Want:
		o.Passed++
	case got == Error && slices.Contains(cell.ErrorAlsoOK, api):
		o.Provisional[cell.Provisional]++
	default:
		o.Failures = append(o.Failures, fmt.Sprintf("[%s] %s %s %s: got %s, want %s",
			severity(got, cell.Want), scope, cell.ID, api, got, cell.Want))
	}
}

// severity labels a wrong result by how much harm it can do.
func severity(got, want Result) string {
	switch {
	case got == Allowed:
		return "CRITICAL allowed-should-not-be"
	case got == Denied && want == Allowed:
		return "HIGH denied-should-not-be"
	case got == Denied:
		return "HIGH answer-where-error-expected"
	default:
		return "MEDIUM error-where-answer-exists"
	}
}

// Report fails the test once for every failure in the outcome and logs the summary.
func (o *Outcome) Report(t *testing.T, name string) {
	t.Helper()
	t.Logf("%s: %s", name, o.Summary())
	const maxShown = 60
	for i, f := range o.Failures {
		if i == maxShown {
			t.Errorf("%s: %d more failures not shown", name, len(o.Failures)-maxShown)
			break
		}
		t.Error(f)
	}
}

// RunOptions tunes how a group is executed.
type RunOptions struct {
	// APIs to run. Empty means AllAPIs.
	APIs []API
	// Sample runs every Nth cell. Zero or one runs all of them.
	Sample int
}

// RunGroup creates a store for the group, writes its model and tuples, and
// runs every cell against the requested APIs.
func RunGroup(t *testing.T, client Client, scope string, g *Group, opts RunOptions) *Outcome {
	t.Helper()
	ctx := context.Background()
	out := newOutcome()

	storeID, modelID := setupStore(t, client, g.Name, g.ModelDSL)
	for _, tk := range g.Tuples {
		_, err := client.Write(ctx, &openfgav1.WriteRequest{
			StoreId:              storeID,
			AuthorizationModelId: modelID,
			Writes:               &openfgav1.WriteRequestWrites{TupleKeys: []*openfgav1.TupleKey{tk}},
		})
		require.NoErrorf(t, err, "group %s: writing tuple %s", g.Name, tuple.TupleKeyWithConditionToString(tk))
	}

	apis := opts.APIs
	if len(apis) == 0 {
		apis = AllAPIs
	}

	cells := make([]*Cell, 0, len(g.Cells))
	for i := range g.Cells {
		if opts.Sample > 1 && i%opts.Sample != 0 {
			continue
		}
		cells = append(cells, &g.Cells[i])
	}

	scope = scope + "/" + g.Name
	for _, api := range apis {
		switch api {
		case APICheck:
			for _, c := range cells {
				out.judge(scope, c, api, doCheck(ctx, client, storeID, modelID, c))
			}
		case APIListObjects:
			for _, c := range cells {
				out.judge(scope, c, api, doListObjects(ctx, client, storeID, modelID, c))
			}
		case APIListUsers:
			for _, c := range cells {
				out.judge(scope, c, api, doListUsers(ctx, client, storeID, modelID, c))
			}
		case APIBatchCheck:
			const batchSize = 40
			for start := 0; start < len(cells); start += batchSize {
				chunk := cells[start:min(start+batchSize, len(cells))]
				results := doBatchCheck(ctx, t, client, storeID, modelID, chunk)
				for i, c := range chunk {
					out.judge(scope, c, api, results[i])
				}
			}
		}
	}
	return out
}

func setupStore(t *testing.T, client Client, name, dsl string) (storeID, modelID string) {
	t.Helper()
	ctx := context.Background()

	if len(name) > 60 {
		name = name[:60]
	}
	store, err := client.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: "conditions-" + name})
	require.NoError(t, err)

	model, err := parser.TransformDSLToProto(dsl)
	require.NoErrorf(t, err, "group %s: model does not parse:\n%s", name, dsl)

	written, err := client.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         store.GetId(),
		SchemaVersion:   model.GetSchemaVersion(),
		TypeDefinitions: model.GetTypeDefinitions(),
		Conditions:      model.GetConditions(),
	})
	require.NoErrorf(t, err, "group %s: writing model", name)
	return store.GetId(), written.GetAuthorizationModelId()
}

func toStruct(m map[string]any) *structpb.Struct {
	if m == nil {
		return nil
	}
	s, err := structpb.NewStruct(m)
	if err != nil {
		panic(fmt.Sprintf("conditions matrix: bad context %v: %v", m, err))
	}
	return s
}

func doCheck(ctx context.Context, client Client, storeID, modelID string, c *Cell) Result {
	resp, err := client.Check(ctx, &openfgav1.CheckRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		TupleKey:             &openfgav1.CheckRequestTupleKey{User: c.User, Relation: c.Relation, Object: c.Object},
		Context:              toStruct(c.Context),
	})
	switch {
	case err != nil:
		return Error
	case resp.GetAllowed():
		return Allowed
	default:
		return Denied
	}
}

func doBatchCheck(ctx context.Context, t *testing.T, client Client, storeID, modelID string, cells []*Cell) []Result {
	t.Helper()
	req := &openfgav1.BatchCheckRequest{StoreId: storeID, AuthorizationModelId: modelID}
	for i, c := range cells {
		req.Checks = append(req.Checks, &openfgav1.BatchCheckItem{
			CorrelationId: fmt.Sprintf("c%d", i),
			TupleKey:      &openfgav1.CheckRequestTupleKey{User: c.User, Relation: c.Relation, Object: c.Object},
			Context:       toStruct(c.Context),
		})
	}
	resp, err := client.BatchCheck(ctx, req)
	require.NoError(t, err, "BatchCheck must fail per item, not as a whole")

	results := make([]Result, len(cells))
	for i := range cells {
		item, ok := resp.GetResult()[fmt.Sprintf("c%d", i)]
		switch {
		case !ok || item.GetError() != nil:
			results[i] = Error
		case item.GetAllowed():
			results[i] = Allowed
		default:
			results[i] = Denied
		}
	}
	return results
}

// doListObjects asks which objects of the cell's type the user can reach and
// turns the answer into a result for the cell's object.
func doListObjects(ctx context.Context, client Client, storeID, modelID string, c *Cell) Result {
	objectType, _ := tuple.SplitObject(c.Object)
	resp, err := client.ListObjects(ctx, &openfgav1.ListObjectsRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Type:                 objectType,
		Relation:             c.Relation,
		User:                 c.User,
		Context:              toStruct(c.Context),
	})
	switch {
	case err != nil:
		return Error
	case slices.Contains(resp.GetObjects(), c.Object):
		return Allowed
	default:
		return Denied
	}
}

// doListUsers asks which users have the relation on the cell's object and
// turns the answer into a result for the cell's user. A typed wildcard in the
// answer counts as the user being listed.
func doListUsers(ctx context.Context, client Client, storeID, modelID string, c *Cell) Result {
	objectType, objectID := tuple.SplitObject(c.Object)
	userObject, userRelation := tuple.SplitObjectRelation(c.User)
	userType, _ := tuple.SplitObject(userObject)
	resp, err := client.ListUsers(ctx, &openfgav1.ListUsersRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Object:               &openfgav1.Object{Type: objectType, Id: objectID},
		Relation:             c.Relation,
		UserFilters:          []*openfgav1.UserTypeFilter{{Type: userType, Relation: userRelation}},
		Context:              toStruct(c.Context),
	})
	if err != nil {
		return Error
	}
	for _, u := range resp.GetUsers() {
		s := tuple.UserProtoToString(u)
		if s == c.User || s == userType+":*" {
			return Allowed
		}
	}
	return Denied
}

// InlineCondition builds the condition of a $expression tuple.
func InlineCondition(expression string, parameters map[string]string) *openfgav1.RelationshipCondition {
	fields := map[string]any{"expression": expression}
	if parameters != nil {
		params := make(map[string]any, len(parameters))
		for k, v := range parameters {
			params[k] = v
		}
		fields["parameters"] = params
	}
	return &openfgav1.RelationshipCondition{Name: InlineName, Context: toStruct(fields)}
}

// NamedCondition builds the condition of a tuple that uses a named condition.
func NamedCondition(name string) *openfgav1.RelationshipCondition {
	return &openfgav1.RelationshipCondition{Name: name}
}

// Tuple builds a tuple key.
func Tuple(user, relation, object string, cond *openfgav1.RelationshipCondition) *openfgav1.TupleKey {
	return &openfgav1.TupleKey{User: user, Relation: relation, Object: object, Condition: cond}
}
