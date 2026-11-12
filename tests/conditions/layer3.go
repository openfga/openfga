package conditions

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
)

// store is a small handle on one store used by the lifecycle scenarios.
type store struct {
	t       *testing.T
	client  Client
	id      string
	modelID string
}

func newStore(t *testing.T, client Client, name, dsl string) *store {
	t.Helper()
	id, modelID := setupStore(t, client, name, dsl)
	return &store{t: t, client: client, id: id, modelID: modelID}
}

// setModel writes a new model to the store and makes it the one used by
// later requests.
func (s *store) setModel(dsl string) {
	s.t.Helper()
	model, err := parser.TransformDSLToProto(dsl)
	require.NoError(s.t, err)
	written, err := s.client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         s.id,
		SchemaVersion:   model.GetSchemaVersion(),
		TypeDefinitions: model.GetTypeDefinitions(),
		Conditions:      model.GetConditions(),
	})
	require.NoError(s.t, err)
	s.modelID = written.GetAuthorizationModelId()
}

func (s *store) write(tuples ...*openfgav1.TupleKey) error {
	_, err := s.client.Write(context.Background(), &openfgav1.WriteRequest{
		StoreId:              s.id,
		AuthorizationModelId: s.modelID,
		Writes:               &openfgav1.WriteRequestWrites{TupleKeys: tuples},
	})
	return err
}

func (s *store) mustWrite(tuples ...*openfgav1.TupleKey) {
	s.t.Helper()
	for _, tk := range tuples {
		require.NoError(s.t, s.write(tk))
	}
}

func (s *store) remove(user, relation, object string) error {
	_, err := s.client.Write(context.Background(), &openfgav1.WriteRequest{
		StoreId:              s.id,
		AuthorizationModelId: s.modelID,
		Deletes: &openfgav1.WriteRequestDeletes{TupleKeys: []*openfgav1.TupleKeyWithoutCondition{
			{User: user, Relation: relation, Object: object},
		}},
	})
	return err
}

// check asks whether user:u has the relation. It asks with higher
// consistency, so that a scenario which has just changed a tuple is not
// answered from a cache.
func (s *store) check(relation, object string, reqCtx map[string]any, contextual ...*openfgav1.TupleKey) Result {
	req := &openfgav1.CheckRequest{
		StoreId:              s.id,
		AuthorizationModelId: s.modelID,
		TupleKey:             &openfgav1.CheckRequestTupleKey{User: "user:u", Relation: relation, Object: object},
		Context:              toStruct(reqCtx),
		Consistency:          openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
	}
	if len(contextual) > 0 {
		req.ContextualTuples = &openfgav1.ContextualTupleKeys{TupleKeys: contextual}
	}
	resp, err := s.client.Check(context.Background(), req)
	switch {
	case err != nil:
		return Error
	case resp.GetAllowed():
		return Allowed
	default:
		return Denied
	}
}

// cachedCheck asks with the default consistency, so caches may answer.
func (s *store) cachedCheck(user, relation, object string, reqCtx map[string]any) Result {
	return doCheck(context.Background(), s.client, s.id, s.modelID,
		&Cell{User: user, Relation: relation, Object: object, Context: reqCtx})
}

func (s *store) listObjects(user, relation, object string, reqCtx map[string]any) Result {
	return doListObjects(context.Background(), s.client, s.id, s.modelID,
		&Cell{User: user, Relation: relation, Object: object, Context: reqCtx})
}

func (s *store) listUsers(user, relation, object string, reqCtx map[string]any) Result {
	return doListUsers(context.Background(), s.client, s.id, s.modelID,
		&Cell{User: user, Relation: relation, Object: object, Context: reqCtx})
}

const lifecycleModel = `model
  schema 1.1

type user

type doc
  relations
    define base: [user]
    define grant: [user with $expression]
    define blocked: [user with $expression]
    define deny: base but not blocked
`

var channelIsA = InlineCondition(`channel == 'A'`, map[string]string{"channel": "string"})

// RunLayer3 runs the Lifecycle layer: the answer must stay right when the
// model, a policy, the request path or the caller's arguments change.
func RunLayer3(t *testing.T, client Client) {
	t.Run("model_change_removes_expression_from_a_grant", func(t *testing.T) {
		s := newStore(t, client, "l3-model-grant", lifecycleModel)
		s.mustWrite(Tuple("user:u", "grant", "doc:1", InlineCondition(`true`, nil)))
		require.Equal(t, Allowed, s.check("grant", "doc:1", nil))

		// The relation no longer allows $expression. The stored tuple does not
		// fit the model any more and is ignored.
		s.setModel(`model
  schema 1.1

type user

type doc
  relations
    define grant: [user]
`)
		require.Equal(t, Denied, s.check("grant", "doc:1", nil))
		require.Equal(t, Denied, s.listObjects("user:u", "grant", "doc:1", nil))
		require.Equal(t, Denied, s.listUsers("user:u", "grant", "doc:1", nil))
	})

	t.Run("model_change_removes_expression_from_a_deny_rule", func(t *testing.T) {
		s := newStore(t, client, "l3-model-deny", lifecycleModel)
		s.mustWrite(
			Tuple("user:u", "base", "doc:1", nil),
			Tuple("user:u", "blocked", "doc:1", InlineCondition(`true`, nil)),
		)
		require.Equal(t, Denied, s.check("deny", "doc:1", nil))

		// The deny relation no longer allows $expression, so the deny rule is
		// ignored and the user is allowed. This follows the rule that a tuple
		// which does not fit the model is not read. It is the one lifecycle
		// change that opens access without an error, and it is asserted here
		// so the behavior stays visible.
		s.setModel(`model
  schema 1.1

type user

type doc
  relations
    define base: [user]
    define blocked: [user]
    define deny: base but not blocked
`)
		require.Equal(t, Allowed, s.check("deny", "doc:1", nil))
	})

	t.Run("model_change_from_named_condition_to_expression", func(t *testing.T) {
		s := newStore(t, client, "l3-model-named", `model
  schema 1.1

type user

type doc
  relations
    define grant: [user with always]

condition always(unused: string) {
  true
}
`)
		s.mustWrite(Tuple("user:u", "grant", "doc:1", NamedCondition("always")))
		require.Equal(t, Allowed, s.check("grant", "doc:1", map[string]any{"unused": "x"}))

		s.setModel(lifecycleModel)
		require.Equal(t, Denied, s.check("grant", "doc:1", map[string]any{"unused": "x"}))
	})

	t.Run("replace_an_expression_in_one_request", func(t *testing.T) {
		s := newStore(t, client, "l3-replace", lifecycleModel)
		s.mustWrite(Tuple("user:u", "grant", "doc:1", channelIsA))

		_, err := s.client.Write(context.Background(), &openfgav1.WriteRequest{
			StoreId:              s.id,
			AuthorizationModelId: s.modelID,
			Deletes: &openfgav1.WriteRequestDeletes{TupleKeys: []*openfgav1.TupleKeyWithoutCondition{
				{User: "user:u", Relation: "grant", Object: "doc:1"},
			}},
			Writes: &openfgav1.WriteRequestWrites{TupleKeys: []*openfgav1.TupleKey{
				Tuple("user:u", "grant", "doc:1", InlineCondition(`channel == 'B'`, map[string]string{"channel": "string"})),
			}},
		})
		// Known gap: the request is rejected today. When it is accepted, this
		// assertion fails and the scenario should assert the new expression.
		require.Errorf(t, err, "[GAP CLOSED] improvement %d (%s): assert the replaced expression here",
			GapAtomicReplace.Improvement, GapAtomicReplace.Summary)

		// The rejected request must leave the stored policy as it was.
		require.Equal(t, Allowed, s.check("grant", "doc:1", map[string]any{"channel": "A"}))
		require.Equal(t, Denied, s.check("grant", "doc:1", map[string]any{"channel": "B"}))
	})

	t.Run("edit_an_expression_with_delete_then_write", func(t *testing.T) {
		s := newStore(t, client, "l3-edit", lifecycleModel)
		s.mustWrite(Tuple("user:u", "grant", "doc:1", channelIsA))
		require.Equal(t, Allowed, s.check("grant", "doc:1", map[string]any{"channel": "A"}))

		require.NoError(t, s.remove("user:u", "grant", "doc:1"))
		require.Equal(t, Denied, s.check("grant", "doc:1", map[string]any{"channel": "A"}))

		s.mustWrite(Tuple("user:u", "grant", "doc:1", InlineCondition(`channel == 'B'`, map[string]string{"channel": "string"})))
		require.Equal(t, Denied, s.check("grant", "doc:1", map[string]any{"channel": "A"}))
		require.Equal(t, Allowed, s.check("grant", "doc:1", map[string]any{"channel": "B"}))
	})

	t.Run("write_again_with_on_duplicate_ignore", func(t *testing.T) {
		s := newStore(t, client, "l3-duplicate", lifecycleModel)
		s.mustWrite(Tuple("user:u", "grant", "doc:1", channelIsA))

		again := func(cond *openfgav1.RelationshipCondition) error {
			_, err := s.client.Write(context.Background(), &openfgav1.WriteRequest{
				StoreId:              s.id,
				AuthorizationModelId: s.modelID,
				Writes: &openfgav1.WriteRequestWrites{
					OnDuplicate: "ignore",
					TupleKeys:   []*openfgav1.TupleKey{Tuple("user:u", "grant", "doc:1", cond)},
				},
			})
			return err
		}
		require.NoError(t, again(proto.Clone(channelIsA).(*openfgav1.RelationshipCondition)), "the same expression is ignored")
		require.Error(t, again(InlineCondition(`channel == 'B'`, map[string]string{"channel": "string"})),
			"a different expression must not be silently ignored")
		require.Equal(t, Allowed, s.check("grant", "doc:1", map[string]any{"channel": "A"}))
	})

	t.Run("read_returns_the_expression_as_written", func(t *testing.T) {
		s := newStore(t, client, "l3-read", lifecycleModel)
		s.mustWrite(Tuple("user:u", "grant", "doc:1", channelIsA))

		resp, err := s.client.Read(context.Background(), &openfgav1.ReadRequest{
			StoreId:  s.id,
			TupleKey: &openfgav1.ReadRequestTupleKey{User: "user:u", Relation: "grant", Object: "doc:1"},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetTuples(), 1)
		require.True(t, proto.Equal(channelIsA, resp.GetTuples()[0].GetKey().GetCondition()))
	})

	t.Run("contextual_tuple_with_an_expression", func(t *testing.T) {
		s := newStore(t, client, "l3-contextual", lifecycleModel)
		s.mustWrite(Tuple("user:u", "grant", "doc:stored", channelIsA))

		allow := Tuple("user:u", "grant", "doc:1", InlineCondition(`true`, nil))
		deny := Tuple("user:u", "grant", "doc:1", InlineCondition(`false`, nil))
		conditional := Tuple("user:u", "grant", "doc:1", channelIsA)
		require.Equal(t, Allowed, s.check("grant", "doc:1", nil, allow))
		require.Equal(t, Denied, s.check("grant", "doc:1", nil, deny))
		require.Equal(t, Allowed, s.check("grant", "doc:1", map[string]any{"channel": "A"}, conditional))
		require.Equal(t, Denied, s.check("grant", "doc:1", map[string]any{"channel": "B"}, conditional))
		require.Equal(t, Error, s.check("grant", "doc:1", nil, conditional))

		// A contextual tuple takes the place of the stored tuple with the same key.
		override := Tuple("user:u", "grant", "doc:stored", InlineCondition(`true`, nil))
		require.Equal(t, Denied, s.check("grant", "doc:stored", map[string]any{"channel": "B"}))
		require.Equal(t, Allowed, s.check("grant", "doc:stored", map[string]any{"channel": "B"}, override))
	})

	t.Run("same_tuple_different_contexts_in_sequence", func(t *testing.T) {
		s := newStore(t, client, "l3-sequence", lifecycleModel)
		s.mustWrite(
			Tuple("user:u", "base", "doc:1", nil),
			Tuple("user:u", "grant", "doc:1", channelIsA),
			Tuple("user:u", "blocked", "doc:1", channelIsA),
		)
		steps := []struct {
			reqCtx map[string]any
			grant  Result
		}{
			{map[string]any{"channel": "A"}, Allowed},
			{map[string]any{"channel": "B"}, Denied},
			{nil, Error},
			{map[string]any{"channel": float64(7)}, Error},
			{map[string]any{"channel": "A"}, Allowed},
			{map[string]any{"channel": "B"}, Denied},
			{map[string]any{"channel": "A", "unrelated": true}, Allowed},
			{nil, Error},
		}
		// Asked with default consistency on purpose: a cached answer for one
		// context must never be returned for another.
		for round := range 2 {
			for i, step := range steps {
				require.Equalf(t, step.grant, s.cachedCheck("user:u", "grant", "doc:1", step.reqCtx), "round %d step %d grant", round, i)
				require.Equalf(t, step.grant.Not(), s.cachedCheck("user:u", "deny", "doc:1", step.reqCtx), "round %d step %d deny", round, i)
			}
		}
	})

	t.Run("batch_check_same_tuple_different_contexts", func(t *testing.T) {
		s := newStore(t, client, "l3-batch", lifecycleModel)
		s.mustWrite(Tuple("user:u", "grant", "doc:1", channelIsA))

		cells := []*Cell{
			{User: "user:u", Relation: "grant", Object: "doc:1", Context: map[string]any{"channel": "A"}},
			{User: "user:u", Relation: "grant", Object: "doc:1", Context: map[string]any{"channel": "B"}},
			{User: "user:u", Relation: "grant", Object: "doc:1"},
			{User: "user:u", Relation: "grant", Object: "doc:1", Context: map[string]any{"channel": float64(7)}},
			{User: "user:u", Relation: "grant", Object: "doc:1", Context: map[string]any{"channel": "7"}},
			{User: "user:u", Relation: "grant", Object: "doc:1", Context: map[string]any{"channel": "A"}},
		}
		got := doBatchCheck(context.Background(), t, client, s.id, s.modelID, cells)
		require.Equal(t, []Result{Allowed, Denied, Error, Error, Denied, Allowed}, got)
	})

	t.Run("tool_argument_renamed_or_added", func(t *testing.T) {
		s := newStore(t, client, "l3-arguments", lifecycleModel)
		s.mustWrite(
			Tuple("user:u", "base", "doc:1", nil),
			Tuple("user:u", "grant", "doc:1", channelIsA),
			Tuple("user:u", "blocked", "doc:1", channelIsA),
		)
		// The policy was written for an argument named channel. The tool now
		// calls it channel_id, so the policy's parameter is missing.
		renamed := map[string]any{"channel_id": "A"}
		require.Equal(t, Error, s.check("grant", "doc:1", renamed))
		require.Equal(t, Error, s.check("deny", "doc:1", renamed), "a deny rule with a missing argument must not allow")

		// The tool gained an argument. The policy does not mention it and is unaffected.
		added := map[string]any{"channel": "A", "thread": "t1"}
		require.Equal(t, Allowed, s.check("grant", "doc:1", added))
		require.Equal(t, Denied, s.check("deny", "doc:1", added))
	})

	t.Run("policies_do_not_follow_a_renamed_object", func(t *testing.T) {
		s := newStore(t, client, "l3-rename", lifecycleModel)
		s.mustWrite(Tuple("user:u", "grant", "doc:old_name", InlineCondition(`true`, nil)))
		require.Equal(t, Allowed, s.check("grant", "doc:old_name", nil))
		require.Equal(t, Denied, s.check("grant", "doc:new_name", nil))
	})

	t.Run("deleted_policy_stops_applying", func(t *testing.T) {
		s := newStore(t, client, "l3-delete", lifecycleModel)
		s.mustWrite(
			Tuple("user:u", "base", "doc:1", nil),
			Tuple("user:u", "blocked", "doc:1", InlineCondition(`true`, nil)),
		)
		require.Equal(t, Denied, s.check("deny", "doc:1", nil))
		require.NoError(t, s.remove("user:u", "blocked", "doc:1"))
		require.Equal(t, Allowed, s.check("deny", "doc:1", nil))
	})
}
