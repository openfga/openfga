package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/server/commands/reverseexpand/pipeline"
	"github.com/openfga/openfga/pkg/server/commands/reverseexpand/pipeline/obj"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

type StaticObjectStore struct {
	m map[string]map[string][]string
}

func (s *StaticObjectStore) Add(tuples ...string) {
	if s.m == nil {
		s.m = make(map[string]map[string][]string)
	}
	for _, tuple := range tuples {
		objectRelation, user, _ := strings.Cut(tuple, "@")
		if _, ok := s.m[user]; !ok {
			s.m[user] = make(map[string][]string)
		}
		objectMap := s.m[user]
		object, relation, _ := strings.Cut(objectRelation, "#")
		objectType, _, _ := strings.Cut(object, ":")
		objectMap[objectType+"#"+relation] = append(objectMap[objectType+"#"+relation], object)
	}
}

func (s *StaticObjectStore) Read(ctx context.Context, q pipeline.ObjectQuery) iter.Seq[pipeline.Item] {
	var objects []string
	for _, user := range q.Users {
		if objectMap, ok := s.m[user]; ok {
			if ids, ok := objectMap[q.ObjectType+"#"+q.Relation]; ok {
				objects = append(objects, ids...)
			}
		}
	}

	return func(yield func(o pipeline.Item) bool) {
		for _, o := range objects {
			if !yield(pipeline.Item{Value: o}) {
				break
			}
		}
	}
}

type ErrorObjectStore struct {
	err error
}

func NewErrorObjectStore(err error) *ErrorObjectStore {
	return &ErrorObjectStore{err: err}
}

func (e *ErrorObjectStore) Read(_ context.Context, _ pipeline.ObjectQuery) iter.Seq[pipeline.Item] {
	return func(yield func(pipeline.Item) bool) {
		yield(pipeline.Item{Err: e.err})
	}
}

func TestPipelineShutdown(t *testing.T) {
	const bufferSize int = 4
	const chunkSize int = 1

	const dsl string = `
	model
	  schema 1.1

	type user

	type org
	  relations
	  	define reader: [user]
	  	define writer: [user]
	  	define blocked: [user]
	  	define moderator: (reader and writer) but not blocked
	    define member: moderator or member from parent
	    define parent: [team]

	type team
	  relations
	  	define reader: [user]
	  	define writer: [user]
	  	define blocked: [user]
	  	define moderator: (reader and writer) but not blocked
	    define member: moderator or member from parent
	    define parent: [org]

	type document
	  relations
	    define viewer: [org#member, team#member]
	`

	const user string = "user:bob"
	const nestLevel int = 100

	var tuples []string

	var child string
	var parent string

	var documents []string

	for i := range nestLevel {
		if i%2 == 0 {
			child = "team:" + strconv.Itoa(i)
			parent = "org:" + strconv.Itoa(i+1)
		} else {
			child = "org:" + strconv.Itoa(i)
			parent = "team:" + strconv.Itoa(i+1)
		}
		tuples = append(tuples, fmt.Sprintf("%s#parent@%s", child, parent))
		documents = append(documents, fmt.Sprintf("document:%d", i))
		tuples = append(tuples, fmt.Sprintf("document:%d#viewer@%s#member", i, child))

		for j := range 20 {
			tuples = append(
				tuples,
				fmt.Sprintf("%s#reader@user:%d", child, j),
				fmt.Sprintf("%s#writer@user:%d", child, j),
				fmt.Sprintf("%s#blocked@user:%d", child, j),
			)
		}
	}

	tuples = append(tuples, fmt.Sprintf("%s#reader@%s", parent, user), fmt.Sprintf("%s#writer@%s", parent, user))

	slices.Reverse(documents)

	var ds StaticObjectStore
	ds.Add(tuples...)

	model := testutils.MustTransformDSLToProtoWithID(dsl)

	typesys, err := typesystem.NewAndValidate(
		context.Background(),
		model,
	)

	require.NoError(t, err)

	g := typesys.GetWeightedGraph()

	spec := pipeline.Spec{
		ObjectType: "document",
		Relation:   "viewer",
		User:       "user:bob",
	}

	pl := pipeline.New(g, &ds, pipeline.WithBufferCapacity(bufferSize), pipeline.WithChunkSize(chunkSize))

	t.Run("NoAbandon", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		seq, err := pl.Expand(context.Background(), spec)
		require.NoError(t, err)

		values := make([]string, 0, len(documents))
		for object := range seq {
			value, err := object.Object()
			require.NoError(t, err)
			values = append(values, value)
		}
		require.ElementsMatch(t, documents, values)
	})

	t.Run("AbandonWithoutPull", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		_, err := pl.Expand(context.Background(), spec)
		require.NoError(t, err)
	})

	t.Run("AbandonAfterPull", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		seq, err := pl.Expand(context.Background(), spec)
		require.NoError(t, err)

		var value string
		for objects := range seq {
			value, err = objects.Object()
			break
		}
		require.NoError(t, err)
		require.NotEmpty(t, value)
	})

	t.Run("AbandonMidProcessing", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		seq, err := pl.Expand(context.Background(), spec)
		require.NoError(t, err)

		var count int
		limit := nestLevel / 2
		for range seq {
			count++
			if count >= limit {
				break
			}
		}
	})

	t.Run("CancelWithoutPull", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctx, cancel := context.WithCancel(context.Background())

		_, err := pl.Expand(ctx, spec)
		require.NoError(t, err)

		cancel()
	})

	t.Run("CancelBeforePull", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctx, cancel := context.WithCancel(context.Background())

		seq, err := pl.Expand(ctx, spec)
		require.NoError(t, err)

		cancel()
		for range seq {
			t.Fatal("received item after context canceled")
		}
	})

	t.Run("CancelAfterPull", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctx, cancel := context.WithCancel(context.Background())

		defer cancel()

		seq, err := pl.Expand(ctx, spec)
		require.NoError(t, err)

		var value string
		for object := range seq {
			var v string
			v, err = object.Object()
			if err == nil {
				value = v
			}
			cancel()
		}
		require.NotEmpty(t, value)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("CancelMidProcessing", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctx, cancel := context.WithCancel(context.Background())

		defer cancel()

		seq, err := pl.Expand(ctx, spec)
		require.NoError(t, err)

		var count int
		limit := nestLevel / 2
		for object := range seq {
			_, err = object.Object()
			count++
			if count >= limit {
				cancel()
			}
		}
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("TimeoutAfterPull", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)

		defer cancel()

		seq, err := pl.Expand(ctx, spec)
		require.NoError(t, err)

		var count int
		for object := range seq {
			_, err = object.Object()
			if count > bufferSize+chunkSize+1 {
				t.Fatalf("received unexpected value")
			}

			if count == 0 {
				// wait long enough for timeout to occur
				time.Sleep(200 * time.Millisecond)
			}
			count++
		}
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("TimeoutBeforePull", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)

		seq, err := pl.Expand(ctx, spec)
		require.NoError(t, err)

		defer cancel()

		// wait long enough for timeout to occur
		time.Sleep(2 * time.Millisecond)

		for range seq {
			t.Fatal("received value after context deadline exceeded")
		}
	})
}

var ErrSignal = errors.New("signal error")

func TestPipelineError(t *testing.T) {
	const bufferSize int = 4
	const chunkSize int = 1

	const dsl string = `
	model
	  schema 1.1

	type user

	type org
	  relations
	  	define reader: [user]
	  	define writer: [user]
	  	define blocked: [user]
	  	define moderator: (reader and writer) but not blocked
	    define member: moderator or member from parent
	    define parent: [team]

	type team
	  relations
	  	define reader: [user]
	  	define writer: [user]
	  	define blocked: [user]
	  	define moderator: (reader and writer) but not blocked
	    define member: moderator or member from parent
	    define parent: [org]

	type document
	  relations
	    define viewer: [org#member, team#member]
	`
	ds := NewErrorObjectStore(ErrSignal)

	model := testutils.MustTransformDSLToProtoWithID(dsl)

	typesys, err := typesystem.NewAndValidate(
		context.Background(),
		model,
	)

	require.NoError(t, err)

	g := typesys.GetWeightedGraph()

	spec := pipeline.Spec{
		ObjectType: "document",
		Relation:   "viewer",
		User:       "user:bob",
	}

	pl := pipeline.New(g, ds, pipeline.WithBufferCapacity(bufferSize), pipeline.WithChunkSize(chunkSize))

	t.Run("ShouldReturnErrorThenEnd", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		seq, err := pl.Expand(context.Background(), spec)
		require.NoError(t, err)

		for obj := range seq {
			value, err := obj.Object()
			require.Empty(t, value)
			require.ErrorIs(t, err, ErrSignal)
		}
	})
}

type testcase struct {
	name       string
	model      string
	tuples     []string
	objectType string
	relation   string
	user       string
	expected   []string
}

func evaluate(t *testing.T, tc testcase, seq iter.Seq[pipeline.Item]) {
	var results []string
	for object := range seq {
		value, err := object.Object()
		require.NoError(t, err)
		results = append(results, value)
	}
	require.ElementsMatch(t, tc.expected, results)
}

var cases = []testcase{
	{
		name: "policy",
		model: `
		model
		schema 1.1

		type user

		type group
			relations
				define member: [user, group#member]

		type role
			relations
				define member: [user, group#member]

		type policy
			relations
				define denied: [role]
				define allowed: [role]
				define can_perform: member from allowed but not member from denied

		type resource
			relations
				define read_policy: [policy]
				define write_policy: [policy]
				define reader: can_perform from read_policy
				define writer: can_perform from write_policy
		`,
		tuples: []string{
			"group:admin#member@user:bob",
			"group:employee#member@group:admin#member",
			"group:employee#member@user:bob",
			"group:employee#member@user:betty",
			"group:terminated#member@user:betty",
			"role:admin#member@group:admin#member",
			"role:read_only#member@group:employee#member",
			"role:blocked#member@group:terminated#member",
			"policy:document_writer#allowed@role:admin",
			"policy:document_writer#denied@role:blocked",
			"policy:document_reader#allowed@role:admin",
			"policy:document_reader#allowed@role:read_only",
			"policy:document_reader#denied@role:blocked",
			"resource:document_1#write_policy@policy:document_writer",
			"resource:document_1#read_policy@policy:document_reader",
			"resource:document_2#write_policy@policy:document_writer",
			"resource:document_2#read_policy@policy:document_reader",
		},
		objectType: "resource",
		relation:   "writer",
		user:       "user:bob",
		expected:   []string{"resource:document_1", "resource:document_2"},
	},
	{
		name: "recursive_ttu_intersection",
		model: `
		model
		schema 1.1

		type user

		type org
			relations
				define member: [user] or member from parent
				define parent: [org]
				define admin: [user]
				define moderator: admin and member

		type doc
			relations
				define viewer: [org#moderator]
		`,
		tuples: []string{
			"org:1#member@user:1",
			"org:2#parent@org:1",
			"org:2#admin@user:1",
			"doc:1#viewer@org:2#moderator",
		},
		objectType: "doc",
		relation:   "viewer",
		user:       "user:1",
		expected:   []string{"doc:1"},
	},
	{
		name: "recursive_userset_intersection",
		model: `
		model
		schema 1.1

		type user

		type org
			relations
				define member: [user, org#member]
				define admin: [user]
				define moderator: admin and member

		type doc
			relations
				define viewer: [org#moderator]
		`,
		tuples: []string{
			"org:2#member@user:1",
			"org:1#admin@user:1",
			"org:1#member@org:2#member",
			"doc:1#viewer@org:1#moderator",
		},
		objectType: "doc",
		relation:   "viewer",
		user:       "user:1",
		expected:   []string{"doc:1"},
	},
	{
		name: "recursive_recursion",
		model: `
		model
		schema 1.1

		type user

		type org
			relations
				define parent: [org]
				define member: [user, org#member] or member from parent
				define admin: [user]
				define moderator: admin and member

		type doc
			relations
				define viewer: [org#moderator]
		`,
		tuples: []string{
			"org:1#member@user:bob",
			"org:2#member@org:1#member",
			"org:3#parent@org:2",
			"org:3#admin@user:bob",
			"doc:1#viewer@org:3#moderator",
		},
		objectType: "doc",
		relation:   "viewer",
		user:       "user:bob",
		expected:   []string{"doc:1"},
	},
	{
		name: "ttu_recursive",
		model: `model
				  schema 1.1

				type user
				type org
				  relations
					define parent: [org]
					define ttu_recursive: [user] or ttu_recursive from parent
		`,
		tuples: []string{
			"org:a#ttu_recursive@user:justin",
			"org:b#parent@org:a", // org:a is parent of b
			"org:c#parent@org:b", // org:b is parent of org:c
			"org:d#parent@org:c", // org:c is parent of org:d
		},
		objectType: "org",
		relation:   "ttu_recursive",
		user:       "user:justin",
		expected:   []string{"org:a", "org:b", "org:c", "org:d"},
	},
	{
		name: "userset_as_user",
		model: `
		model
		schema 1.1

		type user

		type group
			relations
				define member: [user]
		type document
			relations
				define viewer: [group#member]
		`,
		tuples: []string{
			"document:1#viewer@group:x#member",
			"group:x#member@user:aardvark",
		},
		objectType: "document",
		relation:   "viewer",
		user:       "group:x#member",
		expected:   []string{"document:1"},
	},
	{
		name: "err_and_true_return_err",
		model: `
		model
		schema 1.1

		type user

		type resource
			relations
				define a1: [user]
				define a2: [resource#a1]
				define a3: [resource#a2]
				define a4: [resource#a3]
              			define a5: [resource#a4]
              			define a6: [resource#a5]
              			define a7: [resource#a6]
              			define a8: [resource#a7]
              			define a9: [resource#a8]
              			define a10: [resource#a9]
              			define a11: [resource#a10]
              			define a12: [resource#a11]
              			define a13: [resource#a12]
              			define a14: [resource#a13]
              			define a15: [resource#a14]
              			define a16: [resource#a15]
              			define a17: [resource#a16]
              			define a18: [resource#a17]
              			define a19: [resource#a18]
              			define a20: [resource#a19]
              			define a21: [resource#a20]
              			define a22: [resource#a21]
              			define a23: [resource#a22]
              			define a24: [resource#a23]
              			define a25: [resource#a24]
              			define a26: [resource#a25]
              			define a27: [resource#a26]
              			define can_view: a27
		`,
		tuples: []string{
			"resource:1#a27@resource:1#a26",
			"resource:1#a26@resource:1#a25",
			"resource:1#a25@resource:1#a24",
			"resource:1#a24@resource:1#a23",
			"resource:1#a23@resource:1#a22",
			"resource:1#a22@resource:1#a21",
			"resource:1#a21@resource:1#a20",
			"resource:1#a20@resource:1#a19",
			"resource:1#a19@resource:1#a18",
			"resource:1#a18@resource:1#a17",
			"resource:1#a17@resource:1#a16",
			"resource:1#a16@resource:1#a15",
			"resource:1#a15@resource:1#a14",
			"resource:1#a14@resource:1#a13",
			"resource:1#a13@resource:1#a12",
			"resource:1#a12@resource:1#a11",
			"resource:1#a11@resource:1#a10",
			"resource:1#a10@resource:1#a9",
			"resource:1#a9@resource:1#a8",
			"resource:1#a8@resource:1#a7",
			"resource:1#a7@resource:1#a6",
			"resource:1#a6@resource:1#a5",
			"resource:1#a5@resource:1#a4",
			"resource:1#a4@resource:1#a3",
			"resource:1#a3@resource:1#a2",
			"resource:1#a2@resource:1#a1",
			"resource:1#a1@user:maria",
		},
		objectType: "resource",
		relation:   "can_view",
		user:       "user:maria",
		expected:   []string{"resource:1"},
	},
	{
		name: "double_ttu",
		model: `
		model
		schema 1.1

		type user

		type directs
			relations
				define direct: [user]
				define mixed: [user]
				define da: [user]

		type usersets-user
			relations
				define da: [user]
				define mixed: [directs#direct, user] and da

		type wrapper
			relations
				define parent: [usersets-user, directs]
				define assigned: mixed from parent but not da from parent
		`,
		tuples: []string{
			"directs:1#mixed@user:bob",
			"usersets-user:1#da@user:bob",
			"directs:2#mixed@user:bob",
			"wrapper:1#parent@directs:1",
			"wrapper:1#parent@usersets-user:1",
			"wrapper:2#parent@directs:2",
		},
		objectType: "wrapper",
		relation:   "assigned",
		user:       "user:bob",
		expected:   []string{"wrapper:2"},
	},
	{
		name: "simple_userset_child_computed_userset",
		model: `
		model
		schema 1.1

		type user

		type group
			relations
				define member: [user]
				define member_c1: member
				define member_c2: member_c1
				define member_c3: member_c2
				define member_c4: member_c3

		type folder
			relations
				define viewer: [group#member_c4]
		`,
		tuples: []string{
			"group:fga#member@user:anne",
			"folder:1#viewer@group:fga#member_c4",
		},
		objectType: "folder",
		relation:   "viewer",
		user:       "group:fga#member",
		expected:   []string{"folder:1"},
	},
	{
		name: "simple_userset_child_wildcard",
		model: `
		model
            	schema 1.1
			
          	type user
	  
          	type user2
		
          	type group
            		relations
              			define member: [user, user:*, user2, user2:*]

          	type folder
            		relations
              			define viewer: [group#member]
		`,
		tuples: []string{
			"group:fga#member@user:*",
			"group:engineering#member@user:maria",
			"folder:1#viewer@group:fga#member",
			"folder:2#viewer@group:engineering#member",
		},
		objectType: "folder",
		relation:   "viewer",
		user:       "user2:foo",
		expected:   []string{},
	},
	{
		name: "ttu_mix_with_userset",
		model: `
		model
		schema 1.1

		type user

		type group
			relations
				define member: [user, user:*]
		
		type folder
			relations
				define viewer: [user, group#member]

		type document
			relations
				define parent: [folder]
				define viewer: viewer from parent
		`,
		tuples: []string{
			"group:1#member@user:anne",
			"group:2#member@user:anne",
			"group:2#member@user:bob",
			"group:1#member@user:charlie",
			"folder:a#viewer@group:1#member",
			"folder:a#viewer@group:2#member",
			"folder:a#viewer@user:daemon",
			"document:a#parent@folder:a",
			"group:3#member@user:elle",
			"group:public#member@user:*",
			"folder:public#viewer@group:public#member",
			"document:public#parent@folder:public",
		},
		objectType: "document",
		relation:   "viewer",
		user:       "user:anne",
		expected:   []string{"document:a", "document:public"},
	},
	{
		name: "wild_card",
		model: `model
				  schema 1.1
					type user
				  type group
					relations
					  define member: [user, user:*]
				  type folder
					relations
					  define viewer: [user,group#member]
				  type document
					relations
					  define parent: [folder]
					  define viewer: viewer from parent
		`,
		tuples: []string{
			"group:1#member@user:anne",
			"group:1#member@user:charlie",
			"group:2#member@user:anne",
			"group:2#member@user:bob",
			"group:3#member@user:elle",
			"group:public#member@user:*",
			"document:a#parent@folder:a",
			"document:public#parent@folder:public",
			"folder:a#viewer@group:1#member",
			"folder:a#viewer@group:2#member",
			"folder:a#viewer@user:daemon",
			"folder:public#viewer@group:public#member",
		},
		objectType: "document",
		relation:   "viewer",
		user:       "user:*",
		expected:   []string{"document:public"},
	},
	{
		name: "computed",
		model: `
		model
		schema 1.1

		type user

		type document
			relations
				define viewer: [team#member]

		type team
			relations
				define rel1: [user]
				define rel2: rel1
				define rel3: rel2
				define member: rel3
		`,
		tuples: []string{
			"team:1#rel1@user:justin",
			"document:1#viewer@team:1#member",
			"document:2#viewer@team:1#member",
			"document:3#viewer@team:2#member",
		},
		objectType: "document",
		relation:   "viewer",
		user:       "user:justin",
		expected:   []string{"document:1", "document:2"},
	},
	{
		name: "union",
		model: `
		model
		schema 1.1

		type user

		type document
			relations
				define rel1: [user]
				define rel2: [user]
				define rel3: [user]
				define viewer: rel1 or rel2 or rel3
		`,
		tuples: []string{
			"document:1#rel1@user:justin",
			"document:1#rel2@user:justin",
			"document:1#rel3@user:justin",
			"document:1#rel1@user:bob",
			"document:1#rel2@user:bob",
			"document:1#rel3@user:bob",
			"document:2#rel1@user:justin",
			"document:2#rel2@user:justin",
			"document:2#rel1@user:bob",
			"document:2#rel2@user:bob",
			"document:2#rel3@user:bob",
			"document:3#rel1@user:justin",
			"document:3#rel2@user:justin",
			"document:3#rel3@user:justin",
		},
		objectType: "document",
		relation:   "viewer",
		user:       "user:justin",
		expected:   []string{"document:1", "document:2", "document:3"},
	},
	{
		name: "ttu",
		model: `
		model
		schema 1.1

		type user

		type company
			relations
				define employee: [user]

		type document
			relations
				define viewer: employee from parent
				define parent: [company]
		`,
		tuples: []string{
			"company:auth0#employee@user:bob",
			"document:1#parent@company:auth0",
			"document:2#parent@company:auth0",
			"document:3#parent@company:fga",
		},
		objectType: "document",
		relation:   "viewer",
		user:       "user:bob",
		expected:   []string{"document:1", "document:2"},
	},
	{
		name: "ttu_in_intersection",
		model: `
		model
		schema 1.1

		type user

		type company
			relations
				define employee: [user]

		type document
			relations
				define viewer: employee from admin and employee from relative
				define relative: [company]
				define admin: [company]
		`,
		tuples: []string{
			"company:auth0#employee@user:bob",
			"document:1#relative@company:auth0",
			"document:1#admin@company:auth0",
			"document:2#relative@company:auth0",
			"document:3#admin@company:auth0",
		},
		objectType: "document",
		relation:   "viewer",
		user:       "user:bob",
		expected:   []string{"document:1"},
	},
	{
		name: "ttu_in_cycle_with_union",
		model: `
		model
		schema 1.1

		type user

		type company
			relations
				define employee: [user] or employee from parent
				define parent: [org, company]

		type org
			relations
				define employee: [user] or employee from parent
				define parent: [company, org]

		type document
			relations
				define viewer: employee from parent
				define parent: [company, org]
		`,
		tuples: []string{
			"company:auth0#employee@user:bob",
			"document:1#parent@company:auth0",
			"org:auth0#employee@user:bob",
			"company:auth0#parent@org:auth0",
			"org:auth0#parent@company:auth0",
			"document:2#parent@org:auth0",
			"document:3#parent@company:auth0",
			"document:4#parent@company:fga",
			"document:5#parent@org:fga",
		},
		objectType: "document",
		relation:   "viewer",
		user:       "user:bob",
		expected:   []string{"document:1", "document:2", "document:3"},
	},
	{
		name: "ttu_in_cycle_with_intersection",
		model: `
		model
		schema 1.1

		type user

		type company
			relations
				define employee: [user] and employee from parent
				define parent: [org]

		type org
			relations
				define employee: [user]

		type document
			relations
				define viewer: employee from parent
				define parent: [company, org]
		`,
		tuples: []string{
			"company:auth0#employee@user:bob",
			"document:1#parent@company:auth0",
			"org:auth0#employee@user:bob",
			"company:auth0#parent@org:auth0",
			"document:2#parent@org:auth0",
			"document:3#parent@company:fga",
			"document:4#parent@org:fga",
			"company:fga#employee@user:bob",
		},
		objectType: "document",
		relation:   "viewer",
		user:       "user:bob",
		expected:   []string{"document:1", "document:2"},
	},
	{
		name: "ttu_with_two_directs",
		model: `
		model
		schema 1.1

		type user

		type company
			relations
				define employee: [user]

		type org
			relations
				define employee: [user]

		type document
			relations
				define viewer: employee from parent
				define parent: [company, org]
		`,
		tuples: []string{
			"company:auth0#employee@user:bob",
			"org:fga#employee@user:bob",
			"document:1#parent@company:auth0",
			"document:2#parent@company:auth0",
			"document:3#parent@org:fga",
		},
		objectType: "document",
		relation:   "viewer",
		user:       "user:bob",
		expected:   []string{"document:1", "document:2", "document:3"},
	},
	{
		name: "ttu_with_complexity",
		model: `
		model
		schema 1.1

		type user

		type company
			relations
				define rel1: [user]
				define employee: [user] or rel1

		type org
			relations
				define rel1: [user]
				define employee: [user] and rel1

		type document
			relations
				define viewer: employee from parent
				define parent: [company, org]
		`,
		tuples: []string{
			"company:auth0#employee@user:bob",
			"org:fga#employee@user:bob",
			"org:fga#rel1@user:bob",
			"org:x#employee@user:bob",
			"document:1#parent@company:auth0",
			"document:2#parent@company:auth0",
			"document:3#parent@org:fga",
			"document:4#parent@org:x",
		},
		objectType: "document",
		relation:   "viewer",
		user:       "user:bob",
		expected:   []string{"document:1", "document:2", "document:3"},
	},
	{
		name: "intersection",
		model: `
		model
		schema 1.1

		type user

		type document
			relations
				define rel1: [user]
				define rel2: [user]
				define rel3: [user]
				define viewer: rel1 and rel2 and rel3
		`,
		tuples: []string{
			"document:1#rel1@user:justin",
			"document:1#rel2@user:justin",
			"document:1#rel3@user:justin",
			"document:1#rel1@user:bob",
			"document:1#rel2@user:bob",
			"document:1#rel3@user:bob",
			"document:2#rel1@user:justin",
			"document:2#rel2@user:justin",
			"document:2#rel1@user:bob",
			"document:2#rel2@user:bob",
			"document:2#rel3@user:bob",
			"document:3#rel1@user:justin",
			"document:3#rel2@user:justin",
			"document:3#rel3@user:justin",
		},
		objectType: "document",
		relation:   "viewer",
		user:       "user:justin",
		expected:   []string{"document:1", "document:3"},
	},
	{
		name: "direct_userset",
		model: `
		model
		schema 1.1

		type user

		type document
			relations
				define viewer: [team#member]

		type team
			relations
				define member: [user]
		`,
		tuples: []string{
			"team:1#member@user:justin",
			"team:2#member@user:justin",
			"document:1#viewer@team:1#member",
			"document:2#viewer@team:1#member",
			"document:3#viewer@team:2#member",
			"document:4#viewer@team:2#member",
		},
		objectType: "document",
		relation:   "viewer",
		user:       "user:justin",
		expected:   []string{"document:1", "document:2", "document:3", "document:4"},
	},
	{
		name: "beast_mode",
		model: `
		model
		schema 1.1

		type user

		type document
			relations
				define viewer: [team#member, org#employee]

		type team
			relations
				define member: [user, document#viewer, org#employee]

		type org
			relations
				define employee: [user, document#viewer, team#member]
		`,
		tuples: []string{
			"team:1#member@user:justin",
			"team:2#member@user:justin",
			"org:1#employee@user:justin",
			"org:2#employee@user:justin",
			"org:3#employee@document:1#viewer",
			"org:4#employee@document:2#viewer",
			"team:3#member@document:3#viewer",
			"document:3#viewer@team:4#member",
			"team:4#member@user:justin",
			"document:5#viewer@team:3#member",
			"document:1#viewer@org:4#employee",
			"document:8#viewer@org:3#employee",
			"org:4#employee@user:justin",

			"document:a#viewer@team:a#member",
			"team:a#member@user:justin",
			"document:b#viewer@org:b#employee",
			"org:b#employee@document:a#viewer",
			// expect document:a, document:b

			"document:c#viewer@org:c#employee",
			"org:c#employee@user:justin",
			"document:d#viewer@team:b#member",
			"team:b#member@document:c#viewer",
			// expect document:c, document:d

			"document:e#viewer@team:e#member",
			"team:e#member@org:e#employee",
			"org:e#employee@user:justin",
			"document:f#viewer@org:f#employee",
			"org:f#employee@document:e#viewer",
			"document:g#viewer@team:g#member",
			"team:g#member@org:g#employee",
			"org:g#employee@document:f#viewer",
			// expect document:e, document:f, document:g

			"document:h#viewer@org:h#employee",
			"org:h#employee@team:h#member",
			"team:h#member@user:justin",
			"document:i#viewer@team:i#member",
			"team:i#member@document:h#viewer",
			"document:j#viewer@org:i#employee",
			"org:i#employee@team:j#member",
			"team:j#member@document:i#viewer",
			// expect document:h, document:i, document:j
		},
		objectType: "document",
		relation:   "viewer",
		user:       "user:justin",
		expected:   []string{"document:1", "document:3", "document:5", "document:8", "document:a", "document:b", "document:c", "document:d", "document:e", "document:f", "document:g", "document:h", "document:i", "document:j"},
	},
	{
		name: "mean_tuple_cycle",
		model: `
		model
		schema 1.1

		type user

		type document
			relations
				define viewer: [team#member, org#employee]

		type team
			relations
				define member: [user, document#viewer, org#employee]

		type org
			relations
				define employee: [user, document#viewer, team#member]
		`,
		tuples: []string{
			"team:1#member@user:justin",
			"team:2#member@user:justin",
			"org:1#employee@user:justin",
			"org:2#employee@user:justin",
			"org:3#employee@document:1#viewer",
			"org:4#employee@document:2#viewer",
			"team:3#member@document:3#viewer",
			"document:3#viewer@team:4#member",
			"team:4#member@user:justin",
			"document:5#viewer@team:3#member",
			"document:1#viewer@org:4#employee",
			"document:8#viewer@org:3#employee",
			"org:4#employee@user:justin",
			"document:0#viewer@org:0#employee",
			"org:0#employee@team:0#member",
			"team:0#member@org:00#employee",
			"org:00#employee@team:00#member",
			"team:00#member@document:8#viewer",
		},
		objectType: "document",
		relation:   "viewer",
		user:       "user:justin",
		expected:   []string{"document:0", "document:1", "document:3", "document:5", "document:8"},
	},
	{
		name: "indirect_userset",
		model: `
		model
		schema 1.1

		type user

		type document
			relations
				define viewer: [team#member]

		type team
			relations
				define member: [user, org#employee]

		type org
			relations
				define employee: [user, team#member]
		`,
		tuples: []string{
			"team:1#member@user:justin",
			"team:2#member@user:justin",
			"org:22#employee@team:2#member",
			"team:22#member@org:22#employee",
			"org:1#employee@user:justin",
			"org:2#employee@user:justin",
			"team:3#member@org:1#employee",
			"team:4#member@org:2#employee",
			"org:3#employee@team:3#member",
			"org:4#employee@team:4#member",
			"team:5#member@org:3#employee",
			"team:6#member@org:4#employee",
			"document:1#viewer@team:1#member",
			"document:2#viewer@team:2#member",
			"document:3#viewer@team:3#member",
			"document:4#viewer@team:4#member",
			"document:5#viewer@team:5#member",
			"document:6#viewer@team:6#member",
			"document:7#viewer@team:0#member",
			"document:22#viewer@team:22#member",
		},
		objectType: "document",
		relation:   "viewer",
		user:       "user:justin",
		expected:   []string{"document:1", "document:2", "document:3", "document:4", "document:5", "document:6", "document:22"},
	},
	{
		name: "recursive",
		model: `
		model
		schema 1.1

		type user

		type team
			relations
				define member: [user, team#member]

		type group
			relations
				define member: [user, team#member, group#member]

		type document
			relations
				define viewer: [group#member]
		`,
		tuples: []string{
			"team:fga#member@user:justin",
			"team:xyz#member@team:fga#member",
			"group:abc#member@team:xyz#member",
			"group:xyz#member@group:abc#member",
			"group:fga#member@group:xyz#member",
			"group:cncf#member@group:fga#member",
			"document:1#viewer@group:cncf#member",
			"team:1#member@user:justin",
			"team:2#member@team:1#member",
			"team:3#member@team:2#member",
			"group:2#member@team:3#member",
			"document:2#viewer@group:2#member",
		},
		objectType: "document",
		relation:   "viewer",
		user:       "user:justin",
		expected:   []string{"document:1", "document:2"},
	},
	{
		name: "tuple_cycle",
		model: `
		model
		schema 1.1

		type user

		type team
			relations
				define member: [user, team#member]
		`,
		tuples: []string{
			"team:fga#member@user:justin",
			"team:cncf#member@team:fga#member",
			"team:lnf#member@team:cncf#member",
		},
		objectType: "team",
		relation:   "member",
		user:       "user:justin",
		expected:   []string{"team:fga", "team:cncf", "team:lnf"},
	},
	{
		name: "simple_exclusion",
		model: `model
				  schema 1.1

				type user
				type org
				  relations
					define banned: [user]
					define member: [user] but not banned
		`,
		tuples: []string{
			"org:a#banned@user:bob",
			"org:b#member@user:bob",
			"org:a#member@user:bob",
		},
		objectType: "org",
		relation:   "member",
		user:       "user:bob",
		expected:   []string{"org:b"},
	},
	{
		name: "exclusion_on_itself",
		model: `model
					schema 1.1

				type user
				type org
				  relations
					define banned: [user]
					define member: banned but not banned
		`,
		tuples: []string{
			"org:a#banned@user:bob",
		},
		objectType: "org",
		relation:   "member",
		user:       "user:bob",
		expected:   []string{},
	},
	{
		name: "exclusion_no_connection_base",
		model: `model
					schema 1.1

				type user
				type user2
				type org
				  relations
					define banned: [user2]
					define member: [user] but not banned
		`,
		tuples: []string{
			"org:b#member@user:bob",
			"org:a#member@user:bob",
			"org:c#banned@user2:bob",
		},
		objectType: "org",
		relation:   "member",
		user:       "user2:bob",
		expected:   []string{},
	},
	{
		name: "exclusion_no_connection_exclusion_path",
		model: `model
					schema 1.1

				type user
				type user2
				type org
				  relations
					define banned: [user2]
					define member: [user] but not banned
		`,
		tuples: []string{
			"org:a#banned@user:bob",
			"org:b#member@user:bob",
			"org:a#member@user:bob",
		},
		objectType: "org",
		relation:   "member",
		user:       "user:bob",
		expected:   []string{"org:a", "org:b"},
	},
	{
		name: "simple_exclusion_no_direct_assignment",
		model: `model
				  schema 1.1

				type user
				type org
				  relations
					define banned: [user]
					define member: [user]
					define viewer: member but not banned
		`,
		tuples: []string{
			"org:a#banned@user:bob",
			"org:b#member@user:bob",
			"org:a#member@user:bob",
		},
		objectType: "org",
		relation:   "viewer",
		user:       "user:bob",
		expected:   []string{"org:b"},
	},
	{
		name: "simple_exclusion_multiple_direct_assignments",
		model: `model
				  schema 1.1

				type user
				type team
				  relations
					define member: [user]
				type org
				  relations
					define allowed: [user]
					define member: [user, team#member] but not allowed
		`,
		tuples: []string{
			"org:a#allowed@user:bob",
			"org:b#member@user:bob",
			"org:a#member@user:bob",
			"org:c#member@team:c#member",
			"team:c#member@user:bob",
		},
		objectType: "org",
		relation:   "member",
		user:       "user:bob",
		expected:   []string{"org:b", "org:c"},
	},
	{
		name: "simple_exclusion_multiple_direct_assignments_not_linked_1",
		model: `model
				  schema 1.1

				type user
				type user2
				type org
				  relations
					define allowed: [user]
					define member: [user, user2] but not allowed
		`,
		tuples: []string{
			"org:a#allowed@user:bob",
			"org:b#member@user:bob",
			"org:a#member@user:bob",
			"org:c#allowed@user:bob",
			"org:d#member@user2:bob", // bob is user2 and there should be no link
		},
		objectType: "org",
		relation:   "member",
		user:       "user:bob",
		expected:   []string{"org:b"},
	},
	{
		name: "ttu_with_exclusion",
		model: `model
				  schema 1.1

				type user
				type team
				  relations
					define member: [user] but not banned from parent
					define parent: [org]
				type org
				  relations
					define banned: [user]
		`,
		tuples: []string{
			"org:a#banned@user:bob",
			"team:1#parent@org:a",
			"team:1#member@user:bob",
			"org:b#banned@user:2",
			"team:2#parent@org:b",
			"team:2#member@user:bob",
		},
		objectType: "team",
		relation:   "member",
		user:       "user:bob",
		expected:   []string{"team:2"},
	},
	{
		name: "simple_exclusion_multiple_direct_assignments_not_linked_2",
		model: `model
				  schema 1.1

				type user
				type user2
				type org
				  relations
					define allowed: [user]
					define member: [user, user2] but not allowed
		`,
		tuples: []string{
			"org:a#allowed@user:bob",
			"org:b#member@user:bob",
			"org:a#member@user:bob",
			"org:c#allowed@user:bob",
			"org:d#member@user2:bob", // even if right side not connected, it should still be good
		},
		objectType: "org",
		relation:   "member",
		user:       "user2:bob",
		expected:   []string{"org:d"},
	},
	{
		name: "simple_exclusion_with_double_negative",
		model: `model
				  schema 1.1

				type user
				type org
				  relations
					define allowed: [user]
					define granted: [user]
					define member: [user] but not (allowed but not granted)
		`,
		tuples: []string{
			"org:a#member@user:bob",
			"org:c#member@user:bob",
			"org:c#allowed@user:bob",
			"org:c#granted@user:bob",
			"org:d#member@user:bob",
			"org:d#granted@user:bob",
			// negative cases
			"org:b#member@user:bob",
			"org:b#allowed@user:bob",
		},
		objectType: "org",
		relation:   "member",
		user:       "user:bob",
		expected:   []string{"org:a", "org:c", "org:d"},
	},
	{
		name: "exclusion_has_no_direct_assignment",
		model: `model
				  schema 1.1

				type user
				type team
				  relations
					define member: [user]
				type org
				  relations
					define allowed: [user]
					define granted: [user]
					define member: [team#member]
					define can_access: member but not (allowed but not granted)
		`,
		tuples: []string{
			"team:a#member@user:bob",
			"org:a#member@team:a#member",
			"org:a#member@user:bob",
			"team:c#member@user:bob",
			"org:c#member@team:c#member",
			"org:c#allowed@user:bob",
			"org:c#granted@user:bob",
			"team:d#member@user:bob",
			"org:d#member@team:d#member",
			"org:d#granted@user:bob",
			// negative cases
			"team:b#member@user:bob",
			"org:b#member@team:b#member",
			"org:b#allowed@user:bob",
		},
		objectType: "org",
		relation:   "can_access",
		user:       "user:bob",
		expected:   []string{"org:a", "org:c", "org:d"},
	},
	{
		name: "complex_exclusion_nested",
		model: `model
				  schema 1.1

				type user
				type team
				  relations
					define member: [user]
				type org
				  relations
					define allowed: [user]
					define granted: [user]
					define also_allowed: [user]
					define also_also_allowed: [user]
					define member: [team#member] but not (((allowed or also_also_allowed) but not also_allowed) but not granted)
		`,
		tuples: []string{
			"team:a#member@user:bob",
			"org:a#member@team:a#member",
			"team:c#member@user:bob",
			"org:c#member@team:c#member",
			"org:c#also_also_allowed@user:bob",
			"org:c#also_allowed@user:bob",
			"team:d#member@user:bob",
			"org:d#member@team:d#member",
			"org:d#also_also_allowed@user:bob",
			"org:d#granted@user:bob",
			// negative cases
			"team:b#member@user:bob",
			"org:b#member@team:b#member",
			"org:b#also_also_allowed@user:bob",
		},
		objectType: "org",
		relation:   "member",
		user:       "user:bob",
		expected:   []string{"org:a", "org:c", "org:d"},
	},
	{
		name: "complex_exclusion_nested_and_union",
		model: `model
				  schema 1.1

				type user
				type team
				  relations
					define member: [user]
				type org
				  relations
					define allowed: [user]
					define granted: [user]
					define also_allowed: [user]
					define member: [team#member] but not ((allowed but not also_allowed) or granted)
		`,
		tuples: []string{
			"team:a#member@user:bob",
			"org:a#member@team:a#member",
			"team:c#member@user:bob",
			"org:c#member@team:c#member",
			"org:c#allowed@user:bob",
			"org:c#also_allowed@user:bob",
			// negative cases
			"team:b#member@user:bob",
			"org:b#member@team:b#member",
			"org:b#allowed@user:bob",
			"team:d#member@user:bob",
			"org:d#member@team:d#member",
			"org:d#granted@user:bob",
			"org:e#granted@user:bob",
		},
		objectType: "org",
		relation:   "member",
		user:       "user:bob",
		expected:   []string{"org:a", "org:c"},
	},
	{
		name: "exclusion_intersection_1",
		model: `model
				  schema 1.1

				type user
				type team
				  relations
					define member: [user]
				type org
				  relations
					define allowed: [user]
					define also_allowed: [user]
					define member: [team#member] but not (allowed and also_allowed)
		`,
		tuples: []string{
			"team:a#member@user:bob",
			"org:a#member@team:a#member",
			"team:b#member@user:bob",
			"org:b#member@team:b#member",
			"org:b#allowed@user:bob",
			"team:c#member@user:bob",
			"org:c#member@team:c#member",
			"org:c#also_allowed@user:bob",
			// negative cases
			"team:d#member@user:bob",
			"org:d#member@team:d#member",
			"org:d#allowed@user:bob",
			"org:d#also_allowed@user:bob",
		},
		objectType: "org",
		relation:   "member",
		user:       "user:bob",
		expected:   []string{"org:a", "org:b", "org:c"},
	},
	{
		name: "exclusion_intersection_2",
		model: `model
				  schema 1.1

				type user
				type team
				  relations
					define member: [user]
				type org
				  relations
					define allowed: [user]
					define also_allowed: [user]
					define member: [team#member] and (allowed but not also_allowed)
		`,
		tuples: []string{
			"team:a#member@user:bob",
			"org:a#member@team:a#member",
			// negative cases
			"team:b#member@user:bob",
			"org:b#member@team:b#member",
			"org:b#allowed@user:bob",
			"team:c#member@user:bob",
			"org:c#member@team:c#member",
			"org:c#allowed@user:bob",
			"org:c#also_allowed@user:bob",
			"team:d#member@user:bob",
			"org:d#member@team:d#member",
			"org:d#also_allowed@user:bob",
		},
		objectType: "org",
		relation:   "member",
		user:       "user:bob",
		expected:   []string{"org:b"},
	},
	{
		name: "exclusion_lowest_weight_is_TTU",
		model: `model
				  schema 1.1

				type user
				type dept
		       relations
		         define member: [user]
				type team
				  relations
					define member: [user]
					define dept_member: [dept#member]
				type org
				  relations
					define team: [team]
					define member: [team#dept_member] but not member from team
		`,
		tuples: []string{
			"org:a#member@team:a#dept_member",
			"team:a#dept_member@dept:a#member",
			"dept:a#member@user:bob",
			"org:c#member@team:c#dept_member",
			"team:c#dept_member@dept:c#member",
			"dept:c#member@user:bob",
			"org:c#team@team:c",
			// negative cases
			"org:b#member@team:b#dept_member",
			"team:b#dept_member@dept:b#member",
			"dept:b#member@user:bob",
			"org:b#team@team:b",
			"team:b#member@user:bob",
		},
		objectType: "org",
		relation:   "member",
		user:       "user:bob",
		expected:   []string{"org:a", "org:c"},
	},
	{
		name: "exclusion_ttu_multipleparents",
		model: `model
				  schema 1.1
			    type user
				type subteam
		          relations
		            define member: [user]
				type team
				  relations
					define member: [user]
					define dept_member: [user]
				type org
				  relations
					define parent: [team, subteam]
					define member: [user, team#dept_member] but not member from parent
		`,
		tuples: []string{
			"org:b#member@user:bob",
			"org:a#member@team:t1#dept_member",
			"team:t1#dept_member@user:bob",
			"org:c#member@team:t2#dept_member",
			"team:t2#dept_member@user:bob",
			"org:b#parent@team:t1",
			"org:b#parent@team:t2",
			"org:a#parent@subteam:st1",
			"org:a#parent@subteam:st2",
			"team:t1#member@user:bob",
		},
		objectType: "org",
		relation:   "member",
		user:       "user:bob",
		expected:   []string{"org:a", "org:c"},
	},
	{
		name: "lowest_weight_is_TTU_intersection_with_intersections",
		model: `model
				  schema 1.1

				type user
				type dept
		         relations
		           define member: [user]
				type team
				  relations
					define member: [user]
					define dept_member: [dept#member] and member
				type org
				  relations
					define team: [team]
					define member: [team#dept_member] and member from team
		`,
		tuples: []string{
			"team:a#member@user:bob",
			"org:a#team@team:a",
			"org:a#member@team:a#dept_member",
			"team:a#dept_member@dept:a#member",
			"team:a#member@user:bob",
			"dept:a#member@user:bob",
			// negative cases
			"team:b#member@user:bob",
			"org:b#team@team:b",
			"dept:b#member@user:bob",
			"org:c#member@team:c#dept_member",
			"team:c#dept_member@dept:c#member",
			"dept:c#member@user:bob",
		},
		objectType: "org",
		relation:   "member",
		user:       "user:bob",
		expected:   []string{"org:a"},
	},
	{
		name: "mix_of_union_intersection_and_exclusion",
		model: `model
				  schema 1.1

				type user
				type dept
		         relations
		           define member: [user]
				type team
				  relations
					define member: [user]
					define allowed: [user]
				type org
				  relations
					define team: [team]
					define dept: [dept]
					define member: [user] or ((member from team and allowed from team ) but not member from dept)
		`,
		tuples: []string{
			"org:a#member@user:bob",
			"org:b#team@team:b",
			"team:b#member@user:bob",
			"team:b#allowed@user:bob",
			// negative cases
			"org:c#team@team:c",
			"team:c#member@user:bob",
			"team:c#allowed@user:bob",
			"org:c#dept@dept:c",
			"dept:c#member@user:bob",
			"org:d#dept@dept:d",
			"dept:d#member@user:bob",
		},
		objectType: "org",
		relation:   "member",
		user:       "user:bob",
		expected:   []string{"org:a", "org:b"},
	},
	{
		name: "intersection_with_TTU",
		model: `model
				schema 1.1
			  type user

			  type folder
				relations
				  define viewer: [user]

			  type document
				relations
				  define parent: [folder]
				  define writer: [user]
				  define viewer: writer and viewer from parent
		`,
		tuples: []string{
			"document:1#parent@folder:X",
			"folder:X#viewer@user:a",
			"document:1#writer@user:a",
			// negative cases
			"folder:X#viewer@user:b",
			"document:2#writer@user:c",
		},
		objectType: "document",
		relation:   "viewer",
		user:       "user:a",
		expected:   []string{"document:1"},
	},
	{
		name: "intersection_with_high_weights",
		model: `model
				schema 1.1
			  type user

			  type folder
				relations
				  define viewer: [user]

			  type document
				relations
				  define other_parent: [folder]
				  define parent: [folder]
				  define viewer: viewer from parent and viewer from other_parent
		`,
		tuples: []string{
			"document:1#parent@folder:X",
			"folder:X#viewer@user:a",
			"document:1#other_parent@folder:X",
			"document:3#parent@folder:A",
			"folder:A#viewer@user:a",
			"document:3#other_parent@folder:B",
			"folder:B#viewer@user:a",
			// negative cases
			"folder:X#viewer@user:b",
			"document:2#parent@folder:Y",
			"folder:Y#viewer@user:a",
			"document:2#other_parent@folder:Z",
		},
		objectType: "document",
		relation:   "viewer",
		user:       "user:a",
		expected:   []string{"document:1", "document:3"},
	},
	{
		name: "exclusion_with_TTU",
		model: `model
				schema 1.1
			  type user

			  type folder
				relations
				  define viewer: [user]

			  type document
				relations
				  define parent: [folder]
				  define writer: [user]
				  define viewer: writer but not viewer from parent
		`,
		tuples: []string{
			"document:2#writer@user:a",
			"document:3#writer@user:a",
			"document:3#parent@folder:Z",
			// negative cases
			"document:1#parent@folder:X",
			"folder:X#viewer@user:a",
			"document:1#writer@user:a",
			"folder:Y#viewer@user:a",
		},
		objectType: "document",
		relation:   "viewer",
		user:       "user:a",
		expected:   []string{"document:2", "document:3"},
	},
	{
		name: "exclusion_with_high_weights",
		model: `model
				schema 1.1
			  type user

			  type folder
				relations
				  define viewer: [user]

			  type document
				relations
				  define other_parent: [folder]
				  define parent: [folder]
				  define viewer: viewer from parent but not viewer from other_parent
		`,
		tuples: []string{
			"document:2#parent@folder:Y",
			"folder:Y#viewer@user:a",
			"document:4#parent@folder:D",
			"folder:D#viewer@user:a",
			"document:4#other_parent@folder:E",
			// negative cases
			"document:1#parent@folder:X",
			"folder:X#viewer@user:a",
			"document:1#other_parent@folder:X",
			"document:3#parent@folder:A",
			"folder:A#viewer@user:a",
			"document:3#other_parent@folder:B",
			"folder:B#viewer@user:a",
			"document:2#other_parent@folder:Z",
			"document:5#other_parent@folder:F",
			"folder:F#viewer@user:a",
		},
		objectType: "document",
		relation:   "viewer",
		user:       "user:a",
		expected:   []string{"document:2", "document:4"},
	},
	{
		name: "tuple_to_userset_intersection",
		model: `model
				schema 1.1
			  type user

			  type and_folder
				relations
				  define writer: [user]
				  define editor: [user]
				  define viewer: writer and editor

			  type document
				relations
				  define and_parent: [and_folder]
				  define viewer: viewer from and_parent
		`,
		tuples: []string{
			"document:a#and_parent@and_folder:a",
			"and_folder:a#writer@user:a",
			"and_folder:a#editor@user:a",
			// negative cases
			"document:b#and_parent@and_folder:b",
			"and_folder:b#writer@user:b",
			"document:c#and_parent@and_folder:c",
			"and_folder:c#editor@user:c",
			"document:d#and_parent@and_folder:d",
			"and_folder:e#editor@user:e",
			"and_folder:e#editor@user:e",
		},
		objectType: "document",
		relation:   "viewer",
		user:       "user:a",
		expected:   []string{"document:a"},
	},
	{
		name: "tuple_to_userset_exclusion",
		model: `model
				schema 1.1
			  type user

			  type but_not_folder
				relations
				  define writer: [user]
				  define editor: [user]
				  define viewer: writer but not editor

			  type document
				relations
				  define but_not_parent: [but_not_folder]
				  define viewer: viewer from but_not_parent
		`,
		tuples: []string{
			"document:a#but_not_parent@but_not_folder:a",
			"but_not_folder:a#writer@user:a",
			// negative cases
			"document:b#but_not_parent@but_not_folder:b",
			"but_not_folder:b#writer@user:b",
			"but_not_folder:b#editor@user:b",
			"document:c#but_not_parent@but_not_folder:c",
			"but_not_folder:c#editor@user:c",
			"but_not_folder:d#writer@user:d",
		},
		objectType: "document",
		relation:   "viewer",
		user:       "user:a",
		expected:   []string{"document:a"},
	},
	{
		name: "duplicate_parent_ttu",
		model: `
				model
					schema 1.1
				type user
				type thing
					relations
						define account: [account]
						define parent: [account]
						define can_view: super_admin from account or super_admin from parent
				type account
					relations
						define super_admin: [user]
		`,
		tuples: []string{
			"thing:4#parent@account:4",
			"account:4#super_admin@user:1",
		},
		objectType: "thing",
		relation:   "can_view",
		user:       "user:1",
		expected: []string{
			"thing:4",
		},
	},
	{
		name: "multiple_ttus_going_to_same_terminal_typerel",
		model: `
				model
					schema 1.1
				type user
				type thing
					relations
						define resource: [resource]
						define can_view: can_view from parent or admin from resource
						define parent: [document]
				type document
					relations
						define resource: [resource]
						define can_view: admin from resource
				type resource
					relations
						define owner: [user] and also_user
						define admin: ([user] and also_user) or owner
						define also_user: [user]
		`,
		tuples: []string{
			"thing:1#resource@resource:1",
			"resource:1#also_user@user:1",
			"resource:1#owner@user:1",

			"thing:2#resource@resource:2",
			"resource:2#also_user@user:1",
			"resource:2#owner@user:1",

			"thing:3#resource@resource:3",
			"resource:3#also_user@user:1",
			"resource:3#owner@user:1",

			"thing:4#resource@resource:4",
			"resource:4#also_user@user:1",
			"resource:4#owner@user:1",

			"thing:5#resource@resource:5",
			"resource:5#also_user@user:1",
			"resource:5#owner@user:1",
		},
		objectType: "thing",
		relation:   "can_view",
		user:       "user:1",
		expected: []string{
			"thing:1",
			"thing:2",
			"thing:3",
			"thing:4",
			"thing:5",
		},
	},
	{
		name: "multiple_ttus_same_terminal_typerel_additional_paths",
		model: `
				model
					schema 1.1
				type user
				type thing
					relations
						define resource: [resource]
						define can_view: owner or super_admin from resource or can_view from parent
						define owner: [user] and also_user from resource
						define parent: [document]
				type document
					relations
						define _also_user: also_user from resource
						define resource: [resource]
						define can_view: viewer or editor or owner or super_admin from resource
						define editor: [user, team#member, resource#member, resource#admin] and _also_user
						define owner: [user] and _also_user
						define viewer: [user, team#member, resource#member, resource#admin] and _also_user
				type resource
					relations
						define admin: ([user] and also_user) or super_admin
						define member: ([user] and also_user) or admin
						define owner: [user] and also_user
						define super_admin: ([user] and also_user) or owner
						define also_user: [user]
				type team
					relations
						define member: [user]
		`,
		tuples: []string{
			// This satisfies can_view: 'owner'
			"thing:1#resource@resource:1",
			"resource:1#also_user@user:1",
			"thing:1#owner@user:1",

			// satisfies one of resource#super_admin edges for can_view: 'super_admin from resource'
			"thing:2#resource@resource:2",
			"resource:2#also_user@user:1",
			"resource:2#super_admin@user:1",

			// satisfies OR edge of resource#super_admin
			"thing:3#resource@resource:3",
			"resource:3#also_user@user:1",
			"resource:3#owner@user:1",

			// satisfies one of parent#can_view #viewer relation
			"thing:4#parent@document:1",
			"document:1#viewer@user:1",
			"document:1#resource@resource:4",
			"resource:4#also_user@user:1",

			// satisfies team#member of parent#can_view #viewer relation
			"thing:5#parent@document:2",
			"document:2#viewer@team:1#member",
			"team:1#member@user:1",
			"document:2#resource@resource:5",
			"resource:5#also_user@user:1",

			// satisfies resource#member of parent#can_view #viewer relation
			"thing:6#parent@document:3",
			"document:3#resource@resource:6",
			"resource:6#also_user@user:1",
			"resource:6#member@user:1",
			"document:3#viewer@resource:6#member",

			// satisfies resource#member of parent#can_view #viewer relation via resource#member
			// when the also_user is from a different resource
			"thing:7#parent@document:4",
			"document:4#resource@resource:7",
			"resource:7#also_user@user:1",
			"resource:7#member@user:1",
			"resource:8#also_user@user:1",
			"resource:8#member@user:1",
			"document:4#viewer@resource:8#member",
		},
		objectType: "thing",
		relation:   "can_view",
		user:       "user:1",
		expected: []string{
			"thing:1",
			"thing:2",
			"thing:3",
			"thing:4",
			"thing:5",
			"thing:6",
			"thing:7",
		},
	},
}

func BenchmarkPipeline(b *testing.B) {
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			var ds StaticObjectStore
			ds.Add(tc.tuples...)

			model := testutils.MustTransformDSLToProtoWithID(tc.model)

			typesys, err := typesystem.NewAndValidate(
				context.Background(),
				model,
			)

			require.NoError(b, err)

			g := typesys.GetWeightedGraph()

			b.ResetTimer()

			for b.Loop() {
				spec := pipeline.Spec{
					ObjectType: tc.objectType,
					Relation:   tc.relation,
					User:       tc.user,
				}

				seq, err := pipeline.New(g, &ds).Expand(context.Background(), spec)

				require.NoError(b, err)

				for range seq {
				}
			}
		})
	}
}

func TestPipeline(t *testing.T) {
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer goleak.VerifyNone(t)

			var ds StaticObjectStore
			ds.Add(tc.tuples...)

			model := testutils.MustTransformDSLToProtoWithID(tc.model)

			typesys, err := typesystem.NewAndValidate(
				context.Background(),
				model,
			)

			require.NoError(t, err)

			g := typesys.GetWeightedGraph()

			spec := pipeline.Spec{
				ObjectType: tc.objectType,
				Relation:   tc.relation,
				User:       tc.user,
			}

			seq, err := pipeline.New(g, &ds).Expand(context.Background(), spec)

			require.NoError(t, err)

			evaluate(t, tc, seq)
		})
	}
}

func TestPipelineTTUWithCondition(t *testing.T) {
	const dsl string = `
	model
	  schema 1.2

	type user

	type group
	  relations
	    define member: [user]

	type folder
	  relations
	    define editor: [group#member]

	type document
	  relations
	    define parent: [folder with not_expired]
	    define can_edit: editor from parent

	condition not_expired(current_time: timestamp, expiry: timestamp) {
	  current_time < expiry
	}
	`

	storeID := "test-store"
	model := testutils.MustTransformDSLToProtoWithID(dsl)
	typesys, err := typesystem.NewAndValidate(context.Background(), model)
	require.NoError(t, err)

	g := typesys.GetWeightedGraph()

	spec := pipeline.Spec{
		ObjectType: "document",
		Relation:   "can_edit",
		User:       "user:alice",
	}

	tests := []struct {
		name        string
		expiry      string
		currentTime string
		expected    []string
	}{
		{
			name:        "not_expired_returns_documents",
			expiry:      "2099-01-01T00:00:00Z",
			currentTime: "2025-01-01T00:00:00Z",
			expected:    []string{"document:1"},
		},
		{
			name:        "expired_excludes_documents",
			expiry:      "2020-01-01T00:00:00Z",
			currentTime: "2025-01-01T00:00:00Z",
			expected:    []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer goleak.VerifyNone(t)

			ds := memory.New()
			t.Cleanup(ds.Close)

			err := ds.WriteAuthorizationModel(context.Background(), storeID, model)
			require.NoError(t, err)

			err = ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:eng", "member", "user:alice"),
				tuple.NewTupleKey("folder:1", "editor", "group:eng#member"),
				tuple.NewTupleKeyWithCondition("document:1", "parent", "folder:1", "not_expired", &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"expiry": structpb.NewStringValue(tc.expiry),
					},
				}),
			})
			require.NoError(t, err)

			reqCtx := &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"current_time": structpb.NewStringValue(tc.currentTime),
				},
			}

			reader := obj.NewReader(
				ds,
				storeID,
				obj.WithValidator(obj.NewValidator(context.Background(), typesys, reqCtx)),
			)

			seq, err := pipeline.New(g, reader).Expand(context.Background(), spec)
			require.NoError(t, err)

			var results []string
			for object := range seq {
				value, err := object.Object()
				require.NoError(t, err)
				results = append(results, value)
			}
			require.ElementsMatch(t, tc.expected, results)
		})
	}
}
