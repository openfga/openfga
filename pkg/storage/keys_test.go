package storage

import (
	"encoding/hex"
	"sync"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage/cache/keys"
	"github.com/openfga/openfga/pkg/tuple"
)

// requireValidHex asserts the key is non-empty and valid uppercase hex.
func requireValidHex(t *testing.T, key keys.Key) {
	t.Helper()
	s := key.String()
	require.NotEmpty(t, s)
	_, err := hex.DecodeString(s)
	require.NoError(t, err, "key must be valid hex: %s", s)
}

// ─────────────────────────────────────────────────────────────────────────────
// CheckCacheKey + InvariantCacheKey
// ─────────────────────────────────────────────────────────────────────────────

func TestCheckCacheKey(t *testing.T) {
	storeID := "store123"

	t.Run("non_empty_valid_hex", func(t *testing.T) {
		key := CheckCacheKey(storeID, "document:1", "viewer", "user:alice", 0)
		requireValidHex(t, key)
	})

	t.Run("contains_subproblem_prefix_bytes", func(t *testing.T) {
		key := CheckCacheKey(storeID, "document:1", "viewer", "user:alice", 0)
		// The TLV-encoded prefix "SP" appears as tagString + varint(2) + "SP"
		// in hex. Verify the key encodes the prefix somewhere in the output.
		require.Contains(t, key.String(), hex.EncodeToString([]byte(PrefixSubproblemCache)))
	})

	t.Run("different_tuples_produce_different_keys", func(t *testing.T) {
		a := CheckCacheKey(storeID, "document:1", "viewer", "user:alice", 0)
		b := CheckCacheKey(storeID, "document:1", "viewer", "user:bob", 0)
		c := CheckCacheKey(storeID, "document:2", "viewer", "user:alice", 0)
		d := CheckCacheKey(storeID, "document:1", "editor", "user:alice", 0)
		require.NotEqual(t, a, b)
		require.NotEqual(t, a, c)
		require.NotEqual(t, a, d)
	})

	t.Run("different_invariants_produce_different_keys", func(t *testing.T) {
		a := CheckCacheKey(storeID, "document:1", "viewer", "user:alice", 0)
		b := CheckCacheKey(storeID, "document:1", "viewer", "user:alice", 42)
		require.NotEqual(t, a, b)
	})

	t.Run("identical_inputs_produce_identical_keys", func(t *testing.T) {
		a := CheckCacheKey(storeID, "document:1", "viewer", "user:alice", 99)
		b := CheckCacheKey(storeID, "document:1", "viewer", "user:alice", 99)
		require.Equal(t, a, b)
	})

	t.Run("different_stores_produce_different_keys", func(t *testing.T) {
		a := CheckCacheKey("store-a", "document:1", "viewer", "user:alice", 0)
		b := CheckCacheKey("store-b", "document:1", "viewer", "user:alice", 0)
		require.NotEqual(t, a, b)
	})
}

func TestInvariantCacheKey(t *testing.T) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	t.Run("differs_with_contextual_tuples_present_vs_absent", func(t *testing.T) {
		ctxTuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "user:anne"),
		}
		with := InvariantCacheKey(storeID, modelID, nil, ctxTuples...)
		without := InvariantCacheKey(storeID, modelID, nil)
		require.NotEqual(t, with, without)
	})

	t.Run("differs_with_context_present_vs_absent", func(t *testing.T) {
		ctx, err := structpb.NewStruct(map[string]any{"key1": true})
		require.NoError(t, err)
		with := InvariantCacheKey(storeID, modelID, ctx)
		without := InvariantCacheKey(storeID, modelID, nil)
		require.NotEqual(t, with, without)
	})

	t.Run("contextual_tuple_order_is_ignored", func(t *testing.T) {
		t1 := tuple.NewTupleKey("document:1", "viewer", "user:anne")
		t2 := tuple.NewTupleKey("document:2", "admin", "user:jon")
		a := InvariantCacheKey(storeID, modelID, nil, t1, t2)
		b := InvariantCacheKey(storeID, modelID, nil, t2, t1)
		require.Equal(t, a, b)
	})

	t.Run("different_conditions_produce_different_keys", func(t *testing.T) {
		t1 := tuple.NewTupleKeyWithCondition("document:2", "admin", "user:jon", "cond_a", nil)
		t2 := tuple.NewTupleKeyWithCondition("document:2", "admin", "user:jon", "cond_b", nil)
		a := InvariantCacheKey(storeID, modelID, nil, t1)
		b := InvariantCacheKey(storeID, modelID, nil, t2)
		require.NotEqual(t, a, b)
	})

	t.Run("condition_context_changes_key", func(t *testing.T) {
		s1, err := structpb.NewStruct(map[string]any{"k": "foo"})
		require.NoError(t, err)
		s2, err := structpb.NewStruct(map[string]any{"k": "bar"})
		require.NoError(t, err)
		t1 := tuple.NewTupleKeyWithCondition("document:2", "admin", "user:jon", "cond", s1)
		t2 := tuple.NewTupleKeyWithCondition("document:2", "admin", "user:jon", "cond", s2)
		a := InvariantCacheKey(storeID, modelID, nil, t1)
		b := InvariantCacheKey(storeID, modelID, nil, t2)
		require.NotEqual(t, a, b)
	})

	t.Run("condition_context_key_order_does_not_matter", func(t *testing.T) {
		s1, err := structpb.NewStruct(map[string]any{"a": "1", "b": "2"})
		require.NoError(t, err)
		s2, err := structpb.NewStruct(map[string]any{"b": "2", "a": "1"})
		require.NoError(t, err)
		t1 := tuple.NewTupleKeyWithCondition("document:2", "admin", "user:jon", "cond", s1)
		t2 := tuple.NewTupleKeyWithCondition("document:2", "admin", "user:jon", "cond", s2)
		a := InvariantCacheKey(storeID, modelID, nil, t1)
		b := InvariantCacheKey(storeID, modelID, nil, t2)
		require.Equal(t, a, b)
	})

	t.Run("request_context_distinguishes_values_and_types", func(t *testing.T) {
		s1, err := structpb.NewStruct(map[string]any{"key1": "foo", "key2": "bar"})
		require.NoError(t, err)
		s2, err := structpb.NewStruct(map[string]any{"key2": "bar", "key1": "foo"})
		require.NoError(t, err)
		s3, err := structpb.NewStruct(map[string]any{"key2": "x", "key1": "foo"})
		require.NoError(t, err)
		s4, err := structpb.NewStruct(map[string]any{"key2": "x", "key1": true})
		require.NoError(t, err)

		k1 := InvariantCacheKey(storeID, modelID, s1)
		k2 := InvariantCacheKey(storeID, modelID, s2)
		k3 := InvariantCacheKey(storeID, modelID, s3)
		k4 := InvariantCacheKey(storeID, modelID, s4)
		require.Equal(t, k1, k2)
		require.NotEqual(t, k1, k3)
		require.NotEqual(t, k1, k4)
		require.NotEqual(t, k3, k4)
	})

	t.Run("model_id_changes_key", func(t *testing.T) {
		a := InvariantCacheKey(storeID, "model-a", nil)
		b := InvariantCacheKey(storeID, "model-b", nil)
		require.NotEqual(t, a, b)
	})

	t.Run("store_id_changes_key", func(t *testing.T) {
		a := InvariantCacheKey("store-a", modelID, nil)
		b := InvariantCacheKey("store-b", modelID, nil)
		require.NotEqual(t, a, b)
	})

	t.Run("nil_context_equals_empty_struct_context", func(t *testing.T) {
		// `InvariantCacheKey(_, _, nil)` and `InvariantCacheKey(_, _, &structpb.Struct{})`
		// both represent "no context" and should collapse to the same key.
		// This locks in the seam between the two representations.
		empty := &structpb.Struct{}
		require.Equal(t,
			InvariantCacheKey(storeID, modelID, nil),
			InvariantCacheKey(storeID, modelID, empty),
		)
	})

	t.Run("empty_struct_context_equals_struct_with_nil_fields_map", func(t *testing.T) {
		// `&structpb.Struct{}` and `&structpb.Struct{Fields: nil}` are
		// indistinguishable at the proto level and must hash identically.
		emptyA := &structpb.Struct{}
		emptyB := &structpb.Struct{Fields: nil}
		require.Equal(t,
			InvariantCacheKey(storeID, modelID, emptyA),
			InvariantCacheKey(storeID, modelID, emptyB),
		)
	})

	t.Run("pinned_seed_produces_stable_hash", func(t *testing.T) {
		original := keys.Seed
		keys.Seed = 0
		t.Cleanup(func() { keys.Seed = original })

		ctxStruct, err := structpb.NewStruct(map[string]any{"key1": true})
		require.NoError(t, err)
		hash := InvariantCacheKey(
			"fake_store_id",
			"fake_model_id",
			ctxStruct,
			tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:anne", "condition_name", ctxStruct),
		)
		require.NotEqual(t, uint64(0), hash)
		hash2 := InvariantCacheKey(
			"fake_store_id",
			"fake_model_id",
			ctxStruct,
			tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:anne", "condition_name", ctxStruct),
		)
		require.Equal(t, hash, hash2)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// ReadKey / ReadStartingWithUserKey / ReadUsersetTuplesKey
// ─────────────────────────────────────────────────────────────────────────────

func TestReadKey(t *testing.T) {
	store := ulid.Make().String()
	base := ReadFilter{Object: "document:1", Relation: "viewer", User: "user:alice"}

	t.Run("valid_hex", func(t *testing.T) {
		requireValidHex(t, ReadKey(store, base))
	})

	t.Run("different_components_produce_different_keys", func(t *testing.T) {
		other := base
		other.User = "user:bob"
		require.NotEqual(t, ReadKey(store, base), ReadKey(store, other))

		other = base
		other.Relation = "editor"
		require.NotEqual(t, ReadKey(store, base), ReadKey(store, other))

		other = base
		other.Object = "document:2"
		require.NotEqual(t, ReadKey(store, base), ReadKey(store, other))
	})

	t.Run("different_store_produces_different_key", func(t *testing.T) {
		require.NotEqual(t, ReadKey(store, base), ReadKey(ulid.Make().String(), base))
	})

	t.Run("conditions_are_part_of_the_key", func(t *testing.T) {
		withCond := base
		withCond.Conditions = []string{"some_condition"}
		require.NotEqual(t, ReadKey(store, base), ReadKey(store, withCond))
	})

	t.Run("conditions_order_does_not_matter", func(t *testing.T) {
		a := base
		a.Conditions = []string{"x", "y"}
		b := base
		b.Conditions = []string{"y", "x"}
		require.Equal(t, ReadKey(store, a), ReadKey(store, b))
	})

	t.Run("identical_inputs_produce_identical_keys", func(t *testing.T) {
		key1 := ReadKey(store, base)
		key2 := ReadKey(store, base)

		require.Equal(t, key1, key2)
	})
}

func TestReadUsersetTuplesKey(t *testing.T) {
	store := ulid.Make().String()
	base := ReadUsersetTuplesFilter{
		Object:   "document:1",
		Relation: "viewer",
	}

	t.Run("valid_hex", func(t *testing.T) {
		requireValidHex(t, ReadUsersetTuplesKey(store, base))
	})

	t.Run("conditions_are_part_of_the_key", func(t *testing.T) {
		withCond := base
		withCond.Conditions = []string{"some_condition"}
		require.NotEqual(t, ReadUsersetTuplesKey(store, base), ReadUsersetTuplesKey(store, withCond))
	})

	t.Run("allowed_user_type_restrictions_are_part_of_the_key", func(t *testing.T) {
		withRel := base
		withRel.AllowedUserTypeRestrictions = []*openfgav1.RelationReference{
			{Type: "group", RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "member"}},
		}
		require.NotEqual(t, ReadUsersetTuplesKey(store, base), ReadUsersetTuplesKey(store, withRel))
	})

	t.Run("allowed_user_type_restrictions_order_does_not_matter", func(t *testing.T) {
		a := base
		a.AllowedUserTypeRestrictions = []*openfgav1.RelationReference{
			{Type: "group", RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "member"}},
			{Type: "user"},
		}
		b := base
		b.AllowedUserTypeRestrictions = []*openfgav1.RelationReference{
			{Type: "user"},
			{Type: "group", RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "member"}},
		}
		require.Equal(t, ReadUsersetTuplesKey(store, a), ReadUsersetTuplesKey(store, b))
	})

	t.Run("identical_inputs_produce_identical_keys", func(t *testing.T) {
		key1 := ReadUsersetTuplesKey(store, base)
		key2 := ReadUsersetTuplesKey(store, base)

		require.Equal(t, key1, key2)
	})
}

func TestReadStartingWithUserKey(t *testing.T) {
	store := ulid.Make().String()
	base := ReadStartingWithUserFilter{
		ObjectType: "document",
		Relation:   "viewer",
	}

	t.Run("valid_hex", func(t *testing.T) {
		requireValidHex(t, ReadStartingWithUserKey(store, base))
	})

	t.Run("user_filter_is_part_of_the_key", func(t *testing.T) {
		withUser := base
		withUser.UserFilter = []*openfgav1.ObjectRelation{
			{Object: "user:alice"},
		}
		require.NotEqual(t,
			ReadStartingWithUserKey(store, base),
			ReadStartingWithUserKey(store, withUser),
		)
	})

	t.Run("user_filter_order_does_not_matter", func(t *testing.T) {
		a := base
		a.UserFilter = []*openfgav1.ObjectRelation{
			{Object: "user:alice"},
			{Object: "user:bob"},
		}
		b := base
		b.UserFilter = []*openfgav1.ObjectRelation{
			{Object: "user:bob"},
			{Object: "user:alice"},
		}
		require.Equal(t, ReadStartingWithUserKey(store, a), ReadStartingWithUserKey(store, b))
	})

	t.Run("conditions_are_part_of_the_key", func(t *testing.T) {
		withCond := base
		withCond.Conditions = []string{"some_condition"}
		require.NotEqual(t,
			ReadStartingWithUserKey(store, base),
			ReadStartingWithUserKey(store, withCond),
		)
	})

	t.Run("object_ids_are_part_of_the_key", func(t *testing.T) {
		withOIDs := base
		withOIDs.ObjectIDs = NewSortedSet("1", "2", "3")
		require.NotEqual(t,
			ReadStartingWithUserKey(store, base),
			ReadStartingWithUserKey(store, withOIDs),
		)
	})

	t.Run("empty_object_ids_collapse_to_no_object_ids", func(t *testing.T) {
		empty := base
		empty.ObjectIDs = NewSortedSet()
		require.Equal(t,
			ReadStartingWithUserKey(store, base),
			ReadStartingWithUserKey(store, empty),
		)
	})

	t.Run("hash_slots_are_disambiguated", func(t *testing.T) {
		withUserFilter := base
		withUserFilter.UserFilter = []*openfgav1.ObjectRelation{{Object: "user:alice"}}

		withConditions := base
		withConditions.Conditions = []string{"user:alice"}

		require.NotEqual(t,
			ReadStartingWithUserKey(store, withUserFilter),
			ReadStartingWithUserKey(store, withConditions),
		)
	})

	t.Run("identical_inputs_produce_identical_keys", func(t *testing.T) {
		f := base
		f.UserFilter = []*openfgav1.ObjectRelation{{Object: "user:alice"}}
		f.ObjectIDs = NewSortedSet("1", "2")
		f.Conditions = []string{"cond_a"}

		key1 := ReadStartingWithUserKey(store, f)
		key2 := ReadStartingWithUserKey(store, f)

		require.Equal(t, key1, key2)
	})

	t.Run("nil_object_ids_equals_empty_object_ids", func(t *testing.T) {
		// The implementation branches on `filter.ObjectIDs != nil`, but both
		// "no filter set" and "empty filter set" represent the same logical
		// query and should hash identically.
		nilOIDs := base
		emptyOIDs := base
		emptyOIDs.ObjectIDs = NewSortedSet()
		require.Equal(t,
			ReadStartingWithUserKey(store, nilOIDs),
			ReadStartingWithUserKey(store, emptyOIDs),
		)
	})

	t.Run("empty_condition_differs_from_no_conditions", func(t *testing.T) {
		// The empty string is the "unconditioned" sentinel (language NoCond),
		// and the datastore treats Conditions=[""] as "match only rows with no
		// condition" — a different query from Conditions=nil ("match any").
		// The keys must therefore differ.
		noConds := base
		emptyConds := base
		emptyConds.Conditions = []string{""}
		require.NotEqual(t,
			ReadStartingWithUserKey(store, noConds),
			ReadStartingWithUserKey(store, emptyConds),
		)
	})

	t.Run("trailing_empty_condition_differs_from_real_only", func(t *testing.T) {
		// ["foo", ""] (foo OR unconditioned) selects a different tuple set
		// than ["foo"], so the keys must differ.
		realOnly := base
		realOnly.Conditions = []string{"foo"}
		withTrailingEmpty := base
		withTrailingEmpty.Conditions = []string{"foo", ""}
		require.NotEqual(t,
			ReadStartingWithUserKey(store, realOnly),
			ReadStartingWithUserKey(store, withTrailingEmpty),
		)
	})
}

// TestEmptyConditionIsDistinct guards against cache-key collisions between
// semantically different condition filters. The empty string is the
// "unconditioned" sentinel (language NoCond), and the datastore treats
// Conditions=[""] as the filter "match only rows with no condition"
// (COALESCE(condition_name,”) IN (...)). That is a different query from
// Conditions=nil ("match any condition"), so the cache keys must differ.
// Likewise ["foo", ""] (foo OR unconditioned) must differ from ["foo"].
func TestEmptyConditionIsDistinct(t *testing.T) {
	store := ulid.Make().String()

	t.Run("read_key", func(t *testing.T) {
		base := ReadFilter{Object: "document:1", Relation: "viewer", User: "user:alice"}

		emptyOnly := base
		emptyOnly.Conditions = []string{""}
		require.NotEqual(t, ReadKey(store, base), ReadKey(store, emptyOnly))

		realOnly := base
		realOnly.Conditions = []string{"foo"}
		withEmpty := base
		withEmpty.Conditions = []string{"foo", ""}
		require.NotEqual(t, ReadKey(store, realOnly), ReadKey(store, withEmpty))
	})

	t.Run("read_userset_tuples_key", func(t *testing.T) {
		base := ReadUsersetTuplesFilter{Object: "document:1", Relation: "viewer"}

		emptyOnly := base
		emptyOnly.Conditions = []string{""}
		require.NotEqual(t, ReadUsersetTuplesKey(store, base), ReadUsersetTuplesKey(store, emptyOnly))

		realOnly := base
		realOnly.Conditions = []string{"foo"}
		withEmpty := base
		withEmpty.Conditions = []string{"foo", ""}
		require.NotEqual(t, ReadUsersetTuplesKey(store, realOnly), ReadUsersetTuplesKey(store, withEmpty))
	})

	t.Run("read_starting_with_user_key", func(t *testing.T) {
		base := ReadStartingWithUserFilter{ObjectType: "document", Relation: "viewer"}

		emptyOnly := base
		emptyOnly.Conditions = []string{""}
		require.NotEqual(t, ReadStartingWithUserKey(store, base), ReadStartingWithUserKey(store, emptyOnly))

		realOnly := base
		realOnly.Conditions = []string{"foo"}
		withEmpty := base
		withEmpty.Conditions = []string{"foo", ""}
		require.NotEqual(t, ReadStartingWithUserKey(store, realOnly), ReadStartingWithUserKey(store, withEmpty))
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Invalid iterator + Changelog keys
// ─────────────────────────────────────────────────────────────────────────────

func TestInvalidIteratorCacheKeys(t *testing.T) {
	store := ulid.Make().String()

	t.Run("store_level_valid_hex", func(t *testing.T) {
		requireValidHex(t, InvalidIteratorCacheKey(store))
	})

	t.Run("by_object_relation_valid_hex", func(t *testing.T) {
		requireValidHex(t, InvalidIteratorByObjectRelationCacheKey(store, "document:1", "viewer"))
	})

	t.Run("by_user_object_type_valid_hex", func(t *testing.T) {
		requireValidHex(t, InvalidIteratorByUserObjectTypeCacheKey(store, "user:alice", "document"))
	})

	t.Run("by_object_relation_delimiter_in_object_does_not_collide", func(t *testing.T) {
		a := InvalidIteratorByObjectRelationCacheKey(store, "doc:1", "viewer")
		b := InvalidIteratorByObjectRelationCacheKey(store, "doc:1#viewer", "")
		require.NotEqual(t, a, b)
	})

	t.Run("by_user_object_type_delimiter_in_user_does_not_collide", func(t *testing.T) {
		a := InvalidIteratorByUserObjectTypeCacheKey(store, "user:alice|group", "doc")
		b := InvalidIteratorByUserObjectTypeCacheKey(store, "user:alice", "group|doc")
		require.NotEqual(t, a, b)
	})

	t.Run("the_three_key_shapes_do_not_collide", func(t *testing.T) {
		a := InvalidIteratorCacheKey(store)
		b := InvalidIteratorByObjectRelationCacheKey(store, "x", "y")
		c := InvalidIteratorByUserObjectTypeCacheKey(store, "x", "y")
		require.NotEqual(t, a, b)
		require.NotEqual(t, a, c)
		require.NotEqual(t, b, c)
	})
}

func TestChangelogCacheKey(t *testing.T) {
	t.Run("valid_hex", func(t *testing.T) {
		requireValidHex(t, ChangelogCacheKey(ulid.Make().String()))
	})

	t.Run("different_stores_produce_different_keys", func(t *testing.T) {
		require.NotEqual(t, ChangelogCacheKey("store-a"), ChangelogCacheKey("store-b"))
	})

	t.Run("identical_inputs_produce_identical_keys", func(t *testing.T) {
		key1 := ChangelogCacheKey("store-x")
		key2 := ChangelogCacheKey("store-x")

		require.Equal(t, key1, key2)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Cross-cutting: delimiter injection must not cause collisions
// ─────────────────────────────────────────────────────────────────────────────

func TestReadUsersetTuplesKey_DelimiterInjectionDoesNotCollide(t *testing.T) {
	store := ulid.Make().String()

	a := ReadUsersetTuplesKey(store, ReadUsersetTuplesFilter{
		Object:   "doc:1",
		Relation: "viewer",
	})
	b := ReadUsersetTuplesKey(store, ReadUsersetTuplesFilter{
		Object:   "doc:1#viewer/group",
		Relation: "member",
	})
	require.NotEqual(t, a, b)
}

// ─────────────────────────────────────────────────────────────────────────────
// Builder TLV format verification
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilder_TLV_String(t *testing.T) {
	var b keys.Builder
	b.EncodeString("hello")

	raw := b.Bytes()
	// tagString=4, varint(5)=0x05, "hello"
	require.Equal(t, byte(4), raw[0], "first byte must be tagString")
	require.Equal(t, byte(5), raw[1], "second byte must be length 5")
	require.Equal(t, []byte("hello"), raw[2:7])
}

func TestBuilder_TLV_Uint64(t *testing.T) {
	var b keys.Builder
	b.EncodeUint64(0x0102030405060708)

	raw := b.Bytes()
	// tagUint64=3, then 8 bytes little-endian
	require.Equal(t, byte(3), raw[0], "first byte must be tagUint64")
	require.Equal(t, []byte{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}, raw[1:9])
}

func TestBuilder_TLV_Bool(t *testing.T) {
	var b keys.Builder
	b.EncodeBool(true)
	b.EncodeBool(false)

	raw := b.Bytes()
	// tagBool=2, value=1, tagBool=2, value=0
	require.Equal(t, []byte{2, 1, 2, 0}, raw)
}

func TestBuilder_TLV_Byte(t *testing.T) {
	var b keys.Builder
	b.EncodeByte(0xAB)

	raw := b.Bytes()
	// tagByte=1, value=0xAB
	require.Equal(t, []byte{1, 0xAB}, raw)
}

func TestBuilder_TLV_Bytes(t *testing.T) {
	var b keys.Builder
	b.EncodeBytes([]byte{0xDE, 0xAD})

	raw := b.Bytes()
	// tagBytes=5, varint(2)=0x02, 0xDE, 0xAD
	require.Equal(t, byte(5), raw[0], "first byte must be tagBytes")
	require.Equal(t, byte(2), raw[1], "second byte must be length 2")
	require.Equal(t, []byte{0xDE, 0xAD}, raw[2:4])
}

func TestBuilder_TLV_Array(t *testing.T) {
	var b keys.Builder
	b.EncodeArray([]keys.Serializable{
		keys.String("a"),
		keys.String("b"),
	})

	raw := b.Bytes()
	// tagArray=6, varint(2)=0x02, then two TLV strings
	require.Equal(t, byte(6), raw[0], "first byte must be tagArray")
	require.Equal(t, byte(2), raw[1], "second byte must be element count 2")
	// First element: tagString=4, len=1, 'a'
	require.Equal(t, byte(4), raw[2])
	require.Equal(t, byte(1), raw[3])
	require.Equal(t, byte('a'), raw[4])
	// Second element: tagString=4, len=1, 'b'
	require.Equal(t, byte(4), raw[5])
	require.Equal(t, byte(1), raw[6])
	require.Equal(t, byte('b'), raw[7])
}

func TestBuilder_TLV_EmptyArray(t *testing.T) {
	var b keys.Builder
	b.EncodeArray(nil)

	raw := b.Bytes()
	// tagArray=6, varint(0)=0x00
	require.Equal(t, []byte{6, 0}, raw)
}

func TestBuilder_TLV_Pair(t *testing.T) {
	var b keys.Builder
	b.EncodePair(keys.String("k"), keys.String("v"))

	raw := b.Bytes()
	// tagPair=8, tagKey=9, <string "k">, tagValue=10, <string "v">
	require.Equal(t, byte(8), raw[0], "tagPair")
	require.Equal(t, byte(9), raw[1], "tagKey")
	// string "k": tagString=4, len=1, 'k'
	require.Equal(t, byte(4), raw[2])
	require.Equal(t, byte(1), raw[3])
	require.Equal(t, byte('k'), raw[4])
	require.Equal(t, byte(10), raw[5], "tagValue")
	// string "v": tagString=4, len=1, 'v'
	require.Equal(t, byte(4), raw[6])
	require.Equal(t, byte(1), raw[7])
	require.Equal(t, byte('v'), raw[8])
}

func TestBuilder_String_HexEncodes(t *testing.T) {
	var b keys.Builder
	b.EncodeByte(0xAB)

	// raw bytes: [0x01, 0xAB]
	// hex of 0x01 = "01", hex of 0xAB = "AB"
	expected := "01AB"
	require.Equal(t, expected, b.Key().String())
}

// ─────────────────────────────────────────────────────────────────────────────
// Pool hygiene & concurrency
// ─────────────────────────────────────────────────────────────────────────────

func TestPooledBuilder_CloseResetsState(t *testing.T) {
	// A builder pulled from the pool must come back empty even if the
	// previous caller forgot to Reset. Close() is the documented contract;
	// verify it scrubs the buffer before returning to the pool.
	b1 := keys.GetBuilder()
	b1.EncodeString("leftover")
	b1.Close()

	// Subsequent Get may or may not return the same instance, but whichever
	// instance we get must be empty.
	b2 := keys.GetBuilder()
	t.Cleanup(b2.Close)
	require.Empty(t, b2.Bytes(),
		"GetKeyBuilder must return a builder with no leftover bytes")
}

func TestCheckCacheKey_ConcurrentUse(t *testing.T) {
	// The pool is shared across goroutines. Exercise enough parallel
	// Get/Write/String/Close cycles to catch races under `-race` and
	// confirm that concurrent use never produces empty or cross-contaminated
	// keys for identical input.
	const goroutines = 64
	const itersPerG = 200

	want := CheckCacheKey("store-x", "doc:1", "viewer", "user:alice", 7)

	errs := make(chan keys.Key, goroutines*itersPerG)
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for range itersPerG {
				got := CheckCacheKey("store-x", "doc:1", "viewer", "user:alice", 7)
				if got != want {
					errs <- got
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for got := range errs {
		t.Fatalf("concurrent CheckCacheKey produced divergent key: %q (want %q)", got.String(), want.String())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Cross-function collision tests
// ─────────────────────────────────────────────────────────────────────────────

func TestCrossFunction_KeysDoNotCollide(t *testing.T) {
	store := ulid.Make().String()

	t.Run("ReadKey_vs_ReadStartingWithUserKey", func(t *testing.T) {
		readKey := ReadKey(store, ReadFilter{
			Object: "document:1", Relation: "viewer", User: "user:alice",
		})
		rswuKey := ReadStartingWithUserKey(store, ReadStartingWithUserFilter{
			ObjectType: "document",
			Relation:   "viewer",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:alice"}},
		})
		require.NotEqual(t, readKey, rswuKey)
	})

	t.Run("ReadKey_vs_ReadUsersetTuplesKey", func(t *testing.T) {
		readKey := ReadKey(store, ReadFilter{
			Object: "document:1", Relation: "viewer", User: "user:alice",
		})
		rutKey := ReadUsersetTuplesKey(store, ReadUsersetTuplesFilter{
			Object: "document:1", Relation: "viewer",
		})
		require.NotEqual(t, readKey, rutKey)
	})

	t.Run("ReadStartingWithUserKey_vs_ReadUsersetTuplesKey", func(t *testing.T) {
		rswuKey := ReadStartingWithUserKey(store, ReadStartingWithUserFilter{
			ObjectType: "document",
			Relation:   "viewer",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:alice"}},
		})
		rutKey := ReadUsersetTuplesKey(store, ReadUsersetTuplesFilter{
			Object: "document:1", Relation: "viewer",
		})
		require.NotEqual(t, rswuKey, rutKey)
	})

	t.Run("CheckCacheKey_vs_InvalidIteratorCacheKey", func(t *testing.T) {
		checkKey := CheckCacheKey(store, "document:1", "viewer", "user:alice", 0)
		invalidKey := InvalidIteratorCacheKey(store)
		require.NotEqual(t, checkKey, invalidKey)
	})

	t.Run("CheckCacheKey_vs_ChangelogCacheKey", func(t *testing.T) {
		checkKey := CheckCacheKey(store, "document:1", "viewer", "user:alice", 0)
		changelogKey := ChangelogCacheKey(store)
		require.NotEqual(t, checkKey, changelogKey)
	})
}

func TestInvariantCacheKey_IntegrationWithCheckCacheKey(t *testing.T) {
	storeID := "store123"
	modelID := "model456"

	invariant := InvariantCacheKey(storeID, modelID, nil)

	t.Run("same_inputs_produce_same_key", func(t *testing.T) {
		k1 := CheckCacheKey(storeID, "doc:1", "viewer", "user:alice", invariant)
		k2 := CheckCacheKey(storeID, "doc:1", "viewer", "user:alice", invariant)
		require.Equal(t, k1, k2)
	})

	t.Run("different_invariant_produces_different_key", func(t *testing.T) {
		otherInvariant := InvariantCacheKey("other-store", modelID, nil)
		k1 := CheckCacheKey(storeID, "doc:1", "viewer", "user:alice", invariant)
		k2 := CheckCacheKey(storeID, "doc:1", "viewer", "user:alice", otherInvariant)
		require.NotEqual(t, k1, k2)
	})
}
