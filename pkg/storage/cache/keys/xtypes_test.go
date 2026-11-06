package keys_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage/cache/keys"
)

// hashOf serializes a Serializable and returns the Builder's hex digest.
func hashOf(t *testing.T, s keys.Serializable) string {
	t.Helper()
	kb := &keys.Builder{}
	kb.Serialize(s)
	return kb.Key().String()
}

// emptyHash returns the digest of a Builder with no writes, for comparing
// "no-op" behavior.
func emptyHash(t *testing.T) string {
	t.Helper()
	kb := &keys.Builder{}
	return kb.Key().String()
}

func pbBool(b bool) *keys.PbValue      { return (*keys.PbValue)(structpb.NewBoolValue(b)) }
func pbNull() *keys.PbValue            { return (*keys.PbValue)(structpb.NewNullValue()) }
func pbString(s string) *keys.PbValue  { return (*keys.PbValue)(structpb.NewStringValue(s)) }
func pbNumber(n float64) *keys.PbValue { return (*keys.PbValue)(structpb.NewNumberValue(n)) }
func pbList(values ...*structpb.Value) *keys.PbValue {
	return (*keys.PbValue)(structpb.NewListValue(&structpb.ListValue{Values: values}))
}
func pbStruct(fields map[string]*structpb.Value) *keys.PbValue {
	return (*keys.PbValue)(structpb.NewStructValue(&structpb.Struct{Fields: fields}))
}

// -----------------------------------------------------------------------------
// PbValue.WriteTo
// -----------------------------------------------------------------------------

func TestPbValue_Nil_IsUnset(t *testing.T) {
	var v *keys.PbValue
	require.Equal(t, hashOf(t, keys.Unset{}), hashOf(t, v),
		"writing a nil PbValue will write an Unset tag")
}

func TestPbValue_Deterministic(t *testing.T) {
	cases := map[string]*keys.PbValue{
		"bool true":    pbBool(true),
		"bool false":   pbBool(false),
		"null":         pbNull(),
		"empty string": pbString(""),
		"string":       pbString("hello"),
		"number zero":  pbNumber(0),
		"number":       pbNumber(3.14159),
		"empty list":   pbList(),
		"list": pbList(
			structpb.NewStringValue("a"),
			structpb.NewBoolValue(true),
			structpb.NewNumberValue(2),
		),
		"empty struct": pbStruct(map[string]*structpb.Value{}),
		"struct": pbStruct(map[string]*structpb.Value{
			"a": structpb.NewStringValue("x"),
			"b": structpb.NewNumberValue(1),
		}),
	}
	for name, v := range cases {
		t.Run(name, func(t *testing.T) {
			hash1 := hashOf(t, v)
			hash2 := hashOf(t, v)

			require.Equal(t, hash1, hash2,
				"hashing the same PbValue twice must yield the same digest")
		})
	}
}

func TestPbValue_DistinctKindsProduceDistinctHashes(t *testing.T) {
	// Each kind/value should hash to a unique digest. Bool(false), Null, and
	// nil-Kind all use dedicated tag bytes (tagBool, tagNull, tagBytes
	// respectively) so they never collide.
	values := map[string]*keys.PbValue{
		"bool true":  pbBool(true),
		"bool false": pbBool(false),
		"null":       pbNull(),
		"string":     pbString("hello"),
		"number":     pbNumber(3.14),
		"list":       pbList(structpb.NewStringValue("a")),
		"struct":     pbStruct(map[string]*structpb.Value{"a": structpb.NewStringValue("x")}),
	}
	seen := map[string]string{}
	for name, v := range values {
		h := hashOf(t, v)
		if prior, ok := seen[h]; ok {
			t.Fatalf("hash collision between %q and %q (both %s)", name, prior, h)
		}
		seen[h] = name
	}
}

func TestPbValue_BoolFalseDiffersFromNull(t *testing.T) {
	// Null uses tagNull (one byte) and Bool(false) uses tagBool+0 (two bytes).
	// They must hash differently — JSON `null` and JSON `false` are distinct
	// inputs to a CEL condition.
	require.NotEqual(t, hashOf(t, pbNull()), hashOf(t, pbBool(false)))
}

func TestPbValue_BoolFalseDiffersFromByteZero(t *testing.T) {
	// Bool(false) is tagBool+0; a raw Byte(0) is tagByte+0. They must not
	// alias even though the payload byte is identical.
	require.NotEqual(t, hashOf(t, pbBool(false)), hashOf(t, keys.Byte(0)))
}

func TestPbValue_NullDiffersFromByteZero(t *testing.T) {
	require.NotEqual(t, hashOf(t, pbNull()), hashOf(t, keys.Byte(0)))
}

func TestPbValue_BoolTrueDiffersFromBoolFalse(t *testing.T) {
	require.NotEqual(t, hashOf(t, pbBool(true)), hashOf(t, pbBool(false)))
}

func TestPbValue_BoolTrueDiffersFromNull(t *testing.T) {
	require.NotEqual(t, hashOf(t, pbBool(true)), hashOf(t, pbNull()))
}

func TestPbValue_StringDifferentValuesDifferentHash(t *testing.T) {
	require.NotEqual(t, hashOf(t, pbString("a")), hashOf(t, pbString("b")))
}

func TestPbValue_StringEmptyDiffersFromNil(t *testing.T) {
	// An empty PbValue string is still a String kind and writes tag+length.
	// It must not collapse to the same digest as a nil PbValue (no-op).
	require.NotEqual(t, hashOf(t, pbString("")), emptyHash(t))
}

func TestPbValue_StringEmptyDiffersFromNonEmpty(t *testing.T) {
	require.NotEqual(t, hashOf(t, pbString("")), hashOf(t, pbString("a")))
}

func TestPbValue_StringCaseSensitive(t *testing.T) {
	require.NotEqual(t, hashOf(t, pbString("hello")), hashOf(t, pbString("Hello")))
}

func TestPbValue_StringPrefixIsNotEnough(t *testing.T) {
	// Length-prefixed encoding should distinguish "ab"+"c" from "a"+"bc" when
	// embedded inside larger structures.
	a := pbList(structpb.NewStringValue("ab"), structpb.NewStringValue("c"))
	b := pbList(structpb.NewStringValue("a"), structpb.NewStringValue("bc"))
	require.NotEqual(t, hashOf(t, a), hashOf(t, b))
}

func TestPbValue_NumberDifferentValuesDifferentHash(t *testing.T) {
	require.NotEqual(t, hashOf(t, pbNumber(1.0)), hashOf(t, pbNumber(2.0)))
}

func TestPbValue_NumberPositiveVsNegative(t *testing.T) {
	require.NotEqual(t, hashOf(t, pbNumber(1.5)), hashOf(t, pbNumber(-1.5)))
}

func TestPbValue_NumberSignedZeroDifferentBitsDifferentHash(t *testing.T) {
	// +0.0 and -0.0 have different float64 bit patterns, so they should hash
	// differently under bit-level encoding.
	require.NotEqual(t,
		hashOf(t, pbNumber(0.0)),
		hashOf(t, pbNumber(math.Copysign(0, -1))))
}

func TestPbValue_NumberSpecialValuesDeterministic(t *testing.T) {
	// Each special value should hash to itself stably (NaN equality semantics
	// don't apply here — we're comparing the same float64 bit pattern).
	specials := []float64{
		math.Inf(1),
		math.Inf(-1),
		math.MaxFloat64,
		math.SmallestNonzeroFloat64,
	}
	for _, n := range specials {
		hash1 := hashOf(t, pbNumber(n))
		hash2 := hashOf(t, pbNumber(n))

		require.Equal(t, hash1, hash2)
	}
}

func TestPbValue_NumberSpecialValuesDistinct(t *testing.T) {
	values := []float64{
		math.Inf(1),
		math.Inf(-1),
		math.MaxFloat64,
		-math.MaxFloat64,
		math.SmallestNonzeroFloat64,
	}
	seen := map[string]float64{}
	for _, n := range values {
		h := hashOf(t, pbNumber(n))
		if prior, ok := seen[h]; ok {
			t.Fatalf("hash collision between %v and %v", n, prior)
		}
		seen[h] = n
	}
}

func TestPbValue_List_EmptyDiffersFromNil(t *testing.T) {
	// An empty list is a distinct value from "no value at all", so it must
	// emit some bytes (e.g. tagArray+length(0)) and not hash the same as a
	// nil PbValue (no-op).
	require.NotEqual(t, hashOf(t, pbList()), emptyHash(t))
}

func TestPbValue_List_SingleElementDiffersFromElementAlone(t *testing.T) {
	// A list wrapping a string must not hash the same as that string at the
	// top level, since the array tag and length are part of the encoding.
	require.NotEqual(t,
		hashOf(t, pbList(structpb.NewStringValue("a"))),
		hashOf(t, pbString("a")))
}

func TestPbValue_List_OrderMatters(t *testing.T) {
	a := pbList(structpb.NewStringValue("a"), structpb.NewStringValue("b"))
	b := pbList(structpb.NewStringValue("b"), structpb.NewStringValue("a"))
	require.NotEqual(t, hashOf(t, a), hashOf(t, b))
}

func TestPbValue_List_DifferentLengthsDifferentHash(t *testing.T) {
	a := pbList(structpb.NewStringValue("a"))
	b := pbList(structpb.NewStringValue("a"), structpb.NewStringValue("b"))
	require.NotEqual(t, hashOf(t, a), hashOf(t, b))
}

func TestPbValue_List_MixedKinds(t *testing.T) {
	// A heterogeneous list still hashes deterministically and is distinct from
	// a permutation that swaps kinds.
	a := pbList(
		structpb.NewStringValue("1"),
		structpb.NewNumberValue(1),
	)
	b := pbList(
		structpb.NewNumberValue(1),
		structpb.NewStringValue("1"),
	)

	hash1 := hashOf(t, a)
	hash2 := hashOf(t, a)

	require.Equal(t, hash1, hash2)

	hash3 := hashOf(t, b)

	require.NotEqual(t, hash1, hash3)
}

func TestPbValue_Struct_UsesMapTag(t *testing.T) {
	// Structs must encode with tagMap so that {k:"v"} and [Pair{k,"v"}] are
	// distinct at the wire level. Verify that the leading tag byte for a
	// struct differs from that of a list (tagArray=0x06 vs tagMap=0x07).
	s := pbStruct(map[string]*structpb.Value{
		"x": structpb.NewStringValue("y"),
	})
	l := pbList(structpb.NewStringValue("y"))

	sBuilder := &keys.Builder{}
	sBuilder.Serialize(s)
	sRaw := sBuilder.Key().Bytes()

	lBuilder := &keys.Builder{}
	lBuilder.Serialize(l)
	lRaw := lBuilder.Key().Bytes()

	require.NotEqual(t, sRaw[0], lRaw[0],
		"struct and list must use different container tags")
}

func TestPbValue_Struct_MatchesEquivalentMapOfPairs(t *testing.T) {
	// A PbValue struct must encode identically to a direct Map of Pair
	// elements with the same keys/values. PbValue internally accumulates
	// *Pair entries while walking the struct, so this guards against a
	// regression where EncodeMap stops accepting *Pair (which would cause
	// each field to be double-wrapped as Pair{Unset, *Pair{...}}).
	structVal := pbStruct(map[string]*structpb.Value{
		"a": structpb.NewStringValue("1"),
		"b": structpb.NewNumberValue(2),
	})

	equivalent := keys.Map{
		{Key: keys.String("a"), Value: keys.String("1")},
		{Key: keys.String("b"), Value: keys.Uint64(math.Float64bits(2))},
	}

	sBuilder := &keys.Builder{}
	sBuilder.Serialize(structVal)

	mBuilder := &keys.Builder{}
	mBuilder.Serialize(equivalent)

	require.Equal(t, mBuilder.Key().Bytes(), sBuilder.Key().Bytes())
}

func TestPbValue_Struct_DiffersFromListOfSamePairs(t *testing.T) {
	// A struct {"a":"1"} and a list [{"a":"1"}] (where the list element is a
	// struct) have different nesting and must not collide, but more importantly
	// a struct and a plain Array of the same pairs must differ because of the
	// tagMap vs tagArray distinction.
	structVal := pbStruct(map[string]*structpb.Value{
		"a": structpb.NewStringValue("1"),
	})
	listVal := pbList(structpb.NewStringValue("1"))

	require.NotEqual(t, hashOf(t, structVal), hashOf(t, listVal))
}

func TestPbValue_EmptyStruct_DiffersFromEmptyList(t *testing.T) {
	// Both are "empty containers" but with different tags.
	emptyStruct := pbStruct(map[string]*structpb.Value{})
	emptyList := pbList()
	require.NotEqual(t, hashOf(t, emptyStruct), hashOf(t, emptyList))
}

func TestPbValue_Struct_FieldOrderDoesNotMatter(t *testing.T) {
	// Struct fields are sorted internally, so the declaration order of the Go
	// map literal must not affect the digest.
	one := pbStruct(map[string]*structpb.Value{
		"alpha": structpb.NewStringValue("1"),
		"beta":  structpb.NewStringValue("2"),
		"gamma": structpb.NewStringValue("3"),
	})
	two := pbStruct(map[string]*structpb.Value{
		"gamma": structpb.NewStringValue("3"),
		"alpha": structpb.NewStringValue("1"),
		"beta":  structpb.NewStringValue("2"),
	})
	require.Equal(t, hashOf(t, one), hashOf(t, two))
}

func TestPbValue_Struct_DifferentValuesDifferentHash(t *testing.T) {
	a := pbStruct(map[string]*structpb.Value{"k": structpb.NewStringValue("v1")})
	b := pbStruct(map[string]*structpb.Value{"k": structpb.NewStringValue("v2")})
	require.NotEqual(t, hashOf(t, a), hashOf(t, b))
}

func TestPbValue_Struct_DifferentKeysDifferentHash(t *testing.T) {
	// Two structs with the same value(s) but different key names must hash
	// differently — otherwise a cache key can't tell, e.g., {role:"admin"} from
	// {team:"admin"}.
	a := pbStruct(map[string]*structpb.Value{"k1": structpb.NewStringValue("v")})
	b := pbStruct(map[string]*structpb.Value{"k2": structpb.NewStringValue("v")})
	require.NotEqual(t, hashOf(t, a), hashOf(t, b))
}

func TestPbValue_Struct_KeyValueSwapDifferentHash(t *testing.T) {
	// {a:1, b:2} and {a:2, b:1} have the same multiset of keys and values but
	// different bindings; their digests must differ.
	a := pbStruct(map[string]*structpb.Value{
		"a": structpb.NewNumberValue(1),
		"b": structpb.NewNumberValue(2),
	})
	b := pbStruct(map[string]*structpb.Value{
		"a": structpb.NewNumberValue(2),
		"b": structpb.NewNumberValue(1),
	})
	require.NotEqual(t, hashOf(t, a), hashOf(t, b))
}

func TestPbValue_Struct_DifferentArityDifferentHash(t *testing.T) {
	a := pbStruct(map[string]*structpb.Value{
		"a": structpb.NewNumberValue(1),
	})
	b := pbStruct(map[string]*structpb.Value{
		"a": structpb.NewNumberValue(1),
		"b": structpb.NewNumberValue(2),
	})
	require.NotEqual(t, hashOf(t, a), hashOf(t, b))
}

func TestPbValue_NestedStructInList(t *testing.T) {
	v := pbList(
		structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
			"a": structpb.NewNumberValue(1),
		}}),
		structpb.NewStringValue("tail"),
	)
	hash1 := hashOf(t, v)
	hash2 := hashOf(t, v)

	require.Equal(t, hash1, hash2)

	other := pbList(
		structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
			"a": structpb.NewNumberValue(2),
		}}),
		structpb.NewStringValue("tail"),
	)
	require.NotEqual(t, hashOf(t, v), hashOf(t, other))
}

func TestPbValue_NestedListInStruct(t *testing.T) {
	v := pbStruct(map[string]*structpb.Value{
		"xs": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
			structpb.NewStringValue("a"),
			structpb.NewStringValue("b"),
		}}),
	})
	hash1 := hashOf(t, v)
	hash2 := hashOf(t, v)

	require.Equal(t, hash1, hash2)

	other := pbStruct(map[string]*structpb.Value{
		"xs": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
			structpb.NewStringValue("b"),
			structpb.NewStringValue("a"),
		}}),
	})
	require.NotEqual(t, hashOf(t, v), hashOf(t, other))
}

func TestPbValue_NestedStructInStruct(t *testing.T) {
	// Same inner data attached under different outer keys must not collide;
	// the outer key is part of the cache key.
	inner := func() *structpb.Value {
		return structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
			"x": structpb.NewNumberValue(1),
		}})
	}
	a := pbStruct(map[string]*structpb.Value{"outerA": inner()})
	b := pbStruct(map[string]*structpb.Value{"outerB": inner()})
	require.NotEqual(t, hashOf(t, a), hashOf(t, b))
}

func TestPbValue_NestingShapeMatters(t *testing.T) {
	// [["a"]] and ["a"] are structurally distinct values (same leaf,
	// different nesting depth) and must hash differently.
	flat := pbList(structpb.NewStringValue("a"))
	nested := pbList(structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
		structpb.NewStringValue("a"),
	}}))
	require.NotEqual(t, hashOf(t, flat), hashOf(t, nested))
}

func TestPbValue_Struct_MultipleListFields(t *testing.T) {
	// A struct with multiple list-valued fields must hash correctly and
	// produce distinct digests when any field's contents differ.
	a := pbStruct(map[string]*structpb.Value{
		"a": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
			structpb.NewNumberValue(1),
		}}),
		"b": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
			structpb.NewNumberValue(2),
		}}),
	})
	b := pbStruct(map[string]*structpb.Value{
		"a": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
			structpb.NewNumberValue(1),
		}}),
		"b": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
			structpb.NewNumberValue(99),
		}}),
	})

	require.NotEqual(t, emptyHash(t), hashOf(t, a),
		"struct with list fields must not hash to empty")
	require.NotEqual(t, hashOf(t, a), hashOf(t, b),
		"structs differing in a nested list value must hash differently")
}

func TestPbValue_List_MultipleStructElements(t *testing.T) {
	// A list containing multiple struct elements must hash correctly.
	a := pbList(
		structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
			"x": structpb.NewNumberValue(1),
		}}),
		structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
			"y": structpb.NewNumberValue(2),
		}}),
	)
	b := pbList(
		structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
			"x": structpb.NewNumberValue(1),
		}}),
		structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
			"y": structpb.NewNumberValue(99),
		}}),
	)

	require.NotEqual(t, emptyHash(t), hashOf(t, a),
		"list of structs must not hash to empty")
	require.NotEqual(t, hashOf(t, a), hashOf(t, b),
		"lists differing in a nested struct value must hash differently")
}

func TestPbValue_Struct_MixedScalarAndNestedFields(t *testing.T) {
	// A struct with a mix of scalar and nested fields must include all of them
	// in the digest.
	a := pbStruct(map[string]*structpb.Value{
		"name": structpb.NewStringValue("alice"),
		"tags": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
			structpb.NewStringValue("admin"),
		}}),
		"active": structpb.NewBoolValue(true),
	})
	b := pbStruct(map[string]*structpb.Value{
		"name": structpb.NewStringValue("alice"),
		"tags": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
			structpb.NewStringValue("user"),
		}}),
		"active": structpb.NewBoolValue(true),
	})

	require.NotEqual(t, emptyHash(t), hashOf(t, a),
		"struct with mixed fields must not hash to empty")
	require.NotEqual(t, hashOf(t, a), hashOf(t, b),
		"structs differing in a nested list element must hash differently")
}

func TestPbValue_List_MixedScalarsAndNestedLists(t *testing.T) {
	// A list mixing scalars and nested lists must encode all elements.
	a := pbList(
		structpb.NewNumberValue(1),
		structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
			structpb.NewNumberValue(2),
			structpb.NewNumberValue(3),
		}}),
		structpb.NewNumberValue(4),
	)
	b := pbList(
		structpb.NewNumberValue(1),
		structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
			structpb.NewNumberValue(2),
			structpb.NewNumberValue(99),
		}}),
		structpb.NewNumberValue(4),
	)

	require.NotEqual(t, emptyHash(t), hashOf(t, a),
		"list with nested sublists must not hash to empty")
	require.NotEqual(t, hashOf(t, a), hashOf(t, b),
		"lists differing in a nested sublist element must hash differently")
}

func TestPbValue_DeeplyNestedList_NoStackOverflow(t *testing.T) {
	// The implementation uses an explicit heap-allocated stack to avoid blowing
	// the call stack on deeply nested input. 5000 levels would overflow a naive
	// recursive walk.
	depth := 5000
	var current = structpb.NewStringValue("leaf")
	for range depth {
		current = structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{current}})
	}
	v := (*keys.PbValue)(current)

	hash1 := hashOf(t, v)
	hash2 := hashOf(t, v)

	require.Equal(t, hash1, hash2)
}

func TestPbValue_DeeplyNestedStruct_NoStackOverflow(t *testing.T) {
	depth := 5000
	var current = structpb.NewStringValue("leaf")
	for range depth {
		current = structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
			"n": current,
		}})
	}
	v := (*keys.PbValue)(current)

	hash1 := hashOf(t, v)
	hash2 := hashOf(t, v)

	require.Equal(t, hash1, hash2)
}

func TestPbValue_BuilderReusable(t *testing.T) {
	// Writing the same value twice into a single Builder is NOT the same as
	// writing it once — the buffer accumulates. After Reset, the digest should
	// match a fresh Builder writing the same value.
	v := pbString("hello")

	kb := &keys.Builder{}
	kb.Serialize(v)
	once := kb.Key().String()

	kb.Reset()
	kb.Serialize(v)
	require.Equal(t, once, kb.Key().String(),
		"after Reset, builder should hash identically to a fresh Builder")
}

// -----------------------------------------------------------------------------
// Tuple.WriteTo
// -----------------------------------------------------------------------------

func tk(object, relation, user string) *keys.Tuple {
	return (*keys.Tuple)(&openfgav1.TupleKey{
		Object:   object,
		Relation: relation,
		User:     user,
	})
}

func tkCond(object, relation, user, condName string, ctx map[string]*structpb.Value) *keys.Tuple {
	return (*keys.Tuple)(&openfgav1.TupleKey{
		Object:   object,
		Relation: relation,
		User:     user,
		Condition: &openfgav1.RelationshipCondition{
			Name:    condName,
			Context: &structpb.Struct{Fields: ctx},
		},
	})
}

func TestTuple_Deterministic(t *testing.T) {
	a := tk("doc:1", "viewer", "user:alice")
	h1 := hashOf(t, a)
	h2 := hashOf(t, a)
	require.Equal(t, h1, h2)
}

func TestTuple_DifferentObjectDifferentHash(t *testing.T) {
	a := tk("doc:1", "viewer", "user:alice")
	b := tk("doc:2", "viewer", "user:alice")
	require.NotEqual(t, hashOf(t, a), hashOf(t, b))
}

func TestTuple_DifferentRelationDifferentHash(t *testing.T) {
	a := tk("doc:1", "viewer", "user:alice")
	b := tk("doc:1", "editor", "user:alice")
	require.NotEqual(t, hashOf(t, a), hashOf(t, b))
}

func TestTuple_DifferentUserDifferentHash(t *testing.T) {
	a := tk("doc:1", "viewer", "user:alice")
	b := tk("doc:1", "viewer", "user:bob")
	require.NotEqual(t, hashOf(t, a), hashOf(t, b))
}

func TestTuple_AllSameFieldsHashEqually(t *testing.T) {
	// Two distinct *openfgav1.TupleKey pointers with identical field values
	// must hash to the same digest.
	a := tk("doc:1", "viewer", "user:alice")
	b := tk("doc:1", "viewer", "user:alice")
	require.Equal(t, hashOf(t, a), hashOf(t, b))
}

func TestTuple_AllEmptyFieldsDeterministic(t *testing.T) {
	a := tk("", "", "")
	h1 := hashOf(t, a)
	h2 := hashOf(t, a)
	require.Equal(t, h1, h2)
	require.NotEqual(t, h1, hashOf(t, tk("x", "", "")))
}

func TestTuple_FieldsAreNotInterchangeable(t *testing.T) {
	// Swapping object and user must produce a different hash even though the
	// raw string set is identical.
	a := tk("a", "rel", "b")
	b := tk("b", "rel", "a")
	require.NotEqual(t, hashOf(t, a), hashOf(t, b))
}

func TestTuple_FieldBoundaryDisambiguation(t *testing.T) {
	// Because each field is length-prefixed, "ab|cd|ef" must not collide with
	// "a|bcd|ef" when packed across (object, relation, user).
	a := tk("ab", "cd", "ef")
	b := tk("a", "bcd", "ef")
	require.NotEqual(t, hashOf(t, a), hashOf(t, b))
}

func TestTuple_NilCondition_EqualsNoCondition(t *testing.T) {
	// Tuple.WriteTo only writes the condition block when GetCondition() != nil.
	// A TupleKey with no Condition and one with a nil pointer should hash the
	// same way.
	a := tk("doc:1", "viewer", "user:alice")
	b := (*keys.Tuple)(&openfgav1.TupleKey{
		Object:    "doc:1",
		Relation:  "viewer",
		User:      "user:alice",
		Condition: nil,
	})
	require.Equal(t, hashOf(t, a), hashOf(t, b))
}

func TestTuple_WithoutConditionDifferFromWithCondition(t *testing.T) {
	plain := tk("doc:1", "viewer", "user:alice")
	withCond := tkCond("doc:1", "viewer", "user:alice", "cond1", map[string]*structpb.Value{
		"x": structpb.NewStringValue("y"),
	})
	require.NotEqual(t, hashOf(t, plain), hashOf(t, withCond))
}

func TestTuple_DifferentConditionNameDifferentHash(t *testing.T) {
	a := tkCond("doc:1", "viewer", "user:alice", "cond1", map[string]*structpb.Value{
		"x": structpb.NewStringValue("y"),
	})
	b := tkCond("doc:1", "viewer", "user:alice", "cond2", map[string]*structpb.Value{
		"x": structpb.NewStringValue("y"),
	})
	require.NotEqual(t, hashOf(t, a), hashOf(t, b))
}

func TestTuple_DifferentConditionContextDifferentHash(t *testing.T) {
	a := tkCond("doc:1", "viewer", "user:alice", "cond1", map[string]*structpb.Value{
		"x": structpb.NewStringValue("y1"),
	})
	b := tkCond("doc:1", "viewer", "user:alice", "cond1", map[string]*structpb.Value{
		"x": structpb.NewStringValue("y2"),
	})
	require.NotEqual(t, hashOf(t, a), hashOf(t, b))
}

func TestTuple_ConditionContextKeysMatter(t *testing.T) {
	// Same condition value bound to different context keys must hash
	// differently.
	a := tkCond("doc:1", "viewer", "user:alice", "cond1", map[string]*structpb.Value{
		"role": structpb.NewStringValue("admin"),
	})
	b := tkCond("doc:1", "viewer", "user:alice", "cond1", map[string]*structpb.Value{
		"team": structpb.NewStringValue("admin"),
	})
	require.NotEqual(t, hashOf(t, a), hashOf(t, b))
}

func TestTuple_ConditionContextFieldOrderDoesNotMatter(t *testing.T) {
	// Condition context is a structpb.Struct, which PbValue sorts internally.
	a := tkCond("doc:1", "viewer", "user:alice", "cond1", map[string]*structpb.Value{
		"a": structpb.NewStringValue("1"),
		"b": structpb.NewStringValue("2"),
	})
	b := tkCond("doc:1", "viewer", "user:alice", "cond1", map[string]*structpb.Value{
		"b": structpb.NewStringValue("2"),
		"a": structpb.NewStringValue("1"),
	})
	require.Equal(t, hashOf(t, a), hashOf(t, b))
}

func TestTuple_NilConditionContextDeterministic(t *testing.T) {
	// An explicit Condition with a nil Context must still hash deterministically
	// and differ from the no-condition variant.
	withNilCtx := (*keys.Tuple)(&openfgav1.TupleKey{
		Object:   "doc:1",
		Relation: "viewer",
		User:     "user:alice",
		Condition: &openfgav1.RelationshipCondition{
			Name:    "cond1",
			Context: nil,
		},
	})
	h1 := hashOf(t, withNilCtx)
	h2 := hashOf(t, withNilCtx)
	require.Equal(t, h1, h2)
	require.NotEqual(t, h1, hashOf(t, tk("doc:1", "viewer", "user:alice")))
}

func TestTuple_EmptyConditionContextDiffersFromPopulated(t *testing.T) {
	empty := tkCond("doc:1", "viewer", "user:bob", "cond1", map[string]*structpb.Value{})
	populated := tkCond("doc:1", "viewer", "user:bob", "cond1", map[string]*structpb.Value{
		"x": structpb.NewStringValue("y"),
	})
	require.NotEqual(t, hashOf(t, empty), hashOf(t, populated))
}

func TestTuple_ConditionNameSwapWithContextDifferentHash(t *testing.T) {
	// Two tuples whose (name, single context-string-value) are swapped must
	// hash differently — the name field and the context's field-value cannot
	// be conflated.
	a := tkCond("doc:99", "editor", "user:alice", "alpha", map[string]*structpb.Value{
		"x": structpb.NewStringValue("beta"),
	})
	b := tkCond("doc:99", "editor", "user:alice", "beta", map[string]*structpb.Value{
		"x": structpb.NewStringValue("alpha"),
	})
	require.NotEqual(t, hashOf(t, a), hashOf(t, b))
}

// -----------------------------------------------------------------------------
// PbValue — nil/zero-kind values must not panic
// -----------------------------------------------------------------------------

func TestPbValue_NilKindInList_DoesNotPanic(t *testing.T) {
	// A structpb.Value with a nil Kind field (zero-value proto) inside a list
	// must not cause a panic when serialized.
	zeroValue := &structpb.Value{} // Kind is nil
	list := pbList(structpb.NewStringValue("a"), zeroValue, structpb.NewStringValue("b"))

	require.NotPanics(t, func() {
		hashOf(t, list)
	})

	// Must produce a deterministic, non-empty hash
	h1 := hashOf(t, list)
	h2 := hashOf(t, list)
	require.Equal(t, h1, h2)
}

func TestPbValue_NilKindInStruct_DoesNotPanic(t *testing.T) {
	// A structpb.Value with a nil Kind field as a struct field value
	zeroValue := &structpb.Value{} // Kind is nil
	s := pbStruct(map[string]*structpb.Value{
		"good": structpb.NewStringValue("hello"),
		"bad":  zeroValue,
	})

	require.NotPanics(t, func() {
		hashOf(t, s)
	})

	h1 := hashOf(t, s)
	h2 := hashOf(t, s)
	require.Equal(t, h1, h2)
}

func TestPbValue_NilKindTopLevel_DoesNotPanic(t *testing.T) {
	// A top-level structpb.Value with nil Kind
	zeroValue := (*keys.PbValue)(&structpb.Value{})

	require.NotPanics(t, func() {
		hashOf(t, zeroValue)
	})

	h1 := hashOf(t, zeroValue)
	h2 := hashOf(t, zeroValue)
	require.Equal(t, h1, h2)
}

func TestPbValue_NilKindDistinctFromNull(t *testing.T) {
	// A nil-kind value should produce a different hash from an explicit null
	zeroValue := (*keys.PbValue)(&structpb.Value{})
	nullValue := pbNull()

	require.NotEqual(t, hashOf(t, zeroValue), hashOf(t, nullValue))
}

// -----------------------------------------------------------------------------
// PbValue — empty container as a non-terminal sibling
//
// These tests target a bug where pushing a zero-element list/struct onto the
// parent stack leaves it "open" while subsequent siblings are processed. The
// next leaf finds parent.len == parent.cap (both 0) and is silently dropped
// by the `if len(parent.value) < cap(parent.value)` gate; the parent's own
// close-up also wraps a value that has already been overwritten.
//
// Symptom: distinct inputs collide (often to the empty digest), corrupting
// the cache key for any structpb context that mixes empty containers with
// real data.
// -----------------------------------------------------------------------------

func TestPbValue_Struct_EmptyListSiblingPreservesNeighbor(t *testing.T) {
	// {a: [], b: "x"} and {a: [], b: "y"} differ only in b; their digests
	// MUST differ. With the bug, both collapse to the empty digest.
	a := pbStruct(map[string]*structpb.Value{
		"a": structpb.NewListValue(&structpb.ListValue{}),
		"b": structpb.NewStringValue("x"),
	})
	b := pbStruct(map[string]*structpb.Value{
		"a": structpb.NewListValue(&structpb.ListValue{}),
		"b": structpb.NewStringValue("y"),
	})

	require.NotEqual(t, emptyHash(t), hashOf(t, a),
		"struct with empty-list sibling must not collapse to empty")
	require.NotEqual(t, hashOf(t, a), hashOf(t, b),
		"struct with empty-list sibling must still hash the non-empty sibling")
}

func TestPbValue_Struct_EmptyStructSiblingPreservesNeighbor(t *testing.T) {
	// {a: {}, b: "x"} vs {a: {}, b: "y"}.
	a := pbStruct(map[string]*structpb.Value{
		"a": structpb.NewStructValue(&structpb.Struct{}),
		"b": structpb.NewStringValue("x"),
	})
	b := pbStruct(map[string]*structpb.Value{
		"a": structpb.NewStructValue(&structpb.Struct{}),
		"b": structpb.NewStringValue("y"),
	})

	require.NotEqual(t, emptyHash(t), hashOf(t, a),
		"struct with empty-struct sibling must not collapse to empty")
	require.NotEqual(t, hashOf(t, a), hashOf(t, b),
		"struct with empty-struct sibling must still hash the non-empty sibling")
}

func TestPbValue_List_EmptyListInMiddlePreservesNeighbors(t *testing.T) {
	// ["x", [], "y"] vs ["x", [], "z"] differ only in the final element.
	a := pbList(
		structpb.NewStringValue("x"),
		structpb.NewListValue(&structpb.ListValue{}),
		structpb.NewStringValue("y"),
	)
	b := pbList(
		structpb.NewStringValue("x"),
		structpb.NewListValue(&structpb.ListValue{}),
		structpb.NewStringValue("z"),
	)

	require.NotEqual(t, emptyHash(t), hashOf(t, a),
		"list with empty sublist in the middle must not collapse to empty")
	require.NotEqual(t, hashOf(t, a), hashOf(t, b),
		"trailing element after an empty sublist must affect the digest")
}

func TestPbValue_List_EmptyStructInMiddlePreservesNeighbors(t *testing.T) {
	a := pbList(
		structpb.NewStringValue("x"),
		structpb.NewStructValue(&structpb.Struct{}),
		structpb.NewStringValue("y"),
	)
	b := pbList(
		structpb.NewStringValue("x"),
		structpb.NewStructValue(&structpb.Struct{}),
		structpb.NewStringValue("z"),
	)

	require.NotEqual(t, emptyHash(t), hashOf(t, a),
		"list with empty struct in the middle must not collapse to empty")
	require.NotEqual(t, hashOf(t, a), hashOf(t, b),
		"trailing element after an empty struct must affect the digest")
}

func TestPbValue_Struct_MultipleEmptySiblingsThenScalar(t *testing.T) {
	// {a: [], b: {}, c: "real"} vs {a: [], b: {}, c: "different"}.
	// Multiple back-to-back empty containers stress the close-up walk.
	a := pbStruct(map[string]*structpb.Value{
		"a": structpb.NewListValue(&structpb.ListValue{}),
		"b": structpb.NewStructValue(&structpb.Struct{}),
		"c": structpb.NewStringValue("real"),
	})
	b := pbStruct(map[string]*structpb.Value{
		"a": structpb.NewListValue(&structpb.ListValue{}),
		"b": structpb.NewStructValue(&structpb.Struct{}),
		"c": structpb.NewStringValue("different"),
	})

	require.NotEqual(t, emptyHash(t), hashOf(t, a),
		"struct with multiple empty-container siblings must not collapse")
	require.NotEqual(t, hashOf(t, a), hashOf(t, b))
}

func TestPbValue_Struct_LeadingEmptyContainerDoesNotShadowFollowingFields(t *testing.T) {
	// Keys are sorted, so "_empty" sorts before "value". Verifies that an
	// empty container appearing first (the most-likely-to-be-broken position
	// in the iteration order) does not shadow later fields.
	a := pbStruct(map[string]*structpb.Value{
		"_empty": structpb.NewListValue(&structpb.ListValue{}),
		"value":  structpb.NewNumberValue(1),
	})
	b := pbStruct(map[string]*structpb.Value{
		"_empty": structpb.NewListValue(&structpb.ListValue{}),
		"value":  structpb.NewNumberValue(2),
	})

	require.NotEqual(t, emptyHash(t), hashOf(t, a))
	require.NotEqual(t, hashOf(t, a), hashOf(t, b))
}

func TestPbValue_List_AllEmptyChildren(t *testing.T) {
	// [[], [], []] exercises the recursive close-up of a chain of cap=0
	// containers — the path the empty-sibling fix specifically addresses.
	// Distinct lengths of all-empty lists must hash differently.
	two := pbList(
		structpb.NewListValue(&structpb.ListValue{}),
		structpb.NewListValue(&structpb.ListValue{}),
	)
	three := pbList(
		structpb.NewListValue(&structpb.ListValue{}),
		structpb.NewListValue(&structpb.ListValue{}),
		structpb.NewListValue(&structpb.ListValue{}),
	)

	require.NotEqual(t, emptyHash(t), hashOf(t, two),
		"list of empty lists must not collapse to empty")
	require.NotEqual(t, hashOf(t, two), hashOf(t, three),
		"differing counts of empty children must hash differently")
}

func TestPbValue_Struct_AllEmptyChildren(t *testing.T) {
	// {a: [], b: {}, c: []} — every field is an empty container. Two such
	// structs with identical keys must hash equal; differing keys must hash
	// differently.
	a := pbStruct(map[string]*structpb.Value{
		"a": structpb.NewListValue(&structpb.ListValue{}),
		"b": structpb.NewStructValue(&structpb.Struct{}),
		"c": structpb.NewListValue(&structpb.ListValue{}),
	})
	aDup := pbStruct(map[string]*structpb.Value{
		"a": structpb.NewListValue(&structpb.ListValue{}),
		"b": structpb.NewStructValue(&structpb.Struct{}),
		"c": structpb.NewListValue(&structpb.ListValue{}),
	})
	differentKeys := pbStruct(map[string]*structpb.Value{
		"x": structpb.NewListValue(&structpb.ListValue{}),
		"y": structpb.NewStructValue(&structpb.Struct{}),
		"z": structpb.NewListValue(&structpb.ListValue{}),
	})

	require.NotEqual(t, emptyHash(t), hashOf(t, a),
		"struct of all-empty containers must not collapse to empty")
	require.Equal(t, hashOf(t, a), hashOf(t, aDup),
		"structurally identical all-empty structs must hash equal")
	require.NotEqual(t, hashOf(t, a), hashOf(t, differentKeys),
		"all-empty struct values with different keys must hash differently")
}

// -----------------------------------------------------------------------------
// PbValue — nil-Kind contributes to distinctness
// -----------------------------------------------------------------------------

func TestPbValue_NilKind_AffectsListDigest(t *testing.T) {
	// A list with a nil-Kind element must hash differently than the same
	// list without that element. The encoding for nil-Kind (Bytes(nil)) has
	// non-zero bytes (tag + uvarint(0)), so it cannot be invisible.
	zeroValue := &structpb.Value{}

	withNil := pbList(
		zeroValue,
		structpb.NewStringValue("a"),
	)
	withoutNil := pbList(
		structpb.NewStringValue("a"),
	)

	require.NotEqual(t, hashOf(t, withNil), hashOf(t, withoutNil),
		"a nil-Kind element must contribute to the digest, not vanish")
}

func TestPbValue_NilKind_PositionMatters(t *testing.T) {
	// [nil, "a", "b"] and ["a", "b", nil] must hash differently — list
	// element order is significant for every kind, including nil-Kind.
	zeroValue := &structpb.Value{}

	leading := pbList(
		zeroValue,
		structpb.NewStringValue("a"),
		structpb.NewStringValue("b"),
	)
	trailing := pbList(
		structpb.NewStringValue("a"),
		structpb.NewStringValue("b"),
		zeroValue,
	)

	require.NotEqual(t, hashOf(t, leading), hashOf(t, trailing),
		"nil-Kind at a different list position must hash differently")
}

func TestPbValue_NilKind_DistinctFromOtherZeroLikeValues(t *testing.T) {
	// nil-Kind, Null, Bool(false), empty string, and number 0 are all
	// "zero-ish" but each has a distinct tag in the encoding.
	zeroValue := (*keys.PbValue)(&structpb.Value{})

	values := map[string]*keys.PbValue{
		"nil-Kind":     zeroValue,
		"null":         pbNull(),
		"bool false":   pbBool(false),
		"empty string": pbString(""),
		"number zero":  pbNumber(0),
	}
	seen := map[string]string{}
	for name, v := range values {
		h := hashOf(t, v)
		if prior, ok := seen[h]; ok {
			t.Fatalf("hash collision between %q and %q", name, prior)
		}
		seen[h] = name
	}
}

// -----------------------------------------------------------------------------
// Tuple — nil Condition.Context vs empty Condition.Context aliasing
// -----------------------------------------------------------------------------

func TestTuple_NilConditionContextEqualsEmptyContext(t *testing.T) {
	// A Condition with Context == nil and one with Context = &Struct{} both
	// flow through structpb.NewStructValue(nil) / NewStructValue(empty),
	// producing an empty Array either way. The alias is intentional — there
	// is no semantic difference between "no context" and "empty context" at
	// the cache-key layer. Pin it down so a future refactor can't flip it
	// silently.
	withNilCtx := (*keys.Tuple)(&openfgav1.TupleKey{
		Object:   "doc:1",
		Relation: "viewer",
		User:     "user:alice",
		Condition: &openfgav1.RelationshipCondition{
			Name:    "cond1",
			Context: nil,
		},
	})
	withEmptyCtx := (*keys.Tuple)(&openfgav1.TupleKey{
		Object:   "doc:1",
		Relation: "viewer",
		User:     "user:alice",
		Condition: &openfgav1.RelationshipCondition{
			Name:    "cond1",
			Context: &structpb.Struct{},
		},
	})

	require.Equal(t, hashOf(t, withNilCtx), hashOf(t, withEmptyCtx),
		"nil Condition.Context and &Struct{} Condition.Context must hash identically")
}

// -----------------------------------------------------------------------------
// PbValue — empty condition Name corner case (Tuple.WriteTo)
// -----------------------------------------------------------------------------

func TestTuple_EmptyConditionNameDiffersFromNoCondition(t *testing.T) {
	// A Condition with an explicit empty Name and nil Context is still a
	// Condition; its presence must change the digest relative to a tuple
	// with Condition == nil.
	withEmptyNameCond := (*keys.Tuple)(&openfgav1.TupleKey{
		Object:   "doc:1",
		Relation: "viewer",
		User:     "user:alice",
		Condition: &openfgav1.RelationshipCondition{
			Name:    "",
			Context: nil,
		},
	})
	noCondition := tk("doc:1", "viewer", "user:alice")

	require.NotEqual(t, hashOf(t, withEmptyNameCond), hashOf(t, noCondition),
		"presence of a Condition struct must be distinguishable from its absence")
}
