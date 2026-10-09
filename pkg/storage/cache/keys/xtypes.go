package keys

import (
	"math"
	"slices"

	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

// PbValue serializes an arbitrary structpb.Value (including nested structs
// and lists) into the Builder's TLV format. It uses an explicit stack rather
// than recursion to bound memory growth on deeply nested inputs.
type PbValue structpb.Value

func (pbvalue *PbValue) WriteTo(kb *Builder) {
	if pbvalue == nil {
		kb.EncodeUnset()
		return
	}

	// Streaming walk: each frame is a structpb.Value still to be emitted.
	// Container frames write their header (tagArray/tagMap + count) when
	// popped, then push their children in reverse so the next iteration
	// pops them in order. Map-entry keys are encoded inline with the value
	// (flat layout — no per-entry tagPair framing).
	type frame struct {
		key    string
		hasKey bool
		value  *structpb.Value
	}

	var stack []frame
	stack = append(stack, frame{value: (*structpb.Value)(pbvalue)})

	for len(stack) > 0 {
		pos := len(stack) - 1
		current := stack[pos]
		stack = stack[:pos]

		if current.hasKey {
			kb.EncodeString(current.key)
		}

		switch val := current.value.GetKind().(type) {
		case *structpb.Value_BoolValue:
			kb.EncodeBool(val.BoolValue)
		case *structpb.Value_NullValue:
			kb.EncodeNull()
		case *structpb.Value_StringValue:
			kb.EncodeString(val.StringValue)
		case *structpb.Value_NumberValue:
			kb.EncodeUint64(math.Float64bits(val.NumberValue))
		case nil:
			kb.EncodeUnset()
		case *structpb.Value_ListValue:
			vals := val.ListValue.GetValues()
			kb.EncodeArrayHeader(len(vals))
			for _, v := range slices.Backward(vals) {
				stack = append(stack, frame{value: v})
			}
		case *structpb.Value_StructValue:
			fields := val.StructValue.GetFields()
			keys := make([]string, 0, len(fields))
			for k := range fields {
				keys = append(keys, k)
			}
			slices.Sort(keys)

			kb.EncodeMapHeader(len(keys))
			for _, v := range slices.Backward(keys) {
				stack = append(stack, frame{
					key:    v,
					hasKey: true,
					value:  fields[v],
				})
			}
		}
	}
}

type Tuple openfgav1.TupleKey

func (t *Tuple) WriteTo(kb *Builder) {
	tk := (*openfgav1.TupleKey)(t)
	kb.EncodeString(tk.GetObject())
	kb.EncodeString(tk.GetRelation())
	kb.EncodeString(tk.GetUser())
	if condition := tk.GetCondition(); condition != nil {
		kb.EncodeString(condition.GetName())
		// condition.GetContext() may be nil; structpb.NewStructValue wraps it
		// as a Value whose StructValue is nil, and PbValue treats a nil-field
		// struct as an empty Map. The nil tolerance is intentional.
		kb.Serialize((*PbValue)(structpb.NewStructValue(condition.GetContext())))
	}
}
