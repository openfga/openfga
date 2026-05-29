package keys

import (
	"maps"
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
		return
	}

	type stack[T any] struct {
		key    string
		hasKey bool
		isMap  bool
		value  T
		next   *stack[T]
	}

	next := &stack[*structpb.Value]{value: (*structpb.Value)(pbvalue)}

	var parent *stack[[]Serializable]

	for next != nil {
		current := next
		next = next.next

		var value Serializable

		switch val := current.value.GetKind().(type) {
		case *structpb.Value_BoolValue:
			value = Bool(val.BoolValue)
		case *structpb.Value_NullValue:
			value = Null{}
		case *structpb.Value_StringValue:
			value = String(val.StringValue)
		case *structpb.Value_NumberValue:
			bits := math.Float64bits(val.NumberValue)
			value = Uint64(bits)
		case nil:
			value = Unset{}
		case *structpb.Value_ListValue:
			values := val.ListValue.GetValues()
			if len(values) == 0 {
				// Empty containers must NOT be pushed onto the parent stack:
				// a cap=0 parent would silently swallow the next sibling leaf
				// (len < cap is false, so the append is skipped). Treat the
				// empty Array as a leaf value and fall through.
				value = Array{}
				break
			}

			a := make([]Serializable, 0, len(values))
			c := stack[[]Serializable]{key: current.key, hasKey: current.hasKey, value: a, next: parent}
			parent = &c

			for i := len(values) - 1; i >= 0; i-- {
				next = &stack[*structpb.Value]{
					value: values[i],
					next:  next,
				}
			}
			continue
		case *structpb.Value_StructValue:
			fields := val.StructValue.GetFields()
			keys := slices.Sorted(maps.Keys(fields))

			if len(keys) == 0 {
				// See ListValue branch — empty structs are emitted as leaves
				// so they don't shadow following siblings.
				value = Map{}
				break
			}

			a := make([]Serializable, 0, len(keys))
			c := stack[[]Serializable]{key: current.key, hasKey: current.hasKey, isMap: true, value: a, next: parent}
			parent = &c

			for i := len(keys) - 1; i >= 0; i-- {
				next = &stack[*structpb.Value]{
					key:    keys[i],
					hasKey: true,
					value:  fields[keys[i]],
					next:   next,
				}
			}
			continue
		}

		if current.hasKey {
			value = Pair{Key: String(current.key), Value: value}
		}

		if parent == nil {
			kb.Serialize(value)
			continue
		}

		parent.value = append(parent.value, value)

		for parent != nil && len(parent.value) == cap(parent.value) {
			if parent.isMap {
				value = Map(parent.value)
			} else {
				value = Array(parent.value)
			}

			if parent.hasKey {
				value = Pair{Key: String(parent.key), Value: value}
			}

			parent = parent.next

			if parent != nil {
				parent.value = append(parent.value, value)
			} else {
				kb.Serialize(value)
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
