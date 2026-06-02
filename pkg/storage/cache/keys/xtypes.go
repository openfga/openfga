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

	type item[T any] struct {
		key    string
		hasKey bool
		isMap  bool
		value  T
	}

	values := make([]item[*structpb.Value], 0, 10)
	parents := make([]item[[]Serializable], 0, 10)

	values = append(values, item[*structpb.Value]{value: (*structpb.Value)(pbvalue)})

	for len(values) > 0 {
		pos := len(values) - 1
		current := values[pos]
		values = values[:pos]

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
			vals := val.ListValue.GetValues()
			if len(vals) == 0 {
				// Empty containers must NOT be pushed onto the parent stack:
				// a cap=0 parent would silently swallow the next sibling leaf
				// (len < cap is false, so the append is skipped). Treat the
				// empty Array as a leaf value and fall through.
				value = Array{}
				break
			}

			a := make([]Serializable, 0, len(vals))
			c := item[[]Serializable]{key: current.key, hasKey: current.hasKey, value: a}
			parents = append(parents, c)

			for i := len(vals) - 1; i >= 0; i-- {
				values = append(values, item[*structpb.Value]{
					value: vals[i],
				})
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
			c := item[[]Serializable]{key: current.key, hasKey: current.hasKey, isMap: true, value: a}
			parents = append(parents, c)

			for i := len(keys) - 1; i >= 0; i-- {
				values = append(values, item[*structpb.Value]{
					key:    keys[i],
					hasKey: true,
					value:  fields[keys[i]],
				})
			}
			continue
		}

		if current.hasKey {
			value = Pair{Key: String(current.key), Value: value}
		}

		if len(parents) == 0 {
			kb.Serialize(value)
			continue
		}

		for len(parents) > 0 {
			pos = len(parents) - 1
			parents[pos].value = append(parents[pos].value, value)

			if len(parents[pos].value) < cap(parents[pos].value) {
				break
			}

			parent := parents[pos]
			parents = parents[:pos]

			if parent.isMap {
				value = Map(parent.value)
			} else {
				value = Array(parent.value)
			}

			if parent.hasKey {
				value = Pair{Key: String(parent.key), Value: value}
			}
		}

		if len(parents) == 0 {
			kb.Serialize(value)
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
