package keys

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/tuple"
)

// ErrUnexpectedStructValue is an error used to indicate that
// an unexpected structpb.Value kind was encountered.
var ErrUnexpectedStructValue = errors.New("unexpected structpb value encountered")

// WriteValue writes value v to the writer w. An error
// is returned only when the underlying writer returns
// an error or an unexpected value kind is encountered.
func WriteValue(w io.StringWriter, v *structpb.Value) (err error) {
	switch val := v.GetKind().(type) {
	case *structpb.Value_BoolValue:
		_, err = w.WriteString(fmt.Sprintf("%v", val.BoolValue))
	case *structpb.Value_NullValue:
		_, err = w.WriteString("null")
	case *structpb.Value_StringValue:
		_, err = w.WriteString(val.StringValue)
	case *structpb.Value_NumberValue:
		_, err = w.WriteString(strconv.FormatFloat(val.NumberValue, 'f', -1, 64)) // -1 precision ensures we represent the 64-bit value with the maximum precision needed to represent it, see strconv#FormatFloat for more info.
	case *structpb.Value_ListValue:
		values := val.ListValue.GetValues()

		for n, vv := range values {
			if err = WriteValue(w, vv); err != nil {
				return
			}

			if n < len(values)-1 {
				if _, err = w.WriteString(","); err != nil {
					return
				}
			}
		}
	case *structpb.Value_StructValue:
		err = WriteStruct(w, val.StructValue)
	default:
		err = ErrUnexpectedStructValue
	}
	return
}

// WriteStruct writes Struct value s to writer w. When s is nil, a
// nil error is returned. An error is returned only when the underlying
// writer returns an error. The struct fields are written in the sorted
// order of their names. A comma separates fields.
func WriteStruct(w io.StringWriter, s *structpb.Struct) (err error) {
	if s == nil {
		return
	}

	fields := s.GetFields()
	keys := maps.Keys(fields)
	sort.Strings(keys)

	for _, key := range keys {
		if _, err = w.WriteString(fmt.Sprintf("'%s:'", key)); err != nil {
			return
		}

		if err = WriteValue(w, fields[key]); err != nil {
			return
		}

		if _, err = w.WriteString(","); err != nil {
			return
		}
	}
	return
}

// WriteTuples writes the set of tuples to writer w in ascending sorted order.
// The intention of this function is to write the tuples as a unique string.
// Tuples are separated by commas, and when present, conditions are included
// in the tuple string representation. WriteTuples returns an error only when
// the underlying writer returns an error.
func WriteTuples(w io.StringWriter, tuples ...*openfgav1.TupleKey) (err error) {
	sortedTuples := tuple.TupleKeys(tuples)

	// sort tulpes for a deterministic write
	sort.Sort(sortedTuples)

	// prefix to avoid overlap with previous strings written
	_, err = w.WriteString("/")
	if err != nil {
		return
	}

	for n, tupleKey := range sortedTuples {
		_, err = w.WriteString(tupleKey.GetObject() + "#" + tupleKey.GetRelation())
		if err != nil {
			return
		}

		cond := tupleKey.GetCondition()
		if cond != nil {
			// " with " is separated by spaces as those are invalid in relation names
			// and we need to ensure this cache key is unique
			// resultant cache key format is "object:object_id#relation with {condition} {context}@user:user_id"
			_, err = w.WriteString(" with " + cond.GetName())
			if err != nil {
				return
			}

			// if the condition also has context, we need an additional separator
			// which cannot be present in condition names
			if cond.GetContext() != nil {
				_, err = w.WriteString(" ")
				if err != nil {
					return
				}
			}

			// now write context to hash. Is a noop if context is nil.
			if err = WriteStruct(w, cond.GetContext()); err != nil {
				return
			}
		}

		if _, err = w.WriteString("@" + tupleKey.GetUser()); err != nil {
			return
		}

		if n < len(tuples)-1 {
			if _, err = w.WriteString(","); err != nil {
				return
			}
		}
	}
	return
}

type hasher interface {
	WriteString(value string) error
}

type hashableValue interface {
	Append(hasher) error
}

// stringHasher implements the hashableValue interface for string types.
type stringHasher string

var _ hashableValue = (*stringHasher)(nil)

func (s stringHasher) Append(h hasher) error {
	return h.WriteString(string(s))
}

// NewTupleKeysHasher returns a hasher for an array of *openfgav1.TupleKey.
// It sorts the tuples first to guarantee that two arrays that are identical except for the ordering
// return the same hash.
func NewTupleKeysHasher(tupleKeys ...*openfgav1.TupleKey) *tupleKeysHasher {
	return &tupleKeysHasher{tupleKeys}
}

// tupleKeysHasher implements the hashableValue interface for TupleKey protobuf types.
type tupleKeysHasher struct {
	tupleKeys []*openfgav1.TupleKey
}

var _ hashableValue = (*tupleKeysHasher)(nil)

func (t tupleKeysHasher) Append(h hasher) error {
	sortedTupleKeys := append([]*openfgav1.TupleKey(nil), t.tupleKeys...) // Copy input to avoid mutating it

	sort.SliceStable(sortedTupleKeys, func(i, j int) bool {
		if sortedTupleKeys[i].GetObject() != sortedTupleKeys[j].GetObject() {
			return sortedTupleKeys[i].GetObject() < sortedTupleKeys[j].GetObject()
		}

		if sortedTupleKeys[i].GetRelation() != sortedTupleKeys[j].GetRelation() {
			return sortedTupleKeys[i].GetRelation() < sortedTupleKeys[j].GetRelation()
		}

		if sortedTupleKeys[i].GetUser() != sortedTupleKeys[j].GetUser() {
			return sortedTupleKeys[i].GetUser() < sortedTupleKeys[j].GetUser()
		}

		cond1 := sortedTupleKeys[i].GetCondition()
		cond2 := sortedTupleKeys[j].GetCondition()
		if (cond1 != nil || cond2 != nil) && cond1.GetName() != cond2.GetName() {
			return cond1.GetName() < cond2.GetName()
		}
		// Note: conditions also optionally have context structs, but our contextHasher below
		// already handles those properly, we don't have to sort any further here.

		return true
	})

	// prefix to avoid overlap with previous strings written
	if err := h.WriteString("/"); err != nil {
		return err
	}

	n := 0
	for _, tupleKey := range sortedTupleKeys {
		key := strings.Builder{}
		key.WriteString(tupleKey.GetObject())
		key.WriteString("#")
		key.WriteString(tupleKey.GetRelation())

		cond := tupleKey.GetCondition()
		if cond != nil {
			// " with " is separated by spaces as those are invalid in relation names
			// and we need to ensure this cache key is unique
			// resultant cache key format is "object:object_id#relation with {condition} {context}@user:user_id"
			key.WriteString(" with ")
			key.WriteString(cond.GetName())

			// if the condition also has context, we need an additional separator
			// which cannot be present in condition names
			if cond.GetContext() != nil {
				key.WriteString(" ")
			}

			// Write the hash key to this point
			if err := h.WriteString(key.String()); err != nil {
				return err
			}

			// now write context to hash. Is a noop if context is nil.
			if err := NewContextHasher(cond.GetContext()).Append(h); err != nil {
				return err
			}

			// reset key builder for next iteration of loop
			key.Reset()
		}

		key.WriteString("@")
		key.WriteString(tupleKey.GetUser())

		if err := h.WriteString(key.String()); err != nil {
			return err
		}

		if n < len(t.tupleKeys)-1 {
			if err := h.WriteString(","); err != nil {
				return err
			}
		}

		n++
	}

	return nil
}

// contextHasher represents a hashable protobuf Struct.
//
// The contextHasher can be used to generate a stable hash of a protobuf Struct. The fields
// of the struct are ordered to produce a stable hash, and the values for each struct key
// are produced using the structValueHasher, which produces a stable hash value for the Struct
// value.
type contextHasher struct {
	*structpb.Struct
}

// NewContextHasher constructs a contextHasher which can be used to produce
// a stable hash of a protobuf Struct.
func NewContextHasher(s *structpb.Struct) *contextHasher {
	return &contextHasher{s}
}

var _ hashableValue = (*contextHasher)(nil)

func (c contextHasher) Append(h hasher) error {
	if c.Struct == nil {
		return nil
	}

	fields := c.GetFields()
	keys := maps.Keys(fields)
	sort.Strings(keys)

	for _, key := range keys {
		if err := h.WriteString(fmt.Sprintf("'%s:'", key)); err != nil {
			return err
		}

		valueHasher := structValueHasher{fields[key]}
		if err := valueHasher.Append(h); err != nil {
			return err
		}

		if err := h.WriteString(","); err != nil {
			return err
		}
	}

	return nil
}

// structValueHasher represents a hashable protobuf Struct value.
//
// The structValueHasher can be used to generate a stable hash of a protobuf Struct value.
type structValueHasher struct {
	*structpb.Value
}

var _ hashableValue = (*structValueHasher)(nil)

func (s structValueHasher) Append(h hasher) error {
	switch val := s.Kind.(type) {
	case *structpb.Value_BoolValue:
		return h.WriteString(fmt.Sprintf("%v", val.BoolValue))
	case *structpb.Value_NullValue:
		return h.WriteString("null")
	case *structpb.Value_StringValue:
		return h.WriteString(val.StringValue)
	case *structpb.Value_NumberValue:
		return h.WriteString(strconv.FormatFloat(val.NumberValue, 'f', -1, 64)) // -1 precision ensures we represent the 64-bit value with the maximum precision needed to represent it, see strconv#FormatFloat for more info.
	case *structpb.Value_ListValue:
		n := 0
		values := val.ListValue.GetValues()

		for _, v := range values {
			valueHasher := structValueHasher{v}
			if err := valueHasher.Append(h); err != nil {
				return err
			}

			if n < len(values)-1 {
				if err := h.WriteString(","); err != nil {
					return err
				}
			}

			n++
		}
	case *structpb.Value_StructValue:
		return contextHasher{val.StructValue}.Append(h)
	default:
		panic("unexpected structpb value encountered")
	}

	return nil
}
