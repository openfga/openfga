package contextualtuples

import (
	"github.com/openfga/openfga/internal/validation"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type ContextualTuples struct {
	// map where key is of form 'object#relation' and value is the list of tuples that are of form 'object#relation@X'
	usersets map[string][]*openfgapb.TupleKey
}

// New validates the input tuples against the typesystem and, if valid,
// returns those tuples as ContextualTuples
func New(typesys *typesystem.TypeSystem, tupleKeys []*openfgapb.TupleKey) (*ContextualTuples, error) {
	tupleMap := map[string]struct{}{}
	usersets := map[string][]*openfgapb.TupleKey{}
	for _, tk := range tupleKeys {
		if _, ok := tupleMap[tk.String()]; ok {
			return nil, serverErrors.DuplicateContextualTuple(tk)
		}
		tupleMap[tk.String()] = struct{}{}

		if err := validation.ValidateTuple(typesys, tk); err != nil {
			return nil, serverErrors.HandleTupleValidateError(err)
		}

		key := tupleUtils.ToObjectRelationString(tk.GetObject(), tk.GetRelation())
		usersets[key] = append(usersets[key], tk)
	}

	return &ContextualTuples{usersets: usersets}, nil
}

// Read returns the list of contextual tuples that match 'object#relation'
func (c *ContextualTuples) Read(tk *openfgapb.TupleKey) []*openfgapb.TupleKey {
	return c.usersets[tupleUtils.ToObjectRelationString(tk.GetObject(), tk.GetRelation())]
}

// ReadUserTuple returns, if found, a contextual tuple that matches the input 'object#relation@user' and a bool indicating
// whether such tuple was found.
func (c *ContextualTuples) ReadUserTuple(tk *openfgapb.TupleKey) (*openfgapb.TupleKey, bool) {
	tuples := c.Read(tk)
	for _, t := range tuples {
		if t.GetUser() == tk.GetUser() {
			return t, true
		}
	}
	return nil, false
}

// ReadUsersetTuples returns the list of tuples that match the input 'object#relation' and whose user is a userset
func (c *ContextualTuples) ReadUsersetTuples(tk *openfgapb.TupleKey) []*openfgapb.TupleKey {
	tuples := c.Read(tk)
	var res []*openfgapb.TupleKey
	for _, t := range tuples {
		if tupleUtils.GetUserTypeFromUser(t.GetUser()) == tupleUtils.UserSet {
			res = append(res, t)
		}
	}
	return res
}
