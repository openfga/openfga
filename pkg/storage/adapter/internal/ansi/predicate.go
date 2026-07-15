package ansi

import "github.com/openfga/openfga/pkg/storage/adapter"

// predicateNode is a boolean-valued fragment. It always renders parenthesised when
// combined, so AND/OR/NOT compositions keep their intended precedence.
type predicateNode struct{ inner sqlWriter }

func newPredicate(inner sqlWriter) *predicateNode { return &predicateNode{inner: inner} }

func (p *predicateNode) writeSQL(r *renderer) { r.node(p.inner) }

func (p *predicateNode) And(others ...adapter.Predicate) adapter.Predicate {
	return p.combine(" AND ", others)
}

func (p *predicateNode) Or(others ...adapter.Predicate) adapter.Predicate {
	return p.combine(" OR ", others)
}

func (p *predicateNode) combine(sep string, others []adapter.Predicate) adapter.Predicate {
	parts := make([]sqlWriter, 0, len(others)+1)
	parts = append(parts, p)
	for _, o := range others {
		parts = append(parts, predWriter(o))
	}
	return newPredicate(writerFunc(func(r *renderer) {
		r.write("(")
		for i, part := range parts {
			if i > 0 {
				r.write(sep)
			}
			r.node(part)
		}
		r.write(")")
	}))
}

func (p *predicateNode) Not() adapter.Predicate {
	return newPredicate(writerFunc(func(r *renderer) {
		r.write("NOT (")
		r.node(p)
		r.write(")")
	}))
}

// orderTerm is a single ORDER BY item carrying direction and optional null ordering.
type orderTerm struct {
	expr  sqlWriter
	dir   adapter.SortDirection
	nulls adapter.NullOrdering
}

func (o *orderTerm) Nulls(ordering adapter.NullOrdering) adapter.OrderTerm {
	return &orderTerm{expr: o.expr, dir: o.dir, nulls: ordering}
}

func (o *orderTerm) writeSQL(r *renderer) {
	r.node(o.expr)
	r.write(" ")
	r.write(sortSQL(o.dir))
	if ns := nullsSQL(o.nulls); ns != "" {
		r.write(" ")
		r.write(ns)
	}
}

// orderTermWriter unwraps an OrderTerm (built via Expression.Asc / Desc / Order) into a
// renderable term. Every OrderTerm produced by this package is an *orderTerm.
func orderTermWriter(term adapter.OrderTerm) sqlWriter {
	w, ok := term.(sqlWriter)
	if !ok {
		panic("pkg/storage/adapter/internal/ansi: OrderTerm must be built via Expression.Asc / Desc / Order")
	}
	return w
}
