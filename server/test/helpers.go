package test

import openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"

func This() *openfgapb.Userset {
	return &openfgapb.Userset{
		Userset: &openfgapb.Userset_This{
			This: &openfgapb.DirectUserset{},
		},
	}
}

func ComputedUserset(object, relation string) *openfgapb.Userset {
	return &openfgapb.Userset{
		Userset: &openfgapb.Userset_ComputedUserset{
			ComputedUserset: &openfgapb.ObjectRelation{
				Object:   object,
				Relation: relation,
			},
		},
	}
}

func TupleToUserset(tuplesetRelation, targetObject, targetRelation string) *openfgapb.Userset {
	return &openfgapb.Userset{
		Userset: &openfgapb.Userset_TupleToUserset{
			TupleToUserset: &openfgapb.TupleToUserset{
				Tupleset:        &openfgapb.ObjectRelation{Relation: tuplesetRelation},
				ComputedUserset: &openfgapb.ObjectRelation{Object: targetObject, Relation: targetRelation},
			},
		},
	}
}

func Union(rewrites ...*openfgapb.Userset) *openfgapb.Userset {
	var children []*openfgapb.Userset
	children = append(children, rewrites...)

	return &openfgapb.Userset{
		Userset: &openfgapb.Userset_Union{
			Union: &openfgapb.Usersets{
				Child: children,
			},
		},
	}
}

func Intersection(rewrites ...*openfgapb.Userset) *openfgapb.Userset {
	var children []*openfgapb.Userset
	children = append(children, rewrites...)

	return &openfgapb.Userset{
		Userset: &openfgapb.Userset_Intersection{
			Intersection: &openfgapb.Usersets{
				Child: children,
			},
		},
	}
}

func Difference(base *openfgapb.Userset, subtract *openfgapb.Userset) *openfgapb.Userset {

	return &openfgapb.Userset{
		Userset: &openfgapb.Userset_Difference{
			Difference: &openfgapb.Difference{
				Base:     base,
				Subtract: subtract,
			},
		},
	}
}
