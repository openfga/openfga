package typesystem

import (
	"context"
	"errors"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
)

// TestWeightedGraphBuildErrorRecordsSilentFallback pins the observability
// contract added for issue #3306: WeightedGraphBuildError returns the error
// captured while building the weighted graph, and nil when the graph built
// successfully. The build error is not propagated from attachGraphs (that
// requires a deprecation cycle, see the TODO there), so this accessor is the
// only way for callers to learn that every weighted-graph-gated evaluation
// path silently fell back to the default Check evaluator.
func TestWeightedGraphBuildErrorRecordsSilentFallback(t *testing.T) {
	t.Parallel()

	// A model that passes validation but fails the weighted graph build: the
	// `parent` tupleset lists `group` and `document`, `viewer`'s TTU computes
	// `editor` on both, but `group` defines no `editor` relation.
	failingModel := &openfgav1.AuthorizationModel{
		SchemaVersion: "1.1",
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{Type: "user"},
			{Type: "group"},
			{
				Type: "document",
				Relations: map[string]*openfgav1.Userset{
					"parent": {Userset: &openfgav1.Userset_This{This: &openfgav1.DirectUserset{}}},
					"viewer": {Userset: &openfgav1.Userset_TupleToUserset{TupleToUserset: &openfgav1.TupleToUserset{
						Tupleset:        &openfgav1.ObjectRelation{Relation: "parent"},
						ComputedUserset: &openfgav1.ObjectRelation{Relation: "editor"},
					}}},
					"editor": {Userset: &openfgav1.Userset_This{This: &openfgav1.DirectUserset{}}},
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"parent": {DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
							{Type: "group"},
							{Type: "document"},
						}},
						"viewer": {DirectlyRelatedUserTypes: []*openfgav1.RelationReference{}},
						"editor": {DirectlyRelatedUserTypes: []*openfgav1.RelationReference{{Type: "user"}}},
					},
				},
			},
		},
	}

	ts, err := NewAndValidate(context.Background(), failingModel)
	require.NoError(t, err, "the model must pass validation; that is the point of the issue")

	require.Nil(t, ts.GetWeightedGraph(), "the weighted graph must be nil after the failed build")
	buildErr := ts.WeightedGraphBuildError()
	require.Error(t, buildErr, "the discarded build error must be retrievable")
	require.Contains(t, buildErr.Error(), "group type does not have defined editor relation")

	// The fallback contract the nil-gated paths rely on stays unchanged.
	require.False(t, ts.UsersetUseWeight2Resolver("document", "viewer", "user", &openfgav1.RelationReference{Type: "document", RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "editor"}}))

	// Happy path: a well-formed model records no build error.
	okModel := &openfgav1.AuthorizationModel{
		SchemaVersion: "1.1",
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{Type: "user"},
			{
				Type: "document",
				Relations: map[string]*openfgav1.Userset{
					"viewer": {Userset: &openfgav1.Userset_This{This: &openfgav1.DirectUserset{}}},
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"viewer": {DirectlyRelatedUserTypes: []*openfgav1.RelationReference{{Type: "user"}}},
					},
				},
			},
		},
	}
	ts2, err := NewAndValidate(context.Background(), okModel)
	require.NoError(t, err)
	require.NotNil(t, ts2.GetWeightedGraph())
	require.NoError(t, ts2.WeightedGraphBuildError())
	require.False(t, errors.Is(ts2.WeightedGraphBuildError(), ErrInvalidModel))
}
