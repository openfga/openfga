package server

import (
	"context"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/tuple"
)

// TestV1Check_SelfReferential_Probe drives the normal (v1) Check path — no
// weighted-graph flag set — to observe what v1 actually returns for the
// self-referential shape check(o#r, r, o). The CheckReason doc claims v1
// returns TRUE unconditionally; this verifies it empirically, because the
// fallback logging gate (!Allowed) only makes sense if that claim is false.
func TestV1Check_SelfReferential_Probe(t *testing.T) {
	modelDSL := `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user, document#viewer]
	`

	// No tuple that would make document:1#viewer a viewer of document:1 via
	// storage — so any TRUE answer is the self-referential inference itself.
	tuples := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "user:anne"),
	}

	s, req := setupCheckServer(t, modelDSL, tuples)
	ctx := context.Background()

	r := &openfgav1.CheckRequest{
		StoreId:              req.GetStoreId(),
		AuthorizationModelId: req.GetAuthorizationModelId(),
		TupleKey: &openfgav1.CheckRequestTupleKey{
			Object:   "document:1",
			Relation: "viewer",
			User:     "document:1#viewer",
		},
	}

	resp, err := s.Check(ctx, r)
	if err != nil {
		t.Logf("v1 self-referential check => ERROR: %v", err)
		return
	}
	t.Logf("v1 self-referential check(document:1, viewer, document:1#viewer) => allowed=%v", resp.GetAllowed())
}
