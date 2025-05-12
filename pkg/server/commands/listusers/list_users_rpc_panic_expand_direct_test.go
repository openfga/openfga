package listusers

import (
	"context"
	"sync/atomic"
	"testing"

	"go.uber.org/goleak"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/logger"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestListUsersExclusionPanicExpandDirect(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	tests := ListUsersTests{
		{
			name: "exclusion_with_chained_negation_panic_expand_direct",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "2"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
					schema 1.1

				type user

				type document
					relations
						define unblocked: [user]
						define blocked: [user, document#viewer] but not unblocked
						define viewer: [user, document#blocked] but not blocked
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "document:2#blocked"),
				tuple.NewTupleKey("document:2", "blocked", "document:1#viewer"),
				tuple.NewTupleKey("document:2", "viewer", "user:jon"),
				tuple.NewTupleKey("document:2", "unblocked", "user:jon"),
			},
			expectedUsers:     []string{},
			expectedErrorMsg:  ErrPanic.Error(),
			newListUsersQuery: NewListUsersQueryPanicExpandDirect,
		},
		{
			name: "non_stratifiable_exclusion_containing_cycle_1_panic_expand_direct",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type:     "document",
						Relation: "blocked",
					},
				},
			},
			model: `
				model
					schema 1.1

				type user

				type document
					relations
						define blocked: [user, document#viewer]
						define viewer: [user, document#blocked] but not blocked
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "document:2#blocked"),
				tuple.NewTupleKey("document:2", "blocked", "document:1#viewer"),
			},
			expectedUsers:     []string{},
			expectedErrorMsg:  ErrPanic.Error(),
			newListUsersQuery: NewListUsersQueryPanicExpandDirect,
		},
	}
	tests.runListUsersTestCases(t)
}

func NewListUsersQueryPanicExpandDirect(ds storage.RelationshipTupleReader, contextualTuples []*openfgav1.TupleKey, opts ...ListUsersQueryOption) *listUsersQuery {
	l := &listUsersQuery{
		logger:                  logger.NewNoopLogger(),
		resolveNodeBreadthLimit: serverconfig.DefaultResolveNodeBreadthLimit,
		resolveNodeLimit:        serverconfig.DefaultResolveNodeLimit,
		deadline:                serverconfig.DefaultListUsersDeadline,
		maxResults:              serverconfig.DefaultListUsersMaxResults,
		maxConcurrentReads:      serverconfig.DefaultMaxConcurrentReadsForListUsers,
		wasThrottled:            new(atomic.Bool),
		expandDirectDispatch: func(ctx context.Context, listUsersQuery *listUsersQuery, req *internalListUsersRequest, userObjectType, userObjectID, userRelation string, resp expandResponse, foundUsersChan chan<- foundUser, hasCycle *atomic.Bool) expandResponse {
			panic(ErrPanic)
		},
	}

	for _, opt := range opts {
		opt(l)
	}

	l.datastore = storagewrappers.NewRequestStorageWrapper(ds, contextualTuples, l.maxConcurrentReads)

	return l
}
