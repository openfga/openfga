package listusers

import (
	"context"
	"sync/atomic"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/logger"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/tuple"
	"go.uber.org/goleak"
)

func TestListUsersUsersetsPanicExpandTTU(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	tests := ListUsersTests{
		{
			name: "evaluate_userset_in_computed_relation_of_ttu_panic_expand_ttu",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "repo", Id: "fga"},
				Relation: "reader",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type:     "org",
						Relation: "member",
					},
				},
			},
			model: `
				model
					schema 1.1
				type user

				type org
					relations
						define member: [user]
						define admin: [org#member]

				type repo
					relations
						define owner: [org]
						define reader: admin from owner`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("repo:fga", "owner", "org:x"),
				tuple.NewTupleKey("org:x", "admin", "org:x#member"),
				tuple.NewTupleKey("org:x", "member", "user:will"),
			},
			expectedUsers:     []string{},
			expectedErrorMsg:  ErrPanic.Error(),
			newListUsersQuery: NewListUsersQueryPanicExpandTTU,
		},
	}
	tests.runListUsersTestCases(t)
}

func NewListUsersQueryPanicExpandTTU(ds storage.RelationshipTupleReader, contextualTuples []*openfgav1.TupleKey, opts ...ListUsersQueryOption) *listUsersQuery {
	l := &listUsersQuery{
		logger:                  logger.NewNoopLogger(),
		resolveNodeBreadthLimit: serverconfig.DefaultResolveNodeBreadthLimit,
		resolveNodeLimit:        serverconfig.DefaultResolveNodeLimit,
		deadline:                serverconfig.DefaultListUsersDeadline,
		maxResults:              serverconfig.DefaultListUsersMaxResults,
		maxConcurrentReads:      serverconfig.DefaultMaxConcurrentReadsForListUsers,
		wasThrottled:            new(atomic.Bool),
		expandTTUDispatch: func(ctx context.Context, l *listUsersQuery, req *internalListUsersRequest, userObjectType, userObjectID, computedRelation string, resp expandResponse, foundUsersChan chan<- foundUser) expandResponse {
			panic(ErrPanic)
		},
	}

	for _, opt := range opts {
		opt(l)
	}

	l.datastore = storagewrappers.NewRequestStorageWrapper(ds, contextualTuples, l.maxConcurrentReads)

	return l
}
