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

func TestListUsersIntersectionPanic(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	tests := ListUsersTests{
		{
			name: "intersection_panic_expand_rewrite",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
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
						define required: [user]
						define required_other: [user]
						define viewer: required and required_other`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "required", "user:will"),
				tuple.NewTupleKey("document:1", "required_other", "user:will"),

				tuple.NewTupleKey("document:1", "required", "user:jon"),
				tuple.NewTupleKey("document:1", "required_other", "user:maria"),
			},
			expectedUsers:     []string{},
			expectedErrorMsg:  ErrPanic.Error(),
			newListUsersQuery: NewListUsersQueryPanicExpandIntersectionExpandRewrite,
		},
	}
	tests.runListUsersTestCases(t)
}

func NewListUsersQueryPanicExpandIntersectionExpandRewrite(ds storage.RelationshipTupleReader, contextualTuples []*openfgav1.TupleKey, opts ...ListUsersQueryOption) *listUsersQuery {
	l := &listUsersQuery{
		logger:                  logger.NewNoopLogger(),
		resolveNodeBreadthLimit: serverconfig.DefaultResolveNodeBreadthLimit,
		resolveNodeLimit:        serverconfig.DefaultResolveNodeLimit,
		deadline:                serverconfig.DefaultListUsersDeadline,
		maxResults:              serverconfig.DefaultListUsersMaxResults,
		maxConcurrentReads:      serverconfig.DefaultMaxConcurrentReadsForListUsers,
		wasThrottled:            new(atomic.Bool),
		expandIntersectionExpandRewrite: func(ctx context.Context, l *listUsersQuery, req *internalListUsersRequest, rewrite *openfgav1.Userset, intersectionFoundUsersChans []chan foundUser, i int) expandResponse {
			panic(ErrPanic)
		},
		expandIntersectionCloseChannels: expandIntersectionCloseChannels,
		populateFoundUsersCountMap:      populateFoundUsersCountMap,
	}

	for _, opt := range opts {
		opt(l)
	}

	l.datastore = storagewrappers.NewRequestStorageWrapper(ds, contextualTuples, l.maxConcurrentReads)

	return l
}
