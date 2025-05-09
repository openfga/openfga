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

func TestListUsersUnionPanicExpandUnion(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	tests := ListUsersTests{
		{
			name: "union_panic_expand_rewrite",
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
						define optional_1: [user]
						define optional_2: [user]
						define viewer: optional_1 or optional_2`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "optional_1", "user:will"),
				tuple.NewTupleKey("document:1", "optional_2", "user:will"),

				tuple.NewTupleKey("document:1", "optional_1", "user:jon"),
				tuple.NewTupleKey("document:1", "optional_2", "user:maria"),
			},
			expectedUsers:     []string{},
			expectedErrorMsg:  ErrPanic.Error(),
			newListUsersQuery: NewListUsersQueryPanicExpandUnionExpandRewrite,
		},
	}
	tests.runListUsersTestCases(t)
}

func NewListUsersQueryPanicExpandUnionExpandRewrite(ds storage.RelationshipTupleReader, contextualTuples []*openfgav1.TupleKey, opts ...ListUsersQueryOption) *listUsersQuery {
	l := &listUsersQuery{
		logger:                  logger.NewNoopLogger(),
		resolveNodeBreadthLimit: serverconfig.DefaultResolveNodeBreadthLimit,
		resolveNodeLimit:        serverconfig.DefaultResolveNodeLimit,
		deadline:                serverconfig.DefaultListUsersDeadline,
		maxResults:              serverconfig.DefaultListUsersMaxResults,
		maxConcurrentReads:      serverconfig.DefaultMaxConcurrentReadsForListUsers,
		wasThrottled:            new(atomic.Bool),
		expandUnionExpandRewrite: func(ctx context.Context, l *listUsersQuery, req *internalListUsersRequest, rewrite *openfgav1.Userset, unionFoundUsersChans []chan foundUser, i int) expandResponse {
			panic("panic expandUnionExpandRewrite")
		},
		expandUnionCloseChannels: expandUnionCloseChannels,
	}

	for _, opt := range opts {
		opt(l)
	}

	l.datastore = storagewrappers.NewRequestStorageWrapper(ds, contextualTuples, l.maxConcurrentReads)

	return l
}
