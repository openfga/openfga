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

func TestListUsersExclusionPanicExpandRewrite(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	tests := ListUsersTests{
		{
			name: "exclusion_panic_expand_rewrite_base",
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
						define blocked: [user]
						define viewer: [user] but not blocked`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:blocked_user"),
				tuple.NewTupleKey("document:1", "blocked", "user:blocked_user"),
				tuple.NewTupleKey("document:1", "viewer", "user:will"),
				tuple.NewTupleKey("document:1", "blocked", "user:another_blocked_user"),
			},
			expectedUsers:     []string{},
			expectedErrorMsg:  ErrPanic.Error(),
			newListUsersQuery: NewListUsersQueryPanicListUsersExpandRewriteBase,
		},
		{
			name: "exclusion_panic_expand_rewrite_subtract",
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
						define blocked: [user]
						define viewer: [user] but not blocked`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:blocked_user"),
				tuple.NewTupleKey("document:1", "blocked", "user:blocked_user"),
				tuple.NewTupleKey("document:1", "viewer", "user:will"),
				tuple.NewTupleKey("document:1", "blocked", "user:another_blocked_user"),
			},
			expectedUsers:     []string{},
			expectedErrorMsg:  ErrPanic.Error(),
			newListUsersQuery: NewListUsersQueryPanicListUsersExpandRewriteSubtract,
		},
	}
	tests.runListUsersTestCases(t)
}

func NewListUsersQueryPanicListUsersExpandRewriteBase(ds storage.RelationshipTupleReader, contextualTuples []*openfgav1.TupleKey, opts ...ListUsersQueryOption) *listUsersQuery {
	l := &listUsersQuery{
		logger:                  logger.NewNoopLogger(),
		resolveNodeBreadthLimit: serverconfig.DefaultResolveNodeBreadthLimit,
		resolveNodeLimit:        serverconfig.DefaultResolveNodeLimit,
		deadline:                serverconfig.DefaultListUsersDeadline,
		maxResults:              serverconfig.DefaultListUsersMaxResults,
		maxConcurrentReads:      serverconfig.DefaultMaxConcurrentReadsForListUsers,
		wasThrottled:            new(atomic.Bool),
		expandExclusionExpandRewriteBase: func(ctx context.Context, l *listUsersQuery, req *internalListUsersRequest, userset *openfgav1.Userset, baseFoundUsersCh chan foundUser) expandResponse {
			panic(ErrPanic)
		},
		expandExclusionExpandRewriteSubtract: expandExclusionExpandRewrite,
	}

	for _, opt := range opts {
		opt(l)
	}

	l.datastore = storagewrappers.NewRequestStorageWrapper(ds, contextualTuples, l.maxConcurrentReads)

	return l
}

func NewListUsersQueryPanicListUsersExpandRewriteSubtract(ds storage.RelationshipTupleReader, contextualTuples []*openfgav1.TupleKey, opts ...ListUsersQueryOption) *listUsersQuery {
	l := &listUsersQuery{
		logger:                           logger.NewNoopLogger(),
		resolveNodeBreadthLimit:          serverconfig.DefaultResolveNodeBreadthLimit,
		resolveNodeLimit:                 serverconfig.DefaultResolveNodeLimit,
		deadline:                         serverconfig.DefaultListUsersDeadline,
		maxResults:                       serverconfig.DefaultListUsersMaxResults,
		maxConcurrentReads:               serverconfig.DefaultMaxConcurrentReadsForListUsers,
		wasThrottled:                     new(atomic.Bool),
		expandExclusionExpandRewriteBase: expandExclusionExpandRewrite,
		expandExclusionExpandRewriteSubtract: func(ctx context.Context, l *listUsersQuery, req *internalListUsersRequest, userset *openfgav1.Userset, baseFoundUsersCh chan foundUser) expandResponse {
			panic(ErrPanic)
		},
	}

	for _, opt := range opts {
		opt(l)
	}

	l.datastore = storagewrappers.NewRequestStorageWrapper(ds, contextualTuples, l.maxConcurrentReads)

	return l
}
