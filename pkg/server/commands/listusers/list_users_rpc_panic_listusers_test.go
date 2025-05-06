package listusers

import (
	"context"
	"sync/atomic"
	"testing"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/goleak"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/logger"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestListUsersDirectRelationshipPanicListUsers(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user]`

	tests := ListUsersTests{
		{
			name: "direct_relationship_panic_listusers_max_results",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: model,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:will"),
				tuple.NewTupleKey("document:1", "viewer", "user:maria"),
				tuple.NewTupleKey("document:2", "viewer", "user:jon"),
			},
			expectedUsers: []string{},
			//expectedErrorMsg:  ErrPanic.Error(),
			newListUsersQuery: NewListUsersQueryPanicListUsersMaxResults,
		},
	}
	tests.runListUsersTestCases(t)
}

func TestListUsersComputedRelationshipPanicListUsersExpand(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	tests := ListUsersTests{
		{
			name: "computed_relationship_with_contextual_tuples_panic_listusers_expand",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
				ContextualTuples: []*openfgav1.TupleKey{
					tuple.NewTupleKey("document:1", "owner", "user:will"),
					tuple.NewTupleKey("document:1", "owner", "user:maria"),
					tuple.NewTupleKey("document:2", "viewer", "user:jon"),
				},
			},
			model: `
				model
					schema 1.1
				type user
				type document
					relations
						define owner: [user]
						define viewer: owner`,
			tuples:            []*openfgav1.TupleKey{},
			expectedUsers:     []string{},
			expectedErrorMsg:  ErrPanic.Error(),
			newListUsersQuery: NewListUsersQueryPanicListUsersExpand,
		},
	}
	tests.runListUsersTestCases(t)
}

func NewListUsersQueryPanicListUsersMaxResults(ds storage.RelationshipTupleReader, contextualTuples []*openfgav1.TupleKey, opts ...ListUsersQueryOption) *listUsersQuery {
	l := &listUsersQuery{
		logger:                  logger.NewNoopLogger(),
		resolveNodeBreadthLimit: serverconfig.DefaultResolveNodeBreadthLimit,
		resolveNodeLimit:        serverconfig.DefaultResolveNodeLimit,
		deadline:                serverconfig.DefaultListUsersDeadline,
		maxResults:              serverconfig.DefaultListUsersMaxResults,
		maxConcurrentReads:      serverconfig.DefaultMaxConcurrentReadsForListUsers,
		wasThrottled:            new(atomic.Bool),
		listUsersMaxResults: func(foundUsersCh chan foundUser, foundUsersUnique map[tuple.UserString]foundUser, maxResults uint32, span trace.Span, doneWithFoundUsersCh chan struct{}) {
			panic(ErrPanic)
		},
		listUsersExpand: listUsersExpand,
	}

	for _, opt := range opts {
		opt(l)
	}

	l.datastore = storagewrappers.NewRequestStorageWrapper(ds, contextualTuples, l.maxConcurrentReads)

	return l
}

func NewListUsersQueryPanicListUsersExpand(ds storage.RelationshipTupleReader, contextualTuples []*openfgav1.TupleKey, opts ...ListUsersQueryOption) *listUsersQuery {
	l := &listUsersQuery{
		logger:                  logger.NewNoopLogger(),
		resolveNodeBreadthLimit: serverconfig.DefaultResolveNodeBreadthLimit,
		resolveNodeLimit:        serverconfig.DefaultResolveNodeLimit,
		deadline:                serverconfig.DefaultListUsersDeadline,
		maxResults:              serverconfig.DefaultListUsersMaxResults,
		maxConcurrentReads:      serverconfig.DefaultMaxConcurrentReadsForListUsers,
		wasThrottled:            new(atomic.Bool),
		listUsersMaxResults:     listUsersMaxResults,
		listUsersExpand: func(cancellableCtx context.Context, l *listUsersQuery, req *openfgav1.ListUsersRequest, dispatchCount *atomic.Uint32, foundUsersCh chan foundUser) expandResponse {
			panic(ErrPanic)
		},
	}

	for _, opt := range opts {
		opt(l)
	}

	l.datastore = storagewrappers.NewRequestStorageWrapper(ds, contextualTuples, l.maxConcurrentReads)

	return l
}
