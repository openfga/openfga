package test

import (
	"context"
	"errors"
	"os"
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser"
	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/server/commands"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	defaultResolveNodeLimit = 25
	gitHubTestDataFile      = "testdata/github.json" // relative to project root
)

var githubTuples = []*openfgapb.TupleKey{
	tuple.NewTupleKey("org:openfga", "member", "erik"),
	tuple.NewTupleKey("org:openfga", "repo_admin", "org:openfga#member"),
	tuple.NewTupleKey("repo:openfga/openfga", "admin", "team:openfga/iam#member"),
	tuple.NewTupleKey("repo:openfga/openfga", "owner", "org:openfga"),
	tuple.NewTupleKey("repo:openfga/openfga", "reader", "anne"),
	tuple.NewTupleKey("repo:openfga/openfga", "writer", "beth"),
	tuple.NewTupleKey("team:openfga/iam", "member", "charles"),
	tuple.NewTupleKey("team:openfga/iam", "member", "team:openfga/protocols#member"),
	tuple.NewTupleKey("team:openfga/protocols", "member", "diane"),
}

func CheckQueryTest(t *testing.T, datastore storage.OpenFGADatastore) {
	var tests = []struct {
		name             string
		model            string
		tuples           []*openfgapb.TupleKey
		resolveNodeLimit uint32
		request          *openfgapb.CheckRequest
		err              error
		response         *openfgapb.CheckResponse
	}{
		{
			name:             "ExecuteWithEmptyTupleKey",
			model:            `type repo`,
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: &openfgapb.TupleKey{},
			},
			err: serverErrors.InvalidCheckInput,
		},
		{
			name:             "ExecuteWithEmptyObject",
			model:            `type repo`,
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("", "reader", "someUser"),
			},
			err: serverErrors.InvalidCheckInput,
		},
		{
			name:             "ExecuteWithEmptyRelation",
			model:            `type repo`,
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "", "someUser"),
			},
			err: serverErrors.InvalidCheckInput,
		},
		{
			name:             "ExecuteWithEmptyUser",
			model:            `type repo`,
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "reader", ""),
			},
			err: serverErrors.InvalidCheckInput,
		},
		{
			name:             "ExecuteWithRequestRelationInexistentInTypeDefinition",
			model:            `type repo`,
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "inexistent", "someUser"),
			},
			err: serverErrors.RelationNotFound("inexistent", "repo", tuple.NewTupleKey("repo:openfga/openfga", "inexistent", "someUser")),
		},
		{
			name: "ExecuteFailsWithInvalidUser",
			model: `
type repo
	relations
		define admin as self
`,
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "john:albert:doe"),
			},
			err: serverErrors.InvalidUser("john:albert:doe"),
		},
		{
			name: "ExecuteReturnsErrorNotStackOverflowForInfinitelyRecursiveResolution",
			model: `
type repo
	relations
		define reader as writer
		define writer as reader
`,
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "reader", "someUser"),
			},
			err: serverErrors.AuthorizationModelResolutionTooComplex,
		},
		{
			name: "ExecuteReturnsResolutionTooComplexErrorForComplexResolution",
			model: `
type repo
	relations
		define reader as self
		define writer as reader
`,
			resolveNodeLimit: 2,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "writer", "someUser"),
			},
			err: serverErrors.AuthorizationModelResolutionTooComplex,
		},
		{
			name: "ExecuteReturnsResolutionTooComplexErrorForComplexUnionResolution",
			model: `
type repo
	relations
		define writer as self
		define reader as self or writer
`,
			resolveNodeLimit: 2,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "reader", "someUser"),
			},
			err: serverErrors.AuthorizationModelResolutionTooComplex,
		},
		{
			name: "ExecuteWithExistingTupleKeyAndEmptyUserSetReturnsAllowed",
			model: `
type repo
	relations
		define admin as self
`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga/openfga", "admin", "github|jose@openfga"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "github|jose@openfga"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".(direct).",
			},
		},
		{
			name: "ExecuteWithAllowAllTupleKeyAndEmptyUserSetReturnsAllowed",
			model: `
type repo
	relations
		define admin as self
`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga/openfga", "admin", "*"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "github|jose@openfga"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".(direct).",
			},
		},
		{
			name: "ExecuteWithNonExistingTupleKeyAndEmptyUserSetReturnsNotAllowed",
			model: `
type repo
	relations
		define admin as self
`,
			tuples:           []*openfgapb.TupleKey{},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "github|jose@openfga"),
			},
			response: &openfgapb.CheckResponse{
				Allowed: false,
			},
		},
		{
			name: "ExecuteWithUnionAndDirectUserSetReturnsAllowedIfDirectTupleExists",
			model: `
type repo
	relations
		define admin as self
`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga/openfga", "admin", "github|jose@openfga"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "github|jose@openfga"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".(direct).",
			},
		},
		{
			name: "ExecuteWithUnionAndDirectUserSetReturnsAllowedIfAllUsersTupleExists",
			model: `
type repo
	relations
		define admin as self
`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga/openfga", "admin", "*"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "github|jose@openfga"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".(direct).",
			},
		},
		{
			name: "ExecuteWithUnionAndComputedUserSetReturnsNotAllowedIfComputedUsersetDoesNotIncludeUser",
			model: `
type repo
	relations
		define owner as self
		define admin as owner
`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga/openfga", "admin", "github|jose@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "owner", "team/iam"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "github|jose@openfga"),
			},
			response: &openfgapb.CheckResponse{
				Allowed: false,
			},
		},
		{
			name: "ExecuteWithUnionAndComputedUserSetReturnsAllowedIfComputedUsersetIncludesUser",
			model: `
type repo
	relations
		define writer as self 
		define reader as writer
`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga/openfga", "writer", "github|jose@openfga"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "reader", "github|jose@openfga"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".(computed-userset).repo:openfga/openfga#writer.(direct).",
			},
		},
		{
			name: "ExecuteDirectSetReturnsAllowedIfUserHasRelationWithAnObjectThatHasUserAccessToTheTargetObject",
			model: `
type repo
	relations
		define admin as self
type team
	relations
		define team_member as self
`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("team:iam", "team_member", "github|jose@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "admin", "team:iam#team_member"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "github|jose@openfga"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".(direct).team:iam#team_member.(direct).",
			},
		},
		{
			name: "ExecuteReturnsAllowedIfUserIsHasRelationToAnObjectThatIsInComputedUserSetForAnotherObject",
			model: `
type repo
	relations
		define writer as self
		define reader as self or writer
type team
	relations
		define team_member as self
`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("team:iam", "team_member", "github|jose@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "writer", "team:iam#team_member"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "reader", "github|jose@openfga"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.1(computed-userset).repo:openfga/openfga#writer.(direct).team:iam#team_member.(direct).",
			},
		},
		{
			name: "ExecuteReturnsNotAllowedIfIntersectionIsRequiredAndUserIsInOneUserSetButNotTheOther",
			model: `
type openfgastore
	relations
		define create_user as self
		define write_organization as self
		define create_organization_user as create_user and write_organization
`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("openfgastore:yenkel-dev", "create_user", "github|yenkel@openfga"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("openfgastore:yenkel-dev", "create_organization_user", "github|yenkel@openfga"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed: false,
			},
		},
		{
			name: "ExecuteReturnsAllowedIfIntersectionIsRequiredAndUserIsInAllUserSets",
			model: `
type openfgastore
	relations
		define create_user as self
		define write_organization as self
		define create_organization_user as create_user and write_organization
`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("openfgastore:yenkel-dev", "create_user", "github|yenkel@openfga"),
				tuple.NewTupleKey("openfgastore:yenkel-dev", "write_organization", "github|yenkel@openfga"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("openfgastore:yenkel-dev", "create_organization_user", "github|yenkel@openfga"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".[.0(computed-userset).openfgastore:yenkel-dev#create_user.(direct).,.1(computed-userset).openfgastore:yenkel-dev#write_organization.(direct).]",
			},
		},
		{
			name: "ExecuteSupportsNestedIntersectionAndCorrectlyTraces",
			model: `
type openfgastore
	relations
		define create_user_a as self
		define create_user_b as self
		define write_organization as self
		define create_organization_user as create_user and write_organization
		define create_user as create_user_a and create_user_b
`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("openfgastore:yenkel-dev", "create_user_a", "github|yenkel@openfga"),
				tuple.NewTupleKey("openfgastore:yenkel-dev", "create_user_b", "github|yenkel@openfga"),
				tuple.NewTupleKey("openfgastore:yenkel-dev", "write_organization", "github|yenkel@openfga"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("openfgastore:yenkel-dev", "create_organization_user", "github|yenkel@openfga"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".[.0(computed-userset).openfgastore:yenkel-dev#create_user.[.0(computed-userset).openfgastore:yenkel-dev#create_user.0(computed-userset).openfgastore:yenkel-dev#create_user_a.(direct).,.0(computed-userset).openfgastore:yenkel-dev#create_user.1(computed-userset).openfgastore:yenkel-dev#create_user_b.(direct).],.1(computed-userset).openfgastore:yenkel-dev#write_organization.(direct).]",
			},
		},
		{
			name: "ExecuteReturnsAllowedForUserNotRemovedByDifference",
			model: `
type repo
	relations
		define banned as self
		define admin as self but not banned
`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga", "admin", "github|anna@openfga"),
				tuple.NewTupleKey("repo:openfga", "banned", "github|jose@openfga"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga", "admin", "github|anna@openfga"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".0(direct).",
			},
		},
		{
			name: "ExecuteReturnsNotAllowedForUserRemovedByDifference",
			model: `
type repo
	relations
		define banned as self
		define admin as self but not banned
`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga/canaveral", "admin", "github|jon.allie@openfga"),
				tuple.NewTupleKey("repo:openfga/canaveral", "banned", "github|jon.allie@openfga"),
				tuple.NewTupleKey("repo:openfga/canaveral", "banned", "github|jose@openfga"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/canaveral", "admin", "github|jon.allie@openfga"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed: false,
			},
		},
		{
			name: "ExecuteReturnsAllowedForTupleToUserset",
			model: `
type repo
	relations
		define manager as self
		define admin as self or repo_admin from manager
type org
	relations
		define repo_admin as self
`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga/canaveral", "manager", "org:openfga#repo_admin"),
				tuple.NewTupleKey("org:openfga", "repo_admin", "github|jose@openfga"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/canaveral", "admin", "github|jose@openfga"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.1(tuple-to-userset).repo:openfga/canaveral#manager.org:openfga#repo_admin.(direct).",
			},
		},
		{
			name: "ExecuteCanResolveRecursiveComputedUserSets",
			model: `
type repo
	relations
		define admin as self or repo_admin from manager
		define maintainer as self or admin
		define writer as self or maintainer or repo_writer from manager
		define triager as self or writer
		define reader as self or triager or repo_reader from manager
type team
	relations
		define member as self
`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga/openfga", "writer", "team:openfga#member"),
				tuple.NewTupleKey("team:openfga", "member", "github|iaco@openfga"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "reader", "github|iaco@openfga"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.0union.1(computed-userset).repo:openfga/openfga#triager.union.1(computed-userset).repo:openfga/openfga#writer.union.0union.0(direct).team:openfga#member.(direct).",
			},
		},
		{
			name: "ExecuteCanResolveRecursiveTupleToUserSets",
			model: `
type document
	relations
		define parent as self
		define owner as self
		define editor as self or owner
		define viewer as editor or viewer from parent
`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:octo_v2_draft", "parent", "document:octo_folder"),
				tuple.NewTupleKey("document:octo_folder", "editor", "google|iaco@openfga"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("document:octo_v2_draft", "viewer", "google|iaco@openfga"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.1(tuple-to-userset).document:octo_v2_draft#parent.document:octo_folder#viewer.union.0(computed-userset).document:octo_folder#editor.union.0(direct).",
			},
		},
		{
			name: "CheckWithUsersetAsUser",
			model: `
type team
	relations
		define member as self
type org
	relations
		define member as self
`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("team:iam", "member", "team:engineering#member"),
				tuple.NewTupleKey("team:engineering", "member", "org:openfga#member"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("team:iam", "member", "org:openfga#member"),
			},
			response: &openfgapb.CheckResponse{
				Allowed: true,
			},
		},
		{
			name: "CheckUsersetAsUser_WithContextualTuples",
			model: `
type team
	relations
		define member as self
type org
	relations
		define member as self
`,
			tuples:           []*openfgapb.TupleKey{},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("team:iam", "member", "org:openfga#member"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{
						tuple.NewTupleKey("team:iam", "member", "team:engineering#member"),
						tuple.NewTupleKey("team:engineering", "member", "org:openfga#member"),
					},
				},
			},
			response: &openfgapb.CheckResponse{
				Allowed: true,
			},
		},
		{
			name: "CheckUsersetAsUser_WithContextualTuples",
			model: `
type team
	relations
		define member as self
type org
	relations
		define member as self
`,
			tuples:           []*openfgapb.TupleKey{},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("team:iam", "member", "org:openfga#member"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{
						tuple.NewTupleKey("team:iam", "member", "team:engineering#member"),
						tuple.NewTupleKey("team:engineering", "member", "org:openfga#member"),
					},
				},
			},
			response: &openfgapb.CheckResponse{
				Allowed: true,
			},
		},
		{
			name: "CheckUsersetAsUser_WithContextualTuples",
			model: `
type team
	relations
		define member as self
type org
	relations
		define member as self
`,
			tuples:           []*openfgapb.TupleKey{},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("team:iam", "member", "org:openfga#member"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{
						tuple.NewTupleKey("team:iam", "member", "team:engineering#member"),
						tuple.NewTupleKey("team:engineering", "member", "org:openfga#member"),
					},
				},
			},
			response: &openfgapb.CheckResponse{
				Allowed: true,
			},
		},
		{
			name: "Check with TupleToUserset involving no object or userset",
			model: `
type folder
	relations
		define viewer as self
type document
	relations
		define parent as self
		define viewer as viewer from parent
`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:doc1", "parent", "folder1"), // folder1 isn't an object or userset
				tuple.NewTupleKey("folder:folder1", "viewer", "anne"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("document:doc1", "viewer", "anne"),
			},
			response: &openfgapb.CheckResponse{
				Allowed: false,
			},
		},
		{
			name: "TupleToUserset Check Passes when at least one tupleset relation resolves",
			model: `
type folder
	relations
		define viewer as self
type document
	relations
		define parent as self
		define viewer as viewer from parent
`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:doc1", "parent", "folder1"), // folder1 isn't an object or userset
				tuple.NewTupleKey("document:doc1", "parent", "folder:folder1"),
				tuple.NewTupleKey("folder:folder1", "viewer", "anne"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("document:doc1", "viewer", "anne"),
			},
			response: &openfgapb.CheckResponse{
				Allowed: true,
			},
		},
		{
			name: "Error if * encountered in TupleToUserset evaluation",
			model: `
type folder
	relations
		define viewer as self
type document
	relations
		define parent as self
		define viewer as viewer from parent
`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:doc1", "parent", "*"), // wildcard not allowed on tupleset relations
				tuple.NewTupleKey("folder:folder1", "viewer", "user:anne"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("document:doc1", "viewer", "user:anne"),
			},
			err: serverErrors.InvalidTuple(
				"unexpected wildcard evaluated on relation 'document#parent'",
				tuple.NewTupleKey("document:doc1", "parent", commands.Wildcard),
			),
		},
		{
			name: "Error if * encountered in TTU evaluation including ContextualTuples",
			model: `
type folder
	relations
		define viewer as self
type document
	relations
		define parent as self
		define viewer as viewer from parent
`,
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("document:doc1", "viewer", "user:anne"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{
						tuple.NewTupleKey("document:doc1", "parent", "*"), // wildcard not allowed on tupleset relations
						tuple.NewTupleKey("folder:folder1", "viewer", "user:anne"),
					},
				},
			},
			err: serverErrors.InvalidTuple(
				"unexpected wildcard evaluated on relation 'document#parent'",
				tuple.NewTupleKey("document:doc1", "parent", commands.Wildcard),
			),
		},
		{
			name: "Error if rewrite encountered in tupleset relation",
			model: `
type document
	relations
		define editor as self
		define parent as editor
		define viewer as viewer from parent
`,
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey:         tuple.NewTupleKey("document:doc1", "viewer", "anne"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{},
			},
			err: serverErrors.InvalidAuthorizationModelInput(
				errors.New("unexpected rewrite on relation 'document#parent'"),
			),
		},
	}

	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	meter := telemetry.NewNoopMeter()
	logger := logger.NewNoopLogger()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := ulid.Make().String()
			model := &openfgapb.AuthorizationModel{
				Id:              ulid.Make().String(),
				SchemaVersion:   typesystem.SchemaVersion1_0,
				TypeDefinitions: parser.MustParse(test.model),
			}

			err := datastore.WriteAuthorizationModel(ctx, store, model)
			require.NoError(t, err)

			if test.tuples != nil {
				err := datastore.Write(ctx, store, nil, test.tuples)
				require.NoError(t, err)
			}

			cmd := commands.NewCheckQuery(datastore, tracer, meter, logger, test.resolveNodeLimit)
			test.request.StoreId = store
			test.request.AuthorizationModelId = model.Id
			resp, err := cmd.Execute(ctx, test.request)
			require.ErrorIs(t, err, test.err)

			if test.response != nil {
				require.NoError(t, err)

				require.Equal(t, test.response.Allowed, resp.Allowed)

				if test.response.Allowed {
					require.Equal(t, test.response.Resolution, resp.Resolution)
				}
			}
		})
	}
}

func TestCheckQueryAgainstGitHubModel(t *testing.T, datastore storage.OpenFGADatastore) {
	var tests = []struct {
		name             string
		resolveNodeLimit uint32
		request          *openfgapb.CheckRequest
		err              error
		response         *openfgapb.CheckResponse
	}{
		{
			name:             "GitHubAssertion1",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "reader", "anne"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.0(direct).",
			},
		},
		{
			name:             "GitHubAssertion2",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "triager", "anne"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed: false,
			},
		},
		{
			name:             "GitHubAssertion3",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "diane"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.0(direct).team:openfga/iam#member.(direct).team:openfga/protocols#member.(direct).",
			},
		},
		{
			name:             "GitHubAssertion4",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "reader", "erik"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.1(computed-userset).repo:openfga/openfga#triager.union.1(computed-userset).repo:openfga/openfga#writer.union.1(computed-userset).repo:openfga/openfga#maintainer.union.1(computed-userset).repo:openfga/openfga#admin.union.1(tuple-to-userset).repo:openfga/openfga#owner.org:openfga#repo_admin.(direct).org:openfga#member.union.0(direct).",
			},
		},
		{
			name:             "GitHubAssertion5",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "writer", "charles"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.1(computed-userset).repo:openfga/openfga#maintainer.union.1(computed-userset).repo:openfga/openfga#admin.union.0(direct).team:openfga/iam#member.(direct).",
			},
		},
		{
			name:             "GitHubAssertion6",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "beth"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed: false,
			},
		},
	}

	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	meter := telemetry.NewNoopMeter()
	logger := logger.NewNoopLogger()

	data, err := os.ReadFile(gitHubTestDataFile)
	require.NoError(t, err)

	var gitHubDefinition openfgapb.WriteAuthorizationModelRequest
	err = protojson.Unmarshal(data, &gitHubDefinition)
	require.NoError(t, err)

	store := ulid.Make().String()
	model := &openfgapb.AuthorizationModel{
		Id:              ulid.Make().String(),
		SchemaVersion:   typesystem.SchemaVersion1_0,
		TypeDefinitions: gitHubDefinition.GetTypeDefinitions(),
	}

	err = datastore.WriteAuthorizationModel(ctx, store, model)
	require.NoError(t, err)

	err = datastore.Write(ctx, store, nil, githubTuples)
	require.NoError(t, err)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd := commands.NewCheckQuery(datastore, tracer, meter, logger, test.resolveNodeLimit)
			test.request.StoreId = store
			test.request.AuthorizationModelId = model.Id
			resp, gotErr := cmd.Execute(ctx, test.request)

			if test.err == nil {
				require.NoError(t, gotErr)
			}

			if test.err != nil {
				require.EqualError(t, test.err, gotErr.Error())
			}

			if test.response != nil {
				require.NoError(t, gotErr)

				require.Equal(t, test.response.Allowed, resp.Allowed)

				if test.response.Allowed {
					require.Equal(t, test.response.Resolution, resp.Resolution)
				}
			}
		})
	}
}

func TestCheckQueryWithContextualTuplesAgainstGitHubModel(t *testing.T, datastore storage.OpenFGADatastore) {
	var tests = []struct {
		name             string
		resolveNodeLimit uint32
		request          *openfgapb.CheckRequest
		err              error
		response         *openfgapb.CheckResponse
	}{
		{
			name:             "ContextualTuplesGitHubAssertion1",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey:         tuple.NewTupleKey("repo:openfga/openfga", "reader", "anne"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{TupleKeys: githubTuples},
				Trace:            true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.0(direct).",
			},
		},
		{
			name:             "ContextualTuplesGitHubAssertion2",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey:         tuple.NewTupleKey("repo:openfga/openfga", "triager", "anne"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{TupleKeys: githubTuples},
				Trace:            true,
			},
			response: &openfgapb.CheckResponse{
				Allowed: false,
			},
		},
		{
			name:             "ContextualTuplesGitHubAssertion3",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey:         tuple.NewTupleKey("repo:openfga/openfga", "admin", "diane"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{TupleKeys: githubTuples},
				Trace:            true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.0(direct).team:openfga/iam#member.(direct).team:openfga/protocols#member.(direct).",
			},
		},
		{
			name:             "ContextualTuplesGitHubAssertion4",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey:         tuple.NewTupleKey("repo:openfga/openfga", "reader", "erik"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{TupleKeys: githubTuples},
				Trace:            true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.1(computed-userset).repo:openfga/openfga#triager.union.1(computed-userset).repo:openfga/openfga#writer.union.1(computed-userset).repo:openfga/openfga#maintainer.union.1(computed-userset).repo:openfga/openfga#admin.union.1(tuple-to-userset).repo:openfga/openfga#owner.org:openfga#repo_admin.(direct).org:openfga#member.union.0(direct).",
			},
		},
		{
			name:             "ContextualTuplesGitHubAssertion5",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey:         tuple.NewTupleKey("repo:openfga/openfga", "writer", "charles"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{TupleKeys: githubTuples},
				Trace:            true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.1(computed-userset).repo:openfga/openfga#maintainer.union.1(computed-userset).repo:openfga/openfga#admin.union.0(direct).team:openfga/iam#member.(direct).",
			},
		},
		{
			name:             "ContextualTuplesGitHubAssertion6",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey:         tuple.NewTupleKey("repo:openfga/openfga", "admin", "beth"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{TupleKeys: githubTuples},
				Trace:            true,
			},
			response: &openfgapb.CheckResponse{
				Allowed: false,
			},
		},
		{
			name:             "RepeatedContextualTuplesShouldError",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "reader", "anne"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{
						tuple.NewTupleKey("repo:openfga/openfga", "reader", "anne"),
						tuple.NewTupleKey("repo:openfga/openfga", "reader", "anne"),
					},
				},
				Trace: true,
			},
			err: serverErrors.DuplicateContextualTuple(tuple.NewTupleKey("repo:openfga/openfga", "reader", "anne")),
		},
		{
			name:             "ContextualTuplesWithEmptyUserFails",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "beth"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{
						tuple.NewTupleKey("org:openfga", "member", ""),
					},
				},
				Trace: true,
			},
			err: serverErrors.InvalidContextualTuple(tuple.NewTupleKey("org:openfga", "member", "")),
		},
		{
			name:             "ContextualTuplesWithEmptyRelationFails",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "beth"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{
						tuple.NewTupleKey("org:openfga", "", "anne"),
					},
				},
				Trace: true,
			},
			err: serverErrors.InvalidContextualTuple(tuple.NewTupleKey("org:openfga", "", "anne")),
		},
		{
			name:             "ContextualTuplesWithEmptyObjectFails",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "beth"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{
						tuple.NewTupleKey("", "member", "anne"),
					},
				},
				Trace: true,
			},
			err: serverErrors.InvalidContextualTuple(tuple.NewTupleKey("", "member", "anne")),
		},
	}

	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	meter := telemetry.NewNoopMeter()
	logger := logger.NewNoopLogger()

	data, err := os.ReadFile(gitHubTestDataFile)
	require.NoError(t, err)

	var gitHubDefinition openfgapb.WriteAuthorizationModelRequest
	err = protojson.Unmarshal(data, &gitHubDefinition)
	require.NoError(t, err)

	storeID := ulid.Make().String()
	model := &openfgapb.AuthorizationModel{
		Id:              ulid.Make().String(),
		SchemaVersion:   typesystem.SchemaVersion1_0,
		TypeDefinitions: gitHubDefinition.GetTypeDefinitions(),
	}

	err = datastore.WriteAuthorizationModel(ctx, storeID, model)
	require.NoError(t, err)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd := commands.NewCheckQuery(datastore, tracer, meter, logger, test.resolveNodeLimit)
			test.request.StoreId = storeID
			test.request.AuthorizationModelId = model.Id
			resp, gotErr := cmd.Execute(ctx, test.request)

			if test.err == nil {
				require.NoError(t, gotErr)
			}

			if test.err != nil {
				require.EqualError(t, test.err, gotErr.Error())
			}

			if test.response != nil {
				require.NoError(t, gotErr)

				require.Equal(t, test.response.Allowed, resp.Allowed)

				if test.response.Allowed {
					require.Equal(t, test.response.Resolution, resp.Resolution)
				}
			}
		})
	}
}

func TestCheckQueryAuthorizationModelsVersioning(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	meter := telemetry.NewNoopMeter()
	logger := logger.NewNoopLogger()
	store := ulid.Make().String()

	oldModel := &openfgapb.AuthorizationModel{
		Id:            ulid.Make().String(),
		SchemaVersion: typesystem.SchemaVersion1_0,
		TypeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"owner": {Userset: &openfgapb.Userset_This{}},
					"editor": {
						Userset: &openfgapb.Userset_Union{
							Union: &openfgapb.Usersets{Child: []*openfgapb.Userset{
								{Userset: &openfgapb.Userset_This{}},
								{Userset: &openfgapb.Userset_ComputedUserset{ComputedUserset: &openfgapb.ObjectRelation{Relation: "owner"}}},
							}},
						},
					},
				},
			},
		},
	}

	err := datastore.WriteAuthorizationModel(ctx, store, oldModel)
	require.NoError(t, err)

	updatedModel := &openfgapb.AuthorizationModel{
		Id:            ulid.Make().String(),
		SchemaVersion: typesystem.SchemaVersion1_0,
		TypeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"owner":  {Userset: &openfgapb.Userset_This{}},
					"editor": {Userset: &openfgapb.Userset_This{}},
				},
			},
		},
	}

	err = datastore.WriteAuthorizationModel(ctx, store, updatedModel)
	require.NoError(t, err)

	err = datastore.Write(ctx, store, []*openfgapb.TupleKey{}, []*openfgapb.TupleKey{{Object: "repo:openfgapb", Relation: "owner", User: "yenkel"}})
	require.NoError(t, err)

	oldResp, err := commands.NewCheckQuery(datastore, tracer, meter, logger, defaultResolveNodeLimit).Execute(ctx, &openfgapb.CheckRequest{
		StoreId:              store,
		AuthorizationModelId: oldModel.Id,
		TupleKey: &openfgapb.TupleKey{
			Object:   "repo:openfgapb",
			Relation: "editor",
			User:     "yenkel",
		},
	})
	require.NoError(t, err)
	require.True(t, oldResp.Allowed)

	updatedResp, err := commands.NewCheckQuery(datastore, tracer, meter, logger, defaultResolveNodeLimit).Execute(ctx, &openfgapb.CheckRequest{
		StoreId:              store,
		AuthorizationModelId: updatedModel.Id,
		TupleKey: &openfgapb.TupleKey{
			Object:   "repo:openfgapb",
			Relation: "editor",
			User:     "yenkel",
		},
	})
	require.NoError(t, err)
	require.False(t, updatedResp.Allowed)
}

var tuples = []*openfgapb.TupleKey{
	tuple.NewTupleKey("repo:openfga/openfga", "reader", "team:openfga#member"),
	tuple.NewTupleKey("team:openfga", "member", "github|iaco@openfga"),
}

// Used to avoid compiler optimizations (see https://dave.cheney.net/2013/06/30/how-to-write-benchmarks-in-go)
var result *openfgapb.CheckResponse //nolint

func BenchmarkCheckWithoutTrace(b *testing.B, datastore storage.OpenFGADatastore) {
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	meter := telemetry.NewNoopMeter()
	logger := logger.NewNoopLogger()
	store := ulid.Make().String()

	data, err := os.ReadFile(gitHubTestDataFile)
	require.NoError(b, err)

	var gitHubTypeDefinitions openfgapb.WriteAuthorizationModelRequest
	err = protojson.Unmarshal(data, &gitHubTypeDefinitions)
	require.NoError(b, err)

	model := &openfgapb.AuthorizationModel{
		Id:              ulid.Make().String(),
		SchemaVersion:   typesystem.SchemaVersion1_0,
		TypeDefinitions: gitHubTypeDefinitions.GetTypeDefinitions(),
	}

	err = datastore.WriteAuthorizationModel(ctx, store, model)
	require.NoError(b, err)

	err = datastore.Write(ctx, store, []*openfgapb.TupleKey{}, tuples)
	require.NoError(b, err)

	checkQuery := commands.NewCheckQuery(datastore, tracer, meter, logger, defaultResolveNodeLimit)

	var r *openfgapb.CheckResponse

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ = checkQuery.Execute(ctx, &openfgapb.CheckRequest{
			StoreId:              store,
			AuthorizationModelId: model.Id,
			TupleKey: &openfgapb.TupleKey{
				Object:   "repo:openfga/openfga",
				Relation: "reader",
				User:     "github|iaco@openfga",
			},
		})
	}

	result = r
}

func BenchmarkWithTrace(b *testing.B, datastore storage.OpenFGADatastore) {
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	meter := telemetry.NewNoopMeter()
	logger := logger.NewNoopLogger()
	store := ulid.Make().String()

	data, err := os.ReadFile(gitHubTestDataFile)
	require.NoError(b, err)

	var gitHubTypeDefinitions openfgapb.WriteAuthorizationModelRequest
	err = protojson.Unmarshal(data, &gitHubTypeDefinitions)
	require.NoError(b, err)

	model := &openfgapb.AuthorizationModel{
		Id:              ulid.Make().String(),
		SchemaVersion:   typesystem.SchemaVersion1_0,
		TypeDefinitions: gitHubTypeDefinitions.GetTypeDefinitions(),
	}

	err = datastore.WriteAuthorizationModel(ctx, store, model)
	require.NoError(b, err)

	err = datastore.Write(ctx, store, []*openfgapb.TupleKey{}, tuples)
	require.NoError(b, err)

	checkQuery := commands.NewCheckQuery(datastore, tracer, meter, logger, defaultResolveNodeLimit)

	var r *openfgapb.CheckResponse

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ = checkQuery.Execute(ctx, &openfgapb.CheckRequest{
			StoreId:              store,
			AuthorizationModelId: model.Id,
			TupleKey: &openfgapb.TupleKey{
				Object:   "repo:openfga/openfga",
				Relation: "reader",
				User:     "github|iaco@openfga",
			},
			Trace: true,
		})
	}

	result = r
}
