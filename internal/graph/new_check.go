package graph

import (
	"context"
	"errors"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/typesystem"
)

//Get a request
//Get the authorisation model
//Get the tuple
//Validate whether the model supports the relation
//Case 1 [user:1, user:2] relation [object:1, object:2], Since object-id will be the hash key for cache, we want to stick to single relation, so that peer dispatch can resolve in a single dispatch.
//Check for object 1 - user 1, user 2 : server-1
//Check for object 2 - user 1, user 2 : server-2
//We don't use relation here as relation rewrite might cause unwanted dispatch
//Case 2 [user:1] relation [object:1, object:2]
// Google Drive - Folder View - I want to get all the folders related to user:poovam - use batch check - Search with index

type Object struct {
	objectType string
	id         string
}

func (o Object) isSubject() {

}

func (o Object) GetType() string {
	return o.objectType
}

func (o Object) GetSubjectKind() SubjectKind {
	return ObjectKind
}

type SubjectKind int

const (
	ObjectKind     SubjectKind = iota + 1 // EnumIndex = 1
	SubjectSetKind                        // EnumIndex = 2
	WildcardKind
)

type SubjectSet struct {
	Object
	relation string
}

type WildcardSubject struct {
	objectType string
}

type Subject interface {
	GetType() string
	GetSubjectKind() SubjectKind
	isSubject()
}

type DispatchCheckRequest struct {
	objectType string
	objectIds  []string
	relation   string
	subjects   []Subject
}

type Result struct {
	subject Subject
	object  string
	allowed bool
}

type DispatchCheckResponse struct {
	Results []Result
}

//Base case - Direct relationships
/**
type user

type document
	relations
	define viewer: [user]

Req - [user:1] viewer [doc:1, doc:2]

Check- doc:1#viewer@user:1
Check- doc:2#viewer@user:1

*/
func ResolveCheck(ctx context.Context, request *DispatchCheckRequest) (*DispatchCheckResponse, error) {
	var typeSystem *typesystem.TypeSystem
	for _, objectId := range request.objectIds {
		relation, err := typeSystem.GetRelation(request.objectType, request.relation)
		if err != nil {
			return nil, errors.New("object doesn't have relation")
		}

		rewriteRule := relation.GetRewrite()
		handleRewrite(mapToRewriteRule(rewriteRule))
	}

}

func handleRewrite(rewrite RewriteRule) {
	switch rewrite {

	case DirectRewrite:

	}
}

type RewriteRule int

const (
	DirectRewrite RewriteRule = iota + 1 // EnumIndex = 1
)

func mapToRewriteRule(userset *openfgav1.Userset) RewriteRule {
	switch userset.GetUserset().(type) {
	case *openfgav1.Userset_This:
		return DirectRewrite
	default:
		panic("case not handled")
	}
}
