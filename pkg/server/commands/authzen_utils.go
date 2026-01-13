package commands

import (
	"google.golang.org/protobuf/types/known/structpb"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
)

// SubjectPropertiesProvider is an interface for types that provide subject properties.
// Both Subject and SubjectFilter implement this interface.
type SubjectPropertiesProvider interface {
	GetProperties() *structpb.Struct
}

// MergePropertiesToContext merges subject, resource, and action properties into
// the context struct. Properties are namespaced with their source prefix using
// underscore as separator (e.g., "subject_department") because OpenFGA does not
// allow condition parameters with "." in their names.
// Precedence (lowest to highest): subject.properties, resource.properties,
// action.properties, request context (request context wins on conflicts).
func MergePropertiesToContext(
	requestContext *structpb.Struct,
	subject SubjectPropertiesProvider,
	resource *authzenv1.Resource,
	action *authzenv1.Action,
) (*structpb.Struct, error) {
	merged := make(map[string]interface{})

	// Add subject properties with "subject_" prefix
	if subject != nil && subject.GetProperties() != nil {
		for k, v := range subject.GetProperties().AsMap() {
			merged["subject_"+k] = v
		}
	}

	// Add resource properties with "resource_" prefix
	if resource != nil && resource.GetProperties() != nil {
		for k, v := range resource.GetProperties().AsMap() {
			merged["resource_"+k] = v
		}
	}

	// Add action properties with "action_" prefix
	if action != nil && action.GetProperties() != nil {
		for k, v := range action.GetProperties().AsMap() {
			merged["action_"+k] = v
		}
	}

	// Request context takes precedence (overrides any conflicting property keys)
	if requestContext != nil {
		for k, v := range requestContext.AsMap() {
			merged[k] = v
		}
	}

	if len(merged) == 0 {
		return nil, nil
	}

	return structpb.NewStruct(merged)
}
