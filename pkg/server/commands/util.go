package commands

import "github.com/openfga/openfga/pkg/typesystem"

// IsAuthorizationModelObsolete returns whether the model schema is allowed or it is obsolete
func IsAuthorizationModelObsolete(schemaVersion string, allowSchema10 bool) bool {
	return !allowSchema10 && schemaVersion == typesystem.SchemaVersion1_0
}
