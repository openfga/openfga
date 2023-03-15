package commands

import (
	"errors"

	"github.com/openfga/openfga/pkg/typesystem"
)

var (
	ErrObsoleteAuthorizationModel = errors.New("obsolete authorization model schema version")
)

// ProhibitModel1_0 returns whether the model should be prohibited due to being obsolete with schema 1.0
func ProhibitModel1_0(schema string, allowModel1_0 bool) bool {
	return schema == typesystem.SchemaVersion1_0 && !allowModel1_0
}
