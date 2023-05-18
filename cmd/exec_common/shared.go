package exec_common

import (
	"fmt"
	"strings"
)

// DatastoreEngine type to define different engine types
type DatastoreEngine string

// Types of Datastore Engines
const (
	Memory   DatastoreEngine = "memory"
	Postgres DatastoreEngine = "postgres"
	MySQL    DatastoreEngine = "mysql"
)

func (e DatastoreEngine) String() string {
	return string(e)
}

func NewDatastoreEngine(engine string) (*DatastoreEngine, error) {
	var isValid bool
	var validEngine DatastoreEngine
	for _, engineType := range []DatastoreEngine{Memory, Postgres, MySQL} {
		if engineType.String() == engine {
			isValid = true
			validEngine = engineType
		}
	}
	if !isValid {
		return nil, fmt.Errorf("invalid datastore engine '(%s)'", engine)
	}
	return &validEngine, nil
}

func VerifyDatastoreEngine(uri string, engine DatastoreEngine) error {
	var URIType string
	URITokens := strings.Split(uri, ":")
	if len(URITokens) > 1 {
		URIType = URITokens[0]
		// For now adding check for 'postgres'
		if URIType == "postgresql" || URIType == "postgres" {
			if engine != Postgres {
				return fmt.Errorf("config 'Engine' must be (%s)", Postgres.String())
			}
		}
	}
	return nil
}
