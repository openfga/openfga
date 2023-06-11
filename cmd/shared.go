package cmd

import (
	"fmt"
)

// DatastoreEngine type to define different engine types
type DatastoreEngine string

// Types of Datastore Engines
const (
	Memory   DatastoreEngine = "memory"
	Postgres DatastoreEngine = "postgres"
	MySQL    DatastoreEngine = "mysql"
)

// String converts DatastoreEngine e to string
func (e DatastoreEngine) String() string {
	return string(e)
}

// IsValid checks if DatastoreEngine e is valid
func (e DatastoreEngine) IsValid() bool {
	var isValid bool
	for _, engineType := range []DatastoreEngine{Memory, Postgres, MySQL} {
		if engineType.String() == e.String() {
			isValid = true
		}
	}
	return isValid
}

// NewDatastoreEngine inits a new valid DatastoreEngine type
func NewDatastoreEngine(engine string) (*DatastoreEngine, error) {
	dsEngine := DatastoreEngine(engine)
	if isValid := dsEngine.IsValid(); !isValid {
		return nil, fmt.Errorf("invalid datastore engine '(%s)'", engine)
	}
	return &dsEngine, nil
}
