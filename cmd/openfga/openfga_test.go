package main

import (
	"testing"

	"github.com/openfga/openfga/pkg/logger"
)

func TestBuildServerWithDefaultConfiguration(t *testing.T) {
	noopLogger := logger.NewNoopLogger()

	datastore, server := BuildServerAndDatastore(noopLogger)
	if datastore == nil {
		t.Fatal("Failed to build datastore")
	}
	if server == nil {
		t.Fatal("Failed to build server")
	}
}
