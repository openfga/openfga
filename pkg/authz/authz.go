package authz

import (
	"fmt"

	"github.com/openfga/openfga/pkg/logger"
)

type Config struct {
	StoreID string
	ModelID string
}

type Authorizer struct {
	config *Config
	server ServerInterface
	logger logger.Logger
}

// NewAuthorizer creates a new authorizer.
func NewAuthorizer(config *Config, server ServerInterface, logger logger.Logger) (*Authorizer, error) {
	if config == nil || config.StoreID == "" || config.ModelID == "" {
		return nil, fmt.Errorf("'StoreID' and 'ModelID' configs must be set")
	}

	return &Authorizer{
		config: config,
		server: server,
		logger: logger,
	}, nil
}
