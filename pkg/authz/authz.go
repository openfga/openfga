package authz

import (
	"context"
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
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
		return nil, fmt.Errorf("config is invalid")
	}

	return &Authorizer{
		config: config,
		server: server,
		logger: logger,
	}, nil
}

func (a *Authorizer) getRelation(apiMethod string) (string, error) {
	switch apiMethod {
	case "ReadAuthorizationModel", "ReadAuthorizationModels":
		return "can_call_read_authorization_models", nil
	case "Read":
		return "can_call_read", nil
	case "Write":
		return "can_call_write", nil
	case "ListObjects", "StreamedListObjects":
		return "can_call_list_objects", nil
	case "Check":
		return "can_call_check", nil
	case "ListUsers":
		return "can_call_list_users", nil
	case "WriteAssertions":
		return "can_call_write_assertions", nil
	case "ReadAssertions":
		return "can_call_read_assertions", nil
	case "WriteAuthorizationModel":
		return "can_call_write_authorization_models", nil
	// case "ListStores":
	// 	return "can_call_list_stores", nil
	// case "CreateStore":
	// 	return "can_call_create_store", nil
	// case "GetStore":
	// 	return "can_call_get_store", nil
	// case "DeleteStore":
	// 	return "can_call_delete", nil
	case "Expand":
		return "can_call_expand", nil
	case "ReadChanges":
		return "can_call_read_changes", nil
	default:
		return "", fmt.Errorf("unknown api method: %s", apiMethod)
	}
}

// Authorize checks if the user has access to the resource.
func (a *Authorizer) Authorize(ctx context.Context, clientID, storeID, apiMethod string) (bool, error) {
	relation, err := a.getRelation(apiMethod)
	if err != nil {
		return false, err
	}

	req := &openfgav1.CheckRequest{
		StoreId:              a.config.StoreID,
		AuthorizationModelId: a.config.ModelID,
		TupleKey: &openfgav1.CheckRequestTupleKey{
			User:     fmt.Sprintf(`application:%s`, clientID),
			Relation: relation,
			Object:   fmt.Sprintf(`store:%s`, storeID),
		},
	}

	resp, err := a.server.CheckWithoutAuthz(ctx, req)
	if err != nil {
		return false, err
	}

	if !resp.Allowed {
		return false, nil
	}

	return true, nil
}
