package featureflags

import (
	"context"
	"github.com/open-feature/go-sdk/openfeature"
)

type Client interface {
	Boolean(flagName string, defaultValue bool, featureCtx map[string]any) bool
}

type defaultClient struct {
	client *openfeature.Client
}

func NewDefaultClient(flags []string) Client {
	defaultProvider := NewDefaultProvider(flags)
	if err := openfeature.SetProviderAndWait(defaultProvider); err != nil {
		panic("failed to initialize feature flag service")
	}
	return &defaultClient{client: openfeature.NewClient("fga")}
}

func (c *defaultClient) Boolean(flagName string, defaultValue bool, featureCtx map[string]any) bool {
	evalCtx := openfeature.NewEvaluationContext("", featureCtx)
	return c.client.Boolean(context.Background(), flagName, defaultValue, evalCtx)
}
