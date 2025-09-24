package featureflags

type Client interface {
	Boolean(flagName string, defaultValue bool, featureCtx map[string]any) bool
}

type defaultClient struct {
	flags map[string]any
}

// NewDefaultClient creates a default feature flag client which takes in a static list of enabled feature flag names
// and stores them as keys in a map.
func NewDefaultClient(flags []string) Client {
	enabledFlags := make(map[string]any)
	for _, flag := range flags {
		enabledFlags[flag] = struct{}{}
	}
	return &defaultClient{
		flags: enabledFlags,
	}
}

func (c *defaultClient) Boolean(flagName string, defaultValue bool, featureCtx map[string]any) bool {
	_, ok := c.flags[flagName]
	return ok
}
