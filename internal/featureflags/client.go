package featureflags

type Client interface {
	Boolean(flagName string, defaultValue bool, featureCtx map[string]any) bool
}

type defaultClient struct {
	flags map[string]any
}

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
