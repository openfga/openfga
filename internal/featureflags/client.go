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
	return ok || defaultValue
}

type hardcodedBooleanClient struct {
	result bool // this client will always return this result
}

// NewHardcodedBooleanClient creates a hardcodedBooleanClient which always returns the value of `response` it's given.
// The hardcodedBooleanClient is used in testing and in shadow code paths where we want to force enable/disable a feature.
func NewHardcodedBooleanClient(result bool) Client {
	return &hardcodedBooleanClient{result: result}
}

func (h *hardcodedBooleanClient) Boolean(flagName string, defaultValue bool, featureCtx map[string]any) bool {
	return h.result
}
