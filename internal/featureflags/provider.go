package featureflags

import (
	"context"
	"fmt"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/openfga/openfga/pkg/server"
)

type Provider struct {
	//svc string // TODO: this will be our feature flags thign
	flags map[string]struct{}
}

type ProviderOption func(*Provider)

func NewDefaultProvider(flags []server.ExperimentalFeatureFlag, opts ...ProviderOption) *Provider {
	// take in the experimental flags by default, and add them to map
	enabledFlags := make(map[string]struct{})
	for _, flag := range flags {
		enabledFlags[string(flag)] = struct{}{}
	}
	fmt.Printf("<--------------------------> default features enabled: %v\n", enabledFlags)
	return &Provider{
		flags: enabledFlags,
	}
}

func (p *Provider) Metadata() openfeature.Metadata {
	return openfeature.Metadata{
		Name: "Default OpenFGA feature provider",
	}
}

func (p *Provider) Hooks() []openfeature.Hook {
	return []openfeature.Hook{}
}

func (p *Provider) BooleanEvaluation(ctx context.Context, flag string, defaultValue bool, flatCtx openfeature.FlattenedContext) openfeature.BoolResolutionDetail {
	_, ok := p.flags[flag]
	return openfeature.BoolResolutionDetail{
		Value:                    ok,
		ProviderResolutionDetail: openfeature.ProviderResolutionDetail{},
	}
}

func (p *Provider) StringEvaluation(ctx context.Context, flag string, defaultValue string, flatCtx openfeature.FlattenedContext) openfeature.StringResolutionDetail {
	return openfeature.StringResolutionDetail{
		Value:                    defaultValue,
		ProviderResolutionDetail: openfeature.ProviderResolutionDetail{},
	}
}

func (p *Provider) FloatEvaluation(ctx context.Context, flag string, defaultValue float64, flatCtx openfeature.FlattenedContext) openfeature.FloatResolutionDetail {
	return openfeature.FloatResolutionDetail{
		Value:                    defaultValue,
		ProviderResolutionDetail: openfeature.ProviderResolutionDetail{},
	}
}

func (p *Provider) IntEvaluation(ctx context.Context, flag string, defaultValue int64, flatCtx openfeature.FlattenedContext) openfeature.IntResolutionDetail {
	return openfeature.IntResolutionDetail{
		Value:                    defaultValue,
		ProviderResolutionDetail: openfeature.ProviderResolutionDetail{},
	}
}

func (p *Provider) ObjectEvaluation(ctx context.Context, flag string, defaultValue any, flatCtx openfeature.FlattenedContext) openfeature.InterfaceResolutionDetail {
	return openfeature.InterfaceResolutionDetail{
		Value:                    defaultValue,
		ProviderResolutionDetail: openfeature.ProviderResolutionDetail{},
	}
}
