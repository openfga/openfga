package telemetry

import (
	"strings"
	"testing"
)

func TestResolveOTLPSecurity(t *testing.T) {
	tests := []struct {
		name         string
		configSecure bool
		schemeSecure bool
		expected     bool
	}{
		{
			name:         "both_false",
			configSecure: false,
			schemeSecure: false,
			expected:     false,
		},
		{
			name:         "config_secure_only",
			configSecure: true,
			schemeSecure: false,
			expected:     true,
		},
		{
			name:         "scheme_secure_only",
			configSecure: false,
			schemeSecure: true,
			expected:     true,
		},
		{
			name:         "both_secure",
			configSecure: true,
			schemeSecure: true,
			expected:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ResolveOTLPSecurity(tt.configSecure, tt.schemeSecure)
			if got != tt.expected {
				t.Errorf("ResolveOTLPSecurity(%v, %v) = %v, want %v", tt.configSecure, tt.schemeSecure, got, tt.expected)
			}
		})
	}
}

func TestParseOTLPEndpoint(t *testing.T) {
	tests := []struct {
		name             string
		input            string
		expectedEndpoint string
		expectedSecure   bool
	}{
		{
			name:             "bare_host_port",
			input:            "collector.example.com:4317",
			expectedEndpoint: "collector.example.com:4317",
			expectedSecure:   false,
		},
		{
			name:             "http_scheme",
			input:            "http://collector.example.com:4317",
			expectedEndpoint: "collector.example.com:4317",
			expectedSecure:   false,
		},
		{
			name:             "https_scheme",
			input:            "https://collector.example.com:4317",
			expectedEndpoint: "collector.example.com:4317",
			expectedSecure:   true,
		},
		{
			name:             "http_k8s_service_dns",
			input:            "http://k8se-otel.k8se-apps.svc.cluster.local:4317",
			expectedEndpoint: "k8se-otel.k8se-apps.svc.cluster.local:4317",
			expectedSecure:   false,
		},
		{
			name:             "bare_ipv4_unspecified",
			input:            "0.0.0.0:4317",
			expectedEndpoint: "0.0.0.0:4317",
			expectedSecure:   false,
		},
		{
			name:             "http_localhost",
			input:            "http://localhost:4317",
			expectedEndpoint: "localhost:4317",
			expectedSecure:   false,
		},
		{
			name:             "https_no_port",
			input:            "https://collector.example.com",
			expectedEndpoint: "collector.example.com",
			expectedSecure:   true,
		},
		{
			name:             "http_no_port",
			input:            "http://collector.example.com",
			expectedEndpoint: "collector.example.com",
			expectedSecure:   false,
		},
		{
			name:             "http_with_path",
			input:            "http://collector.example.com:4317/v1/traces",
			expectedEndpoint: "collector.example.com:4317",
			expectedSecure:   false,
		},
		{
			name:             "https_with_path",
			input:            "https://collector.example.com:4317/v1/traces",
			expectedEndpoint: "collector.example.com:4317",
			expectedSecure:   true,
		},
		{
			name:             "http_ipv6",
			input:            "http://[::1]:4317",
			expectedEndpoint: "[::1]:4317",
			expectedSecure:   false,
		},
		{
			name:             "https_ipv6",
			input:            "https://[::1]:4317",
			expectedEndpoint: "[::1]:4317",
			expectedSecure:   true,
		},
		{
			name:             "empty_string",
			input:            "",
			expectedEndpoint: "",
			expectedSecure:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			endpoint, secure := ParseOTLPEndpoint(tt.input)
			if endpoint != tt.expectedEndpoint {
				t.Errorf("ParseOTLPEndpoint(%q) endpoint = %q, want %q", tt.input, endpoint, tt.expectedEndpoint)
			}
			if secure != tt.expectedSecure {
				t.Errorf("ParseOTLPEndpoint(%q) secure = %v, want %v", tt.input, secure, tt.expectedSecure)
			}
		})
	}
}

func TestResolveSampler(t *testing.T) {
	tests := []struct {
		name            string
		samplerName     string
		ratio           float64
		descriptionMust []string // substrings that must appear in Description()
	}{
		{
			name:            "always_on",
			samplerName:     "always_on",
			ratio:           0.5,
			descriptionMust: []string{"AlwaysOnSampler"},
		},
		{
			name:            "always_off",
			samplerName:     "always_off",
			ratio:           0.5,
			descriptionMust: []string{"AlwaysOffSampler"},
		},
		{
			name:            "traceidratio",
			samplerName:     "traceidratio",
			ratio:           0.5,
			descriptionMust: []string{"TraceIDRatioBased", "0.5"},
		},
		{
			name:            "parentbased_always_on",
			samplerName:     "parentbased_always_on",
			ratio:           0.5,
			descriptionMust: []string{"ParentBased", "AlwaysOnSampler"},
		},
		{
			name:            "parentbased_always_off",
			samplerName:     "parentbased_always_off",
			ratio:           0.5,
			descriptionMust: []string{"ParentBased", "AlwaysOffSampler"},
		},
		{
			name:            "parentbased_traceidratio",
			samplerName:     "parentbased_traceidratio",
			ratio:           0.3,
			descriptionMust: []string{"ParentBased", "TraceIDRatioBased", "0.3"},
		},
		{
			name:            "unknown_falls_back_to_traceidratio",
			samplerName:     "unknown",
			ratio:           0.5,
			descriptionMust: []string{"TraceIDRatioBased", "0.5"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sampler := ResolveSampler(tt.samplerName, tt.ratio)
			desc := sampler.Description()
			for _, must := range tt.descriptionMust {
				if !strings.Contains(desc, must) {
					t.Errorf("ResolveSampler(%q, %v).Description() = %q, want it to contain %q", tt.samplerName, tt.ratio, desc, must)
				}
			}
		})
	}
}
