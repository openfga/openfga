package telemetry

import (
	"testing"
)

func boolPtr(b bool) *bool { return &b }

func TestResolveOTLPInsecure(t *testing.T) {
	tests := []struct {
		name           string
		configInsecure bool
		schemeInsecure *bool
		expected       bool
	}{
		{
			name:           "no_scheme_config_insecure",
			configInsecure: true,
			schemeInsecure: nil,
			expected:       true,
		},
		{
			name:           "no_scheme_config_secure",
			configInsecure: false,
			schemeInsecure: nil,
			expected:       false,
		},
		{
			name:           "http_overrides_secure_config",
			configInsecure: false,
			schemeInsecure: boolPtr(true),
			expected:       true,
		},
		{
			name:           "https_overrides_insecure_config",
			configInsecure: true,
			schemeInsecure: boolPtr(false),
			expected:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ResolveOTLPInsecure(tt.configInsecure, tt.schemeInsecure)
			if got != tt.expected {
				t.Errorf("ResolveOTLPInsecure(%v, %v) = %v, want %v", tt.configInsecure, tt.schemeInsecure, got, tt.expected)
			}
		})
	}
}

func TestParseOTLPEndpoint(t *testing.T) {
	tests := []struct {
		name             string
		input            string
		expectedEndpoint string
		expectedInsecure *bool
	}{
		{
			name:             "bare_host_port",
			input:            "collector.example.com:4317",
			expectedEndpoint: "collector.example.com:4317",
			expectedInsecure: nil,
		},
		{
			name:             "http_scheme",
			input:            "http://collector.example.com:4317",
			expectedEndpoint: "collector.example.com:4317",
			expectedInsecure: boolPtr(true),
		},
		{
			name:             "https_scheme",
			input:            "https://collector.example.com:4317",
			expectedEndpoint: "collector.example.com:4317",
			expectedInsecure: boolPtr(false),
		},
		{
			name:             "http_k8s_service_dns",
			input:            "http://k8se-otel.k8se-apps.svc.cluster.local:4317",
			expectedEndpoint: "k8se-otel.k8se-apps.svc.cluster.local:4317",
			expectedInsecure: boolPtr(true),
		},
		{
			name:             "bare_ipv4_unspecified",
			input:            "0.0.0.0:4317",
			expectedEndpoint: "0.0.0.0:4317",
			expectedInsecure: nil,
		},
		{
			name:             "http_localhost",
			input:            "http://localhost:4317",
			expectedEndpoint: "localhost:4317",
			expectedInsecure: boolPtr(true),
		},
		{
			name:             "https_no_port",
			input:            "https://collector.example.com",
			expectedEndpoint: "collector.example.com",
			expectedInsecure: boolPtr(false),
		},
		{
			name:             "http_no_port",
			input:            "http://collector.example.com",
			expectedEndpoint: "collector.example.com",
			expectedInsecure: boolPtr(true),
		},
		{
			name:             "http_with_path",
			input:            "http://collector.example.com:4317/v1/traces",
			expectedEndpoint: "collector.example.com:4317",
			expectedInsecure: boolPtr(true),
		},
		{
			name:             "https_with_path",
			input:            "https://collector.example.com:4317/v1/traces",
			expectedEndpoint: "collector.example.com:4317",
			expectedInsecure: boolPtr(false),
		},
		{
			name:             "http_ipv6",
			input:            "http://[::1]:4317",
			expectedEndpoint: "[::1]:4317",
			expectedInsecure: boolPtr(true),
		},
		{
			name:             "https_ipv6",
			input:            "https://[::1]:4317",
			expectedEndpoint: "[::1]:4317",
			expectedInsecure: boolPtr(false),
		},
		{
			name:             "empty_string",
			input:            "",
			expectedEndpoint: "",
			expectedInsecure: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			endpoint, insecure := ParseOTLPEndpoint(tt.input)
			if endpoint != tt.expectedEndpoint {
				t.Errorf("ParseOTLPEndpoint(%q) endpoint = %q, want %q", tt.input, endpoint, tt.expectedEndpoint)
			}
			if (insecure == nil) != (tt.expectedInsecure == nil) {
				t.Errorf("ParseOTLPEndpoint(%q) insecure = %v, want %v", tt.input, insecure, tt.expectedInsecure)
			} else if insecure != nil && *insecure != *tt.expectedInsecure {
				t.Errorf("ParseOTLPEndpoint(%q) insecure = %v, want %v", tt.input, *insecure, *tt.expectedInsecure)
			}
		})
	}
}
