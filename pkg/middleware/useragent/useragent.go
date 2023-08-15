package useragent

import (
	"context"

	"google.golang.org/grpc/metadata"
)

const (
	userAgentHeader string = "grpcgateway-user-agent"
)

// FromContext returns the user agent field stored in context.
// If context does not have user agent field, function will return empty string and false.
func FromContext(ctx context.Context) (string, bool) {
	if headers, ok := metadata.FromIncomingContext(ctx); ok {
		if header := headers.Get(userAgentHeader); len(header) > 0 {
			return header[0], true
		}
	}
	return "", false
}
