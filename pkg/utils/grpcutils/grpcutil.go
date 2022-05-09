package grpcutils

import (
	"context"

	"github.com/openfga/openfga/pkg/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func SetHeaderLogError(ctx context.Context, key, value string, log logger.Logger) {
	if err := grpc.SetHeader(ctx, metadata.Pairs(key, value)); err != nil {
		log.ErrorWithContext(
			ctx,
			"failed to set grpc header",
			logger.Error(err),
			logger.String("header", key),
		)
	}
}

func GetStatusFromError(err error) string {
	if err == nil {
		return codes.OK.String()
	}
	return status.Convert(err).Proto().String()
}
