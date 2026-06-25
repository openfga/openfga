package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/server/errors"
)

func TestHTTPResponseModifier(t *testing.T) {
	t.Run("no_server_metadata_is_noop", func(t *testing.T) {
		w := httptest.NewRecorder()
		err := HTTPResponseModifier(context.Background(), w, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("sets_status_from_x_http_code", func(t *testing.T) {
		w := httptest.NewRecorder()
		md := runtime.ServerMetadata{
			HeaderMD: metadata.New(map[string]string{XHttpCode: strconv.Itoa(http.StatusCreated)}),
		}
		ctx := runtime.NewServerMetadataContext(context.Background(), md)

		err := HTTPResponseModifier(ctx, w, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, w.Code)
		// The x-http-code header must be stripped from the response metadata.
		require.Empty(t, md.HeaderMD.Get(XHttpCode))
	})

	t.Run("invalid_x_http_code_errors", func(t *testing.T) {
		w := httptest.NewRecorder()
		md := runtime.ServerMetadata{
			HeaderMD: metadata.New(map[string]string{XHttpCode: "not-a-number"}),
		}
		ctx := runtime.NewServerMetadataContext(context.Background(), md)

		err := HTTPResponseModifier(ctx, w, nil)
		require.Error(t, err)
	})

	t.Run("no_x_http_code_leaves_status_default", func(t *testing.T) {
		w := httptest.NewRecorder()
		md := runtime.ServerMetadata{HeaderMD: metadata.New(map[string]string{"foo": "bar"})}
		ctx := runtime.NewServerMetadataContext(context.Background(), md)

		err := HTTPResponseModifier(ctx, w, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, w.Code)
	})
}

func TestCustomHTTPErrorHandler_ForwardsTrailers(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/check", nil)
	// Signal that the client accepts trailers so the trailer-forwarding path runs.
	req.Header.Set("TE", "trailers")

	w := httptest.NewRecorder()
	e := errors.NewEncodedError(int32(openfgav1.ErrorCode_validation_error), "boom")

	md := runtime.ServerMetadata{
		HeaderMD:  metadata.New(map[string]string{"foo": "bar"}),
		TrailerMD: metadata.New(map[string]string{"grpc-trailer": "value"}),
	}
	ctx := runtime.NewServerMetadataContext(context.Background(), md)

	CustomHTTPErrorHandler(ctx, w, req, e)
	res := w.Result()
	defer res.Body.Close()

	require.Equal(t, "chunked", res.Header.Get("Transfer-Encoding"))
	// A Trailer header announcing the forwarded trailer key must be present.
	require.NotEmpty(t, res.Header.Get("Trailer"))
}
