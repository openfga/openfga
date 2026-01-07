package http

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/server/errors"
)

func TestCustomHTTPErrorHandler(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/upper?word=abc", nil)
	w := httptest.NewRecorder()
	e := errors.NewEncodedError(int32(openfgav1.ErrorCode_assertions_too_many_items), "some error")
	metaData := runtime.ServerMetadata{
		HeaderMD: metadata.New(map[string]string{
			"foo": "boo",
		}),
		TrailerMD: metadata.New(map[string]string{}),
	}
	ctx := runtime.NewServerMetadataContext(context.Background(), metaData)
	CustomHTTPErrorHandler(ctx, w, req, e)
	res := w.Result()
	defer res.Body.Close()
	require.Equal(t, http.StatusBadRequest, res.StatusCode)

	header := res.Header.Get("Foo")
	require.Equal(t, "boo", header)

	contentType := res.Header.Get("Content-Type")
	require.Equal(t, "application/json", contentType)

	data, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	expectedData := "{\"code\":\"assertions_too_many_items\",\"message\":\"some error\"}"
	require.Equal(t, expectedData, strings.TrimSpace(string(data)))
}

func TestCustomHTTPErrorHandlerSpecialEncoding(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/upper?word=abc", nil)
	w := httptest.NewRecorder()
	e := errors.NewEncodedError(int32(openfgav1.ErrorCode_assertions_too_many_items), "invalid character '<' looking for beginning of value,")
	metaData := runtime.ServerMetadata{
		HeaderMD: metadata.New(map[string]string{
			"foo": "boo",
		}),
		TrailerMD: metadata.New(map[string]string{}),
	}
	ctx := runtime.NewServerMetadataContext(context.Background(), metaData)
	CustomHTTPErrorHandler(ctx, w, req, e)
	res := w.Result()
	defer res.Body.Close()
	require.Equal(t, http.StatusBadRequest, res.StatusCode)

	header := res.Header.Get("Foo")
	require.Equal(t, "boo", header)

	contentType := res.Header.Get("Content-Type")
	require.Equal(t, "application/json", contentType)

	data, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	expectedData := "{\"code\":\"assertions_too_many_items\",\"message\":\"invalid character '<' looking for beginning of value,\"}"
	require.Equal(t, expectedData, strings.TrimSpace(string(data)))
}
