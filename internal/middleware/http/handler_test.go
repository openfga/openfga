package http

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/openfga/openfga/server/errors"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc/metadata"
)

func TestCustomHTTPErrorHandler(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/upper?word=abc", nil)
	w := httptest.NewRecorder()
	e := errors.NewEncodedError(int32(openfgav1pb.ErrorCode_assertions_too_many_items), "some error")
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
	header := res.Header.Get("Foo")
	if header != "boo" {
		t.Errorf("Expect header encoded to boo but actual %s", header)
	}
	contentType := res.Header.Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Expect content type application/json, actual %s", contentType)
	}
	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		t.Errorf("Expect error to be nil but actual %v", err)
	}
	expectedData := "{\"code\":\"assertions_too_many_items\",\"message\":\"some error\"}"
	if strings.Compare(strings.TrimSpace(string(data)), expectedData) != 0 {
		t.Errorf("Expect data %s actual %s", expectedData, string(data))
	}
	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("Expect http code %d actual %d", http.StatusBadRequest, res.StatusCode)
	}
}

func TestCustomHTTPErrorHandlerSpeicalEncoding(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/upper?word=abc", nil)
	w := httptest.NewRecorder()
	e := errors.NewEncodedError(int32(openfgav1pb.ErrorCode_assertions_too_many_items), "invalid character '<' looking for beginning of value,")
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
	header := res.Header.Get("Foo")
	if header != "boo" {
		t.Errorf("Expect header encoded to boo but actual %s", header)
	}
	contentType := res.Header.Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Expect content type application/json, actual %s", contentType)
	}
	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		t.Errorf("Expect error to be nil but actual %v", err)
	}
	expectedData := "{\"code\":\"assertions_too_many_items\",\"message\":\"invalid character '<' looking for beginning of value,\"}"
	if strings.Compare(strings.TrimSpace(string(data)), expectedData) != 0 {
		t.Errorf("Expect data %s actual %s", strings.TrimSpace(expectedData), strings.TrimSpace(string(data)))
	}
	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("Expect http code %d actual %d", http.StatusBadRequest, res.StatusCode)
	}
}
