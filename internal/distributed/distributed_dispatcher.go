package distributed

import (
	"bytes"
	"context"
	"fmt"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/dispatcher"
	"io"
	"log"
	"net/http"
)

type DistributedDispatcher struct {
	delegate dispatcher.Dispatcher
}

func (r *DistributedDispatcher) SetDelegate(delegate dispatcher.Dispatcher) {
	r.delegate = delegate
}

func (r *DistributedDispatcher) GetDelegate() dispatcher.Dispatcher {
	return r.delegate
}

var _ dispatcher.Dispatcher = (*DistributedDispatcher)(nil)

// Command to run multiple servers - go run . run --http-addr 0.0.0.0:8082 --metrics-addr 0.0.0.0:2113 --grpc-addr 0.0.0.0:8083 --playground-port 3001  --dispatch-throttling-enabled=true --datastore-engine=postgres --datastore-uri='postgres://postgres:password@localhost:5432/postgres?sslmode=disable'
func (d *DistributedDispatcher) Dispatch(ctx context.Context, request *openfgav1.BaseRequest, metadata *openfgav1.DispatchMetadata, additionalParameters any) (*openfgav1.BaseResponse, *openfgav1.DispatchMetadata, error) {
	resp, err := http.Post("http://localhost:8082/stores/dispatch-check", "application/json", bytes.NewBuffer([]byte(` {   "tuple_key": {     "user": "user:1",     "relation": "viewer",     "object": "folder:1"   } }`)))
	log.Println(resp)
	//check if response is there or send to dispatch
	if err != nil {
		log.Println(err)
	}
	if resp != nil {
		data, _ := io.ReadAll(resp.Body)
		log.Println(fmt.Sprintf("Response from Dispatched Request printed below: %s", string(data)))
	}
	return d.delegate.Dispatch(ctx, request, metadata, additionalParameters)
}

func (d *DistributedDispatcher) Close() {
}
