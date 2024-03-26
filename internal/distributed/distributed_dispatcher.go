package distributed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/dispatcher"
	serverconfig "github.com/openfga/openfga/internal/server/config"
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
	log.Printf("Distributed Dispatcher - %s running in %s", request.GetDispatchedCheckRequest(), serverconfig.ServerName)
	switch (request.GetBaseRequest()).(type) {
	case *openfgav1.BaseRequest_DispatchedCheckRequest:
		{
			user := request.GetDispatchedCheckRequest().TupleKey.User
			relation := request.GetDispatchedCheckRequest().TupleKey.Relation
			object := request.GetDispatchedCheckRequest().TupleKey.Object
			if relation == "owner" && serverconfig.ServerName == "server-1" {
				log.Println(fmt.Sprintf("Distributed Dispatch for %s#%s@%s", user, relation, object))
				resp, err := http.Post("http://localhost:8082/stores/dispatch-check", "application/json", bytes.NewBuffer([]byte(fmt.Sprintf(`{"dispatchedCheckRequest":{"storeId":"01HSHHEYFKMR0B80HK18YNK055", "authorizationModelId":"none","tupleKey":{"user":"%s","relation":"%s","object":"%s","condition":{"name":"condition1","context":{}}}}}`, user, relation, object))))
				if err != nil {
					log.Println(err)
					break
				}
				if resp != nil {
					decoder := json.NewDecoder(resp.Body)
					parsed := &openfgav1.BaseResponse_CheckResponse{CheckResponse: &openfgav1.CheckResponse{}}
					err := decoder.Decode(parsed)
					if err != nil {
						return nil, nil, err
					}
					return &openfgav1.BaseResponse{BaseResponse: parsed}, metadata, nil
				}
			}
		}

	}
	return d.delegate.Dispatch(ctx, request, metadata, additionalParameters)
}

func (d *DistributedDispatcher) Close() {
}
