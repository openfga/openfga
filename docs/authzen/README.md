# OpenFGA AuthZEN Implementation and Interop scenarios

## AuthZEN Implementation

This branch includes an experimental implementation of the [AuthZEN Authorization API 1.1 – draft 02](https://openid.net/specs/authorization-api-1_0-02.html). 

It maps the `evaluation` and `evaluations` endpoints to the OpenFGA `check` and `batch-check` endpoints.

The AuthZEN [`evaluation`](https://openid.net/specs/authorization-api-1_0-02.html#name-access-evaluation-api) endpoint implementation maps to an [OpenFGA `check`](https://openfga.dev/api/service#/Relationship%20Queries/Check) call:

### AuthZEN Evaluation

POST /stores/<store_id>/evaluation
```json
{
  "subject": {
    "type": "user",
    "id": "<user_id>"
  },
  "resource": {
    "type": "document",
    "id": "<document_id>"
  },
  "action": {
    "name": "can_read",
  },
  "context": {
    "current_time": "1985-10-26T01:22-07:00"
  }
}
```
### OpenFGA Check

POST /stores/<store_id>/check

```json
{
  "tuple_key": {
    "user": "user:<user_id>",
    "relation": "can_read",
    "object": "document:<document_id>",
    "context": {
      "current_time":"1985-10-26T01:22-07:00"
    }  
  }
}
```

The AuthZEN [`evaluations`](https://openid.net/specs/authorization-api-1_0-02.html#name-access-evaluations-api) endpoint implementation maps to an [OpenFGA `batch-check`](https://openfga.dev/api/service#/Relationship%20Queries/BatchCheck) call:

### AuthZEN Evaluations
POST /stores/<store_id>/evaluations

```json
{
  "subject": {
    "type": "user",
    "id": "<user_id>"
  },
  "context":{
    "time": "2024-05-31T15:22-07:00"
  },
  "evaluations": [
    {
      "action": {
        "name": "can_read"
      },
      "resource": {
        "type": "document",
        "id": "<document_id>"
      }
    },
    {
      "action": {
        "name": "can_edit"
      },
      "resource": {
        "type": "document",
        "id": "<document_id>"
      }
    }
  ]
}
```

### OpenFGA Batch-Check
POST /stores/<store_id>/batch-check

```json
{
  "checks": [
     {
       "tuple_key": {
         "object": "document:<document_id>"
         "relation": "can_edit",
         "user": "user:<user_id>",
       },
        "context":{
            "time": "2024-05-31T15:22-07:00"
        },
       "correlation_id": "1"
     },
    {
       "tuple_key": {
         "object": "document:<document_id>"
         "relation": "can_read",
         "user": "user:<user_id>",
       },
        "context":{
            "time": "2024-05-31T15:22-07:00"
        },
       "correlation_id": "2"
     }
   ]
}
```

## AuthZEN Interop Scenarios

The [AuthZEN working group](https://openid.net/wg/authzen/) has defined two interoperability scenarios:

- [Todo App Interop Scenario](https://authzen-interop.net/docs/scenarios/todo-1.1/)
- [API Gateway Interop Scenario](https://authzen-interop.net/docs/category/api-gateway-10-draft-02)

These scenarios require their own OpenFGA store, with their model and tuples:

- [OpenFGA Todo App Interop Model](./authzen-todo.fga.yaml)
- [OpenFGA Gateway Interop Model](./authzen-gateway.fga.yaml)

The model files have inline documentation explaining the rationale for the design.

To run tests using the [FGA CLI](https://github.com/openfga/cli), use:

```bash
fga model test --test authzen-todo.fga.yaml
fga model test --test authzen-gateway.fga.yaml
```

There can also use [`authzen-todo.http`](./authzen-todo.http) and [`authzen-gateway.http`](./authzen-gateway.http) using [REST Client](https://marketplace.visualstudio.com/items?itemName=humao.rest-client). 


You can also try it live on the [AuthZen interop website](https://todo.authzen-interop.net/). Use the user credentials specified [here](https://github.com/openid/authzen/blob/main/interop/authzen-todo-application/README.md#identities).

## Running the AuthZEN Todo Interop test suite

- Clone https://github.com/openid/authzen
- Go to the `interop/authzen-todo-backend` folder and follow the instructions in the readme to build the project.
- Set the shared key as an environment variable. You can find it on the OpenFGA vault under the "OpenFGA AuthZeN shared key" name.

```
export AUTHZEN_PDP_API_KEY="<shared-key>"
```

- Run the tests:
```
yarn test https://authzen-interop.openfga.dev/stores/01JG9JGS4W0950VN17G8NNAH3C 
```

## Running the AuthZEN API Gateway Interop test suite

- Clone https://github.com/openid/authzen
- Go to the `interop/authzen-api-gateway` folder and follow the instructions in the readme to build the project.
- Set the shared key as an environment variable. You can find it on the OpenFGA vault under the "OpenFGA AuthZeN shared key" name.

```
export AUTHZEN_PDP_API_KEY="<shared-key>"
```

- Run the tests:
```
yarn test https://authzen-interop.openfga.dev/stores/01JG9JGS4W0950VN17G8NNAH3C 
```
## Running the AuthZEN Todo Application

- Set the shared key as an environment variable. You can find it on the OpenFGA vault under the "OpenFGA AuthZeN shared key" name.

```
export AUTHZEN_PDP_API_KEY='{"OpenFGA": "Bearer <shared-key>"}'
```

- Change the `gateways.--Pass Through--` entry in [pdps.json](https://github.com/openid/authzen/blob/main/interop/authzen-todo-backend/src/pdps.json) to point to "http://localhost:8080".

- Change the `VITE_API_ORIGIN` in the [.env]`https://github.com/openid/authzen/blob/main/interop/authzen-todo-application/.env` file to `http://localhost:8080`

- Follow the instructions in this [readme](https://github.com/openid/authzen/tree/main/interop/authzen-todo-backend) to build and run the back-end app.

- Follow the instructions in this [readme](https://github.com/openid/authzen/blob/main/interop/authzen-todo-application/README.md) to run the front-end app.

## Pointing to a local OpenFGA instance

To run the test suites or the interop application pointing to a local OpenFGA instance, you need to:

- Change the `AUTHZEN_PDP_API_KEY` values to match the ones used by OpenFGA locally.
- Change the [pdps.json](https://github.com/openid/authzen/blob/main/interop/authzen-todo-backend/src/pdps.json) OpenFGA entries and point to the local OpenFGA instance.
- You can run the test suites by pointing to the local OpenFGA instance too:

```
yarn test http://localhost:8080/stores/01JG9JGS4W0950VN17G8NNAH3C 
```
- If you want the Todo App pointing to a local OpenFGA instance, you'll need to change the port that OpenFGA uses, as it conflicts with the one used by the interop backend app:

```
dist/openfga run --http-addr 0.0.0.0:4000        
```

## TODO

Next steps:

- Add a lot of unit tests for [evaluate](/pkg/server/commands/evaluate_test.go) and [evaluates](/pkg/server/commands/batch_evaluate_test.go). 
  - Verify if we return the right error codes.
- Consider mapping additional attributes that can be specified in AuthZEN calls to either `context` values or contextual tuples.
- Support [Evaluation Options](https://openid.net/specs/authorization-api-1_0-02.html#name-evaluations-options)
- Add experimental flag
