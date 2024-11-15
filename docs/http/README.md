# Quick and Easy API Testing
We have provided a simple `.http` file to help quickly test the OpenFGA API.

## Prerequisites
1. **OpenFGA API**: Ensure the OpenFGA API is running locally. We will assume the API is running on http://localhost:8080.
1. **VS Code**: Use an editor that supports `.http` files for executing HTTP requests.
2. **REST Client Extension** (for VS Code): Install the [REST Client](https://marketplace.visualstudio.com/items?itemName=humao.rest-client) extension to send HTTP requests and view the response in Visual Studio Code directly.

## Using the .http file
1. Open `openfga.http` in VS Code (or your preferred IDE that supports .http files).
1. Click on the `Send Request` button above each request section to execute it.
1. View the response for each request.

### Variables
The .http file has three variables. `@fga_api_url` has a default, but `@fga_store_id` and `@fga_model_id` are intentionally blank. When a store is created or a model is updated, the variables are automatically updated based on the id's in the response. 

```
### Create Store
# @name store
POST {{fga_api_url}}/stores
Content-Type: application/json

{ "name": "openfga-demo" }

### Save API token as a request variable
@fga_store_id = {{store.response.body.id}}
```

You can override the variables by replacing them at the top of the file if you want.

---

For more details on each API endpoint, refer to the [OpenFGA API Documentation](https://openfga.dev/docs/fga).

For more details about using `.http` files or their syntax, refer to the [Rest Client for VS Code](https://marketplace.visualstudio.com/items?itemName=humao.rest-client) documentation. 