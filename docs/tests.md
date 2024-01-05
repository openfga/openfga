## Location of tests

Take the Write API. It has:

1. Unit tests in pkg/server/commands/write_test.go. These mock all the external dependencies. The tests here should test all business logic that is independent of the external dependencies.
2. **Exported** integration tests in pkg/server/test/write.go. These take in a `datastore` dependency. This is so that anyone that writes their own implementation of the datastore can run these tests by doing
    ```go
    import openfgatest "github.com/openfga/openfga/pkg/server/test"
    
    openfgatest.RunAllTests(t, datastore)
    ```
   
Now take the Check API. It has:

1. Unit tests in internal/graph/check_test.go. These mock all the external dependencies.
2. **Exported** integration tests in tests/check/check.go. These take in a `client` dependency. This is so that anyone that writes their own version of the server can run these tests by doing
    ```go
    import "github.com/openfga/openfga/tests/check"
    
    check.RunAllTests(t, client)
    ```

    The reason these tests live here is because 
      - the test cases are defined using easy-to-write YAML files. This enables us to write test cases that apply to both Check and ListObjects APIs. (If one Check returns `allowed=true`, the corresponding ListObjects call will, depending on server parameters and other factors, also include it in the response.)
      - since these tests are exported, it allows authors of datastores to verify that their implementations of some methods related to Read of Tuples are correct.

3. Integration tests in cmd/openfga/main_test.go. These are basically tests for the validations written in the proto files living in https://github.com/openfga/api.