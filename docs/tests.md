## Location of tests

The purpose of this guide is to inform contributors where to write new tests.

## Storage implementations

1. Any change to the storage interface must have an integration test in pkg/storage/test/storage.go. This is so that anyone writing their own storage adapter can have confidence that it is correct.

## APIs

1. All APIs use an underlying `command`. These must have unit tests. For example, Write API is backed by Write Command, which has unit tests in pkg/server/commands/write_test.go. These should target mocked dependencies to cover the majority of the business logic. If you are changing business logic, add a test here.
2. Functional tests in tests/functional_test.go. These are tests for validating the API inputs. The inputs are defined in the proto files (https://github.com/openfga/api).
3. Some APIs' behavior can be fine-tuned with server flags. If this is the case, the tests for the correct wiring of the flags live in pkg/server/server_test.go.
4. Some APIs set specific response headers. If this is the case, update the test `TestHTTPHeaders` in cmd/run/run_test.go.

## Write APIs

Take the Write API. Aside from the tests mentioned in "All APIs", it has:

1. **Exported** integration tests in pkg/server/test/write.go. These take in a `datastore` dependency. The goal of these is to test the underlying datastore methods used. This is so that anyone that writes their own implementation of the datastore can run these tests by doing
    ```go
    import openfgatest "github.com/openfga/openfga/pkg/server/test"
    
    openfgatest.RunAllTests(t, datastore)
    ```
   
   If, for example, your change is about calling a **new** datastore method within the Write API, add a test here.
   
## Read APIs

Take the Check API. Aside from the tests mentioned in "All APIs", it has:

1. **Exported** integration tests in tests/check/check.go. These take in a `client` dependency. This is so that anyone that writes their own version of the server can run these tests by doing
    ```go
    import "github.com/openfga/openfga/tests/check"
    
    check.RunAllTests(t, client)
    ```

    The reason these tests live here is because:
      - the test cases are defined using easy-to-write YAML files. This enables us to write test cases that apply to all query APIs. (If one Check returns `allowed=true`, the corresponding ListObjects call will, depending on server parameters and other factors, also include it in the response.)
      - since these tests are exported, it allows authors of datastores to get extra confidence that their implementations of some methods related to Read of Tuples are correct.

2. Test that assert on the log fields that are emmitted: `TestCheckLogs` in tests/check/check_test.go.

## Docker integration

OpenFGA can be run within Docker. If changing any of the commands, or changing the Dockerfile itself, the tests in `cmd/openfga/main_test.go` must be updated.