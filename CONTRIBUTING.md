# Contributing to OpenFGA

Please see our [main guide](https://github.com/openfga/.github/blob/main/CONTRIBUTING.md).

# Testing Guidelines

OpenFGA strives to maintain good code coverage with tests so that the possibility of regressions is reduced.

The purpose of this guide is to inform contributors where to write new tests.

## 1. Storage layer

1. Any change to the OpenFGA storage interface must have an integration test in `pkg/storage/test/storage.go`. This is so that anyone writing their own storage adapter can have confidence that it is correct.

## 2. API layer

1. All APIs use an underlying reusable `command` object. These commands must have unit tests. For example, Write API is backed by Write Command, which has unit tests in `pkg/server/commands/write_test.go`. These should target mocked dependencies to cover the majority of the business logic. If you are changing business logic, add a test in the appropriate commands test file.
2. Functional tests in `tests/functional_test.go`. These are tests for validating the API inputs and functional API behavior. The inputs are defined in the proto files (https://github.com/openfga/api).
3. Some APIs' behavior can be fine-tuned with server flags. If this is the case, the tests for the correct wiring of the flags live in `pkg/server/server_test.go`.
4. Some APIs set specific response headers. If this is the case, update the test `TestHTTPHeaders` in `cmd/run/run_test.go`.

### Query APIs (Check, ListObjects, etc.)

Take the Check API. Aside from the tests mentioned in "All APIs", it has:

1. **Exported** integration tests in `tests/check/check.go`. These take in a `client` dependency. This is so that anyone who writes their own version of the server and/or datastore can run these tests by doing
    
    ```go
    import "github.com/openfga/openfga/tests/check"
    
    check.RunAllTests(t, client)
    ```

   The reason these tests live here is because:
    - the test cases are defined using easy-to-write YAML files. This enables us to write test cases that apply to all query APIs. (If one Check returns `allowed=true`, the corresponding ListObjects call will, depending on server parameters and other factors, also include it in the response.)
    - since these tests are exported, it allows authors of datastores to get extra confidence that their implementations of some methods related to Read of Tuples are correct.

> NOTE TO MAINTAINERS: ideally, these tests need not be exported, since they are testing for correctness and should be independent of the storage layer. They are the result of not having enough coverage with unit tests.

2. One test that asserts on the log fields that are emitted: `TestCheckLogs` in `tests/check/check_test.go`.

> NOTE TO MAINTAINERS: ideally, we have a similar test for other APIs as well.

## Docker integration

OpenFGA can be run within Docker. If changing any of the commands, or changing the Dockerfile itself, the tests in `cmd/openfga/main_test.go` must be updated.
