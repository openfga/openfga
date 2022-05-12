local_postgres ?= postgresql://localhost:5432
postgres_test_table ?= dev-openfga
basic_code_locations=./internal/...,./pkg/...,./server/...,./storage/...

backends ?= TUPLE_BACKEND_URL=$(local_postgres) TUPLE_BACKEND_TYPE=postgres AUTHORIZATION_MODEL_BACKEND_URL=$(local_postgres) AUTHORIZATION_MODEL_BACKEND_TYPE=postgres SETTINGS_BACKEND_URL=$(local_postgres) SETTINGS_BACKEND_TYPE=postgres ASSERTIONS_BACKEND_URL=$(local_postgres) ASSERTIONS_BACKEND_TYPE=postgres CHANGELOG_BACKEND_URL=$(local_postgres) CHANGELOG_BACKEND_TYPE=postgres STORES_BACKEND_URL=$(local_postgres) STORES_BACKEND_TYPE=postgres
test_backend ?= $(backends) TEST_CONFIG_BACKEND_TYPE=postgres TEST_CONFIG_BACKEND_URL=$(local_postgres) TUPLE_DATABASE_NAME=$(postgres_test_table) AUTHORIZATION_MODEL_DATABASE_NAME=$(postgres_test_table) SETTINGS_DATABASE_NAME=$(postgres_test_table) ASSERTIONS_DATABASE_NAME=$(postgres_test_table) CHANGELOG_DATABASE_NAME=$(postgres_test_table) STORES_DATABASE_NAME=$(postgres_test_table)

.DEFAULT_GOAL := help

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: download
download:
	@cd tools/ && go mod download

.PHONY: install-tools
install-tools: download ## Install developer tooling
	@cd tools && go list -f '{{range .Imports}}{{.}} {{end}}' tools.go | CGO_ENABLED=0 xargs go install -mod=readonly

.PHONY: lint
lint:  ## Lint Go source files
	@golangci-lint run

.PHONY: clean
clean: ## Clean files
	rm ./bin/openfga

.PHONY: build
build:  ## Build/compile the OpenFGA service
	go build -o ./bin/openfga ./cmd/openfga

.PHONY: run
run: build ## Run the OpenFGA server with in-memory storage
	./bin/openfga

.PHONY: run-postgres
run-postgres: build ## Run the OpenFGA server with Postgres storage
	docker-compose down
	docker-compose up --detach postgres
	$(test_backend) make run

.PHONY: test
test: test-unit test-functional ## Run tests (unit and functional) against the OpenFGA server

.PHONY: test-unit
test-unit: ## Run unit tests against the OpenFGA server
	go test $(gotest_extra_flags) -v \
		-coverprofile=coverageunitmemory.out \
		-covermode=atomic -race \
		-coverpkg=$(basic_code_locations) \
		`go list ./server/... | grep -v postgres` `go list ./internal/... | grep -v postgres` `go list ./storage/... | grep -v postgres`

.PHONY: test-unit-ci
test-unit-ci:  ## Run unit tests against the OpenFGA server in CI/CD environment
	$(test_backend) go test $(gotest_extra_flags) -v \
		-coverprofile=coverageunitpostgres.out \
		-covermode=atomic -race \
		-p 1 \
		-coverpkg=$(basic_code_locations) \
		`go list ./server/... | grep -v memory` `go list ./internal/... | grep -v memory` `go list ./storage/... | grep -v memory`

.PHONY: test-functional
test-functional: ## Run functional tests against the OpenFGA server
	go test $(gotest_extra_flags) \
		-coverprofile=coveragefunctional.out \
		-covermode=atomic -v --tags=functional \
		-coverpkg=$(basic_code_locations) \
		./server/...

.PHONY: initialize-db
initialize-db:
    #TODO wait for docker postgres container to be healthy
	go clean -testcache; $(test_backend) go test ./storage/postgres -run TestReadTypeDefinition; \
