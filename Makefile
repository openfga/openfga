basic_code_locations=./internal/...,./pkg/...,./server/...,./storage/...

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

.PHONY: test-unit-memory
test-unit-memory: ## Run unit tests against the OpenFGA server using the in-memory datastore
	go test $(gotest_extra_flags) -v \
		-coverprofile=coverageunitmemory.out \
		-covermode=atomic -race \
		-coverpkg=$(basic_code_locations) \
    	`go list ./internal/...` `go list ./pkg/...` `go list ./server/...` `go list ./storage/... | grep -v postgres`

.PHONY: test-unit-postgres
test-unit-postgres:  ## Run unit tests against the OpenFGA server using the Postgres datastore
	TEST_CONFIG_BACKEND_TYPE=postgres go test $(gotest_extra_flags) -v \
		-coverprofile=coverageunitpostgres.out \
		-covermode=atomic -race \
		-p 1 \
		-coverpkg=$(basic_code_locations) \
    	`go list ./internal/...` `go list ./pkg/...` `go list ./server/...` `go list ./storage/... | grep -v memory`

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
