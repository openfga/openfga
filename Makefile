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
build: ## Build/compile the OpenFGA service
	go build -o ./bin/openfga ./cmd/openfga

.PHONY: run
run: build ## Run the OpenFGA server with in-memory storage
	./bin/openfga

.PHONY: run-postgres
run-postgres: build ## Run the OpenFGA server with Postgres storage
	docker-compose down
	docker-compose up -d postgres
	$(test_backend) make run

.PHONY: go-generate
go-generate: install-tools
	go generate ./...

.PHONY: unit-test
unit-test: go-generate ## Run unit tests
	go test $(gotest_extra_flags) -v \
			-coverprofile=coverageunit.out \
			-covermode=atomic -race \
			-count=1 \
			./...

.PHONY: initialize-db
initialize-db: go-generate
    #TODO wait for docker postgres container to be healthy
	go clean -testcache; $(test_backend) go test ./storage/postgres -run TestReadTypeDefinition; \
