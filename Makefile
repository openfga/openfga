.DEFAULT_GOAL := help
COVERPKG := $(shell eval go list ./... | grep -v mocks |  paste -sd "," -)

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
	rm ./openfga

.PHONY: build
build: ## Build/compile the OpenFGA service
	go build -o ./openfga ./cmd/openfga

.PHONY: run
run: build ## Run the OpenFGA server with in-memory storage
	./openfga run


.PHONY: start-postgres-container
start-postgres-container:
	docker run -d --name postgres -p 5432:5432 -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=password postgres:14
	@echo \> use \'postgres://postgres:password@localhost:5432/postgres\' to connect to postgres

.PHONY: migrate-postgres
migrate-postgres: build
	# nosemgrep: detected-username-and-password-in-uri
	./openfga migrate --datastore-engine postgres --datastore-uri 'postgres://postgres:password@localhost:5432/postgres'

.PHONY: run-postgres
run-postgres: build
	./openfga run --datastore-engine postgres --datastore-uri 'postgres://postgres:password@localhost:5432/postgres'

.PHONY: go-generate
go-generate: install-tools
	go generate ./...

.PHONY: unit-test
unit-test: go-generate ## Run unit tests
	go test $(gotest_extra_flags) -v \
			-coverprofile=coverageunit.out \
			-coverpkg=$(COVERPKG) \
			-covermode=atomic -race \
			-count=1 \
			./...


.PHONY: functional-test
functional-test: 
	go test $(gotest_extra_flags) -v \
			-coverprofile=coveragefunctional.out \
			-coverpkg=$(COVERPKG) \
			-covermode=atomic -race \
			-count=1 \
			-tags=functional \
			./cmd/openfga/...

.PHONY: bench
bench: go-generate
	go test ./... -bench=. -run=XXX -benchmem
