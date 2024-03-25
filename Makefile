.DEFAULT_GOAL := help

GO_BIN ?= $(shell go env GOPATH)/bin

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

$(GO_BIN)/golangci-lint:
	@echo "==> Installing golangci-lint within "${GO_BIN}""
	@go install -v github.com/golangci/golangci-lint/cmd/golangci-lint@latest

.PHONY: lint
lint: $(GO_BIN)/golangci-lint ## Lint Go source files
	@echo "==> Linting Go source files"
	@golangci-lint run -v --fix -c .golangci.yaml ./...

.PHONY: clean
clean: ## Clean files
	rm ./openfga

.PHONY: build
build: ## Build the OpenFGA service
	go build -o ./openfga ./cmd/openfga

.PHONY: run
run: build ## Run the OpenFGA server with in-memory storage
	./openfga run

.PHONY: start-postgres
start-postgres: ## Start a Postgres Docker container
	docker run -d --name postgres -p 5432:5432 -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=password postgres:14
	@echo \> use \'postgres://postgres:password@localhost:5432/postgres\' to connect to postgres

.PHONY: migrate-postgres
migrate-postgres: build ## Run Postgres migrations
	# nosemgrep: detected-username-and-password-in-uri
	./openfga migrate --datastore-engine postgres --datastore-uri 'postgres://postgres:password@localhost:5432/postgres'

.PHONY: run-postgres
run-postgres: build ## Run the OpenFGA server with Postgres storage
	./openfga run --datastore-engine postgres --datastore-uri 'postgres://postgres:password@localhost:5432/postgres'

.PHONY: start-mysql
start-mysql: build ## Start a MySQL Docker container
	docker run -d --name mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=secret -e MYSQL_DATABASE=openfga mysql:8

.PHONY: migrate-mysql
migrate-mysql: build ## Run MySQL migrations
	# nosemgrep: detected-username-and-password-in-uri
	./openfga migrate --datastore-engine mysql --datastore-uri 'root:secret@tcp(localhost:3306)/openfga?parseTime=true'

.PHONY: run-mysql
run-mysql: build ## Run the OpenFGA server with MySQL storage
	./openfga run --datastore-engine mysql --datastore-uri 'root:secret@tcp(localhost:3306)/openfga?parseTime=true'

.PHONY: download
download:
	@cd tools/ && go mod download

.PHONY: install-tools
install-tools: download ## Install developer tooling
	@cd tools && go list -e -f '{{range .Imports}}{{.}} {{end}}' tools.go | CGO_ENABLED=0 xargs go install -mod=readonly

.PHONY: go-generate
go-generate: install-tools
	go generate ./...

.PHONY: test
test: go-generate ## Run all tests
	go test -race \
			-coverpkg=./... \
			-coverprofile=coverageunit.tmp.out \
			-covermode=atomic \
			-count=1 \
			-timeout=10m \
			./...
	@cat coverageunit.tmp.out | grep -v "mocks" > coverageunit.out
	@rm coverageunit.tmp.out

build-docker-test-image: ## Build Docker image needed to run Docker tests
	docker build -t="openfga/openfga:dockertest" .

.PHONY: test-docker-ci
test-docker-ci:
	go test -v \
			-count=1 \
			-timeout=5m \
			-tags=docker \
			./cmd/openfga/...

.PHONY: test-docker-local
test-docker-local: build-docker-test-image ## Run Docker tests (locally)
	make test-docker-ci

.PHONY: bench
bench: go-generate ## Run benchmark test. See https://pkg.go.dev/cmd/go#hdr-Testing_flags
	go test ./... -bench . -benchtime 5s -timeout 0 -run=XXX -cpu 1 -benchmem
