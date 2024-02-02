#-----------------------------------------------------------------------------------------------------------------------
# Variables (https://www.gnu.org/software/make/manual/html_node/Using-Variables.html#Using-Variables)
#-----------------------------------------------------------------------------------------------------------------------
.DEFAULT_GOAL := help

BINARY_NAME = openfga
BUILD_DIR ?= $(CURDIR)/dist
GO_BIN ?= $(shell go env GOPATH)/bin
GO_PACKAGES := $(shell go list ./... | grep -vE "vendor")
GO_PACKAGES_COMMA_SEPARATED := $(shell go list ./... | grep -vE "vendor" | paste -sd, -)

# Colors for the printf
RESET = $(shell tput sgr0)
COLOR_WHITE = $(shell tput setaf 7)
COLOR_BLUE = $(shell tput setaf 4)
TEXT_ENABLE_STANDOUT = $(shell tput smso)
TEXT_DISABLE_STANDOUT = $(shell tput rmso)

#-----------------------------------------------------------------------------------------------------------------------
# Rules (https://www.gnu.org/software/make/manual/html_node/Rule-Introduction.html#Rule-Introduction)
#-----------------------------------------------------------------------------------------------------------------------
.PHONY: help clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

clean: ## Clean project files
	${call print, "Removing ${BUILD_DIR}/${BINARY_NAME}"}
	@rm "${BUILD_DIR}/${BINARY_NAME}"
	@go clean -x -r -i

#-----------------------------------------------------------------------------------------------------------------------
# Dependencies
#-----------------------------------------------------------------------------------------------------------------------
.PHONY: deps

deps: ## Download dependencies
	${call print, "Downloading dependencies"}
	@go mod vendor && go mod tidy

$(GO_BIN)/golangci-lint:
	${call print, "Installing golangci-lint within ${GO_BIN}"}
	@go install -v github.com/golangci/golangci-lint/cmd/golangci-lint@latest

$(GO_BIN)/mockgen:
	${call print, "Installing mockgen within ${GO_BIN}"}
	@go install -v go.uber.org/mock/mockgen@latest

$(GO_BIN)/CompileDaemon:
	${call print, "Installing CompileDaemon within ${GO_BIN}"}
	@go install -v github.com/githubnemo/CompileDaemon@latest

$(GO_BIN)/openfga: install

#-----------------------------------------------------------------------------------------------------------------------
# Building & Installing
#-----------------------------------------------------------------------------------------------------------------------
.PHONY: build install

build: ## Build the OpenFGA service
	${call print, "Building the OpenFGA binary within ${BUILD_DIR}/${BINARY_NAME}"}
	@go build -v -o "${BUILD_DIR}/${BINARY_NAME}" "$(CURDIR)/cmd/openfga"

install: ## Install the OpenFGA service
	${call print, "Installing the OpenFGA binary within ${GO_BIN}"}
	@go install -v "$(CURDIR)/cmd/${BINARY_NAME}"

#-----------------------------------------------------------------------------------------------------------------------
# Checks
#-----------------------------------------------------------------------------------------------------------------------
.PHONY: lint

lint: $(GO_BIN)/golangci-lint ## Lint Go source files
	${call print, "Linting Go source files"}
	@golangci-lint run -v --fix -c .golangci.yaml ./...

#-----------------------------------------------------------------------------------------------------------------------
# Tests
#-----------------------------------------------------------------------------------------------------------------------
.PHONY: test test-docker test-bench test-mocks

test: test-mocks ## Run all tests
	${call print, "Running tests"}
	@go test -race \
			-coverpkg=${GO_PACKAGES_COMMA_SEPARATED} \
			-coverprofile=coverageunit.tmp.out \
			-covermode=atomic \
			-count=1 \
			-timeout=10m \
			${GO_PACKAGES}
	@cat coverageunit.tmp.out | grep -v "mocks" > coverageunit.out
	@rm coverageunit.tmp.out

test-docker: ## Run Docker tests
	${call print, "Running docker tests"}
	@if [ -z "$${CI}" ]; then \
		docker build -t="openfga/openfga:dockertest" .; \
	fi
	@go test -v -count=1 -timeout=5m -tags=docker ./cmd/openfga/...

test-bench: test-mocks ## Run benchmark tests. See https://pkg.go.dev/cmd/go#hdr-Testing_flags
	${call print, "Running benchmark tests"}
	@go test ./... -bench . -benchtime 5s -timeout 0 -run=XXX -cpu 1 -benchmem

test-mocks: $(GO_BIN)/mockgen ## Generate test mocks
	${call print, "Generating test mocks"}
	@go generate -v ./...

#-----------------------------------------------------------------------------------------------------------------------
# Development
#-----------------------------------------------------------------------------------------------------------------------
.PHONY: dev-run

dev-run: $(GO_BIN)/CompileDaemon $(GO_BIN)/openfga ## Run the OpenFGA server
	${call print, "Starting OpenFGA server"}
	@echo "<> Select an option for the data storage:"
	@select option in "In-Memory" "MySQL" "Postgres"; do \
		case $$option in \
			"In-Memory") \
				echo "==> Running OpenFGA with In-Memory data storage"; \
				CompileDaemon -graceful-kill -build='make install' -command='openfga run'; \
				break; \
				;; \
			"MySQL") \
				echo "==> Running OpenFGA with MySQL data storage"; \
				docker run -d --name mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=secret -e MYSQL_DATABASE=openfga mysql:8  > /dev/null 2>&1 || docker start mysql; \
				sleep 2; \
				openfga migrate --datastore-engine mysql --datastore-uri 'root:secret@tcp(localhost:3306)/openfga?parseTime=true'; \
				CompileDaemon -graceful-kill -build='make install' -command="openfga run --datastore-engine mysql --datastore-uri root:secret@tcp(localhost:3306)/openfga?parseTime=true"; \
				break; \
				;; \
			"Postgres") \
				echo "==> Running OpenFGA with Postgres data storage"; \
				docker run -d --name postgres -p 5432:5432 -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=password postgres:14  > /dev/null 2>&1 || docker start postgres; \
				sleep 2; \
				openfga migrate --datastore-engine postgres --datastore-uri 'postgres://postgres:password@localhost:5432/postgres'; \
				CompileDaemon -graceful-kill -build='make install' -command="openfga run --datastore-engine postgres --datastore-uri postgres://postgres:password@localhost:5432/postgres"; \
				break; \
				;; \
			*) \
				echo "Invalid option. Try again."; \
				;; \
		esac; \
	done

#-----------------------------------------------------------------------------------------------------------------------
# Helpers
#-----------------------------------------------------------------------------------------------------------------------
define print
	@printf "${TEXT_ENABLE_STANDOUT}${COLOR_WHITE} 🚀 ${COLOR_BLUE} %-70s ${COLOR_WHITE} ${TEXT_DISABLE_STANDOUT}\n" $(1)
endef
