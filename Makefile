.PHONY: build test test-all lint fmt clean help restore integration-test integration-up integration-down conformance-test package

# Integration stack configuration. Every value is overridable so the suite can run
# against any reachable Streamline server, not just a hard-coded local one.
STREAMLINE_IMAGE ?= ghcr.io/streamlinelabs/streamline:latest
STREAMLINE_KAFKA_PORT ?= 9092
STREAMLINE_HTTP_PORT ?= 9094
STREAMLINE_BOOTSTRAP_SERVERS ?= localhost:$(STREAMLINE_KAFKA_PORT)
STREAMLINE_HTTP_URL ?= http://localhost:$(STREAMLINE_HTTP_PORT)
STREAMLINE_READY_TIMEOUT_SECONDS ?= 60

COMPOSE_ENV = STREAMLINE_IMAGE=$(STREAMLINE_IMAGE) \
	STREAMLINE_KAFKA_PORT=$(STREAMLINE_KAFKA_PORT) \
	STREAMLINE_HTTP_PORT=$(STREAMLINE_HTTP_PORT)

INTEGRATION_ENV = STREAMLINE_INTEGRATION=1 \
	STREAMLINE_BOOTSTRAP_SERVERS=$(STREAMLINE_BOOTSTRAP_SERVERS) \
	STREAMLINE_HTTP_URL=$(STREAMLINE_HTTP_URL) \
	STREAMLINE_READY_TIMEOUT_SECONDS=$(STREAMLINE_READY_TIMEOUT_SECONDS)

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

restore: ## Restore NuGet packages
	dotnet restore

build: restore ## Build the SDK
	dotnet build --no-restore

test: build ## Run the hermetic test suite (no server required)
	dotnet test --no-build --verbosity normal

fmt: ## Format code with dotnet format
	dotnet format

lint: ## Check for vulnerabilities
	dotnet list package --vulnerable 2>/dev/null || true

clean: ## Clean build artifacts
	dotnet clean
	rm -rf **/bin **/obj ./artifacts

package: restore ## Create NuGet packages in ./artifacts
	# pack defaults to Release, so the Release binaries must exist before --no-build.
	dotnet build --no-restore --configuration Release
	dotnet pack --no-build --configuration Release -o ./artifacts

integration-up: ## Start the Streamline server used by integration tests
	$(COMPOSE_ENV) docker compose -f docker-compose.test.yml up -d --wait

integration-down: ## Stop the Streamline server used by integration tests
	$(COMPOSE_ENV) docker compose -f docker-compose.test.yml down -v

integration-test: build ## Run integration tests against a running server (opt-in)
	$(INTEGRATION_ENV) dotnet test --no-build --filter "Category=Integration" --verbosity normal

conformance-test: build ## Run the conformance suite against a running server (opt-in)
	$(INTEGRATION_ENV) dotnet test tests/Streamline.Conformance --no-build --filter "Category=Conformance" --verbosity normal

test-all: ## Start a server, run hermetic + integration tests, then tear it down
	$(MAKE) integration-up
	$(MAKE) integration-test || ( $(MAKE) integration-down; exit 1 )
	$(MAKE) integration-down
