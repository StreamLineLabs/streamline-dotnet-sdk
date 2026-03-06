.PHONY: build test lint fmt clean help restore integration-test

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

restore: ## Restore NuGet packages
	dotnet restore

build: restore ## Build the SDK
	dotnet build --no-restore

test: build ## Run tests
	dotnet test --no-build --verbosity normal

fmt: ## Format code with dotnet format
	dotnet format

lint: ## Check for vulnerabilities
	dotnet list package --vulnerable 2>/dev/null || true

clean: ## Clean build artifacts
	dotnet clean
	rm -rf **/bin **/obj

package: build ## Create NuGet package
	dotnet pack --no-build -o ./artifacts

integration-test: build ## Run integration tests (requires docker compose)
	docker compose -f docker-compose.test.yml up -d
	@echo "Waiting for Streamline server..."
	@for i in $$(seq 1 30); do \
		if curl -sf http://localhost:9094/health/live > /dev/null 2>&1; then \
			echo "Server ready"; \
			break; \
		fi; \
		sleep 2; \
	done
	dotnet test --no-build --filter "Category=Integration" --verbosity normal || true
	docker compose -f docker-compose.test.yml down -v
