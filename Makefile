.PHONY: build test lint clean help restore

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

restore: ## Restore NuGet packages
	dotnet restore

build: restore ## Build the SDK
	dotnet build --no-restore

test: build ## Run tests
	dotnet test --no-build --verbosity normal

lint: ## Check for vulnerabilities
	dotnet list package --vulnerable 2>/dev/null || true

clean: ## Clean build artifacts
	dotnet clean
	rm -rf **/bin **/obj

package: build ## Create NuGet package
	dotnet pack --no-build -o ./artifacts
