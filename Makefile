.PHONY: test test-unit test-integration build

# Run all tests
test:
	@echo "Running all tests..."
	go test -v -race -count=1 ./...

# Run unit tests only (tests that don't require Redis)
test-unit:
	@echo "Running unit tests..."
	go test -v -run "TestRandomString|TestGetTemplate|TestTemplateConstants|TestJobOptions_Default" -count=1

# Run integration tests (requires Redis)
test-integration:
	@echo "Running integration tests..."
	@echo "Note: Redis must be running on localhost:6379"
	go test -v -run "TestIntegration" -count=1

# Build the library
build:
	@echo "Building..."
	go build ./...

# Help
help:
	@echo "Available targets:"
	@echo "  make test              - Run all tests"
	@echo "  make test-unit         - Run unit tests (no Redis required)"
	@echo "  make test-integration  - Run integration tests (Redis required)"
	@echo "  make build             - Build the library"
