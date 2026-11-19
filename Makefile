GOLANGCI_LINT := go tool -modfile=tools/golangci-lint/go.mod golangci-lint

.PHONY: fmt lint test check

fmt:
	$(GOLANGCI_LINT) fmt

lint:
	$(GOLANGCI_LINT) run

test:
	go test -race ./...

check: lint test
