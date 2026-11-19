GOLANGCI_LINT := go tool -modfile=tools/golangci-lint/go.mod golangci-lint

.PHONY: fmt lint test check up down

fmt:
	$(GOLANGCI_LINT) fmt

lint:
	$(GOLANGCI_LINT) run

test:
	go test -race ./...

check: lint test

up:
	docker compose up --build --wait

down:
	docker compose down --volumes
