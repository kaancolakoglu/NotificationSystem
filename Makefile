GOLANGCI_LINT := go tool -modfile=tools/golangci-lint/go.mod golangci-lint
SQLC := go tool -modfile=tools/sqlc/go.mod sqlc

.PHONY: fmt lint test test-short check generate up down

fmt:
	$(GOLANGCI_LINT) fmt

lint:
	$(GOLANGCI_LINT) run

test:
	go test -race ./...

test-short:
	go test -race -short ./...

check: lint test

generate:
	$(SQLC) generate

up:
	docker compose up --build --wait

down:
	docker compose down --volumes
