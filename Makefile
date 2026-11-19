GOLANGCI_LINT := go tool -modfile=tools/golangci-lint/go.mod golangci-lint
SQLC := go tool -modfile=tools/sqlc/go.mod sqlc
OAPI_CODEGEN := go tool -modfile=tools/oapi-codegen/go.mod oapi-codegen

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
	$(OAPI_CODEGEN) -config oapi-codegen.yaml api/openapi.yaml

up:
	docker compose up --build --wait

down:
	docker compose down --volumes
