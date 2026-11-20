GOLANGCI_LINT := go tool -modfile=tools/golangci-lint/go.mod golangci-lint
SQLC := go tool -modfile=tools/sqlc/go.mod sqlc
OAPI_CODEGEN := go tool -modfile=tools/oapi-codegen/go.mod oapi-codegen

.PHONY: fmt lint test test-short test-isolated check generate up down loadtest loadtest-latency

fmt:
	$(GOLANGCI_LINT) fmt

lint:
	$(GOLANGCI_LINT) run

test:
	go test -race ./...

test-short:
	go test -race -short ./...

test-isolated:
	TESTDB_ISOLATED=1 go test -race ./...

check: lint test

generate:
	$(SQLC) generate
	$(OAPI_CODEGEN) -config oapi-codegen.yaml api/openapi.yaml

up:
	docker compose up --build --wait

down:
	docker compose down --volumes

loadtest:
	docker compose exec -T postgres psql --username=postgres --quiet --command="TRUNCATE notifications, delivery_attempts, idempotency_keys"
	docker compose --profile loadtest run --rm k6 run /scripts/$(script).js

loadtest-latency:
	docker compose exec -T postgres psql --username=postgres < loadtest/latency.sql
