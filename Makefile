DB_DSN ?= postgres://orch:orch@localhost:5433/orch
PG_CONTAINER ?= orch-pg-test

.PHONY: build test run lint docker-build docker-run clean
.PHONY: migrate-up migrate-down migrate-up-container migrate-down-container pg-start pg-stop test-unit

build:
	go build -o bin/orchestrator ./cmd/orchestrator
	go build -o bin/orchctl ./cmd/orchctl
	go build -o bin/migrate ./cmd/migrate

test:
	go test ./... -race -coverprofile=coverage.out -coverpkg=./...

test-unit:
	go test ./...

run:
	go run ./cmd/orchestrator

lint:
	go vet ./...

# Apply migrations against DB_DSN (default: a throwaway local Postgres).
migrate-up: build
	./bin/migrate up "$(DB_DSN)"

migrate-down: build
	./bin/migrate down "$(DB_DSN)"

# Convenience: spin up a throwaway Postgres container, migrate up, then tear it down.
pg-start:
	@docker rm -f $(PG_CONTAINER) >/dev/null 2>&1 || true
	docker run -d --name $(PG_CONTAINER) \
		-e POSTGRES_USER=orch -e POSTGRES_PASSWORD=orch -e POSTGRES_DB=orch \
		-p 5433:5432 postgres:16-alpine
	@until pg_isready -h localhost -p 5433 -U orch -d orch >/dev/null 2>&1; do echo "waiting for pg..."; sleep 1; done

pg-stop:
	@docker rm -f $(PG_CONTAINER) >/dev/null 2>&1 || true

migrate-up-container: pg-start migrate-up
	@echo "migrations applied against $(PG_CONTAINER)"

migrate-down-container: migrate-down pg-stop
	@echo "migrations rolled back; container stopped"

docker-build:
	docker build -t ai-crypto-onramp/transaction-orchestrator .

docker-run:
	docker run --rm -p 8080:8080 ai-crypto-onramp/transaction-orchestrator

clean:
	rm -rf bin/ coverage.out