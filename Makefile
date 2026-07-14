.PHONY: build test run lint cover docker-build docker-run clean \
	migrate-up migrate-down migrate-new

build:
	go build -o bin/transaction-orchestrator ./cmd/orchestrator

test:
	go test ./internal/... -race -coverprofile=coverage.out -coverpkg=./internal/...

run:
	go run ./cmd/orchestrator

migrate-up:
	go run ./cmd/migrate --up

migrate-down:
	go run ./cmd/migrate --down

migrate-new:
	@test -n "$(NAME)" || (echo "usage: make migrate-new NAME=add_widgets" && exit 1)
	@next=$$(printf '%04d' $$(( $$(ls internal/migrations/*.up.sql 2>/dev/null | wc -l | tr -d ' ') + 1 ))); \
	touch internal/migrations/$${next}_$(NAME).up.sql internal/migrations/$${next}_$(NAME).down.sql; \
	echo "created internal/migrations/$${next}_$(NAME).{up,down}.sql"

lint:
	golangci-lint run

cover: test
	go tool cover -func=coverage.out | tail -1

docker-build:
	docker build -t ai-crypto-onramp/transaction-orchestrator .

docker-run:
	docker run --rm -p 8080:8080 ai-crypto-onramp/transaction-orchestrator

clean:
	rm -rf bin/ coverage.out
