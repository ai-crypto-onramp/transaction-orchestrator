.PHONY: build test run lint cover docker-build docker-run clean

build:
	go build -o bin/server ./cmd/orchestrator

test:
	go test ./cmd/... ./internal/... -race -coverprofile=coverage.out -coverpkg=./cmd/...,./internal/...

run:
	go run ./cmd/orchestrator

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
