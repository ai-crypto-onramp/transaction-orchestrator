.PHONY: build test run lint docker-build docker-run clean

build:
	go build -o bin/server ./cmd/orchestrator

test:
	go test ./cmd/... ./internal/... -race -coverprofile=coverage.out -coverpkg=./...

run:
	go run ./cmd/orchestrator

lint:
	go vet ./...

docker-build:
	docker build -t ai-crypto-onramp/transaction-orchestrator .

docker-run:
	docker run --rm -p 8080:8080 ai-crypto-onramp/transaction-orchestrator

clean:
	rm -rf bin/ coverage.out
