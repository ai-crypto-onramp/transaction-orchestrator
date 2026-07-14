.PHONY: build test run lint cover docker-build docker-run clean \
	migrate-up migrate-down migrate-new proto

build:
	go build -o bin/transaction-orchestrator ./cmd/orchestrator

test:
	@coverpkg=$$(go list ./internal/... | grep -v '/internal/pb/' | tr '\n' ','); \
	go test ./internal/... -race -coverprofile=coverage.out -coverpkg=$${coverpkg%,}

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

# proto regenerates the gRPC bindings under internal/pb from proto/*.proto.
# Requires protoc, protoc-gen-go, and protoc-gen-go-grpc on $PATH.
proto:
	@for svc in policy payment kyt mpc blockchain ledger; do \
		protoc -I proto \
			--go_out=internal/pb/$$svc --go_opt=paths=source_relative \
			--go-grpc_out=internal/pb/$$svc --go-grpc_opt=paths=source_relative \
			proto/$$svc.proto; \
	done
	@echo "regenerated internal/pb/{policy,payment,kyt,mpc,blockchain,ledger}"
