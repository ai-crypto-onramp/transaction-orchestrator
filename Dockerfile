# syntax=docker/dockerfile:1.6
FROM golang:1.25 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -o /out/orchestrator ./cmd/orchestrator
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -o /out/orchctl       ./cmd/orchctl

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/orchestrator /orchestrator
COPY --from=builder /out/orchctl       /orchctl
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/orchestrator"]