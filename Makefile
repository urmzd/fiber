.PHONY: all init build test race lint fmt check up down integration example

all: check

init:
	git config core.hooksPath .githooks
	go mod download && go mod tidy

build:
	go build ./...

test:
	go test ./...

race:
	go test ./... -race

lint:
	golangci-lint run

fmt:
	gofmt -w .

check: fmt lint test

# Postgres (with pgvector) and Redis for the backend conformance suites.
up:
	docker compose up --wait

down:
	docker compose down

integration:
	FIBER_POSTGRES_TEST_URL=postgres://fiber:fiber@localhost:$${FIBER_POSTGRES_PORT:-5432}/fiber?sslmode=disable \
	FIBER_REDIS_ADDR=localhost:$${FIBER_REDIS_PORT:-6379} \
	go test -race -count=1 ./...

example:
	go run ./examples/local
	go run ./examples/downstream
