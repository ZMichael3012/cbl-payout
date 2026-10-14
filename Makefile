.PHONY: build test vet lint fmt up down

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

lint:
	docker run --rm -v "$(CURDIR)":/app -w /app golangci/golangci-lint:v1.64.8-alpine golangci-lint run ./...

fmt:
	gofmt -l .

up:
	docker compose -f deploy/docker-compose.yml up -d

down:
	docker compose -f deploy/docker-compose.yml down -v