.PHONY: build test test-integration test-e2e vet lint fmt up down

build:
	go build ./...

test:
	go test ./...

test-integration:
	go test -tags integration -count=1 ./internal/...

# test-e2e runs the end-to-end saga on testcontainers: Kafka+PostgreSQL, fake
# Ledger (no compose stand needed).
test-e2e:
	go test -tags e2e -count=1 -timeout 12m ./test/e2e/...

# E2E: compose stand runs follow the test/e2e/E2E.md instruction (scenarios
# E2E_Payout_Schema and E2E_Payout_Approval; artifacts go to
# test/e2e/.runs/<UTC mark>/); there are no PowerShell run scripts.

vet:
	go vet ./...

# lint mounts the whole services/ directory: the replace directive to ../cbl-kit
# resolves inside the container.
lint:
	docker run --rm -v "$(CURDIR)":/src -w /src -e GOPRIVATE=github.com/cbl/* -e GIT_CONFIG_COUNT=1 -e GIT_CONFIG_KEY_0=url.https://github.com/ZMichael3012/.insteadOf -e GIT_CONFIG_VALUE_0=https://github.com/cbl/ golangci/golangci-lint:latest-alpine golangci-lint run ./...

fmt:
	gofmt -l .

up:
	docker compose -f deploy/docker-compose.yml --profile app up -d --build

down:
	docker compose -f deploy/docker-compose.yml down -v
