MODULE := github.com/notification-fanout/service
BINARY := bin/notification-fanout
DATABASE_URL ?= postgres://postgres:postgres@localhost:5432/notification_fanout?sslmode=disable

.PHONY: run lint build test e2e migrate tidy

run: build
	./$(BINARY)

lint:
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "Files need gofmt:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi
	go vet ./...

build:
	go build -o $(BINARY) ./cmd/notification-fanout

test:
	go test ./...

e2e:
	go test ./e2e/...

migrate:
	@if [ -z "$(DATABASE_URL)" ]; then \
		echo "DATABASE_URL is required (e.g. make migrate DATABASE_URL=...)"; \
		exit 1; \
	fi
	@found=0; \
	for f in migrations/*.sql; do \
		if [ -f "$$f" ]; then \
			found=1; \
			echo "Applying $$f"; \
			psql "$(DATABASE_URL)" -v ON_ERROR_STOP=1 -f "$$f"; \
		fi; \
	done; \
	if [ "$$found" -eq 0 ]; then \
		echo "No SQL migrations found in migrations/"; \
	fi

tidy:
	go mod tidy
