MODULE := github.com/notification-fanout/service
BINARY := bin/notification-fanout
DATABASE_URL ?= postgres://postgres:postgres@localhost:5432/notification_fanout?sslmode=disable

.PHONY: run lint build test e2e e2e-verbose e2econtainer e2elocaldb ensure-local-postgres migrate tidy

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

e2e-verbose:
	go test -v ./e2e/...

e2econtainer:
	@command -v docker >/dev/null 2>&1 || { echo "docker is required for make e2econtainer"; exit 1; }
	@set -eu; \
	CONTAINER="notification-fanout-e2e-db-$$PPID"; \
	echo "Starting ephemeral Postgres container: $$CONTAINER"; \
	trap 'docker rm -f "$$CONTAINER" >/dev/null 2>&1 || true' EXIT; \
	docker run -d -P --name "$$CONTAINER" \
		-e POSTGRES_USER=postgres \
		-e POSTGRES_PASSWORD=postgres \
		-e POSTGRES_DB=notification_fanout \
		postgres:16 >/dev/null; \
	for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do \
		if docker exec "$$CONTAINER" pg_isready -U postgres -d notification_fanout >/dev/null 2>&1; then \
			break; \
		fi; \
		sleep 1; \
		if [ "$$i" -eq 30 ]; then \
			echo "Postgres container did not become ready in time"; \
			exit 1; \
		fi; \
	done; \
	HOST_PORT="$$(docker port "$$CONTAINER" 5432/tcp | awk -F: '{print $$NF}')"; \
	DATABASE_URL="postgres://postgres:postgres@127.0.0.1:$$HOST_PORT/notification_fanout?sslmode=disable"; \
	echo "Running DB-backed local e2e with DATABASE_URL=$$DATABASE_URL"; \
	DATABASE_URL="$$DATABASE_URL" go test -v -count=1 -tags localdb ./e2edb/...

e2elocaldb: ensure-local-postgres
	@if [ -z "$(DATABASE_URL)" ]; then \
		echo "DATABASE_URL is required (e.g. make e2elocaldb DATABASE_URL=...)"; \
		exit 1; \
	fi
	@echo "Running DB-backed local e2e with existing DATABASE_URL=$(DATABASE_URL)"
	DATABASE_URL="$(DATABASE_URL)" go test -v -count=1 -tags localdb ./e2edb/...

ensure-local-postgres:
	@if ! command -v pg_isready >/dev/null 2>&1 || ! command -v psql >/dev/null 2>&1; then \
		echo "Postgres tools not found; installing postgresql + postgresql-client..."; \
		if ! command -v apt-get >/dev/null 2>&1; then \
			echo "apt-get not found. Install Postgres manually, then rerun make e2elocaldb."; \
			exit 1; \
		fi; \
		sudo apt-get update && sudo apt-get install -y postgresql postgresql-client; \
	fi
	@if pg_isready -h localhost -p 5432 -U postgres -d notification_fanout >/dev/null 2>&1; then \
		echo "Local Postgres already running and reachable"; \
	else \
		echo "Local Postgres not reachable; attempting to start service..."; \
		if command -v systemctl >/dev/null 2>&1; then \
			sudo systemctl start postgresql || true; \
		fi; \
		if command -v service >/dev/null 2>&1; then \
			sudo service postgresql start || true; \
		fi; \
		for i in 1 2 3 4 5 6 7 8 9 10; do \
			if pg_isready -h localhost -p 5432 -U postgres >/dev/null 2>&1; then \
				break; \
			fi; \
			sleep 1; \
			if [ "$$i" -eq 10 ]; then \
				echo "Could not start local Postgres automatically. Start it manually, then re-run make e2elocaldb."; \
				exit 1; \
			fi; \
		done; \
	fi
	@echo "Ensuring postgres password and notification_fanout database exist..."
	@sudo -u postgres psql -v ON_ERROR_STOP=1 -c "ALTER USER postgres PASSWORD 'postgres';" >/dev/null 2>&1 || true
	@sudo -u postgres psql -v ON_ERROR_STOP=1 -tc "SELECT 1 FROM pg_database WHERE datname='notification_fanout'" | grep -q 1 || \
		sudo -u postgres psql -v ON_ERROR_STOP=1 -c "CREATE DATABASE notification_fanout;" >/dev/null

migrate:
	@if [ -z "$(DATABASE_URL)" ]; then \
		echo "DATABASE_URL is required (e.g. make migrate DATABASE_URL=...)"; \
		exit 1; \
	fi
	@command -v psql >/dev/null 2>&1 || { \
		echo "psql is required for make migrate"; \
		exit 1; \
	}
	@found=0; \
	for f in migrations/*.sql; do \
		if [ -f "$$f" ]; then \
			found=1; \
			echo "Applying $$f"; \
			psql "$(DATABASE_URL)" -v ON_ERROR_STOP=1 -f "$$f" || exit 1; \
		fi; \
	done; \
	if [ "$$found" -eq 0 ]; then \
		echo "No SQL migrations found in migrations/"; \
	fi

tidy:
	go mod tidy
