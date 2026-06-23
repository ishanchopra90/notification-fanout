# Notification Fanout Service (MVP)

Event-driven webhook fanout service in Go.

It accepts events, matches active subscriptions, creates delivery jobs, retries failed webhook calls with backoff, and exposes delivery audit history.

## Architecture

For the full design, decisions, failure modes, and extension plan, see [`architecture.md`](architecture.md).

## Project structure

```text
.
├── cmd/notification-fanout/      # Service entrypoint and process lifecycle
├── internal/
│   ├── api/                      # HTTP handlers and routing
│   ├── config/                   # Env config + slog logger setup
│   ├── matcher/                  # Filter parsing and match evaluation
│   ├── store/                    # Postgres access, migrations, CRUD, fanout ingest
│   └── worker/                   # Delivery worker pool and retry/backoff logic
├── migrations/                   # SQL schema migrations
├── e2e/                          # End-to-end flow tests
├── Makefile                      # run/lint/build/test/e2e/migrate targets
├── architecture.md               # Detailed architecture document
└── checklist.MD                  # Implementation checklist
```

## What is implemented in MVP

- Event ingestion endpoint with `Idempotency-Key` support (`POST /events`).
- Subscription management (`POST /subscriptions`, `GET /subscriptions`, `DELETE /subscriptions/{id}`).
- Delivery worker pool with claim (`FOR UPDATE SKIP LOCKED`), webhook send, retry/backoff, and terminal failed state.
- Delivery audit API (`GET /deliveries?event_id=...` or `?subscription_id=...`) with attempt history.
- Health/readiness endpoints (`/healthz`, `/readyz`).
- DB migrations and startup migration application.

## Requirements

- Go 1.26+
- Postgres (local or remote)
- `psql` client (for manual `make migrate` usage)

## Configuration

Environment variables:

- `DATABASE_URL` (default: `postgres://postgres:postgres@localhost:5432/notification_fanout?sslmode=disable`)
- `PORT` (default: `8080`)
- `WORKER_COUNT` (default: `4`)
- `HTTP_TIMEOUT` (default: `10s`)
- `REQUEST_TIMEOUT` (default: `30s`)
- `SHUTDOWN_TIMEOUT` (default: `30s`)
- `LOG_LEVEL` (default: `info`)

## Setup and Run

1. Start Postgres and create a database:

```bash
createdb notification_fanout
```

2. (Optional) export DB URL:

```bash
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/notification_fanout?sslmode=disable"
```

3. Run the service:

```bash
make run
```

Notes:

- The app applies SQL migrations on startup.
- You can also apply migrations explicitly with `make migrate`.

## API quick examples

Create subscription:

```bash
curl -X POST http://localhost:8080/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "webhook_url":"https://example.com/webhook",
    "filter":{
      "type":"order.created",
      "source":"checkout-svc",
      "payload":{"amount":{"gt":100}}
    }
  }'
```

Ingest event:

```bash
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: ik-123" \
  -d '{
    "type":"order.created",
    "source":"checkout-svc",
    "payload":{"amount":120,"region":"us"}
  }'
```

Audit deliveries:

```bash
curl "http://localhost:8080/deliveries?event_id=1"
```

## Filter rule syntax

A subscription filter is JSON with optional `type`, optional `source`, and optional `payload` conditions.

- `type` / `source`: exact string match.
- `payload`: flat top-level key checks.
- Supported operators:
  - equality: literal value or `{ "eq": ... }`
  - numeric comparisons: `{ "gt": ... }`, `{ "gte": ... }`, `{ "lt": ... }`, `{ "lte": ... }`

Example:

```json
{
  "type": "order.created",
  "source": "checkout-svc",
  "payload": {
    "region": "us",
    "amount": { "gt": 100 }
  }
}
```

## Test and build commands

- `make lint` - gofmt check + `go vet ./...`
- `make build` - build service binary
- `make test` - run all tests
- `make e2e` - run end-to-end tests

## Sacrifices for simplicity vs. harden next

### Kept simple in MVP

- Single binary (API + workers) and Postgres-as-queue model.
- No external queue/broker.
- At-least-once delivery semantics (duplicates possible, subscriber should dedupe by delivery ID).
- Basic webhook URL validation (scheme + host), no SSRF hardening yet.
- No auth/rate limiting/signatures yet.

### Harden next

- Introduce managed queue + outbox relay for higher throughput and independent worker scaling.
- Add SSRF protections (block private/link-local targets, stricter allowlist).
- Add metrics/tracing/alerts and richer operational visibility.
- Add replay API and optional per-source ordered delivery.
- Add CI pipeline and deployment manifests.
