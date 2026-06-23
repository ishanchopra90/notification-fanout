# Implementation Checklist — Notification Fanout Service

Fast-implementation task list derived from `architecture.md`. Tasks are ordered so the
**Minimum Viable Set (MVP)** comes first (a runnable base-design service with `slog`
logging, unit tests, and at least 1–2 end-to-end tests), followed by nice-to-haves and
deployment.

## Instructions (read before each task)

- **Wait for the user to prompt the next task.** Do not start a task until asked.
- **When executing a task:**
  1. Implement the task.
  2. Add **unit test coverage** for the new code.
  3. **Run the unit tests and ensure they pass.**
  4. **Check off the task** (`[x]`) in this file.
- Keep changes scoped to the task. Prefer small, compiling increments.
- Reference `architecture.md` for design decisions, data model, and failure modes.

---

## MVP — Base Design (do these first)

1. - [x] **Project scaffolding** — `go mod init`, package layout (`cmd/`, `internal/{api,store,matcher,worker,config}`), pin deps (`chi`, `pgx`), and a `Makefile` (`run`, `test`, `migrate`).
2. - [x] **Config + structured logging** — env-based config (DB URL, port, worker count, timeouts); initialize **`log/slog` JSON logger to stdout/stderr** as the app-wide logger. Unit test config parsing.
3. - [x] **DB schema & migrations** — `events`, `subscriptions`, `deliveries`, `delivery_attempts` tables + indexes (incl. `events(idempotency_key) UNIQUE`, `subscriptions(type,source) WHERE active`, `deliveries(status,next_attempt_at)`). Apply on startup.
4. - [x] **Store layer (pgx)** — connection pool, `Ping`/`SELECT 1` for readiness, and typed CRUD helpers for each table. Unit test against a test DB / transaction.
5. - [x] **Filter parsing + Matcher** — parse/validate filter JSON (`type`/`source` + flat payload ops `eq/gt/gte/lt/lte`); implement match evaluation. **Unit tests** for match/no-match and each operator.
6. - [x] **Subscription API** — `POST` (validate URL+filter, denormalize `type`/`source`), `GET` (list active), `DELETE` (soft-delete). Unit tests for handlers + store.
7. - [x] **Event ingestion + fanout** — `POST /events` with `Idempotency-Key` (`ON CONFLICT DO NOTHING`); in one tx insert event, match subscriptions, insert `pending` deliveries; return `202`. Unit tests incl. idempotent replay + fanout count.
8. - [x] **Delivery worker pool** — claim due rows via `FOR UPDATE SKIP LOCKED`, POST to webhook with timeout, record `delivery_attempts`, update status (`delivered`/`retrying`/`failed`) with exponential backoff. **Unit tests** for backoff + status transitions.
9. - [x] **Delivery audit API** — `GET /deliveries?event_id=|subscription_id=` returning status + attempt history. Unit tests for both query params + validation.
10. - [x] **Health, readiness & graceful shutdown** — `/healthz`, `/readyz` (DB-gated), and `SIGTERM` drain (stop accepting, stop claiming, finish in-flight, close pool). Unit test readiness states.
11. - [ ] **End-to-end tests (1–2)** — (a) ingest → fanout → deliver to a stub webhook server → assert audit shows `delivered`; (b) failing webhook → retry → `failed` after max attempts. Ensure they run and pass.
12. - [ ] **Makefile targets** — provide and verify each target runs:
    - [ ] `make lint` — run linters/`go vet` (and `gofmt`/`golangci-lint` if available).
    - [ ] `make build` — compile all packages / the service binary.
    - [ ] `make test` — run **all unit tests**.
    - [ ] `make e2e` — run **all end-to-end tests**.
13. - [ ] **README (MVP)** — setup/run instructions, filter rule syntax, and the "sacrifice vs. harden next" summary.

> **MVP cutoff.** After task 13 the service satisfies the mandatory functional + engineering requirements locally with tests.

---

## Nice-to-have / Deployment / Extensions

14. - [ ] **Dockerfile** — multi-stage build producing a small static image.
15. - [ ] **GitHub Actions CI** — `go vet` + `go test ./...` with a Postgres service container on push.
16. - [ ] **Kubernetes manifests (DOKS)** — `Deployment`, `Service`, `ConfigMap`/`Secret`, probes wired to `/healthz` & `/readyz`.
17. - [ ] **Deploy to DOKS** — push image, apply manifests, verify end-to-end against the cluster.
18. - [ ] **Metrics** — Prometheus `/metrics` (ingest rate, fanout size, delivery success/failure, retry depth).
19. - [ ] **SSRF protection** — validate/deny private & link-local webhook targets; scheme allowlist.
20. - [ ] **Replay API** — re-deliver events over a time range.
21. - [ ] **Managed queue backbone** — outbox relay + Redis/Valkey Streams (or Kafka) per architecture §14.2.
22. - [ ] **Per-source ordered delivery** — serialize a source's deliveries to a subscriber.
23. - [ ] **Subscription read-through cache** — only if matching becomes a bottleneck (§14.1).
