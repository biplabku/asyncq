# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This changelog covers the whole `asyncq` workspace (`asyncq`, `asyncq-derive`,
`asyncq-redis`, `asyncq-postgres`, `asyncq-axum`, `asyncq-actix`), which are
versioned and released together. There are no git tags in this repository yet,
so entries below are reconstructed from commit history rather than linked to
tagged ranges.

## [Unreleased]

### Fixed
- `documentation` link in `asyncq-derive`, `asyncq-redis`, `asyncq-axum`,
  `asyncq-postgres`, and `asyncq-actix` pointed at `docs.rs/asyncq` (inherited
  from the workspace default) instead of each crate's own docs.rs page. Each
  sub-crate now sets its own `documentation` field explicitly.
- `asyncq-redis`'s test suite hardcoded `redis://127.0.0.1/` and silently
  ignored the `REDIS_URL` environment variable that CI sets — it only worked
  because CI's Redis service also happens to run on the default port. Tests
  now read `REDIS_URL` for real.
- `asyncq-postgres`'s test suite had a copy-pasted connection string
  (`hooksmith`/`hooksmith`, left over from the sibling `webhooksmith` crate)
  as its fallback default. Corrected to `asyncq`/`asyncq`.

### Added
- `[package.metadata.docs.rs]` with `all-features = true` on every crate so
  docs.rs builds with optional backends/features enabled instead of guessing.
- `examples/admin_server.rs` for `asyncq-axum` and `asyncq-actix` — minimal
  runnable examples mounting the admin API over `InMemoryBackend`.
- Architecture diagram in the root README showing how the core crate, storage
  backends, and framework integrations fit together.
- This CHANGELOG.

## [0.1.8] - 2026-09-25

### Added
- `asyncq-postgres` examples: `basic` and `transactional_outbox`.
- CI: GitHub Actions workflow running tests against real Redis and PostgreSQL
  service containers, plus a clippy job.

### Fixed
- Ignore the `pg_type` race that can occur when two `migrate()` calls run
  concurrently — `CREATE TABLE IF NOT EXISTS` isn't fully atomic, so a
  `unique_violation` (23505) from the losing caller is now treated as success
  rather than propagated as an error.
- Resolved clippy warnings (`drain_collect`, format-string style, an unused
  import).

## [0.1.7] - 2026-09-24

### Fixed
- Redis backend now tracks completed job count correctly. `stats().completed`
  previously was hardcoded to 0; it's now incremented (`INCR`) on ack and read
  back with `GET`, defaulting to 0 for queues with no completions yet.

## [0.1.6] - 2026-09-24

### Added
- `asyncq-actix` — actix-web admin API with the same endpoints as
  `asyncq-axum`.
- `admin_with_queues` on both `asyncq-axum` and `asyncq-actix` — live
  Prometheus gauges (pending/running/dead/completed) per monitored queue.
- `scheduler` feature on `asyncq` (enabled by default) making the `cron`
  dependency optional; `default-features = false` drops it for environments
  that never use `Scheduler`.

### Changed
- Improved keywords across all crates for crates.io discoverability.

### Fixed
- `asyncq-postgres` description now leads with "no Redis needed" instead of
  a generic blurb.

## [0.1.5] - 2026-09-24

### Added
- Cron scheduling via `Scheduler::register::<Job>("0 0 9 * * *")`, run
  alongside a `Worker` with `tokio::select!`.

### Changed
- README rewritten with badges, a clearer value proposition, and a roadmap
  section.

## [0.1.4] - 2026-09-23

### Fixed
- Broken link and inaccurate PostgreSQL how-to text in the README.

## [0.1.3] - 2026-09-23

### Added
- PostgreSQL backend (`asyncq-postgres`): full `Backend` trait implementation
  over SQLx/`PgPool` — `enqueue`, `claim` (`SELECT ... FOR UPDATE SKIP
  LOCKED`), `ack`, `nack`, `heartbeat`, `reap_stuck`, `stats`, `dead_jobs`,
  `retry_dead`, `retry_all_dead`.
- `enqueue_in_tx` on the PostgreSQL backend for the transactional outbox
  pattern — enqueue a job in the same transaction as your business logic.

### Fixed
- Removed a false "PostgreSQL support" claim from `asyncq`'s own crate
  description that had persisted since 0.1.2.
- Removed a stale `jobqueuesmith` reference from `asyncq-postgres`'s
  description.

## [0.1.2] - 2026-09-23

### Added
- `asyncq-axum` admin `Router`: `GET /queues/:name`, `GET
  /queues/:name/dlq` (paginated), `POST /queues/:name/dlq/retry-all`,
  `DELETE /queues/:name/dlq/:id`, `GET /metrics` (Prometheus text).

### Changed
- README: documented the `asyncq-axum` endpoints and updated the backends
  table.

### Fixed
- Removed a false PostgreSQL-support claim from `asyncq`'s description
  (later found to have persisted; fully removed in 0.1.3).

## [0.1.1] - 2026-09-23

### Added
- Redis backend (`asyncq-redis`) — full `Backend` trait implementation.
- README, examples, and end-to-end integration tests.
- `readme` field on the `asyncq-derive` and `asyncq-redis` manifests.

### Fixed
- Corrected `asyncq-derive`'s crate description.

## [0.1.0] - 2026-09-19

Initial release. Originally published under the name `jobqueuesmith`, then
renamed to `asyncq` before any further releases.

### Added
- `Job` / `Perform` traits, `JobContext`, `Backend` trait.
- `#[derive(Job)]` proc macro (`asyncq-derive`) with `queue`, `retries`,
  `timeout_secs`, and `kind` attributes.
- `InMemoryBackend` — full `Backend` implementation for tests; no Redis or
  PostgreSQL required. Includes `with_immediate_retries()` and
  `completed_count` / `failed_count` / `dead_count` / `pending_count`
  inspection helpers.
- `Worker`: `register::<J>()`, `run()`, and `run_once()` for infrastructure-
  free testing.
- `Queue<B>`: `enqueue` / `enqueue_in` / `enqueue_at`, `stats`, and
  dead-letter queue management.
- Exponential backoff retries: `30s * 2^attempt`, capped at 1 hour.
- `JobError::retry()` / `JobError::discard()` to control retry vs.
  dead-letter behavior.
- 13 integration tests, 12 doc tests, all passing, clippy clean.
