# asyncq

[![Crates.io](https://img.shields.io/crates/v/asyncq.svg)](https://crates.io/crates/asyncq)
[![Documentation](https://docs.rs/asyncq/badge.svg)](https://docs.rs/asyncq)
[![License](https://img.shields.io/crates/l/asyncq.svg)](LICENSE)

**Type-safe background jobs for Rust.** Define once with `#[derive(Job)]`, run anywhere.

```toml
[dependencies]
asyncq = "0.1"
asyncq-redis = "0.1"   # Redis backend (recommended for production)
# asyncq-postgres = "0.1"  # PostgreSQL backend — no Redis needed
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
```

> **Note:** `#[derive(Job)]` comes from `asyncq` directly — you do not need to add `asyncq-derive` as a separate dependency.

## Why asyncq?

Most Rust job queues copy the Tower/Service pattern — you write boilerplate for every job type. asyncq takes a different approach: **derive macros with compile-time validation**.

```rust
// asyncq — define your job, implement perform, done
#[derive(Job, Serialize, Deserialize)]
#[job(queue = "emails", retries = 3)]
struct WelcomeEmail { to: String }

impl Perform for WelcomeEmail {
    async fn perform(self, ctx: JobContext) -> JobResult {
        send_email(&self.to).await
    }
}
```

Compare to apalis (Tower-based):
```rust
// apalis — define job, define service, define layer, wire builder
struct WelcomeEmail { to: String }
async fn send_email(job: WelcomeEmail, ctx: JobContext) -> Result<(), Error> { ... }

WorkerBuilder::new("email-worker")
    .layer(RetryLayer::new(DefaultRetryPolicy))
    .layer(TimeoutLayer::new(Duration::from_secs(30)))
    .source(storage.clone())
    .build_fn(send_email)
```

**asyncq moves configuration to the type level** — retries, timeouts, and queue names are part of the job definition, not runtime wiring. Typos become compile errors.

## Quick start

```rust
use asyncq::{Job, Perform, JobContext, JobResult, Queue, Worker};
use asyncq_redis::RedisBackend;
use serde::{Serialize, Deserialize};

#[derive(Job, Serialize, Deserialize)]
#[job(queue = "emails", retries = 3)]
struct WelcomeEmail { to: String }

impl Perform for WelcomeEmail {
    async fn perform(self, _ctx: JobContext) -> JobResult {
        println!("Sending to {}", self.to);
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let backend = RedisBackend::new("redis://127.0.0.1/").await.unwrap();
    let queue = Queue::new(backend);

    // Enqueue from anywhere
    queue.enqueue(WelcomeEmail { to: "user@example.com".into() }).await.unwrap();

    // Process
    Worker::new(queue)
        .register::<WelcomeEmail>()
        .concurrency(10)
        .run()
        .await;
}
```

## Features

### State injection

Pass app state once at startup. Access via `ctx.state::<T>()` — same pattern as axum.

```rust
let pool = Arc::new(PgPool::connect("postgres://...").await?);
let queue = Queue::new(backend).with_state(Arc::clone(&pool));

impl Perform for ProcessOrder {
    async fn perform(self, ctx: JobContext) -> JobResult {
        let pool = ctx.state::<Arc<PgPool>>().expect("pool");
        sqlx::query!("UPDATE orders SET status = 'processed' WHERE id = $1", self.id)
            .execute(pool.as_ref()).await?;
        Ok(())
    }
}
```

### Scheduling

```rust
queue.enqueue(job).await?;                                    // now
queue.enqueue_in(job, Duration::from_secs(3600)).await?;      // 1 hour
queue.enqueue_at(job, Utc::now() + Duration::days(1)).await?; // tomorrow
```

### Cron scheduling

Run jobs on a recurring schedule:

```rust
use asyncq::Scheduler;

#[derive(Job, Serialize, Deserialize, Default)]
#[job(queue = "reports")]
struct DailyReport;

let scheduler = Scheduler::new(queue.clone())
    .register::<DailyReport>("0 0 9 * * *")      // every day at 9am
    .register::<WeeklyDigest>("0 0 0 * * Sun");  // every Sunday at midnight

// Run alongside your worker
tokio::select! {
    _ = scheduler.run() => {}
    _ = worker.run() => {}
}
```

Cron format: `second minute hour day-of-month month day-of-week`

### Error handling

```rust
impl Perform for ProcessPayment {
    async fn perform(self, ctx: JobContext) -> JobResult {
        match charge_card(&self.token).await {
            Ok(_) => Ok(()),
            Err(e) if e.is_retryable() => Err(JobError::retry(e)),   // retry with backoff
            Err(e) => Err(JobError::discard(e)),                     // skip to DLQ
        }
    }
}
```

Backoff is exponential: `30s × 2^attempt`, capped at 1 hour.

### Job attributes

```rust
#[derive(Job, Serialize, Deserialize)]
#[job(
    queue = "payments",    // required
    retries = 5,           // default: 10
    timeout_secs = 30,     // optional
    kind = "ProcessPay",   // optional: custom type identifier
)]
struct ProcessPayment { amount_cents: u64 }
```

### Worker options

```rust
Worker::new(queue)
    .register::<WelcomeEmail>()
    .register::<ProcessPayment>()
    .concurrency(20)                          // concurrent jobs
    .queues(["critical", "default", "low"])   // priority order
    .stuck_timeout(Duration::from_secs(120))  // requeue stuck jobs
    .run()
    .await;
```

### Graceful shutdown

```rust
worker.run_graceful(async {
    tokio::signal::ctrl_c().await.ok();
}).await;
```

### Dead-letter queue

```rust
let dead = queue.dead_jobs("payments", 50, 0).await?;
queue.retry_dead(dead[0].id).await?;           // retry one
queue.retry_all_dead("payments").await?;       // retry all
```

### Queue stats

```rust
let stats = queue.stats("emails").await?;
println!("pending={} running={} dead={}", stats.pending, stats.running, stats.dead);
```

## Backends and integrations

| Crate | Use case |
|-------|----------|
| [`asyncq-redis`](https://crates.io/crates/asyncq-redis) | Production Redis backend — fast, battle-tested |
| [`asyncq-postgres`](https://crates.io/crates/asyncq-postgres) | PostgreSQL — no Redis needed, transactional outbox |
| [`asyncq-axum`](https://crates.io/crates/asyncq-axum) | Axum admin API + live Prometheus metrics |
| [`asyncq-actix`](https://crates.io/crates/asyncq-actix) | Actix-web admin API + live Prometheus metrics |

### PostgreSQL + transactional outbox

Enqueue jobs in the same transaction as your business logic — if the transaction rolls back, so does the job.

```rust
use asyncq_postgres::PostgresBackend;

let backend = PostgresBackend::new("postgres://...").await?;
backend.migrate().await?;  // creates asyncq_jobs table

let mut tx = pool.begin().await?;
sqlx::query("INSERT INTO orders ...").execute(&mut *tx).await?;
backend.enqueue_in_tx("SendReceipt", "emails", payload, 3, Utc::now(), &mut tx).await?;
tx.commit().await?;  // job visible only after commit
```

### Admin API (asyncq-axum / asyncq-actix)

Mount admin endpoints to inspect and manage your queues in production.

**Axum:**
```rust
use asyncq_axum::admin_with_queues;
use axum::Router;

let app = Router::new()
    .nest("/admin", admin_with_queues(queue.clone(), vec!["emails".into(), "payments".into()]));
```

**Actix-web:**
```rust
use asyncq_actix::admin_with_queues;
use actix_web::{web, App};

App::new()
    .service(
        web::scope("/admin")
            .configure(admin_with_queues(queue.clone(), vec!["emails".into(), "payments".into()]))
    );
```

| Endpoint | Description |
|----------|-------------|
| `GET /queues/:name` | Queue stats (pending, running, dead) |
| `GET /queues/:name/dlq` | Dead jobs (paginated: `?limit=50&offset=0`) |
| `POST /queues/:name/dlq/retry-all` | Requeue all dead jobs |
| `DELETE /queues/:name/dlq/:id` | Retry one dead job by ID |
| `GET /metrics` | Live Prometheus gauges per queue |

**Prometheus output** (with `admin_with_queues`):
```
asyncq_jobs_pending{queue="emails"} 3
asyncq_jobs_running{queue="emails"} 1
asyncq_jobs_dead{queue="emails"} 0
asyncq_jobs_completed{queue="emails"} 142
```

## Testing

Use `InMemoryBackend` — no Redis required:

```rust
#[tokio::test]
async fn test_my_job() {
    let backend = InMemoryBackend::new().with_immediate_retries();
    let queue = Queue::new(backend.clone());

    queue.enqueue(MyJob { .. }).await.unwrap();
    Worker::new(queue).register::<MyJob>().run_once().await.unwrap();

    assert_eq!(backend.completed_count("default").await, 1);
}
```

`run_once()` processes all pending jobs and returns — no Ctrl-C needed.

## Feature flags

`asyncq` ships with the `scheduler` feature enabled by default. To opt out of the `cron` dependency (e.g., serverless environments where you never use `Scheduler`):

```toml
asyncq = { version = "0.1", default-features = false }
```

## Roadmap

- [x] **Cron scheduling** — `Scheduler::register::<Job>("0 0 9 * * *")`
- [x] **Actix-web integration** — `asyncq-actix` admin API + Prometheus metrics
- [x] **Live Prometheus metrics** — `admin_with_queues` exposes per-queue gauges
- [ ] **Unique jobs** — deduplicate by payload hash
- [ ] **Middleware** — before/after hooks
- [ ] **Rate limiting** — per-queue throttling
- [ ] **Job chains** — `JobA.then(JobB).then(JobC)`

## Changelog

### 0.1.6
- Add `asyncq-actix` — actix-web admin API with the same endpoints as `asyncq-axum`
- Add `admin_with_queues` to both axum and actix integrations — live Prometheus gauges per queue
- Make `cron` dependency optional via `scheduler` feature (default on, zero breakage)
- Improve keywords across all crates for better crates.io discovery
- Fix `asyncq-postgres` description to highlight "no Redis needed"

### 0.1.5
- Admin REST API via `asyncq-axum` — stats, DLQ, retry, metrics endpoints
- PostgreSQL backend with `SELECT FOR UPDATE SKIP LOCKED`
- `enqueue_in_tx` — transactional outbox for PostgreSQL

### 0.1.4
- Cron scheduling via `Scheduler` — `register::<Job>("0 0 9 * * *")`
- `enqueue_in`, `enqueue_at` — delayed and scheduled jobs
- Redis ZSET-based delayed job promotion

### 0.1.0
- Initial release: `#[derive(Job)]` API, Redis backend, `InMemoryBackend` for tests
- `Worker::run_once` for infrastructure-free testing
- State injection via `Queue::with_state` + `ctx.state::<T>()`
- DLQ with `retry_dead` / `retry_all_dead`

## License

MIT OR Apache-2.0
