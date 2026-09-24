# asyncq

Async background job processing for Rust. Like Sidekiq, but with a derive macro.

```toml
[dependencies]
asyncq = "0.1"
asyncq-redis = "0.1"
serde = { version = "1", features = ["derive"] }
```

## Why asyncq?

Every other Rust job queue makes you write boilerplate for every job type. asyncq uses a derive macro instead:

```rust
// asyncq — one attribute, done
#[derive(Job, Serialize, Deserialize)]
#[job(queue = "emails", retries = 3)]
struct WelcomeEmail { email: String }

// apalis — tower::Service boilerplate per job type
WorkerBuilder::new("email-worker")
    .layer(RetryLayer::new(DefaultRetryPolicy))
    .source(pg.clone())
    .build_fn(email_service)
```

## Quick start

```rust
use asyncq::{Job, Perform, JobContext, JobResult, Queue, Worker};
use asyncq_redis::RedisBackend;
use serde::{Serialize, Deserialize};

// 1. Define your job
#[derive(Job, Serialize, Deserialize)]
#[job(queue = "emails", retries = 3)]
struct WelcomeEmail {
    email: String,
}

// 2. Implement the work
impl Perform for WelcomeEmail {
    async fn perform(self, _ctx: JobContext) -> JobResult {
        println!("Sending welcome email to {}", self.email);
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    // 3. Connect to Redis
    let backend = RedisBackend::new("redis://127.0.0.1/").await.unwrap();
    let queue = Queue::new(backend);

    // 4. Enqueue from anywhere (axum handler, CLI, cron, etc.)
    queue.enqueue(WelcomeEmail { email: "user@example.com".into() }).await.unwrap();

    // 5. Process — register all job types once, then run
    Worker::new(queue)
        .register::<WelcomeEmail>()
        .concurrency(10)
        .run()         // blocks; use run_graceful(ctrl_c()) for clean shutdown
        .await;
}
```

## Shared state (database pool, config, etc.)

Pass your app state once at startup. Jobs access it via `ctx.state::<T>()` — the same pattern as axum's `State<T>`.

```rust
use std::sync::Arc;
use sqlx::PgPool;

let pool = Arc::new(PgPool::connect("postgres://...").await?);

// Attach state to the queue
let queue = Queue::new(backend).with_state(Arc::clone(&pool));

// Access it inside any job
impl Perform for SendInvoice {
    async fn perform(self, ctx: JobContext) -> JobResult {
        let pool = ctx.state::<Arc<PgPool>>().expect("pool not in state");
        sqlx::query!("INSERT INTO invoices ...").execute(pool.as_ref()).await?;
        Ok(())
    }
}
```

## Scheduling

```rust
use std::time::Duration;
use chrono::Utc;

// Run immediately
queue.enqueue(MyJob { .. }).await?;

// Run after a delay
queue.enqueue_in(ReminderJob { .. }, Duration::from_secs(3600)).await?;

// Run at a specific time
queue.enqueue_at(ReportJob { .. }, Utc::now() + chrono::Duration::days(1)).await?;
```

## Error handling

Return `Ok(())` to mark a job complete. Return `Err` to retry. Return `JobError::discard` to skip retries and send directly to the dead-letter queue.

```rust
use asyncq::{JobError, JobResult};

impl Perform for ProcessPayment {
    async fn perform(self, ctx: JobContext) -> JobResult {
        match charge_card(&self.card_token).await {
            Ok(_) => Ok(()),
            Err(e) if e.is_retryable() => Err(JobError::retry(e)),
            Err(e) => Err(JobError::discard(e)),  // permanent failure → DLQ
        }
    }
}
```

Retry backoff is exponential: `30s × 2^attempt`, capped at 1 hour.

## Job attributes

```rust
#[derive(Job, Serialize, Deserialize)]
#[job(
    queue = "payments",       // required: which queue to enqueue to
    retries = 5,              // default: 10
    timeout_secs = 30,        // optional: per-job execution timeout
    kind = "MyCustomKind",    // optional: override the job type identifier
)]
struct ProcessPayment {
    amount_cents: u64,
}
```

## Worker options

```rust
Worker::new(queue)
    .register::<WelcomeEmail>()
    .register::<ProcessPayment>()
    .register::<GenerateReport>()
    .concurrency(20)                          // concurrent jobs (default: 10)
    .queues(["high", "default", "low"])       // explicit priority order
    .stuck_timeout(Duration::from_secs(120)) // reset jobs stuck > 120s (default)
    .run()
    .await;
```

## Graceful shutdown

```rust
// Stop cleanly on Ctrl-C — current batch completes before exit
worker.run_graceful(async {
    tokio::signal::ctrl_c().await.ok();
}).await;
```

## Dead-letter queue

```rust
// List dead jobs
let dead = queue.dead_jobs("payments", 50, 0).await?;

// Retry one
queue.retry_dead(dead[0].id).await?;

// Retry all
let requeued = queue.retry_all_dead("payments").await?;
println!("Requeued {} jobs", requeued);
```

## Queue stats

```rust
let stats = queue.stats("emails").await?;
println!("pending={} running={} dead={}", stats.pending, stats.running, stats.dead);
```

## Admin UI (asyncq-axum)

Mount the admin router to get REST endpoints and Prometheus metrics:

```toml
[dependencies]
asyncq-axum = "0.1"
```

```rust
use asyncq_axum::admin;
use axum::Router;

let app = Router::new()
    .nest("/admin", admin(queue.clone()))
    /* ... your routes ... */;
```

| Method | Path | Description |
|--------|------|-------------|
| GET | `/admin/queues/:name` | Stats (pending, running, dead) |
| GET | `/admin/queues/:name/dlq` | List dead-letter jobs (paginated) |
| POST | `/admin/queues/:name/dlq/retry-all` | Requeue all dead jobs |
| DELETE | `/admin/queues/:name/dlq/:id` | Retry one dead job |
| GET | `/admin/metrics` | Prometheus text format |

## Backends

| Crate | Backend | Status |
|-------|---------|--------|
| `asyncq-redis` | Redis | ✅ v0.1.0 |
| `asyncq-axum` | Admin router + Prometheus | ✅ v0.1.0 |
| `asyncq-postgres` | PostgreSQL | 🔜 v0.2.0 |

## Testing without Redis

Use `InMemoryBackend` in tests — no external infrastructure required:

```rust
use asyncq::{Queue, Worker, backends::InMemoryBackend};

#[tokio::test]
async fn test_my_job() {
    let backend = InMemoryBackend::new().with_immediate_retries();
    let queue = Queue::new(backend.clone());

    queue.enqueue(WelcomeEmail { email: "test@test.com".into() }).await.unwrap();

    Worker::new(queue)
        .register::<WelcomeEmail>()
        .run_once()   // process all pending jobs and return — no Ctrl-C needed
        .await
        .unwrap();

    assert_eq!(backend.completed_count("emails").await, 1);
    assert_eq!(backend.failed_count("emails").await, 0);
}
```

`with_immediate_retries()` makes retried jobs immediately claimable, so you can test retry cycles without sleeping.

## Running examples

```bash
# Start Redis
docker run -d -p 6379:6379 redis:7-alpine

# Basic example
cargo run --example basic

# With state (shared DB pool pattern)
cargo run --example with_state
```

## License

MIT OR Apache-2.0
