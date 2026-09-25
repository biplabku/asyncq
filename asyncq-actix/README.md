# asyncq-actix

Actix-web integration for [asyncq](https://crates.io/crates/asyncq) — admin REST API with Prometheus metrics and DLQ management.

```toml
[dependencies]
asyncq-actix = "0.1"
asyncq = "0.1"
asyncq-redis = "0.1"  # or asyncq-postgres
```

## Usage

```rust
use asyncq::{Queue, Worker};
use asyncq_redis::RedisBackend;
use asyncq_actix::admin_with_queues;
use actix_web::{web, App, HttpServer};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let backend = RedisBackend::new("redis://127.0.0.1/").await.unwrap();
    let queue = Queue::new(backend);

    // Start worker in background
    let worker_queue = queue.clone();
    tokio::spawn(async move {
        Worker::new(worker_queue)
            .register::<MyJob>()
            .run()
            .await;
    });

    HttpServer::new(move || {
        App::new()
            .service(
                web::scope("/admin")
                    .configure(admin_with_queues(
                        queue.clone(),
                        vec!["emails".into(), "payments".into()],
                    ))
            )
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
```

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/admin/queues/{name}` | Queue stats (pending, running, dead) |
| GET | `/admin/queues/{name}/dlq` | Dead-letter queue (paginated) |
| POST | `/admin/queues/{name}/dlq/retry-all` | Requeue all dead jobs |
| DELETE | `/admin/queues/{name}/dlq/{id}` | Retry one dead job by ID |
| GET | `/admin/metrics` | Prometheus text format |

## Prometheus metrics

With `admin_with_queues`, `/admin/metrics` exposes live gauges:

```
asyncq_jobs_pending{queue="emails"} 3
asyncq_jobs_running{queue="emails"} 1
asyncq_jobs_dead{queue="emails"} 0
asyncq_jobs_completed{queue="emails"} 142
```

For axum users, see [asyncq-axum](https://crates.io/crates/asyncq-axum).
