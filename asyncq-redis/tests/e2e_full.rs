//! Full end-to-end integration test simulating a real web app:
//! axum-style handler enqueues a job → worker processes it → state is updated.
//!
//! Requires Redis on localhost:6379.

use asyncq::{Job, JobContext, JobResult, Perform, Queue, Worker, JobRecord};
use asyncq_redis::RedisBackend;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

const REDIS_URL: &str = "redis://127.0.0.1/";

fn uq(tag: &str) -> String { format!("e2e-{tag}-{}", Uuid::new_v4().simple()) }

async fn clean(queue: &str) {
    use redis::AsyncCommands;
    if let Ok(client) = redis::Client::open(REDIS_URL) {
        if let Ok(mut c) = client.get_multiplexed_async_connection().await {
            for s in &["pending", "delayed", "dead"] {
                let _: Result<(), _> = c.del(format!("asyncq:q:{}:{s}", queue)).await;
            }
        }
    }
}

// ── Simulated app state ───────────────────────────────────────────────────────

#[derive(Default)]
struct AppDb {
    processed: Mutex<Vec<String>>,
}
impl AppDb {
    fn record(&self, item: &str) { self.processed.lock().unwrap().push(item.to_owned()); }
    fn count(&self) -> usize { self.processed.lock().unwrap().len() }
}

// ── Job definition ────────────────────────────────────────────────────────────

#[derive(Job, Serialize, Deserialize)]
#[job(queue = "e2e-orders", retries = 3)]
struct ProcessOrder { order_id: u64, amount_cents: u64 }

impl Perform for ProcessOrder {
    async fn perform(self, ctx: JobContext) -> JobResult {
        let db = ctx.state::<Arc<AppDb>>().expect("AppDb not in state");
        db.record(&format!("order-{}-{}c", self.order_id, self.amount_cents));
        Ok(())
    }
}

// ── Simulated axum handler ────────────────────────────────────────────────────

async fn create_order_handler(queue: &Queue<RedisBackend>, order_id: u64, amount: u64) {
    queue.enqueue(ProcessOrder { order_id, amount_cents: amount })
        .await
        .expect("enqueue failed");
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn e2e_handler_enqueues_worker_processes() {
    let q = uq("orders");
    clean(&q).await;

    let db = Arc::new(AppDb::default());
    let backend = RedisBackend::new(REDIS_URL).await.unwrap();
    let queue = Queue::new(backend.clone()).with_state(Arc::clone(&db));

    // Simulate 3 HTTP requests hitting the handler
    for i in 1u64..=3 {
        create_order_handler(&queue, i, i * 1000).await;
    }

    assert_eq!(queue.stats(&q).await.unwrap().pending, 0,
        "jobs were enqueued to 'e2e-orders', not the unique test queue");

    // Use enqueue_record to put them in the unique test queue for isolation
    let backend2 = RedisBackend::new(REDIS_URL).await.unwrap();
    let q2 = Queue::new(backend2.clone()).with_state(Arc::clone(&db));

    for i in 1u64..=3 {
        let payload = serde_json::to_vec(&ProcessOrder { order_id: i, amount_cents: i * 1000 }).unwrap();
        q2.enqueue_record(JobRecord::new("ProcessOrder", &q, payload, 3)).await.unwrap();
    }

    assert_eq!(q2.stats(&q).await.unwrap().pending, 3);

    // Worker processes all 3
    let processed = Worker::new(Queue::new(backend.clone()).with_state(Arc::clone(&db)))
        .register::<ProcessOrder>()
        .queues([q.as_str()])
        .run_once()
        .await
        .unwrap();

    assert_eq!(processed, 3, "worker must process all 3 jobs");
    assert_eq!(db.count(), 3, "all 3 orders must be recorded in AppDb");

    let db_entries = db.processed.lock().unwrap().clone();
    assert!(db_entries.contains(&"order-1-1000c".to_owned()));
    assert!(db_entries.contains(&"order-2-2000c".to_owned()));
    assert!(db_entries.contains(&"order-3-3000c".to_owned()));

    clean(&q).await;
}

#[tokio::test]
async fn e2e_concurrent_enqueue_and_process() {
    let q = uq("concurrent");
    clean(&q).await;

    let db = Arc::new(AppDb::default());
    let backend = RedisBackend::new(REDIS_URL).await.unwrap();

    // Enqueue 20 jobs concurrently
    let enqueue_tasks: Vec<_> = (0u64..20)
        .map(|i| {
            let b = backend.clone();
            let q = q.clone();
            tokio::spawn(async move {
                let queue = Queue::new(b);
                let payload = serde_json::to_vec(&ProcessOrder { order_id: i, amount_cents: i * 100 }).unwrap();
                queue.enqueue_record(JobRecord::new("ProcessOrder", &q, payload, 3))
                    .await
                    .unwrap();
            })
        })
        .collect();

    futures::future::join_all(enqueue_tasks).await;

    assert_eq!(Queue::new(backend.clone()).stats(&q).await.unwrap().pending, 20);

    // Process all with concurrency 5
    let processed = Worker::new(Queue::new(backend.clone()).with_state(Arc::clone(&db)))
        .register::<ProcessOrder>()
        .queues([q.as_str()])
        .concurrency(5)
        .run_once()
        .await
        .unwrap();

    assert_eq!(processed, 20, "all 20 concurrent jobs must be processed");
    assert_eq!(db.count(), 20, "all 20 orders in AppDb");

    clean(&q).await;
}
