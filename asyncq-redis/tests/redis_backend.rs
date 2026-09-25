//! E2E tests against a real Redis instance (localhost:6379).
//! Requires Redis running. Each test uses a unique queue name to prevent
//! parallel-test interference.

use asyncq::{Job, JobContext, JobError, JobResult, JobRecord, Perform, Queue, Worker};
use asyncq_redis::RedisBackend;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use uuid::Uuid;
use chrono::{Utc, Duration as ChronoDur};

const REDIS_URL: &str = "redis://127.0.0.1/";

/// Unique queue prefix per test call — no parallel interference.
fn uq(tag: &str) -> String { format!("t-{tag}-{}", Uuid::new_v4().simple()) }

async fn backend() -> RedisBackend {
    RedisBackend::new(REDIS_URL).await.expect("Redis must be running on localhost:6379")
}

/// Best-effort cleanup of all asyncq keys for a queue.
async fn cleanup(queue: &str) {
    use redis::AsyncCommands;
    if let Ok(client) = redis::Client::open(REDIS_URL) {
        if let Ok(mut c) = client.get_multiplexed_async_connection().await {
            for suffix in &["pending", "delayed", "dead"] {
                let _: Result<(), _> = c.del(format!("asyncq:q:{}:{}", queue, suffix)).await;
            }
        }
    }
}

fn raw_payload<T: serde::Serialize>(v: &T) -> Vec<u8> {
    serde_json::to_vec(v).unwrap()
}

// ── Test jobs ─────────────────────────────────────────────────────────────────

/// Generic job that just succeeds — queue name set at test time via JobRecord.
#[derive(Job, Serialize, Deserialize)]
#[job(queue = "t-simple", retries = 3)]
struct OkJob { value: u32 }
impl Perform for OkJob {
    async fn perform(self, _ctx: JobContext) -> JobResult { Ok(()) }
}

/// Always discards — never retried.
#[derive(Job, Serialize, Deserialize)]
#[job(queue = "t-discard", retries = 3)]
struct DiscardJob;
impl Perform for DiscardJob {
    async fn perform(self, _ctx: JobContext) -> JobResult {
        Err(JobError::discard("permanent"))
    }
}

/// Increments a shared counter via state injection.
#[derive(Job, Serialize, Deserialize)]
#[job(queue = "t-state", retries = 1)]
struct CounterJob;
impl Perform for CounterJob {
    async fn perform(self, ctx: JobContext) -> JobResult {
        *ctx.state::<Arc<Mutex<u32>>>()
            .expect("counter not in state")
            .lock()
            .unwrap() += 1;
        Ok(())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn redis_connect() {
    let _b = backend().await; // just tests the connection
}

#[tokio::test]
async fn redis_enqueue_and_process() {
    let q = uq("ok");
    cleanup(&q).await;

    let b = backend().await;
    let queue = Queue::new(b.clone());

    let record = JobRecord::new("OkJob", &q, raw_payload(&OkJob { value: 7 }), 3);
    queue.enqueue_record(record).await.unwrap();

    assert_eq!(queue.stats(&q).await.unwrap().pending, 1);

    Worker::new(Queue::new(b.clone()))
        .queues([q.as_str()])
        .run_once()
        .await
        .unwrap();

    assert_eq!(queue.stats(&q).await.unwrap().pending, 0,
        "job must be acked and removed after processing");

    cleanup(&q).await;
}

#[tokio::test]
async fn redis_multiple_jobs() {
    let q = uq("multi");
    cleanup(&q).await;

    let b = backend().await;
    let queue = Queue::new(b.clone());

    for i in 0..5u32 {
        queue.enqueue_record(JobRecord::new("OkJob", &q, raw_payload(&OkJob { value: i }), 3))
            .await.unwrap();
    }

    let n = Worker::new(Queue::new(b.clone()))
        .queues([q.as_str()])
        .run_once()
        .await
        .unwrap();

    assert_eq!(n, 5, "all 5 jobs must be processed in one run_once cycle");
    cleanup(&q).await;
}

#[tokio::test]
async fn redis_delayed_not_immediately_claimable() {
    let q = uq("delayed");
    cleanup(&q).await;

    let b = backend().await;
    let queue = Queue::new(b.clone());

    let record = JobRecord::new("OkJob", &q, raw_payload(&OkJob { value: 1 }), 3)
        .with_scheduled_at(Utc::now() + ChronoDur::seconds(60));
    queue.enqueue_record(record).await.unwrap();

    let n = Worker::new(Queue::new(b.clone()))
        .queues([q.as_str()])
        .run_once()
        .await
        .unwrap();

    assert_eq!(n, 0, "future-scheduled job must not be processed now");

    cleanup(&q).await;
}

#[tokio::test]
async fn redis_discard_goes_to_dlq() {
    let q = uq("discard");
    cleanup(&q).await;

    let b = backend().await;
    let queue = Queue::new(b.clone());

    let record = JobRecord::new("DiscardJob", &q, raw_payload(&DiscardJob), 3);
    queue.enqueue_record(record).await.unwrap();

    Worker::new(Queue::new(b.clone()))
        .queues([q.as_str()])
        .run_once()
        .await
        .unwrap();

    let dead = queue.dead_jobs(&q, 10, 0).await.unwrap();
    assert_eq!(dead.len(), 1, "discarded job must land in DLQ");

    cleanup(&q).await;
}

#[tokio::test]
async fn redis_retry_dead_requeues() {
    let q = uq("retry-dead");
    cleanup(&q).await;

    let b = backend().await;
    let queue = Queue::new(b.clone());

    let record = JobRecord::new("DiscardJob", &q, raw_payload(&DiscardJob), 3);
    let id = queue.enqueue_record(record).await.unwrap();

    Worker::new(Queue::new(b.clone()))
        .queues([q.as_str()])
        .run_once()
        .await
        .unwrap();

    assert_eq!(queue.dead_jobs(&q, 10, 0).await.unwrap().len(), 1,
        "job must be in DLQ after discard");

    queue.retry_dead(id).await.unwrap();

    assert_eq!(queue.dead_jobs(&q, 10, 0).await.unwrap().len(), 0,
        "DLQ must be empty after retry");
    assert_eq!(queue.stats(&q).await.unwrap().pending, 1,
        "retried job must be back in pending");

    cleanup(&q).await;
}

#[tokio::test]
async fn redis_retry_all_dead() {
    let q = uq("retry-all");
    cleanup(&q).await;

    let b = backend().await;
    let queue = Queue::new(b.clone());

    for _ in 0..3 {
        let record = JobRecord::new("DiscardJob", &q, raw_payload(&DiscardJob), 3);
        queue.enqueue_record(record).await.unwrap();
    }

    Worker::new(Queue::new(b.clone()))
        .queues([q.as_str()])
        .run_once()
        .await
        .unwrap();

    assert_eq!(queue.dead_jobs(&q, 10, 0).await.unwrap().len(), 3);

    let requeued = queue.retry_all_dead(&q).await.unwrap();
    assert_eq!(requeued, 3);
    assert_eq!(queue.dead_jobs(&q, 10, 0).await.unwrap().len(), 0);
    assert_eq!(queue.stats(&q).await.unwrap().pending, 3);

    cleanup(&q).await;
}

#[tokio::test]
async fn redis_state_injected() {
    let q = uq("state");
    cleanup(&q).await;

    let counter = Arc::new(Mutex::new(0u32));
    let b = backend().await;

    let queue = Queue::new(b.clone()).with_state(Arc::clone(&counter));
    // Use enqueue_record with the unique queue name
    let record = JobRecord::new("CounterJob", &q, raw_payload(&CounterJob), 1);
    queue.enqueue_record(record).await.unwrap();

    Worker::new(Queue::new(b.clone()).with_state(Arc::clone(&counter)))
        .register::<CounterJob>()
        .queues([q.as_str()])
        .run_once()
        .await
        .unwrap();

    assert_eq!(*counter.lock().unwrap(), 1,
        "state counter must be incremented by job");

    cleanup(&q).await;
}

#[tokio::test]
async fn redis_stats_reflect_queue_state() {
    let q = uq("stats");
    cleanup(&q).await;

    let b = backend().await;
    let queue = Queue::new(b.clone());

    // 0 pending initially
    assert_eq!(queue.stats(&q).await.unwrap().pending, 0);

    let record = JobRecord::new("OkJob", &q, raw_payload(&OkJob { value: 1 }), 3);
    queue.enqueue_record(record).await.unwrap();

    assert_eq!(queue.stats(&q).await.unwrap().pending, 1);
    assert_eq!(queue.stats(&q).await.unwrap().dead, 0);

    cleanup(&q).await;
}

#[tokio::test]
async fn redis_completed_counter_increments_on_success() {
    let q = uq("completed");
    cleanup(&q).await;
    // Also clean the completed counter key
    {
        use redis::AsyncCommands;
        if let Ok(client) = redis::Client::open(REDIS_URL) {
            if let Ok(mut c) = client.get_multiplexed_async_connection().await {
                let _: Result<(), _> = c.del(format!("asyncq:q:{}:completed", q)).await;
            }
        }
    }

    let b = backend().await;
    let queue = Queue::new(b.clone());

    assert_eq!(queue.stats(&q).await.unwrap().completed, 0, "starts at 0");

    // Enqueue and process 3 jobs that all succeed
    for i in 0..3u32 {
        let record = JobRecord::new("OkJob", &q, raw_payload(&OkJob { value: i }), 3);
        queue.enqueue_record(record).await.unwrap();
    }

    Worker::new(queue.clone())
        .register::<OkJob>()
        .queues([q.as_str()])
        .run_once()
        .await
        .unwrap();

    let stats = queue.stats(&q).await.unwrap();
    assert_eq!(stats.completed, 3, "completed must equal number of successful jobs");
    assert_eq!(stats.pending, 0);

    cleanup(&q).await;
}

#[tokio::test]
async fn redis_completed_counter_not_incremented_on_discard() {
    let q = uq("completed-discard");
    cleanup(&q).await;
    {
        use redis::AsyncCommands;
        if let Ok(client) = redis::Client::open(REDIS_URL) {
            if let Ok(mut c) = client.get_multiplexed_async_connection().await {
                let _: Result<(), _> = c.del(format!("asyncq:q:{}:completed", q)).await;
            }
        }
    }

    let b = backend().await;
    let queue = Queue::new(b.clone());

    let record = JobRecord::new("DiscardJob", &q, raw_payload(&DiscardJob), 1);
    queue.enqueue_record(record).await.unwrap();

    Worker::new(queue.clone())
        .register::<DiscardJob>()
        .queues([q.as_str()])
        .run_once()
        .await
        .unwrap();

    let stats = queue.stats(&q).await.unwrap();
    assert_eq!(stats.completed, 0, "discarded jobs must not increment completed");
    assert_eq!(stats.dead, 1, "discarded job must be in DLQ");

    cleanup(&q).await;
}
