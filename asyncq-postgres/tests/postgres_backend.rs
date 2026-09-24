//! E2E tests against a real PostgreSQL instance.
//! Requires: postgres://hooksmith:hooksmith@localhost/hooksmith
//! Each test uses a unique queue name to avoid parallel interference.

use asyncq::{Job, JobContext, JobError, JobResult, JobRecord, Perform, Queue, Worker};
use asyncq_postgres::PostgresBackend;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

const DB_URL: &str = "postgres://hooksmith:hooksmith@localhost/hooksmith";

fn uq(tag: &str) -> String { format!("pgtest-{tag}-{}", Uuid::new_v4().simple()) }

async fn backend() -> PostgresBackend {
    let b = PostgresBackend::new(DB_URL).await.expect("Postgres must be running");
    b.migrate().await.expect("migration failed");
    b
}

async fn cleanup(pool: &sqlx::PgPool, queue: &str) {
    let _ = sqlx::query("DELETE FROM asyncq_jobs WHERE queue = $1")
        .bind(queue).execute(pool).await;
}

fn payload<T: serde::Serialize>(v: &T) -> Vec<u8> { serde_json::to_vec(v).unwrap() }

// ── Test job types ────────────────────────────────────────────────────────────

#[derive(Job, Serialize, Deserialize)]
#[job(queue = "pg-ok", retries = 3)]
struct PgOkJob { value: u32 }
impl Perform for PgOkJob {
    async fn perform(self, _ctx: JobContext) -> JobResult { Ok(()) }
}

#[derive(Job, Serialize, Deserialize)]
#[job(queue = "pg-discard", retries = 3)]
struct PgDiscardJob;
impl Perform for PgDiscardJob {
    async fn perform(self, _ctx: JobContext) -> JobResult {
        Err(JobError::discard("permanent"))
    }
}

#[derive(Job, Serialize, Deserialize)]
#[job(queue = "pg-state", retries = 1)]
struct PgStateJob;
impl Perform for PgStateJob {
    async fn perform(self, ctx: JobContext) -> JobResult {
        *ctx.state::<Arc<Mutex<u32>>>().expect("counter").lock().unwrap() += 1;
        Ok(())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn pg_connect_and_migrate() {
    let _b = backend().await;
}

#[tokio::test]
async fn pg_enqueue_and_process() {
    let b = backend().await;
    let q = uq("ok");
    let queue = Queue::new(b.clone());
    cleanup(b.pool_ref(), &q).await;

    queue.enqueue_record(JobRecord::new("PgOkJob", &q, payload(&PgOkJob { value: 1 }), 3))
        .await.unwrap();

    assert_eq!(queue.stats(&q).await.unwrap().pending, 1, "one job pending");

    Worker::new(Queue::new(b.clone()))
        .register::<PgOkJob>()
        .queues([q.as_str()])
        .run_once()
        .await.unwrap();

    assert_eq!(queue.stats(&q).await.unwrap().pending, 0, "no pending after process");
    assert_eq!(queue.stats(&q).await.unwrap().completed, 1, "one completed");

    cleanup(b.pool_ref(), &q).await;
}

#[tokio::test]
async fn pg_multiple_jobs() {
    let b = backend().await;
    let q = uq("multi");
    let queue = Queue::new(b.clone());
    cleanup(b.pool_ref(), &q).await;

    for i in 0..5u32 {
        queue.enqueue_record(JobRecord::new("PgOkJob", &q, payload(&PgOkJob { value: i }), 3))
            .await.unwrap();
    }

    let n = Worker::new(Queue::new(b.clone()))
        .register::<PgOkJob>()
        .queues([q.as_str()])
        .run_once().await.unwrap();

    assert_eq!(n, 5, "all 5 processed");
    assert_eq!(queue.stats(&q).await.unwrap().completed, 5);
    cleanup(b.pool_ref(), &q).await;
}

#[tokio::test]
async fn pg_discard_goes_to_dlq() {
    let b = backend().await;
    let q = uq("discard");
    let queue = Queue::new(b.clone());
    cleanup(b.pool_ref(), &q).await;

    queue.enqueue_record(JobRecord::new("PgDiscardJob", &q, payload(&PgDiscardJob), 3))
        .await.unwrap();

    Worker::new(Queue::new(b.clone()))
        .register::<PgDiscardJob>()
        .queues([q.as_str()])
        .run_once().await.unwrap();

    let dead = queue.dead_jobs(&q, 10, 0).await.unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].last_error.as_deref(), Some("permanent"));

    cleanup(b.pool_ref(), &q).await;
}

#[tokio::test]
async fn pg_retry_dead() {
    let b = backend().await;
    let q = uq("retry");
    let queue = Queue::new(b.clone());
    cleanup(b.pool_ref(), &q).await;

    let id = queue.enqueue_record(JobRecord::new("PgDiscardJob", &q, payload(&PgDiscardJob), 3))
        .await.unwrap();
    Worker::new(Queue::new(b.clone()))
        .register::<PgDiscardJob>()
        .queues([q.as_str()])
        .run_once().await.unwrap();
    assert_eq!(queue.dead_jobs(&q, 10, 0).await.unwrap().len(), 1);

    queue.retry_dead(id).await.unwrap();
    assert_eq!(queue.dead_jobs(&q, 10, 0).await.unwrap().len(), 0);
    assert_eq!(queue.stats(&q).await.unwrap().pending, 1);

    cleanup(b.pool_ref(), &q).await;
}

#[tokio::test]
async fn pg_retry_all_dead() {
    let b = backend().await;
    let q = uq("retry-all");
    let queue = Queue::new(b.clone());
    cleanup(b.pool_ref(), &q).await;

    for _ in 0..3 {
        queue.enqueue_record(JobRecord::new("PgDiscardJob", &q, payload(&PgDiscardJob), 3))
            .await.unwrap();
    }
    Worker::new(Queue::new(b.clone()))
        .register::<PgDiscardJob>()
        .queues([q.as_str()])
        .run_once().await.unwrap();
    assert_eq!(queue.dead_jobs(&q, 10, 0).await.unwrap().len(), 3);

    let n = queue.retry_all_dead(&q).await.unwrap();
    assert_eq!(n, 3);
    assert_eq!(queue.stats(&q).await.unwrap().pending, 3);

    cleanup(b.pool_ref(), &q).await;
}

#[tokio::test]
async fn pg_state_injection() {
    let b = backend().await;
    let q = uq("state");
    let counter = Arc::new(Mutex::new(0u32));
    let queue = Queue::new(b.clone()).with_state(Arc::clone(&counter));
    cleanup(b.pool_ref(), &q).await;

    queue.enqueue_record(JobRecord::new("PgStateJob", &q, payload(&PgStateJob), 1))
        .await.unwrap();

    Worker::new(Queue::new(b.clone()).with_state(Arc::clone(&counter)))
        .register::<PgStateJob>()
        .queues([q.as_str()])
        .run_once().await.unwrap();

    assert_eq!(*counter.lock().unwrap(), 1);
    cleanup(b.pool_ref(), &q).await;
}

#[tokio::test]
async fn pg_stats_accurate() {
    let b = backend().await;
    let q = uq("stats");
    let queue = Queue::new(b.clone());
    cleanup(b.pool_ref(), &q).await;

    assert_eq!(queue.stats(&q).await.unwrap().pending, 0);
    queue.enqueue_record(JobRecord::new("PgOkJob", &q, payload(&PgOkJob { value: 1 }), 3))
        .await.unwrap();
    assert_eq!(queue.stats(&q).await.unwrap().pending, 1);
    assert_eq!(queue.stats(&q).await.unwrap().dead, 0);

    cleanup(b.pool_ref(), &q).await;
}
