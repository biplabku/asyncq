//! Regression: a job that runs longer than `stuck_timeout` must not be reaped
//! and executed a second time while it is still running.

use asyncq::{backends::InMemoryBackend, Job, JobContext, JobResult, Perform, Queue, Worker};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

static RUNS: AtomicU32 = AtomicU32::new(0);

#[derive(Job, Serialize, Deserialize)]
#[job(queue = "long", retries = 1)]
struct LongJob;

impl Perform for LongJob {
    async fn perform(self, _ctx: JobContext) -> JobResult {
        RUNS.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(1500)).await;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn long_job_is_not_reaped_while_running() {
    let queue = Queue::new(InMemoryBackend::new());
    queue.enqueue(LongJob).await.unwrap();

    let worker = Worker::new(queue)
        .register::<LongJob>()
        .queues(["long"])
        .concurrency(4)
        .poll_timeout(Duration::from_millis(50))
        .stuck_timeout(Duration::from_millis(300));
    let handle = tokio::spawn(async move { worker.run().await });

    tokio::time::sleep(Duration::from_millis(2500)).await;
    handle.abort();

    assert_eq!(RUNS.load(Ordering::SeqCst), 1, "long job was executed more than once");
}
