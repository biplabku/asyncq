use std::time::Duration;
use chrono::{DateTime, Utc};
use async_trait::async_trait;

use crate::{
    error::Result,
    job::{JobId, JobRecord, QueueStats},
};

/// Storage backend for job persistence.
///
/// You use a concrete backend — `InMemoryBackend`, `RedisBackend`, or `PostgresBackend` —
/// and pass it to [`Queue::new`](crate::Queue::new). You do not implement this trait directly.
///
/// See [`InMemoryBackend`](crate::backends::InMemoryBackend) for testing without
/// external infrastructure.
#[async_trait]
pub trait Backend: Send + Sync + 'static {
    /// Persist a job record for future processing.
    async fn enqueue(&self, record: JobRecord) -> Result<JobId>;

    /// Claim the next available job from any of the given queues.
    ///
    /// Blocks until a job is available or `timeout` expires.
    /// Returns `None` on timeout.
    async fn claim(
        &self,
        queues: &[&str],
        timeout: Duration,
    ) -> Result<Option<JobRecord>>;

    /// Mark a job as successfully completed.
    async fn ack(&self, id: JobId) -> Result<()>;

    /// Mark a job as failed.
    ///
    /// If `retry_at` is `Some`, the job is rescheduled. If `None`, the job
    /// is moved to the dead-letter queue.
    async fn nack(
        &self,
        id: JobId,
        error: &str,
        retry_at: Option<DateTime<Utc>>,
    ) -> Result<()>;

    /// Update the heartbeat timestamp for a running job.
    ///
    /// Called periodically by the worker to prevent the job from being
    /// reclaimed by another worker if it's taking a long time.
    async fn heartbeat(&self, id: JobId) -> Result<()>;

    /// Reset jobs that have been running longer than `older_than` back to pending.
    ///
    /// Called automatically each worker cycle to recover jobs from crashed workers.
    async fn reap_stuck(&self, older_than: Duration) -> Result<u64>;

    /// Statistics for a single queue.
    async fn stats(&self, queue: &str) -> Result<QueueStats>;

    /// Paginated list of dead-lettered jobs for a queue.
    async fn dead_jobs(
        &self,
        queue: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<JobRecord>>;

    /// Requeue a single dead-lettered job for another delivery attempt.
    async fn retry_dead(&self, id: JobId) -> Result<()>;

    /// Requeue all dead-lettered jobs for a queue.
    ///
    /// Returns the number of jobs requeued.
    async fn retry_all_dead(&self, queue: &str) -> Result<u64>;
}
