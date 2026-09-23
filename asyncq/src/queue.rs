use std::sync::Arc;
use std::any::Any;
use std::time::Duration;
use chrono::{DateTime, Utc};

use crate::{
    backend::Backend,
    error::Result,
    job::{Job, JobId, JobRecord, Perform, QueueStats},
};

/// The main entry point for enqueueing jobs.
///
/// Create one `Queue` per application and share it (it's `Clone`).
/// Pass it to your axum `State`, actix `Data`, or any shared state container.
///
/// # Quick start
///
/// ```rust,no_run
/// use asyncq::{Queue, backends::InMemoryBackend};
///
/// let backend = InMemoryBackend::new();
/// let queue = Queue::new(backend);
/// ```
///
/// # With shared state (database pool, config, etc.)
///
/// ```rust,no_run
/// # use asyncq::{Queue, backends::InMemoryBackend};
/// # use std::sync::Arc;
/// # let my_db_pool = 42u32;
/// let queue = Queue::new(InMemoryBackend::new())
///     .with_state(Arc::new(my_db_pool));
/// ```
#[derive(Clone)]
pub struct Queue<B: Backend> {
    pub(crate) backend: Arc<B>,
    pub(crate) state: Arc<dyn Any + Send + Sync>,
}

impl<B: Backend> Queue<B> {
    /// Create a new queue backed by `backend`.
    pub fn new(backend: B) -> Self {
        Self {
            backend: Arc::new(backend),
            state: Arc::new(()),
        }
    }

    /// Attach shared state that will be injected into every job's [`JobContext`](crate::JobContext).
    ///
    /// Jobs retrieve state with `ctx.state::<T>()`.
    ///
    /// ```rust,no_run
    /// # use asyncq::{Queue, backends::InMemoryBackend};
    /// # use std::sync::Arc;
    /// # let db_pool = 42u32;
    /// let queue = Queue::new(InMemoryBackend::new())
    ///     .with_state(Arc::new(db_pool));
    /// ```
    pub fn with_state<S: Send + Sync + 'static>(mut self, state: S) -> Self {
        self.state = Arc::new(state);
        self
    }

    // ── Enqueueing ────────────────────────────────────────────────────────────

    /// Enqueue a job for immediate processing.
    ///
    /// ```rust,no_run
    /// # use asyncq::{Queue, backends::InMemoryBackend, Job, Perform, JobContext, JobResult};
    /// # use serde::{Serialize, Deserialize};
    /// # #[derive(Job, Serialize, Deserialize)]
    /// # #[job(queue = "emails", retries = 3)]
    /// # struct WelcomeEmail { email: String }
    /// # impl Perform for WelcomeEmail {
    /// #     async fn perform(self, _ctx: JobContext) -> JobResult { Ok(()) }
    /// # }
    /// # async fn _run() -> Result<(), Box<dyn std::error::Error>> {
    /// # let queue = Queue::new(InMemoryBackend::new());
    /// queue.enqueue(WelcomeEmail { email: "user@example.com".into() }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enqueue<J: Perform>(&self, job: J) -> Result<JobId> {
        let record = self.build_record(&job, Utc::now())?;
        self.backend.enqueue(record).await
    }

    /// Enqueue a job to run after `delay`.
    ///
    /// ```rust,no_run
    /// # use asyncq::{Queue, backends::InMemoryBackend, Job, Perform, JobContext, JobResult};
    /// # use serde::{Serialize, Deserialize};
    /// # use std::time::Duration;
    /// # #[derive(Job, Serialize, Deserialize)]
    /// # #[job(queue = "emails", retries = 3)]
    /// # struct ReminderEmail { email: String }
    /// # impl Perform for ReminderEmail {
    /// #     async fn perform(self, _ctx: JobContext) -> JobResult { Ok(()) }
    /// # }
    /// # async fn _run() -> Result<(), Box<dyn std::error::Error>> {
    /// # let queue = Queue::new(InMemoryBackend::new());
    /// // Send in 24 hours
    /// queue.enqueue_in(ReminderEmail { email: "user@example.com".into() },
    ///     Duration::from_secs(86400)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enqueue_in<J: Perform>(&self, job: J, delay: Duration) -> Result<JobId> {
        let at = Utc::now() + chrono::Duration::from_std(delay).unwrap_or_default();
        let record = self.build_record(&job, at)?;
        self.backend.enqueue(record).await
    }

    /// Enqueue a job to run at a specific UTC time.
    pub async fn enqueue_at<J: Perform>(&self, job: J, at: DateTime<Utc>) -> Result<JobId> {
        let record = self.build_record(&job, at)?;
        self.backend.enqueue(record).await
    }

    /// Enqueue a raw [`JobRecord`] directly.
    ///
    /// Useful for testing backends without going through the typed `Perform`
    /// API, or for advanced use cases where you construct the record manually.
    pub async fn enqueue_record(&self, record: crate::job::JobRecord) -> Result<JobId> {
        self.backend.enqueue(record).await
    }

    // ── Observability ─────────────────────────────────────────────────────────

    /// Queue statistics: pending, running, completed, failed, dead counts.
    pub async fn stats(&self, queue: &str) -> Result<QueueStats> {
        self.backend.stats(queue).await
    }

    // ── Dead-letter queue management ──────────────────────────────────────────

    /// Paginated list of dead-lettered jobs for a queue.
    pub async fn dead_jobs(
        &self,
        queue: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<JobRecord>> {
        self.backend.dead_jobs(queue, limit, offset).await
    }

    /// Requeue a specific dead-lettered job for another delivery attempt.
    pub async fn retry_dead(&self, id: JobId) -> Result<()> {
        self.backend.retry_dead(id).await
    }

    /// Requeue all dead-lettered jobs for a queue.
    ///
    /// Returns the number of jobs requeued.
    pub async fn retry_all_dead(&self, queue: &str) -> Result<u64> {
        self.backend.retry_all_dead(queue).await
    }

    // ── Internal ─────────────────────────────────────────────────────────────

    fn build_record<J: Job>(&self, job: &J, scheduled_at: DateTime<Utc>) -> Result<JobRecord> {
        let payload = serde_json::to_vec(job)?;
        Ok(JobRecord::new(J::KIND, J::QUEUE, payload, J::MAX_RETRIES)
            .with_scheduled_at(scheduled_at))
    }
}
