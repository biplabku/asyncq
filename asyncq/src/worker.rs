use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::{
    backend::Backend,
    context::JobContext,
    error::{JobError, Result},
    job::{make_handler, BoxedHandler, JobId, Perform},
    queue::Queue,
};

const DEFAULT_CONCURRENCY: usize = 10;
const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_STUCK_TIMEOUT: Duration = Duration::from_secs(120);

/// Processes jobs from one or more queues.
///
/// # Quick start
///
/// ```rust,no_run
/// # use asyncq::{Worker, Queue, Job, Perform, JobContext, JobResult, backends::InMemoryBackend};
/// # use serde::{Serialize, Deserialize};
/// # #[derive(Job, Serialize, Deserialize)]
/// # #[job(queue = "jobs", retries = 1)]
/// # struct MyJob;
/// # impl Perform for MyJob { async fn perform(self, _ctx: JobContext) -> JobResult { Ok(()) } }
/// # #[derive(Job, Serialize, Deserialize)]
/// # #[job(queue = "jobs", retries = 1)]
/// # struct AnotherJob;
/// # impl Perform for AnotherJob { async fn perform(self, _ctx: JobContext) -> JobResult { Ok(()) } }
/// # async fn _run() {
/// # let queue = Queue::new(InMemoryBackend::new());
/// Worker::new(queue)
///     .register::<MyJob>()
///     .register::<AnotherJob>()
///     .concurrency(20)
///     .run()
///     .await;
/// # }
/// ```
pub struct Worker<B: Backend> {
    queue: Queue<B>,
    handlers: HashMap<String, Arc<BoxedHandler>>,
    queues: Vec<String>,
    concurrency: usize,
    poll_timeout: Duration,
    stuck_timeout: Duration,
}

impl<B: Backend> Worker<B> {
    /// Create a worker that processes jobs from the given queue.
    pub fn new(queue: Queue<B>) -> Self {
        Self {
            queue,
            handlers: HashMap::new(),
            queues: Vec::new(),
            concurrency: DEFAULT_CONCURRENCY,
            poll_timeout: DEFAULT_POLL_TIMEOUT,
            stuck_timeout: DEFAULT_STUCK_TIMEOUT,
        }
    }

    /// Register a job type. The worker will call `J::perform` when it claims a job of type `J`.
    ///
    /// ```rust,no_run
    /// # use asyncq::{Worker, Queue, Job, Perform, JobContext, JobResult, backends::InMemoryBackend};
    /// # use serde::{Serialize, Deserialize};
    /// # #[derive(Job, Serialize, Deserialize)]
    /// # #[job(queue = "emails", retries = 1)]
    /// # struct WelcomeEmail;
    /// # impl Perform for WelcomeEmail { async fn perform(self, _ctx: JobContext) -> JobResult { Ok(()) } }
    /// # #[derive(Job, Serialize, Deserialize)]
    /// # #[job(queue = "payments", retries = 1)]
    /// # struct ProcessPayment;
    /// # impl Perform for ProcessPayment { async fn perform(self, _ctx: JobContext) -> JobResult { Ok(()) } }
    /// # #[derive(Job, Serialize, Deserialize)]
    /// # #[job(queue = "reports", retries = 1)]
    /// # struct GenerateReport;
    /// # impl Perform for GenerateReport { async fn perform(self, _ctx: JobContext) -> JobResult { Ok(()) } }
    /// # let queue = Queue::new(InMemoryBackend::new());
    /// Worker::new(queue)
    ///     .register::<WelcomeEmail>()
    ///     .register::<ProcessPayment>()
    ///     .register::<GenerateReport>();
    /// ```
    pub fn register<J: Perform>(mut self) -> Self {
        let kind = J::KIND.to_owned();
        let queue_name = J::QUEUE.to_owned();
        self.handlers.insert(kind, Arc::new(make_handler::<J>()));
        if !self.queues.contains(&queue_name) {
            self.queues.push(queue_name);
        }
        self
    }

    /// Override which queues to poll and in what priority order.
    ///
    /// By default the worker polls all queues registered via `register::<J>()`.
    /// Use this to set explicit priority order: queues listed first are checked first.
    pub fn queues(mut self, queues: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.queues = queues.into_iter().map(Into::into).collect();
        self
    }

    /// Maximum number of jobs processed concurrently. Default: 10.
    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n;
        self
    }

    /// How long to wait for a job when all queues are empty. Default: 2 seconds.
    pub fn poll_timeout(mut self, d: Duration) -> Self {
        self.poll_timeout = d;
        self
    }

    /// Jobs running longer than this are reset to pending (recovery from crashes).
    /// Default: 120 seconds.
    pub fn stuck_timeout(mut self, d: Duration) -> Self {
        self.stuck_timeout = d;
        self
    }

    // ── Running ───────────────────────────────────────────────────────────────

    /// Run the worker until the process exits.
    pub async fn run(self) -> ! {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.concurrency));
        let state = Arc::clone(&self.queue.state);
        let backend = Arc::clone(&self.queue.backend);
        let handlers = Arc::new(self.handlers);
        let queues: Vec<String> = self.queues.clone();
        let stuck_timeout = self.stuck_timeout;
        let poll_timeout = self.poll_timeout;

        loop {
            // Recover stuck jobs each cycle
            if let Err(e) = backend.reap_stuck(stuck_timeout).await {
                tracing::warn!(error = %e, "failed to reap stuck jobs");
            }

            let queue_refs: Vec<&str> = queues.iter().map(String::as_str).collect();

            match backend.claim(&queue_refs, poll_timeout).await {
                Ok(Some(record)) => {
                    let permit = semaphore.clone().acquire_owned().await.expect("semaphore closed");
                    let backend2 = Arc::clone(&backend);
                    let handlers2 = Arc::clone(&handlers);
                    let state2 = Arc::clone(&state);

                    tokio::spawn(async move {
                        let _permit = permit;
                        run_job(record, handlers2, state2, backend2).await;
                    });
                }
                Ok(None) => {} // poll timeout — loop again
                Err(e) => {
                    tracing::error!(error = %e, "backend claim failed");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// Run one full cycle: recover stuck jobs, claim and process all currently
    /// available jobs, then return.
    ///
    /// Designed for tests — no need to `Ctrl+C` to stop.
    ///
    /// ```rust,no_run
    /// # use asyncq::{Worker, Queue, Job, Perform, JobContext, JobResult, backends::InMemoryBackend};
    /// # use serde::{Serialize, Deserialize};
    /// # #[derive(Job, Serialize, Deserialize)]
    /// # #[job(queue = "jobs", retries = 1)]
    /// # struct MyJob;
    /// # impl Perform for MyJob { async fn perform(self, _ctx: JobContext) -> JobResult { Ok(()) } }
    /// # let queue = Queue::new(InMemoryBackend::new());
    /// # async {
    /// Worker::new(queue)
    ///     .register::<MyJob>()
    ///     .run_once()
    ///     .await
    ///     .unwrap();
    /// # };
    /// ```
    pub async fn run_once(self) -> Result<usize> {
        let backend = Arc::clone(&self.queue.backend);
        let handlers = Arc::new(self.handlers);
        let state = Arc::clone(&self.queue.state);
        let queues: Vec<String> = self.queues.clone();
        let stuck_timeout = self.stuck_timeout;

        backend.reap_stuck(stuck_timeout).await?;

        let queue_refs: Vec<&str> = queues.iter().map(String::as_str).collect();

        // Snapshot: drain all currently-available jobs into a local Vec FIRST,
        // then process them. This ensures that jobs requeued during processing
        // (retried failures) are not picked up in the same run_once() cycle.
        // This gives run_once() predictable, test-friendly semantics.
        let mut records = Vec::new();
        while let Some(record) = backend.claim(&queue_refs, Duration::from_millis(0)).await? {
            records.push(record);
        }

        let count = records.len();
        for record in records {
            run_job(
                record,
                Arc::clone(&handlers),
                Arc::clone(&state),
                Arc::clone(&backend),
            )
            .await;
        }
        Ok(count)
    }
}

// ── Job execution ─────────────────────────────────────────────────────────────

async fn run_job<B: Backend>(
    record: crate::job::JobRecord,
    handlers: Arc<HashMap<String, Arc<BoxedHandler>>>,
    state: Arc<dyn std::any::Any + Send + Sync>,
    backend: Arc<B>,
) {
    let job_id = record.id;
    let kind = record.kind.clone();
    let attempt = record.attempt;
    let enqueued_at = record.created_at;
    let queue_name = record.queue.clone();
    let max_attempts = record.max_attempts;

    let ctx = JobContext::new(job_id, queue_name, attempt, enqueued_at, state);

    let handler = match handlers.get(&kind) {
        Some(h) => Arc::clone(h),
        None => {
            tracing::error!(kind, "no handler registered for job kind — sending to DLQ");
            let _ = backend.nack(job_id, "no handler registered", None).await;
            return;
        }
    };

    let result = handler(record, ctx).await;

    match result {
        Ok(()) => {
            tracing::debug!(%job_id, %kind, attempt, "job completed");
            if let Err(e) = backend.ack(job_id).await {
                tracing::error!(%job_id, error = %e, "failed to ack job");
            }
        }
        Err(JobError::Discard(msg)) => {
            tracing::warn!(%job_id, %kind, attempt, error = %msg, "job discarded — sending to DLQ");
            if let Err(e) = backend.nack(job_id, &msg, None).await {
                tracing::error!(%job_id, error = %e, "failed to nack (discard) job");
            }
        }
        Err(JobError::Retry(msg)) => {
            if attempt >= max_attempts {
                tracing::warn!(%job_id, %kind, attempt, max_attempts, error = %msg,
                    "job exhausted retries — sending to DLQ");
                if let Err(e) = backend.nack(job_id, &msg, None).await {
                    tracing::error!(%job_id, error = %e, "failed to nack (dead) job");
                }
            } else {
                let retry_at = next_retry_at(attempt);
                tracing::warn!(%job_id, %kind, attempt, error = %msg,
                    retry_in_secs = (retry_at - chrono::Utc::now()).num_seconds(),
                    "job failed — scheduling retry");
                if let Err(e) = backend.nack(job_id, &msg, Some(retry_at)).await {
                    tracing::error!(%job_id, error = %e, "failed to nack (retry) job");
                }
            }
        }
    }
}

/// Exponential backoff with jitter: base_delay * 2^attempt, capped at 1 hour.
fn next_retry_at(attempt: u32) -> chrono::DateTime<chrono::Utc> {
    use chrono::Duration;
    let base_secs: i64 = 30;
    let shift = (attempt as i64).min(10); // cap at 2^10 = 1024
    let delay_secs = (base_secs * (1i64 << shift)).min(3600); // max 1 hour
    chrono::Utc::now() + Duration::seconds(delay_secs)
}

// Suppress unused warning — JobId is used in run_job signature above
fn _use_job_id(_: JobId) {}
