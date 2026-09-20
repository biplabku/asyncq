use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Serialize};
use uuid::Uuid;

use crate::{context::JobContext, error::JobResult};

/// Unique identifier for a job instance.
pub type JobId = Uuid;

/// Metadata and payload for a job stored in the backend.
#[derive(Debug, Clone)]
pub struct JobRecord {
    pub id: JobId,
    /// The job type name, e.g. `"WelcomeEmail"`. Used to route to the right handler.
    pub kind: String,
    /// Which queue this job belongs to, e.g. `"emails"`.
    pub queue: String,
    /// JSON-serialized job payload.
    pub payload: Vec<u8>,
    /// How many times this job has been attempted (0 = not yet run).
    pub attempt: u32,
    /// Maximum attempts before the job is dead-lettered.
    pub max_attempts: u32,
    /// When this job should next be processed.
    pub scheduled_at: DateTime<Utc>,
    /// When this job was originally enqueued.
    pub created_at: DateTime<Utc>,
    /// Error message from the last failure, if any.
    pub last_error: Option<String>,
}

impl JobRecord {
    pub fn new(kind: &str, queue: &str, payload: Vec<u8>, max_attempts: u32) -> Self {
        Self {
            id: Uuid::new_v4(),
            kind: kind.to_owned(),
            queue: queue.to_owned(),
            payload,
            attempt: 0,
            max_attempts,
            scheduled_at: Utc::now(),
            created_at: Utc::now(),
            last_error: None,
        }
    }

    pub fn with_scheduled_at(mut self, at: DateTime<Utc>) -> Self {
        self.scheduled_at = at;
        self
    }
}

/// Statistics for a single queue.
#[derive(Debug, Clone, Default)]
pub struct QueueStats {
    pub queue: String,
    pub pending: u64,
    pub running: u64,
    pub completed: u64,
    pub failed: u64,
    pub dead: u64,
}

// ── Core traits ───────────────────────────────────────────────────────────────

/// Marker trait auto-implemented by `#[derive(Job)]`.
///
/// You do not implement this manually. Use `#[derive(Job)]` on your struct and
/// specify queue name, retry count, and optional timeout in the attribute.
///
/// ```rust,ignore
/// #[derive(Job, Serialize, Deserialize)]
/// #[job(queue = "emails", retries = 3, timeout_secs = 30)]
/// struct WelcomeEmail {
///     email: String,
/// }
/// ```
pub trait Job: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// Unique name for this job type. Generated from the struct name by `#[derive(Job)]`.
    const KIND: &'static str;

    /// Default queue name. Override with `#[job(queue = "...")]`.
    const QUEUE: &'static str;

    /// Maximum delivery attempts before dead-lettering. Override with `#[job(retries = N)]`.
    const MAX_RETRIES: u32;

    /// Optional per-job timeout in seconds. Override with `#[job(timeout_secs = N)]`.
    const TIMEOUT_SECS: Option<u64>;

    fn kind(&self) -> &'static str {
        Self::KIND
    }
    fn queue(&self) -> &'static str {
        Self::QUEUE
    }
    fn max_retries(&self) -> u32 {
        Self::MAX_RETRIES
    }
}

/// The work a job does.
///
/// Implement this on your job struct alongside `#[derive(Job)]`:
///
/// ```rust,ignore
/// impl Perform for WelcomeEmail {
///     async fn perform(self, ctx: JobContext) -> JobResult {
///         let db = ctx.state::<PgPool>().expect("db not in state");
///         send_email(db, &self.email).await?;
///         Ok(())
///     }
/// }
/// ```
///
/// Returning `Ok(())` marks the job as completed.
/// Returning `Err(e)` schedules a retry according to the job's retry policy.
/// Returning `Err(JobError::discard(e))` sends the job to the DLQ immediately.
pub trait Perform: Job {
    fn perform(self, ctx: JobContext) -> impl std::future::Future<Output = JobResult> + Send;
}

/// Type-erased job handler. The worker uses this to call perform() without knowing
/// the concrete job type at compile time.
pub(crate) type BoxedHandler = Box<
    dyn Fn(JobRecord, JobContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = JobResult> + Send>>
        + Send
        + Sync,
>;

/// Build a `BoxedHandler` for a concrete job type `J`.
pub(crate) fn make_handler<J: Perform>() -> BoxedHandler {
    Box::new(|record: JobRecord, ctx: JobContext| {
        Box::pin(async move {
            let job: J = serde_json::from_slice(&record.payload)
                .map_err(|e| crate::error::JobError::discard(format!("deserialization failed: {e}")))?;
            job.perform(ctx).await
        })
    })
}
