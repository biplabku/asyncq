use std::sync::Arc;
use std::any::Any;
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Injected into every job execution. Provides job metadata and shared app state.
///
/// # Accessing shared state
///
/// State is injected via [`Queue::with_state`](crate::Queue::with_state) and
/// accessed with `ctx.state::<T>()` — the same mental model as axum's `State<T>`:
///
/// ```rust,ignore
/// // Set up — pass your app state once when building the queue
/// let queue = Queue::new(backend).with_state(Arc::new(db_pool));
///
/// // Inside a job — retrieve it by type
/// impl Perform for MyJob {
///     async fn perform(self, ctx: JobContext) -> JobResult {
///         let db = ctx.state::<Arc<PgPool>>().expect("db not in state");
///         // use db ...
///         Ok(())
///     }
/// }
/// ```
#[derive(Clone)]
pub struct JobContext {
    /// Unique ID of this job instance.
    pub job_id: Uuid,
    /// Name of the queue this job was claimed from.
    pub queue: String,
    /// Delivery attempt number (1 = first attempt).
    pub attempt: u32,
    /// When this job was originally enqueued.
    pub enqueued_at: DateTime<Utc>,
    /// Shared application state set via `Queue::with_state`.
    state: Arc<dyn Any + Send + Sync>,
}

impl JobContext {
    pub(crate) fn new(
        job_id: Uuid,
        queue: String,
        attempt: u32,
        enqueued_at: DateTime<Utc>,
        state: Arc<dyn Any + Send + Sync>,
    ) -> Self {
        Self { job_id, queue, attempt, enqueued_at, state }
    }

    /// Retrieve shared state by type.
    ///
    /// Returns `None` if no state of type `T` was set on the queue.
    ///
    /// ```rust,ignore
    /// let pool = ctx.state::<Arc<PgPool>>().expect("PgPool not in state");
    /// ```
    pub fn state<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.state.downcast_ref::<T>()
    }

    /// Whether this is the first attempt (attempt == 1).
    pub fn is_first_attempt(&self) -> bool {
        self.attempt == 1
    }

    /// Whether this is a retry (attempt > 1).
    pub fn is_retry(&self) -> bool {
        self.attempt > 1
    }
}
