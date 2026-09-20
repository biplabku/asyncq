use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("backend error: {0}")]
    Backend(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("job not found: {0}")]
    NotFound(String),

    #[error("job already dead: {0}")]
    AlreadyDead(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Outcome of a job execution.
///
/// Return `Ok(())` to mark the job delivered.
/// Return `Err(JobError::retry(e))` to schedule a retry (default).
/// Return `Err(JobError::discard(e))` to send directly to the DLQ without retrying.
#[derive(Debug, Error)]
pub enum JobError {
    /// Retry the job according to its retry policy.
    #[error("{0}")]
    Retry(String),

    /// Send the job to the dead-letter queue immediately — do not retry.
    #[error("{0}")]
    Discard(String),
}

impl JobError {
    /// Retry the job. This is the default when you return `Err(e.into())`.
    pub fn retry(msg: impl ToString) -> Self {
        Self::Retry(msg.to_string())
    }

    /// Send directly to DLQ without retrying.
    /// Use for permanent errors where retrying would never help.
    pub fn discard(msg: impl ToString) -> Self {
        Self::Discard(msg.to_string())
    }

    pub fn is_discard(&self) -> bool {
        matches!(self, Self::Discard(_))
    }
}

// Use JobError::retry(e) or .map_err(JobError::retry)? in perform() implementations.
// We intentionally do not impl From<E: Error> to avoid conflicting with the
// stdlib blanket From<T> for T impl.
impl From<String> for JobError {
    fn from(s: String) -> Self { Self::Retry(s) }
}
impl From<&str> for JobError {
    fn from(s: &str) -> Self { Self::Retry(s.to_owned()) }
}

/// The return type of [`Perform::perform`].
pub type JobResult = std::result::Result<(), JobError>;
