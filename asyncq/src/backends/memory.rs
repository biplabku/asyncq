use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use chrono::{DateTime, Utc};
use tokio::sync::Mutex;
use async_trait::async_trait;

use crate::{
    backend::Backend,
    error::{Error, Result},
    job::{JobId, JobRecord, QueueStats},
};

#[derive(Debug, Default)]
struct QueueState {
    pending:   VecDeque<JobRecord>,
    running:   HashMap<JobId, (JobRecord, DateTime<Utc>)>, // id → (record, heartbeat)
    completed: Vec<JobRecord>,
    failed:    Vec<JobRecord>,
    dead:      Vec<JobRecord>,
}

/// In-memory backend for testing. No Redis or PostgreSQL required.
///
/// Jobs are stored in memory and lost when the process exits.
/// Use this in tests with `Worker::run_once()` for fast, infrastructure-free CI.
///
/// # Example
///
/// ```rust,no_run
/// use asyncq::{Queue, Worker, Job, Perform, JobContext, JobResult};
/// use asyncq::backends::InMemoryBackend;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Job, Serialize, Deserialize)]
/// #[job(queue = "test", retries = 1)]
/// struct MyJob { value: u32 }
///
/// impl Perform for MyJob {
///     async fn perform(self, _ctx: JobContext) -> JobResult {
///         println!("processing {}", self.value);
///         Ok(())
///     }
/// }
///
/// #[tokio::test]
/// async fn test_my_job() {
///     let backend = InMemoryBackend::new();
///     let queue = Queue::new(backend.clone());
///
///     queue.enqueue(MyJob { value: 42 }).await.unwrap();
///
///     Worker::new(queue).register::<MyJob>().run_once().await.unwrap();
///
///     assert_eq!(backend.completed_count("test").await, 1);
/// }
/// ```
#[derive(Clone, Default)]
pub struct InMemoryBackend {
    queues: Arc<Mutex<HashMap<String, QueueState>>>,
    /// When true, scheduled_at is ignored during claim — all pending jobs are
    /// immediately claimable regardless of delay. Use in tests.
    immediate: bool,
}

impl InMemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }

    /// Skip scheduled_at filtering when claiming jobs.
    ///
    /// Use in tests so retried jobs (which have a future scheduled_at due to
    /// exponential backoff) are immediately claimable by `run_once()`.
    ///
    /// ```rust,no_run
    /// # use asyncq::backends::InMemoryBackend;
    /// let backend = InMemoryBackend::new().with_immediate_retries();
    /// ```
    pub fn with_immediate_retries(mut self) -> Self {
        self.immediate = true;
        self
    }

    /// Number of successfully completed jobs in a queue.
    pub async fn completed_count(&self, queue: &str) -> usize {
        self.queues.lock().await
            .get(queue)
            .map(|q| q.completed.len())
            .unwrap_or(0)
    }

    /// Number of failed (pending retry) jobs in a queue.
    pub async fn failed_count(&self, queue: &str) -> usize {
        self.queues.lock().await
            .get(queue)
            .map(|q| q.failed.len())
            .unwrap_or(0)
    }

    /// Number of dead-lettered jobs in a queue.
    pub async fn dead_count(&self, queue: &str) -> usize {
        self.queues.lock().await
            .get(queue)
            .map(|q| q.dead.len())
            .unwrap_or(0)
    }

    /// Number of pending (not yet claimed) jobs in a queue.
    pub async fn pending_count(&self, queue: &str) -> usize {
        self.queues.lock().await
            .get(queue)
            .map(|q| q.pending.len())
            .unwrap_or(0)
    }

    fn get_or_create<'a>(
        map: &'a mut HashMap<String, QueueState>,
        queue: &str,
    ) -> &'a mut QueueState {
        map.entry(queue.to_owned()).or_default()
    }
}

#[async_trait]
impl Backend for InMemoryBackend {
    async fn enqueue(&self, record: JobRecord) -> Result<JobId> {
        let id = record.id;
        let mut lock = self.queues.lock().await;
        let q = Self::get_or_create(&mut lock, &record.queue.clone());
        q.pending.push_back(record);
        Ok(id)
    }

    async fn claim(&self, queues: &[&str], timeout: Duration) -> Result<Option<JobRecord>> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            {
                let immediate = self.immediate;
                let mut lock = self.queues.lock().await;
                for &queue in queues {
                    let q = Self::get_or_create(&mut lock, queue);
                    let pos = if immediate {
                        // In test mode: claim any pending job regardless of scheduled_at
                        if q.pending.is_empty() { None } else { Some(0) }
                    } else {
                        // In production mode: only claim jobs whose scheduled_at has passed
                        q.pending.iter().position(|r| r.scheduled_at <= Utc::now())
                    };
                    if let Some(pos) = pos {
                        let mut record = q.pending.remove(pos).unwrap();
                        record.attempt += 1;
                        q.running.insert(record.id, (record.clone(), Utc::now()));
                        return Ok(Some(record));
                    }
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(None);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn ack(&self, id: JobId) -> Result<()> {
        let mut lock = self.queues.lock().await;
        for q in lock.values_mut() {
            if let Some((record, _)) = q.running.remove(&id) {
                q.completed.push(record);
                return Ok(());
            }
        }
        Err(Error::NotFound(id.to_string()))
    }

    async fn nack(
        &self,
        id: JobId,
        error: &str,
        retry_at: Option<DateTime<Utc>>,
    ) -> Result<()> {
        let mut lock = self.queues.lock().await;
        for q in lock.values_mut() {
            if let Some((mut record, _)) = q.running.remove(&id) {
                record.last_error = Some(error.to_owned());
                match retry_at {
                    Some(at) => {
                        record.scheduled_at = at;
                        q.failed.push(record.clone());
                        q.pending.push_back(record);
                    }
                    None => {
                        q.dead.push(record);
                    }
                }
                return Ok(());
            }
        }
        Err(Error::NotFound(id.to_string()))
    }

    async fn heartbeat(&self, id: JobId) -> Result<()> {
        let mut lock = self.queues.lock().await;
        for q in lock.values_mut() {
            if let Some((_, hb)) = q.running.get_mut(&id) {
                *hb = Utc::now();
                return Ok(());
            }
        }
        Ok(()) // not found is OK — job may have completed
    }

    async fn reap_stuck(&self, older_than: Duration) -> Result<u64> {
        let cutoff = Utc::now()
            - chrono::Duration::from_std(older_than).unwrap_or(chrono::Duration::seconds(120));
        let mut count = 0u64;
        let mut lock = self.queues.lock().await;
        for q in lock.values_mut() {
            let stuck: Vec<JobId> = q.running
                .iter()
                .filter(|(_, (_, hb))| *hb < cutoff)
                .map(|(id, _)| *id)
                .collect();
            for id in stuck {
                if let Some((mut record, _)) = q.running.remove(&id) {
                    // Reset to pending for re-processing
                    record.scheduled_at = Utc::now();
                    q.pending.push_back(record);
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    async fn stats(&self, queue: &str) -> Result<QueueStats> {
        let lock = self.queues.lock().await;
        let q = lock.get(queue);
        Ok(QueueStats {
            queue: queue.to_owned(),
            pending:   q.map(|q| q.pending.len() as u64).unwrap_or(0),
            running:   q.map(|q| q.running.len() as u64).unwrap_or(0),
            completed: q.map(|q| q.completed.len() as u64).unwrap_or(0),
            failed:    q.map(|q| q.failed.len() as u64).unwrap_or(0),
            dead:      q.map(|q| q.dead.len() as u64).unwrap_or(0),
        })
    }

    async fn dead_jobs(&self, queue: &str, limit: i64, offset: i64) -> Result<Vec<JobRecord>> {
        let lock = self.queues.lock().await;
        Ok(lock.get(queue)
            .map(|q| {
                q.dead.iter()
                    .skip(offset as usize)
                    .take(limit as usize)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn retry_dead(&self, id: JobId) -> Result<()> {
        let mut lock = self.queues.lock().await;
        for q in lock.values_mut() {
            if let Some(pos) = q.dead.iter().position(|r| r.id == id) {
                let mut record = q.dead.remove(pos);
                record.scheduled_at = Utc::now();
                q.pending.push_back(record);
                return Ok(());
            }
        }
        Err(Error::NotFound(id.to_string()))
    }

    async fn retry_all_dead(&self, queue: &str) -> Result<u64> {
        let mut lock = self.queues.lock().await;
        let q = Self::get_or_create(&mut lock, queue);
        let count = q.dead.len() as u64;
        let dead: Vec<_> = q.dead.drain(..).collect();
        for mut record in dead {
            record.scheduled_at = Utc::now();
            q.pending.push_back(record);
        }
        Ok(count)
    }
}
