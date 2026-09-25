//! Cron-based job scheduling.
//!
//! The [`Scheduler`] runs alongside your [`Worker`](crate::Worker) and automatically
//! enqueues jobs based on cron expressions.
//!
//! # Example
//!
//! ```rust,ignore
//! use asyncq::{Scheduler, Queue, Worker};
//!
//! let queue = Queue::new(backend);
//!
//! let scheduler = Scheduler::new(queue.clone())
//!     .register::<DailyReport>("0 0 9 * * *")    // every day at 9am
//!     .register::<WeeklyDigest>("0 0 0 * * 0");  // every Sunday at midnight
//!
//! // Run scheduler and worker concurrently
//! tokio::select! {
//!     _ = scheduler.run() => {}
//!     _ = Worker::new(queue).register::<DailyReport>().register::<WeeklyDigest>().run() => {}
//! }
//! ```

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use cron::Schedule;
use tracing::{debug, info, warn};

use crate::backend::Backend;
use crate::error::Result;
use crate::job::Perform;
use crate::queue::Queue;

/// A scheduled job entry.
struct CronEntry {
    kind: &'static str,
    queue: &'static str,
    schedule: Schedule,
    next_fire: DateTime<Utc>,
    enqueue_fn: Box<dyn Fn() -> Vec<u8> + Send + Sync>,
    max_attempts: u32,
}

impl CronEntry {
    fn advance(&mut self) {
        if let Some(next) = self.schedule.upcoming(Utc).next() {
            self.next_fire = next;
        }
    }
}

/// Cron-based job scheduler.
///
/// Runs in a loop, checking which jobs are due and enqueuing them.
/// Designed to run alongside a [`Worker`](crate::Worker).
///
/// # Distributed deployments
///
/// If you run multiple scheduler instances, each will fire cron jobs independently.
/// For single-fire semantics, use unique jobs (coming in v0.2) or run only one scheduler.
pub struct Scheduler<B: Backend> {
    queue: Queue<B>,
    entries: Vec<CronEntry>,
    poll_interval: Duration,
}

impl<B: Backend> Scheduler<B> {
    /// Create a new scheduler attached to the given queue.
    pub fn new(queue: Queue<B>) -> Self {
        Self {
            queue,
            entries: Vec::new(),
            poll_interval: Duration::from_secs(1),
        }
    }

    /// Register a job type with a cron expression.
    ///
    /// The job will be enqueued with default (empty) payload whenever
    /// the cron expression fires.
    ///
    /// # Cron expression format
    ///
    /// 6-field cron: `second minute hour day-of-month month day-of-week`
    ///
    /// Examples:
    /// - `"0 0 9 * * *"` — every day at 9:00 AM
    /// - `"0 */15 * * * *"` — every 15 minutes
    /// - `"0 0 0 * * 0"` — every Sunday at midnight
    /// - `"0 0 0 1 * *"` — first day of every month at midnight
    /// - `"0 30 9 * * Mon-Fri"` — weekdays at 9:30 AM
    ///
    /// # Panics
    ///
    /// Panics if the cron expression is invalid.
    pub fn register<J: Perform + Default>(mut self, cron_expr: &str) -> Self {
        let schedule: Schedule = cron_expr
            .parse()
            .unwrap_or_else(|e| panic!("invalid cron expression '{cron_expr}': {e}"));

        let next_fire = schedule
            .upcoming(Utc)
            .next()
            .expect("cron schedule has no upcoming fires");

        let entry = CronEntry {
            kind: J::KIND,
            queue: J::QUEUE,
            schedule,
            next_fire,
            enqueue_fn: Box::new(|| {
                serde_json::to_vec(&J::default()).expect("job serialization failed")
            }),
            max_attempts: J::MAX_RETRIES,
        };

        info!(
            kind = J::KIND,
            queue = J::QUEUE,
            cron = cron_expr,
            next_fire = %next_fire,
            "registered cron job"
        );

        self.entries.push(entry);
        self
    }

    /// Register a job with a custom payload factory.
    ///
    /// Use this when your job needs dynamic payload data at fire time.
    ///
    /// ```rust,ignore
    /// scheduler.register_with::<DailyReport, _>("0 9 * * *", || DailyReport {
    ///     date: Utc::now().date_naive(),
    /// })
    /// ```
    pub fn register_with<J, F>(mut self, cron_expr: &str, payload_fn: F) -> Self
    where
        J: Perform,
        F: Fn() -> J + Send + Sync + 'static,
    {
        let schedule: Schedule = cron_expr
            .parse()
            .unwrap_or_else(|e| panic!("invalid cron expression '{cron_expr}': {e}"));

        let next_fire = schedule
            .upcoming(Utc)
            .next()
            .expect("cron schedule has no upcoming fires");

        let entry = CronEntry {
            kind: J::KIND,
            queue: J::QUEUE,
            schedule,
            next_fire,
            enqueue_fn: Box::new(move || {
                serde_json::to_vec(&payload_fn()).expect("job serialization failed")
            }),
            max_attempts: J::MAX_RETRIES,
        };

        info!(
            kind = J::KIND,
            queue = J::QUEUE,
            cron = cron_expr,
            next_fire = %next_fire,
            "registered cron job with custom payload"
        );

        self.entries.push(entry);
        self
    }

    /// Set how often to check for due jobs. Default: 1 second.
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Run the scheduler forever, enqueuing jobs as they come due.
    ///
    /// This method never returns. Run it concurrently with your worker:
    ///
    /// ```rust,ignore
    /// tokio::select! {
    ///     _ = scheduler.run() => {}
    ///     _ = worker.run() => {}
    /// }
    /// ```
    pub async fn run(mut self) -> ! {
        info!(
            jobs = self.entries.len(),
            poll_interval_ms = self.poll_interval.as_millis(),
            "scheduler started"
        );

        loop {
            let now = Utc::now();

            for entry in &mut self.entries {
                if entry.next_fire <= now {
                    let payload = (entry.enqueue_fn)();
                    let record = crate::job::JobRecord::new(
                        entry.kind,
                        entry.queue,
                        payload,
                        entry.max_attempts,
                    );

                    match self.queue.enqueue_record(record).await {
                        Ok(id) => {
                            info!(
                                kind = entry.kind,
                                job_id = %id,
                                "cron job enqueued"
                            );
                        }
                        Err(e) => {
                            warn!(
                                kind = entry.kind,
                                error = %e,
                                "failed to enqueue cron job"
                            );
                        }
                    }

                    entry.advance();
                    debug!(
                        kind = entry.kind,
                        next_fire = %entry.next_fire,
                        "next fire scheduled"
                    );
                }
            }

            tokio::time::sleep(self.poll_interval).await;
        }
    }

    /// Run the scheduler once, firing any due jobs and returning.
    ///
    /// Useful for testing.
    pub async fn run_once(&mut self) -> Result<usize> {
        let now = Utc::now();
        let mut fired = 0;

        for entry in &mut self.entries {
            if entry.next_fire <= now {
                let payload = (entry.enqueue_fn)();
                let record = crate::job::JobRecord::new(
                    entry.kind,
                    entry.queue,
                    payload,
                    entry.max_attempts,
                );

                self.queue.enqueue_record(record).await?;
                entry.advance();
                fired += 1;
            }
        }

        Ok(fired)
    }

    /// Check which jobs are registered and when they'll next fire.
    pub fn status(&self) -> HashMap<&'static str, DateTime<Utc>> {
        self.entries
            .iter()
            .map(|e| (e.kind, e.next_fire))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::InMemoryBackend;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct TestCronJob;

    impl crate::job::Job for TestCronJob {
        const KIND: &'static str = "TestCronJob";
        const QUEUE: &'static str = "test";
        const MAX_RETRIES: u32 = 3;
        const TIMEOUT_SECS: Option<u64> = None;
    }

    impl Perform for TestCronJob {
        async fn perform(self, _ctx: crate::JobContext) -> crate::JobResult {
            Ok(())
        }
    }

    #[tokio::test]
    async fn scheduler_registers_job() {
        let backend = InMemoryBackend::new();
        let queue = Queue::new(backend);

        let scheduler = Scheduler::new(queue).register::<TestCronJob>("0 * * * * *");

        assert_eq!(scheduler.entries.len(), 1);
        assert_eq!(scheduler.entries[0].kind, "TestCronJob");
    }

    #[tokio::test]
    async fn scheduler_fires_due_job() {
        let backend = InMemoryBackend::new();
        let queue = Queue::new(backend.clone());

        // Use "every second" pattern - schedule that's always due
        let mut scheduler = Scheduler::new(queue).register::<TestCronJob>("* * * * * *");

        // Manually set next_fire to past
        scheduler.entries[0].next_fire = Utc::now() - chrono::Duration::seconds(1);

        let fired = scheduler.run_once().await.unwrap();
        assert_eq!(fired, 1);

        let stats = backend.stats("test").await.unwrap();
        assert_eq!(stats.pending, 1);
    }

    #[tokio::test]
    async fn scheduler_does_not_fire_future_job() {
        let backend = InMemoryBackend::new();
        let queue = Queue::new(backend.clone());

        let mut scheduler = Scheduler::new(queue).register::<TestCronJob>("0 0 0 1 1 *"); // Jan 1 only

        let fired = scheduler.run_once().await.unwrap();
        assert_eq!(fired, 0);

        let stats = backend.stats("test").await.unwrap();
        assert_eq!(stats.pending, 0);
    }
}
