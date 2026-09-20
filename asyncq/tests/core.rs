use asyncq::{backends::InMemoryBackend, Job, JobContext, JobError, JobResult, Perform, Queue, Worker};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

// ── Test jobs ─────────────────────────────────────────────────────────────────

#[derive(Job, Serialize, Deserialize)]
#[job(queue = "test", retries = 3)]
struct SimpleJob {
    value: u32,
}

impl Perform for SimpleJob {
    async fn perform(self, _ctx: JobContext) -> JobResult {
        Ok(())
    }
}

#[derive(Job, Serialize, Deserialize)]
#[job(queue = "test", retries = 2)]
struct FailingJob {
    fail_count: u32,
}

impl Perform for FailingJob {
    async fn perform(self, ctx: JobContext) -> JobResult {
        if ctx.attempt <= self.fail_count {
            Err(JobError::retry("intentional failure"))
        } else {
            Ok(())
        }
    }
}

#[derive(Job, Serialize, Deserialize)]
#[job(queue = "discard_queue", retries = 3)]
struct DiscardJob;

impl Perform for DiscardJob {
    async fn perform(self, _ctx: JobContext) -> JobResult {
        Err(JobError::discard("permanent failure — do not retry"))
    }
}

#[derive(Job, Serialize, Deserialize)]
#[job(queue = "state_queue", retries = 1)]
struct StateJob;

impl Perform for StateJob {
    async fn perform(self, ctx: JobContext) -> JobResult {
        let counter = ctx.state::<Arc<Mutex<u32>>>().expect("counter not in state");
        *counter.lock().unwrap() += 1;
        Ok(())
    }
}

#[derive(Job, Serialize, Deserialize)]
#[job(queue = "custom_queue", retries = 1, timeout_secs = 30, kind = "MyCustomKind")]
struct CustomKindJob;

impl Perform for CustomKindJob {
    async fn perform(self, _ctx: JobContext) -> JobResult {
        Ok(())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn derive_macro_sets_correct_constants() {
    assert_eq!(SimpleJob::KIND, "SimpleJob");
    assert_eq!(SimpleJob::QUEUE, "test");
    assert_eq!(SimpleJob::MAX_RETRIES, 3);
    assert_eq!(SimpleJob::TIMEOUT_SECS, None);
}

#[tokio::test]
async fn custom_kind_override() {
    assert_eq!(CustomKindJob::KIND, "MyCustomKind");
    assert_eq!(CustomKindJob::TIMEOUT_SECS, Some(30));
}

#[tokio::test]
async fn enqueue_and_run_once_completes_job() {
    let backend = InMemoryBackend::new();
    let queue = Queue::new(backend.clone());

    queue.enqueue(SimpleJob { value: 42 }).await.unwrap();

    assert_eq!(backend.pending_count("test").await, 1);
    assert_eq!(backend.completed_count("test").await, 0);

    let processed = Worker::new(queue)
        .register::<SimpleJob>()
        .run_once()
        .await
        .unwrap();

    assert_eq!(processed, 1);
    assert_eq!(backend.completed_count("test").await, 1);
    assert_eq!(backend.pending_count("test").await, 0);
}

#[tokio::test]
async fn multiple_jobs_all_processed() {
    let backend = InMemoryBackend::new();
    let queue = Queue::new(backend.clone());

    for i in 0..5 {
        queue.enqueue(SimpleJob { value: i }).await.unwrap();
    }

    Worker::new(queue).register::<SimpleJob>().run_once().await.unwrap();

    assert_eq!(backend.completed_count("test").await, 5);
    assert_eq!(backend.pending_count("test").await, 0);
}

#[tokio::test]
async fn failing_job_retries_then_succeeds() {
    // with_immediate_retries() skips scheduled_at so retried jobs are
    // immediately claimable in tests (bypasses the exponential backoff delay).
    let backend = InMemoryBackend::new().with_immediate_retries();
    let queue = Queue::new(backend.clone());

    // fail_count=1 means fail on attempt 1, succeed on attempt 2
    queue.enqueue(FailingJob { fail_count: 1 }).await.unwrap();

    let worker = || {
        Worker::new(Queue::new(backend.clone()))
            .register::<FailingJob>()
    };

    // First run: job fails, requeued for retry
    worker().run_once().await.unwrap();
    assert_eq!(backend.completed_count("test").await, 0);
    assert_eq!(backend.failed_count("test").await, 1);

    // Second run: job succeeds (immediate_retries lets us claim it now)
    worker().run_once().await.unwrap();
    assert_eq!(backend.completed_count("test").await, 1);
}

#[tokio::test]
async fn discard_goes_directly_to_dlq_without_retry() {
    let backend = InMemoryBackend::new();
    let queue = Queue::new(backend.clone());

    queue.enqueue(DiscardJob).await.unwrap();

    Worker::new(queue).register::<DiscardJob>().run_once().await.unwrap();

    // Dead-lettered immediately — not retried
    assert_eq!(backend.dead_count("discard_queue").await, 1);
    assert_eq!(backend.completed_count("discard_queue").await, 0);
    assert_eq!(backend.failed_count("discard_queue").await, 0);
}

#[tokio::test]
async fn exhausted_retries_dead_letters_job() {
    let backend = InMemoryBackend::new().with_immediate_retries();
    let queue = Queue::new(backend.clone());

    // fail_count=99 means always fail — will exhaust MAX_RETRIES (2)
    queue.enqueue(FailingJob { fail_count: 99 }).await.unwrap();

    let worker = || {
        Worker::new(Queue::new(backend.clone()))
            .register::<FailingJob>()
    };

    // Run enough times to exhaust retries (MAX_RETRIES = 2, so 3 runs total)
    for _ in 0..=FailingJob::MAX_RETRIES {
        worker().run_once().await.unwrap();
    }

    assert_eq!(backend.completed_count("test").await, 0);
    assert_eq!(backend.dead_count("test").await, 1);
}

#[tokio::test]
async fn state_is_accessible_in_job() {
    let counter = Arc::new(Mutex::new(0u32));
    let backend = InMemoryBackend::new();
    let queue = Queue::new(backend.clone()).with_state(Arc::clone(&counter));

    queue.enqueue(StateJob).await.unwrap();
    queue.enqueue(StateJob).await.unwrap();
    queue.enqueue(StateJob).await.unwrap();

    Worker::new(queue).register::<StateJob>().run_once().await.unwrap();

    assert_eq!(*counter.lock().unwrap(), 3, "all 3 jobs must have incremented the counter");
}

#[tokio::test]
async fn retry_dead_requeues_job() {
    let backend = InMemoryBackend::new();
    let queue = Queue::new(backend.clone());

    queue.enqueue(DiscardJob).await.unwrap();
    Worker::new(queue.clone()).register::<DiscardJob>().run_once().await.unwrap();

    assert_eq!(backend.dead_count("discard_queue").await, 1);

    // Manually retry the dead job
    let dead = queue.dead_jobs("discard_queue", 10, 0).await.unwrap();
    assert_eq!(dead.len(), 1);
    queue.retry_dead(dead[0].id).await.unwrap();

    assert_eq!(backend.dead_count("discard_queue").await, 0);
    assert_eq!(backend.pending_count("discard_queue").await, 1);
}

#[tokio::test]
async fn retry_all_dead_requeues_everything() {
    let backend = InMemoryBackend::new();
    let queue = Queue::new(backend.clone());

    for _ in 0..3 {
        queue.enqueue(DiscardJob).await.unwrap();
    }
    Worker::new(queue.clone()).register::<DiscardJob>().run_once().await.unwrap();
    assert_eq!(backend.dead_count("discard_queue").await, 3);

    let requeued = queue.retry_all_dead("discard_queue").await.unwrap();
    assert_eq!(requeued, 3);
    assert_eq!(backend.dead_count("discard_queue").await, 0);
    assert_eq!(backend.pending_count("discard_queue").await, 3);
}

#[tokio::test]
async fn queue_stats_are_accurate() {
    let backend = InMemoryBackend::new();
    let queue = Queue::new(backend.clone());

    queue.enqueue(SimpleJob { value: 1 }).await.unwrap();
    queue.enqueue(SimpleJob { value: 2 }).await.unwrap();

    let stats = queue.stats("test").await.unwrap();
    assert_eq!(stats.pending, 2);
    assert_eq!(stats.completed, 0);

    Worker::new(queue.clone()).register::<SimpleJob>().run_once().await.unwrap();

    let stats = queue.stats("test").await.unwrap();
    assert_eq!(stats.pending, 0);
    assert_eq!(stats.completed, 2);
}

#[tokio::test]
async fn unregistered_job_kind_goes_to_dlq() {
    let backend = InMemoryBackend::new();
    let queue = Queue::new(backend.clone());

    queue.enqueue(SimpleJob { value: 1 }).await.unwrap();

    // Worker polls the right queue but has no handler registered for SimpleJob.
    // The job should be DLQ'd immediately.
    Worker::new(queue)
        .queues(["test"])  // poll "test" queue explicitly
        // intentionally register nothing — unknown kind → DLQ
        .run_once()
        .await
        .unwrap();

    assert_eq!(backend.dead_count("test").await, 1);
    assert_eq!(backend.completed_count("test").await, 0);
}

#[tokio::test]
async fn context_has_correct_metadata() {
    #[derive(Job, Serialize, Deserialize)]
    #[job(queue = "meta_queue", retries = 1)]
    struct MetaJob;

    let captured = Arc::new(Mutex::new(Option::<(u32, String)>::None));
    let cap2 = Arc::clone(&captured);

    // We need a closure-based job since we can't capture state into impl Perform.
    // Instead, use state to capture.
    impl Perform for MetaJob {
        async fn perform(self, ctx: JobContext) -> JobResult {
            // Just verify the fields exist and have sane values
            assert_eq!(ctx.attempt, 1);
            assert!(!ctx.job_id.is_nil());
            assert_eq!(ctx.queue, "meta_queue");
            assert!(ctx.is_first_attempt());
            assert!(!ctx.is_retry());
            Ok(())
        }
    }

    let backend = InMemoryBackend::new();
    let queue = Queue::new(backend.clone());
    queue.enqueue(MetaJob).await.unwrap();
    Worker::new(queue).register::<MetaJob>().run_once().await.unwrap();
    assert_eq!(backend.completed_count("meta_queue").await, 1);
}
