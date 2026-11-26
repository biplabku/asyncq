//! Regression tests found with queue-chaos: claim fencing and dead-lettering of
//! jobs whose worker keeps dying.
use asyncq::{Backend, Job, JobContext, JobResult, Perform, Queue};
use asyncq::backends::InMemoryBackend;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Job, Serialize, Deserialize)]
#[job(queue = "fence", retries = 2)]
struct Fenced;
impl Perform for Fenced {
    async fn perform(self, _ctx: JobContext) -> JobResult { Ok(()) }
}

#[derive(Job, Serialize, Deserialize)]
#[job(queue = "crashloop", retries = 2)]
struct CrashLoop;
impl Perform for CrashLoop {
    async fn perform(self, _ctx: JobContext) -> JobResult { Ok(()) }
}

async fn wait_past_heartbeat() {
    // Redis heartbeats have 1s resolution.
    tokio::time::sleep(Duration::from_millis(1100)).await;
}

#[tokio::test]
async fn stale_claim_cannot_ack_after_reclaim() {
    let b = InMemoryBackend::new();
    let q = Queue::new(b.clone());
    q.enqueue(Fenced).await.unwrap();
    let r1 = b.claim(&["fence"], Duration::ZERO).await.unwrap().expect("first claim");
    wait_past_heartbeat().await;
    assert!(b.reap_stuck(Duration::ZERO).await.unwrap() >= 1);
    let r2 = b.claim(&["fence"], Duration::ZERO).await.unwrap().expect("re-claim");
    assert_eq!(r1.id, r2.id);
    assert!(r2.attempt > r1.attempt);
    assert!(!b.ack_claim(r1.id, r1.attempt).await.unwrap(), "stale claim acked");
    assert!(!b.nack_claim(r1.id, r1.attempt, "x", None).await.unwrap(), "stale claim nacked");
    assert!(b.ack_claim(r2.id, r2.attempt).await.unwrap(), "current claim refused");
}

#[tokio::test]
async fn job_whose_worker_keeps_dying_is_dead_lettered() {
    let b = InMemoryBackend::new();
    let q = Queue::new(b.clone());
    q.enqueue(CrashLoop).await.unwrap();
    let (mut claims, mut max) = (0, 0);
    while let Some(r) = b.claim(&["crashloop"], Duration::ZERO).await.unwrap() {
        claims += 1;
        max = r.max_attempts;
        assert!(claims <= 10, "job keeps coming back: never dead-lettered");
        wait_past_heartbeat().await; // worker "dies": no ack, no heartbeat
        b.reap_stuck(Duration::ZERO).await.unwrap();
    }
    assert_eq!(claims, max, "should be claimed exactly max_attempts times");
    assert_eq!(q.stats("crashloop").await.unwrap().dead, 1);
}
