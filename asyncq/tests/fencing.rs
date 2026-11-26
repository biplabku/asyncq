//! Regression tests for claim fencing and dead-lettering of jobs whose worker
//! keeps dying. Safe to run in parallel against a shared database: each test
//! uses its own queue, and reaping only touches jobs claimed > 1s ago.
use asyncq::{Backend, JobRecord, Queue};
use asyncq::backends::InMemoryBackend;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const STALE_AFTER: Duration = Duration::from_secs(1); // Redis heartbeats have 1s resolution

fn unique_queue(tag: &str) -> String {
    let n = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("fence-{tag}-{n}")
}

async fn make_stale() {
    tokio::time::sleep(Duration::from_millis(2100)).await;
}

#[tokio::test]
async fn stale_claim_cannot_ack_after_reclaim() {
    let b = InMemoryBackend::new();
    let q = unique_queue("ack");
    Queue::new(b.clone())
        .enqueue_record(JobRecord::new("Fenced", &q, b"null".to_vec(), 3))
        .await
        .unwrap();
    let r1 = b.claim(&[&q], Duration::ZERO).await.unwrap().expect("first claim");
    make_stale().await;
    b.reap_stuck(STALE_AFTER).await.unwrap(); // this or a concurrent test's reaper requeues it
    let r2 = b.claim(&[&q], Duration::ZERO).await.unwrap().expect("re-claim after reap");
    assert_eq!(r1.id, r2.id);
    assert!(r2.attempt > r1.attempt);
    assert!(!b.ack_claim(r1.id, r1.attempt).await.unwrap(), "stale claim acked");
    assert!(!b.nack_claim(r1.id, r1.attempt, "x", None).await.unwrap(), "stale claim nacked");
    assert!(b.ack_claim(r2.id, r2.attempt).await.unwrap(), "current claim refused");
}

#[tokio::test]
async fn job_whose_worker_keeps_dying_is_dead_lettered() {
    let b = InMemoryBackend::new();
    let q = unique_queue("dlq");
    let queue = Queue::new(b.clone());
    queue
        .enqueue_record(JobRecord::new("CrashLoop", &q, b"null".to_vec(), 3))
        .await
        .unwrap();
    let mut claims = 0;
    while let Some(_r) = b.claim(&[&q], Duration::ZERO).await.unwrap() {
        claims += 1;
        assert!(claims <= 10, "job keeps coming back: never dead-lettered");
        make_stale().await; // worker "dies": no ack, no heartbeat
        b.reap_stuck(STALE_AFTER).await.unwrap();
    }
    assert_eq!(claims, 3, "should be claimed exactly max_attempts times");
    assert_eq!(queue.stats(&q).await.unwrap().dead, 1);
}
