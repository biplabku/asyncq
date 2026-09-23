//! Shows how to inject shared state (e.g. a database pool) into jobs.
//! This is the pattern you'd use in a real web app.
//!
//! Run: cargo run --example with_state

use asyncq::{Job, Perform, JobContext, JobError, JobResult, Queue, Worker};
use asyncq_redis::RedisBackend;
use serde::{Serialize, Deserialize};
use std::sync::{Arc, Mutex};

// ── Fake database (stands in for sqlx::PgPool in a real app) ─────────────────

#[derive(Default)]
struct FakeDb {
    sent_emails: Mutex<Vec<String>>,
}

impl FakeDb {
    fn record_sent(&self, email: &str) {
        self.sent_emails.lock().unwrap().push(email.to_owned());
    }
    fn sent_count(&self) -> usize {
        self.sent_emails.lock().unwrap().len()
    }
}

// ── Job definitions ───────────────────────────────────────────────────────────

#[derive(Job, Serialize, Deserialize)]
#[job(queue = "emails", retries = 3)]
struct SendEmail {
    to: String,
    subject: String,
}

impl Perform for SendEmail {
    async fn perform(self, ctx: JobContext) -> JobResult {
        // Access shared state via ctx.state::<T>()
        let db = ctx.state::<Arc<FakeDb>>()
            .ok_or_else(|| JobError::discard("db not in state"))?;

        // Simulate sending the email
        println!("  → Sending '{}' to {}", self.subject, self.to);
        db.record_sent(&self.to);

        Ok(())
    }
}

/// Job that permanently fails — goes directly to DLQ (no retry).
#[derive(Job, Serialize, Deserialize)]
#[job(queue = "emails", retries = 3)]
struct InvalidEmail {
    to: String,
}

impl Perform for InvalidEmail {
    async fn perform(self, _ctx: JobContext) -> JobResult {
        println!("  ✗ {} is invalid — discarding (no retry)", self.to);
        Err(JobError::discard("invalid email address"))
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .init();

    let db = Arc::new(FakeDb::default());
    let backend = RedisBackend::new("redis://127.0.0.1/").await?;

    // Attach shared state — jobs will access it via ctx.state::<Arc<FakeDb>>()
    let queue = Queue::new(backend).with_state(Arc::clone(&db));

    // Flush all asyncq keys for "emails" queue from previous runs
    {
        use redis::AsyncCommands;
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        for suffix in &["pending", "delayed", "dead"] {
            let _: Result<(), _> = conn.del(format!("asyncq:q:emails:{suffix}")).await;
        }
    }

    // Enqueue jobs — 2 valid, 1 invalid
    println!("Enqueueing jobs...");
    queue.enqueue(SendEmail { to: "alice@example.com".into(), subject: "Welcome!".into() }).await?;
    queue.enqueue(SendEmail { to: "bob@example.com".into(),   subject: "Invoice #1001".into() }).await?;
    queue.enqueue(InvalidEmail { to: "not-an-email".into() }).await?;

    println!("\nProcessing...");
    Worker::new(queue.clone())
        .register::<SendEmail>()
        .register::<InvalidEmail>()
        .run_once()
        .await?;

    println!("\nResults:");
    println!("  Emails recorded in DB: {}", db.sent_count());
    let dead = queue.dead_jobs("emails", 10, 0).await?;
    println!("  Jobs in dead-letter queue: {}", dead.len());

    assert_eq!(db.sent_count(), 2, "2 valid emails must be recorded");
    assert_eq!(dead.len(), 1, "1 invalid email must be in DLQ");
    assert_eq!(dead[0].last_error.as_deref(), Some("invalid email address"));

    // Clean up DLQ for repeated example runs
    queue.retry_all_dead("emails").await?;

    println!("\nDone! State injection, DLQ, and stats all working.");
    Ok(())
}
