//! Transactional outbox example — enqueue a job inside a business transaction.
//!
//! The job is only visible to workers after the transaction commits.
//! If the transaction rolls back, the job disappears with it — no orphan notifications.
//!
//! Run: cargo run -p asyncq-postgres --example transactional_outbox
//! Requires: Postgres at DATABASE_URL (default: postgres://hooksmith:hooksmith@localhost/hooksmith)

use asyncq::{Job, JobContext, JobResult, Perform, Queue, Worker};
use asyncq_postgres::PostgresBackend;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;

fn db_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://hooksmith:hooksmith@localhost/hooksmith".to_string())
}

// ── Job definitions ───────────────────────────────────────────────────────────

#[derive(Job, Serialize, Deserialize)]
#[job(queue = "orders", retries = 3)]
struct SendOrderConfirmation {
    order_id: i64,
    email: String,
}

impl Perform for SendOrderConfirmation {
    async fn perform(self, _ctx: JobContext) -> JobResult {
        println!(
            "  → Sending confirmation for order #{} to {}",
            self.order_id, self.email
        );
        Ok(())
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_target(false).init();

    let backend = PostgresBackend::new(&db_url()).await?;
    backend.migrate().await?;
    let pool = backend.pool_ref().clone();

    // Create a minimal orders table for this demo
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS demo_orders (
            id     BIGSERIAL PRIMARY KEY,
            email  TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending'
        )",
    )
    .execute(&pool)
    .await?;

    let queue = Queue::new(backend.clone());

    // ── Scenario 1: committed transaction ─────────────────────────────────────
    println!("Scenario 1: committed transaction → job queued");
    {
        let mut tx = pool.begin().await?;

        // Insert business data
        let order_id: i64 =
            sqlx::query_scalar("INSERT INTO demo_orders (email) VALUES ($1) RETURNING id")
                .bind("alice@example.com")
                .fetch_one(&mut *tx)
                .await?;

        // Enqueue the notification job in the same transaction.
        // The job is stored in asyncq_jobs with status = 'pending',
        // but is invisible to workers until this transaction commits.
        let payload = serde_json::to_vec(&json!({ "order_id": order_id, "email": "alice@example.com" }))?;
        backend
            .enqueue_in_tx("SendOrderConfirmation", "orders", payload, 3, Utc::now(), &mut tx)
            .await?;

        tx.commit().await?;
        println!("  Transaction committed — job is now pending.");
    }

    // ── Scenario 2: rolled-back transaction ───────────────────────────────────
    println!("\nScenario 2: rolled-back transaction → job disappears");
    {
        let before = queue.stats("orders").await?.pending;

        let mut tx = pool.begin().await?;

        let order_id: i64 =
            sqlx::query_scalar("INSERT INTO demo_orders (email) VALUES ($1) RETURNING id")
                .bind("bob@example.com")
                .fetch_one(&mut *tx)
                .await?;

        let payload = serde_json::to_vec(&json!({ "order_id": order_id, "email": "bob@example.com" }))?;
        backend
            .enqueue_in_tx("SendOrderConfirmation", "orders", payload, 3, Utc::now(), &mut tx)
            .await?;

        // Simulate a business logic failure → rollback
        tx.rollback().await?;

        let after = queue.stats("orders").await?.pending;
        assert_eq!(before, after, "rollback must not leave a phantom job");
        println!("  Transaction rolled back — pending count unchanged ({before} → {after}). ✓");
    }

    // ── Process the committed job ─────────────────────────────────────────────
    println!("\nProcessing pending jobs...");
    let processed = Worker::new(queue)
        .register::<SendOrderConfirmation>()
        .run_once()
        .await?;
    println!("Done — processed {processed} job(s).");

    // Cleanup
    sqlx::query("DROP TABLE IF EXISTS demo_orders")
        .execute(&pool)
        .await?;

    Ok(())
}
