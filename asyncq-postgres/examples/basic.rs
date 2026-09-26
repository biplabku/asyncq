//! Basic asyncq-postgres example — enqueue 5 jobs and process them.
//!
//! Run: cargo run -p asyncq-postgres --example basic
//! Requires: Postgres running at DATABASE_URL (default: postgres://hooksmith:hooksmith@localhost/hooksmith)

use asyncq::{Job, JobContext, JobResult, Perform, Queue, Worker};
use asyncq_postgres::PostgresBackend;
use serde::{Deserialize, Serialize};

fn db_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://hooksmith:hooksmith@localhost/hooksmith".to_string())
}

#[derive(Job, Serialize, Deserialize)]
#[job(queue = "greetings", retries = 3)]
struct Greet {
    name: String,
}

impl Perform for Greet {
    async fn perform(self, ctx: JobContext) -> JobResult {
        println!(
            "[attempt {}] Hello, {}! (job_id={})",
            ctx.attempt, self.name, ctx.job_id
        );
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let backend = PostgresBackend::new(&db_url()).await?;
    backend.migrate().await?;

    let queue = Queue::new(backend);

    // Enqueue 5 jobs
    let names = ["Alice", "Bob", "Carol", "Dave", "Eve"];
    for name in names {
        queue.enqueue(Greet { name: name.to_owned() }).await?;
        println!("Enqueued: Greet({name})");
    }

    println!("\nProcessing...\n");

    let processed = Worker::new(queue)
        .register::<Greet>()
        .run_once()
        .await?;

    println!("\nDone — processed {processed} jobs.");
    Ok(())
}
