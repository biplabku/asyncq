//! Basic asyncq example — enqueue 5 jobs and process them.
//!
//! Run: cargo run --example basic
//! Requires: Redis on localhost:6379

use asyncq::{Job, Perform, JobContext, JobResult, Queue, Worker};
use asyncq_redis::RedisBackend;
use serde::{Serialize, Deserialize};

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

    let backend = RedisBackend::new("redis://127.0.0.1/").await?;
    let queue = Queue::new(backend);

    // Enqueue 5 jobs
    let names = ["Alice", "Bob", "Carol", "Dave", "Eve"];
    for name in names {
        queue.enqueue(Greet { name: name.to_owned() }).await?;
        println!("Enqueued: Greet({name})");
    }

    println!("\nProcessing...\n");

    // Process all pending jobs and return (run_once for demos; use run() in production)
    let processed = Worker::new(queue)
        .register::<Greet>()
        .run_once()
        .await?;

    println!("\nDone — processed {processed} jobs.");
    Ok(())
}
