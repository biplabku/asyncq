//! Cron scheduling example.
//!
//! Run: cargo run -p asyncq-redis --example cron
//!
//! Requires Redis at localhost:6379:
//!   docker run -d -p 6379:6379 redis:7-alpine

use asyncq::{Job, JobContext, JobResult, Perform, Queue, Scheduler, Worker};
use asyncq_redis::RedisBackend;
use serde::{Deserialize, Serialize};

/// A job that runs every 10 seconds.
#[derive(Job, Serialize, Deserialize, Default, Debug)]
#[job(queue = "cron", retries = 3)]
struct HeartbeatJob;

impl Perform for HeartbeatJob {
    async fn perform(self, ctx: JobContext) -> JobResult {
        println!(
            "[{}] heartbeat (attempt {})",
            chrono::Utc::now().format("%H:%M:%S"),
            ctx.attempt
        );
        Ok(())
    }
}

/// A job that runs at the start of every minute.
#[derive(Job, Serialize, Deserialize, Default, Debug)]
#[job(queue = "cron", retries = 3)]
struct MinuteJob;

impl Perform for MinuteJob {
    async fn perform(self, _ctx: JobContext) -> JobResult {
        println!(
            "[{}] minute tick!",
            chrono::Utc::now().format("%H:%M:%S"),
        );
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    // Initialize tracing for logs
    tracing_subscriber::fmt::init();

    let backend = RedisBackend::new("redis://127.0.0.1/")
        .await
        .expect("Redis connection failed");

    let queue = Queue::new(backend);

    // Create scheduler with cron jobs
    let scheduler = Scheduler::new(queue.clone())
        .register::<HeartbeatJob>("*/10 * * * * *")  // every 10 seconds
        .register::<MinuteJob>("0 * * * * *");       // every minute at :00

    // Create worker to process the jobs
    let worker = Worker::new(queue)
        .register::<HeartbeatJob>()
        .register::<MinuteJob>()
        .concurrency(2);

    println!("Starting scheduler and worker...");
    println!("  HeartbeatJob: every 10 seconds");
    println!("  MinuteJob: every minute at :00");
    println!("Press Ctrl-C to stop.\n");

    // Run both concurrently
    tokio::select! {
        _ = scheduler.run() => {}
        _ = worker.run() => {}
    }
}
