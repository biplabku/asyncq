//! Admin API example — mount asyncq-axum's admin router, enqueue a few jobs
//! with `InMemoryBackend`, and inspect them over HTTP.
//!
//! Run: cargo run --example admin_server
//! Then in another terminal:
//!   curl http://127.0.0.1:8080/admin/queues/greetings
//!   curl http://127.0.0.1:8080/admin/queues/greetings/dlq
//!   curl http://127.0.0.1:8080/admin/metrics

use asyncq::{backends::InMemoryBackend, Job, JobContext, JobResult, Perform, Queue, Worker};
use asyncq_axum::admin_with_queues;
use axum::Router;
use serde::{Deserialize, Serialize};

#[derive(Job, Serialize, Deserialize)]
#[job(queue = "greetings", retries = 3)]
struct Greet {
    name: String,
}

impl Perform for Greet {
    async fn perform(self, _ctx: JobContext) -> JobResult {
        println!("Hello, {}!", self.name);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let backend = InMemoryBackend::new();
    let queue = Queue::new(backend);

    // Enqueue a few jobs so the admin endpoints have something to show.
    for name in ["Alice", "Bob", "Carol"] {
        queue
            .enqueue(Greet { name: name.to_owned() })
            .await?;
    }

    // Process them once so stats/metrics reflect completed jobs too.
    Worker::new(queue.clone()).register::<Greet>().run_once().await?;

    let app = Router::new().nest(
        "/admin",
        admin_with_queues(queue, vec!["greetings".into()]),
    );

    println!("Admin API listening on http://127.0.0.1:8080/admin");
    println!("Try: curl http://127.0.0.1:8080/admin/queues/greetings");
    println!("     curl http://127.0.0.1:8080/admin/metrics");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    axum::serve(listener, app).await?;
    Ok(())
}
