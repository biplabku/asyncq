//! Admin API example — mount asyncq-actix's admin scope, enqueue a few jobs
//! with `InMemoryBackend`, and inspect them over HTTP.
//!
//! Run: cargo run --example admin_server
//! Then in another terminal:
//!   curl http://127.0.0.1:8080/admin/queues/greetings
//!   curl http://127.0.0.1:8080/admin/queues/greetings/dlq
//!   curl http://127.0.0.1:8080/admin/metrics

use actix_web::{web, App, HttpServer};
use asyncq::{backends::InMemoryBackend, Job, JobContext, JobResult, Perform, Queue, Worker};
use asyncq_actix::admin_with_queues;
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

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let backend = InMemoryBackend::new();
    let queue = Queue::new(backend);

    // Enqueue a few jobs so the admin endpoints have something to show.
    for name in ["Alice", "Bob", "Carol"] {
        queue
            .enqueue(Greet { name: name.to_owned() })
            .await
            .expect("enqueue");
    }

    // Process them once so stats/metrics reflect completed jobs too.
    Worker::new(queue.clone())
        .register::<Greet>()
        .run_once()
        .await
        .expect("run_once");

    println!("Admin API listening on http://127.0.0.1:8080/admin");
    println!("Try: curl http://127.0.0.1:8080/admin/queues/greetings");
    println!("     curl http://127.0.0.1:8080/admin/metrics");

    HttpServer::new(move || {
        App::new().service(
            web::scope("/admin")
                .configure(admin_with_queues(queue.clone(), vec!["greetings".into()])),
        )
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
