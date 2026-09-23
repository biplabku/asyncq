//! Background job processing for Rust.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use asyncq::{Job, Perform, JobContext, JobResult, Queue, Worker};
//! use asyncq::backends::InMemoryBackend;
//! use serde::{Serialize, Deserialize};
//!
//! // 1. Define your job payload and metadata
//! #[derive(Job, Serialize, Deserialize)]
//! #[job(queue = "emails", retries = 3)]
//! struct WelcomeEmail {
//!     email: String,
//! }
//!
//! // 2. Implement the work
//! impl Perform for WelcomeEmail {
//!     async fn perform(self, _ctx: JobContext) -> JobResult {
//!         println!("sending welcome email to {}", self.email);
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     // 3. Create a queue (swap InMemoryBackend for RedisBackend in production)
//!     let queue = Queue::new(InMemoryBackend::new());
//!
//!     // 4. Enqueue from anywhere
//!     queue.enqueue(WelcomeEmail { email: "user@example.com".into() }).await.unwrap();
//!
//!     // 5. Process
//!     Worker::new(queue)
//!         .register::<WelcomeEmail>()
//!         .concurrency(10)
//!         .run()
//!         .await;
//! }
//! ```
//!
//! # Testing without infrastructure
//!
//! Use [`backends::InMemoryBackend`] with [`Worker::run_once`] for fast,
//! infrastructure-free tests:
//!
//! ```rust,no_run
//! use asyncq::{Queue, Worker, backends::InMemoryBackend};
//!
//! #[tokio::test]
//! async fn test_my_job() {
//!     let backend = InMemoryBackend::new();
//!     let queue = Queue::new(backend.clone());
//!
//!     queue.enqueue(WelcomeEmail { email: "test@test.com".into() }).await.unwrap();
//!
//!     Worker::new(queue).register::<WelcomeEmail>().run_once().await.unwrap();
//!
//!     assert_eq!(backend.completed_count("emails").await, 1);
//! }
//! ```
//!
//! # Shared state in jobs
//!
//! Pass your app state (database pool, config, etc.) via `Queue::with_state`.
//! Jobs access it through `ctx.state::<T>()` — the same mental model as axum:
//!
//! ```rust,ignore
//! use std::sync::Arc;
//!
//! let queue = Queue::new(backend).with_state(Arc::new(db_pool));
//!
//! impl Perform for WelcomeEmail {
//!     async fn perform(self, ctx: JobContext) -> JobResult {
//!         let db = ctx.state::<Arc<PgPool>>().expect("db not in state");
//!         // use db ...
//!         Ok(())
//!     }
//! }
//! ```

pub mod backend;
pub mod backends;
pub mod context;
pub mod error;
pub mod job;
pub mod queue;
pub mod worker;

// Flat re-exports — everything a user needs at the top level
pub use backend::Backend;
pub use backends::InMemoryBackend;
pub use context::JobContext;
pub use error::{Error, JobError, JobResult, Result};
pub use job::{Job, JobId, JobRecord, JobRecord as RawJobRecord, Perform, QueueStats};
pub use queue::Queue;
pub use worker::Worker;

// Re-export the derive macro so users only need `use asyncq::Job`
pub use asyncq_derive::Job;
