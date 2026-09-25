//! Actix-web integration for asyncq — admin REST endpoints with Prometheus metrics.
//!
//! # Setup
//!
//! Mount the admin scope in your actix-web application:
//!
//! ```rust,no_run
//! use asyncq::{Queue, backends::InMemoryBackend};
//! use asyncq_actix::admin;
//! use actix_web::{web, App, HttpServer};
//!
//! #[actix_web::main]
//! async fn main() -> std::io::Result<()> {
//!     let queue = Queue::new(InMemoryBackend::new());
//!
//!     HttpServer::new(move || {
//!         App::new()
//!             .service(web::scope("/admin").configure(admin(queue.clone())))
//!     })
//!     .bind("127.0.0.1:8080")?
//!     .run()
//!     .await
//! }
//! ```
//!
//! For live Prometheus metrics, use [`admin_with_queues`]:
//!
//! ```rust,no_run
//! use asyncq_actix::admin_with_queues;
//! # use asyncq::{Queue, backends::InMemoryBackend};
//! # use actix_web::{web, App};
//! # let queue = Queue::new(InMemoryBackend::new());
//! App::new()
//!     .service(
//!         web::scope("/admin")
//!             .configure(admin_with_queues(queue, vec!["emails".into(), "payments".into()]))
//!     );
//! ```
//!
//! # Endpoints
//!
//! | Method | Path | Description |
//! |--------|------|-------------|
//! | GET | `/admin/queues/{name}` | Stats for a queue |
//! | GET | `/admin/queues/{name}/dlq` | Dead-letter queue (paginated) |
//! | POST | `/admin/queues/{name}/dlq/retry-all` | Requeue all dead jobs |
//! | DELETE | `/admin/queues/{name}/dlq/{id}` | Retry one dead job by ID |
//! | GET | `/admin/metrics` | Prometheus text metrics |

use actix_web::{
    HttpResponse, Responder,
    web::{self, Data, Path, Query, ServiceConfig},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use asyncq::{backend::Backend, job::QueueStats, Queue};

// ── State ─────────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AdminState<B: Backend> {
    queue: Queue<B>,
    monitored_queues: Vec<String>,
}

// ── Admin configurator ────────────────────────────────────────────────────────

/// Returns an actix-web configurator that mounts admin endpoints.
/// Apply to a scope: `web::scope("/admin").configure(admin(queue))`.
///
/// The `/metrics` endpoint returns a minimal Prometheus stub.
/// Use [`admin_with_queues`] for live per-queue gauges.
pub fn admin<B: Backend + Clone + 'static>(
    queue: Queue<B>,
) -> impl Fn(&mut ServiceConfig) + Clone {
    admin_with_queues(queue, vec![])
}

/// Returns an actix-web configurator with live Prometheus metrics for the given queues.
pub fn admin_with_queues<B: Backend + Clone + 'static>(
    queue: Queue<B>,
    queues: Vec<String>,
) -> impl Fn(&mut ServiceConfig) + Clone {
    let state = Data::new(AdminState { queue, monitored_queues: queues });

    move |cfg: &mut ServiceConfig| {
        cfg.app_data(state.clone())
            .route("/queues/{name}", web::get().to(queue_stats::<B>))
            .route("/queues/{name}/dlq", web::get().to(list_dlq::<B>))
            .route("/queues/{name}/dlq/retry-all", web::post().to(retry_all::<B>))
            .route("/queues/{name}/dlq/{id}", web::delete().to(retry_one::<B>))
            .route("/metrics", web::get().to(metrics::<B>));
    }
}

// ── Handlers ──────────────────────────────────────────────────────────────────

async fn queue_stats<B: Backend + Clone + 'static>(
    path: Path<String>,
    state: Data<AdminState<B>>,
) -> impl Responder {
    match state.queue.stats(&path.into_inner()).await {
        Ok(stats) => HttpResponse::Ok().json(StatsResponse::from(stats)),
        Err(e) => HttpResponse::InternalServerError()
            .json(serde_json::json!({ "error": e.to_string() })),
    }
}

#[derive(Deserialize)]
struct Pagination {
    #[serde(default = "default_limit")]
    limit: i64,
    #[serde(default)]
    offset: i64,
}
fn default_limit() -> i64 { 50 }

async fn list_dlq<B: Backend + Clone + 'static>(
    path: Path<String>,
    query: Query<Pagination>,
    state: Data<AdminState<B>>,
) -> impl Responder {
    match state.queue.dead_jobs(&path.into_inner(), query.limit, query.offset).await {
        Ok(jobs) => {
            let items: Vec<DeadJobResponse> =
                jobs.into_iter().map(DeadJobResponse::from).collect();
            let count = items.len();
            HttpResponse::Ok().json(serde_json::json!({ "jobs": items, "count": count }))
        }
        Err(e) => HttpResponse::InternalServerError()
            .json(serde_json::json!({ "error": e.to_string() })),
    }
}

async fn retry_all<B: Backend + Clone + 'static>(
    path: Path<String>,
    state: Data<AdminState<B>>,
) -> impl Responder {
    match state.queue.retry_all_dead(&path.into_inner()).await {
        Ok(n) => HttpResponse::Ok().json(serde_json::json!({ "requeued": n })),
        Err(e) => HttpResponse::InternalServerError()
            .json(serde_json::json!({ "error": e.to_string() })),
    }
}

async fn retry_one<B: Backend + Clone + 'static>(
    path: Path<(String, String)>,
    state: Data<AdminState<B>>,
) -> impl Responder {
    let (_, id_str) = path.into_inner();
    let id = match Uuid::parse_str(&id_str) {
        Ok(id) => id,
        Err(_) => {
            return HttpResponse::BadRequest()
                .json(serde_json::json!({ "error": "invalid job id" }));
        }
    };
    match state.queue.retry_dead(id).await {
        Ok(()) => HttpResponse::NoContent().finish(),
        Err(e) => HttpResponse::NotFound()
            .json(serde_json::json!({ "error": e.to_string() })),
    }
}

async fn metrics<B: Backend + Clone + 'static>(
    state: Data<AdminState<B>>,
) -> impl Responder {
    let mut all_stats: Vec<(String, QueueStats)> = Vec::new();
    for q in &state.monitored_queues {
        if let Ok(stats) = state.queue.stats(q).await {
            all_stats.push((q.clone(), stats));
        }
    }

    let mut lines: Vec<String> = Vec::new();

    lines.push("# HELP asyncq_jobs_pending Jobs waiting to be processed".into());
    lines.push("# TYPE asyncq_jobs_pending gauge".into());
    for (q, st) in &all_stats {
        lines.push(format!("asyncq_jobs_pending{{queue=\"{}\"}} {}", q, st.pending));
    }

    lines.push("".into());
    lines.push("# HELP asyncq_jobs_running Jobs currently being processed".into());
    lines.push("# TYPE asyncq_jobs_running gauge".into());
    for (q, st) in &all_stats {
        lines.push(format!("asyncq_jobs_running{{queue=\"{}\"}} {}", q, st.running));
    }

    lines.push("".into());
    lines.push("# HELP asyncq_jobs_dead Jobs in the dead-letter queue".into());
    lines.push("# TYPE asyncq_jobs_dead gauge".into());
    for (q, st) in &all_stats {
        lines.push(format!("asyncq_jobs_dead{{queue=\"{}\"}} {}", q, st.dead));
    }

    lines.push("".into());
    lines.push("# HELP asyncq_jobs_completed Jobs completed successfully".into());
    lines.push("# TYPE asyncq_jobs_completed counter".into());
    for (q, st) in &all_stats {
        lines.push(format!("asyncq_jobs_completed{{queue=\"{}\"}} {}", q, st.completed));
    }

    lines.push("".into());
    lines.push("# HELP asyncq_info asyncq version info".into());
    lines.push("# TYPE asyncq_info gauge".into());
    lines.push(format!(
        "asyncq_info{{version=\"{}\"}} 1",
        env!("CARGO_PKG_VERSION")
    ));
    lines.push("".into());

    HttpResponse::Ok()
        .content_type("text/plain; version=0.0.4; charset=utf-8")
        .body(lines.join("\n"))
}

// ── Response types ────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct StatsResponse {
    queue: String,
    pending: u64,
    running: u64,
    completed: u64,
    failed: u64,
    dead: u64,
}

impl From<QueueStats> for StatsResponse {
    fn from(s: QueueStats) -> Self {
        Self {
            queue: s.queue,
            pending: s.pending,
            running: s.running,
            completed: s.completed,
            failed: s.failed,
            dead: s.dead,
        }
    }
}

#[derive(Serialize)]
struct DeadJobResponse {
    id: String,
    kind: String,
    queue: String,
    attempt: u32,
    max_attempts: u32,
    last_error: Option<String>,
    created_at: String,
}

impl From<asyncq::job::JobRecord> for DeadJobResponse {
    fn from(r: asyncq::job::JobRecord) -> Self {
        Self {
            id: r.id.to_string(),
            kind: r.kind,
            queue: r.queue,
            attempt: r.attempt,
            max_attempts: r.max_attempts,
            last_error: r.last_error,
            created_at: r.created_at.to_rfc3339(),
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{test, web, App};
    use asyncq::{
        backends::InMemoryBackend, Job, JobContext, JobResult, Perform, Queue, Worker,
    };
    use serde::{Deserialize, Serialize};

    #[derive(Job, Serialize, Deserialize)]
    #[job(queue = "actix-test", retries = 1)]
    struct TestJob;

    impl Perform for TestJob {
        async fn perform(self, _ctx: JobContext) -> JobResult {
            Err(asyncq::error::JobError::discard("test discard"))
        }
    }

    async fn test_app(queue: Queue<InMemoryBackend>) -> impl actix_web::dev::Service<
        actix_http::Request,
        Response = actix_web::dev::ServiceResponse,
        Error = actix_web::Error,
    > {
        test::init_service(
            App::new().service(
                web::scope("/admin").configure(admin(queue))
            )
        ).await
    }

    #[actix_rt::test]
    async fn stats_returns_200() {
        let queue = Queue::new(InMemoryBackend::new());
        let app = test_app(queue).await;
        let req = test::TestRequest::get()
            .uri("/admin/queues/actix-test")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);
    }

    #[actix_rt::test]
    async fn dlq_empty_returns_200_with_zero_count() {
        let queue = Queue::new(InMemoryBackend::new());
        let app = test_app(queue).await;
        let req = test::TestRequest::get()
            .uri("/admin/queues/actix-test/dlq")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["count"], 0);
    }

    #[actix_rt::test]
    async fn dlq_shows_dead_jobs() {
        let backend = InMemoryBackend::new();
        let queue = Queue::new(backend.clone());
        let app = test_app(queue.clone()).await;

        queue.enqueue(TestJob).await.unwrap();
        Worker::new(queue).register::<TestJob>().run_once().await.unwrap();

        let req = test::TestRequest::get()
            .uri("/admin/queues/actix-test/dlq")
            .to_request();
        let resp = test::call_service(&app, req).await;
        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["count"], 1, "DLQ must show the discarded job");
        assert_eq!(body["jobs"][0]["kind"], "TestJob");
    }

    #[actix_rt::test]
    async fn retry_all_requeues_from_dlq() {
        let backend = InMemoryBackend::new();
        let queue = Queue::new(backend.clone());
        let app = test_app(queue.clone()).await;

        queue.enqueue(TestJob).await.unwrap();
        Worker::new(queue).register::<TestJob>().run_once().await.unwrap();
        assert_eq!(backend.dead_count("actix-test").await, 1);

        let req = test::TestRequest::post()
            .uri("/admin/queues/actix-test/dlq/retry-all")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["requeued"], 1);
        assert_eq!(backend.dead_count("actix-test").await, 0);
    }

    #[actix_rt::test]
    async fn metrics_returns_prometheus_format() {
        let queue = Queue::new(InMemoryBackend::new());
        let app = test_app(queue).await;
        let req = test::TestRequest::get()
            .uri("/admin/metrics")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);
        let ct = resp.headers().get("content-type").unwrap().to_str().unwrap();
        assert!(ct.contains("text/plain"));
        let body = test::read_body(resp).await;
        let text = std::str::from_utf8(&body).unwrap();
        assert!(text.contains("asyncq_info{version="));
    }

    #[actix_rt::test]
    async fn metrics_with_queues_shows_live_data() {
        let backend = InMemoryBackend::new();
        let queue = Queue::new(backend.clone());
        let app = test::init_service(
            App::new().service(
                web::scope("/admin")
                    .configure(admin_with_queues(queue.clone(), vec!["actix-test".into()]))
            )
        ).await;

        queue.enqueue(TestJob).await.unwrap();
        queue.enqueue(TestJob).await.unwrap();

        let req = test::TestRequest::get()
            .uri("/admin/metrics")
            .to_request();
        let resp = test::call_service(&app, req).await;
        let body = test::read_body(resp).await;
        let text = std::str::from_utf8(&body).unwrap();
        assert!(text.contains("asyncq_jobs_pending{queue=\"actix-test\"} 2"), "pending must be 2");
        assert!(text.contains("asyncq_jobs_dead{queue=\"actix-test\"}"));
    }
}
