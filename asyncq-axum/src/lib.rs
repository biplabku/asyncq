//! Axum integration for asyncq — admin REST endpoints with Prometheus metrics.
//!
//! # Setup
//!
//! Mount the admin router on any path in your axum application:
//!
//! ```rust,no_run
//! use asyncq::{Queue, backends::InMemoryBackend};
//! use asyncq_axum::admin;
//! use axum::Router;
//!
//! let queue = Queue::new(InMemoryBackend::new());
//! let app = Router::new()
//!     .nest("/admin", admin(queue))
//!     /* ... your other routes ... */;
//! ```
//!
//! # Endpoints
//!
//! | Method | Path | Description |
//! |--------|------|-------------|
//! | GET | `/admin/queues/:name` | Stats for a queue |
//! | GET | `/admin/queues/:name/dlq` | Dead-letter queue (paginated) |
//! | POST | `/admin/queues/:name/dlq/retry-all` | Requeue all dead jobs |
//! | DELETE | `/admin/queues/:name/dlq/:id` | Retry one dead job by ID |
//! | GET | `/admin/metrics` | Prometheus text metrics |

use std::sync::Arc;
use axum::{
    Router,
    extract::{Path, Query, State},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json,
};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use asyncq::{backend::Backend, job::QueueStats, Queue};

// ── State ─────────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AdminState<B: Backend> {
    queue: Queue<B>,
    /// Queue names exposed in /metrics. Empty = metrics stub only.
    monitored_queues: Vec<String>,
}

// ── Admin router ──────────────────────────────────────────────────────────────

/// Build the admin Router. Mount it with `router.nest("/admin", admin(queue))`.
///
/// The `/metrics` endpoint returns a minimal Prometheus stub. To get live
/// per-queue gauges, use [`admin_with_queues`] instead.
pub fn admin<B: Backend + Clone + 'static>(queue: Queue<B>) -> Router {
    admin_with_queues(queue, vec![])
}

/// Build the admin Router with live Prometheus metrics for the given queue names.
///
/// ```rust,no_run
/// use asyncq::{Queue, backends::InMemoryBackend};
/// use asyncq_axum::admin_with_queues;
/// use axum::Router;
///
/// let queue = Queue::new(InMemoryBackend::new());
/// let app = Router::new()
///     .nest("/admin", admin_with_queues(queue, vec!["emails".into(), "payments".into()]));
/// ```
///
/// The `/metrics` endpoint will then expose:
/// ```text
/// asyncq_jobs_pending{queue="emails"} 3
/// asyncq_jobs_running{queue="emails"} 1
/// asyncq_jobs_dead{queue="emails"} 0
/// ```
pub fn admin_with_queues<B: Backend + Clone + 'static>(
    queue: Queue<B>,
    queues: Vec<String>,
) -> Router {
    let state = Arc::new(AdminState { queue, monitored_queues: queues });
    Router::new()
        .route("/queues/:name",              get(queue_stats))
        .route("/queues/:name/dlq",          get(list_dlq))
        .route("/queues/:name/dlq/retry-all",post(retry_all))
        .route("/queues/:name/dlq/:id",      delete(retry_one))
        .route("/metrics",                   get(metrics))
        .with_state(state)
}

// ── Handlers ──────────────────────────────────────────────────────────────────

async fn queue_stats<B: Backend + Clone + 'static>(
    Path(name): Path<String>,
    State(s): State<Arc<AdminState<B>>>,
) -> impl IntoResponse {
    match s.queue.stats(&name).await {
        Ok(stats) => Json(StatsResponse::from(stats)).into_response(),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
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
    Path(name): Path<String>,
    Query(p): Query<Pagination>,
    State(s): State<Arc<AdminState<B>>>,
) -> impl IntoResponse {
    match s.queue.dead_jobs(&name, p.limit, p.offset).await {
        Ok(jobs) => {
            let items: Vec<DeadJobResponse> = jobs.into_iter().map(DeadJobResponse::from).collect();
            Json(serde_json::json!({ "jobs": items, "count": items.len() })).into_response()
        }
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

async fn retry_all<B: Backend + Clone + 'static>(
    Path(name): Path<String>,
    State(s): State<Arc<AdminState<B>>>,
) -> impl IntoResponse {
    match s.queue.retry_all_dead(&name).await {
        Ok(n) => Json(serde_json::json!({ "requeued": n })).into_response(),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

async fn retry_one<B: Backend + Clone + 'static>(
    Path((name, id_str)): Path<(String, String)>,
    State(s): State<Arc<AdminState<B>>>,
) -> impl IntoResponse {
    let id = match Uuid::parse_str(&id_str) {
        Ok(id) => id,
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "invalid job id"),
    };
    let _ = name; // queue is implicit from the job record
    match s.queue.retry_dead(id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => error_response(StatusCode::NOT_FOUND, &e.to_string()),
    }
}

async fn metrics<B: Backend + Clone + 'static>(
    State(s): State<Arc<AdminState<B>>>,
) -> impl IntoResponse {
    let mut lines: Vec<String> = Vec::new();

    // Collect live stats for each monitored queue
    let mut all_stats: Vec<(String, QueueStats)> = Vec::new();
    for q in &s.monitored_queues {
        if let Ok(stats) = s.queue.stats(q).await {
            all_stats.push((q.clone(), stats));
        }
    }

    // pending
    lines.push("# HELP asyncq_jobs_pending Jobs waiting to be processed".into());
    lines.push("# TYPE asyncq_jobs_pending gauge".into());
    for (q, st) in &all_stats {
        lines.push(format!("asyncq_jobs_pending{{queue=\"{}\"}} {}", q, st.pending));
    }

    // running
    lines.push("".into());
    lines.push("# HELP asyncq_jobs_running Jobs currently being processed".into());
    lines.push("# TYPE asyncq_jobs_running gauge".into());
    for (q, st) in &all_stats {
        lines.push(format!("asyncq_jobs_running{{queue=\"{}\"}} {}", q, st.running));
    }

    // dead
    lines.push("".into());
    lines.push("# HELP asyncq_jobs_dead Jobs in the dead-letter queue".into());
    lines.push("# TYPE asyncq_jobs_dead gauge".into());
    for (q, st) in &all_stats {
        lines.push(format!("asyncq_jobs_dead{{queue=\"{}\"}} {}", q, st.dead));
    }

    // completed
    lines.push("".into());
    lines.push("# HELP asyncq_jobs_completed Jobs completed successfully".into());
    lines.push("# TYPE asyncq_jobs_completed counter".into());
    for (q, st) in &all_stats {
        lines.push(format!("asyncq_jobs_completed{{queue=\"{}\"}} {}", q, st.completed));
    }

    // version info
    lines.push("".into());
    lines.push("# HELP asyncq_info asyncq version info".into());
    lines.push("# TYPE asyncq_info gauge".into());
    lines.push(format!("asyncq_info{{version=\"{}\"}} 1", env!("CARGO_PKG_VERSION")));

    lines.push("".into());
    let body = lines.join("\n");

    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
        .into_response()
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

fn error_response(status: StatusCode, message: &str) -> Response {
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use asyncq::{backends::InMemoryBackend, Job, JobContext, JobResult, Perform, Queue, Worker, JobRecord};
    use axum::{body::Body, http::Request};
    use serde::{Deserialize, Serialize};

    #[derive(Job, Serialize, Deserialize)]
    #[job(queue = "admin-test", retries = 1)]
    struct TestJob;

    impl Perform for TestJob {
        async fn perform(self, _ctx: JobContext) -> JobResult {
            Err(asyncq::error::JobError::discard("test discard"))
        }
    }

    fn test_app() -> (Router, Queue<InMemoryBackend>) {
        let backend = InMemoryBackend::new();
        let queue = Queue::new(backend);
        let app = Router::new().nest("/admin", admin(queue.clone()));
        (app, queue)
    }

    async fn call(app: Router, req: Request<Body>) -> axum::response::Response {
        use tower::ServiceExt as _;
        app.oneshot(req).await.unwrap()
    }

    #[tokio::test]
    async fn stats_returns_200() {
        let (app, _) = test_app();
        let req = Request::builder()
            .uri("/admin/queues/admin-test")
            .body(Body::empty())
            .unwrap();
        let resp = call(app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn dlq_empty_returns_200() {
        let (app, _) = test_app();
        let req = Request::builder()
            .uri("/admin/queues/admin-test/dlq")
            .body(Body::empty())
            .unwrap();
        let resp = call(app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["count"], 0);
    }

    #[tokio::test]
    async fn dlq_shows_dead_jobs() {
        let backend = InMemoryBackend::new();
        let queue = Queue::new(backend.clone());
        let app = Router::new().nest("/admin", admin(queue.clone()));

        // Enqueue and discard a job → it lands in DLQ
        queue.enqueue(TestJob).await.unwrap();
        Worker::new(queue.clone()).register::<TestJob>().run_once().await.unwrap();

        let req = Request::builder()
            .uri("/admin/queues/admin-test/dlq")
            .body(Body::empty())
            .unwrap();
        let resp = call(app, req).await;
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["count"], 1, "DLQ must show the discarded job");
        assert_eq!(json["jobs"][0]["kind"], "TestJob");
    }

    #[tokio::test]
    async fn retry_all_requeues_from_dlq() {
        let backend = InMemoryBackend::new();
        let queue = Queue::new(backend.clone());
        let app = Router::new().nest("/admin", admin(queue.clone()));

        queue.enqueue(TestJob).await.unwrap();
        Worker::new(queue.clone()).register::<TestJob>().run_once().await.unwrap();
        assert_eq!(backend.dead_count("admin-test").await, 1);

        let req = Request::builder()
            .method("POST")
            .uri("/admin/queues/admin-test/dlq/retry-all")
            .body(Body::empty())
            .unwrap();
        let resp = call(app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["requeued"], 1);
        assert_eq!(backend.dead_count("admin-test").await, 0);
    }

    #[tokio::test]
    async fn retry_one_by_id() {
        let backend = InMemoryBackend::new();
        let queue = Queue::new(backend.clone());
        let app = Router::new().nest("/admin", admin(queue.clone()));

        let payload = serde_json::to_vec(&TestJob).unwrap();
        let id = queue.enqueue_record(JobRecord::new("TestJob", "admin-test", payload, 1))
            .await.unwrap();
        Worker::new(queue.clone()).register::<TestJob>().run_once().await.unwrap();
        assert_eq!(backend.dead_count("admin-test").await, 1);

        let req = Request::builder()
            .method("DELETE")
            .uri(format!("/admin/queues/admin-test/dlq/{id}"))
            .body(Body::empty())
            .unwrap();
        let resp = call(app, req).await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert_eq!(backend.dead_count("admin-test").await, 0);
    }

    #[tokio::test]
    async fn metrics_returns_prometheus_format() {
        let (app, _) = test_app();
        let req = Request::builder()
            .uri("/admin/metrics")
            .body(Body::empty())
            .unwrap();
        let resp = call(app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp.headers()
            .get("content-type").unwrap()
            .to_str().unwrap();
        assert!(ct.contains("text/plain"), "metrics must be Prometheus text format");
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        assert!(text.contains("# HELP asyncq_info"), "must include HELP comment");
        assert!(text.contains("asyncq_info{version="), "must include version gauge");
    }

    #[tokio::test]
    async fn metrics_with_queues_shows_live_data() {
        let backend = InMemoryBackend::new();
        let queue = Queue::new(backend.clone());
        let app = Router::new().nest(
            "/admin",
            admin_with_queues(queue.clone(), vec!["admin-test".into()]),
        );

        // Enqueue two jobs so stats are non-zero
        queue.enqueue(TestJob).await.unwrap();
        queue.enqueue(TestJob).await.unwrap();

        let req = Request::builder()
            .uri("/admin/metrics")
            .body(Body::empty())
            .unwrap();
        let resp = call(app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();

        assert!(text.contains("asyncq_jobs_pending{queue=\"admin-test\"}"), "must contain pending gauge");
        assert!(text.contains("asyncq_jobs_dead{queue=\"admin-test\"}"), "must contain dead gauge");
        // 2 pending jobs
        assert!(text.contains("asyncq_jobs_pending{queue=\"admin-test\"} 2"), "pending count must be 2");
    }
}
