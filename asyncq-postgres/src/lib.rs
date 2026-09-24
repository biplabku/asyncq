//! PostgreSQL backend for asyncq.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use asyncq::{Queue, Worker, Job, Perform, JobContext, JobResult};
//! use asyncq_postgres::PostgresBackend;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Job, Serialize, Deserialize)]
//! #[job(queue = "emails", retries = 3)]
//! struct SendEmail { to: String }
//!
//! impl Perform for SendEmail {
//!     async fn perform(self, _ctx: JobContext) -> JobResult {
//!         println!("Sending to {}", self.to);
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let backend = PostgresBackend::new("postgres://user:pass@localhost/mydb").await.unwrap();
//!     backend.migrate().await.unwrap();
//!
//!     let queue = Queue::new(backend);
//!     queue.enqueue(SendEmail { to: "user@example.com".into() }).await.unwrap();
//!     Worker::new(queue).register::<SendEmail>().run().await;
//! }
//! ```
//!
//! # Transactional outbox
//!
//! PostgreSQL enables the transactional outbox pattern natively: enqueue a job
//! in the same transaction as your business logic.
//!
//! ```rust,ignore
//! # use asyncq::{Queue, JobRecord};
//! # use asyncq_postgres::PostgresBackend;
//! # async fn example(queue: Queue<PostgresBackend>, pool: sqlx::PgPool) -> anyhow::Result<()> {
//! let mut tx = pool.begin().await?;
//! sqlx::query!("INSERT INTO orders ...").execute(&mut *tx).await?;
//! // Job is enqueued in the same transaction — rolls back if the business logic fails
//! queue.enqueue_in_tx(
//!     "order.created",
//!     serde_json::json!({"order_id": 1}),
//!     &mut tx,
//! ).await?;
//! tx.commit().await?;
//! # Ok(())
//! # }
//! ```

use std::time::Duration;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, postgres::PgPoolOptions, Row};
use uuid::Uuid;

use asyncq::{
    backend::Backend,
    error::{Error, Result},
    job::{JobId, JobRecord, QueueStats},
};

const MIGRATION_SQL: &str = include_str!("../migrations/0001_create_asyncq_jobs.sql");

/// PostgreSQL backend for asyncq.
///
/// Uses a connection pool (`sqlx::PgPool`) for concurrent job processing.
/// Call [`migrate()`](PostgresBackend::migrate) once at startup to create
/// the `asyncq_jobs` table.
#[derive(Clone)]
pub struct PostgresBackend {
    pool: PgPool,
}

impl PostgresBackend {
    /// Connect to PostgreSQL at `database_url` with default pool settings.
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(20)
            .connect(database_url)
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;
        Ok(Self { pool })
    }

    /// Build from an existing `sqlx::PgPool` (e.g. shared with your web app).
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Access the underlying connection pool (e.g. for cleanup in tests).
    pub fn pool_ref(&self) -> &PgPool {
        &self.pool
    }

    /// Create the `asyncq_jobs` table and indexes. Safe to call on every startup (idempotent).
    pub async fn migrate(&self) -> Result<()> {
        sqlx::raw_sql(MIGRATION_SQL)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Backend(format!("migration failed: {e}")))?;
        Ok(())
    }

    /// Enqueue a job inside an existing transaction (transactional outbox pattern).
    ///
    /// The job is only visible to workers after `tx.commit()`. If the transaction
    /// rolls back, the job disappears — no orphaned notifications.
    pub async fn enqueue_in_tx(
        &self,
        kind: &str,
        queue: &str,
        payload: Vec<u8>,
        max_attempts: u32,
        scheduled_at: DateTime<Utc>,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> Result<JobId> {
        let id = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO asyncq_jobs
               (id, kind, queue, payload, max_attempts, scheduled_at)
               VALUES ($1, $2, $3, $4, $5, $6)"#,
        )
        .bind(id)
        .bind(kind)
        .bind(queue)
        .bind(&payload)
        .bind(max_attempts as i32)
        .bind(scheduled_at)
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Backend(e.to_string()))?;
        Ok(id)
    }
}

#[async_trait]
impl Backend for PostgresBackend {
    async fn enqueue(&self, record: JobRecord) -> Result<JobId> {
        let id = record.id;
        sqlx::query(
            r#"INSERT INTO asyncq_jobs
               (id, kind, queue, payload, attempt, max_attempts, scheduled_at, created_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"#,
        )
        .bind(record.id)
        .bind(&record.kind)
        .bind(&record.queue)
        .bind(&record.payload)
        .bind(record.attempt as i32)
        .bind(record.max_attempts as i32)
        .bind(record.scheduled_at)
        .bind(record.created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Backend(e.to_string()))?;
        Ok(id)
    }

    async fn claim(&self, queues: &[&str], timeout: Duration) -> Result<Option<JobRecord>> {
        let deadline = tokio::time::Instant::now() + timeout;
        let sleep_ms = 50u64;

        loop {
            // Skip LOCK SKIP LOCKED — use a single UPDATE that atomically claims
            let row = sqlx::query(
                r#"UPDATE asyncq_jobs
                   SET status = 'running',
                       attempt = attempt + 1,
                       heartbeat_at = NOW()
                   WHERE id = (
                       SELECT id FROM asyncq_jobs
                       WHERE queue = ANY($1)
                         AND status = 'pending'
                         AND scheduled_at <= NOW()
                       ORDER BY scheduled_at ASC
                       LIMIT 1
                       FOR UPDATE SKIP LOCKED
                   )
                   RETURNING id, kind, queue, payload, attempt, max_attempts,
                             scheduled_at, created_at, last_error"#,
            )
            .bind(queues)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;

            if let Some(row) = row {
                return Ok(Some(row_to_record(row)?));
            }

            if timeout.is_zero() || tokio::time::Instant::now() >= deadline {
                return Ok(None);
            }
            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
        }
    }

    async fn ack(&self, id: JobId) -> Result<()> {
        sqlx::query("UPDATE asyncq_jobs SET status = 'completed' WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;
        Ok(())
    }

    async fn nack(&self, id: JobId, error: &str, retry_at: Option<DateTime<Utc>>) -> Result<()> {
        match retry_at {
            Some(at) => {
                sqlx::query(
                    r#"UPDATE asyncq_jobs
                       SET status = 'pending', scheduled_at = $2, last_error = $3,
                           heartbeat_at = NULL
                       WHERE id = $1"#,
                )
                .bind(id)
                .bind(at)
                .bind(error)
                .execute(&self.pool)
                .await
                .map_err(|e| Error::Backend(e.to_string()))?;
            }
            None => {
                sqlx::query(
                    r#"UPDATE asyncq_jobs
                       SET status = 'dead', last_error = $2, heartbeat_at = NULL
                       WHERE id = $1"#,
                )
                .bind(id)
                .bind(error)
                .execute(&self.pool)
                .await
                .map_err(|e| Error::Backend(e.to_string()))?;
            }
        }
        Ok(())
    }

    async fn heartbeat(&self, id: JobId) -> Result<()> {
        sqlx::query("UPDATE asyncq_jobs SET heartbeat_at = NOW() WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;
        Ok(())
    }

    async fn reap_stuck(&self, older_than: Duration) -> Result<u64> {
        let cutoff = Utc::now()
            - chrono::Duration::from_std(older_than).unwrap_or(chrono::Duration::seconds(120));
        let result = sqlx::query(
            r#"UPDATE asyncq_jobs
               SET status = 'pending', heartbeat_at = NULL
               WHERE status = 'running' AND heartbeat_at < $1"#,
        )
        .bind(cutoff)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Backend(e.to_string()))?;
        Ok(result.rows_affected())
    }

    async fn stats(&self, queue: &str) -> Result<QueueStats> {
        let row = sqlx::query(
            r#"SELECT
               COUNT(*) FILTER (WHERE status = 'pending')   AS pending,
               COUNT(*) FILTER (WHERE status = 'running')   AS running,
               COUNT(*) FILTER (WHERE status = 'completed') AS completed,
               COUNT(*) FILTER (WHERE status = 'failed')    AS failed,
               COUNT(*) FILTER (WHERE status = 'dead')      AS dead
               FROM asyncq_jobs WHERE queue = $1"#,
        )
        .bind(queue)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| Error::Backend(e.to_string()))?;

        Ok(QueueStats {
            queue: queue.to_owned(),
            pending:   row.get::<i64, _>("pending")   as u64,
            running:   row.get::<i64, _>("running")   as u64,
            completed: row.get::<i64, _>("completed") as u64,
            failed:    row.get::<i64, _>("failed")    as u64,
            dead:      row.get::<i64, _>("dead")      as u64,
        })
    }

    async fn dead_jobs(&self, queue: &str, limit: i64, offset: i64) -> Result<Vec<JobRecord>> {
        let rows = sqlx::query(
            r#"SELECT id, kind, queue, payload, attempt, max_attempts,
                      scheduled_at, created_at, last_error
               FROM asyncq_jobs
               WHERE queue = $1 AND status = 'dead'
               ORDER BY created_at DESC
               LIMIT $2 OFFSET $3"#,
        )
        .bind(queue)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::Backend(e.to_string()))?;

        rows.into_iter().map(row_to_record).collect()
    }

    async fn retry_dead(&self, id: JobId) -> Result<()> {
        let result = sqlx::query(
            r#"UPDATE asyncq_jobs
               SET status = 'pending', scheduled_at = NOW(),
                   last_error = NULL, heartbeat_at = NULL
               WHERE id = $1 AND status = 'dead'"#,
        )
        .bind(id)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Backend(e.to_string()))?;

        if result.rows_affected() == 0 {
            return Err(Error::NotFound(id.to_string()));
        }
        Ok(())
    }

    async fn retry_all_dead(&self, queue: &str) -> Result<u64> {
        let result = sqlx::query(
            r#"UPDATE asyncq_jobs
               SET status = 'pending', scheduled_at = NOW(),
                   last_error = NULL, heartbeat_at = NULL
               WHERE queue = $1 AND status = 'dead'"#,
        )
        .bind(queue)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Backend(e.to_string()))?;
        Ok(result.rows_affected())
    }
}

// ── Row mapping ───────────────────────────────────────────────────────────────

fn row_to_record(row: sqlx::postgres::PgRow) -> Result<JobRecord> {
    Ok(JobRecord {
        id:           row.get("id"),
        kind:         row.get("kind"),
        queue:        row.get("queue"),
        payload:      row.get("payload"),
        attempt:      row.get::<i32, _>("attempt") as u32,
        max_attempts: row.get::<i32, _>("max_attempts") as u32,
        scheduled_at: row.get("scheduled_at"),
        created_at:   row.get("created_at"),
        last_error:   row.get("last_error"),
    })
}
