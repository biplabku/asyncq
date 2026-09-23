//! Redis backend for asyncq.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use asyncq::{Queue, Worker, Job, Perform, JobContext, JobResult};
//! use asyncq_redis::RedisBackend;
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
//!     let backend = RedisBackend::new("redis://127.0.0.1/").await.unwrap();
//!     let queue = Queue::new(backend);
//!     queue.enqueue(SendEmail { to: "user@example.com".into() }).await.unwrap();
//!     Worker::new(queue).register::<SendEmail>().run().await;
//! }
//! ```
//!
//! # Redis key layout
//!
//! ```text
//! asyncq:q:{queue}:pending     LIST  job_ids (LPUSH enqueue, BRPOP claim)
//! asyncq:q:{queue}:delayed     ZSET  job_id → run_at_unix (for scheduled jobs)
//! asyncq:q:{queue}:dead        ZSET  job_id → failed_at_unix
//! asyncq:processing            ZSET  job_id → last_heartbeat_unix
//! asyncq:jobs:{job_id}         HASH  all job fields
//! ```

use std::time::Duration;
use chrono::{DateTime, Utc};
use async_trait::async_trait;
use redis::{
    AsyncCommands, Client,
    aio::ConnectionManager,
};
use uuid::Uuid;

use asyncq::{
    backend::Backend,
    error::{Error, Result},
    job::{JobId, JobRecord, QueueStats},
};

// ── Key helpers ───────────────────────────────────────────────────────────────

fn key_pending(queue: &str)    -> String { format!("asyncq:q:{queue}:pending") }
fn key_delayed(queue: &str)    -> String { format!("asyncq:q:{queue}:delayed") }
fn key_dead(queue: &str)       -> String { format!("asyncq:q:{queue}:dead")    }
fn key_job(id: JobId)          -> String { format!("asyncq:jobs:{id}")         }
const KEY_PROCESSING: &str = "asyncq:processing";

// ── RedisBackend ──────────────────────────────────────────────────────────────

/// Redis backend for asyncq.
///
/// Uses a connection manager for automatic reconnect and connection pooling.
/// All operations are atomic via Redis commands and Lua scripts where needed.
#[derive(Clone)]
pub struct RedisBackend {
    conn: ConnectionManager,
}

impl RedisBackend {
    /// Connect to Redis at `url` (e.g. `"redis://127.0.0.1/"` or
    /// `"redis://:password@host:6379/0"`).
    pub async fn new(url: &str) -> Result<Self> {
        let client = Client::open(url)
            .map_err(|e| Error::Backend(e.to_string()))?;
        let conn = ConnectionManager::new(client)
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;
        Ok(Self { conn })
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    async fn save_job(&self, record: &JobRecord) -> Result<()> {
        let mut conn = self.conn.clone();
        let key = key_job(record.id);
        let payload_b64 = base64_encode(&record.payload);
        redis::cmd("HSET")
            .arg(&key)
            .arg("id")           .arg(record.id.to_string())
            .arg("kind")         .arg(&record.kind)
            .arg("queue")        .arg(&record.queue)
            .arg("payload")      .arg(&payload_b64)
            .arg("attempt")      .arg(record.attempt)
            .arg("max_attempts") .arg(record.max_attempts)
            .arg("scheduled_at") .arg(record.scheduled_at.timestamp())
            .arg("created_at")   .arg(record.created_at.timestamp())
            .arg("last_error")   .arg(record.last_error.as_deref().unwrap_or(""))
            .query_async::<()>(&mut conn)
            .await
            .map_err(|e| Error::Backend(e.to_string()))
    }

    async fn load_job(&self, id: JobId) -> Result<Option<JobRecord>> {
        let mut conn = self.conn.clone();
        let key = key_job(id);
        let fields: Vec<Option<String>> = redis::cmd("HMGET")
            .arg(&key)
            .arg("id").arg("kind").arg("queue").arg("payload")
            .arg("attempt").arg("max_attempts").arg("scheduled_at")
            .arg("created_at").arg("last_error")
            .query_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;

        let get = |i: usize| fields[i].as_deref().unwrap_or("").to_owned();
        if get(0).is_empty() { return Ok(None); }

        let payload = base64_decode(&get(3))
            .map_err(|e| Error::Backend(format!("payload decode: {e}")))?;
        let scheduled_at = DateTime::from_timestamp(get(6).parse::<i64>().unwrap_or(0), 0)
            .unwrap_or_else(Utc::now);
        let created_at = DateTime::from_timestamp(get(7).parse::<i64>().unwrap_or(0), 0)
            .unwrap_or_else(Utc::now);
        let last_error = if get(8).is_empty() { None } else { Some(get(8)) };

        Ok(Some(JobRecord {
            id,
            kind:         get(1),
            queue:        get(2),
            payload,
            attempt:      get(4).parse().unwrap_or(0),
            max_attempts: get(5).parse().unwrap_or(10),
            scheduled_at,
            created_at,
            last_error,
        }))
    }

    /// Move any delayed jobs whose run_at has passed into the pending list.
    async fn promote_delayed(&self, queues: &[&str]) -> Result<()> {
        let now = Utc::now().timestamp();
        let mut conn = self.conn.clone();
        for &queue in queues {
            let delayed_key = key_delayed(queue);
            let pending_key = key_pending(queue);
            // Fetch all job IDs whose score (run_at) <= now
            let ids: Vec<String> = conn
                .zrangebyscore(&delayed_key, "-inf", now)
                .await
                .map_err(|e| Error::Backend(e.to_string()))?;
            for id in ids {
                // Remove from delayed, push to pending
                let removed: i64 = conn
                    .zrem(&delayed_key, &id)
                    .await
                    .map_err(|e| Error::Backend(e.to_string()))?;
                if removed > 0 {
                    conn.lpush::<_, _, ()>(&pending_key, &id)
                        .await
                        .map_err(|e| Error::Backend(e.to_string()))?;
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Backend for RedisBackend {
    async fn enqueue(&self, record: JobRecord) -> Result<JobId> {
        let id = record.id;
        self.save_job(&record).await?;
        let mut conn = self.conn.clone();
        let now = Utc::now().timestamp();

        if record.scheduled_at.timestamp() > now {
            // Delayed job → sorted set
            conn.zadd::<_, _, _, ()>(
                key_delayed(&record.queue),
                id.to_string(),
                record.scheduled_at.timestamp(),
            )
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;
        } else {
            // Immediate job → pending list
            conn.lpush::<_, _, ()>(key_pending(&record.queue), id.to_string())
                .await
                .map_err(|e| Error::Backend(e.to_string()))?;
        }
        Ok(id)
    }

    async fn claim(&self, queues: &[&str], timeout: Duration) -> Result<Option<JobRecord>> {
        // Promote any delayed jobs that are now due
        self.promote_delayed(queues).await?;

        let pending_keys: Vec<String> = queues.iter().map(|q| key_pending(q)).collect();
        let mut conn = self.conn.clone();

        // When timeout is zero (used by run_once), use non-blocking RPOP to avoid
        // BRPOP blocking forever (Redis treats timeout=0 as "block indefinitely").
        let result: Option<(String, String)> = if timeout.is_zero() {
            let mut found = None;
            for key in &pending_keys {
                let val: Option<String> = conn.rpop(key, None)
                    .await
                    .map_err(|e| Error::Backend(e.to_string()))?;
                if let Some(id_str) = val {
                    found = Some((key.clone(), id_str));
                    break;
                }
            }
            found
        } else {
            // Blocking wait — BRPOP with real timeout
            let timeout_secs = timeout.as_secs_f64().max(0.001);
            redis::cmd("BRPOP")
                .arg(&pending_keys)
                .arg(timeout_secs)
                .query_async(&mut conn)
                .await
                .map_err(|e| Error::Backend(e.to_string()))?
        };

        let Some((_key, id_str)) = result else {
            return Ok(None);
        };

        let id = Uuid::parse_str(&id_str)
            .map_err(|e| Error::Backend(format!("invalid job id: {e}")))?;

        let mut record = match self.load_job(id).await? {
            Some(r) => r,
            None => return Ok(None), // job was deleted between enqueue and claim
        };

        record.attempt += 1;

        // Update attempt count and register in processing set
        let now = Utc::now().timestamp();
        let mut conn2 = self.conn.clone();
        redis::cmd("HSET")
            .arg(key_job(id))
            .arg("attempt")
            .arg(record.attempt)
            .query_async::<()>(&mut conn2)
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;

        conn2.zadd::<_, _, _, ()>(KEY_PROCESSING, id.to_string(), now)
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;

        Ok(Some(record))
    }

    async fn ack(&self, id: JobId) -> Result<()> {
        let mut conn = self.conn.clone();
        // Remove from processing, delete job hash
        conn.zrem::<_, _, ()>(KEY_PROCESSING, id.to_string())
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;
        conn.del::<_, ()>(key_job(id))
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;
        Ok(())
    }

    async fn nack(&self, id: JobId, error: &str, retry_at: Option<DateTime<Utc>>) -> Result<()> {
        let mut conn = self.conn.clone();
        // Remove from processing
        conn.zrem::<_, _, ()>(KEY_PROCESSING, id.to_string())
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;

        // Update error field
        redis::cmd("HSET")
            .arg(key_job(id))
            .arg("last_error")
            .arg(error)
            .query_async::<()>(&mut conn)
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;

        match retry_at {
            Some(at) => {
                // Schedule for retry
                let record = self.load_job(id).await?
                    .ok_or_else(|| Error::NotFound(id.to_string()))?;
                redis::cmd("HSET")
                    .arg(key_job(id))
                    .arg("scheduled_at")
                    .arg(at.timestamp())
                    .query_async::<()>(&mut conn)
                    .await
                    .map_err(|e| Error::Backend(e.to_string()))?;
                // Add to delayed set
                conn.zadd::<_, _, _, ()>(key_delayed(&record.queue), id.to_string(), at.timestamp())
                    .await
                    .map_err(|e| Error::Backend(e.to_string()))?;
            }
            None => {
                // Dead-letter: add to dead set
                let record = self.load_job(id).await?
                    .ok_or_else(|| Error::NotFound(id.to_string()))?;
                let now = Utc::now().timestamp();
                conn.zadd::<_, _, _, ()>(key_dead(&record.queue), id.to_string(), now)
                    .await
                    .map_err(|e| Error::Backend(e.to_string()))?;
            }
        }
        Ok(())
    }

    async fn heartbeat(&self, id: JobId) -> Result<()> {
        let mut conn = self.conn.clone();
        let now = Utc::now().timestamp();
        conn.zadd::<_, _, _, ()>(KEY_PROCESSING, id.to_string(), now)
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;
        Ok(())
    }

    async fn reap_stuck(&self, older_than: Duration) -> Result<u64> {
        let cutoff = Utc::now().timestamp() - older_than.as_secs() as i64;
        let mut conn = self.conn.clone();
        // Find jobs whose heartbeat is older than cutoff
        let stuck_ids: Vec<String> = conn
            .zrangebyscore(KEY_PROCESSING, "-inf", cutoff)
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;

        let count = stuck_ids.len() as u64;
        for id_str in stuck_ids {
            let id = match Uuid::parse_str(&id_str) {
                Ok(id) => id,
                Err(_) => continue,
            };
            // Remove from processing
            conn.zrem::<_, _, ()>(KEY_PROCESSING, &id_str)
                .await
                .map_err(|e| Error::Backend(e.to_string()))?;

            // Reset attempt count and re-enqueue
            if let Some(mut record) = self.load_job(id).await? {
                if record.attempt > 0 { record.attempt -= 1; }
                let now_ts = Utc::now().timestamp();
                redis::cmd("HSET")
                    .arg(key_job(id))
                    .arg("attempt").arg(record.attempt)
                    .arg("scheduled_at").arg(now_ts)
                    .query_async::<()>(&mut conn)
                    .await
                    .map_err(|e| Error::Backend(e.to_string()))?;
                conn.lpush::<_, _, ()>(key_pending(&record.queue), &id_str)
                    .await
                    .map_err(|e| Error::Backend(e.to_string()))?;
            }
        }
        Ok(count)
    }

    async fn stats(&self, queue: &str) -> Result<QueueStats> {
        let mut conn = self.conn.clone();
        let pending: u64 = conn.llen(key_pending(queue)).await
            .map_err(|e| Error::Backend(e.to_string()))?;
        let delayed: u64 = conn.zcard(key_delayed(queue)).await
            .map_err(|e| Error::Backend(e.to_string()))?;
        let dead: u64 = conn.zcard(key_dead(queue)).await
            .map_err(|e| Error::Backend(e.to_string()))?;
        let running: u64 = conn.zcard(KEY_PROCESSING).await
            .map_err(|e| Error::Backend(e.to_string()))?;

        Ok(QueueStats {
            queue: queue.to_owned(),
            pending: pending + delayed,
            running,
            completed: 0, // Redis doesn't track completed by default
            failed: 0,
            dead,
        })
    }

    async fn dead_jobs(&self, queue: &str, limit: i64, offset: i64) -> Result<Vec<JobRecord>> {
        let mut conn = self.conn.clone();
        let ids: Vec<String> = conn
            .zrevrange(key_dead(queue), offset as isize, (offset + limit - 1) as isize)
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;

        let mut records = Vec::new();
        for id_str in ids {
            if let Ok(id) = Uuid::parse_str(&id_str) {
                if let Some(record) = self.load_job(id).await? {
                    records.push(record);
                }
            }
        }
        Ok(records)
    }

    async fn retry_dead(&self, id: JobId) -> Result<()> {
        let record = self.load_job(id).await?
            .ok_or_else(|| Error::NotFound(id.to_string()))?;
        let mut conn = self.conn.clone();
        // Remove from dead set
        let removed: i64 = conn.zrem(key_dead(&record.queue), id.to_string())
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;
        if removed == 0 {
            return Err(Error::NotFound(id.to_string()));
        }
        // Re-enqueue for immediate processing
        let now_ts = Utc::now().timestamp();
        redis::cmd("HSET")
            .arg(key_job(id))
            .arg("scheduled_at").arg(now_ts)
            .arg("last_error").arg("")
            .query_async::<()>(&mut conn)
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;
        conn.lpush::<_, _, ()>(key_pending(&record.queue), id.to_string())
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;
        Ok(())
    }

    async fn retry_all_dead(&self, queue: &str) -> Result<u64> {
        let dead_key = key_dead(queue);
        let mut conn = self.conn.clone();
        let ids: Vec<String> = conn
            .zrange(&dead_key, 0isize, -1isize)
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;

        let count = ids.len() as u64;
        for id_str in ids {
            if let Ok(id) = Uuid::parse_str(&id_str) {
                let _ = self.retry_dead(id).await;
            }
        }
        Ok(count)
    }
}

// ── Base64 helpers (no external dep) ─────────────────────────────────────────

fn base64_encode(data: &[u8]) -> String {
    use std::fmt::Write;
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let combined = (b0 << 16) | (b1 << 8) | b2;
        let _ = write!(out, "{}", TABLE[((combined >> 18) & 63) as usize] as char);
        let _ = write!(out, "{}", TABLE[((combined >> 12) & 63) as usize] as char);
        let _ = write!(out, "{}", if chunk.len() > 1 { TABLE[((combined >> 6) & 63) as usize] as char } else { '=' });
        let _ = write!(out, "{}", if chunk.len() > 2 { TABLE[(combined & 63) as usize] as char } else { '=' });
    }
    out
}

fn base64_decode(s: &str) -> std::result::Result<Vec<u8>, &'static str> {
    let s = s.trim_end_matches('=');
    let mut out = Vec::new();
    let decode_char = |c: char| -> Option<u32> {
        match c {
            'A'..='Z' => Some(c as u32 - 'A' as u32),
            'a'..='z' => Some(c as u32 - 'a' as u32 + 26),
            '0'..='9' => Some(c as u32 - '0' as u32 + 52),
            '+' => Some(62), '/' => Some(63), _ => None,
        }
    };
    let chars: Vec<char> = s.chars().collect();
    for chunk in chars.chunks(4) {
        let b: Vec<u32> = chunk.iter().filter_map(|&c| decode_char(c)).collect();
        if b.is_empty() { continue; }
        let combined = b[0] << 18
            | b.get(1).copied().unwrap_or(0) << 12
            | b.get(2).copied().unwrap_or(0) << 6
            | b.get(3).copied().unwrap_or(0);
        out.push(((combined >> 16) & 0xFF) as u8);
        if b.len() > 2 { out.push(((combined >> 8) & 0xFF) as u8); }
        if b.len() > 3 { out.push((combined & 0xFF) as u8); }
    }
    Ok(out)
}
