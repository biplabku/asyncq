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
//! asyncq:q:{queue}:pending     LIST    job_ids (LPUSH enqueue, BRPOP claim)
//! asyncq:q:{queue}:delayed     ZSET    job_id → run_at_unix (scheduled jobs)
//! asyncq:q:{queue}:dead        ZSET    job_id → failed_at_unix
//! asyncq:q:{queue}:completed   INTEGER lifetime count of successfully acked jobs
//! asyncq:processing            ZSET    job_id → last_heartbeat_unix
//! asyncq:jobs:{job_id}         HASH    all job fields
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

fn key_pending(queue: &str)    -> String { format!("asyncq:q:{queue}:pending")   }
fn key_delayed(queue: &str)    -> String { format!("asyncq:q:{queue}:delayed")   }
fn key_dead(queue: &str)       -> String { format!("asyncq:q:{queue}:dead")      }
fn key_completed(queue: &str)  -> String { format!("asyncq:q:{queue}:completed") }
fn key_job(id: JobId)          -> String { format!("asyncq:jobs:{id}")           }
const KEY_PROCESSING: &str = "asyncq:processing";

// ── RedisBackend ──────────────────────────────────────────────────────────────

/// Atomically take the next job: pop its id from the first non-empty pending list, bump its
/// attempt, and register it in the processing set, all in one script. Doing these as separate
/// commands lost the job if the worker died between the pop and the ZADD.
/// KEYS[1] = processing set, KEYS[2..] = pending lists; ARGV[1] = now (unix s), ARGV[2] = job key prefix.
const CLAIM_LUA: &str = r#"
for i = 2, #KEYS do
  while true do
    local id = redis.call('RPOP', KEYS[i])
    if not id then break end
    local job = ARGV[2] .. id
    if redis.call('EXISTS', job) == 1 then
      redis.call('HINCRBY', job, 'attempt', 1)
      redis.call('ZADD', KEYS[1], ARGV[1], id)
      return id
    end
    -- job hash deleted after enqueue: drop the stale id and keep looking
  end
end
return false
"#;

/// Upper bound on how long `claim` sleeps between polls while waiting for work.
const CLAIM_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Ack, optionally fenced by attempt. KEYS[1] processing, KEYS[2] job hash;
/// ARGV[1] id, ARGV[2] attempt ('' = unfenced), ARGV[3] queue key prefix.
/// Returns 1 if applied, 0 if the claim was lost.
const ACK_LUA: &str = r#"
if ARGV[2] ~= '' then
  if redis.call('HGET', KEYS[2], 'attempt') ~= ARGV[2] then return 0 end
  if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then return 0 end
else
  redis.call('ZREM', KEYS[1], ARGV[1])
end
local q = redis.call('HGET', KEYS[2], 'queue')
redis.call('DEL', KEYS[2])
if q then redis.call('INCR', ARGV[3] .. q .. ':completed') end
return 1
"#;

/// Nack (retry later or dead-letter), optionally fenced by attempt.
/// KEYS[1] processing, KEYS[2] job hash; ARGV[1] id, ARGV[2] attempt ('' = unfenced),
/// ARGV[3] queue key prefix, ARGV[4] error, ARGV[5] retry_at unix s ('' = dead-letter), ARGV[6] now.
/// Returns 1 applied, 0 claim lost, -1 job not found.
const NACK_LUA: &str = r#"
if ARGV[2] ~= '' then
  if redis.call('HGET', KEYS[2], 'attempt') ~= ARGV[2] then return 0 end
  if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then return 0 end
else
  redis.call('ZREM', KEYS[1], ARGV[1])
end
local q = redis.call('HGET', KEYS[2], 'queue')
if not q then return -1 end
redis.call('HSET', KEYS[2], 'last_error', ARGV[4])
if ARGV[5] ~= '' then
  redis.call('HSET', KEYS[2], 'scheduled_at', ARGV[5])
  redis.call('ZADD', ARGV[3] .. q .. ':delayed', ARGV[5], ARGV[1])
else
  redis.call('ZADD', ARGV[3] .. q .. ':dead', ARGV[6], ARGV[1])
end
return 1
"#;

/// Re-enqueue (or dead-letter, if attempts are exhausted) every job whose heartbeat is older
/// than the cutoff, in one script so a reaper that dies mid-sweep can't strand a job.
/// KEYS[1] processing; ARGV[1] cutoff, ARGV[2] now, ARGV[3] job key prefix,
/// ARGV[4] queue key prefix, ARGV[5] error for exhausted jobs. Returns the number reaped.
const REAP_LUA: &str = r#"
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
local n = 0
for _, id in ipairs(ids) do
  redis.call('ZREM', KEYS[1], id)
  n = n + 1
  local job = ARGV[3] .. id
  local q = redis.call('HGET', job, 'queue')
  if q then
    local attempt = tonumber(redis.call('HGET', job, 'attempt') or '0')
    local max_attempts = tonumber(redis.call('HGET', job, 'max_attempts') or '0')
    if attempt >= max_attempts then
      redis.call('HSET', job, 'last_error', ARGV[5])
      redis.call('ZADD', ARGV[4] .. q .. ':dead', ARGV[2], id)
    else
      redis.call('HSET', job, 'scheduled_at', ARGV[2])
      redis.call('LPUSH', ARGV[4] .. q .. ':pending', id)
    end
  end
end
return n
"#;

/// Move due delayed jobs to pending. KEYS[1] delayed set, KEYS[2] pending list; ARGV[1] now.
const PROMOTE_LUA: &str = r#"
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
for _, id in ipairs(ids) do
  redis.call('ZREM', KEYS[1], id)
  redis.call('LPUSH', KEYS[2], id)
end
return #ids
"#;

const JOB_PREFIX: &str = "asyncq:jobs:";
const QUEUE_PREFIX: &str = "asyncq:q:";
const REAPED_EXHAUSTED: &str = "worker stopped heartbeating; attempts exhausted";

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

    async fn run_ack(&self, id: JobId, attempt: Option<u32>) -> Result<bool> {
        let mut conn = self.conn.clone();
        let applied: i64 = redis::Script::new(ACK_LUA)
            .key(KEY_PROCESSING)
            .key(key_job(id))
            .arg(id.to_string())
            .arg(attempt.map(|a| a.to_string()).unwrap_or_default())
            .arg(QUEUE_PREFIX)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;
        Ok(applied == 1)
    }

    async fn run_nack(
        &self,
        id: JobId,
        attempt: Option<u32>,
        error: &str,
        retry_at: Option<DateTime<Utc>>,
    ) -> Result<bool> {
        let mut conn = self.conn.clone();
        let applied: i64 = redis::Script::new(NACK_LUA)
            .key(KEY_PROCESSING)
            .key(key_job(id))
            .arg(id.to_string())
            .arg(attempt.map(|a| a.to_string()).unwrap_or_default())
            .arg(QUEUE_PREFIX)
            .arg(error)
            .arg(retry_at.map(|t| t.timestamp().to_string()).unwrap_or_default())
            .arg(Utc::now().timestamp())
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;
        match applied {
            -1 => Err(Error::NotFound(id.to_string())),
            n => Ok(n == 1),
        }
    }

    async fn promote_delayed(&self, queues: &[&str]) -> Result<()> {
        let now = Utc::now().timestamp();
        let mut conn = self.conn.clone();
        for &queue in queues {
            redis::Script::new(PROMOTE_LUA)
                .key(key_delayed(queue))
                .key(key_pending(queue))
                .arg(now)
                .invoke_async::<i64>(&mut conn)
                .await
                .map_err(|e| Error::Backend(e.to_string()))?;
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
        let script = redis::Script::new(CLAIM_LUA);
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            let mut call = script.prepare_invoke();
            call.key(KEY_PROCESSING);
            for key in &pending_keys {
                call.key(key);
            }
            let claimed: Option<String> = call
                .arg(Utc::now().timestamp())
                .arg(JOB_PREFIX)
                .invoke_async(&mut conn)
                .await
                .map_err(|e| Error::Backend(e.to_string()))?;

            if let Some(id_str) = claimed {
                let id = Uuid::parse_str(&id_str)
                    .map_err(|e| Error::Backend(format!("invalid job id: {e}")))?;
                // attempt was already incremented by the script
                return self.load_job(id).await;
            }

            let now = tokio::time::Instant::now();
            if timeout.is_zero() || now >= deadline {
                return Ok(None);
            }
            tokio::time::sleep(CLAIM_POLL_INTERVAL.min(deadline - now)).await;
        }
    }

    async fn ack(&self, id: JobId) -> Result<()> {
        self.run_ack(id, None).await.map(|_| ())
    }

    async fn nack(&self, id: JobId, error: &str, retry_at: Option<DateTime<Utc>>) -> Result<()> {
        self.run_nack(id, None, error, retry_at).await.map(|_| ())
    }

    async fn ack_claim(&self, id: JobId, attempt: u32) -> Result<bool> {
        self.run_ack(id, Some(attempt)).await
    }

    async fn nack_claim(
        &self,
        id: JobId,
        attempt: u32,
        error: &str,
        retry_at: Option<DateTime<Utc>>,
    ) -> Result<bool> {
        self.run_nack(id, Some(attempt), error, retry_at).await
    }

    async fn heartbeat(&self, id: JobId) -> Result<()> {
        let mut conn = self.conn.clone();
        let now = Utc::now().timestamp();
        // XX: only refresh jobs still in processing; never re-add a reaped/acked job.
        redis::cmd("ZADD")
            .arg(KEY_PROCESSING)
            .arg("XX")
            .arg(now)
            .arg(id.to_string())
            .query_async::<()>(&mut conn)
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;
        Ok(())
    }

    async fn reap_stuck(&self, older_than: Duration) -> Result<u64> {
        let now = Utc::now().timestamp();
        let mut conn = self.conn.clone();
        let reaped: i64 = redis::Script::new(REAP_LUA)
            .key(KEY_PROCESSING)
            .arg(now - older_than.as_secs() as i64)
            .arg(now)
            .arg(JOB_PREFIX)
            .arg(QUEUE_PREFIX)
            .arg(REAPED_EXHAUSTED)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;
        Ok(reaped as u64)
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
        // Returns None (nil) if no jobs have been acked yet on this queue
        let completed: u64 = conn
            .get::<_, Option<u64>>(key_completed(queue))
            .await
            .map_err(|e| Error::Backend(e.to_string()))?
            .unwrap_or(0);

        Ok(QueueStats {
            queue: queue.to_owned(),
            pending: pending + delayed,
            running,
            completed,
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
