CREATE TABLE IF NOT EXISTS asyncq_jobs (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    kind         TEXT        NOT NULL,
    queue        TEXT        NOT NULL,
    payload      BYTEA       NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'pending'
                             CHECK (status IN ('pending','running','completed','failed','dead')),
    attempt      INT         NOT NULL DEFAULT 0,
    max_attempts INT         NOT NULL DEFAULT 10,
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    heartbeat_at TIMESTAMPTZ,
    last_error   TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Claim pending jobs: filter by queue, scheduled_at passed, ordered FIFO
CREATE INDEX IF NOT EXISTS asyncq_jobs_pending_idx
    ON asyncq_jobs (queue, scheduled_at ASC)
    WHERE status = 'pending';

-- Reap stuck running jobs by heartbeat
CREATE INDEX IF NOT EXISTS asyncq_jobs_running_hb_idx
    ON asyncq_jobs (heartbeat_at ASC)
    WHERE status = 'running';

-- DLQ listing: newest dead first
CREATE INDEX IF NOT EXISTS asyncq_jobs_dead_idx
    ON asyncq_jobs (queue, created_at DESC)
    WHERE status = 'dead';
