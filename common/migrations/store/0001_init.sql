CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    type TEXT NOT NULL,
    seq INTEGER NOT NULL,
    produced_at TEXT NOT NULL,
    payload TEXT NOT NULL,
    sent INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_events_user_seq ON events(user_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_sent ON events(sent);