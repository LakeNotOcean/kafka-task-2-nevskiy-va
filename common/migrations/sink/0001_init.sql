CREATE TABLE IF NOT EXISTS processed (
    event_id TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    consumer_pod TEXT NOT NULL,
    topic TEXT NOT NULL,
    "partition" INTEGER NOT NULL,
    "offset" INTEGER NOT NULL,
    processed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dup_stats (
    consumer_pod TEXT PRIMARY KEY,
    duplicates INTEGER NOT NULL
);