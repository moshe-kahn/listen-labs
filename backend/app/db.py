from __future__ import annotations

import json
import hashlib
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
import re
from typing import Any, Iterator

from backend.app.config import get_settings
from backend.app.track_variant_policy import classify_label_families

SCHEMA_VERSION_SQL = """
CREATE TABLE IF NOT EXISTS schema_version (
  version INTEGER NOT NULL
);
"""

INITIALIZE_SCHEMA_VERSION_SQL = """
INSERT INTO schema_version (version)
SELECT 0
WHERE NOT EXISTS (SELECT 1 FROM schema_version);
"""

MIGRATIONS: dict[int, str] = {
    1: """
CREATE TABLE ingest_run (
  id TEXT PRIMARY KEY,
  source_type TEXT NOT NULL,
  source_ref TEXT,
  started_at TEXT NOT NULL,
  completed_at TEXT,
  status TEXT NOT NULL,
  row_count INTEGER NOT NULL DEFAULT 0,
  inserted_count INTEGER NOT NULL DEFAULT 0,
  duplicate_count INTEGER NOT NULL DEFAULT 0,
  error_count INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
""",
    2: """
CREATE TABLE raw_play_event (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ingest_run_id TEXT NOT NULL REFERENCES ingest_run(id),

  source_type TEXT NOT NULL,
  source_event_id TEXT,
  source_row_key TEXT NOT NULL,
  cross_source_event_key TEXT,

  played_at TEXT NOT NULL,
  ms_played INTEGER NOT NULL,
  skipped INTEGER NOT NULL DEFAULT 0,

  spotify_track_uri TEXT,
  spotify_track_id TEXT,
  track_name_raw TEXT,
  artist_name_raw TEXT,
  album_name_raw TEXT,
  spotify_album_id TEXT,
  spotify_artist_ids_json TEXT,

  raw_payload_json TEXT NOT NULL,
  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX idx_raw_play_event_played_at
  ON raw_play_event(played_at);

CREATE UNIQUE INDEX ux_raw_play_event_source_event
  ON raw_play_event(source_type, source_event_id)
  WHERE source_event_id IS NOT NULL;

CREATE UNIQUE INDEX ux_raw_play_event_source_row_key
  ON raw_play_event(source_row_key);

CREATE INDEX idx_raw_play_event_cross_source_event_key
  ON raw_play_event(cross_source_event_key)
  WHERE cross_source_event_key IS NOT NULL;
""",
    3: """
ALTER TABLE raw_play_event RENAME TO raw_play_event_old;

CREATE TABLE raw_play_event (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ingest_run_id TEXT REFERENCES ingest_run(id),

  source_type TEXT NOT NULL,
  source_event_id TEXT,
  source_row_key TEXT NOT NULL,
  cross_source_event_key TEXT,

  played_at TEXT NOT NULL,
  ms_played INTEGER NOT NULL,
  skipped INTEGER NOT NULL DEFAULT 0,

  spotify_track_uri TEXT,
  spotify_track_id TEXT,
  track_name_raw TEXT,
  artist_name_raw TEXT,
  album_name_raw TEXT,
  spotify_album_id TEXT,
  spotify_artist_ids_json TEXT,

  raw_payload_json TEXT NOT NULL,
  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

INSERT INTO raw_play_event (
  id,
  ingest_run_id,
  source_type,
  source_event_id,
  source_row_key,
  cross_source_event_key,
  played_at,
  ms_played,
  skipped,
  spotify_track_uri,
  spotify_track_id,
  track_name_raw,
  artist_name_raw,
  album_name_raw,
  spotify_album_id,
  spotify_artist_ids_json,
  raw_payload_json,
  inserted_at
)
SELECT
  id,
  ingest_run_id,
  source_type,
  source_event_id,
  source_row_key,
  cross_source_event_key,
  played_at,
  ms_played,
  skipped,
  spotify_track_uri,
  spotify_track_id,
  track_name_raw,
  artist_name_raw,
  album_name_raw,
  spotify_album_id,
  spotify_artist_ids_json,
  raw_payload_json,
  inserted_at
FROM raw_play_event_old;

DROP TABLE raw_play_event_old;

CREATE INDEX idx_raw_play_event_played_at
  ON raw_play_event(played_at);

CREATE UNIQUE INDEX ux_raw_play_event_source_event
  ON raw_play_event(source_type, source_event_id)
  WHERE source_event_id IS NOT NULL;

CREATE UNIQUE INDEX ux_raw_play_event_source_row_key
  ON raw_play_event(source_row_key);

CREATE INDEX idx_raw_play_event_cross_source_event_key
  ON raw_play_event(cross_source_event_key)
  WHERE cross_source_event_key IS NOT NULL;
""",
    4: """
CREATE TABLE spotify_sync_state (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  last_successful_played_at TEXT,
  overlap_lookback_seconds INTEGER NOT NULL DEFAULT 21600,
  last_started_at TEXT,
  last_completed_at TEXT,
  last_run_id TEXT REFERENCES ingest_run(id),
  updated_at TEXT NOT NULL
);

INSERT INTO spotify_sync_state (
  id,
  last_successful_played_at,
  overlap_lookback_seconds,
  last_started_at,
  last_completed_at,
  last_run_id,
  updated_at
)
SELECT
  1,
  NULL,
  21600,
  NULL,
  NULL,
  NULL,
  strftime('%Y-%m-%dT%H:%M:%fZ','now')
WHERE NOT EXISTS (SELECT 1 FROM spotify_sync_state WHERE id = 1);
""",
    5: """
ALTER TABLE raw_play_event RENAME TO raw_play_event_old_v5;

CREATE TABLE raw_play_event (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ingest_run_id TEXT REFERENCES ingest_run(id),

  source_type TEXT NOT NULL,
  source_event_id TEXT,
  source_row_key TEXT NOT NULL,
  cross_source_event_key TEXT,

  played_at TEXT NOT NULL,
  ms_played INTEGER NOT NULL,
  ms_played_method TEXT NOT NULL CHECK (
    ms_played_method IN ('history_source', 'api_chronology', 'default_guess')
  ),
  track_duration_ms INTEGER,
  skipped INTEGER NOT NULL DEFAULT 0,

  spotify_track_uri TEXT,
  spotify_track_id TEXT,
  track_name_raw TEXT,
  artist_name_raw TEXT,
  album_name_raw TEXT,
  spotify_album_id TEXT,
  spotify_artist_ids_json TEXT,

  raw_payload_json TEXT NOT NULL,
  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

INSERT INTO raw_play_event (
  id,
  ingest_run_id,
  source_type,
  source_event_id,
  source_row_key,
  cross_source_event_key,
  played_at,
  ms_played,
  ms_played_method,
  track_duration_ms,
  skipped,
  spotify_track_uri,
  spotify_track_id,
  track_name_raw,
  artist_name_raw,
  album_name_raw,
  spotify_album_id,
  spotify_artist_ids_json,
  raw_payload_json,
  inserted_at
)
SELECT
  id,
  ingest_run_id,
  source_type,
  source_event_id,
  source_row_key,
  cross_source_event_key,
  played_at,
  ms_played,
  'history_source',
  NULL,
  skipped,
  spotify_track_uri,
  spotify_track_id,
  track_name_raw,
  artist_name_raw,
  album_name_raw,
  spotify_album_id,
  spotify_artist_ids_json,
  raw_payload_json,
  inserted_at
FROM raw_play_event_old_v5;

DROP TABLE raw_play_event_old_v5;

CREATE INDEX idx_raw_play_event_played_at
  ON raw_play_event(played_at);

CREATE UNIQUE INDEX ux_raw_play_event_source_event
  ON raw_play_event(source_type, source_event_id)
  WHERE source_event_id IS NOT NULL;

CREATE UNIQUE INDEX ux_raw_play_event_source_row_key
  ON raw_play_event(source_row_key);

CREATE INDEX idx_raw_play_event_cross_source_event_key
  ON raw_play_event(cross_source_event_key)
  WHERE cross_source_event_key IS NOT NULL;
""",
    6: """
ALTER TABLE raw_play_event RENAME TO raw_play_event_old_v6;

CREATE TABLE raw_play_event (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ingest_run_id TEXT REFERENCES ingest_run(id),

  source_type TEXT NOT NULL,
  source_event_id TEXT,
  source_row_key TEXT NOT NULL,
  cross_source_event_key TEXT,

  played_at TEXT NOT NULL,
  ms_played INTEGER NOT NULL,
  ms_played_method TEXT NOT NULL CHECK (
    ms_played_method IN ('history_source', 'api_chronology', 'default_guess')
  ),
  track_duration_ms INTEGER,
  reason_start TEXT,
  reason_end TEXT,
  skipped INTEGER,
  platform TEXT,
  shuffle INTEGER,
  offline INTEGER,
  conn_country TEXT,

  spotify_track_uri TEXT,
  spotify_track_id TEXT,
  track_name_raw TEXT,
  artist_name_raw TEXT,
  album_name_raw TEXT,
  spotify_album_id TEXT,
  spotify_artist_ids_json TEXT,

  raw_payload_json TEXT NOT NULL,
  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

INSERT INTO raw_play_event (
  id,
  ingest_run_id,
  source_type,
  source_event_id,
  source_row_key,
  cross_source_event_key,
  played_at,
  ms_played,
  ms_played_method,
  track_duration_ms,
  reason_start,
  reason_end,
  skipped,
  platform,
  shuffle,
  offline,
  conn_country,
  spotify_track_uri,
  spotify_track_id,
  track_name_raw,
  artist_name_raw,
  album_name_raw,
  spotify_album_id,
  spotify_artist_ids_json,
  raw_payload_json,
  inserted_at
)
SELECT
  id,
  ingest_run_id,
  source_type,
  source_event_id,
  source_row_key,
  cross_source_event_key,
  played_at,
  ms_played,
  ms_played_method,
  track_duration_ms,
  NULL,
  NULL,
  skipped,
  NULL,
  NULL,
  NULL,
  NULL,
  spotify_track_uri,
  spotify_track_id,
  track_name_raw,
  artist_name_raw,
  album_name_raw,
  spotify_album_id,
  spotify_artist_ids_json,
  raw_payload_json,
  inserted_at
FROM raw_play_event_old_v6;

DROP TABLE raw_play_event_old_v6;

CREATE INDEX idx_raw_play_event_played_at
  ON raw_play_event(played_at);

CREATE UNIQUE INDEX ux_raw_play_event_source_event
  ON raw_play_event(source_type, source_event_id)
  WHERE source_event_id IS NOT NULL;

CREATE UNIQUE INDEX ux_raw_play_event_source_row_key
  ON raw_play_event(source_row_key);

CREATE INDEX idx_raw_play_event_cross_source_event_key
  ON raw_play_event(cross_source_event_key)
  WHERE cross_source_event_key IS NOT NULL;
""",
    7: """
ALTER TABLE raw_play_event
ADD COLUMN duplicate_row_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE raw_play_event
ADD COLUMN duplicate_merge_strategy TEXT NOT NULL DEFAULT 'none';

CREATE TABLE raw_play_event_membership (
  canonical_event_id INTEGER NOT NULL REFERENCES raw_play_event(id),
  source_row_key TEXT NOT NULL,
  source_type TEXT NOT NULL,
  PRIMARY KEY (canonical_event_id, source_row_key)
);

CREATE UNIQUE INDEX ux_raw_play_event_membership_source_row_key
  ON raw_play_event_membership(source_row_key);

CREATE INDEX idx_raw_play_event_membership_canonical_event_id
  ON raw_play_event_membership(canonical_event_id);

INSERT INTO raw_play_event_membership (
  canonical_event_id,
  source_row_key,
  source_type
)
SELECT
  id,
  source_row_key,
  source_type
FROM raw_play_event;
""",
    8: """
CREATE TABLE spotify_auth (
  user_id TEXT PRIMARY KEY,
  spotify_user_id TEXT NOT NULL,
  access_token_encrypted TEXT NOT NULL,
  refresh_token_encrypted TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  scopes TEXT NOT NULL,
  reauth_required INTEGER NOT NULL DEFAULT 0,
  reauth_reason TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  last_refreshed_at TEXT
);

CREATE INDEX idx_spotify_auth_spotify_user_id
  ON spotify_auth(spotify_user_id);
""",
    9: """
CREATE TABLE live_playback_event (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  observed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  user_id TEXT NOT NULL,
  spotify_user_id TEXT,
  source TEXT NOT NULL DEFAULT 'current_playback',
  has_playback INTEGER NOT NULL DEFAULT 1,
  item_type TEXT,
  item_id TEXT,
  item_name TEXT,
  spotify_track_uri TEXT,
  artist_names_json TEXT,
  album_name TEXT,
  progress_ms INTEGER,
  duration_ms INTEGER,
  is_playing INTEGER,
  device_id TEXT,
  device_name TEXT,
  device_type TEXT,
  spotify_timestamp_ms INTEGER,
  raw_payload_json TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX idx_live_playback_event_user_observed_at
  ON live_playback_event(user_id, observed_at DESC);

CREATE INDEX idx_live_playback_event_item_id
  ON live_playback_event(item_id);
""",
    10: """
CREATE TABLE artist (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  canonical_name TEXT NOT NULL,
  sort_name TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE source_artist (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_name TEXT NOT NULL,
  external_id TEXT NOT NULL,
  external_uri TEXT,
  source_name_raw TEXT,
  raw_payload_json TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(source_name, external_id)
);

CREATE TABLE source_artist_map (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_artist_id INTEGER NOT NULL REFERENCES source_artist(id),
  artist_id INTEGER NOT NULL REFERENCES artist(id),
  match_method TEXT NOT NULL,
  confidence REAL NOT NULL,
  status TEXT NOT NULL,
  is_user_confirmed INTEGER NOT NULL DEFAULT 0,
  explanation TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(source_artist_id, artist_id)
);

CREATE TABLE release_album (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  primary_name TEXT NOT NULL,
  normalized_name TEXT,
  release_year INTEGER,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE source_album (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_name TEXT NOT NULL,
  external_id TEXT NOT NULL,
  external_uri TEXT,
  source_name_raw TEXT,
  raw_payload_json TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(source_name, external_id)
);

CREATE TABLE source_album_map (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_album_id INTEGER NOT NULL REFERENCES source_album(id),
  release_album_id INTEGER NOT NULL REFERENCES release_album(id),
  match_method TEXT NOT NULL,
  confidence REAL NOT NULL,
  status TEXT NOT NULL,
  is_user_confirmed INTEGER NOT NULL DEFAULT 0,
  explanation TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(source_album_id, release_album_id)
);

CREATE TABLE release_track (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  primary_name TEXT NOT NULL,
  normalized_name TEXT,
  duration_ms INTEGER,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE source_track (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_name TEXT NOT NULL,
  external_id TEXT NOT NULL,
  external_uri TEXT,
  isrc TEXT,
  source_name_raw TEXT,
  raw_payload_json TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(source_name, external_id)
);

CREATE TABLE source_track_map (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_track_id INTEGER NOT NULL REFERENCES source_track(id),
  release_track_id INTEGER NOT NULL REFERENCES release_track(id),
  match_method TEXT NOT NULL,
  confidence REAL NOT NULL,
  status TEXT NOT NULL,
  is_user_confirmed INTEGER NOT NULL DEFAULT 0,
  explanation TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(source_track_id, release_track_id)
);

CREATE TABLE album_artist (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  release_album_id INTEGER NOT NULL REFERENCES release_album(id),
  artist_id INTEGER NOT NULL REFERENCES artist(id),
  role TEXT NOT NULL DEFAULT 'primary',
  billing_index INTEGER,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(release_album_id, artist_id, role)
);

CREATE TABLE track_artist (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  release_track_id INTEGER NOT NULL REFERENCES release_track(id),
  artist_id INTEGER NOT NULL REFERENCES artist(id),
  role TEXT NOT NULL DEFAULT 'primary',
  billing_index INTEGER,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(release_track_id, artist_id, role)
);

CREATE TABLE album_track (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  release_album_id INTEGER NOT NULL REFERENCES release_album(id),
  release_track_id INTEGER NOT NULL REFERENCES release_track(id),
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(release_album_id, release_track_id)
);

CREATE INDEX idx_source_artist_map_artist_id
  ON source_artist_map(artist_id);

CREATE INDEX idx_source_album_map_release_album_id
  ON source_album_map(release_album_id);

CREATE INDEX idx_source_track_map_release_track_id
  ON source_track_map(release_track_id);

CREATE INDEX idx_album_artist_artist_id
  ON album_artist(artist_id);

CREATE INDEX idx_track_artist_artist_id
  ON track_artist(artist_id);

CREATE INDEX idx_album_track_release_track_id
  ON album_track(release_track_id);
""",
    11: """
ALTER TABLE track_artist
ADD COLUMN credited_as TEXT;

ALTER TABLE track_artist
ADD COLUMN match_method TEXT NOT NULL DEFAULT 'backfill';

ALTER TABLE track_artist
ADD COLUMN confidence REAL NOT NULL DEFAULT 1.0;

ALTER TABLE track_artist
ADD COLUMN source_basis TEXT;

ALTER TABLE album_artist
ADD COLUMN credited_as TEXT;

ALTER TABLE album_artist
ADD COLUMN match_method TEXT NOT NULL DEFAULT 'backfill';

ALTER TABLE album_artist
ADD COLUMN confidence REAL NOT NULL DEFAULT 1.0;

ALTER TABLE album_artist
ADD COLUMN source_basis TEXT;
""",
    12: """
CREATE TABLE analysis_track (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  primary_name TEXT NOT NULL,
  grouping_note TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE analysis_track_map (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  release_track_id INTEGER NOT NULL REFERENCES release_track(id),
  analysis_track_id INTEGER NOT NULL REFERENCES analysis_track(id),
  match_method TEXT NOT NULL,
  confidence REAL NOT NULL,
  status TEXT NOT NULL,
  is_user_confirmed INTEGER NOT NULL DEFAULT 0,
  explanation TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(release_track_id, analysis_track_id)
);

CREATE TABLE track_relationship (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  from_release_track_id INTEGER NOT NULL REFERENCES release_track(id),
  to_release_track_id INTEGER NOT NULL REFERENCES release_track(id),
  relationship_type TEXT NOT NULL,
  match_method TEXT NOT NULL,
  confidence REAL NOT NULL,
  status TEXT NOT NULL,
  is_user_confirmed INTEGER NOT NULL DEFAULT 0,
  explanation TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(from_release_track_id, to_release_track_id, relationship_type)
);

CREATE INDEX idx_analysis_track_map_analysis_track_id
  ON analysis_track_map(analysis_track_id);

CREATE INDEX idx_track_relationship_to_release_track_id
  ON track_relationship(to_release_track_id);
""",
    13: """
CREATE TABLE release_track_merge_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  obsolete_release_track_id INTEGER NOT NULL,
  canonical_release_track_id INTEGER NOT NULL REFERENCES release_track(id),
  release_album_id INTEGER REFERENCES release_album(id),
  obsolete_primary_name TEXT,
  canonical_primary_name TEXT,
  match_method TEXT NOT NULL,
  confidence REAL NOT NULL,
  status TEXT NOT NULL,
  explanation TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(obsolete_release_track_id, canonical_release_track_id)
);

CREATE INDEX idx_release_track_merge_log_canonical_release_track_id
  ON release_track_merge_log(canonical_release_track_id);
""",
    14: """
CREATE TABLE raw_spotify_recent (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ingest_run_id TEXT REFERENCES ingest_run(id),
  source_row_key TEXT NOT NULL UNIQUE,
  source_event_id TEXT,
  played_at TEXT NOT NULL,
  played_at_unix_ms INTEGER,
  spotify_track_id TEXT,
  spotify_track_uri TEXT,
  spotify_album_id TEXT,
  spotify_artist_ids_json TEXT,
  track_name_raw TEXT,
  artist_name_raw TEXT,
  album_name_raw TEXT,
  track_duration_ms INTEGER,
  ms_played_estimate INTEGER NOT NULL,
  ms_played_method TEXT NOT NULL,
  ms_played_confidence TEXT NOT NULL,
  ms_played_fallback_class TEXT,
  context_type TEXT,
  context_uri TEXT,
  raw_payload_json TEXT NOT NULL,
  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE raw_spotify_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ingest_run_id TEXT REFERENCES ingest_run(id),
  source_row_key TEXT NOT NULL UNIQUE,
  played_at TEXT NOT NULL,
  played_at_unix_ms INTEGER,
  spotify_track_id TEXT,
  spotify_track_uri TEXT,
  spotify_album_id TEXT,
  spotify_artist_ids_json TEXT,
  track_name_raw TEXT,
  artist_name_raw TEXT,
  album_name_raw TEXT,
  ms_played INTEGER NOT NULL,
  reason_start TEXT,
  reason_end TEXT,
  skipped INTEGER,
  shuffle INTEGER,
  offline INTEGER,
  platform TEXT,
  conn_country TEXT,
  private_session INTEGER,
  raw_payload_json TEXT NOT NULL,
  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE fact_play_event (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  canonical_started_at TEXT,
  canonical_ended_at TEXT NOT NULL,
  canonical_ms_played INTEGER,
  ms_played_confidence TEXT,
  canonical_reason_start TEXT,
  canonical_reason_end TEXT,
  canonical_skipped INTEGER,
  canonical_shuffle INTEGER,
  canonical_offline INTEGER,
  canonical_private_session INTEGER,
  canonical_context_type TEXT,
  canonical_context_uri TEXT,
  spotify_track_id TEXT,
  spotify_track_uri TEXT,
  spotify_album_id TEXT,
  spotify_artist_ids_json TEXT,
  track_name_canonical TEXT,
  artist_name_canonical TEXT,
  album_name_canonical TEXT,
  timing_source TEXT NOT NULL,
  matched_state TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE fact_play_event_recent_link (
  fact_play_event_id INTEGER NOT NULL REFERENCES fact_play_event(id),
  raw_spotify_recent_id INTEGER NOT NULL UNIQUE REFERENCES raw_spotify_recent(id),
  match_delta_ms INTEGER,
  match_tier TEXT,
  is_primary INTEGER NOT NULL DEFAULT 1,
  UNIQUE(fact_play_event_id, raw_spotify_recent_id)
);

CREATE TABLE fact_play_event_history_link (
  fact_play_event_id INTEGER NOT NULL REFERENCES fact_play_event(id),
  raw_spotify_history_id INTEGER NOT NULL UNIQUE REFERENCES raw_spotify_history(id),
  match_delta_ms INTEGER,
  match_tier TEXT,
  is_primary INTEGER NOT NULL DEFAULT 1,
  UNIQUE(fact_play_event_id, raw_spotify_history_id)
);

CREATE INDEX idx_raw_spotify_recent_played_at
  ON raw_spotify_recent(played_at);
CREATE INDEX idx_raw_spotify_recent_track_id_played_at
  ON raw_spotify_recent(spotify_track_id, played_at);
CREATE INDEX idx_raw_spotify_recent_track_uri_played_at
  ON raw_spotify_recent(spotify_track_uri, played_at);

CREATE INDEX idx_raw_spotify_history_played_at
  ON raw_spotify_history(played_at);
CREATE INDEX idx_raw_spotify_history_track_id_played_at
  ON raw_spotify_history(spotify_track_id, played_at);
CREATE INDEX idx_raw_spotify_history_track_uri_played_at
  ON raw_spotify_history(spotify_track_uri, played_at);

CREATE INDEX idx_fact_play_event_recent_link_fact
  ON fact_play_event_recent_link(fact_play_event_id);
CREATE INDEX idx_fact_play_event_history_link_fact
  ON fact_play_event_history_link(fact_play_event_id);

INSERT OR IGNORE INTO raw_spotify_recent (
  ingest_run_id,
  source_row_key,
  source_event_id,
  played_at,
  played_at_unix_ms,
  spotify_track_id,
  spotify_track_uri,
  spotify_album_id,
  spotify_artist_ids_json,
  track_name_raw,
  artist_name_raw,
  album_name_raw,
  track_duration_ms,
  ms_played_estimate,
  ms_played_method,
  ms_played_confidence,
  ms_played_fallback_class,
  context_type,
  context_uri,
  raw_payload_json,
  inserted_at
)
SELECT
  ingest_run_id,
  source_row_key,
  source_event_id,
  played_at,
  CAST(strftime('%s', replace(played_at, 'Z', '')) AS INTEGER) * 1000,
  spotify_track_id,
  spotify_track_uri,
  spotify_album_id,
  spotify_artist_ids_json,
  track_name_raw,
  artist_name_raw,
  album_name_raw,
  track_duration_ms,
  ms_played,
  ms_played_method,
  CASE
    WHEN ms_played_method = 'api_chronology' THEN 'high'
    ELSE 'low'
  END,
  CASE
    WHEN ms_played_method = 'api_chronology' THEN NULL
    ELSE 'fallback_likely_complete'
  END,
  NULL,
  NULL,
  raw_payload_json,
  inserted_at
FROM raw_play_event
WHERE source_type = 'spotify_recent';

INSERT OR IGNORE INTO raw_spotify_history (
  ingest_run_id,
  source_row_key,
  played_at,
  played_at_unix_ms,
  spotify_track_id,
  spotify_track_uri,
  spotify_album_id,
  spotify_artist_ids_json,
  track_name_raw,
  artist_name_raw,
  album_name_raw,
  ms_played,
  reason_start,
  reason_end,
  skipped,
  shuffle,
  offline,
  platform,
  conn_country,
  private_session,
  raw_payload_json,
  inserted_at
)
SELECT
  ingest_run_id,
  source_row_key,
  played_at,
  CAST(strftime('%s', replace(played_at, 'Z', '')) AS INTEGER) * 1000,
  spotify_track_id,
  spotify_track_uri,
  spotify_album_id,
  spotify_artist_ids_json,
  track_name_raw,
  artist_name_raw,
  album_name_raw,
  ms_played,
  reason_start,
  reason_end,
  skipped,
  shuffle,
  offline,
  platform,
  conn_country,
  NULL,
  raw_payload_json,
  inserted_at
FROM raw_play_event
WHERE source_type = 'export';

CREATE VIEW v_raw_spotify_observation AS
SELECT
  'spotify_recent' AS source_type,
  id AS raw_id,
  ingest_run_id,
  source_row_key,
  played_at,
  spotify_track_id,
  spotify_track_uri,
  spotify_album_id,
  spotify_artist_ids_json,
  track_name_raw,
  artist_name_raw,
  album_name_raw,
  ms_played_estimate AS ms_played,
  ms_played_method,
  ms_played_confidence,
  ms_played_fallback_class,
  NULL AS reason_start,
  NULL AS reason_end,
  NULL AS skipped,
  NULL AS shuffle,
  NULL AS offline,
  NULL AS platform,
  NULL AS conn_country,
  NULL AS private_session
FROM raw_spotify_recent
UNION ALL
SELECT
  'spotify_history' AS source_type,
  id AS raw_id,
  ingest_run_id,
  source_row_key,
  played_at,
  spotify_track_id,
  spotify_track_uri,
  spotify_album_id,
  spotify_artist_ids_json,
  track_name_raw,
  artist_name_raw,
  album_name_raw,
  ms_played AS ms_played,
  'history_source' AS ms_played_method,
  'high' AS ms_played_confidence,
  NULL AS ms_played_fallback_class,
  reason_start,
  reason_end,
  skipped,
  shuffle,
  offline,
  platform,
  conn_country,
  private_session
FROM raw_spotify_history;

CREATE VIEW v_fact_play_event_with_sources AS
SELECT
  f.*,
  fr.raw_spotify_recent_id,
  fh.raw_spotify_history_id,
  fr.match_delta_ms AS recent_match_delta_ms,
  fr.match_tier AS recent_match_tier,
  fh.match_delta_ms AS history_match_delta_ms,
  fh.match_tier AS history_match_tier
FROM fact_play_event f
LEFT JOIN fact_play_event_recent_link fr
  ON fr.fact_play_event_id = f.id
LEFT JOIN fact_play_event_history_link fh
  ON fh.fact_play_event_id = f.id;
""",
    15: """
CREATE INDEX IF NOT EXISTS idx_raw_play_event_cross_source_event_key
  ON raw_play_event(cross_source_event_key)
  WHERE cross_source_event_key IS NOT NULL;
""",
    16: """
ALTER TABLE ingest_run ADD COLUMN last_heartbeat_at TEXT;
ALTER TABLE ingest_run ADD COLUMN file_discovery_ms REAL;
ALTER TABLE ingest_run ADD COLUMN file_read_ms REAL;
ALTER TABLE ingest_run ADD COLUMN file_parse_ms REAL;
ALTER TABLE ingest_run ADD COLUMN mapping_ms REAL;
ALTER TABLE ingest_run ADD COLUMN raw_inserts_ms REAL;
ALTER TABLE ingest_run ADD COLUMN matcher_ms REAL;
ALTER TABLE ingest_run ADD COLUMN projector_ms REAL;
ALTER TABLE ingest_run ADD COLUMN downstream_pipeline_ms REAL;
ALTER TABLE ingest_run ADD COLUMN final_commit_ms REAL;
ALTER TABLE ingest_run ADD COLUMN total_duration_ms REAL;
""",
}


def get_sqlite_db_path() -> Path:
    settings = get_settings()
    return Path(settings.sqlite_db_path)


@contextmanager
def sqlite_connection(*, write: bool = False, row_factory: Any | None = None) -> Iterator[sqlite3.Connection]:
    connection = sqlite3.connect(get_sqlite_db_path(), timeout=30)
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 30000")
    if row_factory is not None:
        connection.row_factory = row_factory
    try:
        yield connection
        if write:
            connection.commit()
    except Exception:
        if write:
            connection.rollback()
        raise
    finally:
        connection.close()


def ensure_sqlite_db() -> Path:
    db_path = get_sqlite_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite_connection(write=True) as connection:
        connection.execute(SCHEMA_VERSION_SQL)
        connection.execute(INITIALIZE_SCHEMA_VERSION_SQL)

    return db_path


def get_schema_version() -> int:
    with sqlite_connection() as connection:
        row = connection.execute("SELECT version FROM schema_version LIMIT 1").fetchone()
    return int(row[0])


def set_schema_version(version: int) -> None:
    with sqlite_connection(write=True) as connection:
        connection.execute("UPDATE schema_version SET version = ?", (version,))


def execute_sql(sql: str) -> None:
    with sqlite_connection(write=True) as connection:
        connection.executescript(sql)


def insert_raw_play_event(
    *,
    source_type: str,
    source_row_key: str,
    played_at: str,
    ms_played: int,
    ms_played_method: str,
    raw_payload_json: str,
    ingest_run_id: str | None = None,
    source_event_id: str | None = None,
    cross_source_event_key: str | None = None,
    track_duration_ms: int | None = None,
    reason_start: str | None = None,
    reason_end: str | None = None,
    skipped: int | None = None,
    platform: str | None = None,
    shuffle: int | None = None,
    offline: int | None = None,
    conn_country: str | None = None,
    spotify_track_uri: str | None = None,
    spotify_track_id: str | None = None,
    track_name_raw: str | None = None,
    artist_name_raw: str | None = None,
    album_name_raw: str | None = None,
    spotify_album_id: str | None = None,
    spotify_artist_ids_json: str | None = None,
) -> int:
    with sqlite_connection(write=True, row_factory=sqlite3.Row) as connection:
        cursor = connection.execute(
            """
            INSERT INTO raw_play_event (
              ingest_run_id,
              source_type,
              source_event_id,
              source_row_key,
              cross_source_event_key,
              played_at,
              ms_played,
              ms_played_method,
              track_duration_ms,
              reason_start,
              reason_end,
              skipped,
              platform,
              shuffle,
              offline,
              conn_country,
              spotify_track_uri,
              spotify_track_id,
              track_name_raw,
              artist_name_raw,
              album_name_raw,
              spotify_album_id,
              spotify_artist_ids_json,
              raw_payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ingest_run_id,
                source_type,
                source_event_id,
                source_row_key,
                cross_source_event_key,
                played_at,
                ms_played,
                ms_played_method,
                track_duration_ms,
                reason_start,
                reason_end,
                skipped,
                platform,
                shuffle,
                offline,
                conn_country,
                spotify_track_uri,
                spotify_track_id,
                track_name_raw,
                artist_name_raw,
                album_name_raw,
                spotify_album_id,
                spotify_artist_ids_json,
                raw_payload_json,
            ),
        )
        row_id = int(cursor.lastrowid)
        _attach_membership_with_connection(
            connection,
            canonical_event_id=row_id,
            source_row_key=source_row_key,
            source_type=source_type,
        )
    return row_id


def insert_raw_play_event_if_new(
    *,
    source_type: str,
    source_row_key: str,
    played_at: str,
    ms_played: int,
    ms_played_method: str,
    raw_payload_json: str,
    ingest_run_id: str | None = None,
    source_event_id: str | None = None,
    cross_source_event_key: str | None = None,
    track_duration_ms: int | None = None,
    reason_start: str | None = None,
    reason_end: str | None = None,
    skipped: int | None = None,
    platform: str | None = None,
    shuffle: int | None = None,
    offline: int | None = None,
    conn_country: str | None = None,
    spotify_track_uri: str | None = None,
    spotify_track_id: str | None = None,
    track_name_raw: str | None = None,
    artist_name_raw: str | None = None,
    album_name_raw: str | None = None,
    spotify_album_id: str | None = None,
    spotify_artist_ids_json: str | None = None,
) -> int | None:
    with sqlite_connection(write=True, row_factory=sqlite3.Row) as connection:
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO raw_play_event (
              ingest_run_id,
              source_type,
              source_event_id,
              source_row_key,
              cross_source_event_key,
              played_at,
              ms_played,
              ms_played_method,
              track_duration_ms,
              reason_start,
              reason_end,
              skipped,
              platform,
              shuffle,
              offline,
              conn_country,
              spotify_track_uri,
              spotify_track_id,
              track_name_raw,
              artist_name_raw,
              album_name_raw,
              spotify_album_id,
              spotify_artist_ids_json,
              raw_payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ingest_run_id,
                source_type,
                source_event_id,
                source_row_key,
                cross_source_event_key,
                played_at,
                ms_played,
                ms_played_method,
                track_duration_ms,
                reason_start,
                reason_end,
                skipped,
                platform,
                shuffle,
                offline,
                conn_country,
                spotify_track_uri,
                spotify_track_id,
                track_name_raw,
                artist_name_raw,
                album_name_raw,
                spotify_album_id,
                spotify_artist_ids_json,
                raw_payload_json,
            ),
        )
        row_id = int(cursor.lastrowid) if cursor.lastrowid else None
        if row_id is not None:
            _attach_membership_with_connection(
                connection,
                canonical_event_id=row_id,
                source_row_key=source_row_key,
                source_type=source_type,
            )
    return row_id


def _to_unix_ms(timestamp_value: str | None) -> int | None:
    if timestamp_value is None:
        return None
    parsed = datetime.fromisoformat(str(timestamp_value).replace("Z", "+00:00"))
    return int(parsed.timestamp() * 1000)


def _insert_raw_spotify_recent_observation_with_connection(
    connection: sqlite3.Connection,
    *,
    ingest_run_id: str | None,
    source_row_key: str,
    played_at: str,
    ms_played_estimate: int,
    ms_played_method: str,
    ms_played_confidence: str,
    raw_payload_json: str,
    source_event_id: str | None = None,
    spotify_track_id: str | None = None,
    spotify_track_uri: str | None = None,
    spotify_album_id: str | None = None,
    spotify_artist_ids_json: str | None = None,
    track_name_raw: str | None = None,
    artist_name_raw: str | None = None,
    album_name_raw: str | None = None,
    track_duration_ms: int | None = None,
    ms_played_fallback_class: str | None = None,
    context_type: str | None = None,
    context_uri: str | None = None,
) -> dict[str, Any]:
    connection.row_factory = sqlite3.Row
    existing = connection.execute(
        """
        SELECT id
        FROM raw_spotify_recent
        WHERE source_row_key = ?
        LIMIT 1
        """,
        (source_row_key,),
    ).fetchone()
    if existing is not None:
        return {"row_id": int(existing["id"]), "action": "unchanged"}

    cursor = connection.execute(
        """
        INSERT INTO raw_spotify_recent (
          ingest_run_id,
          source_row_key,
          source_event_id,
          played_at,
          played_at_unix_ms,
          spotify_track_id,
          spotify_track_uri,
          spotify_album_id,
          spotify_artist_ids_json,
          track_name_raw,
          artist_name_raw,
          album_name_raw,
          track_duration_ms,
          ms_played_estimate,
          ms_played_method,
          ms_played_confidence,
          ms_played_fallback_class,
          context_type,
          context_uri,
          raw_payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ingest_run_id,
            source_row_key,
            source_event_id,
            played_at,
            _to_unix_ms(played_at),
            spotify_track_id,
            spotify_track_uri,
            spotify_album_id,
            spotify_artist_ids_json,
            track_name_raw,
            artist_name_raw,
            album_name_raw,
            track_duration_ms,
            ms_played_estimate,
            ms_played_method,
            ms_played_confidence,
            ms_played_fallback_class,
            context_type,
            context_uri,
            raw_payload_json,
        ),
    )
    return {"row_id": int(cursor.lastrowid), "action": "inserted"}


def insert_raw_spotify_recent_observation(
    *,
    ingest_run_id: str | None,
    source_row_key: str,
    played_at: str,
    ms_played_estimate: int,
    ms_played_method: str,
    ms_played_confidence: str,
    raw_payload_json: str,
    source_event_id: str | None = None,
    spotify_track_id: str | None = None,
    spotify_track_uri: str | None = None,
    spotify_album_id: str | None = None,
    spotify_artist_ids_json: str | None = None,
    track_name_raw: str | None = None,
    artist_name_raw: str | None = None,
    album_name_raw: str | None = None,
    track_duration_ms: int | None = None,
    ms_played_fallback_class: str | None = None,
    context_type: str | None = None,
    context_uri: str | None = None,
) -> dict[str, Any]:
    with sqlite_connection(write=True) as connection:
        return _insert_raw_spotify_recent_observation_with_connection(
            connection,
            ingest_run_id=ingest_run_id,
            source_row_key=source_row_key,
            played_at=played_at,
            ms_played_estimate=ms_played_estimate,
            ms_played_method=ms_played_method,
            ms_played_confidence=ms_played_confidence,
            raw_payload_json=raw_payload_json,
            source_event_id=source_event_id,
            spotify_track_id=spotify_track_id,
            spotify_track_uri=spotify_track_uri,
            spotify_album_id=spotify_album_id,
            spotify_artist_ids_json=spotify_artist_ids_json,
            track_name_raw=track_name_raw,
            artist_name_raw=artist_name_raw,
            album_name_raw=album_name_raw,
            track_duration_ms=track_duration_ms,
            ms_played_fallback_class=ms_played_fallback_class,
            context_type=context_type,
            context_uri=context_uri,
        )


def _insert_raw_spotify_history_observation_with_connection(
    connection: sqlite3.Connection,
    *,
    ingest_run_id: str | None,
    source_row_key: str,
    played_at: str,
    ms_played: int,
    raw_payload_json: str,
    spotify_track_id: str | None = None,
    spotify_track_uri: str | None = None,
    spotify_album_id: str | None = None,
    spotify_artist_ids_json: str | None = None,
    track_name_raw: str | None = None,
    artist_name_raw: str | None = None,
    album_name_raw: str | None = None,
    reason_start: str | None = None,
    reason_end: str | None = None,
    skipped: int | None = None,
    shuffle: int | None = None,
    offline: int | None = None,
    platform: str | None = None,
    conn_country: str | None = None,
    private_session: int | None = None,
) -> dict[str, Any]:
    connection.row_factory = sqlite3.Row
    existing = connection.execute(
        """
        SELECT id
        FROM raw_spotify_history
        WHERE source_row_key = ?
        LIMIT 1
        """,
        (source_row_key,),
    ).fetchone()
    if existing is not None:
        return {"row_id": int(existing["id"]), "action": "unchanged"}

    cursor = connection.execute(
        """
        INSERT INTO raw_spotify_history (
          ingest_run_id,
          source_row_key,
          played_at,
          played_at_unix_ms,
          spotify_track_id,
          spotify_track_uri,
          spotify_album_id,
          spotify_artist_ids_json,
          track_name_raw,
          artist_name_raw,
          album_name_raw,
          ms_played,
          reason_start,
          reason_end,
          skipped,
          shuffle,
          offline,
          platform,
          conn_country,
          private_session,
          raw_payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ingest_run_id,
            source_row_key,
            played_at,
            _to_unix_ms(played_at),
            spotify_track_id,
            spotify_track_uri,
            spotify_album_id,
            spotify_artist_ids_json,
            track_name_raw,
            artist_name_raw,
            album_name_raw,
            ms_played,
            reason_start,
            reason_end,
            skipped,
            shuffle,
            offline,
            platform,
            conn_country,
            private_session,
            raw_payload_json,
        ),
    )
    return {"row_id": int(cursor.lastrowid), "action": "inserted"}


def insert_raw_spotify_history_observation(
    *,
    ingest_run_id: str | None,
    source_row_key: str,
    played_at: str,
    ms_played: int,
    raw_payload_json: str,
    spotify_track_id: str | None = None,
    spotify_track_uri: str | None = None,
    spotify_album_id: str | None = None,
    spotify_artist_ids_json: str | None = None,
    track_name_raw: str | None = None,
    artist_name_raw: str | None = None,
    album_name_raw: str | None = None,
    reason_start: str | None = None,
    reason_end: str | None = None,
    skipped: int | None = None,
    shuffle: int | None = None,
    offline: int | None = None,
    platform: str | None = None,
    conn_country: str | None = None,
    private_session: int | None = None,
) -> dict[str, Any]:
    with sqlite_connection(write=True) as connection:
        return _insert_raw_spotify_history_observation_with_connection(
            connection,
            ingest_run_id=ingest_run_id,
            source_row_key=source_row_key,
            played_at=played_at,
            ms_played=ms_played,
            raw_payload_json=raw_payload_json,
            spotify_track_id=spotify_track_id,
            spotify_track_uri=spotify_track_uri,
            spotify_album_id=spotify_album_id,
            spotify_artist_ids_json=spotify_artist_ids_json,
            track_name_raw=track_name_raw,
            artist_name_raw=artist_name_raw,
            album_name_raw=album_name_raw,
            reason_start=reason_start,
            reason_end=reason_end,
            skipped=skipped,
            shuffle=shuffle,
            offline=offline,
            platform=platform,
            conn_country=conn_country,
            private_session=private_session,
        )


def _ms_played_method_rank(method: str | None) -> int:
    ranks = {
        "default_guess": 1,
        "api_chronology": 2,
        "history_source": 3,
    }
    return ranks.get(str(method), 0)


def _cap_ms_played(*, ms_played: int, track_duration_ms: int | None) -> int:
    if track_duration_ms is None:
        return int(ms_played)
    return max(0, min(int(ms_played), int(track_duration_ms)))


def _get_existing_event_by_source_row_key_with_connection(
    connection: sqlite3.Connection,
    *,
    source_row_key: str,
) -> sqlite3.Row | None:
    connection.row_factory = sqlite3.Row
    return connection.execute(
        """
        SELECT r.*
        FROM raw_play_event_membership m
        JOIN raw_play_event r
          ON r.id = m.canonical_event_id
        WHERE m.source_row_key = ?
        LIMIT 1
        """,
        (source_row_key,),
    ).fetchone()


def _get_existing_event_by_cross_source_event_key_with_connection(
    connection: sqlite3.Connection,
    *,
    cross_source_event_key: str,
) -> sqlite3.Row | None:
    connection.row_factory = sqlite3.Row
    return connection.execute(
        """
        SELECT *
        FROM raw_play_event
        WHERE cross_source_event_key = ?
        ORDER BY id ASC
        LIMIT 1
        """,
        (cross_source_event_key,),
    ).fetchone()


def _attach_membership_with_connection(
    connection: sqlite3.Connection,
    *,
    canonical_event_id: int,
    source_row_key: str,
    source_type: str,
) -> bool:
    before = connection.total_changes
    connection.execute(
        """
        INSERT OR IGNORE INTO raw_play_event_membership (
          canonical_event_id,
          source_row_key,
          source_type
        )
        VALUES (?, ?, ?)
        """,
        (canonical_event_id, source_row_key, source_type),
    )
    return (connection.total_changes - before) > 0


def _insert_or_upgrade_raw_play_event_with_connection(
    connection: sqlite3.Connection,
    *,
    source_type: str,
    source_row_key: str,
    played_at: str,
    ms_played: int,
    ms_played_method: str,
    raw_payload_json: str,
    ingest_run_id: str | None = None,
    source_event_id: str | None = None,
    cross_source_event_key: str | None = None,
    track_duration_ms: int | None = None,
    reason_start: str | None = None,
    reason_end: str | None = None,
    skipped: int | None = None,
    platform: str | None = None,
    shuffle: int | None = None,
    offline: int | None = None,
    conn_country: str | None = None,
    spotify_track_uri: str | None = None,
    spotify_track_id: str | None = None,
    track_name_raw: str | None = None,
    artist_name_raw: str | None = None,
    album_name_raw: str | None = None,
    spotify_album_id: str | None = None,
    spotify_artist_ids_json: str | None = None,
) -> dict[str, Any]:
    connection.row_factory = sqlite3.Row
    existing_from_source_row = _get_existing_event_by_source_row_key_with_connection(
        connection,
        source_row_key=source_row_key,
    )
    if existing_from_source_row is not None:
        return {
            "row_id": int(existing_from_source_row["id"]),
            "action": "unchanged",
            "match_type": "source_row_key",
        }

    existing_from_cross_source: sqlite3.Row | None = None
    if cross_source_event_key is not None:
        existing_from_cross_source = _get_existing_event_by_cross_source_event_key_with_connection(
            connection,
            cross_source_event_key=cross_source_event_key,
        )

    if existing_from_cross_source is None:
        cursor = connection.execute(
            """
            INSERT INTO raw_play_event (
              ingest_run_id,
              source_type,
              source_event_id,
              source_row_key,
              cross_source_event_key,
              played_at,
              ms_played,
              ms_played_method,
              duplicate_row_count,
              duplicate_merge_strategy,
              track_duration_ms,
              reason_start,
              reason_end,
              skipped,
              platform,
              shuffle,
              offline,
              conn_country,
              spotify_track_uri,
              spotify_track_id,
              track_name_raw,
              artist_name_raw,
              album_name_raw,
              spotify_album_id,
              spotify_artist_ids_json,
              raw_payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ingest_run_id,
                source_type,
                source_event_id,
                source_row_key,
                cross_source_event_key,
                played_at,
                ms_played,
                ms_played_method,
                0,
                "none",
                track_duration_ms,
                reason_start,
                reason_end,
                skipped,
                platform,
                shuffle,
                offline,
                conn_country,
                spotify_track_uri,
                spotify_track_id,
                track_name_raw,
                artist_name_raw,
                album_name_raw,
                spotify_album_id,
                spotify_artist_ids_json,
                raw_payload_json,
            ),
        )
        row_id = int(cursor.lastrowid)
        _attach_membership_with_connection(
            connection,
            canonical_event_id=row_id,
            source_row_key=source_row_key,
            source_type=source_type,
        )
        return {"row_id": row_id, "action": "inserted", "match_type": "none"}

    canonical_event_id = int(existing_from_cross_source["id"])
    membership_added = _attach_membership_with_connection(
        connection,
        canonical_event_id=canonical_event_id,
        source_row_key=source_row_key,
        source_type=source_type,
    )

    existing_method = str(existing_from_cross_source["ms_played_method"])
    incoming_method = str(ms_played_method)
    existing_rank = _ms_played_method_rank(existing_method)
    incoming_rank = _ms_played_method_rank(incoming_method)

    next_method = existing_method
    chosen_ms_played = int(existing_from_cross_source["ms_played"])
    merge_strategy = "same_event_max_ms_played"

    if incoming_rank > existing_rank:
        next_method = incoming_method
        chosen_ms_played = int(ms_played)
        merge_strategy = "same_event_upgraded_by_method"
    elif incoming_rank == existing_rank and int(ms_played) > int(existing_from_cross_source["ms_played"]):
        chosen_ms_played = int(ms_played)

    chosen_track_duration = (
        int(track_duration_ms)
        if track_duration_ms is not None
        else (
            int(existing_from_cross_source["track_duration_ms"])
            if existing_from_cross_source["track_duration_ms"] is not None
            else None
        )
    )
    chosen_ms_played = _cap_ms_played(
        ms_played=chosen_ms_played,
        track_duration_ms=chosen_track_duration,
    )

    connection.execute(
        """
        UPDATE raw_play_event
        SET
          ms_played = ?,
          ms_played_method = ?,
          duplicate_row_count = duplicate_row_count + ?,
          duplicate_merge_strategy = ?,
          track_duration_ms = COALESCE(?, track_duration_ms),
          reason_start = COALESCE(?, reason_start),
          reason_end = COALESCE(?, reason_end),
          skipped = COALESCE(?, skipped),
          platform = COALESCE(?, platform),
          shuffle = COALESCE(?, shuffle),
          offline = COALESCE(?, offline),
          conn_country = COALESCE(?, conn_country)
        WHERE id = ?
        """,
        (
            chosen_ms_played,
            next_method,
            1 if membership_added else 0,
            merge_strategy,
            track_duration_ms,
            reason_start,
            reason_end,
            skipped,
            platform,
            shuffle,
            offline,
            conn_country,
            canonical_event_id,
        ),
    )
    return {
        "row_id": canonical_event_id,
        "action": "merged_duplicate_row" if membership_added else "unchanged",
        "match_type": "cross_source_event_key",
        "merge_strategy": merge_strategy,
    }


def insert_or_upgrade_raw_play_event(
    *,
    source_type: str,
    source_row_key: str,
    played_at: str,
    ms_played: int,
    ms_played_method: str,
    raw_payload_json: str,
    ingest_run_id: str | None = None,
    source_event_id: str | None = None,
    cross_source_event_key: str | None = None,
    track_duration_ms: int | None = None,
    reason_start: str | None = None,
    reason_end: str | None = None,
    skipped: int | None = None,
    platform: str | None = None,
    shuffle: int | None = None,
    offline: int | None = None,
    conn_country: str | None = None,
    spotify_track_uri: str | None = None,
    spotify_track_id: str | None = None,
    track_name_raw: str | None = None,
    artist_name_raw: str | None = None,
    album_name_raw: str | None = None,
    spotify_album_id: str | None = None,
    spotify_artist_ids_json: str | None = None,
) -> dict[str, Any]:
    with sqlite_connection(write=True) as connection:
        return _insert_or_upgrade_raw_play_event_with_connection(
            connection,
            source_type=source_type,
            source_row_key=source_row_key,
            played_at=played_at,
            ms_played=ms_played,
            ms_played_method=ms_played_method,
            raw_payload_json=raw_payload_json,
            ingest_run_id=ingest_run_id,
            source_event_id=source_event_id,
            cross_source_event_key=cross_source_event_key,
            track_duration_ms=track_duration_ms,
            reason_start=reason_start,
            reason_end=reason_end,
            skipped=skipped,
            platform=platform,
            shuffle=shuffle,
            offline=offline,
            conn_country=conn_country,
            spotify_track_uri=spotify_track_uri,
            spotify_track_id=spotify_track_id,
            track_name_raw=track_name_raw,
            artist_name_raw=artist_name_raw,
            album_name_raw=album_name_raw,
            spotify_album_id=spotify_album_id,
            spotify_artist_ids_json=spotify_artist_ids_json,
        )


def get_raw_play_event_by_source_row_key(source_row_key: str) -> dict[str, Any] | None:
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        row = connection.execute(
            """
            SELECT r.*
            FROM raw_play_event_membership m
            JOIN raw_play_event r
              ON r.id = m.canonical_event_id
            WHERE m.source_row_key = ?
            LIMIT 1
            """,
            (source_row_key,),
        ).fetchone()
    return dict(row) if row is not None else None


def list_canonical_play_events(limit: int = 50) -> list[dict[str, Any]]:
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        rows = connection.execute(
            """
            SELECT
              id,
              NULL AS ingest_run_id,
              CASE
                WHEN raw_spotify_recent_id IS NOT NULL AND raw_spotify_history_id IS NOT NULL THEN 'canonical_matched'
                WHEN raw_spotify_history_id IS NOT NULL THEN 'spotify_history'
                WHEN raw_spotify_recent_id IS NOT NULL THEN 'spotify_recent'
                ELSE 'canonical'
              END AS source_type,
              NULL AS source_event_id,
              NULL AS source_row_key,
              NULL AS cross_source_event_key,
              canonical_ended_at AS played_at,
              canonical_ms_played AS ms_played,
              timing_source AS ms_played_method,
              NULL AS track_duration_ms,
              canonical_reason_start AS reason_start,
              canonical_reason_end AS reason_end,
              canonical_skipped AS skipped,
              canonical_shuffle AS shuffle,
              canonical_offline AS offline,
              NULL AS platform,
              NULL AS conn_country,
              spotify_track_uri,
              spotify_track_id,
              track_name_canonical AS track_name_raw,
              artist_name_canonical AS artist_name_raw,
              album_name_canonical AS album_name_raw,
              spotify_album_id,
              spotify_artist_ids_json,
              NULL AS raw_payload_json,
              created_at AS inserted_at,
              raw_spotify_recent_id,
              raw_spotify_history_id
            FROM v_fact_play_event_with_sources
            WHERE canonical_ended_at IS NOT NULL
            ORDER BY canonical_ended_at DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]


def list_raw_play_events(limit: int = 50) -> list[dict[str, Any]]:
    # Compatibility wrapper: this now returns canonicalized play events.
    # Prefer list_canonical_play_events() for new callers.
    return list_canonical_play_events(limit=limit)


def list_raw_spotify_recent_rows(*, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
    bounded_limit = max(1, int(limit))
    bounded_offset = max(0, int(offset))
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        rows = connection.execute(
            """
            SELECT
              id,
              ingest_run_id,
              source_row_key,
              source_event_id,
              played_at,
              played_at_unix_ms,
              spotify_track_id,
              spotify_track_uri,
              spotify_album_id,
              spotify_artist_ids_json,
              track_name_raw,
              artist_name_raw,
              album_name_raw,
              track_duration_ms,
              ms_played_estimate,
              ms_played_method,
              ms_played_confidence,
              ms_played_fallback_class,
              context_type,
              context_uri,
              raw_payload_json,
              inserted_at
            FROM raw_spotify_recent
            ORDER BY played_at DESC, id DESC
            LIMIT ?
            OFFSET ?
            """,
            (bounded_limit, bounded_offset),
        ).fetchall()
    return [dict(row) for row in rows]


def list_unified_top_tracks(
    *,
    limit: int = 50,
    recent_window_days: int = 28,
    as_of_iso: str | None = None,
) -> list[dict[str, Any]]:
    as_of_dt = (
        datetime.fromisoformat(as_of_iso.replace("Z", "+00:00"))
        if as_of_iso
        else datetime.now(UTC)
    )
    recent_cutoff = (as_of_dt - timedelta(days=max(0, int(recent_window_days)))).astimezone(UTC)
    recent_cutoff_iso = recent_cutoff.isoformat().replace("+00:00", "Z")

    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        rows = connection.execute(
            """
            WITH normalized AS (
              SELECT
                id,
                canonical_ended_at AS played_at,
                CASE
                  WHEN spotify_track_id IS NOT NULL AND spotify_track_id != '' THEN spotify_track_id
                  WHEN spotify_track_uri IS NOT NULL AND spotify_track_uri != '' THEN spotify_track_uri
                  ELSE '__unknown__:' || LOWER(TRIM(COALESCE(track_name_canonical, ''))) || ':' || LOWER(TRIM(COALESCE(artist_name_canonical, '')))
                END AS track_id,
                track_name_canonical AS track_name_raw,
                artist_name_canonical AS artist_name_raw
              FROM v_fact_play_event_with_sources
              WHERE canonical_ended_at IS NOT NULL
            ),
            agg AS (
              SELECT
                track_id,
                COUNT(*) AS total_plays,
                SUM(CASE WHEN played_at >= ? THEN 1 ELSE 0 END) AS recent_plays,
                MAX(played_at) AS last_played_at
              FROM normalized
              GROUP BY track_id
            )
            SELECT
              agg.track_id AS track_id,
              COALESCE(
                (
                  SELECT n.track_name_raw
                  FROM normalized n
                  WHERE n.track_id = agg.track_id
                  ORDER BY n.played_at DESC, n.id DESC
                  LIMIT 1
                ),
                'Unknown track'
              ) AS track_name,
              COALESCE(
                (
                  SELECT n.artist_name_raw
                  FROM normalized n
                  WHERE n.track_id = agg.track_id
                  ORDER BY n.played_at DESC, n.id DESC
                  LIMIT 1
                ),
                'Unknown artist'
              ) AS artist_name,
              agg.total_plays AS total_plays,
              agg.recent_plays AS recent_plays,
              agg.last_played_at AS last_played_at
            FROM agg
            ORDER BY agg.total_plays DESC, agg.last_played_at DESC, agg.track_id ASC
            LIMIT ?
            """,
            (recent_cutoff_iso, limit),
        ).fetchall()
        return [dict(row) for row in rows]


def raw_play_event_exists(
    *,
    source_row_key: str,
    cross_source_event_key: str | None = None,
) -> bool:
    with sqlite_connection() as connection:
        row = connection.execute(
            """
            SELECT 1
            FROM raw_play_event_membership
            WHERE source_row_key = ?
            LIMIT 1
            """,
            (source_row_key,),
        ).fetchone()
        if row is not None:
            return True

        if cross_source_event_key is None:
            return False

        cross_row = connection.execute(
            """
            SELECT 1
            FROM raw_play_event
            WHERE cross_source_event_key = ?
            LIMIT 1
            """,
            (cross_source_event_key,),
        ).fetchone()
        return cross_row is not None


def insert_ingest_run(
    *,
    run_id: str,
    source_type: str,
    started_at: str,
    source_ref: str | None = None,
    status: str = "running",
) -> str:
    with sqlite_connection(write=True) as connection:
        _insert_ingest_run_with_connection(
            connection,
            run_id=run_id,
            source_type=source_type,
            started_at=started_at,
            source_ref=source_ref,
            status=status,
        )
    return run_id


def recover_stale_ingest_runs(*, stale_after_minutes: int = 60) -> dict[str, Any]:
    stale_after_minutes = max(1, int(stale_after_minutes))
    cutoff_dt = (datetime.now(UTC) - timedelta(minutes=stale_after_minutes)).isoformat().replace("+00:00", "Z")
    recovered_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    with sqlite_connection(write=True, row_factory=sqlite3.Row) as connection:
        stale_rows = connection.execute(
            """
            SELECT id
            FROM ingest_run
            WHERE status = 'running'
              AND completed_at IS NULL
              AND COALESCE(last_heartbeat_at, started_at) <= ?
            ORDER BY started_at ASC
            """,
            (cutoff_dt,),
        ).fetchall()

        stale_ids = [str(row["id"]) for row in stale_rows]
        if stale_ids:
            placeholders = ",".join("?" for _ in stale_ids)
            connection.execute(
                f"""
                UPDATE ingest_run
                SET
                  status = 'failed',
                  completed_at = ?,
                  error_count = CASE
                    WHEN error_count <= 0 THEN 1
                    ELSE error_count
                  END,
                  last_heartbeat_at = COALESCE(last_heartbeat_at, started_at)
                WHERE id IN ({placeholders})
                """,
                (recovered_at, *stale_ids),
            )

    return {
        "stale_after_minutes": stale_after_minutes,
        "cutoff_last_heartbeat_at": cutoff_dt,
        "recovered_at": recovered_at,
        "recovered_count": len(stale_ids),
        "recovered_run_ids": stale_ids,
    }


def _insert_ingest_run_with_connection(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    source_type: str,
    started_at: str,
    source_ref: str | None = None,
    status: str = "running",
) -> str:
    connection.execute(
        """
        INSERT INTO ingest_run (
          id,
          source_type,
          source_ref,
          started_at,
          status,
          last_heartbeat_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (run_id, source_type, source_ref, started_at, status, started_at),
    )
    return run_id


def patch_ingest_run_heartbeat(
    *,
    run_id: str,
    heartbeat_at: str,
) -> None:
    with sqlite_connection(write=True) as connection:
        cursor = connection.execute(
            """
            UPDATE ingest_run
            SET last_heartbeat_at = ?
            WHERE id = ?
              AND status = 'running'
            """,
            (heartbeat_at, run_id),
        )
        if cursor.rowcount not in (0, 1):
            raise RuntimeError(f"unexpected ingest_run update count for id={run_id}: {cursor.rowcount}")


def patch_ingest_run_timing_phases(
    *,
    run_id: str,
    timing_phases_ms: dict[str, float] | None,
) -> None:
    phases = dict(timing_phases_ms or {})
    with sqlite_connection(write=True) as connection:
        cursor = connection.execute(
            """
            UPDATE ingest_run
            SET
              file_discovery_ms = ?,
              file_read_ms = ?,
              file_parse_ms = ?,
              mapping_ms = ?,
              raw_inserts_ms = ?,
              matcher_ms = ?,
              projector_ms = ?,
              downstream_pipeline_ms = ?,
              final_commit_ms = ?,
              total_duration_ms = ?
            WHERE id = ?
            """,
            (
                phases.get("file_discovery_ms"),
                phases.get("file_read_ms"),
                phases.get("file_parse_ms"),
                phases.get("mapping_ms"),
                phases.get("raw_inserts_ms"),
                phases.get("matcher_ms"),
                phases.get("projector_ms"),
                phases.get("downstream_pipeline_ms"),
                phases.get("final_commit_ms"),
                phases.get("total_duration_ms"),
                run_id,
            ),
        )
        if cursor.rowcount != 1:
            raise RuntimeError(f"ingest_run not found for id={run_id}")


def _complete_ingest_run_with_connection(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    completed_at: str,
    row_count: int,
    inserted_count: int,
    duplicate_count: int,
    error_count: int = 0,
    status: str = "completed",
) -> None:
    cursor = connection.execute(
        """
        UPDATE ingest_run
        SET
          completed_at = ?,
          last_heartbeat_at = ?,
          row_count = ?,
          inserted_count = ?,
          duplicate_count = ?,
          error_count = ?,
          status = ?
        WHERE id = ?
        """,
        (
            completed_at,
            completed_at,
            row_count,
            inserted_count,
            duplicate_count,
            error_count,
            status,
            run_id,
        ),
    )
    if cursor.rowcount != 1:
        raise RuntimeError(f"ingest_run not found for id={run_id}")


def complete_ingest_run(
    *,
    run_id: str,
    completed_at: str,
    row_count: int,
    inserted_count: int,
    duplicate_count: int,
    error_count: int = 0,
    status: str = "completed",
) -> None:
    with sqlite_connection(write=True) as connection:
        _complete_ingest_run_with_connection(
            connection,
            run_id=run_id,
            completed_at=completed_at,
            row_count=row_count,
            inserted_count=inserted_count,
            duplicate_count=duplicate_count,
            error_count=error_count,
            status=status,
        )


def complete_ingest_run_and_patch_spotify_sync_state(
    *,
    run_id: str,
    completed_at: str,
    row_count: int,
    inserted_count: int,
    duplicate_count: int,
    error_count: int = 0,
    last_successful_played_at: str | None = None,
) -> None:
    with sqlite_connection(write=True) as connection:
        ingest_cursor = connection.execute(
            """
            UPDATE ingest_run
            SET
              completed_at = ?,
              last_heartbeat_at = ?,
              row_count = ?,
              inserted_count = ?,
              duplicate_count = ?,
              error_count = ?,
              status = 'completed'
            WHERE id = ?
            """,
            (
                completed_at,
                completed_at,
                row_count,
                inserted_count,
                duplicate_count,
                error_count,
                run_id,
            ),
        )
        if ingest_cursor.rowcount != 1:
            raise RuntimeError(f"ingest_run not found for id={run_id}")

        sync_cursor = connection.execute(
            """
            UPDATE spotify_sync_state
            SET
              last_successful_played_at = COALESCE(?, last_successful_played_at),
              last_completed_at = ?,
              last_run_id = ?,
              updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id = 1
            """,
            (
                last_successful_played_at,
                completed_at,
                run_id,
            ),
        )
        if sync_cursor.rowcount != 1:
            raise RuntimeError("spotify_sync_state row missing")


def get_spotify_sync_state() -> dict[str, Any]:
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        row = connection.execute(
            "SELECT * FROM spotify_sync_state WHERE id = 1"
        ).fetchone()
    if row is None:
        raise RuntimeError("spotify_sync_state row missing")
    return dict(row)


def patch_spotify_sync_state(
    *,
    last_successful_played_at: str | None = None,
    overlap_lookback_seconds: int | None = None,
    last_started_at: str | None = None,
    last_completed_at: str | None = None,
    last_run_id: str | None = None,
    updated_at: str | None = None,
) -> None:
    with sqlite_connection(write=True) as connection:
        connection.execute(
            """
            UPDATE spotify_sync_state
            SET
              last_successful_played_at = COALESCE(?, last_successful_played_at),
              overlap_lookback_seconds = COALESCE(?, overlap_lookback_seconds),
              last_started_at = COALESCE(?, last_started_at),
              last_completed_at = COALESCE(?, last_completed_at),
              last_run_id = COALESCE(?, last_run_id),
              updated_at = COALESCE(?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            WHERE id = 1
            """,
            (
                last_successful_played_at,
                overlap_lookback_seconds,
                last_started_at,
                last_completed_at,
                last_run_id,
                updated_at,
            ),
        )


def get_ingest_run(run_id: str) -> dict[str, Any] | None:
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        row = connection.execute(
            """
            SELECT
              id,
              source_type,
              source_ref,
              started_at,
              completed_at,
              last_heartbeat_at,
              status,
              row_count,
              inserted_count,
              duplicate_count,
              error_count,
              file_discovery_ms,
              file_read_ms,
              file_parse_ms,
              mapping_ms,
              raw_inserts_ms,
              matcher_ms,
              projector_ms,
              downstream_pipeline_ms,
              final_commit_ms,
              total_duration_ms,
              created_at
            FROM ingest_run
            WHERE id = ?
            """,
            (run_id,),
        ).fetchone()
    return dict(row) if row is not None else None


def list_ingest_runs(
    *,
    source_type: str | None = None,
    source_ref: str | None = None,
    status: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if source_type is not None:
        clauses.append("source_type = ?")
        params.append(source_type)
    if source_ref is not None:
        clauses.append("source_ref = ?")
        params.append(source_ref)
    if status is not None:
        clauses.append("status = ?")
        params.append(status)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    bounded_limit = max(1, int(limit))

    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        rows = connection.execute(
            f"""
            SELECT
              id,
              source_type,
              source_ref,
              started_at,
              completed_at,
              last_heartbeat_at,
              status,
              row_count,
              inserted_count,
              duplicate_count,
              error_count,
              file_discovery_ms,
              file_read_ms,
              file_parse_ms,
              mapping_ms,
              raw_inserts_ms,
              matcher_ms,
              projector_ms,
              downstream_pipeline_ms,
              final_commit_ms,
              total_duration_ms,
              created_at
            FROM ingest_run
            {where_sql}
            ORDER BY started_at DESC, id DESC
            LIMIT ?
            """,
            (*params, bounded_limit),
        ).fetchall()
    return [dict(row) for row in rows]


def delete_ingest_run(
    *,
    run_id: str,
    delete_raw_events: bool = False,
) -> dict[str, int]:
    with sqlite_connection(write=True, row_factory=sqlite3.Row) as connection:
        existing = connection.execute(
            "SELECT id FROM ingest_run WHERE id = ? LIMIT 1",
            (run_id,),
        ).fetchone()
        if existing is None:
            return {
                "deleted_ingest_runs": 0,
                "deleted_raw_events": 0,
                "deleted_memberships": 0,
            }

        raw_count = int(
            connection.execute(
                "SELECT count(*) FROM raw_play_event WHERE ingest_run_id = ?",
                (run_id,),
            ).fetchone()[0]
        )
        if raw_count > 0 and not delete_raw_events:
            raise RuntimeError(
                f"ingest_run {run_id} has {raw_count} raw_play_event rows. "
                "Pass delete_raw_events=True to delete the run and its dependent rows."
            )

        deleted_memberships = 0
        deleted_raw_events = 0
        if delete_raw_events and raw_count > 0:
            membership_cursor = connection.execute(
                """
                DELETE FROM raw_play_event_membership
                WHERE canonical_event_id IN (
                  SELECT id FROM raw_play_event WHERE ingest_run_id = ?
                )
                """,
                (run_id,),
            )
            deleted_memberships = int(membership_cursor.rowcount)

            raw_cursor = connection.execute(
                "DELETE FROM raw_play_event WHERE ingest_run_id = ?",
                (run_id,),
            )
            deleted_raw_events = int(raw_cursor.rowcount)

        run_cursor = connection.execute(
            "DELETE FROM ingest_run WHERE id = ?",
            (run_id,),
        )
        deleted_ingest_runs = int(run_cursor.rowcount)

    return {
        "deleted_ingest_runs": deleted_ingest_runs,
        "deleted_raw_events": deleted_raw_events,
        "deleted_memberships": deleted_memberships,
    }


def update_spotify_sync_state(
    *,
    last_successful_played_at: str | None = None,
    overlap_lookback_seconds: int | None = None,
    last_started_at: str | None = None,
    last_completed_at: str | None = None,
    last_run_id: str | None = None,
    updated_at: str | None = None,
) -> None:
    patch_spotify_sync_state(
        last_successful_played_at=last_successful_played_at,
        overlap_lookback_seconds=overlap_lookback_seconds,
        last_started_at=last_started_at,
        last_completed_at=last_completed_at,
        last_run_id=last_run_id,
        updated_at=updated_at,
    )


def list_spotify_auth_users(*, active_only: bool = True, limit: int = 500) -> list[dict[str, Any]]:
    where_sql = "WHERE reauth_required = 0" if active_only else ""
    bounded_limit = max(1, int(limit))
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        rows = connection.execute(
            f"""
            SELECT
              user_id,
              spotify_user_id,
              scopes,
              reauth_required,
              reauth_reason,
              expires_at,
              updated_at
            FROM spotify_auth
            {where_sql}
            ORDER BY updated_at DESC, user_id ASC
            LIMIT ?
            """,
            (bounded_limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_spotify_auth_record(user_id: str) -> dict[str, Any] | None:
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        row = connection.execute(
            """
            SELECT
              user_id,
              spotify_user_id,
              scopes,
              reauth_required,
              reauth_reason,
              expires_at,
              updated_at
            FROM spotify_auth
            WHERE user_id = ?
            LIMIT 1
            """,
            (str(user_id),),
        ).fetchone()
    return dict(row) if row is not None else None


def delete_spotify_auth_record(user_id: str) -> bool:
    with sqlite_connection(write=True) as connection:
        cursor = connection.execute(
            "DELETE FROM spotify_auth WHERE user_id = ?",
            (str(user_id),),
        )
    return int(cursor.rowcount) > 0


def insert_live_playback_event(
    *,
    user_id: str,
    spotify_user_id: str | None,
    has_playback: bool,
    item_type: str | None = None,
    item_id: str | None = None,
    item_name: str | None = None,
    spotify_track_uri: str | None = None,
    artist_names_json: str | None = None,
    album_name: str | None = None,
    progress_ms: int | None = None,
    duration_ms: int | None = None,
    is_playing: bool | None = None,
    device_id: str | None = None,
    device_name: str | None = None,
    device_type: str | None = None,
    spotify_timestamp_ms: int | None = None,
    raw_payload_json: str | None = None,
    source: str = "current_playback",
) -> int:
    with sqlite_connection(write=True) as connection:
        cursor = connection.execute(
            """
            INSERT INTO live_playback_event (
              user_id,
              spotify_user_id,
              source,
              has_playback,
              item_type,
              item_id,
              item_name,
              spotify_track_uri,
              artist_names_json,
              album_name,
              progress_ms,
              duration_ms,
              is_playing,
              device_id,
              device_name,
              device_type,
              spotify_timestamp_ms,
              raw_payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(user_id),
                str(spotify_user_id) if spotify_user_id else None,
                str(source),
                1 if has_playback else 0,
                item_type,
                item_id,
                item_name,
                spotify_track_uri,
                artist_names_json,
                album_name,
                progress_ms,
                duration_ms,
                1 if is_playing else 0 if is_playing is not None else None,
                device_id,
                device_name,
                device_type,
                spotify_timestamp_ms,
                raw_payload_json,
            ),
        )
    return int(cursor.lastrowid)


def _normalize_name(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(str(value).strip().lower().split())
    return normalized or None


def _stable_text_key(*parts: str | None) -> str:
    payload = "|".join("" if part is None else str(part).strip() for part in parts)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _parse_spotify_track_id_from_uri(uri: str | None) -> str | None:
    if uri is None:
        return None
    candidate = str(uri).strip()
    prefix = "spotify:track:"
    if not candidate.startswith(prefix):
        return None
    external_id = candidate[len(prefix) :].strip()
    return external_id or None


TRACK_ANALYSIS_GROUPING_BLOCK_PATTERN = re.compile(
    r"(\bremix\b|\brework\b|\bcover\b)",
    re.IGNORECASE,
)
TRACK_ANALYSIS_GROUPABLE_FIXED_VARIANT_PATTERN = re.compile(
    r"(\blive\b|\bremaster(?:ed)?\b|\bacoustic\b|\bdemo\b|\binstrumental\b|\bradio edit\b|\bexplicit\b|\bclean\b)",
    re.IGNORECASE,
)
TRAILING_BRACKET_BLOCK_PATTERN = re.compile(r"\s*[\(\[]([^\)\]]+)[\)\]]\s*$")
TRAILING_DASH_BLOCK_PATTERN = re.compile(r"\s*[-–—:]\s*([^–—:\(\)\[\]]+)\s*$")
CONSERVATIVE_ANALYSIS_TRACK_GROUPING_NOTE_PREFIX = "conservative_exact_title_primary_artist:"
SONG_FAMILY_ANALYSIS_MATCH_METHOD = "song_family_title_primary_artist"

def _analysis_variant_label_candidates(value: str | None) -> set[str]:
    if value is None:
        return set()
    text = str(value)
    candidates: set[str] = set()
    bracket_match = TRAILING_BRACKET_BLOCK_PATTERN.search(text)
    if bracket_match:
        candidates.add(_normalized_variant_label(bracket_match.group(1)))
    dash_match = TRAILING_DASH_BLOCK_PATTERN.search(text)
    if dash_match:
        candidates.add(_normalized_variant_label(dash_match.group(1)))
    return candidates


def _normalized_variant_label(value: str) -> str:
    return " ".join(str(value).strip().lower().split())


def _is_groupable_analysis_variant_text(value: str) -> bool:
    if TRACK_ANALYSIS_GROUPABLE_FIXED_VARIANT_PATTERN.search(value):
        return True
    return any(component.groupable_by_default for component in classify_label_families(value))


def _analysis_grouping_base_title(value: str | None) -> str | None:
    if value is None:
        return None
    working = str(value).strip()
    if not working:
        return None
    if TRACK_ANALYSIS_GROUPING_BLOCK_PATTERN.search(working):
        return None

    changed = True
    while changed and working:
        changed = False
        bracket_match = TRAILING_BRACKET_BLOCK_PATTERN.search(working)
        if bracket_match and _is_groupable_analysis_variant_text(bracket_match.group(1)):
            working = working[: bracket_match.start()].strip()
            changed = True
            continue

        dash_match = TRAILING_DASH_BLOCK_PATTERN.search(working)
        if dash_match and _is_groupable_analysis_variant_text(dash_match.group(1)):
            working = working[: dash_match.start()].strip()
            changed = True

    normalized = _normalize_name(working)
    return normalized


def _analysis_variant_categories(value: str | None) -> set[str]:
    if value is None:
        return set()
    categories: set[str] = set()
    for component in classify_label_families(value):
        if component.family == "edit" and component.semantic_category == "broadcast_length_or_content_edit":
            categories.add("radio_edit")
            continue
        if component.family == "content_rating":
            if "explicit" in component.normalized_label:
                categories.add("explicit")
            if "clean" in component.normalized_label:
                categories.add("clean")
            continue
        categories.add(component.family)
    return categories


def _analysis_group_confidence(group_rows: list[sqlite3.Row]) -> float:
    confidence = 0.9
    category_penalties = {
        "live": 0.2,
        "acoustic": 0.08,
        "demo": 0.1,
        "instrumental": 0.08,
        "remaster": 0.03,
        "radio_edit": 0.04,
        "edit": 0.05,
        "explicit": 0.02,
        "clean": 0.02,
        "version": 0.05,
        "packaging": 0.03,
        "mix": 0.04,
        "featured_credit": 0.04,
        "session": 0.08,
        "recording_context": 0.08,
    }

    seen_categories: set[str] = set()
    plain_title_present = False
    distinct_titles: set[str] = set()
    distinct_albums: set[str] = set()

    for row in group_rows:
        title = str(row["primary_name"])
        distinct_titles.add(title)
        categories = _analysis_variant_categories(title)
        if categories:
            seen_categories.update(categories)
        else:
            plain_title_present = True

        album_names = str(row["album_names"]) if "album_names" in row.keys() and row["album_names"] is not None else ""
        if album_names:
            distinct_albums.add(album_names)

    for category in seen_categories:
        confidence -= category_penalties.get(category, 0.0)

    if plain_title_present and seen_categories:
        confidence -= 0.03
    if len(distinct_titles) > 1:
        confidence -= min(0.08, 0.02 * (len(distinct_titles) - 1))
    if len(distinct_albums) > 1:
        confidence -= min(0.08, 0.01 * (len(distinct_albums) - 1))

    return max(0.45, round(confidence, 2))


def _parse_grouping_note(grouping_note: str | None) -> tuple[str | None, str | None]:
    if grouping_note is None:
        return None, None
    value = str(grouping_note).strip()
    prefix = CONSERVATIVE_ANALYSIS_TRACK_GROUPING_NOTE_PREFIX
    if not value.startswith(prefix):
        return None, value
    payload = value[len(prefix) :]
    parts = payload.split("|", 1)
    if len(parts) == 2:
        return parts[0] or None, parts[1] or None
    return parts[0] or None, None


def _extract_spotify_artist_refs(
    *,
    spotify_artist_ids_json: str | None,
    artist_name_raw: str | None,
    raw_payload_json: str | None,
) -> list[dict[str, str | None]]:
    artist_ids: list[str] = []
    try:
        parsed_ids = json.loads(spotify_artist_ids_json) if spotify_artist_ids_json else []
        if isinstance(parsed_ids, list):
            artist_ids = [str(value).strip() for value in parsed_ids if str(value).strip()]
    except json.JSONDecodeError:
        artist_ids = []

    payload_artists: list[dict[str, str | None]] = []
    try:
        payload = json.loads(raw_payload_json) if raw_payload_json else None
    except json.JSONDecodeError:
        payload = None

    if isinstance(payload, dict):
        item = payload.get("track") if isinstance(payload.get("track"), dict) else payload.get("item")
        if isinstance(item, dict):
            artists = item.get("artists")
            if isinstance(artists, list):
                for artist in artists:
                    if isinstance(artist, dict):
                        payload_artists.append(
                            {
                                "external_id": str(artist.get("id")).strip() if artist.get("id") else None,
                                "name": str(artist.get("name")).strip() if artist.get("name") else None,
                                "external_uri": str(artist.get("uri")).strip() if artist.get("uri") else None,
                            }
                        )

    if payload_artists:
        refs: list[dict[str, str | None]] = []
        seen_ids: set[str] = set()
        for artist in payload_artists:
            external_id = artist.get("external_id")
            if external_id:
                seen_ids.add(external_id)
                refs.append(artist)
        for artist_id in artist_ids:
            if artist_id not in seen_ids:
                refs.append({"external_id": artist_id, "name": None, "external_uri": None})
        return refs

    if len(artist_ids) == 1:
        return [
            {
                "external_id": artist_ids[0],
                "name": artist_name_raw.strip() if artist_name_raw else None,
                "external_uri": None,
            }
        ]

    return [
        {
            "external_id": artist_id,
            "name": None,
            "external_uri": None,
        }
        for artist_id in artist_ids
    ]


def _create_artist_with_connection(
    connection: sqlite3.Connection,
    *,
    artist_name: str | None,
) -> int:
    canonical_name = artist_name.strip() if artist_name and artist_name.strip() else "Unknown artist"
    normalized_name = _normalize_name(canonical_name)
    cursor = connection.execute(
        """
        INSERT INTO artist (
          canonical_name,
          sort_name
        )
        VALUES (?, ?)
        """,
        (canonical_name, normalized_name),
    )
    return int(cursor.lastrowid)


def _ensure_source_artist_mapping_with_connection(
    connection: sqlite3.Connection,
    *,
    external_id: str,
    external_uri: str | None,
    artist_name: str | None,
    raw_payload_json: str | None,
) -> int:
    existing = connection.execute(
        """
        SELECT
          sa.id AS source_artist_id,
          sam.artist_id AS artist_id
        FROM source_artist sa
        LEFT JOIN source_artist_map sam
          ON sam.source_artist_id = sa.id
        WHERE sa.source_name = 'spotify'
          AND sa.external_id = ?
        ORDER BY sam.id ASC, sa.id ASC
        LIMIT 1
        """,
        (external_id,),
    ).fetchone()
    if existing is not None and existing["artist_id"] is not None:
        return int(existing["artist_id"])

    source_artist_id: int
    if existing is None:
        cursor = connection.execute(
            """
            INSERT INTO source_artist (
              source_name,
              external_id,
              external_uri,
              source_name_raw,
              raw_payload_json
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            ("spotify", external_id, external_uri, artist_name, raw_payload_json),
        )
        source_artist_id = int(cursor.lastrowid)
    else:
        source_artist_id = int(existing["source_artist_id"])
        connection.execute(
            """
            UPDATE source_artist
            SET
              external_uri = COALESCE(external_uri, ?),
              source_name_raw = COALESCE(source_name_raw, ?),
              raw_payload_json = COALESCE(raw_payload_json, ?),
              updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id = ?
            """,
            (external_uri, artist_name, raw_payload_json, source_artist_id),
        )

    artist_id = _create_artist_with_connection(connection, artist_name=artist_name)
    connection.execute(
        """
        INSERT OR IGNORE INTO source_artist_map (
          source_artist_id,
          artist_id,
          match_method,
          confidence,
          status,
          is_user_confirmed,
          explanation
        )
        VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'Exact Spotify artist ID backfill')
        """,
        (source_artist_id, artist_id),
    )
    return artist_id


def _ensure_history_text_artist_mapping_with_connection(
    connection: sqlite3.Connection,
    *,
    artist_name_raw: str | None,
) -> int | None:
    artist_label = artist_name_raw.strip() if artist_name_raw and artist_name_raw.strip() else None
    if artist_label is None:
        return None

    external_id = _stable_text_key("history_raw_artist", artist_label)
    existing = connection.execute(
        """
        SELECT
          sa.id AS source_artist_id,
          sam.artist_id AS artist_id
        FROM source_artist sa
        LEFT JOIN source_artist_map sam
          ON sam.source_artist_id = sa.id
        WHERE sa.source_name = 'history_raw'
          AND sa.external_id = ?
        ORDER BY sam.id ASC, sa.id ASC
        LIMIT 1
        """,
        (external_id,),
    ).fetchone()
    if existing is not None and existing["artist_id"] is not None:
        return int(existing["artist_id"])

    if existing is None:
        cursor = connection.execute(
            """
            INSERT INTO source_artist (
              source_name,
              external_id,
              external_uri,
              source_name_raw,
              raw_payload_json
            )
            VALUES (?, ?, NULL, ?, NULL)
            """,
            ("history_raw", external_id, artist_label),
        )
        source_artist_id = int(cursor.lastrowid)
    else:
        source_artist_id = int(existing["source_artist_id"])

    cursor = connection.execute(
        """
        INSERT INTO artist (
          canonical_name,
          sort_name
        )
        VALUES (?, ?)
        """,
        (artist_label, _normalize_name(artist_label)),
    )
    artist_id = int(cursor.lastrowid)
    connection.execute(
        """
        INSERT OR IGNORE INTO source_artist_map (
          source_artist_id,
          artist_id,
          match_method,
          confidence,
          status,
          is_user_confirmed,
          explanation
        )
        VALUES (?, ?, 'history_raw_text', 0.6, 'accepted', 0, 'Backfilled from raw artist_name_raw')
        """,
        (source_artist_id, artist_id),
    )
    return artist_id


def _ensure_source_album_mapping_with_connection(
    connection: sqlite3.Connection,
    *,
    external_id: str,
    external_uri: str | None,
    album_name: str | None,
    raw_payload_json: str | None,
) -> int:
    existing = connection.execute(
        """
        SELECT
          sa.id AS source_album_id,
          sam.release_album_id AS release_album_id
        FROM source_album sa
        LEFT JOIN source_album_map sam
          ON sam.source_album_id = sa.id
        WHERE sa.source_name = 'spotify'
          AND sa.external_id = ?
        ORDER BY sam.id ASC, sa.id ASC
        LIMIT 1
        """,
        (external_id,),
    ).fetchone()
    if existing is not None and existing["release_album_id"] is not None:
        return int(existing["release_album_id"])

    source_album_id: int
    if existing is None:
        cursor = connection.execute(
            """
            INSERT INTO source_album (
              source_name,
              external_id,
              external_uri,
              source_name_raw,
              raw_payload_json
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            ("spotify", external_id, external_uri, album_name, raw_payload_json),
        )
        source_album_id = int(cursor.lastrowid)
    else:
        source_album_id = int(existing["source_album_id"])
        connection.execute(
            """
            UPDATE source_album
            SET
              external_uri = COALESCE(external_uri, ?),
              source_name_raw = COALESCE(source_name_raw, ?),
              raw_payload_json = COALESCE(raw_payload_json, ?),
              updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id = ?
            """,
            (external_uri, album_name, raw_payload_json, source_album_id),
        )

    album_title = album_name.strip() if album_name and album_name.strip() else "Unknown album"
    normalized_name = _normalize_name(album_title)
    cursor = connection.execute(
        """
        INSERT INTO release_album (
          primary_name,
          normalized_name
        )
        VALUES (?, ?)
        """,
        (album_title, normalized_name),
    )
    release_album_id = int(cursor.lastrowid)

    connection.execute(
        """
        INSERT OR IGNORE INTO source_album_map (
          source_album_id,
          release_album_id,
          match_method,
          confidence,
          status,
          is_user_confirmed,
          explanation
        )
        VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'Exact Spotify album ID backfill')
        """,
        (source_album_id, release_album_id),
    )
    return release_album_id


def _ensure_history_text_album_mapping_with_connection(
    connection: sqlite3.Connection,
    *,
    album_name_raw: str | None,
    artist_name_raw: str | None,
) -> int | None:
    album_title = album_name_raw.strip() if album_name_raw and album_name_raw.strip() else None
    if album_title is None:
        return None

    external_id = _stable_text_key("history_raw_album", album_title, artist_name_raw)
    existing = connection.execute(
        """
        SELECT
          sa.id AS source_album_id,
          sam.release_album_id AS release_album_id
        FROM source_album sa
        LEFT JOIN source_album_map sam
          ON sam.source_album_id = sa.id
        WHERE sa.source_name = 'history_raw'
          AND sa.external_id = ?
        ORDER BY sam.id ASC, sa.id ASC
        LIMIT 1
        """,
        (external_id,),
    ).fetchone()
    if existing is not None and existing["release_album_id"] is not None:
        return int(existing["release_album_id"])

    if existing is None:
        cursor = connection.execute(
            """
            INSERT INTO source_album (
              source_name,
              external_id,
              external_uri,
              source_name_raw,
              raw_payload_json
            )
            VALUES (?, ?, NULL, ?, NULL)
            """,
            ("history_raw", external_id, album_title),
        )
        source_album_id = int(cursor.lastrowid)
    else:
        source_album_id = int(existing["source_album_id"])

    cursor = connection.execute(
        """
        INSERT INTO release_album (
          primary_name,
          normalized_name
        )
        VALUES (?, ?)
        """,
        (album_title, _normalize_name(album_title)),
    )
    release_album_id = int(cursor.lastrowid)
    connection.execute(
        """
        INSERT OR IGNORE INTO source_album_map (
          source_album_id,
          release_album_id,
          match_method,
          confidence,
          status,
          is_user_confirmed,
          explanation
        )
        VALUES (?, ?, 'history_raw_text', 0.7, 'accepted', 0, 'Backfilled from raw album_name_raw + artist_name_raw')
        """,
        (source_album_id, release_album_id),
    )
    return release_album_id


def _ensure_source_track_mapping_with_connection(
    connection: sqlite3.Connection,
    *,
    source_name: str,
    external_id: str,
    external_uri: str | None,
    isrc: str | None = None,
    track_name: str | None,
    track_duration_ms: int | None,
    raw_payload_json: str | None,
    create_match_method: str,
    create_confidence: float,
    create_explanation: str,
) -> int:
    existing_release_track_id = _find_release_track_mapping_with_connection(
        connection,
        source_name=source_name,
        external_id=external_id,
    )
    if existing_release_track_id is not None:
        return existing_release_track_id

    source_track_id = _ensure_source_track_with_connection(
        connection,
        source_name=source_name,
        external_id=external_id,
        external_uri=external_uri,
        isrc=isrc,
        track_name=track_name,
        raw_payload_json=raw_payload_json,
    )

    equivalent_release_track_id, equivalent_match_method, equivalent_explanation = (
        _find_equivalent_release_track_for_source_track_with_connection(
            connection,
            source_name=source_name,
            external_id=external_id,
            external_uri=external_uri,
        )
    )

    if equivalent_release_track_id is not None:
        _upsert_source_track_map_with_connection(
            connection,
            source_track_id=source_track_id,
            release_track_id=equivalent_release_track_id,
            match_method=equivalent_match_method or create_match_method,
            confidence=1.0,
            status="accepted",
            explanation=equivalent_explanation or create_explanation,
        )
        return equivalent_release_track_id

    release_track_id = _create_release_track_with_connection(
        connection,
        track_name=track_name,
        track_duration_ms=track_duration_ms,
    )
    _upsert_source_track_map_with_connection(
        connection,
        source_track_id=source_track_id,
        release_track_id=release_track_id,
        match_method=create_match_method,
        confidence=create_confidence,
        status="accepted",
        explanation=create_explanation,
    )
    return release_track_id


def _find_release_track_mapping_with_connection(
    connection: sqlite3.Connection,
    *,
    source_name: str,
    external_id: str,
) -> int | None:
    row = connection.execute(
        """
        SELECT stm.release_track_id
        FROM source_track st
        JOIN source_track_map stm
          ON stm.source_track_id = st.id
        WHERE st.source_name = ?
          AND st.external_id = ?
          AND stm.status = 'accepted'
        ORDER BY
          stm.is_user_confirmed DESC,
          stm.confidence DESC,
          stm.id ASC
        LIMIT 1
        """,
        (source_name, external_id),
    ).fetchone()
    if row is None:
        return None
    return int(row[0])


def _ensure_source_track_with_connection(
    connection: sqlite3.Connection,
    *,
    source_name: str,
    external_id: str,
    external_uri: str | None,
    isrc: str | None,
    track_name: str | None,
    raw_payload_json: str | None,
) -> int:
    existing = connection.execute(
        """
        SELECT id
        FROM source_track
        WHERE source_name = ?
          AND external_id = ?
        LIMIT 1
        """,
        (source_name, external_id),
    ).fetchone()
    if existing is None:
        cursor = connection.execute(
            """
            INSERT INTO source_track (
              source_name,
              external_id,
              external_uri,
              isrc,
              source_name_raw,
              raw_payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (source_name, external_id, external_uri, isrc, track_name, raw_payload_json),
        )
        return int(cursor.lastrowid)

    source_track_id = int(existing[0])
    connection.execute(
        """
        UPDATE source_track
        SET
          external_uri = COALESCE(external_uri, ?),
          isrc = COALESCE(isrc, ?),
          source_name_raw = COALESCE(source_name_raw, ?),
          raw_payload_json = COALESCE(raw_payload_json, ?),
          updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        WHERE id = ?
        """,
        (external_uri, isrc, track_name, raw_payload_json, source_track_id),
    )
    return source_track_id


def _find_equivalent_release_track_for_source_track_with_connection(
    connection: sqlite3.Connection,
    *,
    source_name: str,
    external_id: str,
    external_uri: str | None,
) -> tuple[int | None, str | None, str | None]:
    if source_name == "spotify":
        equivalent_uri = external_uri or f"spotify:track:{external_id}"
        release_track_id = _find_release_track_mapping_with_connection(
            connection,
            source_name="spotify_uri",
            external_id=equivalent_uri,
        )
        if release_track_id is not None:
            return (
                release_track_id,
                "spotify_id_uri_equivalent",
                "Matched Spotify track ID to an existing spotify:track URI representation",
            )
        return None, None, None

    if source_name == "spotify_uri":
        spotify_track_id = _parse_spotify_track_id_from_uri(external_id)
        if spotify_track_id is None and external_uri is not None:
            spotify_track_id = _parse_spotify_track_id_from_uri(external_uri)
        if spotify_track_id is None:
            return None, None, None
        release_track_id = _find_release_track_mapping_with_connection(
            connection,
            source_name="spotify",
            external_id=spotify_track_id,
        )
        if release_track_id is not None:
            return (
                release_track_id,
                "spotify_id_uri_equivalent",
                "Matched spotify:track URI to an existing Spotify track ID representation",
            )
    return None, None, None


def _create_release_track_with_connection(
    connection: sqlite3.Connection,
    *,
    track_name: str | None,
    track_duration_ms: int | None,
) -> int:
    release_title = track_name.strip() if track_name and track_name.strip() else "Unknown track"
    normalized_name = _normalize_name(release_title)
    cursor = connection.execute(
        """
        INSERT INTO release_track (
          primary_name,
          normalized_name,
          duration_ms
        )
        VALUES (?, ?, ?)
        """,
        (release_title, normalized_name, track_duration_ms),
    )
    return int(cursor.lastrowid)


def _upsert_source_track_map_with_connection(
    connection: sqlite3.Connection,
    *,
    source_track_id: int,
    release_track_id: int,
    match_method: str,
    confidence: float,
    status: str,
    explanation: str,
) -> None:
    connection.execute(
        """
        INSERT INTO source_track_map (
          source_track_id,
          release_track_id,
          match_method,
          confidence,
          status,
          is_user_confirmed,
          explanation
        )
        VALUES (?, ?, ?, ?, ?, 0, ?)
        ON CONFLICT(source_track_id, release_track_id) DO UPDATE SET
          match_method = CASE
            WHEN source_track_map.is_user_confirmed = 1 THEN source_track_map.match_method
            WHEN excluded.confidence > source_track_map.confidence THEN excluded.match_method
            ELSE source_track_map.match_method
          END,
          confidence = CASE
            WHEN source_track_map.is_user_confirmed = 1 THEN source_track_map.confidence
            WHEN excluded.confidence > source_track_map.confidence THEN excluded.confidence
            ELSE source_track_map.confidence
          END,
          status = CASE
            WHEN source_track_map.is_user_confirmed = 1 THEN source_track_map.status
            WHEN excluded.confidence > source_track_map.confidence THEN excluded.status
            ELSE source_track_map.status
          END,
          explanation = CASE
            WHEN source_track_map.explanation IS NULL OR trim(source_track_map.explanation) = '' THEN excluded.explanation
            WHEN excluded.confidence > source_track_map.confidence THEN excluded.explanation
            ELSE source_track_map.explanation
          END,
          updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        """,
        (source_track_id, release_track_id, match_method, confidence, status, explanation),
    )


def _find_analysis_track_id_by_grouping_note_with_connection(
    connection: sqlite3.Connection,
    *,
    grouping_note: str,
) -> int | None:
    row = connection.execute(
        """
        SELECT id
        FROM analysis_track
        WHERE grouping_note = ?
        LIMIT 1
        """,
        (grouping_note,),
    ).fetchone()
    if row is None:
        return None
    return int(row[0])


def _ensure_analysis_track_with_connection(
    connection: sqlite3.Connection,
    *,
    primary_name: str,
    grouping_note: str,
) -> int:
    existing_analysis_track_id = _find_analysis_track_id_by_grouping_note_with_connection(
        connection,
        grouping_note=grouping_note,
    )
    if existing_analysis_track_id is not None:
        return existing_analysis_track_id

    cursor = connection.execute(
        """
        INSERT INTO analysis_track (
          primary_name,
          grouping_note
        )
        VALUES (?, ?)
        """,
        (primary_name, grouping_note),
    )
    return int(cursor.lastrowid)


def _upsert_analysis_track_map_with_connection(
    connection: sqlite3.Connection,
    *,
    release_track_id: int,
    analysis_track_id: int,
    match_method: str,
    confidence: float,
    status: str,
    explanation: str,
) -> None:
    connection.execute(
        """
        INSERT INTO analysis_track_map (
          release_track_id,
          analysis_track_id,
          match_method,
          confidence,
          status,
          is_user_confirmed,
          explanation
        )
        VALUES (?, ?, ?, ?, ?, 0, ?)
        ON CONFLICT(release_track_id, analysis_track_id) DO UPDATE SET
          match_method = CASE
            WHEN analysis_track_map.is_user_confirmed = 1 THEN analysis_track_map.match_method
            WHEN excluded.confidence > analysis_track_map.confidence THEN excluded.match_method
            ELSE analysis_track_map.match_method
          END,
          confidence = CASE
            WHEN analysis_track_map.is_user_confirmed = 1 THEN analysis_track_map.confidence
            WHEN excluded.confidence > analysis_track_map.confidence THEN excluded.confidence
            ELSE analysis_track_map.confidence
          END,
          status = CASE
            WHEN analysis_track_map.is_user_confirmed = 1 THEN analysis_track_map.status
            WHEN excluded.confidence > analysis_track_map.confidence THEN excluded.status
            ELSE analysis_track_map.status
          END,
          explanation = CASE
            WHEN analysis_track_map.explanation IS NULL OR trim(analysis_track_map.explanation) = '' THEN excluded.explanation
            WHEN excluded.confidence > analysis_track_map.confidence THEN excluded.explanation
            ELSE analysis_track_map.explanation
          END,
          updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        """,
        (release_track_id, analysis_track_id, match_method, confidence, status, explanation),
    )


def _resolve_release_track_id_for_local_backfill_with_connection(
    connection: sqlite3.Connection,
    *,
    spotify_track_id: str | None,
    spotify_track_uri: str | None,
    track_name_raw: str | None,
    artist_name_raw: str | None,
    album_name_raw: str | None,
    track_duration_ms: int | None,
) -> int | None:
    if spotify_track_id:
        release_track_id = _find_release_track_mapping_with_connection(
            connection,
            source_name="spotify",
            external_id=spotify_track_id,
        )
        if release_track_id is not None:
            return release_track_id

    if spotify_track_uri and spotify_track_uri.strip():
        external_id = spotify_track_uri.strip()
        return _ensure_source_track_mapping_with_connection(
            connection,
            source_name="spotify_uri",
            external_id=external_id,
            external_uri=external_id,
            track_name=track_name_raw,
            track_duration_ms=track_duration_ms,
            raw_payload_json=None,
            create_match_method="spotify_track_uri",
            create_confidence=1.0,
            create_explanation="Backfilled from raw spotify_track_uri",
        )

    track_title = track_name_raw.strip() if track_name_raw and track_name_raw.strip() else None
    if track_title is None:
        return None

    external_id = _stable_text_key("history_raw_track", track_title, artist_name_raw, album_name_raw)
    return _ensure_source_track_mapping_with_connection(
        connection,
        source_name="history_raw",
        external_id=external_id,
        external_uri=None,
        track_name=track_title,
        track_duration_ms=track_duration_ms,
        raw_payload_json=None,
        create_match_method="history_raw_text",
        create_confidence=0.75,
        create_explanation="Backfilled from raw track/artist/album text",
    )


def backfill_spotify_source_entities() -> dict[str, int]:
    counts = {
        "rows_scanned": 0,
        "artists_created": 0,
        "source_artists_created": 0,
        "artist_maps_created": 0,
        "release_albums_created": 0,
        "source_albums_created": 0,
        "album_maps_created": 0,
        "release_tracks_created": 0,
        "source_tracks_created": 0,
        "track_maps_created": 0,
        "album_artist_links_created": 0,
        "track_artist_links_created": 0,
        "album_track_links_created": 0,
    }

    with sqlite_connection(write=True, row_factory=sqlite3.Row) as connection:
        before = {
            "artists": int(connection.execute("SELECT count(*) FROM artist").fetchone()[0]),
            "source_artists": int(connection.execute("SELECT count(*) FROM source_artist").fetchone()[0]),
            "artist_maps": int(connection.execute("SELECT count(*) FROM source_artist_map").fetchone()[0]),
            "release_albums": int(connection.execute("SELECT count(*) FROM release_album").fetchone()[0]),
            "source_albums": int(connection.execute("SELECT count(*) FROM source_album").fetchone()[0]),
            "album_maps": int(connection.execute("SELECT count(*) FROM source_album_map").fetchone()[0]),
            "release_tracks": int(connection.execute("SELECT count(*) FROM release_track").fetchone()[0]),
            "source_tracks": int(connection.execute("SELECT count(*) FROM source_track").fetchone()[0]),
            "track_maps": int(connection.execute("SELECT count(*) FROM source_track_map").fetchone()[0]),
            "album_artist": int(connection.execute("SELECT count(*) FROM album_artist").fetchone()[0]),
            "track_artist": int(connection.execute("SELECT count(*) FROM track_artist").fetchone()[0]),
            "album_track": int(connection.execute("SELECT count(*) FROM album_track").fetchone()[0]),
        }
        rows = connection.execute(
            """
            SELECT
              spotify_track_id,
              spotify_track_uri,
              track_name_raw,
              track_duration_ms,
              artist_name_raw,
              album_name_raw,
              spotify_album_id,
              spotify_artist_ids_json,
              raw_payload_json
            FROM raw_play_event
            WHERE spotify_track_id IS NOT NULL
               OR spotify_album_id IS NOT NULL
               OR spotify_artist_ids_json IS NOT NULL
            ORDER BY id ASC
            """
        ).fetchall()

        for row in rows:
            counts["rows_scanned"] += 1

            release_album_id: int | None = None
            if row["spotify_album_id"]:
                release_album_id = _ensure_source_album_mapping_with_connection(
                    connection,
                    external_id=str(row["spotify_album_id"]),
                    external_uri=f"spotify:album:{row['spotify_album_id']}",
                    album_name=row["album_name_raw"],
                    raw_payload_json=row["raw_payload_json"],
                )

            release_track_id: int | None = None
            if row["spotify_track_id"]:
                release_track_id = _ensure_source_track_mapping_with_connection(
                    connection,
                    source_name="spotify",
                    external_id=str(row["spotify_track_id"]),
                    external_uri=row["spotify_track_uri"] or f"spotify:track:{row['spotify_track_id']}",
                    track_name=row["track_name_raw"],
                    track_duration_ms=row["track_duration_ms"],
                    raw_payload_json=row["raw_payload_json"],
                    create_match_method="provider_identity",
                    create_confidence=1.0,
                    create_explanation="Exact Spotify track ID backfill",
                )

            artist_ids: list[int] = []
            for artist_ref in _extract_spotify_artist_refs(
                spotify_artist_ids_json=row["spotify_artist_ids_json"],
                artist_name_raw=row["artist_name_raw"],
                raw_payload_json=row["raw_payload_json"],
            ):
                external_id = artist_ref.get("external_id")
                if not external_id:
                    continue
                artist_id = _ensure_source_artist_mapping_with_connection(
                    connection,
                    external_id=external_id,
                    external_uri=artist_ref.get("external_uri") or f"spotify:artist:{external_id}",
                    artist_name=artist_ref.get("name"),
                    raw_payload_json=row["raw_payload_json"],
                )
                if artist_id not in artist_ids:
                    artist_ids.append(artist_id)

            for billing_index, artist_id in enumerate(artist_ids):
                if release_track_id is not None:
                    connection.execute(
                        """
                        INSERT INTO track_artist (
                          release_track_id,
                          artist_id,
                          role,
                          billing_index,
                          credited_as,
                          match_method,
                          confidence,
                          source_basis
                        )
                        VALUES (?, ?, 'primary', ?, ?, 'provider_identity', 1.0, 'spotify_structured_artist_ids')
                        ON CONFLICT(release_track_id, artist_id, role) DO UPDATE SET
                          billing_index = COALESCE(track_artist.billing_index, excluded.billing_index),
                          credited_as = COALESCE(track_artist.credited_as, excluded.credited_as),
                          match_method = CASE
                            WHEN track_artist.match_method = 'backfill' THEN excluded.match_method
                            ELSE track_artist.match_method
                          END,
                          confidence = CASE
                            WHEN track_artist.match_method = 'backfill' THEN excluded.confidence
                            ELSE track_artist.confidence
                          END,
                          source_basis = COALESCE(track_artist.source_basis, excluded.source_basis)
                        """,
                        (release_track_id, artist_id, billing_index, artist_ref.get("name") or row["artist_name_raw"]),
                    )
                if release_album_id is not None:
                    connection.execute(
                        """
                        INSERT INTO album_artist (
                          release_album_id,
                          artist_id,
                          role,
                          billing_index,
                          credited_as,
                          match_method,
                          confidence,
                          source_basis
                        )
                        VALUES (?, ?, 'primary', ?, ?, 'provider_identity', 1.0, 'spotify_structured_artist_ids')
                        ON CONFLICT(release_album_id, artist_id, role) DO UPDATE SET
                          billing_index = COALESCE(album_artist.billing_index, excluded.billing_index),
                          credited_as = COALESCE(album_artist.credited_as, excluded.credited_as),
                          match_method = CASE
                            WHEN album_artist.match_method = 'backfill' THEN excluded.match_method
                            ELSE album_artist.match_method
                          END,
                          confidence = CASE
                            WHEN album_artist.match_method = 'backfill' THEN excluded.confidence
                            ELSE album_artist.confidence
                          END,
                          source_basis = COALESCE(album_artist.source_basis, excluded.source_basis)
                        """,
                        (release_album_id, artist_id, billing_index, artist_ref.get("name") or row["artist_name_raw"]),
                    )

            if release_album_id is not None and release_track_id is not None:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO album_track (
                      release_album_id,
                      release_track_id
                    )
                    VALUES (?, ?)
                    """,
                    (release_album_id, release_track_id),
                )

        after = {
            "artists": int(connection.execute("SELECT count(*) FROM artist").fetchone()[0]),
            "source_artists": int(connection.execute("SELECT count(*) FROM source_artist").fetchone()[0]),
            "artist_maps": int(connection.execute("SELECT count(*) FROM source_artist_map").fetchone()[0]),
            "release_albums": int(connection.execute("SELECT count(*) FROM release_album").fetchone()[0]),
            "source_albums": int(connection.execute("SELECT count(*) FROM source_album").fetchone()[0]),
            "album_maps": int(connection.execute("SELECT count(*) FROM source_album_map").fetchone()[0]),
            "release_tracks": int(connection.execute("SELECT count(*) FROM release_track").fetchone()[0]),
            "source_tracks": int(connection.execute("SELECT count(*) FROM source_track").fetchone()[0]),
            "track_maps": int(connection.execute("SELECT count(*) FROM source_track_map").fetchone()[0]),
            "album_artist": int(connection.execute("SELECT count(*) FROM album_artist").fetchone()[0]),
            "track_artist": int(connection.execute("SELECT count(*) FROM track_artist").fetchone()[0]),
            "album_track": int(connection.execute("SELECT count(*) FROM album_track").fetchone()[0]),
        }

    counts["artists_created"] = after["artists"] - before["artists"]
    counts["source_artists_created"] = after["source_artists"] - before["source_artists"]
    counts["artist_maps_created"] = after["artist_maps"] - before["artist_maps"]
    counts["release_albums_created"] = after["release_albums"] - before["release_albums"]
    counts["source_albums_created"] = after["source_albums"] - before["source_albums"]
    counts["album_maps_created"] = after["album_maps"] - before["album_maps"]
    counts["release_tracks_created"] = after["release_tracks"] - before["release_tracks"]
    counts["source_tracks_created"] = after["source_tracks"] - before["source_tracks"]
    counts["track_maps_created"] = after["track_maps"] - before["track_maps"]
    counts["album_artist_links_created"] = after["album_artist"] - before["album_artist"]
    counts["track_artist_links_created"] = after["track_artist"] - before["track_artist"]
    counts["album_track_links_created"] = after["album_track"] - before["album_track"]

    return counts


def backfill_local_text_entities() -> dict[str, int]:
    counts = {
        "rows_scanned": 0,
        "artists_created": 0,
        "source_artists_created": 0,
        "artist_maps_created": 0,
        "release_albums_created": 0,
        "source_albums_created": 0,
        "album_maps_created": 0,
        "release_tracks_created": 0,
        "source_tracks_created": 0,
        "track_maps_created": 0,
        "album_artist_links_created": 0,
        "track_artist_links_created": 0,
        "album_track_links_created": 0,
    }

    with sqlite_connection(write=True, row_factory=sqlite3.Row) as connection:
        before = {
            "artists": int(connection.execute("SELECT count(*) FROM artist").fetchone()[0]),
            "source_artists": int(connection.execute("SELECT count(*) FROM source_artist").fetchone()[0]),
            "artist_maps": int(connection.execute("SELECT count(*) FROM source_artist_map").fetchone()[0]),
            "release_albums": int(connection.execute("SELECT count(*) FROM release_album").fetchone()[0]),
            "source_albums": int(connection.execute("SELECT count(*) FROM source_album").fetchone()[0]),
            "album_maps": int(connection.execute("SELECT count(*) FROM source_album_map").fetchone()[0]),
            "release_tracks": int(connection.execute("SELECT count(*) FROM release_track").fetchone()[0]),
            "source_tracks": int(connection.execute("SELECT count(*) FROM source_track").fetchone()[0]),
            "track_maps": int(connection.execute("SELECT count(*) FROM source_track_map").fetchone()[0]),
            "album_artist": int(connection.execute("SELECT count(*) FROM album_artist").fetchone()[0]),
            "track_artist": int(connection.execute("SELECT count(*) FROM track_artist").fetchone()[0]),
            "album_track": int(connection.execute("SELECT count(*) FROM album_track").fetchone()[0]),
        }

        rows = connection.execute(
            """
            SELECT
              spotify_track_id,
              spotify_track_uri,
              track_name_raw,
              track_duration_ms,
              artist_name_raw,
              album_name_raw,
              spotify_album_id
            FROM raw_play_event
            WHERE
              (track_name_raw IS NOT NULL AND trim(track_name_raw) != '')
              OR (artist_name_raw IS NOT NULL AND trim(artist_name_raw) != '')
              OR (album_name_raw IS NOT NULL AND trim(album_name_raw) != '')
              OR (spotify_track_uri IS NOT NULL AND trim(spotify_track_uri) != '')
            ORDER BY id ASC
            """
        ).fetchall()

        for row in rows:
            counts["rows_scanned"] += 1
            release_track_id = _resolve_release_track_id_for_local_backfill_with_connection(
                connection,
                spotify_track_id=row["spotify_track_id"],
                spotify_track_uri=row["spotify_track_uri"],
                track_name_raw=row["track_name_raw"],
                artist_name_raw=row["artist_name_raw"],
                album_name_raw=row["album_name_raw"],
                track_duration_ms=row["track_duration_ms"],
            )

            release_album_id: int | None = None
            if row["spotify_album_id"]:
                release_album_row = connection.execute(
                    """
                    SELECT sam.release_album_id
                    FROM source_album sa
                    JOIN source_album_map sam
                      ON sam.source_album_id = sa.id
                    WHERE sa.source_name = 'spotify'
                      AND sa.external_id = ?
                    ORDER BY sam.id ASC
                    LIMIT 1
                    """,
                    (str(row["spotify_album_id"]),),
                ).fetchone()
                if release_album_row is not None:
                    release_album_id = int(release_album_row[0])
            if release_album_id is None:
                release_album_id = _ensure_history_text_album_mapping_with_connection(
                    connection,
                    album_name_raw=row["album_name_raw"],
                    artist_name_raw=row["artist_name_raw"],
                )

            if row["artist_name_raw"] and row["artist_name_raw"].strip():
                artist_id = _ensure_history_text_artist_mapping_with_connection(
                    connection,
                    artist_name_raw=row["artist_name_raw"],
                )
                if artist_id is not None:
                    if release_track_id is not None:
                        connection.execute(
                            """
                            INSERT INTO track_artist (
                              release_track_id,
                              artist_id,
                              role,
                              billing_index,
                              credited_as,
                              match_method,
                              confidence,
                              source_basis
                            )
                            VALUES (?, ?, 'primary', 0, ?, 'history_raw_text', 0.6, 'artist_name_raw')
                            ON CONFLICT(release_track_id, artist_id, role) DO UPDATE SET
                              billing_index = COALESCE(track_artist.billing_index, excluded.billing_index),
                              credited_as = COALESCE(track_artist.credited_as, excluded.credited_as),
                              match_method = CASE
                                WHEN track_artist.match_method = 'backfill' THEN excluded.match_method
                                ELSE track_artist.match_method
                              END,
                              confidence = CASE
                                WHEN track_artist.match_method = 'backfill' THEN excluded.confidence
                                ELSE track_artist.confidence
                              END,
                              source_basis = COALESCE(track_artist.source_basis, excluded.source_basis)
                            """,
                            (release_track_id, artist_id, row["artist_name_raw"]),
                        )
                    if release_album_id is not None:
                        connection.execute(
                            """
                            INSERT INTO album_artist (
                              release_album_id,
                              artist_id,
                              role,
                              billing_index,
                              credited_as,
                              match_method,
                              confidence,
                              source_basis
                            )
                            VALUES (?, ?, 'primary', 0, ?, 'history_raw_text', 0.6, 'artist_name_raw')
                            ON CONFLICT(release_album_id, artist_id, role) DO UPDATE SET
                              billing_index = COALESCE(album_artist.billing_index, excluded.billing_index),
                              credited_as = COALESCE(album_artist.credited_as, excluded.credited_as),
                              match_method = CASE
                                WHEN album_artist.match_method = 'backfill' THEN excluded.match_method
                                ELSE album_artist.match_method
                              END,
                              confidence = CASE
                                WHEN album_artist.match_method = 'backfill' THEN excluded.confidence
                                ELSE album_artist.confidence
                              END,
                              source_basis = COALESCE(album_artist.source_basis, excluded.source_basis)
                            """,
                            (release_album_id, artist_id, row["artist_name_raw"]),
                        )

            if release_album_id is not None and release_track_id is not None:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO album_track (
                      release_album_id,
                      release_track_id
                    )
                    VALUES (?, ?)
                    """,
                    (release_album_id, release_track_id),
                )

        after = {
            "artists": int(connection.execute("SELECT count(*) FROM artist").fetchone()[0]),
            "source_artists": int(connection.execute("SELECT count(*) FROM source_artist").fetchone()[0]),
            "artist_maps": int(connection.execute("SELECT count(*) FROM source_artist_map").fetchone()[0]),
            "release_albums": int(connection.execute("SELECT count(*) FROM release_album").fetchone()[0]),
            "source_albums": int(connection.execute("SELECT count(*) FROM source_album").fetchone()[0]),
            "album_maps": int(connection.execute("SELECT count(*) FROM source_album_map").fetchone()[0]),
            "release_tracks": int(connection.execute("SELECT count(*) FROM release_track").fetchone()[0]),
            "source_tracks": int(connection.execute("SELECT count(*) FROM source_track").fetchone()[0]),
            "track_maps": int(connection.execute("SELECT count(*) FROM source_track_map").fetchone()[0]),
            "album_artist": int(connection.execute("SELECT count(*) FROM album_artist").fetchone()[0]),
            "track_artist": int(connection.execute("SELECT count(*) FROM track_artist").fetchone()[0]),
            "album_track": int(connection.execute("SELECT count(*) FROM album_track").fetchone()[0]),
        }

    counts["artists_created"] = after["artists"] - before["artists"]
    counts["source_artists_created"] = after["source_artists"] - before["source_artists"]
    counts["artist_maps_created"] = after["artist_maps"] - before["artist_maps"]
    counts["release_albums_created"] = after["release_albums"] - before["release_albums"]
    counts["source_albums_created"] = after["source_albums"] - before["source_albums"]
    counts["album_maps_created"] = after["album_maps"] - before["album_maps"]
    counts["release_tracks_created"] = after["release_tracks"] - before["release_tracks"]
    counts["source_tracks_created"] = after["source_tracks"] - before["source_tracks"]
    counts["track_maps_created"] = after["track_maps"] - before["track_maps"]
    counts["album_artist_links_created"] = after["album_artist"] - before["album_artist"]
    counts["track_artist_links_created"] = after["track_artist"] - before["track_artist"]
    counts["album_track_links_created"] = after["album_track"] - before["album_track"]

    return counts


def suggest_conservative_analysis_track_links() -> dict[str, int]:
    counts = {
        "groups_considered": 0,
        "groups_suggested": 0,
        "analysis_tracks_created": 0,
        "analysis_track_maps_created": 0,
    }

    with sqlite_connection(write=True, row_factory=sqlite3.Row) as connection:
        before_analysis_tracks = int(connection.execute("SELECT count(*) FROM analysis_track").fetchone()[0])
        before_analysis_track_maps = int(connection.execute("SELECT count(*) FROM analysis_track_map").fetchone()[0])

        rows = connection.execute(
            """
            WITH primary_artists AS (
              SELECT
                ordered.release_track_id,
                group_concat(ordered.artist_name, ' | ') AS artist_signature
              FROM (
                SELECT
                  ta.release_track_id,
                  a.canonical_name AS artist_name
                FROM track_artist ta
                JOIN artist a
                  ON a.id = ta.artist_id
                WHERE ta.role = 'primary'
                ORDER BY
                  ta.release_track_id ASC,
                  COALESCE(ta.billing_index, 999999) ASC,
                  ta.id ASC,
                  a.canonical_name ASC
              ) ordered
              GROUP BY ordered.release_track_id
            )
            SELECT
              rt.id AS release_track_id,
              rt.primary_name,
              rt.normalized_name,
              coalesce(ral.album_names, '') AS album_names,
              pa.artist_signature
            FROM release_track rt
            JOIN primary_artists pa
              ON pa.release_track_id = rt.id
            LEFT JOIN (
              SELECT
                at.release_track_id,
                group_concat(ra.primary_name, ' | ') AS album_names
              FROM album_track at
              JOIN release_album ra
                ON ra.id = at.release_album_id
              GROUP BY at.release_track_id
            ) ral
              ON ral.release_track_id = rt.id
            WHERE rt.normalized_name IS NOT NULL
            ORDER BY
              rt.normalized_name ASC,
              pa.artist_signature ASC,
              rt.id ASC
            """
        ).fetchall()

        grouped: dict[tuple[str, str], list[sqlite3.Row]] = {}
        for row in rows:
            analysis_title_key = _analysis_grouping_base_title(row["primary_name"])
            if analysis_title_key is None:
                continue
            key = (analysis_title_key, str(row["artist_signature"]))
            grouped.setdefault(key, []).append(row)

        for (analysis_title_key, artist_signature), group_rows in grouped.items():
            if len(group_rows) < 2:
                continue
            counts["groups_considered"] += 1

            grouping_note = (
                CONSERVATIVE_ANALYSIS_TRACK_GROUPING_NOTE_PREFIX
                + _stable_text_key(analysis_title_key, artist_signature)
                + "|"
                + analysis_title_key
            )
            analysis_track_id = _ensure_analysis_track_with_connection(
                connection,
                primary_name=str(group_rows[0]["primary_name"]),
                grouping_note=grouping_note,
            )
            counts["groups_suggested"] += 1
            confidence = _analysis_group_confidence(group_rows)

            explanation = (
                "Suggested from the same song-family title key and identical ordered primary artist set; "
                "groupable labels such as live, remaster, acoustic, demo, radio edit, explicit, clean, and generic "
                "packaging labels like album/single/extended version or edit were normalized together, while "
                "remix/rework/cover-marked titles and attributed edit/version labels were excluded"
            )
            for row in group_rows:
                _upsert_analysis_track_map_with_connection(
                    connection,
                    release_track_id=int(row["release_track_id"]),
                    analysis_track_id=analysis_track_id,
                    match_method=SONG_FAMILY_ANALYSIS_MATCH_METHOD,
                    confidence=confidence,
                    status="suggested",
                    explanation=explanation,
                )

        after_analysis_tracks = int(connection.execute("SELECT count(*) FROM analysis_track").fetchone()[0])
        after_analysis_track_maps = int(connection.execute("SELECT count(*) FROM analysis_track_map").fetchone()[0])

    counts["analysis_tracks_created"] = after_analysis_tracks - before_analysis_tracks
    counts["analysis_track_maps_created"] = after_analysis_track_maps - before_analysis_track_maps
    return counts


def refresh_conservative_analysis_track_links() -> dict[str, int]:
    counts = {
        "suggested_maps_deleted": 0,
        "analysis_tracks_deleted": 0,
        "groups_considered": 0,
        "groups_suggested": 0,
        "analysis_tracks_created": 0,
        "analysis_track_maps_created": 0,
    }

    with sqlite_connection(write=True) as connection:
        suggested_maps_deleted = connection.execute(
            """
            DELETE FROM analysis_track_map
            WHERE status = 'suggested'
              AND analysis_track_id IN (
                SELECT id
                FROM analysis_track
                WHERE grouping_note LIKE ?
              )
            """,
            (f"{CONSERVATIVE_ANALYSIS_TRACK_GROUPING_NOTE_PREFIX}%",),
        ).rowcount
        analysis_tracks_deleted = connection.execute(
            """
            DELETE FROM analysis_track
            WHERE grouping_note LIKE ?
            """,
            (f"{CONSERVATIVE_ANALYSIS_TRACK_GROUPING_NOTE_PREFIX}%",),
        ).rowcount

    counts["suggested_maps_deleted"] = int(suggested_maps_deleted)
    counts["analysis_tracks_deleted"] = int(analysis_tracks_deleted)

    suggestion_counts = suggest_conservative_analysis_track_links()
    counts["groups_considered"] = suggestion_counts["groups_considered"]
    counts["groups_suggested"] = suggestion_counts["groups_suggested"]
    counts["analysis_tracks_created"] = suggestion_counts["analysis_tracks_created"]
    counts["analysis_track_maps_created"] = suggestion_counts["analysis_track_maps_created"]
    return counts


def refresh_conservative_track_relationships() -> dict[str, int]:
    counts = {
        "groups_considered": 0,
        "relationships_deleted": 0,
        "relationships_created": 0,
    }

    with sqlite_connection(write=True, row_factory=sqlite3.Row) as connection:
        relationships_deleted = connection.execute(
            """
            DELETE FROM track_relationship
            WHERE relationship_type = 'same_composition'
              AND match_method = 'analysis_track_group'
              AND is_user_confirmed = 0
            """
        ).rowcount
        counts["relationships_deleted"] = int(relationships_deleted)

        grouped_rows = connection.execute(
            """
            SELECT
              analysis_track_id,
              release_track_id,
              confidence,
              status
            FROM analysis_track_map
            ORDER BY analysis_track_id ASC, release_track_id ASC
            """
        ).fetchall()

        groups: dict[int, list[sqlite3.Row]] = {}
        for row in grouped_rows:
            groups.setdefault(int(row["analysis_track_id"]), []).append(row)

        now_iso = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        for analysis_track_id, rows in groups.items():
            if len(rows) < 2:
                continue
            counts["groups_considered"] += 1

            release_rows = sorted(rows, key=lambda r: int(r["release_track_id"]))
            for i in range(len(release_rows)):
                for j in range(i + 1, len(release_rows)):
                    left = release_rows[i]
                    right = release_rows[j]
                    from_release_track_id = int(left["release_track_id"])
                    to_release_track_id = int(right["release_track_id"])
                    confidence = min(float(left["confidence"]), float(right["confidence"]))
                    status = (
                        "accepted"
                        if str(left["status"]) == "accepted" and str(right["status"]) == "accepted"
                        else "suggested"
                    )
                    explanation = (
                        "Suggested from shared analysis_track grouping; "
                        "release tracks mapped to the same conservative analysis composition cluster"
                    )

                    cursor = connection.execute(
                        """
                        INSERT OR IGNORE INTO track_relationship (
                          from_release_track_id,
                          to_release_track_id,
                          relationship_type,
                          match_method,
                          confidence,
                          status,
                          is_user_confirmed,
                          explanation,
                          created_at,
                          updated_at
                        )
                        VALUES (?, ?, 'same_composition', 'analysis_track_group', ?, ?, 0, ?, ?, ?)
                        """,
                        (
                            from_release_track_id,
                            to_release_track_id,
                            confidence,
                            status,
                            explanation,
                            now_iso,
                            now_iso,
                        ),
                    )
                    if int(cursor.rowcount or 0) > 0:
                        counts["relationships_created"] += 1

    return counts


def merge_conservative_same_album_release_track_duplicates() -> dict[str, int]:
    counts = {
        "groups_considered": 0,
        "groups_merged": 0,
        "release_tracks_deleted": 0,
        "source_track_maps_repointed": 0,
        "analysis_track_maps_repointed": 0,
        "merge_logs_created": 0,
    }

    with sqlite_connection(write=True, row_factory=sqlite3.Row) as connection:
        before_merge_logs = int(connection.execute("SELECT count(*) FROM release_track_merge_log").fetchone()[0])

        rows = connection.execute(
            """
            WITH single_album_tracks AS (
              SELECT
                at.release_track_id,
                min(at.release_album_id) AS release_album_id
              FROM album_track at
              GROUP BY at.release_track_id
              HAVING count(DISTINCT at.release_album_id) = 1
            )
            SELECT
              rt.id AS release_track_id,
              rt.primary_name,
              rt.normalized_name,
              rt.duration_ms,
              sat.release_album_id
            FROM release_track rt
            JOIN single_album_tracks sat
              ON sat.release_track_id = rt.id
            WHERE rt.normalized_name IS NOT NULL
            ORDER BY
              sat.release_album_id ASC,
              rt.normalized_name ASC,
              rt.id ASC
            """
        ).fetchall()

        grouped: dict[tuple[int, str], list[sqlite3.Row]] = {}
        for row in rows:
            key = (
                int(row["release_album_id"]),
                str(row["normalized_name"]),
            )
            grouped.setdefault(key, []).append(row)

        for (_, _), group_rows in grouped.items():
            if len(group_rows) < 2:
                continue
            counts["groups_considered"] += 1

            non_null_durations = [
                int(row["duration_ms"])
                for row in group_rows
                if row["duration_ms"] is not None
            ]
            if len(non_null_durations) >= 2 and (max(non_null_durations) - min(non_null_durations)) > 2000:
                continue

            winner_row = min(group_rows, key=lambda row: int(row["release_track_id"]))
            winner_release_track_id = int(winner_row["release_track_id"])
            release_album_id = int(winner_row["release_album_id"])
            any_group_merged = False

            for loser_row in group_rows:
                loser_release_track_id = int(loser_row["release_track_id"])
                if loser_release_track_id == winner_release_track_id:
                    continue

                existing_merge = connection.execute(
                    """
                    SELECT 1
                    FROM release_track_merge_log
                    WHERE obsolete_release_track_id = ?
                      AND canonical_release_track_id = ?
                    LIMIT 1
                    """,
                    (loser_release_track_id, winner_release_track_id),
                ).fetchone()
                if existing_merge is not None:
                    continue

                explanation = (
                    "Merged duplicate same-album release_track inferred from identical normalized title "
                    "and identical single album context"
                )

                connection.execute(
                    """
                    INSERT INTO release_track_merge_log (
                      obsolete_release_track_id,
                      canonical_release_track_id,
                      release_album_id,
                      obsolete_primary_name,
                      canonical_primary_name,
                      match_method,
                      confidence,
                      status,
                      explanation
                    )
                    VALUES (?, ?, ?, ?, ?, 'same_album_exact_title_primary_artist', 0.95, 'accepted', ?)
                    """,
                    (
                        loser_release_track_id,
                        winner_release_track_id,
                        release_album_id,
                        loser_row["primary_name"],
                        winner_row["primary_name"],
                        explanation,
                    ),
                )

                repointed_source_rows = connection.execute(
                    """
                    UPDATE source_track_map
                    SET
                      release_track_id = ?,
                      match_method = 'same_album_exact_title_primary_artist',
                      confidence = 0.95,
                      status = 'accepted',
                      explanation = ?,
                      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                    WHERE release_track_id = ?
                    """,
                    (winner_release_track_id, explanation, loser_release_track_id),
                ).rowcount
                counts["source_track_maps_repointed"] += int(repointed_source_rows)

                connection.execute(
                    """
                    INSERT OR IGNORE INTO track_artist (
                      release_track_id,
                      artist_id,
                      role,
                      billing_index,
                      created_at,
                      credited_as,
                      match_method,
                      confidence,
                      source_basis
                    )
                    SELECT
                      ?,
                      artist_id,
                      role,
                      billing_index,
                      created_at,
                      credited_as,
                      match_method,
                      confidence,
                      source_basis
                    FROM track_artist
                    WHERE release_track_id = ?
                    """,
                    (winner_release_track_id, loser_release_track_id),
                )

                connection.execute(
                    """
                    INSERT OR IGNORE INTO album_track (
                      release_album_id,
                      release_track_id,
                      created_at
                    )
                    SELECT
                      release_album_id,
                      ?,
                      created_at
                    FROM album_track
                    WHERE release_track_id = ?
                    """,
                    (winner_release_track_id, loser_release_track_id),
                )

                analysis_track_rows = connection.execute(
                    """
                    SELECT
                      analysis_track_id,
                      match_method,
                      confidence,
                      status,
                      explanation
                    FROM analysis_track_map
                    WHERE release_track_id = ?
                    """,
                    (loser_release_track_id,),
                ).fetchall()
                for analysis_row in analysis_track_rows:
                    _upsert_analysis_track_map_with_connection(
                        connection,
                        release_track_id=winner_release_track_id,
                        analysis_track_id=int(analysis_row["analysis_track_id"]),
                        match_method=str(analysis_row["match_method"]),
                        confidence=float(analysis_row["confidence"]),
                        status=str(analysis_row["status"]),
                        explanation=str(analysis_row["explanation"] or ""),
                    )
                counts["analysis_track_maps_repointed"] += len(analysis_track_rows)

                connection.execute(
                    """
                    UPDATE release_track
                    SET
                      duration_ms = COALESCE(duration_ms, ?),
                      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                    WHERE id = ?
                    """,
                    (loser_row["duration_ms"], winner_release_track_id),
                )

                connection.execute(
                    "DELETE FROM analysis_track_map WHERE release_track_id = ?",
                    (loser_release_track_id,),
                )
                connection.execute(
                    "DELETE FROM track_artist WHERE release_track_id = ?",
                    (loser_release_track_id,),
                )
                connection.execute(
                    "DELETE FROM album_track WHERE release_track_id = ?",
                    (loser_release_track_id,),
                )
                connection.execute(
                    """
                    DELETE FROM track_relationship
                    WHERE from_release_track_id = ?
                       OR to_release_track_id = ?
                    """,
                    (loser_release_track_id, loser_release_track_id),
                )
                connection.execute(
                    "DELETE FROM release_track WHERE id = ?",
                    (loser_release_track_id,),
                )

                counts["release_tracks_deleted"] += 1
                any_group_merged = True

            if any_group_merged:
                counts["groups_merged"] += 1

        after_merge_logs = int(connection.execute("SELECT count(*) FROM release_track_merge_log").fetchone()[0])

    counts["merge_logs_created"] = after_merge_logs - before_merge_logs
    return counts


def apply_pending_migrations() -> None:
    current_version = get_schema_version()
    pending_versions = sorted(version for version in MIGRATIONS if version > current_version)

    for version in pending_versions:
        execute_sql(MIGRATIONS[version])
        set_schema_version(version)
