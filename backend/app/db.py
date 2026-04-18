from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator

from backend.app.config import get_settings

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
    with sqlite3.connect(get_sqlite_db_path()) as connection:
        connection.row_factory = sqlite3.Row
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
    with sqlite3.connect(get_sqlite_db_path()) as connection:
        connection.row_factory = sqlite3.Row
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
    with sqlite3.connect(get_sqlite_db_path()) as connection:
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
    with sqlite3.connect(get_sqlite_db_path()) as connection:
        connection.row_factory = sqlite3.Row
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


def list_raw_play_events(limit: int = 50) -> list[dict[str, Any]]:
    with sqlite3.connect(get_sqlite_db_path()) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            """
            SELECT *
            FROM raw_play_event
            ORDER BY played_at DESC
            LIMIT ?
            """,
            (limit,),
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

    with sqlite3.connect(get_sqlite_db_path()) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            """
            WITH normalized AS (
              SELECT
                id,
                played_at,
                CASE
                  WHEN spotify_track_id IS NOT NULL AND spotify_track_id != '' THEN spotify_track_id
                  WHEN spotify_track_uri IS NOT NULL AND spotify_track_uri != '' THEN spotify_track_uri
                  ELSE '__unknown__:' || LOWER(TRIM(COALESCE(track_name_raw, ''))) || ':' || LOWER(TRIM(COALESCE(artist_name_raw, '')))
                END AS track_id,
                track_name_raw,
                artist_name_raw
              FROM raw_play_event
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
    with sqlite3.connect(get_sqlite_db_path()) as connection:
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
          status
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (run_id, source_type, source_ref, started_at, status),
    )
    return run_id


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
          row_count = ?,
          inserted_count = ?,
          duplicate_count = ?,
          error_count = ?,
          status = ?
        WHERE id = ?
        """,
        (
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
              row_count = ?,
              inserted_count = ?,
              duplicate_count = ?,
              error_count = ?,
              status = 'completed'
            WHERE id = ?
            """,
            (
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
              status,
              row_count,
              inserted_count,
              duplicate_count,
              error_count,
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
              status,
              row_count,
              inserted_count,
              duplicate_count,
              error_count,
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


def apply_pending_migrations() -> None:
    current_version = get_schema_version()
    pending_versions = sorted(version for version in MIGRATIONS if version > current_version)

    for version in pending_versions:
        execute_sql(MIGRATIONS[version])
        set_schema_version(version)
