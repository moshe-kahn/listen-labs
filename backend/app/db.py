from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

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
}


def get_sqlite_db_path() -> Path:
    settings = get_settings()
    return Path(settings.sqlite_db_path)


def ensure_sqlite_db() -> Path:
    db_path = get_sqlite_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as connection:
        connection.execute(SCHEMA_VERSION_SQL)
        connection.execute(INITIALIZE_SCHEMA_VERSION_SQL)

    return db_path


def get_schema_version() -> int:
    with sqlite3.connect(get_sqlite_db_path()) as connection:
        row = connection.execute("SELECT version FROM schema_version LIMIT 1").fetchone()
    return int(row[0])


def set_schema_version(version: int) -> None:
    with sqlite3.connect(get_sqlite_db_path()) as connection:
        connection.execute("UPDATE schema_version SET version = ?", (version,))


def execute_sql(sql: str) -> None:
    with sqlite3.connect(get_sqlite_db_path()) as connection:
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
    return row_id


def _ms_played_method_rank(method: str | None) -> int:
    ranks = {
        "default_guess": 1,
        "api_chronology": 2,
        "history_source": 3,
    }
    return ranks.get(str(method), 0)


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
    existing = connection.execute(
        """
        SELECT id, ms_played_method
        FROM raw_play_event
        WHERE source_row_key = ?
        LIMIT 1
        """,
        (source_row_key,),
    ).fetchone()

    if existing is None and cross_source_event_key is not None:
        existing = connection.execute(
            """
            SELECT id, ms_played_method
            FROM raw_play_event
            WHERE cross_source_event_key = ?
            LIMIT 1
            """,
            (cross_source_event_key,),
        ).fetchone()

    if existing is None:
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
        return {"row_id": int(cursor.lastrowid), "action": "inserted"}

    existing_method = existing["ms_played_method"]
    if _ms_played_method_rank(ms_played_method) > _ms_played_method_rank(existing_method):
        connection.execute(
            """
            UPDATE raw_play_event
            SET
              ms_played = ?,
              ms_played_method = ?,
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
                int(existing["id"]),
            ),
        )
        return {"row_id": int(existing["id"]), "action": "upgraded"}

    return {"row_id": int(existing["id"]), "action": "unchanged"}


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
            "SELECT * FROM raw_play_event WHERE source_row_key = ? LIMIT 1",
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


def raw_play_event_exists(
    *,
    source_row_key: str,
    cross_source_event_key: str | None = None,
) -> bool:
    with sqlite3.connect(get_sqlite_db_path()) as connection:
        row = connection.execute(
            """
            SELECT 1
            FROM raw_play_event
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
    with sqlite3.connect(get_sqlite_db_path()) as connection:
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
    with sqlite3.connect(get_sqlite_db_path()) as connection:
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
    with sqlite3.connect(get_sqlite_db_path()) as connection:
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
    with sqlite3.connect(get_sqlite_db_path()) as connection:
        connection.row_factory = sqlite3.Row
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
    with sqlite3.connect(get_sqlite_db_path()) as connection:
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


def apply_pending_migrations() -> None:
    current_version = get_schema_version()
    pending_versions = sorted(version for version in MIGRATIONS if version > current_version)

    for version in pending_versions:
        execute_sql(MIGRATIONS[version])
        set_schema_version(version)
