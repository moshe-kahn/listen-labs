from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.app.db import get_sqlite_db_path
from backend.app.play_event_matcher import MatchPair, match_recent_history_rows


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _parse_iso_z(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)


def _iso_z(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _fact_id_for_recent(connection: sqlite3.Connection, recent_id: int) -> int | None:
    row = connection.execute(
        "SELECT fact_play_event_id FROM fact_play_event_recent_link WHERE raw_spotify_recent_id = ?",
        (recent_id,),
    ).fetchone()
    return int(row[0]) if row is not None else None


def _fact_id_for_history(connection: sqlite3.Connection, history_id: int) -> int | None:
    row = connection.execute(
        "SELECT fact_play_event_id FROM fact_play_event_history_link WHERE raw_spotify_history_id = ?",
        (history_id,),
    ).fetchone()
    return int(row[0]) if row is not None else None


def _merge_fact_rows(connection: sqlite3.Connection, *, winner_fact_id: int, loser_fact_id: int) -> None:
    if winner_fact_id == loser_fact_id:
        return
    connection.execute(
        "UPDATE fact_play_event_recent_link SET fact_play_event_id = ? WHERE fact_play_event_id = ?",
        (winner_fact_id, loser_fact_id),
    )
    connection.execute(
        "UPDATE fact_play_event_history_link SET fact_play_event_id = ? WHERE fact_play_event_id = ?",
        (winner_fact_id, loser_fact_id),
    )
    connection.execute("DELETE FROM fact_play_event WHERE id = ?", (loser_fact_id,))


def _upsert_recent_link(
    connection: sqlite3.Connection,
    *,
    fact_play_event_id: int,
    recent_id: int,
    delta_ms: int | None,
    match_tier: str | None,
) -> None:
    connection.execute(
        """
        INSERT OR IGNORE INTO fact_play_event_recent_link (
          fact_play_event_id,
          raw_spotify_recent_id,
          match_delta_ms,
          match_tier,
          is_primary
        )
        VALUES (?, ?, ?, ?, 1)
        """,
        (fact_play_event_id, recent_id, delta_ms, match_tier),
    )
    connection.execute(
        """
        UPDATE fact_play_event_recent_link
        SET
          fact_play_event_id = ?,
          match_delta_ms = ?,
          match_tier = ?,
          is_primary = 1
        WHERE raw_spotify_recent_id = ?
        """,
        (fact_play_event_id, delta_ms, match_tier, recent_id),
    )


def _upsert_history_link(
    connection: sqlite3.Connection,
    *,
    fact_play_event_id: int,
    history_id: int,
    delta_ms: int | None,
    match_tier: str | None,
) -> None:
    connection.execute(
        """
        INSERT OR IGNORE INTO fact_play_event_history_link (
          fact_play_event_id,
          raw_spotify_history_id,
          match_delta_ms,
          match_tier,
          is_primary
        )
        VALUES (?, ?, ?, ?, 1)
        """,
        (fact_play_event_id, history_id, delta_ms, match_tier),
    )
    connection.execute(
        """
        UPDATE fact_play_event_history_link
        SET
          fact_play_event_id = ?,
          match_delta_ms = ?,
          match_tier = ?,
          is_primary = 1
        WHERE raw_spotify_history_id = ?
        """,
        (fact_play_event_id, delta_ms, match_tier, history_id),
    )


def _create_fact_placeholder(connection: sqlite3.Connection, *, canonical_ended_at: str) -> int:
    cursor = connection.execute(
        """
        INSERT INTO fact_play_event (
          canonical_started_at,
          canonical_ended_at,
          canonical_ms_played,
          ms_played_confidence,
          canonical_reason_start,
          canonical_reason_end,
          canonical_skipped,
          canonical_shuffle,
          canonical_offline,
          canonical_private_session,
          canonical_context_type,
          canonical_context_uri,
          spotify_track_id,
          spotify_track_uri,
          spotify_album_id,
          spotify_artist_ids_json,
          track_name_canonical,
          artist_name_canonical,
          album_name_canonical,
          timing_source,
          matched_state
        )
        VALUES (NULL, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'recent_fallback', 'recent_only')
        """,
        (canonical_ended_at,),
    )
    return int(cursor.lastrowid)


def _compute_started_at(ended_at: str | None, ms_played: int | None) -> str | None:
    if ended_at is None or ms_played is None:
        return None
    ended_dt = _parse_iso_z(ended_at)
    started_dt = ended_dt - timedelta(milliseconds=int(ms_played))
    return _iso_z(started_dt)


def _reload_fact_from_sources(connection: sqlite3.Connection, *, fact_id: int) -> None:
    recent_row = connection.execute(
        """
        SELECT r.*
        FROM fact_play_event_recent_link l
        JOIN raw_spotify_recent r
          ON r.id = l.raw_spotify_recent_id
        WHERE l.fact_play_event_id = ?
        ORDER BY l.is_primary DESC, r.id ASC
        LIMIT 1
        """,
        (fact_id,),
    ).fetchone()
    history_row = connection.execute(
        """
        SELECT h.*
        FROM fact_play_event_history_link l
        JOIN raw_spotify_history h
          ON h.id = l.raw_spotify_history_id
        WHERE l.fact_play_event_id = ?
        ORDER BY l.is_primary DESC, h.id ASC
        LIMIT 1
        """,
        (fact_id,),
    ).fetchone()

    if history_row is not None:
        canonical_ended_at = str(history_row["played_at"])
        canonical_ms_played = int(history_row["ms_played"]) if history_row["ms_played"] is not None else None
        timing_source = "history"
        ms_played_confidence = "high"
    elif recent_row is not None:
        canonical_ended_at = str(recent_row["played_at"])
        canonical_ms_played = (
            int(recent_row["ms_played_estimate"]) if recent_row["ms_played_estimate"] is not None else None
        )
        if str(recent_row["ms_played_method"]) == "api_chronology":
            timing_source = "recent_chronology"
        else:
            timing_source = "recent_fallback"
        ms_played_confidence = str(recent_row["ms_played_confidence"])
    else:
        return

    if history_row is not None and recent_row is not None:
        matched_state = "matched"
    elif history_row is not None:
        matched_state = "history_only"
    else:
        matched_state = "recent_only"

    canonical_started_at = _compute_started_at(canonical_ended_at, canonical_ms_played)
    spotify_track_id = (
        str(recent_row["spotify_track_id"])
        if recent_row is not None and recent_row["spotify_track_id"] is not None
        else (str(history_row["spotify_track_id"]) if history_row is not None and history_row["spotify_track_id"] is not None else None)
    )
    spotify_track_uri = (
        str(recent_row["spotify_track_uri"])
        if recent_row is not None and recent_row["spotify_track_uri"] is not None
        else (str(history_row["spotify_track_uri"]) if history_row is not None and history_row["spotify_track_uri"] is not None else None)
    )
    spotify_album_id = (
        str(recent_row["spotify_album_id"])
        if recent_row is not None and recent_row["spotify_album_id"] is not None
        else (str(history_row["spotify_album_id"]) if history_row is not None and history_row["spotify_album_id"] is not None else None)
    )
    spotify_artist_ids_json = (
        str(recent_row["spotify_artist_ids_json"])
        if recent_row is not None and recent_row["spotify_artist_ids_json"] is not None
        else (
            str(history_row["spotify_artist_ids_json"])
            if history_row is not None and history_row["spotify_artist_ids_json"] is not None
            else None
        )
    )
    track_name_canonical = (
        str(recent_row["track_name_raw"])
        if recent_row is not None and recent_row["track_name_raw"] is not None
        else (str(history_row["track_name_raw"]) if history_row is not None and history_row["track_name_raw"] is not None else None)
    )
    artist_name_canonical = (
        str(recent_row["artist_name_raw"])
        if recent_row is not None and recent_row["artist_name_raw"] is not None
        else (str(history_row["artist_name_raw"]) if history_row is not None and history_row["artist_name_raw"] is not None else None)
    )
    album_name_canonical = (
        str(recent_row["album_name_raw"])
        if recent_row is not None and recent_row["album_name_raw"] is not None
        else (str(history_row["album_name_raw"]) if history_row is not None and history_row["album_name_raw"] is not None else None)
    )

    connection.execute(
        """
        UPDATE fact_play_event
        SET
          canonical_started_at = ?,
          canonical_ended_at = ?,
          canonical_ms_played = ?,
          ms_played_confidence = ?,
          canonical_reason_start = ?,
          canonical_reason_end = ?,
          canonical_skipped = ?,
          canonical_shuffle = ?,
          canonical_offline = ?,
          canonical_private_session = ?,
          canonical_context_type = ?,
          canonical_context_uri = ?,
          spotify_track_id = ?,
          spotify_track_uri = ?,
          spotify_album_id = ?,
          spotify_artist_ids_json = ?,
          track_name_canonical = ?,
          artist_name_canonical = ?,
          album_name_canonical = ?,
          timing_source = ?,
          matched_state = ?,
          updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        WHERE id = ?
        """,
        (
            canonical_started_at,
            canonical_ended_at,
            canonical_ms_played,
            ms_played_confidence,
            str(history_row["reason_start"]) if history_row is not None and history_row["reason_start"] is not None else None,
            str(history_row["reason_end"]) if history_row is not None and history_row["reason_end"] is not None else None,
            int(history_row["skipped"]) if history_row is not None and history_row["skipped"] is not None else None,
            int(history_row["shuffle"]) if history_row is not None and history_row["shuffle"] is not None else None,
            int(history_row["offline"]) if history_row is not None and history_row["offline"] is not None else None,
            int(history_row["private_session"]) if history_row is not None and history_row["private_session"] is not None else None,
            str(recent_row["context_type"]) if recent_row is not None and recent_row["context_type"] is not None else None,
            str(recent_row["context_uri"]) if recent_row is not None and recent_row["context_uri"] is not None else None,
            spotify_track_id,
            spotify_track_uri,
            spotify_album_id,
            spotify_artist_ids_json,
            track_name_canonical,
            artist_name_canonical,
            album_name_canonical,
            timing_source,
            matched_state,
            fact_id,
        ),
    )


def _pending_recent_candidates_for_run(connection: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """
        SELECT
          r.*,
          l.fact_play_event_id AS existing_fact_id
        FROM raw_spotify_recent r
        LEFT JOIN fact_play_event_recent_link l
          ON l.raw_spotify_recent_id = r.id
        LEFT JOIN fact_play_event_history_link h
          ON h.fact_play_event_id = l.fact_play_event_id
        WHERE r.ingest_run_id = ?
          AND (l.fact_play_event_id IS NULL OR h.fact_play_event_id IS NULL)
        ORDER BY r.played_at ASC, r.id ASC
        """,
        (run_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def _pending_history_candidates_for_run(connection: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """
        SELECT
          h.*,
          l.fact_play_event_id AS existing_fact_id
        FROM raw_spotify_history h
        LEFT JOIN fact_play_event_history_link l
          ON l.raw_spotify_history_id = h.id
        LEFT JOIN fact_play_event_recent_link r
          ON r.fact_play_event_id = l.fact_play_event_id
        WHERE h.ingest_run_id = ?
          AND (l.fact_play_event_id IS NULL OR r.fact_play_event_id IS NULL)
        ORDER BY h.played_at ASC, h.id ASC
        """,
        (run_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def _recent_counterparts_for_history_rows(
    connection: sqlite3.Connection, *, history_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    if not history_rows:
        return []
    min_played_at = str(history_rows[0]["played_at"])
    max_played_at = str(history_rows[-1]["played_at"])
    min_dt = _parse_iso_z(min_played_at) - timedelta(seconds=35)
    max_dt = _parse_iso_z(max_played_at) + timedelta(seconds=35)
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """
        SELECT
          r.*,
          l.fact_play_event_id AS existing_fact_id
        FROM raw_spotify_recent r
        LEFT JOIN fact_play_event_recent_link l
          ON l.raw_spotify_recent_id = r.id
        LEFT JOIN fact_play_event_history_link h
          ON h.fact_play_event_id = l.fact_play_event_id
        WHERE r.played_at >= ?
          AND r.played_at <= ?
          AND (l.fact_play_event_id IS NULL OR h.fact_play_event_id IS NULL)
        ORDER BY r.played_at ASC, r.id ASC
        """,
        (_iso_z(min_dt), _iso_z(max_dt)),
    ).fetchall()
    return [dict(row) for row in rows]


def _history_counterparts_for_recent_rows(
    connection: sqlite3.Connection, *, recent_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    if not recent_rows:
        return []
    min_played_at = str(recent_rows[0]["played_at"])
    max_played_at = str(recent_rows[-1]["played_at"])
    min_dt = _parse_iso_z(min_played_at) - timedelta(seconds=35)
    max_dt = _parse_iso_z(max_played_at) + timedelta(seconds=35)
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """
        SELECT
          h.*,
          l.fact_play_event_id AS existing_fact_id
        FROM raw_spotify_history h
        LEFT JOIN fact_play_event_history_link l
          ON l.raw_spotify_history_id = h.id
        LEFT JOIN fact_play_event_recent_link r
          ON r.fact_play_event_id = l.fact_play_event_id
        WHERE h.played_at >= ?
          AND h.played_at <= ?
          AND (l.fact_play_event_id IS NULL OR r.fact_play_event_id IS NULL)
        ORDER BY h.played_at ASC, h.id ASC
        """,
        (_iso_z(min_dt), _iso_z(max_dt)),
    ).fetchall()
    return [dict(row) for row in rows]


def _apply_match_pairs(connection: sqlite3.Connection, *, pairs: list[MatchPair]) -> list[int]:
    touched_fact_ids: list[int] = []
    for pair in pairs:
        recent_fact_id = _fact_id_for_recent(connection, pair.recent_id)
        history_fact_id = _fact_id_for_history(connection, pair.history_id)

        if recent_fact_id is None and history_fact_id is None:
            ended_at_row = connection.execute(
                "SELECT played_at FROM raw_spotify_history WHERE id = ?",
                (pair.history_id,),
            ).fetchone()
            canonical_ended_at = str(ended_at_row[0]) if ended_at_row is not None else _utc_now_iso()
            winner_fact_id = _create_fact_placeholder(connection, canonical_ended_at=canonical_ended_at)
        elif recent_fact_id is not None and history_fact_id is None:
            winner_fact_id = recent_fact_id
        elif recent_fact_id is None and history_fact_id is not None:
            winner_fact_id = history_fact_id
        else:
            assert recent_fact_id is not None
            assert history_fact_id is not None
            winner_fact_id = min(recent_fact_id, history_fact_id)
            loser_fact_id = max(recent_fact_id, history_fact_id)
            _merge_fact_rows(connection, winner_fact_id=winner_fact_id, loser_fact_id=loser_fact_id)

        _upsert_recent_link(
            connection,
            fact_play_event_id=winner_fact_id,
            recent_id=pair.recent_id,
            delta_ms=pair.delta_ms,
            match_tier=pair.match_tier,
        )
        _upsert_history_link(
            connection,
            fact_play_event_id=winner_fact_id,
            history_id=pair.history_id,
            delta_ms=pair.delta_ms,
            match_tier=pair.match_tier,
        )
        touched_fact_ids.append(winner_fact_id)
    return touched_fact_ids


def _ensure_fact_for_unmatched_recent(connection: sqlite3.Connection, *, recent_id: int) -> int:
    existing_fact_id = _fact_id_for_recent(connection, recent_id)
    if existing_fact_id is not None:
        return existing_fact_id
    played_at_row = connection.execute(
        "SELECT played_at FROM raw_spotify_recent WHERE id = ?",
        (recent_id,),
    ).fetchone()
    canonical_ended_at = str(played_at_row[0]) if played_at_row is not None else _utc_now_iso()
    fact_id = _create_fact_placeholder(connection, canonical_ended_at=canonical_ended_at)
    _upsert_recent_link(
        connection,
        fact_play_event_id=fact_id,
        recent_id=recent_id,
        delta_ms=None,
        match_tier=None,
    )
    return fact_id


def _ensure_fact_for_unmatched_history(connection: sqlite3.Connection, *, history_id: int) -> int:
    existing_fact_id = _fact_id_for_history(connection, history_id)
    if existing_fact_id is not None:
        return existing_fact_id
    played_at_row = connection.execute(
        "SELECT played_at FROM raw_spotify_history WHERE id = ?",
        (history_id,),
    ).fetchone()
    canonical_ended_at = str(played_at_row[0]) if played_at_row is not None else _utc_now_iso()
    fact_id = _create_fact_placeholder(connection, canonical_ended_at=canonical_ended_at)
    _upsert_history_link(
        connection,
        fact_play_event_id=fact_id,
        history_id=history_id,
        delta_ms=None,
        match_tier=None,
    )
    return fact_id


def reconcile_fact_play_events_for_ingest_run(
    *,
    source_type: str,
    run_id: str,
    tight_seconds: int = 10,
    wide_seconds: int = 30,
) -> dict[str, Any]:
    # Assumption: We reconcile in append-only ingest batches and preserve raw rows forever.
    # We only match rows that still lack opposite-source linkage, keeping the process idempotent.
    started_total = datetime.now(UTC)
    with sqlite3.connect(get_sqlite_db_path(), timeout=30) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")

        candidate_started = datetime.now(UTC)
        if source_type == "spotify_recent":
            recent_candidates = _pending_recent_candidates_for_run(connection, run_id)
            history_candidates = _history_counterparts_for_recent_rows(
                connection, recent_rows=recent_candidates
            )
        elif source_type == "export":
            history_candidates = _pending_history_candidates_for_run(connection, run_id)
            recent_candidates = _recent_counterparts_for_history_rows(
                connection, history_rows=history_candidates
            )
        else:
            return {
                "source_type": source_type,
                "run_id": run_id,
                "tight_10s_count": 0,
                "wide_30s_count": 0,
                "matched_pairs_count": 0,
                "unmatched_recent_count": 0,
                "unmatched_history_count": 0,
                "facts_touched_count": 0,
                "candidate_collect_ms": 0.0,
                "matcher_ms": 0.0,
                "projector_ms": 0.0,
                "commit_ms": 0.0,
            }
        candidate_collect_ms = (datetime.now(UTC) - candidate_started).total_seconds() * 1000

        matcher_started = datetime.now(UTC)
        match_result = match_recent_history_rows(
            recent_rows=recent_candidates,
            history_rows=history_candidates,
            tight_seconds=tight_seconds,
            wide_seconds=wide_seconds,
        )
        matcher_ms = (datetime.now(UTC) - matcher_started).total_seconds() * 1000

        projector_started = datetime.now(UTC)
        touched_fact_ids = _apply_match_pairs(connection, pairs=match_result.pairs)

        if source_type == "spotify_recent":
            run_recent_ids = {
                int(row["id"]) for row in recent_candidates if str(row["ingest_run_id"] or "") == run_id
            }
            unmatched_run_recent = sorted(run_recent_ids & set(match_result.unmatched_recent_ids))
            for recent_id in unmatched_run_recent:
                touched_fact_ids.append(_ensure_fact_for_unmatched_recent(connection, recent_id=recent_id))
        else:
            run_history_ids = {
                int(row["id"]) for row in history_candidates if str(row["ingest_run_id"] or "") == run_id
            }
            unmatched_run_history = sorted(run_history_ids & set(match_result.unmatched_history_ids))
            for history_id in unmatched_run_history:
                touched_fact_ids.append(_ensure_fact_for_unmatched_history(connection, history_id=history_id))

        for fact_id in sorted(set(touched_fact_ids)):
            _reload_fact_from_sources(connection, fact_id=fact_id)
        projector_ms = (datetime.now(UTC) - projector_started).total_seconds() * 1000

        commit_started = datetime.now(UTC)
        connection.commit()
        commit_ms = (datetime.now(UTC) - commit_started).total_seconds() * 1000
        total_ms = (datetime.now(UTC) - started_total).total_seconds() * 1000
        return {
            "source_type": source_type,
            "run_id": run_id,
            "tight_10s_count": match_result.tight_10s_count,
            "wide_30s_count": match_result.wide_30s_count,
            "matched_pairs_count": len(match_result.pairs),
            "unmatched_recent_count": len(match_result.unmatched_recent_ids),
            "unmatched_history_count": len(match_result.unmatched_history_ids),
            "facts_touched_count": len(set(touched_fact_ids)),
            "candidate_collect_ms": candidate_collect_ms,
            "matcher_ms": matcher_ms,
            "projector_ms": projector_ms,
            "commit_ms": commit_ms,
            "total_ms": total_ms,
        }
