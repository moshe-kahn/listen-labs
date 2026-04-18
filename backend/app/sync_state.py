from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from backend.app.db import (
    complete_ingest_run_and_patch_spotify_sync_state,
    get_spotify_sync_state,
    insert_ingest_run,
    insert_or_upgrade_raw_play_event,
    patch_spotify_sync_state,
)

logger = logging.getLogger("listenlabs.sync")
file_logger = logging.getLogger("listenlabs.sync.file")


def _max_iso_utc_timestamp(a: str | None, b: str | None) -> str | None:
    if a is None:
        return b
    if b is None:
        return a

    a_dt = datetime.fromisoformat(a.replace("Z", "+00:00"))
    b_dt = datetime.fromisoformat(b.replace("Z", "+00:00"))
    return a if a_dt >= b_dt else b


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _normalize_skipped(value: Any) -> int | None:
    if value in (None, ""):
        return None
    if value in (0, False):
        return 0
    return 1


def _log_sync_started(
    *,
    run_id: str,
    replay_from: str | None,
    source_ref: str | None,
) -> None:
    logger.info("Spotify recent sync started")
    file_logger.debug(
        "event=spotify_recent_sync_started run_id=%s replay_from=%s source_ref=%s",
        run_id,
        replay_from,
        source_ref,
    )


def _log_sync_completed(
    *,
    run_id: str,
    row_count: int,
    inserted_count: int,
    duplicate_count: int,
    watermark: str | None,
) -> None:
    logger.info(
        "Spotify recent sync completed: %s rows, %s new, %s duplicates",
        row_count,
        inserted_count,
        duplicate_count,
    )
    file_logger.debug(
        "event=spotify_recent_sync_completed run_id=%s row_count=%s inserted_count=%s duplicate_count=%s watermark=%s",
        run_id,
        row_count,
        inserted_count,
        duplicate_count,
        watermark,
    )


def _log_sync_failed(
    *,
    run_id: str,
    row_count: int,
    inserted_count: int,
    duplicate_count: int,
    source_ref: str | None,
) -> None:
    logger.info("Spotify recent sync failed")
    file_logger.exception(
        "event=spotify_recent_sync_failed run_id=%s row_count=%s inserted_count=%s duplicate_count=%s source_ref=%s",
        run_id,
        row_count,
        inserted_count,
        duplicate_count,
        source_ref,
    )


def get_spotify_recent_sync_start_point() -> dict[str, Any]:
    state = get_spotify_sync_state()

    last_successful_played_at = state["last_successful_played_at"]
    overlap_lookback_seconds = int(state["overlap_lookback_seconds"])

    if last_successful_played_at is None:
        return {
            "last_successful_played_at": None,
            "overlap_lookback_seconds": overlap_lookback_seconds,
            "fetch_after_played_at": None,
        }

    last_dt = datetime.fromisoformat(last_successful_played_at.replace("Z", "+00:00"))
    fetch_after_dt = last_dt - timedelta(seconds=overlap_lookback_seconds)

    return {
        "last_successful_played_at": last_successful_played_at,
        "overlap_lookback_seconds": overlap_lookback_seconds,
        "fetch_after_played_at": fetch_after_dt.astimezone(UTC).isoformat().replace("+00:00", "Z"),
    }


def start_spotify_recent_sync_run(
    *,
    source_ref: str | None = None,
) -> dict[str, Any]:
    start_point = get_spotify_recent_sync_start_point()
    run_id = str(uuid4())
    started_at = _utc_now_iso()

    insert_ingest_run(
        run_id=run_id,
        source_type="spotify_recent",
        started_at=started_at,
        source_ref=source_ref,
    )

    patch_spotify_sync_state(
        last_started_at=started_at,
        last_run_id=run_id,
    )

    _log_sync_started(
        run_id=run_id,
        replay_from=start_point["fetch_after_played_at"],
        source_ref=source_ref,
    )

    return {
        "run_id": run_id,
        "started_at": started_at,
        "last_successful_played_at": start_point["last_successful_played_at"],
        "overlap_lookback_seconds": start_point["overlap_lookback_seconds"],
        "fetch_after_played_at": start_point["fetch_after_played_at"],
    }


def ingest_spotify_recent_rows(
    *,
    rows: list[dict[str, Any]],
    source_ref: str | None = None,
) -> dict[str, Any]:
    start_info = start_spotify_recent_sync_run(source_ref=source_ref)
    run_id = str(start_info["run_id"])
    started_at = str(start_info["started_at"])
    fetch_after_played_at = start_info["fetch_after_played_at"]

    row_count = 0
    inserted_count = 0
    duplicate_count = 0
    already_seen_source_row_count = 0
    merged_duplicate_row_count = 0
    max_played_at: str | None = None
    min_played_at: str | None = None

    try:
        for row in rows:
            row_count += 1
            source_row_key = str(row["source_row_key"])
            played_at = str(row["played_at"])

            row_result = insert_or_upgrade_raw_play_event(
                ingest_run_id=run_id,
                source_type="spotify_recent",
                source_event_id=row.get("source_event_id"),
                source_row_key=source_row_key,
                cross_source_event_key=row.get("cross_source_event_key"),
                played_at=played_at,
                ms_played=int(row["ms_played"]),
                ms_played_method=str(row["ms_played_method"]),
                track_duration_ms=row.get("track_duration_ms"),
                reason_start=row.get("reason_start"),
                reason_end=row.get("reason_end"),
                skipped=_normalize_skipped(row.get("skipped")),
                platform=row.get("platform"),
                shuffle=row.get("shuffle"),
                offline=row.get("offline"),
                conn_country=row.get("conn_country"),
                spotify_track_uri=row.get("spotify_track_uri"),
                spotify_track_id=row.get("spotify_track_id"),
                track_name_raw=row.get("track_name_raw"),
                artist_name_raw=row.get("artist_name_raw"),
                album_name_raw=row.get("album_name_raw"),
                spotify_album_id=row.get("spotify_album_id"),
                spotify_artist_ids_json=row.get("spotify_artist_ids_json"),
                raw_payload_json=str(row["raw_payload_json"]),
            )

            action = str(row_result["action"])
            raw_play_event_id = int(row_result["row_id"])
            match_type = str(row_result.get("match_type") or "")

            if action == "unchanged":
                duplicate_count += 1
                if match_type == "source_row_key":
                    already_seen_source_row_count += 1
                file_logger.debug(
                    "event=spotify_recent_row_result run_id=%s row_number=%s source_row_key=%s played_at=%s match_type=%s result=%s",
                    run_id,
                    row_count,
                    source_row_key,
                    played_at,
                    match_type,
                    "duplicate",
                )
            elif action == "merged_duplicate_row":
                merged_duplicate_row_count += 1
                file_logger.debug(
                    "event=spotify_recent_row_result run_id=%s row_number=%s source_row_key=%s played_at=%s match_type=%s result=%s raw_play_event_id=%s",
                    run_id,
                    row_count,
                    source_row_key,
                    played_at,
                    match_type,
                    action,
                    raw_play_event_id,
                )
            else:
                inserted_count += 1
                file_logger.debug(
                    "event=spotify_recent_row_result run_id=%s row_number=%s source_row_key=%s played_at=%s match_type=%s result=%s raw_play_event_id=%s",
                    run_id,
                    row_count,
                    source_row_key,
                    played_at,
                    match_type,
                    action,
                    raw_play_event_id,
                )

            max_played_at = _max_iso_utc_timestamp(max_played_at, played_at)
            if min_played_at is None:
                min_played_at = played_at
            else:
                current_min = datetime.fromisoformat(min_played_at.replace("Z", "+00:00"))
                candidate = datetime.fromisoformat(played_at.replace("Z", "+00:00"))
                if candidate < current_min:
                    min_played_at = played_at

        completed_at = _utc_now_iso()
        complete_spotify_recent_sync_run(
            run_id=run_id,
            completed_at=completed_at,
            last_successful_played_at=max_played_at,
            row_count=row_count,
            inserted_count=inserted_count,
            duplicate_count=duplicate_count,
            error_count=0,
        )
    except Exception:
        _log_sync_failed(
            run_id=run_id,
            row_count=row_count,
            inserted_count=inserted_count,
            duplicate_count=duplicate_count,
            source_ref=source_ref,
        )
        raise

    return {
        "run_id": run_id,
        "started_at": started_at,
        "completed_at": completed_at,
        "fetch_after_played_at": fetch_after_played_at,
        "row_count": row_count,
        "inserted_count": inserted_count,
        "duplicate_count": duplicate_count,
        "already_seen_source_row_count": already_seen_source_row_count,
        "merged_duplicate_row_count": merged_duplicate_row_count,
        "earliest_played_at": min_played_at,
        "latest_played_at": max_played_at,
        "last_successful_played_at": max_played_at,
    }


def complete_spotify_recent_sync_run(
    *,
    run_id: str,
    completed_at: str,
    last_successful_played_at: str | None,
    row_count: int,
    inserted_count: int,
    duplicate_count: int,
    error_count: int = 0,
) -> None:
    current_state = get_spotify_sync_state()
    next_last_successful_played_at = _max_iso_utc_timestamp(
        current_state["last_successful_played_at"],
        last_successful_played_at,
    )

    complete_ingest_run_and_patch_spotify_sync_state(
        run_id=run_id,
        completed_at=completed_at,
        row_count=row_count,
        inserted_count=inserted_count,
        duplicate_count=duplicate_count,
        error_count=error_count,
        last_successful_played_at=next_last_successful_played_at,
    )

    _log_sync_completed(
        run_id=run_id,
        row_count=row_count,
        inserted_count=inserted_count,
        duplicate_count=duplicate_count,
        watermark=next_last_successful_played_at,
    )
