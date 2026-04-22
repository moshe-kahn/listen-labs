from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from time import perf_counter
from typing import Any
from uuid import uuid4

from backend.app.db import (
    complete_ingest_run_and_patch_spotify_sync_state,
    get_spotify_sync_state,
    insert_ingest_run,
    insert_or_upgrade_raw_play_event,
    insert_raw_spotify_recent_observation,
    patch_ingest_run_heartbeat,
    patch_ingest_run_timing_phases,
    patch_spotify_sync_state,
)
from backend.app.play_event_projector import reconcile_fact_play_events_for_ingest_run

logger = logging.getLogger("listenlabs.sync")
file_logger = logging.getLogger("listenlabs.sync.file")
HEARTBEAT_INTERVAL_SECONDS = 15.0
MIN_RECENT_OVERLAP_LOOKBACK_SECONDS = 24 * 60 * 60


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


def _estimate_recent_fallback_metadata(row: dict[str, Any]) -> tuple[str, str | None]:
    method = str(row.get("ms_played_method") or "")
    if method == "api_chronology":
        return "high", None

    track_id = str(row.get("spotify_track_id") or "")
    if track_id:
        return "low", "fallback_likely_complete"
    return "low", "fallback_likely_complete"


def _annotate_recent_fallback_sequences(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    chronological = sorted(
        rows,
        key=lambda row: (
            datetime.fromisoformat(str(row["played_at"]).replace("Z", "+00:00")),
            str(row["source_row_key"]),
        ),
    )
    previous_played_at: datetime | None = None
    for row in chronological:
        confidence, fallback_class = _estimate_recent_fallback_metadata(row)
        row["ms_played_confidence"] = confidence
        row["ms_played_fallback_class"] = fallback_class

        track_duration_ms = row.get("track_duration_ms")
        played_at = datetime.fromisoformat(str(row["played_at"]).replace("Z", "+00:00"))
        if (
            str(row.get("ms_played_method")) != "api_chronology"
            and track_duration_ms is not None
            and previous_played_at is not None
            and int(track_duration_ms) > 0
            and int((played_at - previous_played_at).total_seconds() * 1000) > (2 * int(track_duration_ms))
        ):
            # Assumption: rows with a very large prior gap relative to track
            # duration are more likely short transition/skip-like plays.
            row["ms_played_fallback_class"] = "fallback_short_transition"
        previous_played_at = played_at
    return rows


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
    configured_overlap_lookback_seconds = int(state["overlap_lookback_seconds"])
    overlap_lookback_seconds = max(
        configured_overlap_lookback_seconds,
        MIN_RECENT_OVERLAP_LOOKBACK_SECONDS,
    )
    if overlap_lookback_seconds != configured_overlap_lookback_seconds:
        # Keep the persisted setting in sync so diagnostics and future runs reflect
        # the effective overlap window that protects against delayed API visibility.
        patch_spotify_sync_state(overlap_lookback_seconds=overlap_lookback_seconds)
        file_logger.info(
            "event=spotify_recent_overlap_adjusted previous_seconds=%s effective_seconds=%s",
            configured_overlap_lookback_seconds,
            overlap_lookback_seconds,
        )

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
    total_started = perf_counter()
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
    row_outcomes: list[dict[str, Any]] = []

    canonical_projection_summary: dict[str, Any] | None = None
    raw_inserts_elapsed_ms = 0.0
    final_commit_elapsed_ms = 0.0
    matcher_elapsed_ms = 0.0
    projector_elapsed_ms = 0.0
    last_heartbeat_touch = perf_counter()
    try:
        _annotate_recent_fallback_sequences(rows)
        for row in rows:
            row_count += 1
            source_row_key = str(row["source_row_key"])
            played_at = str(row["played_at"])

            raw_insert_started = perf_counter()
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
            insert_raw_spotify_recent_observation(
                ingest_run_id=run_id,
                source_row_key=source_row_key,
                source_event_id=row.get("source_event_id"),
                played_at=played_at,
                ms_played_estimate=int(row["ms_played"]),
                ms_played_method=str(row["ms_played_method"]),
                ms_played_confidence=str(row.get("ms_played_confidence") or "low"),
                ms_played_fallback_class=row.get("ms_played_fallback_class"),
                spotify_track_uri=row.get("spotify_track_uri"),
                spotify_track_id=row.get("spotify_track_id"),
                track_name_raw=row.get("track_name_raw"),
                artist_name_raw=row.get("artist_name_raw"),
                album_name_raw=row.get("album_name_raw"),
                spotify_album_id=row.get("spotify_album_id"),
                spotify_artist_ids_json=row.get("spotify_artist_ids_json"),
                track_duration_ms=row.get("track_duration_ms"),
                context_type=row.get("context_type"),
                context_uri=row.get("context_uri"),
                raw_payload_json=str(row["raw_payload_json"]),
            )
            raw_inserts_elapsed_ms += (perf_counter() - raw_insert_started) * 1000

            action = str(row_result["action"])
            raw_play_event_id = int(row_result["row_id"])
            match_type = str(row_result.get("match_type") or "")

            if action == "unchanged":
                duplicate_count += 1
                if match_type == "source_row_key":
                    already_seen_source_row_count += 1
                row_outcomes.append(
                    {
                        "source_row_key": source_row_key,
                        "played_at": played_at,
                        "outcome": "duplicate",
                        "match_type": match_type,
                    }
                )
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
                row_outcomes.append(
                    {
                        "source_row_key": source_row_key,
                        "played_at": played_at,
                        "outcome": "duplicate",
                        "match_type": match_type,
                    }
                )
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
                row_outcomes.append(
                    {
                        "source_row_key": source_row_key,
                        "played_at": played_at,
                        "outcome": "inserted",
                        "match_type": match_type,
                    }
                )
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

            current_tick = perf_counter()
            if (current_tick - last_heartbeat_touch) >= HEARTBEAT_INTERVAL_SECONDS:
                patch_ingest_run_heartbeat(
                    run_id=run_id,
                    heartbeat_at=_utc_now_iso(),
                )
                last_heartbeat_touch = current_tick

        completed_at = _utc_now_iso()
        complete_started = perf_counter()
        complete_spotify_recent_sync_run(
            run_id=run_id,
            completed_at=completed_at,
            last_successful_played_at=max_played_at,
            row_count=row_count,
            inserted_count=inserted_count,
            duplicate_count=duplicate_count,
            error_count=0,
        )
        final_commit_elapsed_ms = (perf_counter() - complete_started) * 1000
        projector_started = perf_counter()
        canonical_projection_summary = reconcile_fact_play_events_for_ingest_run(
            source_type="spotify_recent",
            run_id=run_id,
        )
        projector_elapsed_ms = (perf_counter() - projector_started) * 1000
        matcher_elapsed_ms = float(canonical_projection_summary.get("matcher_ms", 0.0))
        projector_elapsed_ms = float(canonical_projection_summary.get("projector_ms", projector_elapsed_ms))

        patch_ingest_run_timing_phases(
            run_id=run_id,
            timing_phases_ms={
                "raw_inserts_ms": raw_inserts_elapsed_ms,
                "matcher_ms": matcher_elapsed_ms,
                "projector_ms": projector_elapsed_ms,
                "final_commit_ms": final_commit_elapsed_ms,
                "total_duration_ms": (perf_counter() - total_started) * 1000,
            },
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
        "row_outcomes": row_outcomes,
        "canonical_projection_summary": canonical_projection_summary,
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
