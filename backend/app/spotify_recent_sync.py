from __future__ import annotations
import logging
import os
from datetime import UTC, datetime
from typing import Any

from backend.app.db import raw_play_event_exists
from backend.app.spotify_recent_api import fetch_spotify_recent_play_page
from backend.app.spotify_recent_mapper import map_spotify_recent_play_item
from backend.app.sync_state import get_spotify_recent_sync_start_point, ingest_spotify_recent_rows

logger = logging.getLogger("listenlabs.sync")
file_logger = logging.getLogger("listenlabs.sync.file")


def _parse_played_at(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _canonical_played_at(value: str) -> str:
    return _parse_played_at(value).astimezone(UTC).isoformat().replace("+00:00", "Z")


def _apply_api_chronology_estimates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    estimated_rows = [dict(row) for row in rows]
    chronological_rows = sorted(
        estimated_rows,
        key=lambda row: (_parse_played_at(str(row["played_at"])), str(row["source_row_key"])),
    )

    previous_row: dict[str, Any] | None = None
    previous_played_at: datetime | None = None

    for row in chronological_rows:
        track_duration_ms = row.get("track_duration_ms")
        current_played_at = _parse_played_at(str(row["played_at"]))

        if (
            previous_row is not None
            and previous_played_at is not None
            and track_duration_ms is not None
        ):
            gap_ms = int((current_played_at - previous_played_at).total_seconds() * 1000)
            track_duration_value = int(track_duration_ms)
            if 0 < gap_ms <= track_duration_value:
                row["ms_played"] = gap_ms
                row["ms_played_method"] = "api_chronology"

        previous_row = row
        previous_played_at = current_played_at

    return estimated_rows


def _sort_recent_rows_desc(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (_parse_played_at(str(row["played_at"])), str(row["source_row_key"])),
        reverse=True,
    )


def _should_stop_on_known_row(*, row: dict[str, Any], overlap_cutoff: str | None) -> bool:
    if not raw_play_event_exists(
        source_row_key=str(row["source_row_key"]),
        cross_source_event_key=row.get("cross_source_event_key"),
    ):
        return False

    if overlap_cutoff is None:
        return False

    row_played_at = _canonical_played_at(str(row["played_at"]))
    overlap_cutoff_canonical = _canonical_played_at(overlap_cutoff)
    return _parse_played_at(row_played_at) < _parse_played_at(overlap_cutoff_canonical)


def _collect_recent_rows_for_ingest(
    mapped_rows: list[dict[str, Any]],
    *,
    overlap_cutoff: str | None,
) -> tuple[list[dict[str, Any]], bool]:
    kept_rows: list[dict[str, Any]] = []
    should_stop = False

    for row in _sort_recent_rows_desc(mapped_rows):
        if _should_stop_on_known_row(row=row, overlap_cutoff=overlap_cutoff):
            should_stop = True
            break
        kept_rows.append(row)

    return kept_rows, should_stop


def _log_recent_play_sync_failed(
    *,
    phase: str,
    source_ref: str | None,
    fetched_count: int,
) -> None:
    logger.info("Spotify recent-play sync failed")
    file_logger.exception(
        "event=spotify_recent_play_sync_failed phase=%s fetched_count=%s source_ref=%s",
        phase,
        fetched_count,
        source_ref,
    )


async def sync_spotify_recent_plays(
    access_token: str,
    *,
    source_ref: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    fetched_count = 0
    try:
        start_point = get_spotify_recent_sync_start_point()
        before_cursor: str | None = None
        fetch_after_played_at: str | None = start_point["fetch_after_played_at"]
        rows_to_ingest: list[dict[str, Any]] = []

        while True:
            try:
                page = await fetch_spotify_recent_play_page(
                    access_token,
                    after_played_at=fetch_after_played_at,
                    before_cursor=before_cursor,
                    limit=limit,
                )
            except Exception:
                _log_recent_play_sync_failed(
                    phase="fetch",
                    source_ref=source_ref,
                    fetched_count=fetched_count,
                )
                raise

            page_items = page["items"]
            fetched_count += len(page_items)
            if not page_items:
                break

            try:
                mapped_rows = [map_spotify_recent_play_item(item) for item in page_items]
            except Exception:
                _log_recent_play_sync_failed(
                    phase="map",
                    source_ref=source_ref,
                    fetched_count=fetched_count,
                )
                raise
            kept_rows, should_stop = _collect_recent_rows_for_ingest(
                mapped_rows,
                overlap_cutoff=start_point["fetch_after_played_at"],
            )
            rows_to_ingest.extend(kept_rows)

            # Spotify recently-played endpoint rejects requests that include both
            # "after" and "before". Use "after" only for the initial page.
            fetch_after_played_at = None
            before_cursor = page["before_cursor"]
            if should_stop or before_cursor is None:
                break

        try:
            rows = _apply_api_chronology_estimates(rows_to_ingest)
            ingest_summary = ingest_spotify_recent_rows(
                rows=rows,
                source_ref=source_ref or "spotify_recent_api",
            )
        except Exception:
            _log_recent_play_sync_failed(
                phase="ingest",
                source_ref=source_ref,
                fetched_count=fetched_count,
            )
            raise

        return {
            "fetched_count": fetched_count,
            "run_id": ingest_summary["run_id"],
            "row_count": ingest_summary["row_count"],
            "inserted_count": ingest_summary["inserted_count"],
            "duplicate_count": ingest_summary["duplicate_count"],
            "already_seen_source_row_count": ingest_summary.get("already_seen_source_row_count", 0),
            "merged_duplicate_row_count": ingest_summary.get("merged_duplicate_row_count", 0),
            "earliest_played_at": ingest_summary.get("earliest_played_at"),
            "latest_played_at": ingest_summary.get("latest_played_at"),
            "last_successful_played_at": ingest_summary["last_successful_played_at"],
            "fetch_after_played_at": ingest_summary["fetch_after_played_at"],
        }
    except Exception:
        raise


async def manual_sync_spotify_recent_plays(
    *,
    access_token: str | None = None,
    source_ref: str | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    resolved_access_token = access_token or os.getenv("SPOTIFY_ACCESS_TOKEN", "").strip()
    if not resolved_access_token:
        raise RuntimeError("Set SPOTIFY_ACCESS_TOKEN or pass access_token explicitly.")

    return await sync_spotify_recent_plays(
        resolved_access_token,
        source_ref=source_ref or "manual_spotify_recent_sync",
        limit=limit,
    )
