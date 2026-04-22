from __future__ import annotations
import logging
import os
from datetime import UTC, datetime
from typing import Any

from backend.app.config import get_settings
from backend.app.db import raw_play_event_exists
from backend.app.spotify_recent_api import fetch_spotify_recent_play_page
from backend.app.spotify_recent_mapper import map_spotify_recent_play_item
from backend.app.sync_state import get_spotify_recent_sync_start_point, ingest_spotify_recent_rows

logger = logging.getLogger("listenlabs.sync")
file_logger = logging.getLogger("listenlabs.sync.file")
settings = get_settings()


def _parse_played_at(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _canonical_played_at(value: str) -> str:
    return _parse_played_at(value).astimezone(UTC).isoformat().replace("+00:00", "Z")


def _max_played_at(a: str | None, b: str | None) -> str | None:
    if a is None:
        return b
    if b is None:
        return a
    return a if _parse_played_at(a) >= _parse_played_at(b) else b


def _min_played_at(a: str | None, b: str | None) -> str | None:
    if a is None:
        return b
    if b is None:
        return a
    return a if _parse_played_at(a) <= _parse_played_at(b) else b


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
    keep_known_rows: bool,
    enable_overlap_stop: bool,
) -> tuple[list[dict[str, Any]], dict[str, int], list[dict[str, Any]], bool]:
    kept_rows: list[dict[str, Any]] = []
    outcomes = {
        "inserted": 0,
        "duplicate": 0,
        "filtered": 0,
        "skipped": 0,
    }
    item_decisions: list[dict[str, Any]] = []
    should_stop = False

    for row in _sort_recent_rows_desc(mapped_rows):
        source_row_key = str(row["source_row_key"])
        played_at = str(row["played_at"])
        track_name = row.get("track_name_raw")
        artist_name = row.get("artist_name_raw")
        is_known = raw_play_event_exists(
            source_row_key=source_row_key,
            cross_source_event_key=row.get("cross_source_event_key"),
        )
        if is_known:
            if enable_overlap_stop and _should_stop_on_known_row(row=row, overlap_cutoff=overlap_cutoff):
                outcomes["skipped"] += 1
                should_stop = True
                item_decisions.append(
                    {
                        "source_row_key": source_row_key,
                        "played_at": played_at,
                        "track_name_raw": track_name,
                        "artist_name_raw": artist_name,
                        "outcome": "skipped",
                    }
                )
                file_logger.debug(
                    "event=spotify_recent_row_collected outcome=skipped source_row_key=%s played_at=%s overlap_cutoff=%s",
                    source_row_key,
                    played_at,
                    overlap_cutoff,
                )
                break

            if keep_known_rows:
                outcomes["duplicate"] += 1
                item_decisions.append(
                    {
                        "source_row_key": source_row_key,
                        "played_at": played_at,
                        "track_name_raw": track_name,
                        "artist_name_raw": artist_name,
                        "outcome": "duplicate",
                    }
                )
                file_logger.debug(
                    "event=spotify_recent_row_collected outcome=duplicate source_row_key=%s played_at=%s overlap_cutoff=%s",
                    source_row_key,
                    played_at,
                    overlap_cutoff,
                )
                kept_rows.append(row)
            else:
                outcomes["filtered"] += 1
                item_decisions.append(
                    {
                        "source_row_key": source_row_key,
                        "played_at": played_at,
                        "track_name_raw": track_name,
                        "artist_name_raw": artist_name,
                        "outcome": "filtered",
                    }
                )
                file_logger.debug(
                    "event=spotify_recent_row_collected outcome=filtered source_row_key=%s played_at=%s overlap_cutoff=%s",
                    source_row_key,
                    played_at,
                    overlap_cutoff,
                )
            continue

        outcomes["inserted"] += 1
        item_decisions.append(
            {
                "source_row_key": source_row_key,
                "played_at": played_at,
                "track_name_raw": track_name,
                "artist_name_raw": artist_name,
                "outcome": "inserted",
            }
        )
        kept_rows.append(row)
        file_logger.debug(
            "event=spotify_recent_row_collected outcome=inserted source_row_key=%s played_at=%s overlap_cutoff=%s",
            source_row_key,
            played_at,
            overlap_cutoff,
        )
    return kept_rows, outcomes, item_decisions, should_stop


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
    poll_started_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    fetched_count = 0
    try:
        start_point = get_spotify_recent_sync_start_point()
        saved_watermark_before_run = start_point["last_successful_played_at"]
        computed_after = start_point["fetch_after_played_at"]
        logger.info(
            "Spotify recent poll started at %s (saved watermark=%s, computed after=%s)",
            poll_started_at,
            saved_watermark_before_run,
            computed_after,
        )
        file_logger.info(
            "event=spotify_recent_poll_started poll_started_at=%s saved_watermark_before_run=%s computed_after=%s overlap_lookback_seconds=%s source_ref=%s",
            poll_started_at,
            saved_watermark_before_run,
            computed_after,
            start_point["overlap_lookback_seconds"],
            source_ref,
        )
        use_full_page_mode = bool(settings.spotify_recent_full_page_mode)
        rows_to_ingest: list[dict[str, Any]] = []
        api_oldest_played_at: str | None = None
        api_newest_played_at: str | None = None
        collection_outcomes = {
            "inserted": 0,
            "duplicate": 0,
            "filtered": 0,
            "skipped": 0,
        }
        item_decisions: list[dict[str, Any]] = []

        if use_full_page_mode:
            try:
                page = await fetch_spotify_recent_play_page(
                    access_token,
                    after_played_at=None,
                    before_cursor=None,
                    limit=50,
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

            if page_items:
                try:
                    mapped_rows = [map_spotify_recent_play_item(item) for item in page_items]
                except Exception:
                    _log_recent_play_sync_failed(
                        phase="map",
                        source_ref=source_ref,
                        fetched_count=fetched_count,
                    )
                    raise
                page_played_at_values = sorted(str(row["played_at"]) for row in mapped_rows)
                page_oldest_played_at = page_played_at_values[0]
                page_newest_played_at = page_played_at_values[-1]
                api_oldest_played_at = _min_played_at(api_oldest_played_at, page_oldest_played_at)
                api_newest_played_at = _max_played_at(api_newest_played_at, page_newest_played_at)
                logger.info(
                    "Spotify recent page returned %s items (newest=%s, oldest=%s)",
                    len(mapped_rows),
                    page_newest_played_at,
                    page_oldest_played_at,
                )
                file_logger.info(
                    "event=spotify_recent_page_received page_item_count=%s page_newest_played_at=%s page_oldest_played_at=%s before_cursor=%s after_used=%s",
                    len(mapped_rows),
                    page_newest_played_at,
                    page_oldest_played_at,
                    None,
                    None,
                )

                kept_rows, page_outcomes, page_item_decisions, _ = _collect_recent_rows_for_ingest(
                    mapped_rows,
                    overlap_cutoff=start_point["fetch_after_played_at"],
                    keep_known_rows=True,
                    enable_overlap_stop=False,
                )
                rows_to_ingest.extend(kept_rows)
                collection_outcomes["inserted"] += int(page_outcomes["inserted"])
                collection_outcomes["duplicate"] += int(page_outcomes["duplicate"])
                collection_outcomes["filtered"] += int(page_outcomes["filtered"])
                collection_outcomes["skipped"] += int(page_outcomes["skipped"])
                item_decisions.extend(page_item_decisions)
        else:
            before_cursor: str | None = None
            fetch_after_played_at: str | None = start_point["fetch_after_played_at"]
            requested_limit = max(1, min(limit, 50))
            while True:
                try:
                    page = await fetch_spotify_recent_play_page(
                        access_token,
                        after_played_at=fetch_after_played_at,
                        before_cursor=before_cursor,
                        limit=requested_limit,
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
                page_played_at_values = sorted(str(row["played_at"]) for row in mapped_rows)
                page_oldest_played_at = page_played_at_values[0] if page_played_at_values else None
                page_newest_played_at = page_played_at_values[-1] if page_played_at_values else None
                api_oldest_played_at = _min_played_at(api_oldest_played_at, page_oldest_played_at)
                api_newest_played_at = _max_played_at(api_newest_played_at, page_newest_played_at)
                logger.info(
                    "Spotify recent page returned %s items (newest=%s, oldest=%s)",
                    len(mapped_rows),
                    page_newest_played_at,
                    page_oldest_played_at,
                )
                file_logger.info(
                    "event=spotify_recent_page_received page_item_count=%s page_newest_played_at=%s page_oldest_played_at=%s before_cursor=%s after_used=%s",
                    len(mapped_rows),
                    page_newest_played_at,
                    page_oldest_played_at,
                    before_cursor,
                    fetch_after_played_at,
                )

                kept_rows, page_outcomes, page_item_decisions, should_stop = _collect_recent_rows_for_ingest(
                    mapped_rows,
                    overlap_cutoff=start_point["fetch_after_played_at"],
                    keep_known_rows=False,
                    enable_overlap_stop=True,
                )
                rows_to_ingest.extend(kept_rows)
                collection_outcomes["inserted"] += int(page_outcomes["inserted"])
                collection_outcomes["duplicate"] += int(page_outcomes["duplicate"])
                collection_outcomes["filtered"] += int(page_outcomes["filtered"])
                collection_outcomes["skipped"] += int(page_outcomes["skipped"])
                item_decisions.extend(page_item_decisions)

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
            "poll_started_at": poll_started_at,
            "saved_watermark_before_run": saved_watermark_before_run,
            "computed_after": computed_after,
            "api_oldest_played_at": api_oldest_played_at,
            "api_newest_played_at": api_newest_played_at,
            "collection_outcomes": collection_outcomes,
            "item_decisions": item_decisions,
            "run_id": ingest_summary["run_id"],
            "row_count": ingest_summary["row_count"],
            "inserted_count": ingest_summary["inserted_count"],
            "duplicate_count": ingest_summary["duplicate_count"],
            "already_seen_source_row_count": ingest_summary.get("already_seen_source_row_count", 0),
            "merged_duplicate_row_count": ingest_summary.get("merged_duplicate_row_count", 0),
            "row_outcomes": ingest_summary.get("row_outcomes") or [],
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
