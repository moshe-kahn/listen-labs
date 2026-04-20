from __future__ import annotations

import json
import logging
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import uuid4

from backend.app.config import get_settings
from backend.app.db import (
    _complete_ingest_run_with_connection,
    _insert_raw_spotify_history_observation_with_connection,
    _insert_ingest_run_with_connection,
    _insert_or_upgrade_raw_play_event_with_connection,
    backfill_local_text_entities,
    backfill_spotify_source_entities,
    get_sqlite_db_path,
    merge_conservative_same_album_release_track_duplicates,
    patch_ingest_run_timing_phases,
    refresh_conservative_track_relationships,
    refresh_conservative_analysis_track_links,
)
from backend.app.history_dump_mapper import map_history_dump_row
from backend.app.play_event_projector import reconcile_fact_play_events_for_ingest_run

logger = logging.getLogger("listenlabs.sync")
file_logger = logging.getLogger("listenlabs.sync.file")
ROW_LOG_EVERY = 1000


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _history_audio_file_paths(history_dir: str | Path | None = None) -> list[Path]:
    resolved_dir = Path(history_dir or get_settings().spotify_history_dir)
    if not resolved_dir.exists():
        raise RuntimeError(f"Spotify history directory does not exist: {resolved_dir}")
    if not resolved_dir.is_dir():
        raise RuntimeError(f"Spotify history path is not a directory: {resolved_dir}")

    return sorted(resolved_dir.glob("Streaming_History_Audio_*.json"))


def load_history_dump_rows_from_files(
    *,
    history_dir: str | Path | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    rows, file_paths, _metrics = load_history_dump_rows_from_files_with_metrics(history_dir=history_dir)
    return rows, file_paths


def load_history_dump_rows_from_files_with_metrics(
    *,
    history_dir: str | Path | None = None,
) -> tuple[list[dict[str, Any]], list[str], dict[str, float]]:
    discovery_started = perf_counter()
    rows: list[dict[str, Any]] = []
    file_paths = _history_audio_file_paths(history_dir)
    discovery_elapsed_ms = (perf_counter() - discovery_started) * 1000
    file_read_elapsed_ms = 0.0
    file_parse_elapsed_ms = 0.0
    if not file_paths:
        raise RuntimeError("No Spotify audio history JSON files were found.")
    logger.info("Spotify history files discovered: %s files", len(file_paths))
    file_logger.debug(
        "event=history_dump_files_discovered file_count=%s discovery_ms=%.2f history_dir=%s",
        len(file_paths),
        discovery_elapsed_ms,
        str(Path(history_dir or get_settings().spotify_history_dir)),
    )

    for file_path in file_paths:
        read_started = perf_counter()
        with file_path.open("r", encoding="utf-8") as file_handle:
            raw_text = file_handle.read()
        read_elapsed_ms = (perf_counter() - read_started) * 1000
        file_read_elapsed_ms += read_elapsed_ms

        parse_started = perf_counter()
        payload = json.loads(raw_text)
        parse_elapsed_ms = (perf_counter() - parse_started) * 1000
        file_parse_elapsed_ms += parse_elapsed_ms
        if not isinstance(payload, list):
            raise RuntimeError(f"Spotify history file did not contain a row list: {file_path}")
        valid_rows = [row for row in payload if isinstance(row, dict)]
        rows.extend(valid_rows)
        file_logger.debug(
            "event=history_dump_file_loaded file_path=%s read_ms=%.2f parse_ms=%.2f row_count=%s",
            str(file_path),
            read_elapsed_ms,
            parse_elapsed_ms,
            len(valid_rows),
        )

    return rows, [str(file_path) for file_path in file_paths], {
        "file_discovery_ms": discovery_elapsed_ms,
        "file_read_ms": file_read_elapsed_ms,
        "file_parse_ms": file_parse_elapsed_ms,
    }


def ingest_history_dump_rows(
    *,
    rows: list[dict[str, Any]],
    source_ref: str | None = None,
    continue_on_error: bool = False,
    run_post_ingest_pipeline: bool = True,
    file_timing_phases_ms: dict[str, float] | None = None,
) -> dict[str, Any]:
    total_started = perf_counter()
    run_id = str(uuid4())
    started_at = _utc_now_iso()

    logger.info("Spotify history ingest started")
    file_logger.debug(
        "event=history_dump_ingest_started run_id=%s source_ref=%s",
        run_id,
        source_ref,
    )

    row_count = 0
    inserted_count = 0
    duplicate_count = 0
    already_seen_source_row_count = 0
    merged_duplicate_row_count = 0
    failure_count = 0
    mapping_elapsed_ms = 0.0
    db_ingest_elapsed_ms = 0.0
    commit_elapsed_ms = 0.0
    matcher_elapsed_ms = 0.0
    projector_elapsed_ms = 0.0
    downstream_pipeline_elapsed_ms = 0.0

    canonical_projection_summary: dict[str, Any] | None = None
    downstream_pipeline_summary: dict[str, Any] | None = None
    try:
        with sqlite3.connect(get_sqlite_db_path()) as connection:
            _insert_ingest_run_with_connection(
                connection,
                run_id=run_id,
                source_type="export",
                started_at=started_at,
                source_ref=source_ref,
            )

            for raw_row in rows:
                row_count += 1
                try:
                    mapping_started = perf_counter()
                    mapped = map_history_dump_row(raw_row)
                    mapping_elapsed_ms += (perf_counter() - mapping_started) * 1000

                    db_started = perf_counter()
                    result = _insert_or_upgrade_raw_play_event_with_connection(
                        connection,
                        source_type="export",
                        source_row_key=str(mapped["source_row_key"]),
                        cross_source_event_key=mapped.get("cross_source_event_key"),
                        played_at=str(mapped["played_at"]),
                        ms_played=int(mapped["ms_played"]),
                        ms_played_method=str(mapped["ms_played_method"]),
                        track_duration_ms=mapped.get("track_duration_ms"),
                        reason_start=mapped.get("reason_start"),
                        reason_end=mapped.get("reason_end"),
                        raw_payload_json=str(mapped["raw_payload_json"]),
                        ingest_run_id=run_id,
                        skipped=mapped.get("skipped"),
                        platform=mapped.get("platform"),
                        shuffle=mapped.get("shuffle"),
                        offline=mapped.get("offline"),
                        conn_country=mapped.get("conn_country"),
                        spotify_track_uri=mapped.get("spotify_track_uri"),
                        spotify_track_id=mapped.get("spotify_track_id"),
                        track_name_raw=mapped.get("track_name_raw"),
                        artist_name_raw=mapped.get("artist_name_raw"),
                        album_name_raw=mapped.get("album_name_raw"),
                        spotify_album_id=mapped.get("spotify_album_id"),
                        spotify_artist_ids_json=mapped.get("spotify_artist_ids_json"),
                    )
                    _insert_raw_spotify_history_observation_with_connection(
                        connection,
                        ingest_run_id=run_id,
                        source_row_key=str(mapped["source_row_key"]),
                        played_at=str(mapped["played_at"]),
                        ms_played=int(mapped["ms_played"]),
                        raw_payload_json=str(mapped["raw_payload_json"]),
                        spotify_track_uri=mapped.get("spotify_track_uri"),
                        spotify_track_id=mapped.get("spotify_track_id"),
                        track_name_raw=mapped.get("track_name_raw"),
                        artist_name_raw=mapped.get("artist_name_raw"),
                        album_name_raw=mapped.get("album_name_raw"),
                        spotify_album_id=mapped.get("spotify_album_id"),
                        spotify_artist_ids_json=mapped.get("spotify_artist_ids_json"),
                        reason_start=mapped.get("reason_start"),
                        reason_end=mapped.get("reason_end"),
                        skipped=mapped.get("skipped"),
                        shuffle=mapped.get("shuffle"),
                        offline=mapped.get("offline"),
                        platform=mapped.get("platform"),
                        conn_country=mapped.get("conn_country"),
                        private_session=mapped.get("private_session"),
                    )
                    db_ingest_elapsed_ms += (perf_counter() - db_started) * 1000

                    action = str(result["action"])
                    match_type = str(result.get("match_type") or "")
                    if action == "unchanged":
                        duplicate_count += 1
                        if match_type == "source_row_key":
                            already_seen_source_row_count += 1
                    elif action == "merged_duplicate_row":
                        merged_duplicate_row_count += 1
                    else:
                        inserted_count += 1

                    if file_logger.isEnabledFor(logging.DEBUG) and (
                        row_count <= 5 or row_count % ROW_LOG_EVERY == 0
                    ):
                        file_logger.debug(
                            "event=history_dump_row_result run_id=%s row_number=%s source_row_key=%s match_type=%s result=%s raw_play_event_id=%s",
                            run_id,
                            row_count,
                            mapped["source_row_key"],
                            match_type,
                            action,
                            result["row_id"],
                        )
                except Exception:
                    failure_count += 1
                    if not continue_on_error:
                        raise
                    file_logger.exception(
                        "event=history_dump_row_failed run_id=%s row_number=%s",
                        run_id,
                        row_count,
                    )

            completed_at = _utc_now_iso()
            _complete_ingest_run_with_connection(
                connection,
                run_id=run_id,
                completed_at=completed_at,
                row_count=row_count,
                inserted_count=inserted_count,
                duplicate_count=duplicate_count,
                error_count=failure_count,
                status="completed",
            )
            commit_started = perf_counter()
            connection.commit()
            commit_elapsed_ms = (perf_counter() - commit_started) * 1000
        projector_started = perf_counter()
        canonical_projection_summary = reconcile_fact_play_events_for_ingest_run(
            source_type="export",
            run_id=run_id,
        )
        projector_elapsed_ms = (perf_counter() - projector_started) * 1000
        matcher_elapsed_ms = float(canonical_projection_summary.get("matcher_ms", 0.0))
        projector_elapsed_ms = float(canonical_projection_summary.get("projector_ms", projector_elapsed_ms))

        if run_post_ingest_pipeline:
            pipeline_started = perf_counter()
            downstream_pipeline_summary = {
                "ran": False,
                "skipped_reason": None,
                "steps": {},
            }
            if inserted_count <= 0:
                downstream_pipeline_summary["skipped_reason"] = "no_new_rows_inserted"
            else:
                spotify_exact_started = perf_counter()
                spotify_exact = backfill_spotify_source_entities()
                spotify_exact_ms = (perf_counter() - spotify_exact_started) * 1000

                local_text_started = perf_counter()
                local_text = backfill_local_text_entities()
                local_text_ms = (perf_counter() - local_text_started) * 1000

                dedupe_started = perf_counter()
                dedupe = merge_conservative_same_album_release_track_duplicates()
                dedupe_ms = (perf_counter() - dedupe_started) * 1000

                analysis_started = perf_counter()
                analysis = refresh_conservative_analysis_track_links()
                analysis_ms = (perf_counter() - analysis_started) * 1000

                relationships_started = perf_counter()
                relationships = refresh_conservative_track_relationships()
                relationships_ms = (perf_counter() - relationships_started) * 1000

                downstream_pipeline_summary["ran"] = True
                downstream_pipeline_summary["steps"] = {
                    "backfill_spotify_source_entities": {
                        "duration_ms": spotify_exact_ms,
                        "result": spotify_exact,
                    },
                    "backfill_local_text_entities": {
                        "duration_ms": local_text_ms,
                        "result": local_text,
                    },
                    "merge_conservative_same_album_release_track_duplicates": {
                        "duration_ms": dedupe_ms,
                        "result": dedupe,
                    },
                    "refresh_conservative_analysis_track_links": {
                        "duration_ms": analysis_ms,
                        "result": analysis,
                    },
                    "refresh_conservative_track_relationships": {
                        "duration_ms": relationships_ms,
                        "result": relationships,
                    },
                }
            downstream_pipeline_elapsed_ms = (perf_counter() - pipeline_started) * 1000
        logger.info(
            "Spotify history ingest completed: %s rows, %s new, %s duplicates",
            row_count,
            inserted_count,
            duplicate_count,
        )
        file_logger.debug(
            "event=history_dump_ingest_completed run_id=%s row_count=%s inserted_count=%s duplicate_count=%s already_seen_source_row_count=%s merged_duplicate_row_count=%s failure_count=%s mapping_ms=%.2f db_ingest_ms=%.2f total_ms=%.2f",
            run_id,
            row_count,
            inserted_count,
            duplicate_count,
            already_seen_source_row_count,
            merged_duplicate_row_count,
            failure_count,
            mapping_elapsed_ms,
            db_ingest_elapsed_ms,
            (perf_counter() - total_started) * 1000,
        )
    except Exception:
        logger.info("Spotify history ingest failed")
        file_logger.exception(
            "event=history_dump_ingest_failed run_id=%s row_count=%s inserted_count=%s duplicate_count=%s failure_count=%s mapping_ms=%.2f db_ingest_ms=%.2f total_ms=%.2f source_ref=%s",
            run_id,
            row_count,
            inserted_count,
            duplicate_count,
            failure_count,
            mapping_elapsed_ms,
            db_ingest_elapsed_ms,
            (perf_counter() - total_started) * 1000,
            source_ref,
        )
        raise

    run_duration_ms = (perf_counter() - total_started) * 1000
    timing_phases_ms = {
        "file_discovery_ms": float((file_timing_phases_ms or {}).get("file_discovery_ms", 0.0)),
        "file_read_ms": float((file_timing_phases_ms or {}).get("file_read_ms", 0.0)),
        "file_parse_ms": float((file_timing_phases_ms or {}).get("file_parse_ms", 0.0)),
        "mapping_ms": mapping_elapsed_ms,
        "raw_inserts_ms": db_ingest_elapsed_ms,
        "matcher_ms": matcher_elapsed_ms,
        "projector_ms": projector_elapsed_ms,
        "downstream_pipeline_ms": downstream_pipeline_elapsed_ms,
        "final_commit_ms": commit_elapsed_ms,
        "total_duration_ms": run_duration_ms,
    }
    patch_ingest_run_timing_phases(
        run_id=run_id,
        timing_phases_ms=timing_phases_ms,
    )

    return {
        "run_id": run_id,
        "started_at": started_at,
        "completed_at": completed_at,
        "row_count": row_count,
        "inserted_count": inserted_count,
        "duplicate_count": duplicate_count,
        "already_seen_source_row_count": already_seen_source_row_count,
        "merged_duplicate_row_count": merged_duplicate_row_count,
        "failure_count": failure_count,
        "run_duration_ms": run_duration_ms,
        "timing_phases_ms": timing_phases_ms,
        "canonical_projection_summary": canonical_projection_summary,
        "downstream_pipeline_summary": downstream_pipeline_summary,
    }


def manual_ingest_history_dump_rows(
    rows: list[dict[str, Any]],
    *,
    source_ref: str | None = None,
    run_post_ingest_pipeline: bool = True,
) -> dict[str, Any]:
    return ingest_history_dump_rows(
        rows=rows,
        source_ref=source_ref or "manual_history_dump_ingest",
        run_post_ingest_pipeline=run_post_ingest_pipeline,
    )


def ingest_history_dump_files(
    *,
    history_dir: str | Path | None = None,
    source_ref: str | None = None,
    continue_on_error: bool = False,
    run_post_ingest_pipeline: bool = True,
) -> dict[str, Any]:
    total_started = perf_counter()
    rows, file_paths, load_metrics = load_history_dump_rows_from_files_with_metrics(history_dir=history_dir)
    ingest_summary = ingest_history_dump_rows(
        rows=rows,
        source_ref=source_ref or "spotify_history_files",
        continue_on_error=continue_on_error,
        run_post_ingest_pipeline=run_post_ingest_pipeline,
        file_timing_phases_ms=load_metrics,
    )
    total_elapsed_ms = (perf_counter() - total_started) * 1000
    logger.info("Spotify history file import completed: %s files", len(file_paths))
    file_logger.debug(
        "event=history_dump_files_import_completed file_count=%s row_count=%s total_ms=%.2f",
        len(file_paths),
        ingest_summary["row_count"],
        total_elapsed_ms,
    )
    return {
        "file_count": len(file_paths),
        "file_paths": file_paths,
        "total_elapsed_ms": total_elapsed_ms,
        "timing_phases_ms": dict(ingest_summary.get("timing_phases_ms") or {}),
        **ingest_summary,
    }


def manual_ingest_history_dump_files(
    *,
    history_dir: str | Path | None = None,
    source_ref: str | None = None,
    continue_on_error: bool = False,
    run_post_ingest_pipeline: bool = True,
) -> dict[str, Any]:
    return ingest_history_dump_files(
        history_dir=history_dir,
        source_ref=source_ref or "manual_history_dump_files",
        continue_on_error=continue_on_error,
        run_post_ingest_pipeline=run_post_ingest_pipeline,
    )
