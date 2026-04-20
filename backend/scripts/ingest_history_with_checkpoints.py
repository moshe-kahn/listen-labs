from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import UTC, datetime
from time import perf_counter
from typing import Any
from uuid import uuid4

from backend.app.db import (
    _complete_ingest_run_with_connection,
    _insert_ingest_run_with_connection,
    _insert_or_upgrade_raw_play_event_with_connection,
    _insert_raw_spotify_history_observation_with_connection,
    get_sqlite_db_path,
)
from backend.app.history_dump_ingest import load_history_dump_rows_from_files_with_metrics
from backend.app.history_dump_mapper import map_history_dump_row
from backend.app.play_event_projector import reconcile_fact_play_events_for_ingest_run


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--history-dir", required=True)
    parser.add_argument("--source-ref", default="manual_history_dump_files_checkpointed")
    parser.add_argument("--checkpoint-every", type=int, default=5000)
    args = parser.parse_args()

    total_started = perf_counter()
    rows, file_paths, load_metrics = load_history_dump_rows_from_files_with_metrics(history_dir=args.history_dir)
    print(
        json.dumps(
            {
                "phase": "load_complete",
                "file_count": len(file_paths),
                "row_count": len(rows),
                **load_metrics,
            },
            indent=2,
            sort_keys=True,
        )
    )

    run_id = str(uuid4())
    started_at = _utc_now_iso()
    row_count = 0
    inserted_count = 0
    duplicate_count = 0
    already_seen_source_row_count = 0
    merged_duplicate_row_count = 0
    failure_count = 0
    mapping_elapsed_ms = 0.0
    raw_insert_elapsed_ms = 0.0

    with sqlite3.connect(get_sqlite_db_path()) as connection:
        _insert_ingest_run_with_connection(
            connection,
            run_id=run_id,
            source_type="export",
            started_at=started_at,
            source_ref=args.source_ref,
        )
        connection.commit()

        for raw_row in rows:
            row_count += 1
            map_started = perf_counter()
            mapped = map_history_dump_row(raw_row)
            mapping_elapsed_ms += (perf_counter() - map_started) * 1000

            insert_started = perf_counter()
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
            raw_insert_elapsed_ms += (perf_counter() - insert_started) * 1000

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

            if row_count % max(1, int(args.checkpoint_every)) == 0:
                print(
                    json.dumps(
                        {
                            "phase": "checkpoint",
                            "rows_processed": row_count,
                            "rows_total": len(rows),
                            "inserted_count": inserted_count,
                            "duplicate_count": duplicate_count,
                            "merged_duplicate_row_count": merged_duplicate_row_count,
                            "mapping_ms": mapping_elapsed_ms,
                            "raw_inserts_ms": raw_insert_elapsed_ms,
                            "elapsed_ms_total": (perf_counter() - total_started) * 1000,
                        },
                        sort_keys=True,
                    )
                )
                connection.commit()

        commit_started = perf_counter()
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
        connection.commit()
        final_commit_ms = (perf_counter() - commit_started) * 1000

    projector_started = perf_counter()
    projection_summary = reconcile_fact_play_events_for_ingest_run(source_type="export", run_id=run_id)
    projector_wall_ms = (perf_counter() - projector_started) * 1000

    print(
        json.dumps(
            {
                "phase": "complete",
                "run_id": run_id,
                "started_at": started_at,
                "completed_at": completed_at,
                "row_count": row_count,
                "inserted_count": inserted_count,
                "duplicate_count": duplicate_count,
                "already_seen_source_row_count": already_seen_source_row_count,
                "merged_duplicate_row_count": merged_duplicate_row_count,
                "failure_count": failure_count,
                "timing_phases_ms": {
                    **load_metrics,
                    "mapping_ms": mapping_elapsed_ms,
                    "raw_inserts_ms": raw_insert_elapsed_ms,
                    "matcher_ms": float(projection_summary.get("matcher_ms") or 0.0),
                    "projector_ms": float(projection_summary.get("projector_ms") or projector_wall_ms),
                    "final_commit_ms": final_commit_ms,
                    "total_duration_ms": (perf_counter() - total_started) * 1000,
                },
                "canonical_projection_summary": projection_summary,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

