from __future__ import annotations

import argparse
import json
import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path


def _stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> None:
    parser = argparse.ArgumentParser(description="End-to-end ingest/canonical/downstream regression check.")
    parser.add_argument("--history-dir", required=True, help="Directory with Streaming_History_Audio_*.json files.")
    parser.add_argument("--sample-size", type=int, default=250, help="Rows to ingest per run in isolated DB.")
    parser.add_argument(
        "--db-path",
        default="",
        help="Optional sqlite path. Defaults to backend/data/validation/regression_ingest_pipeline_<stamp>.sqlite3",
    )
    args = parser.parse_args()

    db_path = (
        Path(args.db_path).resolve()
        if args.db_path
        else (Path("backend") / "data" / "validation" / f"regression_ingest_pipeline_{_stamp()}.sqlite3").resolve()
    )
    db_path.parent.mkdir(parents=True, exist_ok=True)
    os.environ["SQLITE_DB_PATH"] = str(db_path)

    from backend.app.db import apply_pending_migrations, ensure_sqlite_db
    from backend.app.history_dump_ingest import ingest_history_dump_rows, load_history_dump_rows_from_files_with_metrics

    ensure_sqlite_db()
    apply_pending_migrations()

    rows, file_paths, load_metrics = load_history_dump_rows_from_files_with_metrics(history_dir=args.history_dir)
    sample_rows = rows[: max(1, int(args.sample_size))]

    first = ingest_history_dump_rows(
        rows=sample_rows,
        source_ref="regression_ingest_pipeline_first",
        continue_on_error=False,
        run_post_ingest_pipeline=True,
    )
    _assert(int(first["failure_count"]) == 0, "First ingest reported failures.")
    _assert(int(first["inserted_count"]) > 0, "First ingest inserted_count should be > 0 in isolated DB.")
    first_pipeline = dict(first.get("downstream_pipeline_summary") or {})
    _assert(bool(first_pipeline.get("ran")), "Downstream pipeline should run on first ingest with inserts.")
    step_keys = set((first_pipeline.get("steps") or {}).keys())
    expected_steps = {
        "backfill_spotify_source_entities",
        "backfill_local_text_entities",
        "merge_conservative_same_album_release_track_duplicates",
        "refresh_conservative_analysis_track_links",
        "refresh_conservative_track_relationships",
    }
    _assert(expected_steps.issubset(step_keys), "Downstream step set is incomplete.")

    with sqlite3.connect(db_path) as connection:
        run_id = str(first["run_id"])
        ingest_run_row = connection.execute(
            """
            SELECT
              id,
              status,
              started_at,
              completed_at,
              last_heartbeat_at,
              file_discovery_ms,
              file_read_ms,
              file_parse_ms,
              mapping_ms,
              raw_inserts_ms,
              matcher_ms,
              projector_ms,
              downstream_pipeline_ms,
              final_commit_ms,
              total_duration_ms
            FROM ingest_run
            WHERE id = ?
            """,
            (run_id,),
        ).fetchone()
        _assert(ingest_run_row is not None, "ingest_run row missing for first run.")
        _assert(ingest_run_row[4] is not None, "last_heartbeat_at should be populated on completed run.")
        _assert(ingest_run_row[8] is not None, "mapping_ms should be persisted on completed run.")
        _assert(ingest_run_row[14] is not None, "total_duration_ms should be persisted on completed run.")

        linked_history = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM raw_spotify_history h
                JOIN fact_play_event_history_link l ON l.raw_spotify_history_id = h.id
                WHERE h.ingest_run_id = ?
                """,
                (run_id,),
            ).fetchone()[0]
        )
        run_history_total = int(
            connection.execute(
                "SELECT COUNT(*) FROM raw_spotify_history WHERE ingest_run_id = ?",
                (run_id,),
            ).fetchone()[0]
        )
        duplicate_pair_violations = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM (
                  SELECT rl.raw_spotify_recent_id, hl.raw_spotify_history_id, COUNT(*) c
                  FROM fact_play_event_recent_link rl
                  JOIN fact_play_event_history_link hl ON hl.fact_play_event_id = rl.fact_play_event_id
                  GROUP BY rl.raw_spotify_recent_id, hl.raw_spotify_history_id
                  HAVING COUNT(*) > 1
                )
                """
            ).fetchone()[0]
        )
        multi_recent_link_violations = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM (
                  SELECT raw_spotify_recent_id, COUNT(*) c
                  FROM fact_play_event_recent_link
                  GROUP BY raw_spotify_recent_id
                  HAVING COUNT(*) > 1
                )
                """
            ).fetchone()[0]
        )
        multi_history_link_violations = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM (
                  SELECT raw_spotify_history_id, COUNT(*) c
                  FROM fact_play_event_history_link
                  GROUP BY raw_spotify_history_id
                  HAVING COUNT(*) > 1
                )
                """
            ).fetchone()[0]
        )

    _assert(run_history_total > 0, "Run inserted no raw_spotify_history rows.")
    _assert(linked_history == run_history_total, "Not all run history rows linked to canonical facts.")
    _assert(duplicate_pair_violations == 0, "Duplicate recent/history pair violations found.")
    _assert(multi_recent_link_violations == 0, "Recent rows linked to multiple facts.")
    _assert(multi_history_link_violations == 0, "History rows linked to multiple facts.")

    second = ingest_history_dump_rows(
        rows=sample_rows,
        source_ref="regression_ingest_pipeline_second",
        continue_on_error=False,
        run_post_ingest_pipeline=True,
    )
    _assert(int(second["inserted_count"]) == 0, "Second ingest should be duplicate-only.")
    second_pipeline = dict(second.get("downstream_pipeline_summary") or {})
    _assert(not bool(second_pipeline.get("ran")), "Downstream pipeline should skip on duplicate-only rerun.")
    _assert(
        str(second_pipeline.get("skipped_reason")) == "no_new_rows_inserted",
        "Duplicate-only rerun should skip downstream with no_new_rows_inserted.",
    )

    result = {
        "status": "PASS",
        "db_path": str(db_path),
        "history_dir": str(Path(args.history_dir).resolve()),
        "file_count": len(file_paths),
        "sample_rows": len(sample_rows),
        "load_metrics": load_metrics,
        "first_run": {
            "run_id": first["run_id"],
            "row_count": int(first["row_count"]),
            "inserted_count": int(first["inserted_count"]),
            "duplicate_count": int(first["duplicate_count"]),
            "timing_phases_ms": dict(first.get("timing_phases_ms") or {}),
            "downstream_pipeline_summary": first_pipeline,
        },
        "first_run_ingest_row": {
            "id": ingest_run_row[0],
            "status": ingest_run_row[1],
            "started_at": ingest_run_row[2],
            "completed_at": ingest_run_row[3],
            "last_heartbeat_at": ingest_run_row[4],
            "file_discovery_ms": ingest_run_row[5],
            "file_read_ms": ingest_run_row[6],
            "file_parse_ms": ingest_run_row[7],
            "mapping_ms": ingest_run_row[8],
            "raw_inserts_ms": ingest_run_row[9],
            "matcher_ms": ingest_run_row[10],
            "projector_ms": ingest_run_row[11],
            "downstream_pipeline_ms": ingest_run_row[12],
            "final_commit_ms": ingest_run_row[13],
            "total_duration_ms": ingest_run_row[14],
        },
        "integrity": {
            "run_history_total": run_history_total,
            "run_history_linked_to_facts": linked_history,
            "duplicate_pair_violations": duplicate_pair_violations,
            "multi_recent_link_violations": multi_recent_link_violations,
            "multi_history_link_violations": multi_history_link_violations,
        },
        "second_run": {
            "run_id": second["run_id"],
            "row_count": int(second["row_count"]),
            "inserted_count": int(second["inserted_count"]),
            "duplicate_count": int(second["duplicate_count"]),
            "timing_phases_ms": dict(second.get("timing_phases_ms") or {}),
            "downstream_pipeline_summary": second_pipeline,
        },
    }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
