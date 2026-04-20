from __future__ import annotations

import sqlite3
from collections import Counter

from backend.app.db import apply_pending_migrations, get_sqlite_db_path
from backend.app.play_event_projector import reconcile_fact_play_events_for_ingest_run


def main() -> None:
    apply_pending_migrations()
    db_path = get_sqlite_db_path()

    summary_counter: Counter[str] = Counter()
    with sqlite3.connect(db_path) as connection:
        recent_run_ids = [
            row[0]
            for row in connection.execute(
                """
                SELECT DISTINCT ingest_run_id
                FROM raw_spotify_recent
                WHERE ingest_run_id IS NOT NULL
                ORDER BY ingest_run_id ASC
                """
            ).fetchall()
        ]
        history_run_ids = [
            row[0]
            for row in connection.execute(
                """
                SELECT DISTINCT ingest_run_id
                FROM raw_spotify_history
                WHERE ingest_run_id IS NOT NULL
                ORDER BY ingest_run_id ASC
                """
            ).fetchall()
        ]

    for run_id in recent_run_ids:
        summary = reconcile_fact_play_events_for_ingest_run(source_type="spotify_recent", run_id=str(run_id))
        summary_counter["recent_runs"] += 1
        summary_counter["matched_pairs"] += int(summary["matched_pairs_count"])
        summary_counter["tight_10s"] += int(summary["tight_10s_count"])
        summary_counter["wide_30s"] += int(summary["wide_30s_count"])

    for run_id in history_run_ids:
        summary = reconcile_fact_play_events_for_ingest_run(source_type="export", run_id=str(run_id))
        summary_counter["history_runs"] += 1
        summary_counter["matched_pairs"] += int(summary["matched_pairs_count"])
        summary_counter["tight_10s"] += int(summary["tight_10s_count"])
        summary_counter["wide_30s"] += int(summary["wide_30s_count"])

    print("rebuild_complete")
    for key in sorted(summary_counter.keys()):
        print(f"{key}={summary_counter[key]}")


if __name__ == "__main__":
    main()

