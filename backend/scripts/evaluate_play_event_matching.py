from __future__ import annotations

import argparse
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from backend.app.db import get_sqlite_db_path


def _utc_now_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _safe_div(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return a / b


def evaluate(*, run_id: str | None = None) -> Path:
    db_path = get_sqlite_db_path()
    output_path = db_path.parent / "logs" / f"play_event_matching_eval_{_utc_now_stamp()}.txt"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        if run_id:
            run_filter_recent = "WHERE r.ingest_run_id = ?"
            run_filter_history = "WHERE h.ingest_run_id = ?"
            params = (run_id,)
        else:
            run_filter_recent = ""
            run_filter_history = ""
            params = ()

        tier_counts = connection.execute(
            """
            SELECT COALESCE(rl.match_tier, '(none)') AS tier, count(*) AS c
            FROM fact_play_event f
            JOIN fact_play_event_recent_link rl
              ON rl.fact_play_event_id = f.id
            JOIN fact_play_event_history_link hl
              ON hl.fact_play_event_id = f.id
            GROUP BY COALESCE(rl.match_tier, '(none)')
            ORDER BY c DESC
            """
        ).fetchall()

        recent_cross_source_matched = connection.execute(
            f"""
            SELECT count(*)
            FROM raw_spotify_recent r
            JOIN fact_play_event_recent_link l
              ON l.raw_spotify_recent_id = r.id
            JOIN fact_play_event_history_link h
              ON h.fact_play_event_id = l.fact_play_event_id
            {run_filter_recent}
            """,
            params,
        ).fetchone()[0]
        recent_total = connection.execute(
            f"SELECT count(*) FROM raw_spotify_recent r {run_filter_recent}",
            params,
        ).fetchone()[0]
        recent_unmatched = int(recent_total) - int(recent_cross_source_matched)

        history_cross_source_matched = connection.execute(
            f"""
            SELECT count(*)
            FROM raw_spotify_history h
            JOIN fact_play_event_history_link l
              ON l.raw_spotify_history_id = h.id
            JOIN fact_play_event_recent_link r
              ON r.fact_play_event_id = l.fact_play_event_id
            {run_filter_history}
            """,
            params,
        ).fetchone()[0]
        history_total = connection.execute(
            f"SELECT count(*) FROM raw_spotify_history h {run_filter_history}",
            params,
        ).fetchone()[0]
        history_unmatched = int(history_total) - int(history_cross_source_matched)

        timing_source_dist = connection.execute(
            """
            SELECT timing_source, count(*) AS c
            FROM fact_play_event
            GROUP BY timing_source
            ORDER BY c DESC
            """
        ).fetchall()

        recent_vs_history_error = connection.execute(
            """
            SELECT
              r.ms_played_method,
              COALESCE(r.ms_played_fallback_class, '(none)') AS fallback_class,
              count(*) AS pair_count,
              AVG(ABS(r.ms_played_estimate - h.ms_played)) AS avg_abs_error_ms,
              MAX(ABS(r.ms_played_estimate - h.ms_played)) AS max_abs_error_ms
            FROM fact_play_event f
            JOIN fact_play_event_recent_link rl
              ON rl.fact_play_event_id = f.id
            JOIN fact_play_event_history_link hl
              ON hl.fact_play_event_id = f.id
            JOIN raw_spotify_recent r
              ON r.id = rl.raw_spotify_recent_id
            JOIN raw_spotify_history h
              ON h.id = hl.raw_spotify_history_id
            GROUP BY r.ms_played_method, COALESCE(r.ms_played_fallback_class, '(none)')
            ORDER BY pair_count DESC
            """
        ).fetchall()

        fallback_dist = connection.execute(
            """
            SELECT
              COALESCE(ms_played_fallback_class, '(none)') AS fallback_class,
              count(*) AS c
            FROM raw_spotify_recent
            GROUP BY COALESCE(ms_played_fallback_class, '(none)')
            ORDER BY c DESC
            """
        ).fetchall()

    with output_path.open("w", encoding="utf-8") as handle:
        handle.write("Play Event Matching Evaluation Report\n")
        handle.write(f"Generated UTC: {_utc_now_iso()}\n")
        handle.write(f"Database: {db_path}\n")
        if run_id:
            handle.write(f"Filtered run_id: {run_id}\n")
        handle.write("\n")

        handle.write("=== Match Counts by Tier ===\n")
        for row in tier_counts:
            handle.write(f"{row['tier']}: {row['c']}\n")
        handle.write("\n")

        handle.write("=== Matched / Unmatched by Source ===\n")
        handle.write(
            f"recent: matched={recent_cross_source_matched} unmatched={recent_unmatched} total={recent_total} matched_rate={_safe_div(float(recent_cross_source_matched), float(recent_total)):.3f}\n"
        )
        handle.write(
            f"history: matched={history_cross_source_matched} unmatched={history_unmatched} total={history_total} matched_rate={_safe_div(float(history_cross_source_matched), float(history_total)):.3f}\n"
        )
        handle.write("\n")

        handle.write("=== Timing Source Distribution ===\n")
        for row in timing_source_dist:
            handle.write(f"{row['timing_source']}: {row['c']}\n")
        handle.write("\n")

        handle.write("=== Error Summary vs History for Recent-Derived Events ===\n")
        for row in recent_vs_history_error:
            handle.write(
                f"method={row['ms_played_method']} fallback={row['fallback_class']} pairs={row['pair_count']} avg_abs_error_ms={row['avg_abs_error_ms']:.1f} max_abs_error_ms={row['max_abs_error_ms']:.1f}\n"
            )
        handle.write("\n")

        handle.write("=== Fallback Class Distribution ===\n")
        for row in fallback_dist:
            handle.write(f"{row['fallback_class']}: {row['c']}\n")
        handle.write("\n")

    return output_path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()
    output_path = evaluate(run_id=args.run_id)
    print(output_path)


if __name__ == "__main__":
    main()
