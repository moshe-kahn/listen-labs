from __future__ import annotations

import argparse
import os
from datetime import UTC, datetime
from pathlib import Path


def _stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--history-dir",
        required=True,
        help="Directory containing Streaming_History_Audio_*.json files",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=5000,
        help="Number of rows to ingest for benchmark",
    )
    parser.add_argument(
        "--db-path",
        default="",
        help="Optional sqlite path. If omitted, uses backend/data/validation sample DB.",
    )
    args = parser.parse_args()

    if args.db_path:
        db_path = Path(args.db_path)
    else:
        db_path = Path("backend") / "data" / "validation" / f"history_benchmark_{_stamp()}.sqlite3"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    os.environ["SQLITE_DB_PATH"] = str(db_path.resolve())

    from backend.app.db import apply_pending_migrations, ensure_sqlite_db
    from backend.app.history_dump_ingest import (
        ingest_history_dump_rows,
        load_history_dump_rows_from_files_with_metrics,
    )

    ensure_sqlite_db()
    apply_pending_migrations()

    rows, files, load_metrics = load_history_dump_rows_from_files_with_metrics(history_dir=args.history_dir)
    sample_rows = rows[: max(0, int(args.sample_size))]
    summary = ingest_history_dump_rows(
        rows=sample_rows,
        source_ref=f"benchmark_sample_{len(sample_rows)}",
        continue_on_error=False,
    )

    print(f"db_path={db_path.resolve()}")
    print(f"file_count={len(files)}")
    print(f"sample_rows={len(sample_rows)}")
    print("timing_phases_ms:")
    merged_timings = {
        **load_metrics,
        **dict(summary.get("timing_phases_ms") or {}),
    }
    for key, value in merged_timings.items():
        print(f"  {key}={value:.2f}")
    print(f"run_duration_ms={float(summary.get('run_duration_ms') or 0.0):.2f}")
    projector = summary.get("canonical_projection_summary") or {}
    if projector:
        print("projector_summary:")
        for key in (
            "matched_pairs_count",
            "tight_10s_count",
            "wide_30s_count",
            "candidate_collect_ms",
            "matcher_ms",
            "projector_ms",
            "commit_ms",
            "total_ms",
        ):
            print(f"  {key}={projector.get(key)}")


if __name__ == "__main__":
    main()

