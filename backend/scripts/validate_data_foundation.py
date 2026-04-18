from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.app.db import (  # noqa: E402
    apply_pending_migrations,
    ensure_sqlite_db,
    get_sqlite_db_path,
    insert_or_upgrade_raw_play_event,
    list_unified_top_tracks,
)
from backend.app.history_dump_ingest import ingest_history_dump_files  # noqa: E402


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _build_db_path(output_dir: Path) -> Path:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return output_dir / f"foundation_validation_{timestamp}.sqlite3"


def _fetch_one_by_cross_source_event_key(cross_source_event_key: str) -> dict[str, Any] | None:
    with sqlite3.connect(get_sqlite_db_path()) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute(
            """
            SELECT *
            FROM raw_play_event
            WHERE cross_source_event_key = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (cross_source_event_key,),
        ).fetchone()
        return dict(row) if row is not None else None


def _count_by_cross_source_event_key(cross_source_event_key: str) -> int:
    with sqlite3.connect(get_sqlite_db_path()) as connection:
        row = connection.execute(
            """
            SELECT COUNT(*)
            FROM raw_play_event
            WHERE cross_source_event_key = ?
            """,
            (cross_source_event_key,),
        ).fetchone()
        return int(row[0]) if row else 0


def _insert_test_row(
    *,
    source_type: str,
    source_row_key: str,
    cross_source_event_key: str,
    played_at: str,
    ms_played: int,
    ms_played_method: str,
    spotify_track_id: str,
    spotify_track_uri: str,
    track_name_raw: str,
    artist_name_raw: str,
    reason_start: str | None = None,
) -> dict[str, Any]:
    return insert_or_upgrade_raw_play_event(
        source_type=source_type,
        source_row_key=source_row_key,
        cross_source_event_key=cross_source_event_key,
        played_at=played_at,
        ms_played=ms_played,
        ms_played_method=ms_played_method,
        raw_payload_json=json.dumps({"source_row_key": source_row_key}, ensure_ascii=True),
        spotify_track_id=spotify_track_id,
        spotify_track_uri=spotify_track_uri,
        track_name_raw=track_name_raw,
        artist_name_raw=artist_name_raw,
        reason_start=reason_start,
    )


def run_merge_validation_matrix() -> list[dict[str, Any]]:
    matrix_results: list[dict[str, Any]] = []

    # 1) history only
    case_key = "matrix:history-only:play-1"
    result = _insert_test_row(
        source_type="export",
        source_row_key="matrix:history-only:row-1",
        cross_source_event_key=case_key,
        played_at="2024-11-20T12:00:00Z",
        ms_played=210000,
        ms_played_method="history_source",
        spotify_track_id="matrix-track-history-only",
        spotify_track_uri="spotify:track:matrix-track-history-only",
        track_name_raw="History Only Song",
        artist_name_raw="History Artist",
    )
    matrix_results.append(
        {
            "scenario": "history only",
            "pass": result["action"] == "inserted" and _count_by_cross_source_event_key(case_key) == 1,
            "details": {"action": result["action"]},
        }
    )

    # 2) API only
    case_key = "matrix:api-only:play-1"
    result = _insert_test_row(
        source_type="spotify_recent",
        source_row_key="matrix:api-only:row-1",
        cross_source_event_key=case_key,
        played_at="2024-11-21T12:00:00Z",
        ms_played=120000,
        ms_played_method="default_guess",
        spotify_track_id="matrix-track-api-only",
        spotify_track_uri="spotify:track:matrix-track-api-only",
        track_name_raw="API Only Song",
        artist_name_raw="API Artist",
    )
    matrix_results.append(
        {
            "scenario": "API only",
            "pass": result["action"] == "inserted" and _count_by_cross_source_event_key(case_key) == 1,
            "details": {"action": result["action"]},
        }
    )

    # 3) same play from both sources
    case_key = "matrix:same-play:shared-play"
    api_result = _insert_test_row(
        source_type="spotify_recent",
        source_row_key="matrix:same-play:api-row-1",
        cross_source_event_key=case_key,
        played_at="2024-11-22T08:00:00Z",
        ms_played=90000,
        ms_played_method="default_guess",
        spotify_track_id="matrix-track-same-play",
        spotify_track_uri="spotify:track:matrix-track-same-play",
        track_name_raw="Same Play Song",
        artist_name_raw="Same Play Artist",
    )
    history_result = _insert_test_row(
        source_type="export",
        source_row_key="matrix:same-play:history-row-1",
        cross_source_event_key=case_key,
        played_at="2024-11-22T08:00:00Z",
        ms_played=187000,
        ms_played_method="history_source",
        spotify_track_id="matrix-track-same-play",
        spotify_track_uri="spotify:track:matrix-track-same-play",
        track_name_raw="Same Play Song",
        artist_name_raw="Same Play Artist",
    )
    upgraded_row = _fetch_one_by_cross_source_event_key(case_key)
    matrix_results.append(
        {
            "scenario": "same play from both sources",
            "pass": (
                api_result["action"] == "inserted"
                and history_result["action"] == "merged_duplicate_row"
                and _count_by_cross_source_event_key(case_key) == 1
                and upgraded_row is not None
                and upgraded_row["ms_played_method"] == "history_source"
            ),
            "details": {
                "api_action": api_result["action"],
                "history_action": history_result["action"],
                "ms_played_method": upgraded_row["ms_played_method"] if upgraded_row else None,
            },
        }
    )

    # 4) same track but two distinct plays
    track_id = "matrix-track-two-plays"
    play_a = "matrix:two-plays:2024-11-23T10:00:00Z"
    play_b = "matrix:two-plays:2024-11-23T11:00:00Z"
    _insert_test_row(
        source_type="spotify_recent",
        source_row_key="matrix:two-plays:api-row-a",
        cross_source_event_key=play_a,
        played_at="2024-11-23T10:00:00Z",
        ms_played=180000,
        ms_played_method="default_guess",
        spotify_track_id=track_id,
        spotify_track_uri=f"spotify:track:{track_id}",
        track_name_raw="Two Plays Song",
        artist_name_raw="Two Plays Artist",
    )
    _insert_test_row(
        source_type="spotify_recent",
        source_row_key="matrix:two-plays:api-row-b",
        cross_source_event_key=play_b,
        played_at="2024-11-23T11:00:00Z",
        ms_played=170000,
        ms_played_method="default_guess",
        spotify_track_id=track_id,
        spotify_track_uri=f"spotify:track:{track_id}",
        track_name_raw="Two Plays Song",
        artist_name_raw="Two Plays Artist",
    )
    matrix_results.append(
        {
            "scenario": "same track but two distinct plays",
            "pass": _count_by_cross_source_event_key(play_a) == 1 and _count_by_cross_source_event_key(play_b) == 1,
            "details": {
                "play_a_rows": _count_by_cross_source_event_key(play_a),
                "play_b_rows": _count_by_cross_source_event_key(play_b),
            },
        }
    )

    # 5) metadata mismatch across sources
    case_key = "matrix:metadata-mismatch:play-1"
    _insert_test_row(
        source_type="spotify_recent",
        source_row_key="matrix:metadata-mismatch:api-row",
        cross_source_event_key=case_key,
        played_at="2024-11-24T09:00:00Z",
        ms_played=100000,
        ms_played_method="default_guess",
        spotify_track_id="matrix-track-mismatch",
        spotify_track_uri="spotify:track:matrix-track-mismatch",
        track_name_raw="Track Name API",
        artist_name_raw="Artist API",
    )
    mismatch_result = _insert_test_row(
        source_type="export",
        source_row_key="matrix:metadata-mismatch:history-row",
        cross_source_event_key=case_key,
        played_at="2024-11-24T09:00:00Z",
        ms_played=195000,
        ms_played_method="history_source",
        spotify_track_id="matrix-track-mismatch",
        spotify_track_uri="spotify:track:matrix-track-mismatch",
        track_name_raw="Track Name History Different",
        artist_name_raw="Artist History Different",
    )
    mismatch_row = _fetch_one_by_cross_source_event_key(case_key)
    matrix_results.append(
        {
            "scenario": "metadata mismatch across sources",
            "pass": (
                mismatch_result["action"] == "merged_duplicate_row"
                and mismatch_row is not None
                and mismatch_row["track_name_raw"] == "Track Name API"
                and mismatch_row["artist_name_raw"] == "Artist API"
            ),
            "details": {
                "action": mismatch_result["action"],
                "track_name_raw": mismatch_row["track_name_raw"] if mismatch_row else None,
                "artist_name_raw": mismatch_row["artist_name_raw"] if mismatch_row else None,
            },
        }
    )

    # 6) richer source arrives later and improves canonical data
    case_key = "matrix:richer-later:play-1"
    _insert_test_row(
        source_type="spotify_recent",
        source_row_key="matrix:richer-later:api-row",
        cross_source_event_key=case_key,
        played_at="2024-11-25T14:00:00Z",
        ms_played=95000,
        ms_played_method="default_guess",
        spotify_track_id="matrix-track-richer",
        spotify_track_uri="spotify:track:matrix-track-richer",
        track_name_raw="Richer Later Song",
        artist_name_raw="Richer Artist",
    )
    richer_result = _insert_test_row(
        source_type="export",
        source_row_key="matrix:richer-later:history-row",
        cross_source_event_key=case_key,
        played_at="2024-11-25T14:00:00Z",
        ms_played=188000,
        ms_played_method="history_source",
        spotify_track_id="matrix-track-richer",
        spotify_track_uri="spotify:track:matrix-track-richer",
        track_name_raw="Richer Later Song",
        artist_name_raw="Richer Artist",
        reason_start="trackdone",
    )
    richer_row = _fetch_one_by_cross_source_event_key(case_key)
    matrix_results.append(
        {
            "scenario": "richer source arriving later improves canonical data",
            "pass": (
                richer_result["action"] == "merged_duplicate_row"
                and richer_row is not None
                and richer_row["ms_played"] == 188000
                and richer_row["ms_played_method"] == "history_source"
                and richer_row["reason_start"] == "trackdone"
            ),
            "details": {
                "action": richer_result["action"],
                "ms_played": richer_row["ms_played"] if richer_row else None,
                "ms_played_method": richer_row["ms_played_method"] if richer_row else None,
                "reason_start": richer_row["reason_start"] if richer_row else None,
            },
        }
    )

    return matrix_results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run backend-only ingest/merge/top-track foundation validation."
    )
    parser.add_argument(
        "--history-dir",
        type=str,
        default=None,
        help="Optional override for Spotify export history directory.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(REPO_ROOT / "backend" / "data" / "validation"),
        help="Directory where an isolated validation SQLite DB will be created.",
    )
    parser.add_argument(
        "--recent-window-days",
        type=int,
        default=28,
        help="Recent window for unified top-track output.",
    )
    parser.add_argument(
        "--top-limit",
        type=int,
        default=15,
        help="Number of unified top tracks to print.",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = _build_db_path(output_dir)

    os.environ["SQLITE_DB_PATH"] = str(db_path)
    if args.history_dir:
        os.environ["SPOTIFY_HISTORY_DIR"] = str(Path(args.history_dir).resolve())

    ensure_sqlite_db()
    apply_pending_migrations()

    full_run_started = perf_counter()
    ingest_summary = ingest_history_dump_files(
        history_dir=args.history_dir,
        source_ref="foundation_validation_full_history",
        continue_on_error=True,
    )
    full_run_duration_ms = (perf_counter() - full_run_started) * 1000

    matrix_results = run_merge_validation_matrix()
    unified_tracks = list_unified_top_tracks(
        limit=args.top_limit,
        recent_window_days=args.recent_window_days,
        as_of_iso=_utc_now_iso(),
    )

    output = {
        "validation_db_path": str(db_path),
        "history_ingest_summary": {
            "rows_processed": ingest_summary["row_count"],
            "inserted_count": ingest_summary["inserted_count"],
            "duplicate_count": ingest_summary["duplicate_count"],
            "failure_count": ingest_summary["failure_count"],
            "run_duration_ms": ingest_summary["run_duration_ms"],
            "wall_clock_duration_ms": full_run_duration_ms,
            "file_count": ingest_summary.get("file_count"),
            "run_id": ingest_summary.get("run_id"),
            "started_at": ingest_summary.get("started_at"),
            "completed_at": ingest_summary.get("completed_at"),
        },
        "merge_validation_matrix": matrix_results,
        "matrix_passed": all(item["pass"] for item in matrix_results),
        "unified_top_tracks_sample": unified_tracks,
    }

    print(json.dumps(output, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
