from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

from backend.app.config import BACKEND_DIR, get_settings
from backend.app.db import ensure_sqlite_db, query_album_family_grouping_candidates


def _build_output(payload: dict[str, object]) -> str:
    settings = get_settings()
    summary = payload.get("summary") or {}
    pagination = payload.get("pagination") or {}
    items = payload.get("items") or []

    lines = [
        "Album Family Grouping Candidates",
        "================================",
        f"DB path: {settings.sqlite_db_path}",
        f"Generated at: {datetime.now(UTC).isoformat().replace('+00:00', 'Z')}",
        "",
        f"total_release_albums_scanned: {summary.get('total_release_albums_scanned', 0)}",
        f"total_candidate_groups: {summary.get('total_candidate_groups', 0)}",
        f"candidate_status: {summary.get('candidate_status', 'suggested_only')}",
        f"mutations_applied: {summary.get('mutations_applied', 0)}",
        "",
        f"page_limit: {pagination.get('limit', 0)}",
        f"page_offset: {pagination.get('offset', 0)}",
        f"page_returned: {pagination.get('returned', 0)}",
        f"page_has_more: {pagination.get('has_more', False)}",
        "",
    ]

    if not items:
        lines.append("No grouping candidates found.")
        return "\n".join(lines) + "\n"

    for index, item in enumerate(items, start=1):
        lines.append(f"[candidate {index}]")
        lines.append(f"  candidate_status={item.get('candidate_status', 'suggested_only')}")
        lines.append(f"  candidate_group_key={item.get('candidate_group_key', '')}")
        lines.append(f"  release_album_count={item.get('release_album_count', 0)}")
        lines.append(
            f"  distinct_effective_family_count={item.get('distinct_effective_family_count', 0)}"
        )
        lines.append(f"  release_album_ids={item.get('release_album_ids', [])}")
        lines.append(f"  current_album_family_ids={item.get('current_album_family_ids', [])}")
        lines.append(f"  album_names={item.get('album_names', [])}")
        lines.append(f"  album_normalized_names={item.get('album_normalized_names', [])}")
        lines.append(f"  primary_artist_names={item.get('primary_artist_names', [])}")
        lines.append(f"  primary_artist_ids={item.get('primary_artist_ids', [])}")
        lines.append(f"  release_years={item.get('release_years', [])}")
        lines.append(f"  track_counts={item.get('track_counts', [])}")
        lines.append(f"  title_similarity_score={item.get('title_similarity_score', 0.0)}")
        lines.append(f"  title_match_reason={item.get('title_match_reason', '')}")
        lines.append(f"  artist_match_signal={item.get('artist_match_signal', '')}")
        lines.append(f"  year_proximity_signal={item.get('year_proximity_signal', '')}")
        lines.append(f"  suffix_version_signal={item.get('suffix_version_signal', '')}")
        lines.append(f"  confidence_score={item.get('confidence_score', 0.0)}")
        lines.append(f"  explanation={item.get('explanation', '')}")
        lines.append(f"  warning_flags={item.get('warning_flags', [])}")
        lines.append(f"  recommended_decision={item.get('recommended_decision', 'needs_more_evidence')}")
        release_albums = item.get("release_albums") or []
        for album in release_albums:
            lines.append(
                "  - "
                f"release_album_id={album.get('release_album_id')} | "
                f"name={album.get('primary_name')} | "
                f"normalized_name={album.get('normalized_name')} | "
                f"primary_artist_signature={album.get('primary_artist_signature')} | "
                f"primary_artist_id_signature={album.get('primary_artist_id_signature')} | "
                f"release_year={album.get('release_year')} | "
                f"track_count={album.get('track_count')} | "
                f"effective_album_family_id={album.get('effective_album_family_id')}"
            )
        lines.append("")

    return "\n".join(lines)


def generate_album_family_candidate_report(*, limit: int = 500, offset: int = 0) -> Path:
    ensure_sqlite_db()
    payload = query_album_family_grouping_candidates(limit=limit, offset=offset)

    logs_dir = BACKEND_DIR / "data" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_path = logs_dir / f"album_family_grouping_candidates_{timestamp}.txt"
    output_path.write_text(_build_output(payload), encoding="utf-8")
    return output_path


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    output_path = generate_album_family_candidate_report(limit=500, offset=0)
    print(output_path)


if __name__ == "__main__":
    main()
