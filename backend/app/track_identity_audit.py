from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.app.config import BACKEND_DIR
from backend.app.db import _parse_grouping_note, get_effective_album_family_id, sqlite_connection


MAX_AMBIGUOUS_REVIEW_LINES = 50_000
AMBIGUOUS_REVIEW_FILE_GLOB = "analysis_ambiguous_review_*.txt"

_SUMMARY_LINE_PATTERN = re.compile(r"^\s*(grouped_review_entries|ungrouped_review_entries)\s*:\s*(\d+)\s*$")
_GENERATED_AT_PATTERN = re.compile(r"^\s*Generated at:\s*(.+?)\s*$", re.IGNORECASE)
_FAMILY_COUNT_PATTERN = re.compile(r"^\s*([a-zA-Z0-9_]+)\s*:\s*(\d+)\s*$")
_ENTRY_LINE_PATTERN = re.compile(
    r"^\s*\[release_track\s+(\d+)\]\s+(.+?)\s+\|\s+artist=(.*?)\s+\|\s+analysis=(.*?)(?:\s+\|\s+song_family_key=(.*?))?(?:\s+\|\s+confidence=([0-9.]+))?\s*$"
)
_REVIEW_META_PATTERN = re.compile(
    r"^\s*review_families=(.*?)\s+\|\s+base='(.*?)'\s+\|\s+dominant='(.*?)'\s*$"
)
_COMPONENTS_PATTERN = re.compile(r"^\s*components=(.*?)\s*$")


def _coerce_iso(value: str | None) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")
    except ValueError:
        return raw


def _safe_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _resolve_ambiguous_review_file(log_path: str | None) -> Path | None:
    if log_path and str(log_path).strip():
        candidate = Path(str(log_path).strip())
        if candidate.exists() and candidate.is_file():
            return candidate
        return None

    workspace_root = BACKEND_DIR.parent.parent
    logs_dir = BACKEND_DIR / "data" / "logs"
    candidates = [
        *workspace_root.glob(AMBIGUOUS_REVIEW_FILE_GLOB),
        *logs_dir.glob(AMBIGUOUS_REVIEW_FILE_GLOB),
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda item: item.stat().st_mtime)


def _empty_ambiguous_review_payload(*, file_path: str | None, parse_warning: str = "") -> dict[str, Any]:
    return {
        "source": {
            "kind": "file",
            "path": file_path or "",
            "generated_at": None,
        },
        "summary": {
            "grouped_review_entries": 0,
            "ungrouped_review_entries": 0,
            "total_review_entries": 0,
        },
        "family_counts": [],
        "pagination": {
            "limit": 0,
            "offset": 0,
            "returned": 0,
            "has_more": False,
        },
        "filters": {
            "family": None,
            "bucket": None,
        },
        "items": [],
        "parse_warning": parse_warning,
    }


def _parse_component_token(raw_token: str) -> dict[str, Any] | None:
    token = raw_token.strip()
    if not token:
        return None
    segments = token.rsplit(":", 3)
    if len(segments) != 4:
        return None
    label, family, semantic_category, groupable = segments
    normalized = str(groupable).strip().lower()
    groupable_value = normalized == "true" if normalized in {"true", "false"} else False
    return {
        "label": label.strip(),
        "family": family.strip(),
        "semantic_category": semantic_category.strip(),
        "groupable_by_default": groupable_value,
    }


def _parse_component_summary(component_summary: str, warnings: list[str]) -> list[dict[str, Any]]:
    value = str(component_summary).strip()
    if not value:
        return []
    parsed_components: list[dict[str, Any]] = []
    # The upstream log joins components with ", ", but labels may also contain commas.
    # We recover by accumulating comma-split fragments until they form a valid component.
    fragment_buffer = ""
    for fragment in value.split(","):
        fragment_buffer = f"{fragment_buffer},{fragment}" if fragment_buffer else fragment
        component = _parse_component_token(fragment_buffer)
        if component is None:
            continue
        parsed_components.append(component)
        fragment_buffer = ""

    if fragment_buffer.strip():
        warnings.append(f"Unrecognized component token: {fragment_buffer.strip()[:80]}")
    return parsed_components


def query_ambiguous_review_queue(
    *,
    log_path: str | None = None,
    limit: int = 500,
    offset: int = 0,
    family: str | None = None,
    bucket: str | None = None,
) -> dict[str, Any]:
    bounded_limit = max(1, min(int(limit), 500))
    bounded_offset = max(0, int(offset))
    family_filter = str(family).strip().lower() if family else None
    bucket_filter = str(bucket).strip().lower() if bucket in {"grouped", "ungrouped"} else None

    resolved_file = _resolve_ambiguous_review_file(log_path)
    if resolved_file is None:
        payload = _empty_ambiguous_review_payload(
            file_path=str(log_path or ""),
            parse_warning="Ambiguous review file was not found.",
        )
        payload["pagination"] = {
            "limit": bounded_limit,
            "offset": bounded_offset,
            "returned": 0,
            "has_more": False,
        }
        payload["filters"] = {
            "family": family_filter,
            "bucket": bucket_filter,
        }
        return payload

    warnings: list[str] = []
    summary = {"grouped_review_entries": 0, "ungrouped_review_entries": 0, "total_review_entries": 0}
    family_counts: dict[str, int] = {}
    generated_at: str | None = None
    entries: list[dict[str, Any]] = []

    section = ""
    current_entry: dict[str, Any] | None = None
    max_lines_hit = False

    try:
        with resolved_file.open("r", encoding="utf-8", errors="replace") as handle:
            for line_index, raw_line in enumerate(handle):
                if line_index >= MAX_AMBIGUOUS_REVIEW_LINES:
                    max_lines_hit = True
                    break
                line = raw_line.rstrip("\n")
                stripped = line.strip()
                lower = stripped.lower()

                if "summary" in lower and "grouped_review_entries" not in lower and "ungrouped_review_entries" not in lower:
                    section = "summary"
                    continue
                if "review family counts" in lower:
                    if current_entry is not None:
                        entries.append(current_entry)
                        current_entry = None
                    section = "family_counts"
                    continue
                if "grouped review entries" in lower:
                    if current_entry is not None:
                        entries.append(current_entry)
                        current_entry = None
                    section = "grouped"
                    continue
                if "ungrouped review entries" in lower:
                    if current_entry is not None:
                        entries.append(current_entry)
                        current_entry = None
                    section = "ungrouped"
                    continue

                generated_match = _GENERATED_AT_PATTERN.match(line)
                if generated_match:
                    generated_at = _coerce_iso(generated_match.group(1))
                    continue

                if section == "summary":
                    summary_match = _SUMMARY_LINE_PATTERN.match(line)
                    if summary_match:
                        key = summary_match.group(1).strip()
                        summary[key] = int(summary_match.group(2))
                    continue

                if section == "family_counts":
                    family_match = _FAMILY_COUNT_PATTERN.match(line)
                    if family_match:
                        family_name = family_match.group(1).strip()
                        family_counts[family_name] = int(family_match.group(2))
                    elif stripped and not set(stripped).issubset({"-", "="}):
                        warnings.append(f"Malformed family count line: {stripped[:120]}")
                    continue

                if section in {"grouped", "ungrouped"}:
                    entry_match = _ENTRY_LINE_PATTERN.match(line)
                    if entry_match:
                        if current_entry is not None:
                            entries.append(current_entry)
                        analysis_name = entry_match.group(4).strip()
                        current_entry = {
                            "entry_id": f"{section}:{entry_match.group(1)}",
                            "bucket": section,
                            "release_track_id": _safe_int(entry_match.group(1)) or 0,
                            "release_track_name": entry_match.group(2).strip(),
                            "artist_name": entry_match.group(3).strip(),
                            "analysis_name": None if analysis_name.lower() in {"(none)", "none", ""} else analysis_name,
                            "song_family_key": (entry_match.group(5) or "").strip() or None,
                            "confidence": _safe_float(entry_match.group(6)),
                            "review_families": [],
                            "dominant_family": None,
                            "base_title_anchor": None,
                            "components": [],
                            "raw_component_summary": "",
                        }
                        continue

                    if current_entry is None:
                        if stripped and not set(stripped).issubset({"-", "="}):
                            warnings.append(f"Unparsed line outside entry: {stripped[:120]}")
                        continue

                    review_match = _REVIEW_META_PATTERN.match(line)
                    if review_match:
                        review_families = [item.strip() for item in review_match.group(1).split(",") if item.strip()]
                        current_entry["review_families"] = review_families
                        current_entry["base_title_anchor"] = review_match.group(2).strip() or None
                        current_entry["dominant_family"] = review_match.group(3).strip() or None
                        continue

                    components_match = _COMPONENTS_PATTERN.match(line)
                    if components_match:
                        raw_component_summary = components_match.group(1).strip()
                        current_entry["raw_component_summary"] = raw_component_summary
                        current_entry["components"] = _parse_component_summary(raw_component_summary, warnings)
                        continue

                    if stripped and not set(stripped).issubset({"-", "="}):
                        warnings.append(f"Malformed entry detail: {stripped[:120]}")
    except OSError as error:
        payload = _empty_ambiguous_review_payload(
            file_path=str(resolved_file),
            parse_warning=f"Unable to read ambiguous review file: {error}",
        )
        payload["pagination"] = {
            "limit": bounded_limit,
            "offset": bounded_offset,
            "returned": 0,
            "has_more": False,
        }
        payload["filters"] = {
            "family": family_filter,
            "bucket": bucket_filter,
        }
        return payload

    if current_entry is not None:
        entries.append(current_entry)

    if max_lines_hit:
        warnings.append(
            f"Parsing stopped after {MAX_AMBIGUOUS_REVIEW_LINES} lines; file may contain additional entries."
        )

    if summary["grouped_review_entries"] == 0 and summary["ungrouped_review_entries"] == 0:
        grouped_count = sum(1 for item in entries if item.get("bucket") == "grouped")
        ungrouped_count = sum(1 for item in entries if item.get("bucket") == "ungrouped")
        summary["grouped_review_entries"] = grouped_count
        summary["ungrouped_review_entries"] = ungrouped_count
    summary["total_review_entries"] = summary["grouped_review_entries"] + summary["ungrouped_review_entries"]

    filtered_entries = entries
    if family_filter:
        filtered_entries = [
            item for item in filtered_entries if family_filter in {str(name).lower() for name in item["review_families"]}
        ]
    if bucket_filter:
        filtered_entries = [item for item in filtered_entries if item["bucket"] == bucket_filter]

    total_filtered = len(filtered_entries)
    page_items = filtered_entries[bounded_offset : bounded_offset + bounded_limit]

    warning_text = ""
    if warnings:
        warning_text = "; ".join(warnings[:8])
        if len(warnings) > 8:
            warning_text = f"{warning_text}; plus {len(warnings) - 8} more parser warnings"

    return {
        "source": {
            "kind": "file",
            "path": str(resolved_file),
            "generated_at": generated_at,
        },
        "summary": summary,
        "family_counts": [
            {"family": family_name, "count": count}
            for family_name, count in sorted(family_counts.items(), key=lambda item: (-item[1], item[0]))
        ],
        "pagination": {
            "limit": bounded_limit,
            "offset": bounded_offset,
            "returned": len(page_items),
            "has_more": bounded_offset + len(page_items) < total_filtered,
        },
        "filters": {
            "family": family_filter,
            "bucket": bucket_filter,
        },
        "items": page_items,
        "parse_warning": warning_text,
    }


def query_suggested_analysis_groups(
    *,
    limit: int = 100,
    offset: int = 0,
    status: str = "suggested",
) -> dict[str, Any]:
    bounded_limit = max(1, min(int(limit), 200))
    bounded_offset = max(0, int(offset))
    status_filter = str(status).strip().lower() if status else "suggested"
    if status_filter not in {"suggested", "accepted", "all"}:
        status_filter = "suggested"

    if status_filter == "all":
        count_case = "COUNT(atm.release_track_id)"
        confidence_case = "MAX(COALESCE(atm.confidence, 0.0))"
        match_method_case = "MIN(atm.match_method)"
        status_clause = "1 = 1"
        where_params: tuple[Any, ...] = ()
    else:
        count_case = "COUNT(CASE WHEN atm.status = ? THEN atm.release_track_id END)"
        confidence_case = "MAX(CASE WHEN atm.status = ? THEN COALESCE(atm.confidence, 0.0) ELSE 0.0 END)"
        match_method_case = "MIN(CASE WHEN atm.status = ? THEN atm.match_method END)"
        status_clause = "grouped.release_track_count > 0"
        where_params = (status_filter, status_filter, status_filter)

    with sqlite_connection() as connection:
        total_query = f"""
            WITH grouped AS (
              SELECT
                at.id AS analysis_track_id,
                at.primary_name AS analysis_track_name,
                at.grouping_note AS grouping_note,
                {match_method_case} AS match_method,
                {confidence_case} AS confidence,
                {count_case} AS release_track_count
              FROM analysis_track at
              LEFT JOIN analysis_track_map atm
                ON atm.analysis_track_id = at.id
              GROUP BY at.id, at.primary_name, at.grouping_note
            )
            SELECT COUNT(*)
            FROM grouped
            WHERE {status_clause}
        """
        total_row = connection.execute(total_query, where_params).fetchone()
        total_groups = int(total_row[0] or 0) if total_row is not None else 0

        groups_query = f"""
            WITH grouped AS (
              SELECT
                at.id AS analysis_track_id,
                at.primary_name AS analysis_track_name,
                at.grouping_note AS grouping_note,
                {match_method_case} AS match_method,
                {confidence_case} AS confidence,
                {count_case} AS release_track_count
              FROM analysis_track at
              LEFT JOIN analysis_track_map atm
                ON atm.analysis_track_id = at.id
              GROUP BY at.id, at.primary_name, at.grouping_note
            )
            SELECT
              grouped.analysis_track_id,
              grouped.analysis_track_name,
              grouped.grouping_note,
              grouped.match_method,
              grouped.confidence,
              grouped.release_track_count
            FROM grouped
            WHERE {status_clause}
            ORDER BY grouped.confidence DESC, grouped.analysis_track_id ASC
            LIMIT ? OFFSET ?
        """
        rows = connection.execute(groups_query, where_params + (bounded_limit, bounded_offset)).fetchall()

        analysis_track_ids = [int(row[0]) for row in rows]
        release_track_rows: list[tuple[Any, ...]] = []
        if analysis_track_ids:
            placeholders = ",".join("?" for _ in analysis_track_ids)
            if status_filter == "all":
                release_track_query = f"""
                    WITH primary_artists AS (
                      SELECT
                        ordered.release_track_id,
                        group_concat(ordered.artist_name, ' | ') AS artist_signature
                      FROM (
                        SELECT
                          ta.release_track_id,
                          a.canonical_name AS artist_name
                        FROM track_artist ta
                        JOIN artist a ON a.id = ta.artist_id
                        WHERE ta.role = 'primary'
                        ORDER BY ta.release_track_id, COALESCE(ta.billing_index, 999999), ta.id, a.canonical_name
                      ) ordered
                      GROUP BY ordered.release_track_id
                    ),
                    release_albums AS (
                      SELECT
                        at.release_track_id,
                        group_concat(ra.primary_name, ' | ') AS album_names,
                        min(at.release_album_id) AS representative_release_album_id
                      FROM album_track at
                      JOIN release_album ra ON ra.id = at.release_album_id
                      GROUP BY at.release_track_id
                    ),
                    source_refs AS (
                      SELECT
                        stm.release_track_id,
                        group_concat(st.source_name || ':' || st.external_id, ' | ') AS source_refs,
                        group_concat(stm.match_method || '@' || printf('%.2f', stm.confidence), ' | ') AS source_map_methods
                      FROM source_track_map stm
                      JOIN source_track st ON st.id = stm.source_track_id
                      GROUP BY stm.release_track_id
                    )
                    SELECT
                      atm.analysis_track_id,
                      rt.id AS release_track_id,
                      rt.primary_name AS release_track_name,
                      rt.normalized_name,
                      COALESCE(pa.artist_signature, '') AS primary_artists,
                      COALESCE(ral.album_names, '') AS album_names,
                      COALESCE(sr.source_refs, '') AS source_refs,
                      COALESCE(sr.source_map_methods, '') AS source_map_methods,
                      atm.confidence,
                      COALESCE(ral.representative_release_album_id, 0) AS representative_release_album_id
                    FROM analysis_track_map atm
                    JOIN release_track rt ON rt.id = atm.release_track_id
                    LEFT JOIN primary_artists pa ON pa.release_track_id = rt.id
                    LEFT JOIN release_albums ral ON ral.release_track_id = rt.id
                    LEFT JOIN source_refs sr ON sr.release_track_id = rt.id
                    WHERE atm.analysis_track_id IN ({placeholders})
                    ORDER BY atm.analysis_track_id ASC, atm.confidence DESC, rt.id ASC
                """
                release_track_rows = connection.execute(release_track_query, tuple(analysis_track_ids)).fetchall()
            else:
                release_track_query = f"""
                    WITH primary_artists AS (
                      SELECT
                        ordered.release_track_id,
                        group_concat(ordered.artist_name, ' | ') AS artist_signature
                      FROM (
                        SELECT
                          ta.release_track_id,
                          a.canonical_name AS artist_name
                        FROM track_artist ta
                        JOIN artist a ON a.id = ta.artist_id
                        WHERE ta.role = 'primary'
                        ORDER BY ta.release_track_id, COALESCE(ta.billing_index, 999999), ta.id, a.canonical_name
                      ) ordered
                      GROUP BY ordered.release_track_id
                    ),
                    release_albums AS (
                      SELECT
                        at.release_track_id,
                        group_concat(ra.primary_name, ' | ') AS album_names,
                        min(at.release_album_id) AS representative_release_album_id
                      FROM album_track at
                      JOIN release_album ra ON ra.id = at.release_album_id
                      GROUP BY at.release_track_id
                    ),
                    source_refs AS (
                      SELECT
                        stm.release_track_id,
                        group_concat(st.source_name || ':' || st.external_id, ' | ') AS source_refs,
                        group_concat(stm.match_method || '@' || printf('%.2f', stm.confidence), ' | ') AS source_map_methods
                      FROM source_track_map stm
                      JOIN source_track st ON st.id = stm.source_track_id
                      GROUP BY stm.release_track_id
                    )
                    SELECT
                      atm.analysis_track_id,
                      rt.id AS release_track_id,
                      rt.primary_name AS release_track_name,
                      rt.normalized_name,
                      COALESCE(pa.artist_signature, '') AS primary_artists,
                      COALESCE(ral.album_names, '') AS album_names,
                      COALESCE(sr.source_refs, '') AS source_refs,
                      COALESCE(sr.source_map_methods, '') AS source_map_methods,
                      atm.confidence,
                      COALESCE(ral.representative_release_album_id, 0) AS representative_release_album_id
                    FROM analysis_track_map atm
                    JOIN release_track rt ON rt.id = atm.release_track_id
                    LEFT JOIN primary_artists pa ON pa.release_track_id = rt.id
                    LEFT JOIN release_albums ral ON ral.release_track_id = rt.id
                    LEFT JOIN source_refs sr ON sr.release_track_id = rt.id
                    WHERE atm.analysis_track_id IN ({placeholders})
                      AND atm.status = ?
                    ORDER BY atm.analysis_track_id ASC, atm.confidence DESC, rt.id ASC
                """
                release_track_rows = connection.execute(
                    release_track_query,
                    tuple(analysis_track_ids) + (status_filter,),
                ).fetchall()

    release_rows_by_analysis: dict[int, list[dict[str, Any]]] = {}
    for row in release_track_rows:
        analysis_track_id = int(row[0])
        representative_release_album_id = int(row[9] or 0)
        if representative_release_album_id > 0:
            # Compatibility probe for future album-family-aware debug reads.
            # Intentionally does not affect payload shape, ordering, scoring, grouping,
            # or ranking in this endpoint; it only exercises the shared family resolver.
            _effective_album_family_probe = get_effective_album_family_id(representative_release_album_id)
        release_rows_by_analysis.setdefault(analysis_track_id, []).append(
            {
                "release_track_id": int(row[1]),
                "release_track_name": str(row[2]),
                "normalized_name": str(row[3] or ""),
                "primary_artists": str(row[4] or ""),
                "album_names": str(row[5] or ""),
                "source_refs": str(row[6] or ""),
                "source_map_methods": str(row[7] or ""),
            }
        )

    items: list[dict[str, Any]] = []
    for row in rows:
        analysis_track_id = int(row[0])
        grouping_note = str(row[2] or "")
        grouping_hash, song_family_key = _parse_grouping_note(grouping_note)
        release_tracks = release_rows_by_analysis.get(analysis_track_id, [])
        items.append(
            {
                "analysis_track_id": analysis_track_id,
                "analysis_track_name": str(row[1] or ""),
                "grouping_note": grouping_note,
                "grouping_hash": grouping_hash,
                "song_family_key": song_family_key,
                "match_method": str(row[3] or ""),
                "confidence": float(row[4] or 0.0),
                "status": status_filter,
                "release_track_count": int(row[5] or 0),
                "release_tracks": release_tracks,
            }
        )

    return {
        "summary": {
            "total_groups": total_groups,
            "status": status_filter,
        },
        "pagination": {
            "limit": bounded_limit,
            "offset": bounded_offset,
            "returned": len(items),
            "has_more": bounded_offset + len(items) < total_groups,
        },
        "items": items,
    }


def _event_counts_by_spotify_track_id(track_ids: list[str]) -> dict[str, int]:
    safe_ids = [track_id for track_id in track_ids if track_id]
    if not safe_ids:
        return {}
    placeholders = ",".join("?" for _ in safe_ids)
    with sqlite_connection() as connection:
        rows = connection.execute(
            f"""
            SELECT spotify_track_id, COUNT(*)
            FROM fact_play_event
            WHERE spotify_track_id IN ({placeholders})
            GROUP BY spotify_track_id
            """,
            safe_ids,
        ).fetchall()
    return {str(row[0]): int(row[1] or 0) for row in rows if row[0]}


def query_same_name_canonical_split_examples(*, limit: int = 5) -> list[dict[str, Any]]:
    bounded_limit = max(1, min(int(limit), 20))
    with sqlite_connection() as connection:
        groups = connection.execute(
            """
            SELECT
              LOWER(TRIM(track_name_canonical)) AS track_key,
              LOWER(TRIM(artist_name_canonical)) AS artist_key,
              MAX(track_name_canonical) AS display_track_name,
              MAX(artist_name_canonical) AS display_artist_name,
              COUNT(DISTINCT spotify_track_id) AS spotify_track_id_count,
              COUNT(*) AS listen_count,
              MIN(canonical_ended_at) AS first_listened_at,
              MAX(canonical_ended_at) AS last_listened_at
            FROM fact_play_event
            WHERE spotify_track_id IS NOT NULL
              AND TRIM(COALESCE(track_name_canonical, '')) != ''
              AND TRIM(COALESCE(artist_name_canonical, '')) != ''
            GROUP BY track_key, artist_key
            HAVING spotify_track_id_count > 1
            ORDER BY listen_count DESC, spotify_track_id_count DESC, display_track_name ASC
            LIMIT ?
            """,
            (bounded_limit,),
        ).fetchall()

        examples: list[dict[str, Any]] = []
        for group in groups:
            variants = connection.execute(
                """
                SELECT
                  spotify_track_id,
                  MAX(spotify_track_uri) AS spotify_track_uri,
                  MAX(spotify_album_id) AS spotify_album_id,
                  MAX(track_name_canonical) AS track_name,
                  MAX(album_name_canonical) AS album_name,
                  COUNT(*) AS listen_count,
                  MIN(canonical_ended_at) AS first_listened_at,
                  MAX(canonical_ended_at) AS last_listened_at
                FROM fact_play_event
                WHERE spotify_track_id IS NOT NULL
                  AND LOWER(TRIM(track_name_canonical)) = ?
                  AND LOWER(TRIM(artist_name_canonical)) = ?
                GROUP BY spotify_track_id
                ORDER BY listen_count DESC, last_listened_at DESC, spotify_track_id ASC
                """,
                (group[0], group[1]),
            ).fetchall()
            examples.append(
                {
                    "example_type": "same_name_canonical_split",
                    "track_name": group[2],
                    "artist_name": group[3],
                    "spotify_track_id_count": int(group[4] or 0),
                    "listen_count": int(group[5] or 0),
                    "first_listened_at": group[6],
                    "last_listened_at": group[7],
                    "variants": [
                        {
                            "spotify_track_id": row[0],
                            "spotify_track_uri": row[1],
                            "spotify_album_id": row[2],
                            "track_name": row[3],
                            "album_name": row[4],
                            "listen_count": int(row[5] or 0),
                            "first_listened_at": row[6],
                            "last_listened_at": row[7],
                        }
                        for row in variants
                    ],
                }
            )
    return examples


def query_release_track_split_examples(*, limit: int = 5) -> list[dict[str, Any]]:
    bounded_limit = max(1, min(int(limit), 20))
    with sqlite_connection() as connection:
        groups = connection.execute(
            """
            SELECT
              rt.id,
              rt.primary_name,
              COUNT(DISTINCT st.id) AS source_track_count,
              GROUP_CONCAT(DISTINCT st.external_id) AS source_track_ids
            FROM release_track rt
            JOIN source_track_map stm
              ON stm.release_track_id = rt.id
             AND stm.status = 'accepted'
            JOIN source_track st
              ON st.id = stm.source_track_id
            GROUP BY rt.id
            HAVING source_track_count > 1
            ORDER BY source_track_count DESC, rt.id ASC
            LIMIT ?
            """,
            (bounded_limit,),
        ).fetchall()

        examples: list[dict[str, Any]] = []
        for group in groups:
            source_rows = connection.execute(
                """
                SELECT
                  st.id,
                  st.source_name,
                  st.external_id,
                  st.external_uri,
                  st.source_name_raw,
                  stm.match_method,
                  stm.confidence,
                  stm.status
                FROM source_track_map stm
                JOIN source_track st
                  ON st.id = stm.source_track_id
                WHERE stm.release_track_id = ?
                  AND stm.status = 'accepted'
                ORDER BY stm.confidence DESC, st.id ASC
                """,
                (int(group[0]),),
            ).fetchall()
            event_counts = _event_counts_by_spotify_track_id([str(row[2]) for row in source_rows])
            examples.append(
                {
                    "example_type": "release_track_source_split",
                    "release_track_id": int(group[0]),
                    "release_track_name": group[1],
                    "source_track_count": int(group[2] or 0),
                    "folded_listen_count": sum(event_counts.values()),
                    "source_tracks": [
                        {
                            "source_track_id": int(row[0]),
                            "source_name": row[1],
                            "external_id": row[2],
                            "external_uri": row[3],
                            "source_name_raw": row[4],
                            "match_method": row[5],
                            "confidence": float(row[6] or 0.0),
                            "status": row[7],
                            "listen_count": event_counts.get(str(row[2]), 0),
                        }
                        for row in source_rows
                    ],
                }
            )
    return examples


def query_analysis_track_group_examples(*, limit: int = 5) -> list[dict[str, Any]]:
    bounded_limit = max(1, min(int(limit), 20))
    with sqlite_connection() as connection:
        groups = connection.execute(
            """
            SELECT
              at.id,
              at.primary_name,
              at.grouping_note,
              COUNT(DISTINCT atm.release_track_id) AS release_track_count
            FROM analysis_track at
            JOIN analysis_track_map atm
              ON atm.analysis_track_id = at.id
            GROUP BY at.id
            HAVING release_track_count > 1
            ORDER BY release_track_count DESC, at.id ASC
            LIMIT ?
            """,
            (bounded_limit,),
        ).fetchall()

        examples: list[dict[str, Any]] = []
        for group in groups:
            release_rows = connection.execute(
                """
                SELECT
                  rt.id,
                  rt.primary_name,
                  atm.match_method,
                  atm.confidence,
                  atm.status,
                  COUNT(DISTINCT stm.source_track_id) AS source_track_count
                FROM analysis_track_map atm
                JOIN release_track rt
                  ON rt.id = atm.release_track_id
                LEFT JOIN source_track_map stm
                  ON stm.release_track_id = rt.id
                 AND stm.status = 'accepted'
                WHERE atm.analysis_track_id = ?
                GROUP BY rt.id, atm.id
                ORDER BY atm.confidence DESC, rt.id ASC
                """,
                (int(group[0]),),
            ).fetchall()
            examples.append(
                {
                    "example_type": "analysis_track_group",
                    "analysis_track_id": int(group[0]),
                    "analysis_track_name": group[1],
                    "grouping_note": group[2],
                    "release_track_count": int(group[3] or 0),
                    "release_tracks": [
                        {
                            "release_track_id": int(row[0]),
                            "release_track_name": row[1],
                            "match_method": row[2],
                            "confidence": float(row[3] or 0.0),
                            "status": row[4],
                            "source_track_count": int(row[5] or 0),
                        }
                        for row in release_rows
                    ],
                }
            )
    return examples


def build_track_identity_audit(limit: int = 5) -> dict[str, Any]:
    bounded_limit = max(1, min(int(limit), 20))
    return {
        "limit": bounded_limit,
        "same_name_canonical_splits": query_same_name_canonical_split_examples(limit=bounded_limit),
        "release_track_source_splits": query_release_track_split_examples(limit=bounded_limit),
        "analysis_track_groups": query_analysis_track_group_examples(limit=bounded_limit),
    }
