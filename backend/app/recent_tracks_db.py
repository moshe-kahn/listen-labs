from __future__ import annotations

import json
import sqlite3
from typing import Any, TypedDict

from backend.app.db import sqlite_connection
from backend.app.track_sections import CanonicalTrackSectionItem, CanonicalTrackSectionPayload


class RecentTrackQueryRow(TypedDict):
    source_row_key: str
    played_at: str
    played_at_unix_ms: int | None
    spotify_track_id: str | None
    spotify_track_uri: str | None
    spotify_album_id: str | None
    track_name_raw: str | None
    artist_name_raw: str | None
    album_name_raw: str | None
    track_duration_ms: int | None
    ms_played_estimate: int | None
    context_type: str | None
    context_uri: str | None
    track_url: str | None
    album_url: str | None
    image_url: str | None
    preview_url: str | None
    album_release_year: str | None
    artists: list[dict[str, str | None]] | None
    spotify_context_url: str | None
    spotify_context_href: str | None
    spotify_is_local: bool | None
    spotify_track_type: str | None
    spotify_track_number: int | None
    spotify_disc_number: int | None
    spotify_explicit: bool | None
    spotify_popularity: int | None
    spotify_album_type: str | None
    spotify_album_total_tracks: int | None
    spotify_available_markets_count: int | None
    played_at_gap_ms: int | None


def _parse_recent_track_payload(raw_payload_json: str | None) -> dict[str, Any]:
    if not raw_payload_json:
        return {}
    try:
        payload = json.loads(raw_payload_json)
    except ValueError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _extract_recent_track_query_row(row: sqlite3.Row) -> RecentTrackQueryRow:
    payload = _parse_recent_track_payload(row["raw_payload_json"])
    track_payload = payload.get("track") if isinstance(payload.get("track"), dict) else {}
    album_payload = track_payload.get("album") if isinstance(track_payload.get("album"), dict) else {}
    context_payload = payload.get("context") if isinstance(payload.get("context"), dict) else {}
    artists_payload = track_payload.get("artists") if isinstance(track_payload.get("artists"), list) else []

    artists: list[dict[str, str | None]] = []
    for artist in artists_payload:
        if not isinstance(artist, dict):
            continue
        if not artist.get("name"):
            continue
        artists.append(
            {
                "artist_id": artist.get("id"),
                "name": artist.get("name"),
                "uri": artist.get("uri"),
                "url": ((artist.get("external_urls") or {}).get("spotify") if isinstance(artist.get("external_urls"), dict) else None),
            }
        )

    release_date = album_payload.get("release_date")

    return {
        "source_row_key": str(row["source_row_key"]),
        "played_at": str(row["played_at"]),
        "played_at_unix_ms": int(row["played_at_unix_ms"]) if isinstance(row["played_at_unix_ms"], int) else None,
        "spotify_track_id": row["spotify_track_id"],
        "spotify_track_uri": row["spotify_track_uri"],
        "spotify_album_id": row["spotify_album_id"],
        "track_name_raw": row["track_name_raw"],
        "artist_name_raw": row["artist_name_raw"],
        "album_name_raw": row["album_name_raw"],
        "track_duration_ms": int(row["track_duration_ms"]) if isinstance(row["track_duration_ms"], int) else None,
        "ms_played_estimate": int(row["ms_played_estimate"]) if isinstance(row["ms_played_estimate"], int) else None,
        "context_type": row["context_type"],
        "context_uri": row["context_uri"],
        "track_url": ((track_payload.get("external_urls") or {}).get("spotify") if isinstance(track_payload.get("external_urls"), dict) else None),
        "album_url": ((album_payload.get("external_urls") or {}).get("spotify") if isinstance(album_payload.get("external_urls"), dict) else None),
        "image_url": ((album_payload.get("images") or [{}])[0]).get("url") if isinstance(album_payload.get("images"), list) else None,
        "preview_url": track_payload.get("preview_url"),
        "album_release_year": str(release_date)[:4] if release_date else None,
        "artists": artists or None,
        "spotify_context_url": ((context_payload.get("external_urls") or {}).get("spotify") if isinstance(context_payload.get("external_urls"), dict) else None),
        "spotify_context_href": context_payload.get("href"),
        "spotify_is_local": payload.get("is_local") if isinstance(payload.get("is_local"), bool) else None,
        "spotify_track_type": track_payload.get("type"),
        "spotify_track_number": int(track_payload["track_number"]) if isinstance(track_payload.get("track_number"), int) else None,
        "spotify_disc_number": int(track_payload["disc_number"]) if isinstance(track_payload.get("disc_number"), int) else None,
        "spotify_explicit": track_payload.get("explicit") if isinstance(track_payload.get("explicit"), bool) else None,
        "spotify_popularity": int(track_payload["popularity"]) if isinstance(track_payload.get("popularity"), int) else None,
        "spotify_album_type": album_payload.get("album_type"),
        "spotify_album_total_tracks": int(album_payload["total_tracks"]) if isinstance(album_payload.get("total_tracks"), int) else None,
        "spotify_available_markets_count": len(track_payload.get("available_markets") or []) if isinstance(track_payload.get("available_markets"), list) else None,
        "played_at_gap_ms": None,
    }


def query_recent_track_rows(*, limit: int) -> list[RecentTrackQueryRow]:
    bounded_limit = max(1, int(limit))
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        raw_rows = connection.execute(
            """
            SELECT
              id,
              source_row_key,
              played_at,
              played_at_unix_ms,
              spotify_track_id,
              spotify_track_uri,
              spotify_album_id,
              track_name_raw,
              artist_name_raw,
              album_name_raw,
              track_duration_ms,
              ms_played_estimate,
              context_type,
              context_uri,
              raw_payload_json
            FROM raw_spotify_recent
            ORDER BY played_at DESC, id DESC
            LIMIT ?
            """,
            (bounded_limit,),
        ).fetchall()

    rows = [_extract_recent_track_query_row(row) for row in raw_rows]
    for index, row in enumerate(rows):
        if index + 1 >= len(rows):
            continue
        current_ms = row.get("played_at_unix_ms")
        next_ms = rows[index + 1].get("played_at_unix_ms")
        if current_ms is None or next_ms is None:
            continue
        gap_ms = current_ms - next_ms
        row["played_at_gap_ms"] = gap_ms if gap_ms > 0 else None
    return rows


def map_recent_track_row_to_canonical_item(row: RecentTrackQueryRow) -> CanonicalTrackSectionItem:
    duration_ms = row.get("track_duration_ms")
    estimated_played_ms = row.get("ms_played_estimate")
    return {
        "track_id": row.get("spotify_track_id"),
        "track_name": row.get("track_name_raw"),
        "artist_name": row.get("artist_name_raw"),
        "album_name": row.get("album_name_raw"),
        "album_release_year": row.get("album_release_year"),
        "artists": row.get("artists"),
        "duration_ms": duration_ms,
        "duration_seconds": round(duration_ms / 1000.0, 3) if isinstance(duration_ms, int) and duration_ms >= 0 else None,
        "uri": row.get("spotify_track_uri"),
        "preview_url": row.get("preview_url"),
        "url": row.get("track_url"),
        "image_url": row.get("image_url"),
        "album_id": row.get("spotify_album_id"),
        "album_url": row.get("album_url"),
        "spotify_played_at": row.get("played_at"),
        "spotify_played_at_unix_ms": row.get("played_at_unix_ms"),
        "spotify_context_type": row.get("context_type"),
        "spotify_context_uri": row.get("context_uri"),
        "spotify_context_url": row.get("spotify_context_url"),
        "spotify_context_href": row.get("spotify_context_href"),
        "spotify_is_local": row.get("spotify_is_local"),
        "spotify_track_type": row.get("spotify_track_type"),
        "spotify_track_number": row.get("spotify_track_number"),
        "spotify_disc_number": row.get("spotify_disc_number"),
        "spotify_explicit": row.get("spotify_explicit"),
        "spotify_popularity": row.get("spotify_popularity"),
        "spotify_album_type": row.get("spotify_album_type"),
        "spotify_album_total_tracks": row.get("spotify_album_total_tracks"),
        "spotify_available_markets_count": row.get("spotify_available_markets_count"),
        "played_at_gap_ms": row.get("played_at_gap_ms"),
        "estimated_played_ms": estimated_played_ms,
        "estimated_played_seconds": round(estimated_played_ms / 1000.0, 3) if isinstance(estimated_played_ms, int) and estimated_played_ms >= 0 else None,
        "estimated_completion_ratio": (
            round(min(1.0, estimated_played_ms / duration_ms), 4)
            if isinstance(estimated_played_ms, int) and isinstance(duration_ms, int) and duration_ms > 0
            else None
        ),
        "play_count": None,
        "all_time_play_count": None,
        "recent_play_count": None,
        "first_played_at": None,
        "last_played_at": None,
        "listening_span_days": None,
        "listening_span_years": None,
        "active_months_count": None,
        "span_months_count": None,
        "consistency_ratio": None,
        "longevity_score": None,
        "debug": {
            "source": "db",
            "primary_source": "db",
            "fallback_source": None,
            "section_kind": "recent_tracks",
            "section_window": "recent",
        },
    }


def build_recent_tracks_section_from_db(
    *,
    limit: int,
    recent_range: str,
    recent_window_days: int,
) -> CanonicalTrackSectionPayload:
    rows = query_recent_track_rows(limit=limit)
    items = [map_recent_track_row_to_canonical_item(row) for row in rows]
    return {
        "items": items,
        "available": bool(items),
        "recent_range": recent_range if recent_range in {"short_term", "medium_term"} else None,
        "recent_window_days": int(recent_window_days),
        "debug": {
            "source": "db",
            "primary_source": "db",
            "fallback_source": None,
            "section_kind": "recent_tracks",
            "section_window": "recent",
        },
    }
