from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any


def _canonical_played_at(value: str | None) -> str:
    if not value:
        return ""
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _spotify_track_id_from_uri(spotify_track_uri: str | None) -> str | None:
    if not spotify_track_uri:
        return None
    if spotify_track_uri.startswith("spotify:track:"):
        return spotify_track_uri.split(":")[-1]
    return None


def _source_row_key(row: dict[str, Any]) -> str:
    played_at = _canonical_played_at(row.get("ts"))
    spotify_track_uri = str(row.get("spotify_track_uri") or "")
    payload = "|".join(
        [
            played_at,
            spotify_track_uri,
            str(row.get("ms_played") or ""),
            str(row.get("master_metadata_track_name") or ""),
            str(row.get("master_metadata_album_artist_name") or ""),
            str(row.get("master_metadata_album_album_name") or ""),
        ]
    )
    return f"history_source_{hashlib.sha1(payload.encode('utf-8')).hexdigest()}"


def _cross_source_event_key(row: dict[str, Any]) -> str | None:
    played_at = _canonical_played_at(row.get("ts"))
    spotify_track_uri = row.get("spotify_track_uri")
    spotify_track_id = _spotify_track_id_from_uri(spotify_track_uri)
    if spotify_track_id:
        payload = f"{played_at}|track_id:{spotify_track_id}"
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()
    if spotify_track_uri:
        payload = f"{played_at}|track_uri:{spotify_track_uri}"
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()
    return None


def map_history_dump_row(row: dict[str, Any]) -> dict[str, Any]:
    played_at = _canonical_played_at(row.get("ts"))
    spotify_track_uri = row.get("spotify_track_uri")
    spotify_track_id = _spotify_track_id_from_uri(spotify_track_uri)

    return {
        "source_row_key": _source_row_key(row),
        "cross_source_event_key": _cross_source_event_key(row),
        "played_at": played_at,
        "ms_played": int(row.get("ms_played") or 0),
        "ms_played_method": "history_source",
        "track_duration_ms": row.get("track_duration_ms"),
        "reason_start": row.get("reason_start"),
        "reason_end": row.get("reason_end"),
        "raw_payload_json": json.dumps(row, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
        "track_name_raw": row.get("master_metadata_track_name"),
        "artist_name_raw": row.get("master_metadata_album_artist_name"),
        "album_name_raw": row.get("master_metadata_album_album_name"),
        "spotify_track_uri": spotify_track_uri,
        "spotify_track_id": spotify_track_id,
        "spotify_album_id": row.get("spotify_album_id"),
        "spotify_artist_ids_json": row.get("spotify_artist_ids_json"),
        "skipped": int(bool(row.get("skipped"))) if row.get("skipped") is not None else None,
        "platform": row.get("platform"),
        "shuffle": int(bool(row.get("shuffle"))) if row.get("shuffle") is not None else None,
        "offline": int(bool(row.get("offline"))) if row.get("offline") is not None else None,
        "conn_country": row.get("conn_country"),
    }
