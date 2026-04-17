from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any


def _artist_names(track: dict[str, Any]) -> list[str]:
    artists = track.get("artists") or []
    return [str(artist.get("name")) for artist in artists if artist.get("name")]


def _artist_ids_json(track: dict[str, Any]) -> str | None:
    artists = track.get("artists") or []
    artist_ids = [str(artist.get("id")) for artist in artists if artist.get("id")]
    if not artist_ids:
        return None
    return json.dumps(artist_ids, ensure_ascii=True, separators=(",", ":"))


def _canonical_played_at(value: str | None) -> str:
    if not value:
        return ""
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _source_row_key(item: dict[str, Any], track: dict[str, Any]) -> str:
    played_at = _canonical_played_at(item.get("played_at"))
    payload = "|".join(
        [
            played_at,
            str(track.get("id") or ""),
            str(track.get("uri") or ""),
            str(track.get("duration_ms") or ""),
            ",".join(_artist_names(track)),
            str(track.get("name") or ""),
        ]
    )
    return f"spotify_recent_{hashlib.sha1(payload.encode('utf-8')).hexdigest()}"


def _cross_source_event_key(item: dict[str, Any], track: dict[str, Any]) -> str | None:
    played_at = _canonical_played_at(item.get("played_at"))
    spotify_track_id = track.get("id")
    if spotify_track_id:
        payload = f"{played_at}|track_id:{spotify_track_id}"
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()

    spotify_track_uri = track.get("uri")
    if spotify_track_uri:
        payload = f"{played_at}|track_uri:{spotify_track_uri}"
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()

    return None


def map_spotify_recent_play_item(item: dict[str, Any]) -> dict[str, Any]:
    track = item.get("track") or {}
    album = track.get("album") or {}
    artist_names = _artist_names(track)
    track_duration_ms_raw = track.get("duration_ms")
    if track_duration_ms_raw is None:
        raise ValueError("Spotify recent-play item is missing track.duration_ms")
    track_duration_ms = int(track_duration_ms_raw)
    ms_played = int(track_duration_ms * 0.65)
    played_at = _canonical_played_at(item.get("played_at"))

    return {
        "source_row_key": _source_row_key(item, track),
        "cross_source_event_key": _cross_source_event_key(item, track),
        "played_at": played_at,
        "ms_played": ms_played,
        "ms_played_method": "default_guess",
        "track_duration_ms": track_duration_ms,
        "reason_start": None,
        "reason_end": None,
        "raw_payload_json": json.dumps(item, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
        "track_name_raw": track.get("name"),
        "artist_name_raw": ", ".join(artist_names) if artist_names else None,
        "album_name_raw": album.get("name"),
        "spotify_track_uri": track.get("uri"),
        "spotify_track_id": track.get("id"),
        "spotify_album_id": album.get("id"),
        "spotify_artist_ids_json": _artist_ids_json(track),
        "skipped": None,
        "platform": None,
        "shuffle": None,
        "offline": None,
        "conn_country": None,
    }
