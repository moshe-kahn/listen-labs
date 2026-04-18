from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from backend.app.db import get_spotify_auth_record, insert_live_playback_event
from backend.app.spotify_token_store import (
    SpotifyTokenStoreError,
    mark_spotify_reauth_required,
    refresh_access_token_if_needed,
)

REQUIRED_PLAYBACK_SCOPE = "user-read-playback-state"
logger = logging.getLogger("listenlabs.current_playback")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _has_scope(scope_text: str | None, required_scope: str) -> bool:
    if not scope_text:
        return False
    return required_scope in {scope.strip() for scope in str(scope_text).split() if scope.strip()}


def _is_permanent_playback_failure(message: str) -> bool:
    value = (message or "").lower()
    return (
        "no longer valid" in value
        or "missing_scope" in value
        or "scope is missing" in value
        or "reauthorization is required" in value
    )


def _normalize_current_playback(payload: dict[str, Any]) -> dict[str, Any]:
    item = payload.get("item") if isinstance(payload.get("item"), dict) else {}
    device = payload.get("device") if isinstance(payload.get("device"), dict) else {}
    item_type = str(payload.get("currently_playing_type") or item.get("type") or "")

    artist_names: list[str] = []
    artists = item.get("artists")
    if isinstance(artists, list):
        artist_names = [str(artist.get("name")) for artist in artists if isinstance(artist, dict) and artist.get("name")]
    elif item_type == "episode":
        show = item.get("show")
        if isinstance(show, dict) and show.get("publisher"):
            artist_names = [str(show.get("publisher"))]

    album_name = None
    album = item.get("album")
    if isinstance(album, dict) and album.get("name"):
        album_name = str(album.get("name"))

    image_url = None
    if isinstance(album, dict):
        album_images = album.get("images")
        if isinstance(album_images, list):
            first_album_image = next(
                (img for img in album_images if isinstance(img, dict) and img.get("url")),
                None,
            )
            if first_album_image:
                image_url = str(first_album_image.get("url"))
    if image_url is None:
        item_images = item.get("images")
        if isinstance(item_images, list):
            first_item_image = next(
                (img for img in item_images if isinstance(img, dict) and img.get("url")),
                None,
            )
            if first_item_image:
                image_url = str(first_item_image.get("url"))

    return {
        "item_type": item_type or None,
        "item_id": item.get("id"),
        "name": item.get("name"),
        "uri": item.get("uri"),
        "image_url": image_url,
        "artist_names": artist_names,
        "album_name": album_name,
        "device_id": device.get("id"),
        "progress_ms": payload.get("progress_ms"),
        "duration_ms": item.get("duration_ms"),
        "is_playing": bool(payload.get("is_playing")),
        "device_name": device.get("name"),
        "device_type": device.get("type"),
        "timestamp": payload.get("timestamp"),
    }


async def fetch_spotify_current_playback_state(access_token: str) -> dict[str, Any] | None:
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(
            "https://api.spotify.com/v1/me/player",
            headers={"Authorization": f"Bearer {access_token}"},
        )

    if response.status_code == 204:
        return None
    if response.status_code == 401:
        raise RuntimeError("Spotify access token is no longer valid.")
    if response.status_code == 403:
        raise RuntimeError("Spotify scope is missing for playback state.")
    if response.status_code == 429:
        retry_after = response.headers.get("Retry-After")
        raise RuntimeError(f"Spotify rate limit reached for playback state. Retry-After={retry_after}")
    if response.status_code >= 400:
        detail = ""
        try:
            payload = response.json()
            detail = payload.get("error_description") or payload.get("error", {}).get("message") or ""
        except ValueError:
            detail = response.text[:160]
        raise RuntimeError(
            f"Failed to fetch Spotify current playback (status {response.status_code})"
            f"{f': {detail}' if detail else ''}"
        )

    payload = response.json()
    if not isinstance(payload, dict):
        return None
    return payload


async def get_current_playback_for_user(user_id: str) -> dict[str, Any]:
    started_at = _utc_now_iso()
    base = {
        "user_id": str(user_id),
        "started_at": started_at,
        "completed_at": None,
        "status": "failed",
        "scope_ok": False,
        "reauth_marked": False,
        "error": None,
        "error_type": None,
        "snapshot": None,
    }

    auth_row = get_spotify_auth_record(str(user_id))
    if auth_row is None:
        return {
            **base,
            "completed_at": _utc_now_iso(),
            "status": "skipped",
            "error_type": "not_found",
            "error": "No Spotify auth record found for user.",
        }

    scope_ok = _has_scope(auth_row.get("scopes"), REQUIRED_PLAYBACK_SCOPE)
    if not scope_ok:
        mark_spotify_reauth_required(str(user_id), f"Missing required scope: {REQUIRED_PLAYBACK_SCOPE}")
        return {
            **base,
            "completed_at": _utc_now_iso(),
            "status": "failed",
            "scope_ok": False,
            "reauth_marked": True,
            "error_type": "missing_scope",
            "error": f"Missing required scope: {REQUIRED_PLAYBACK_SCOPE}",
            "spotify_user_id": auth_row.get("spotify_user_id"),
        }

    try:
        token_row = refresh_access_token_if_needed(str(user_id))
    except SpotifyTokenStoreError as exc:
        return {
            **base,
            "completed_at": _utc_now_iso(),
            "status": "failed",
            "scope_ok": True,
            "error_type": "token_refresh",
            "error": str(exc),
            "reauth_marked": bool(exc.permanent),
            "spotify_user_id": auth_row.get("spotify_user_id"),
        }

    try:
        payload = await fetch_spotify_current_playback_state(str(token_row["access_token"]))
    except Exception as exc:
        message = str(exc)
        permanent = _is_permanent_playback_failure(message)
        if permanent:
            mark_spotify_reauth_required(str(user_id), "Spotify playback-state access requires reauthorization.")
        return {
            **base,
            "completed_at": _utc_now_iso(),
            "status": "failed",
            "scope_ok": True,
            "error_type": "playback_fetch",
            "error": message,
            "reauth_marked": permanent,
            "spotify_user_id": auth_row.get("spotify_user_id"),
        }

    snapshot = _normalize_current_playback(payload) if isinstance(payload, dict) else None
    try:
        if snapshot is not None:
            insert_live_playback_event(
                user_id=str(user_id),
                spotify_user_id=str(auth_row.get("spotify_user_id") or ""),
                has_playback=True,
                item_type=snapshot.get("item_type"),
                item_id=snapshot.get("item_id"),
                item_name=snapshot.get("name"),
                spotify_track_uri=snapshot.get("uri"),
                artist_names_json=json.dumps(snapshot.get("artist_names") or []),
                album_name=snapshot.get("album_name"),
                progress_ms=int(snapshot.get("progress_ms")) if snapshot.get("progress_ms") is not None else None,
                duration_ms=int(snapshot.get("duration_ms") or 0) if snapshot.get("duration_ms") is not None else None,
                is_playing=bool(snapshot.get("is_playing")),
                device_id=snapshot.get("device_id"),
                device_name=snapshot.get("device_name"),
                device_type=snapshot.get("device_type"),
                spotify_timestamp_ms=int(snapshot.get("timestamp") or 0) if snapshot.get("timestamp") is not None else None,
                raw_payload_json=json.dumps(payload, separators=(",", ":")) if isinstance(payload, dict) else None,
            )
        else:
            insert_live_playback_event(
                user_id=str(user_id),
                spotify_user_id=str(auth_row.get("spotify_user_id") or ""),
                has_playback=False,
                raw_payload_json=None,
            )
    except Exception:
        logger.warning("Failed to persist live playback observation for user_id=%s", str(user_id))

    return {
        **base,
        "completed_at": _utc_now_iso(),
        "status": "ok",
        "scope_ok": True,
        "error": None,
        "error_type": None,
        "spotify_user_id": auth_row.get("spotify_user_id"),
        "has_playback": snapshot is not None,
        "snapshot": snapshot,
    }
