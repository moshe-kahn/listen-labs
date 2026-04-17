from __future__ import annotations

from datetime import datetime
from typing import Any

import httpx


def _after_to_epoch_millis(after_played_at: str | None) -> int | None:
    if after_played_at is None:
        return None
    parsed = datetime.fromisoformat(after_played_at.replace("Z", "+00:00"))
    return int(parsed.timestamp() * 1000)


async def fetch_spotify_recent_play_items(
    access_token: str,
    *,
    after_played_at: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    page = await fetch_spotify_recent_play_page(
        access_token,
        after_played_at=after_played_at,
        limit=limit,
    )
    return page["items"]


async def fetch_spotify_recent_play_page(
    access_token: str,
    *,
    after_played_at: str | None = None,
    before_cursor: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": max(1, min(limit, 50))}
    after_millis = _after_to_epoch_millis(after_played_at)
    if after_millis is not None:
        params["after"] = after_millis
    if before_cursor is not None:
        params["before"] = before_cursor

    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(
            "https://api.spotify.com/v1/me/player/recently-played",
            params=params,
            headers={"Authorization": f"Bearer {access_token}"},
        )

    if response.status_code == 401:
        raise RuntimeError("Spotify access token is no longer valid.")
    if response.status_code == 403:
        raise RuntimeError("Spotify scope is missing for recently played.")
    if response.status_code == 429:
        retry_after = response.headers.get("Retry-After")
        raise RuntimeError(f"Spotify rate limit reached for recently played. Retry-After={retry_after}")
    if response.status_code >= 400:
        detail = ""
        try:
            payload = response.json()
            detail = payload.get("error_description") or payload.get("error", {}).get("message") or ""
        except ValueError:
            detail = response.text[:160]
        raise RuntimeError(
            f"Failed to fetch Spotify recently played (status {response.status_code})"
            f"{f': {detail}' if detail else ''}"
        )

    payload = response.json()
    items = payload.get("items") or []
    cursors = payload.get("cursors") or {}
    return {
        "items": [item for item in items if isinstance(item, dict)],
        "next": payload.get("next"),
        "before_cursor": str(cursors["before"]) if cursors.get("before") is not None else None,
        "after_cursor": str(cursors["after"]) if cursors.get("after") is not None else None,
    }
