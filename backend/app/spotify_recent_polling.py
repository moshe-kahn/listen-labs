from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from backend.app.db import get_spotify_auth_record, list_spotify_auth_users
from backend.app.spotify_recent_sync import sync_spotify_recent_plays
from backend.app.spotify_token_store import (
    SpotifyTokenStoreError,
    mark_spotify_reauth_required,
    refresh_access_token_if_needed,
)

REQUIRED_RECENT_SCOPE = "user-read-recently-played"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _has_scope(scope_text: str | None, required_scope: str) -> bool:
    if not scope_text:
        return False
    return required_scope in {scope.strip() for scope in str(scope_text).split() if scope.strip()}


def _is_permanent_poll_failure(message: str) -> bool:
    value = (message or "").lower()
    return (
        "no longer valid" in value
        or "missing_scope" in value
        or "scope is missing" in value
        or "reauthorization is required" in value
    )


async def poll_recent_for_user(user_id: str) -> dict[str, Any]:
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
        "source_ref": f"scheduled_user_poll:{user_id}",
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

    scope_ok = _has_scope(auth_row.get("scopes"), REQUIRED_RECENT_SCOPE)
    if not scope_ok:
        mark_spotify_reauth_required(str(user_id), f"Missing required scope: {REQUIRED_RECENT_SCOPE}")
        return {
            **base,
            "completed_at": _utc_now_iso(),
            "status": "failed",
            "scope_ok": False,
            "reauth_marked": True,
            "error_type": "missing_scope",
            "error": f"Missing required scope: {REQUIRED_RECENT_SCOPE}",
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
        summary = await sync_spotify_recent_plays(
            str(token_row["access_token"]),
            source_ref=f"scheduled_user_poll:{user_id}",
            limit=50,
        )
    except Exception as exc:
        message = str(exc)
        permanent = _is_permanent_poll_failure(message)
        if permanent:
            mark_spotify_reauth_required(str(user_id), "Spotify poll requires reauthorization.")
        return {
            **base,
            "completed_at": _utc_now_iso(),
            "status": "failed",
            "scope_ok": True,
            "error_type": "poll_sync",
            "error": message,
            "reauth_marked": permanent,
            "spotify_user_id": auth_row.get("spotify_user_id"),
        }

    return {
        **base,
        "completed_at": _utc_now_iso(),
        "status": "ok",
        "scope_ok": True,
        "error": None,
        "error_type": None,
        "spotify_user_id": auth_row.get("spotify_user_id"),
        "run_id": summary.get("run_id"),
        "fetched_count": int(summary.get("fetched_count") or 0),
        "row_count": int(summary.get("row_count") or 0),
        "inserted_count": int(summary.get("inserted_count") or 0),
        "duplicate_count": int(summary.get("duplicate_count") or 0),
        "already_seen_source_row_count": int(summary.get("already_seen_source_row_count") or 0),
        "merged_duplicate_row_count": int(summary.get("merged_duplicate_row_count") or 0),
        "earliest_played_at": summary.get("earliest_played_at"),
        "latest_played_at": summary.get("latest_played_at"),
    }


async def poll_recent_for_all_active_users(limit: int = 500) -> dict[str, Any]:
    started_at = _utc_now_iso()
    users = list_spotify_auth_users(active_only=True, limit=limit)
    results: list[dict[str, Any]] = []
    ok_count = 0
    failed_count = 0
    skipped_count = 0

    for row in users:
        user_id = str(row["user_id"])
        result = await poll_recent_for_user(user_id)
        results.append(result)
        status_value = str(result.get("status") or "")
        if status_value == "ok":
            ok_count += 1
        elif status_value == "skipped":
            skipped_count += 1
        else:
            failed_count += 1

    return {
        "started_at": started_at,
        "completed_at": _utc_now_iso(),
        "status": "ok",
        "active_user_count": len(users),
        "ok_count": ok_count,
        "failed_count": failed_count,
        "skipped_count": skipped_count,
        "results": results,
    }
