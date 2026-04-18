from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.app.db import apply_pending_migrations, ensure_sqlite_db, get_spotify_auth_record
from backend.app.spotify_recent_polling import poll_recent_for_user
from backend.app.spotify_token_store import (
    TOKEN_REFRESH_LEEWAY_SECONDS,
    SpotifyTokenStoreError,
    get_spotify_tokens,
    refresh_access_token_if_needed,
    validate_token_encryption_key,
)


def _parse_iso_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _build_base_summary(user_id: str) -> dict[str, Any]:
    return {
        "user_id": user_id,
        "auth_record_found": False,
        "within_refresh_skew": None,
        "refresh_attempted": False,
        "refresh_forced": False,
        "refresh_succeeded": None,
        "refresh_error": None,
        "refresh_error_permanent": None,
        "reauth_required": None,
        "reauth_reason": None,
        "poll_status": None,
        "poll_error": None,
        "poll_inserted_count": None,
        "poll_duplicate_count": None,
        "poll_latest_played_at": None,
    }


async def _run(user_id: str, *, force_refresh: bool) -> dict[str, Any]:
    ensure_sqlite_db()
    apply_pending_migrations()
    validate_token_encryption_key()

    summary = _build_base_summary(user_id)
    auth_record = get_spotify_auth_record(user_id)
    if auth_record is None:
        summary["poll_status"] = "skipped"
        summary["poll_error"] = "No spotify_auth record found for user."
        return summary

    summary["auth_record_found"] = True
    summary["reauth_required"] = bool(auth_record.get("reauth_required"))
    summary["reauth_reason"] = auth_record.get("reauth_reason")

    token_row = get_spotify_tokens(user_id)
    if token_row is None:
        summary["poll_status"] = "skipped"
        summary["poll_error"] = "No decryptable token row found for user."
        return summary

    expires_at = _parse_iso_utc(str(token_row["expires_at"]))
    within_refresh_skew = expires_at <= (datetime.now(UTC) + timedelta(seconds=TOKEN_REFRESH_LEEWAY_SECONDS))
    summary["within_refresh_skew"] = within_refresh_skew

    refresh_attempted = bool(force_refresh or within_refresh_skew)
    summary["refresh_attempted"] = refresh_attempted
    summary["refresh_forced"] = bool(force_refresh)

    if refresh_attempted:
        try:
            refresh_access_token_if_needed(user_id, force_refresh=force_refresh)
            summary["refresh_succeeded"] = True
        except SpotifyTokenStoreError as exc:
            summary["refresh_succeeded"] = False
            summary["refresh_error"] = str(exc)
            summary["refresh_error_permanent"] = bool(exc.permanent)
        except Exception as exc:  # pragma: no cover - defensive catch
            summary["refresh_succeeded"] = False
            summary["refresh_error"] = str(exc)
            summary["refresh_error_permanent"] = None
    else:
        summary["refresh_succeeded"] = None

    refreshed_auth_record = get_spotify_auth_record(user_id)
    if refreshed_auth_record is not None:
        summary["reauth_required"] = bool(refreshed_auth_record.get("reauth_required"))
        summary["reauth_reason"] = refreshed_auth_record.get("reauth_reason")

    poll_result = await poll_recent_for_user(user_id)
    summary["poll_status"] = poll_result.get("status")
    summary["poll_error"] = poll_result.get("error")
    summary["poll_inserted_count"] = poll_result.get("inserted_count")
    summary["poll_duplicate_count"] = poll_result.get("duplicate_count")
    summary["poll_latest_played_at"] = poll_result.get("latest_played_at")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Dev diagnostic for one user's persisted Spotify auth refresh + polling path.",
    )
    parser.add_argument("--user-id", required=True, help="User id key in spotify_auth.")
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Force a refresh attempt even if token is not yet within skew.",
    )
    args = parser.parse_args()

    result = asyncio.run(_run(str(args.user_id), force_refresh=bool(args.force_refresh)))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
