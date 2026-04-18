from __future__ import annotations

import threading
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from cryptography.fernet import Fernet, InvalidToken

from backend.app.config import get_settings
from backend.app.db import sqlite_connection

TOKEN_REFRESH_LEEWAY_SECONDS = 60
_REFRESH_LOCKS: dict[str, threading.Lock] = {}
_REFRESH_LOCKS_GUARD = threading.Lock()


class SpotifyTokenStoreError(RuntimeError):
    def __init__(self, message: str, *, permanent: bool = False) -> None:
        super().__init__(message)
        self.permanent = permanent


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _iso_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_iso_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _normalize_scopes(scopes: str | list[str] | tuple[str, ...] | None) -> str:
    if scopes is None:
        return ""
    if isinstance(scopes, str):
        return " ".join(scope for scope in scopes.split() if scope)
    if isinstance(scopes, (list, tuple)):
        return " ".join(str(scope).strip() for scope in scopes if str(scope).strip())
    return str(scopes).strip()


def _fernet() -> Fernet:
    settings = get_settings()
    key = settings.listenlab_token_encryption_key.strip()
    if not key:
        raise RuntimeError(
            "LISTENLAB_TOKEN_ENCRYPTION_KEY is required for encrypted Spotify token storage."
        )
    try:
        return Fernet(key.encode("utf-8"))
    except Exception as exc:  # pragma: no cover - defensive validation
        raise RuntimeError(
            "LISTENLAB_TOKEN_ENCRYPTION_KEY is invalid. Expected a Fernet key "
            "(urlsafe base64-encoded 32-byte key)."
        ) from exc


def validate_token_encryption_key() -> None:
    _fernet()


def _is_permanent_refresh_failure(status_code: int, payload: dict[str, Any] | None) -> bool:
    if status_code in (400, 401, 403):
        error_value = ""
        if payload:
            error_value = str(payload.get("error") or payload.get("error_description") or "").lower()
        if any(token in error_value for token in ("invalid_grant", "invalid_client", "invalid_request")):
            return True
        return status_code in (401, 403)
    return False


def _user_refresh_lock(user_id: str) -> threading.Lock:
    with _REFRESH_LOCKS_GUARD:
        existing = _REFRESH_LOCKS.get(user_id)
        if existing is not None:
            return existing
        created = threading.Lock()
        _REFRESH_LOCKS[user_id] = created
        return created


def _encrypt(plaintext: str) -> str:
    return _fernet().encrypt(plaintext.encode("utf-8")).decode("utf-8")


def _decrypt(ciphertext: str) -> str:
    try:
        return _fernet().decrypt(ciphertext.encode("utf-8")).decode("utf-8")
    except InvalidToken as exc:
        raise SpotifyTokenStoreError("Stored Spotify token could not be decrypted.", permanent=True) from exc


def upsert_spotify_tokens(
    user_id: str,
    spotify_user_id: str,
    access_token: str,
    refresh_token: str,
    expires_at: str,
    scopes: str | list[str] | tuple[str, ...],
) -> None:
    if not user_id or not spotify_user_id:
        raise RuntimeError("Both user_id and spotify_user_id are required.")
    if not access_token or not refresh_token:
        raise RuntimeError("Both access_token and refresh_token are required.")

    access_token_encrypted = _encrypt(access_token)
    refresh_token_encrypted = _encrypt(refresh_token)
    scope_text = _normalize_scopes(scopes)
    now_iso = _iso_utc(_now_utc())

    with sqlite_connection(write=True) as connection:
        connection.execute(
            """
            INSERT INTO spotify_auth (
              user_id,
              spotify_user_id,
              access_token_encrypted,
              refresh_token_encrypted,
              expires_at,
              scopes,
              reauth_required,
              reauth_reason,
              created_at,
              updated_at,
              last_refreshed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 0, NULL, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
              spotify_user_id = excluded.spotify_user_id,
              access_token_encrypted = excluded.access_token_encrypted,
              refresh_token_encrypted = excluded.refresh_token_encrypted,
              expires_at = excluded.expires_at,
              scopes = excluded.scopes,
              reauth_required = 0,
              reauth_reason = NULL,
              updated_at = excluded.updated_at,
              last_refreshed_at = excluded.last_refreshed_at
            """,
            (
                str(user_id),
                str(spotify_user_id),
                access_token_encrypted,
                refresh_token_encrypted,
                str(expires_at),
                scope_text,
                now_iso,
                now_iso,
                now_iso,
            ),
        )


def get_spotify_tokens(user_id: str) -> dict[str, Any] | None:
    with sqlite_connection(row_factory=None) as connection:
        row = connection.execute(
            """
            SELECT
              user_id,
              spotify_user_id,
              access_token_encrypted,
              refresh_token_encrypted,
              expires_at,
              scopes,
              reauth_required,
              reauth_reason,
              created_at,
              updated_at,
              last_refreshed_at
            FROM spotify_auth
            WHERE user_id = ?
            LIMIT 1
            """,
            (str(user_id),),
        ).fetchone()

    if row is None:
        return None

    return {
        "user_id": str(row[0]),
        "spotify_user_id": str(row[1]),
        "access_token": _decrypt(str(row[2])),
        "refresh_token": _decrypt(str(row[3])),
        "expires_at": str(row[4]),
        "scopes": str(row[5] or ""),
        "reauth_required": bool(row[6]),
        "reauth_reason": row[7],
        "created_at": row[8],
        "updated_at": row[9],
        "last_refreshed_at": row[10],
    }


def mark_spotify_reauth_required(user_id: str, reason: str) -> None:
    reason_text = reason.strip() if reason else "reauth_required"
    with sqlite_connection(write=True) as connection:
        connection.execute(
            """
            UPDATE spotify_auth
            SET
              reauth_required = 1,
              reauth_reason = ?,
              updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE user_id = ?
            """,
            (reason_text, str(user_id)),
        )


def disconnect_spotify_auth(user_id: str) -> bool:
    with sqlite_connection(write=True) as connection:
        cursor = connection.execute(
            "DELETE FROM spotify_auth WHERE user_id = ?",
            (str(user_id),),
        )
    return int(cursor.rowcount) > 0


def refresh_access_token_if_needed(user_id: str, *, force_refresh: bool = False) -> dict[str, Any]:
    lock = _user_refresh_lock(str(user_id))
    with lock:
        token_row = get_spotify_tokens(user_id)
        if token_row is None:
            raise SpotifyTokenStoreError("No stored Spotify credentials for this user.", permanent=True)

        if token_row["reauth_required"]:
            reason = token_row.get("reauth_reason") or "Spotify reauthorization is required."
            raise SpotifyTokenStoreError(str(reason), permanent=True)

        expires_at = _parse_iso_utc(str(token_row["expires_at"]))
        if not force_refresh and expires_at > (_now_utc() + timedelta(seconds=TOKEN_REFRESH_LEEWAY_SECONDS)):
            return token_row

        settings = get_settings()
        try:
            response = httpx.post(
                settings.spotify_token_url,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": token_row["refresh_token"],
                    "client_id": settings.spotify_client_id,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=15.0,
            )
        except Exception as exc:
            raise SpotifyTokenStoreError(
                "Spotify refresh failed due to transient network error.",
                permanent=False,
            ) from exc

        payload: dict[str, Any] | None = None
        try:
            payload = response.json()
        except Exception:
            payload = None

        if response.status_code >= 400:
            permanent = _is_permanent_refresh_failure(response.status_code, payload)
            reason = f"Spotify refresh failed ({response.status_code})."
            if permanent:
                mark_spotify_reauth_required(user_id, reason)
            raise SpotifyTokenStoreError(reason, permanent=permanent)

        payload = payload or {}
        new_access_token = str(payload.get("access_token") or "").strip()
        if not new_access_token:
            reason = "Spotify refresh response missing access_token."
            mark_spotify_reauth_required(user_id, reason)
            raise SpotifyTokenStoreError(reason, permanent=True)

        expires_in = int(payload.get("expires_in") or 0)
        if expires_in <= 0:
            reason = "Spotify refresh response missing expires_in."
            mark_spotify_reauth_required(user_id, reason)
            raise SpotifyTokenStoreError(reason, permanent=True)

        new_refresh_token = str(payload.get("refresh_token") or token_row["refresh_token"])
        next_scopes = str(payload.get("scope") or token_row.get("scopes") or "")
        expires_at_iso = _iso_utc(_now_utc() + timedelta(seconds=expires_in))

        upsert_spotify_tokens(
            user_id=str(token_row["user_id"]),
            spotify_user_id=str(token_row["spotify_user_id"]),
            access_token=new_access_token,
            refresh_token=new_refresh_token,
            expires_at=expires_at_iso,
            scopes=next_scopes,
        )
        refreshed = get_spotify_tokens(user_id)
        if refreshed is None:
            raise SpotifyTokenStoreError("Stored Spotify credentials missing after refresh.", permanent=True)
        return refreshed
