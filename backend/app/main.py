from __future__ import annotations

import base64
import hashlib
import json
import logging
import secrets
import time
from datetime import UTC, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlencode

import httpx
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from starlette.middleware.sessions import SessionMiddleware

from backend.app.config import get_settings
from backend.app.db import (
    apply_pending_migrations,
    ensure_sqlite_db,
    list_spotify_auth_users,
    recover_stale_ingest_runs,
)
from backend.app.history_analysis import clear_history_insights_cache, get_history_signature, load_history_insights
from backend.app.logging_config import configure_logging
from backend.app.spotify_recent_api import fetch_spotify_recent_play_page
from backend.app.spotify_current_playback import get_current_playback_for_user
from backend.app.spotify_recent_polling import poll_recent_for_user
from backend.app.spotify_recent_sync import sync_spotify_recent_plays
from backend.app.spotify_token_store import (
    SpotifyTokenStoreError,
    get_spotify_tokens,
    refresh_access_token_if_needed,
    upsert_spotify_tokens,
    validate_token_encryption_key,
)

settings = get_settings()
logger = logging.getLogger("listenlabs.auth")
SECTION_PREVIEW_LIMIT = 10
ALBUM_ANALYSIS_LIMIT = 10
PLAYLIST_ANALYSIS_LIMIT = 10
LOAD_PROGRESS: dict[str, dict[str, Any]] = {}
SECTION_CACHE: dict[str, dict[str, Any]] = {}
SPOTIFY_RATE_LIMIT_STATE: dict[str, float | None] = {"cooldown_until": None}
INITIAL_DASHBOARD_LIMIT = 5
SHORT_CACHE_TTL_SECONDS = 180
CACHE_VERSION = 1
PERSISTENT_HISTORY_CACHE_SCHEMA = "history_sections.v1"
PERSISTENT_HISTORY_CACHE_FILE = "history_sections.json"
LOCAL_HISTORY_INSIGHTS_CACHE_FILE = "local_history_insights.json"
LOCAL_HISTORY_INSIGHTS_CACHE_SCHEMA = "local_history_insights.v1"
STATIC_METADATA_CACHE_FILE = "spotify_static_metadata.json"
PROGRESS_LOG_FILE = "dashboard-progress.log"
PROGRESS_LOG_MAX_BYTES = 1_000_000
PROGRESS_LOG_KEEP_TAIL_BYTES = 400_000
SPOTIFY_RATE_LIMIT_COOLDOWN_SECONDS = 60
SPOTIFY_MAX_RETRY_AFTER_SECONDS = 600
HISTORY_TRACKS_DISPLAY_LIMIT = 40
HISTORY_CACHE_REBUILD_WINDOW_DAYS = 28
MIN_ALBUM_DISTINCT_TRACKS = 3
MIN_RECENT_ALBUM_DISTINCT_TRACKS = 2
STATIC_METADATA_CACHE_VERSION = 1
STATIC_METADATA_CACHE_SCHEMA = "spotify_static_metadata.v1"
LOCAL_HISTORY_INSIGHTS_CACHE_VERSION = 1
USER_RECENT_CACHE_FILE = "user_recent_sections.json"
USER_RECENT_CACHE_VERSION = 1
USER_RECENT_CACHE_SCHEMA = "user_recent_sections.v1"
USER_PROFILE_SNAPSHOT_CACHE_FILE = "user_profile_snapshots.json"
USER_PROFILE_SNAPSHOT_CACHE_VERSION = 1
USER_PROFILE_SNAPSHOT_CACHE_SCHEMA = "user_profile_snapshots.v1"
USER_RECENT_CACHE_MAX_USERS = 50
USER_RECENT_CACHE_MAX_AGE_SECONDS = 60 * 60 * 12
USER_PROFILE_SNAPSHOT_MAX_AGE_SECONDS = 60 * 60 * 24 * 14
STATIC_METADATA_MAX_ARTISTS = 4_000
STATIC_METADATA_MAX_ALBUMS = 6_000
STATIC_METADATA_MAX_TRACKS_BY_ID = 12_000
STATIC_METADATA_MAX_TRACKS_BY_KEY = 12_000

STATIC_METADATA_CACHE: dict[str, Any] | None = None
STATIC_METADATA_DIRTY_CONTENT = False
STATIC_METADATA_DIRTY_ACCESS = False

app = FastAPI(title="ListenLab API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(
    SessionMiddleware,
    secret_key=settings.session_secret,
    same_site="lax",
    https_only=False,
)


@app.on_event("startup")
async def _ensure_sqlite_db_on_startup() -> None:
    log_file_path = configure_logging()
    validate_token_encryption_key()
    ensure_sqlite_db()
    apply_pending_migrations()
    stale_recovery = recover_stale_ingest_runs(stale_after_minutes=60)
    if int(stale_recovery["recovered_count"]) > 0:
        logger.warning(
            "event=stale_ingest_runs_recovered count=%s run_ids=%s cutoff_last_heartbeat_at=%s",
            stale_recovery["recovered_count"],
            ",".join(stale_recovery["recovered_run_ids"]),
            stale_recovery["cutoff_last_heartbeat_at"],
        )
    logger.info("event=backend_ready sqlite_initialized=true debug_log=%s", log_file_path)


def _is_configured() -> bool:
    return bool(
        settings.spotify_client_id
        and settings.listenlab_token_encryption_key
        and settings.spotify_redirect_uri
        and settings.session_secret
    )


def _session_user_id(request: Request) -> str | None:
    user_id = request.session.get("user_id")
    if user_id:
        return str(user_id)
    spotify_user = request.session.get("spotify_user") or {}
    if spotify_user.get("id"):
        return str(spotify_user["id"])
    return None


def _require_user_id(request: Request) -> str:
    user_id = _session_user_id(request)
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated with Spotify.",
        )
    return user_id


def _restore_session_user_from_token_store(request: Request) -> str | None:
    existing_user_id = _session_user_id(request)
    if existing_user_id:
        return existing_user_id

    active_users = list_spotify_auth_users(active_only=True, limit=2)
    if len(active_users) != 1:
        return None

    candidate_user_id = str(active_users[0].get("user_id") or "").strip()
    if not candidate_user_id:
        return None

    try:
        token_row = refresh_access_token_if_needed(candidate_user_id)
    except SpotifyTokenStoreError:
        return None

    request.session["user_id"] = candidate_user_id
    request.session["token_type"] = "Bearer"
    expires_at = str(token_row.get("expires_at") or "")
    if expires_at:
        try:
            remaining = int((_parse_iso_utc(expires_at) - datetime.now(UTC)).total_seconds())
            request.session["expires_in"] = max(0, remaining)
        except ValueError:
            request.session["expires_in"] = None
    request.session["spotify_user"] = {
        "id": str(token_row.get("spotify_user_id") or candidate_user_id),
        "display_name": None,
        "email": None,
    }
    return candidate_user_id


def _expires_at_from_expires_in(expires_in: int | str | None) -> str:
    seconds = int(expires_in or 0)
    if seconds <= 0:
        seconds = 3600
    return (datetime.now(UTC) + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def _parse_iso_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _require_token(request: Request) -> str:
    user_id = _require_user_id(request)
    try:
        token_row = refresh_access_token_if_needed(user_id)
    except SpotifyTokenStoreError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Spotify session expired. Please reconnect Spotify. ({exc})",
        ) from exc

    request.session["user_id"] = user_id
    request.session["token_type"] = "Bearer"
    expires_at = str(token_row.get("expires_at") or "")
    if expires_at:
        try:
            remaining = int((_parse_iso_utc(expires_at) - datetime.now(UTC)).total_seconds())
            request.session["expires_in"] = max(0, remaining)
        except ValueError:
            request.session["expires_in"] = None
    return str(token_row["access_token"])


async def _refresh_spotify_access_token(request: Request) -> str:
    return _require_token(request)


def _callback_redirect_url(
    reason: str,
    detail: str | None = None,
    extra: dict[str, str] | None = None,
) -> str:
    query = {"status": reason}
    if detail:
        query["detail"] = detail
    if extra:
        query.update(extra)
    return f"{settings.frontend_url}/auth/callback?{urlencode(query)}"


def _pkce_code_challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest).decode("utf-8").rstrip("=")


def _progress_key(request: Request) -> str | None:
    user = request.session.get("spotify_user") or {}
    if user.get("id"):
        return str(user["id"])
    user_id = _session_user_id(request)
    if user_id:
        return f"user:{user_id}"
    return None


def _set_load_progress(request: Request, phase: str, mode: str | None = None) -> None:
    key = _progress_key(request)
    if not key:
        return
    current = LOAD_PROGRESS.get(key)
    current_mode = mode or (str(current.get("mode")) if current and current.get("mode") else None)
    mode_prefix = f"mode={current_mode} " if current_mode else ""
    if current is None:
        LOAD_PROGRESS[key] = {
            "phase": phase,
            "mode": current_mode,
            "started_at": time.perf_counter(),
            "last_at_seconds": 0.0,
            "events": [{"phase": phase, "at_seconds": 0.0}],
        }
        _append_progress_log(key, f"total=0.0s delta=0.0s {mode_prefix}{phase}")
        return
    if current.get("phase") == phase:
        return
    if current_mode:
        current["mode"] = current_mode
    current["phase"] = phase
    started_at = float(current.get("started_at", time.perf_counter()))
    elapsed_seconds = round(time.perf_counter() - started_at, 1)
    previous_elapsed = float(current.get("last_at_seconds", 0.0))
    delta_seconds = round(max(0.0, elapsed_seconds - previous_elapsed), 1)
    current["last_at_seconds"] = elapsed_seconds
    current.setdefault("events", []).append(
        {"phase": phase, "at_seconds": elapsed_seconds}
    )
    _append_progress_log(key, f"total={elapsed_seconds:.1f}s delta={delta_seconds:.1f}s {mode_prefix}{phase}")


def _clear_load_progress(request: Request) -> None:
    key = _progress_key(request)
    if key:
        progress = LOAD_PROGRESS.get(key)
        if progress:
            elapsed_seconds = round(
                time.perf_counter() - float(progress.get("started_at", time.perf_counter())),
                1,
            )
            previous_elapsed = float(progress.get("last_at_seconds", 0.0))
            delta_seconds = round(max(0.0, elapsed_seconds - previous_elapsed), 1)
            mode = progress.get("mode")
            mode_prefix = f"mode={mode} " if mode else ""
            _append_progress_log(key, f"total={elapsed_seconds:.1f}s delta={delta_seconds:.1f}s {mode_prefix}complete")
        LOAD_PROGRESS.pop(key, None)


def _cache_dir() -> Path:
    path = Path(settings.cache_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _persistent_history_cache_path() -> Path:
    return _cache_dir() / PERSISTENT_HISTORY_CACHE_FILE


def _local_history_insights_cache_path() -> Path:
    return _cache_dir() / LOCAL_HISTORY_INSIGHTS_CACHE_FILE


def _static_metadata_cache_path() -> Path:
    return _cache_dir() / STATIC_METADATA_CACHE_FILE


def _user_recent_cache_path() -> Path:
    return _cache_dir() / USER_RECENT_CACHE_FILE


def _user_profile_snapshot_cache_path() -> Path:
    return _cache_dir() / USER_PROFILE_SNAPSHOT_CACHE_FILE


def _progress_log_path() -> Path:
    return _cache_dir() / PROGRESS_LOG_FILE


def _read_json_file(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _write_json_file(path: Path, payload: dict[str, Any]) -> None:
    try:
        path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    except OSError:
        logger.exception("Failed to write cache file: %s", path)


def _append_progress_log(key: str, message: str) -> None:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    log_path = _progress_log_path()
    try:
        if log_path.exists() and log_path.stat().st_size > PROGRESS_LOG_MAX_BYTES:
            raw = log_path.read_bytes()
            tail = raw[-PROGRESS_LOG_KEEP_TAIL_BYTES:]
            first_newline = tail.find(b"\n")
            if first_newline != -1:
                tail = tail[first_newline + 1:]
            header = f"[{timestamp}] [system] progress-log truncated keep_tail_bytes={PROGRESS_LOG_KEEP_TAIL_BYTES}\n".encode("utf-8")
            log_path.write_bytes(header + tail)
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(f"[{timestamp}] [{key}] {message}\n")
    except OSError:
        logger.exception("Failed to append dashboard progress log.")


def _cache_key(section: str, user_id: str | None, limit: int) -> str:
    return f"{section}:{user_id or 'anonymous'}:{limit}"


def _get_short_cache(section: str, user_id: str | None, limit: int) -> Any | None:
    entry = SECTION_CACHE.get(_cache_key(section, user_id, limit))
    if not entry:
        return None
    if time.time() - float(entry.get("stored_at", 0)) > SHORT_CACHE_TTL_SECONDS:
        SECTION_CACHE.pop(_cache_key(section, user_id, limit), None)
        return None
    return entry.get("value")


def _set_short_cache(section: str, user_id: str | None, limit: int, value: Any) -> Any:
    SECTION_CACHE[_cache_key(section, user_id, limit)] = {
        "stored_at": time.time(),
        "value": value,
    }
    return value


def _load_persistent_history_cache(
    history_signature: tuple[tuple[str, int, int], ...] | None,
    recent_window_days: int,
) -> dict[str, Any] | None:
    if not history_signature:
        return None

    cache_path = _persistent_history_cache_path()
    if not cache_path.exists():
        return None

    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    if payload.get("cache_version") != CACHE_VERSION:
        return None
    if payload.get("schema") != PERSISTENT_HISTORY_CACHE_SCHEMA:
        return None
    if payload.get("history_signature") != [list(item) for item in history_signature]:
        return None
    if int(payload.get("recent_window_days", 28)) != recent_window_days:
        return None

    return payload.get("sections")


def _load_persistent_history_cache_any_window(
    history_signature: tuple[tuple[str, int, int], ...] | None,
) -> dict[str, Any] | None:
    if not history_signature:
        return None

    cache_path = _persistent_history_cache_path()
    if not cache_path.exists():
        return None

    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    if payload.get("cache_version") != CACHE_VERSION:
        return None
    if payload.get("schema") != PERSISTENT_HISTORY_CACHE_SCHEMA:
        return None
    if payload.get("history_signature") != [list(item) for item in history_signature]:
        return None

    return payload.get("sections")


def _store_persistent_history_cache(
    history_signature: tuple[tuple[str, int, int], ...] | None,
    recent_window_days: int,
    sections: dict[str, Any],
) -> None:
    if not history_signature:
        return

    payload = {
        "cache_version": CACHE_VERSION,
        "schema": PERSISTENT_HISTORY_CACHE_SCHEMA,
        "history_signature": [list(item) for item in history_signature],
        "recent_window_days": recent_window_days,
        "stored_at": time.time(),
        "sections": sections,
    }

    cache_path = _persistent_history_cache_path()
    cache_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _load_local_history_insights_cache(
    history_signature: tuple[tuple[str, int, int], ...] | None,
    recent_window_days: int,
    min_track_limit: int,
) -> dict[str, Any] | None:
    if not history_signature:
        return None
    payload = _read_json_file(_local_history_insights_cache_path())
    if not payload:
        return None
    if payload.get("cache_version") != LOCAL_HISTORY_INSIGHTS_CACHE_VERSION:
        return None
    if payload.get("schema") != LOCAL_HISTORY_INSIGHTS_CACHE_SCHEMA:
        return None
    if payload.get("history_signature") != [list(item) for item in history_signature]:
        return None
    entries = payload.get("entries") or {}
    entry = entries.get(str(recent_window_days)) or {}
    if int(entry.get("track_limit", 0)) < min_track_limit:
        return None
    insights = entry.get("insights")
    return insights if isinstance(insights, dict) else None


def _store_local_history_insights_cache(
    history_signature: tuple[tuple[str, int, int], ...] | None,
    recent_window_days: int,
    track_limit: int,
    insights: dict[str, Any],
) -> None:
    if not history_signature or not insights:
        return
    path = _local_history_insights_cache_path()
    payload = _read_json_file(path) or {}
    existing_signature = payload.get("history_signature")
    next_signature = [list(item) for item in history_signature]
    if existing_signature != next_signature:
        payload = {
            "cache_version": LOCAL_HISTORY_INSIGHTS_CACHE_VERSION,
            "schema": LOCAL_HISTORY_INSIGHTS_CACHE_SCHEMA,
            "history_signature": next_signature,
            "entries": {},
        }
    payload["cache_version"] = LOCAL_HISTORY_INSIGHTS_CACHE_VERSION
    payload["schema"] = LOCAL_HISTORY_INSIGHTS_CACHE_SCHEMA
    payload["history_signature"] = next_signature
    entries = payload.get("entries") or {}
    entries[str(recent_window_days)] = {
        "track_limit": int(track_limit),
        "stored_at": time.time(),
        "insights": insights,
    }
    payload["entries"] = entries
    _write_json_file(path, payload)


def _static_bucket_caps() -> dict[str, int]:
    return {
        "artists_by_name": STATIC_METADATA_MAX_ARTISTS,
        "albums_by_key": STATIC_METADATA_MAX_ALBUMS,
        "tracks_by_id": STATIC_METADATA_MAX_TRACKS_BY_ID,
        "tracks_by_key": STATIC_METADATA_MAX_TRACKS_BY_KEY,
    }


def _is_static_cache_entry(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and isinstance(value.get("data"), dict)
        and isinstance(value.get("created_at"), (int, float))
        and isinstance(value.get("last_accessed"), (int, float))
    )


def _normalize_static_cache_bucket(raw_bucket: Any, *, default_created_at: float) -> dict[str, dict[str, Any]]:
    normalized: dict[str, dict[str, Any]] = {}
    if not isinstance(raw_bucket, dict):
        return normalized
    for key, value in raw_bucket.items():
        if not key:
            continue
        if _is_static_cache_entry(value):
            normalized[str(key)] = {
                "data": value["data"],
                "created_at": float(value["created_at"]),
                "last_accessed": float(value["last_accessed"]),
            }
            continue
        if isinstance(value, dict):
            normalized[str(key)] = {
                "data": value,
                "created_at": default_created_at,
                "last_accessed": default_created_at,
            }
            continue
        # Drop non-dict values safely.
    return normalized


def _load_static_metadata_cache() -> dict[str, Any]:
    global STATIC_METADATA_CACHE
    global STATIC_METADATA_DIRTY_ACCESS
    global STATIC_METADATA_DIRTY_CONTENT
    if STATIC_METADATA_CACHE is not None:
        return STATIC_METADATA_CACHE
    now = time.time()
    payload = _read_json_file(_static_metadata_cache_path()) or {}
    if (
        payload.get("cache_version") != STATIC_METADATA_CACHE_VERSION
        or payload.get("schema") != STATIC_METADATA_CACHE_SCHEMA
    ):
        payload = {}
    default_created_at = float(payload.get("stored_at") or now)
    STATIC_METADATA_CACHE = {
        "cache_version": STATIC_METADATA_CACHE_VERSION,
        "schema": STATIC_METADATA_CACHE_SCHEMA,
        "stored_at": float(payload.get("stored_at") or now),
        "artists_by_name": _normalize_static_cache_bucket(
            payload.get("artists_by_name"),
            default_created_at=default_created_at,
        ),
        "albums_by_key": _normalize_static_cache_bucket(
            payload.get("albums_by_key"),
            default_created_at=default_created_at,
        ),
        "tracks_by_id": _normalize_static_cache_bucket(
            payload.get("tracks_by_id"),
            default_created_at=default_created_at,
        ),
        "tracks_by_key": _normalize_static_cache_bucket(
            payload.get("tracks_by_key"),
            default_created_at=default_created_at,
        ),
    }
    STATIC_METADATA_DIRTY_ACCESS = False
    STATIC_METADATA_DIRTY_CONTENT = False
    return STATIC_METADATA_CACHE


def _static_metadata_get(bucket_name: str, key: str | None) -> dict[str, Any] | None:
    global STATIC_METADATA_DIRTY_ACCESS
    if not key:
        return None
    cache = _load_static_metadata_cache()
    bucket = cache.get(bucket_name) or {}
    entry = bucket.get(key)
    if not _is_static_cache_entry(entry):
        return None
    now = time.time()
    entry["last_accessed"] = now
    STATIC_METADATA_DIRTY_ACCESS = True
    return entry.get("data")


def _static_metadata_set(bucket_name: str, key: str | None, data: dict[str, Any]) -> None:
    global STATIC_METADATA_DIRTY_ACCESS
    global STATIC_METADATA_DIRTY_CONTENT
    if not key or not isinstance(data, dict):
        return
    cache = _load_static_metadata_cache()
    bucket = cache.get(bucket_name)
    if not isinstance(bucket, dict):
        bucket = {}
        cache[bucket_name] = bucket
    now = time.time()
    existing = bucket.get(key)
    if _is_static_cache_entry(existing):
        if existing.get("data") != data:
            existing["data"] = data
            STATIC_METADATA_DIRTY_CONTENT = True
        existing["last_accessed"] = now
        STATIC_METADATA_DIRTY_ACCESS = True
        return
    bucket[key] = {
        "data": data,
        "created_at": now,
        "last_accessed": now,
    }
    STATIC_METADATA_DIRTY_ACCESS = True
    STATIC_METADATA_DIRTY_CONTENT = True


def _remember_artist_metadata(artist: dict[str, Any]) -> None:
    artist_key = _artist_lookup_key(artist.get("name"))
    if not artist_key:
        return
    normalized = {
        "artist_id": artist.get("artist_id"),
        "followers_total": artist.get("followers_total"),
        "genres": artist.get("genres") or [],
        "popularity": artist.get("popularity"),
        "url": artist.get("url"),
        "image_url": artist.get("image_url"),
    }
    if normalized["image_url"] or normalized["url"] or normalized["artist_id"]:
        _static_metadata_set("artists_by_name", artist_key, normalized)


def _remember_track_metadata(track: dict[str, Any]) -> None:
    normalized = {
        "track_id": track.get("track_id"),
        "track_name": track.get("track_name"),
        "artist_name": track.get("artist_name"),
        "album_name": track.get("album_name"),
        "album_release_year": track.get("album_release_year"),
        "url": track.get("url"),
        "album_url": track.get("album_url"),
        "image_url": track.get("image_url"),
        "album_id": track.get("album_id"),
        "uri": track.get("uri"),
    }
    track_id = normalized.get("track_id")
    track_key = _track_identity_key(normalized.get("track_name"), normalized.get("artist_name"))
    album_key = _album_lookup_key(normalized.get("album_name"), normalized.get("artist_name"))
    if track_id:
        _static_metadata_set("tracks_by_id", str(track_id), normalized)
    if track_key:
        _static_metadata_set("tracks_by_key", track_key, normalized)
    if album_key and (normalized.get("image_url") or normalized.get("album_url") or normalized.get("album_id")):
        _static_metadata_set(
            "albums_by_key",
            album_key,
            {
                "album_id": normalized.get("album_id"),
                "url": normalized.get("album_url") or normalized.get("url"),
                "image_url": normalized.get("image_url"),
                "release_year": normalized.get("album_release_year"),
            },
        )


def _trim_static_metadata_cache(cache: dict[str, Any]) -> bool:
    trimmed = False
    for bucket_name, cap in _static_bucket_caps().items():
        bucket = cache.get(bucket_name)
        if not isinstance(bucket, dict) or len(bucket) <= cap:
            continue
        ranked = sorted(
            bucket.items(),
            key=lambda item: (
                float((item[1] or {}).get("last_accessed", 0.0)),
                float((item[1] or {}).get("created_at", 0.0)),
                item[0],
            ),
        )
        overflow = len(bucket) - cap
        for key, _entry in ranked[:overflow]:
            bucket.pop(key, None)
        trimmed = True
    return trimmed


def _save_static_metadata_cache(cache: dict[str, Any], *, persist_access_only: bool = False) -> None:
    global STATIC_METADATA_DIRTY_ACCESS
    global STATIC_METADATA_DIRTY_CONTENT
    trimmed = _trim_static_metadata_cache(cache)
    if trimmed:
        STATIC_METADATA_DIRTY_CONTENT = True
    should_write = STATIC_METADATA_DIRTY_CONTENT or (persist_access_only and STATIC_METADATA_DIRTY_ACCESS)
    if not should_write:
        return
    cache["cache_version"] = STATIC_METADATA_CACHE_VERSION
    cache["schema"] = STATIC_METADATA_CACHE_SCHEMA
    cache["stored_at"] = time.time()
    payload = {
        "cache_version": cache["cache_version"],
        "schema": cache["schema"],
        "stored_at": cache["stored_at"],
    }
    for bucket_name in _static_bucket_caps():
        payload[bucket_name] = cache.get(bucket_name) or {}
    _write_json_file(_static_metadata_cache_path(), payload)
    STATIC_METADATA_DIRTY_ACCESS = False
    STATIC_METADATA_DIRTY_CONTENT = False


def _load_user_recent_cache() -> dict[str, Any]:
    payload = _read_json_file(_user_recent_cache_path()) or {}
    if payload.get("cache_version") != USER_RECENT_CACHE_VERSION:
        return {"cache_version": USER_RECENT_CACHE_VERSION, "schema": USER_RECENT_CACHE_SCHEMA, "users": {}}
    if payload.get("schema") != USER_RECENT_CACHE_SCHEMA:
        return {"cache_version": USER_RECENT_CACHE_VERSION, "schema": USER_RECENT_CACHE_SCHEMA, "users": {}}
    return {
        "cache_version": USER_RECENT_CACHE_VERSION,
        "schema": USER_RECENT_CACHE_SCHEMA,
        "users": payload.get("users") or {},
    }


def _save_user_recent_cache(payload: dict[str, Any]) -> None:
    payload["cache_version"] = USER_RECENT_CACHE_VERSION
    payload["schema"] = USER_RECENT_CACHE_SCHEMA
    _write_json_file(_user_recent_cache_path(), payload)


def _load_user_profile_snapshot_cache() -> dict[str, Any]:
    payload = _read_json_file(_user_profile_snapshot_cache_path()) or {}
    if payload.get("cache_version") != USER_PROFILE_SNAPSHOT_CACHE_VERSION:
        return {"cache_version": USER_PROFILE_SNAPSHOT_CACHE_VERSION, "schema": USER_PROFILE_SNAPSHOT_CACHE_SCHEMA, "users": {}}
    if payload.get("schema") != USER_PROFILE_SNAPSHOT_CACHE_SCHEMA:
        return {"cache_version": USER_PROFILE_SNAPSHOT_CACHE_VERSION, "schema": USER_PROFILE_SNAPSHOT_CACHE_SCHEMA, "users": {}}
    return {
        "cache_version": USER_PROFILE_SNAPSHOT_CACHE_VERSION,
        "schema": USER_PROFILE_SNAPSHOT_CACHE_SCHEMA,
        "users": payload.get("users") or {},
    }


def _save_user_profile_snapshot_cache(payload: dict[str, Any]) -> None:
    payload["cache_version"] = USER_PROFILE_SNAPSHOT_CACHE_VERSION
    payload["schema"] = USER_PROFILE_SNAPSHOT_CACHE_SCHEMA
    _write_json_file(_user_profile_snapshot_cache_path(), payload)


def _store_user_profile_snapshot(user_id: str | None, snapshot: dict[str, Any]) -> None:
    if not user_id:
        return
    payload = _load_user_profile_snapshot_cache()
    users = payload.get("users") or {}
    existing_snapshot = ((users.get(str(user_id)) or {}).get("snapshot")) or {}
    users[str(user_id)] = {
        "stored_at": time.time(),
        "snapshot": {
            **existing_snapshot,
            **snapshot,
        },
    }
    payload["users"] = users
    _save_user_profile_snapshot_cache(payload)


def _load_user_profile_snapshot(user_id: str | None) -> dict[str, Any] | None:
    if not user_id:
        return None
    payload = _load_user_profile_snapshot_cache()
    users = payload.get("users") or {}
    entry = users.get(str(user_id)) or {}
    if not entry:
        return None
    stored_at = float(entry.get("stored_at", 0.0))
    if time.time() - stored_at > USER_PROFILE_SNAPSHOT_MAX_AGE_SECONDS:
        users.pop(str(user_id), None)
        payload["users"] = users
        _save_user_profile_snapshot_cache(payload)
        return None
    snapshot = entry.get("snapshot")
    if not isinstance(snapshot, dict):
        return None
    return {
        **snapshot,
        "_stored_at": stored_at,
    }


def _store_user_recent_snapshot(
    user_id: str | None,
    recent_range: str,
    snapshot: dict[str, Any],
) -> None:
    if not user_id:
        return
    payload = _load_user_recent_cache()
    users = payload.get("users") or {}
    now = time.time()
    users[str(user_id)] = {
        "stored_at": now,
        "recent_range": recent_range,
        "snapshot": snapshot,
    }
    if len(users) > USER_RECENT_CACHE_MAX_USERS:
        ranked = sorted(
            users.items(),
            key=lambda item: float((item[1] or {}).get("stored_at", 0.0)),
            reverse=True,
        )[:USER_RECENT_CACHE_MAX_USERS]
        users = dict(ranked)
    payload["users"] = users
    _save_user_recent_cache(payload)


def _load_user_recent_snapshot(
    user_id: str | None,
    recent_range: str,
) -> dict[str, Any] | None:
    if not user_id:
        return None
    payload = _load_user_recent_cache()
    users = payload.get("users") or {}
    entry = users.get(str(user_id)) or {}
    if not entry:
        return None
    stored_at = float(entry.get("stored_at", 0.0))
    if time.time() - stored_at > USER_RECENT_CACHE_MAX_AGE_SECONDS:
        users.pop(str(user_id), None)
        payload["users"] = users
        _save_user_recent_cache(payload)
        return None
    if entry.get("recent_range") != recent_range:
        return None
    snapshot = entry.get("snapshot")
    return snapshot if isinstance(snapshot, dict) else None


def _clear_dashboard_caches() -> None:
    SECTION_CACHE.clear()
    global STATIC_METADATA_CACHE
    global STATIC_METADATA_DIRTY_ACCESS
    global STATIC_METADATA_DIRTY_CONTENT
    STATIC_METADATA_CACHE = None
    STATIC_METADATA_DIRTY_ACCESS = False
    STATIC_METADATA_DIRTY_CONTENT = False
    clear_history_insights_cache()
    try:
        _persistent_history_cache_path().unlink(missing_ok=True)
    except OSError:
        logger.exception("Failed to remove persistent history cache.")
    try:
        _local_history_insights_cache_path().unlink(missing_ok=True)
    except OSError:
        logger.exception("Failed to remove local history insights cache.")
    try:
        _static_metadata_cache_path().unlink(missing_ok=True)
    except OSError:
        logger.exception("Failed to remove static metadata cache.")
    try:
        _user_recent_cache_path().unlink(missing_ok=True)
    except OSError:
        logger.exception("Failed to remove user recent cache.")
    try:
        _user_profile_snapshot_cache_path().unlink(missing_ok=True)
    except OSError:
        logger.exception("Failed to remove user profile snapshot cache.")


def _playlist_cache_needs_refresh(playlists: list[dict[str, Any]]) -> bool:
    if not playlists:
        return False
    image_count = sum(1 for playlist in playlists if playlist.get("image_url"))
    return image_count == 0


def _spotify_cooldown_seconds_remaining() -> int:
    cooldown_until = SPOTIFY_RATE_LIMIT_STATE.get("cooldown_until")
    if cooldown_until is None:
        return 0
    return max(0, min(int(cooldown_until - time.time()), SPOTIFY_MAX_RETRY_AFTER_SECONDS))


def _enforce_spotify_cooldown() -> None:
    remaining = _spotify_cooldown_seconds_remaining()
    if remaining > 0:
        raise HTTPException(
            status_code=429,
            detail=_spotify_rate_limit_detail("Spotify is rate-limiting requests right now."),
        )


def _note_spotify_rate_limit(retry_after_seconds: int | None = None) -> None:
    candidate_seconds = retry_after_seconds or SPOTIFY_RATE_LIMIT_COOLDOWN_SECONDS
    cooldown_seconds = min(
        SPOTIFY_MAX_RETRY_AFTER_SECONDS,
        max(1, int(candidate_seconds)),
    )
    cooldown_until = time.time() + cooldown_seconds
    previous = SPOTIFY_RATE_LIMIT_STATE.get("cooldown_until")
    previous_until = float(previous or 0)
    max_allowed_until = time.time() + SPOTIFY_MAX_RETRY_AFTER_SECONDS
    if previous_until > max_allowed_until:
        previous_until = max_allowed_until
    SPOTIFY_RATE_LIMIT_STATE["cooldown_until"] = max(previous_until, cooldown_until)


def _parse_retry_after_seconds(retry_after_header: str | None) -> int | None:
    if not retry_after_header:
        return None
    value = retry_after_header.strip()
    if not value:
        return None
    if value.isdigit():
        return int(value)
    try:
        retry_after_date = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    if retry_after_date.tzinfo is None:
        retry_after_date = retry_after_date.replace(tzinfo=timezone.utc)
    delta_seconds = int((retry_after_date - datetime.now(timezone.utc)).total_seconds())
    return max(1, delta_seconds)


def _spotify_rate_limit_detail(prefix: str) -> str:
    remaining = max(1, _spotify_cooldown_seconds_remaining())
    return f"{prefix} Try again in about {remaining} seconds."


async def _fetch_spotify_profile(access_token: str) -> dict[str, Any]:
    _enforce_spotify_cooldown()
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(
            settings.spotify_me_url,
            headers={"Authorization": f"Bearer {access_token}"},
        )

    if response.status_code == status.HTTP_401_UNAUTHORIZED:
        raise HTTPException(status_code=401, detail="Spotify access token is no longer valid.")
    if response.status_code == status.HTTP_403_FORBIDDEN:
        detail = ""
        try:
            payload = response.json()
            detail = payload.get("error", {}).get("message") or payload.get("error_description") or ""
        except ValueError:
            detail = response.text[:160]
        raise HTTPException(
            status_code=403,
            detail=f"Spotify profile access was denied{f': {detail}' if detail else ''}.",
        )
    if response.status_code == status.HTTP_429_TOO_MANY_REQUESTS:
        retry_after_header = response.headers.get("Retry-After")
        retry_after_seconds = _parse_retry_after_seconds(retry_after_header)
        _note_spotify_rate_limit(retry_after_seconds)
        detail = ""
        try:
            payload = response.json()
            detail = payload.get("error", {}).get("message") or payload.get("error_description") or ""
        except ValueError:
            detail = response.text[:160]
        raise HTTPException(
            status_code=429,
            detail=_spotify_rate_limit_detail(
                f"Spotify rate limit reached while fetching your profile{f': {detail}' if detail else ''}.",
            ),
        )
    if response.status_code >= 400:
        detail = ""
        try:
            payload = response.json()
            detail = payload.get("error", {}).get("message") or payload.get("error_description") or ""
        except ValueError:
            detail = response.text[:160]
        raise HTTPException(
            status_code=502,
            detail=f"Failed to fetch Spotify profile (status {response.status_code}){f': {detail}' if detail else ''}.",
        )

    return response.json()


async def _spotify_get(access_token: str, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    _enforce_spotify_cooldown()
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(
            url,
            params=params,
            headers={"Authorization": f"Bearer {access_token}"},
        )

    if response.status_code == status.HTTP_401_UNAUTHORIZED:
        raise HTTPException(status_code=401, detail="Spotify access token is no longer valid.")
    if response.status_code == status.HTTP_403_FORBIDDEN:
        raise HTTPException(status_code=403, detail="Spotify scope is missing for this resource.")
    if response.status_code == status.HTTP_429_TOO_MANY_REQUESTS:
        retry_after_header = response.headers.get("Retry-After")
        retry_after_seconds = _parse_retry_after_seconds(retry_after_header)
        _note_spotify_rate_limit(retry_after_seconds)
        raise HTTPException(status_code=429, detail=_spotify_rate_limit_detail("Spotify rate limit reached for this resource."))
    if response.status_code >= 400:
        detail = ""
        try:
            payload = response.json()
            detail = payload.get("error_description") or payload.get("error", {}).get("message") or ""
        except ValueError:
            detail = response.text[:160]
        raise HTTPException(
            status_code=502,
            detail=f"Failed to fetch Spotify data from {url} (status {response.status_code}){f': {detail}' if detail else ''}",
        )

    return response.json()


async def _spotify_get_many(access_token: str, url: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    _enforce_spotify_cooldown()
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(
            url,
            params=params,
            headers={"Authorization": f"Bearer {access_token}"},
        )

    if response.status_code == status.HTTP_401_UNAUTHORIZED:
        raise HTTPException(status_code=401, detail="Spotify access token is no longer valid.")
    if response.status_code == status.HTTP_403_FORBIDDEN:
        raise HTTPException(status_code=403, detail="Spotify scope is missing for this resource.")
    if response.status_code == status.HTTP_429_TOO_MANY_REQUESTS:
        retry_after_header = response.headers.get("Retry-After")
        retry_after_seconds = _parse_retry_after_seconds(retry_after_header)
        _note_spotify_rate_limit(retry_after_seconds)
        raise HTTPException(status_code=429, detail=_spotify_rate_limit_detail("Spotify rate limit reached for this resource."))
    if response.status_code >= 400:
        detail = ""
        try:
            payload = response.json()
            detail = payload.get("error_description") or payload.get("error", {}).get("message") or ""
        except ValueError:
            detail = response.text[:160]
        raise HTTPException(
            status_code=502,
            detail=f"Failed to fetch Spotify data from {url} (status {response.status_code}){f': {detail}' if detail else ''}",
        )

    payload = response.json()
    return payload.get("artists") or []


async def _fetch_recent_tracks(access_token: str, limit: int) -> tuple[list[dict[str, Any]], bool]:
    try:
        payload = await _spotify_get(
            access_token,
            "https://api.spotify.com/v1/me/player/recently-played",
            {"limit": min(50, max(limit * 4, limit))},
        )
    except HTTPException as exc:
        if exc.status_code == status.HTTP_403_FORBIDDEN:
            return [], False
        raise

    items = payload.get("items") or []
    results: list[dict[str, Any]] = []
    seen_track_ids: set[str] = set()

    for item in items:
        track = item.get("track") or {}
        track_id = track.get("id")
        if not track_id:
            continue
        if track_id in seen_track_ids:
            continue
        normalized = _normalize_track(track)
        _remember_track_metadata(normalized)
        results.append(normalized)
        seen_track_ids.add(track_id)
        if len(results) >= limit:
            break

    _save_static_metadata_cache(_load_static_metadata_cache())
    return results, True


async def _fetch_owned_playlists(
    access_token: str,
    spotify_user_id: str | None,
    max_items: int | None = None,
) -> tuple[list[dict[str, Any]], bool]:
    if not spotify_user_id:
        return [], False

    results: list[dict[str, Any]] = []
    offset = 0
    limit = 50

    while True:
        try:
            payload = await _spotify_get(
                access_token,
                "https://api.spotify.com/v1/me/playlists",
                {"limit": limit, "offset": offset},
            )
        except HTTPException as exc:
            if exc.status_code == status.HTTP_403_FORBIDDEN:
                return [], False
            raise

        items = payload.get("items") or []
        if not items:
            break

        for item in items:
            owner = item.get("owner") or {}
            if owner.get("id") != spotify_user_id:
                continue
            if not item.get("public"):
                continue

            external_urls = item.get("external_urls") or {}
            items_info = item.get("items") or {}
            tracks = item.get("tracks") or {}
            images = item.get("images") or []

            results.append(
                {
                    "playlist_id": item.get("id"),
                    "name": item.get("name"),
                    "track_count": items_info.get("total", tracks.get("total")),
                    "description": item.get("description"),
                    "is_public": item.get("public"),
                    "url": external_urls.get("spotify"),
                    "image_url": images[0].get("url") if images else None,
                }
            )

        offset += len(items)
        if len(items) < limit:
            break
        if max_items is not None and len(results) >= max_items:
            break

    if max_items is not None:
        results = results[:max_items]

    return results, True


async def _fetch_playlist_tracks(
    access_token: str,
    playlist_id: str,
    limit: int = 5,
) -> list[dict[str, Any]]:
    payload = await _spotify_get(
        access_token,
        f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks",
        {"limit": limit},
    )

    items = payload.get("items") or []
    results: list[dict[str, Any]] = []

    for item in items:
        track = item.get("track") or {}
        if not track.get("id"):
            continue
        results.append(_normalize_track(track))

    return results


async def _fetch_recent_liked_tracks(access_token: str, limit: int) -> tuple[list[dict[str, Any]], bool]:
    try:
        payload = await _spotify_get(
            access_token,
            "https://api.spotify.com/v1/me/tracks",
            {"limit": limit},
        )
    except HTTPException as exc:
        if exc.status_code == status.HTTP_403_FORBIDDEN:
            return [], False
        raise

    items = payload.get("items") or []
    results: list[dict[str, Any]] = []

    for item in items:
        track = item.get("track") or {}
        if not track.get("id"):
            continue
        normalized = _normalize_track(track)
        _remember_track_metadata(normalized)
        results.append(normalized)

    _save_static_metadata_cache(_load_static_metadata_cache())
    return results, True


async def _fetch_followed_artists_total(access_token: str) -> tuple[int | None, bool]:
    try:
        payload = await _spotify_get(
            access_token,
            "https://api.spotify.com/v1/me/following",
            {"type": "artist", "limit": 1},
        )
    except HTTPException as exc:
        if exc.status_code == status.HTTP_403_FORBIDDEN:
            return None, False
        raise

    artists = payload.get("artists") or {}
    return artists.get("total"), True


def _normalize_artist(artist: dict[str, Any]) -> dict[str, Any]:
    images = artist.get("images") or []
    external_urls = artist.get("external_urls") or {}
    followers = artist.get("followers") or {}
    genres = artist.get("genres") or []
    return {
        "artist_id": artist.get("id"),
        "name": artist.get("name"),
        "followers_total": followers.get("total"),
        "genres": genres[:2],
        "popularity": artist.get("popularity"),
        "url": external_urls.get("spotify"),
        "image_url": images[0].get("url") if images else None,
    }


def _artist_enrichment_lookup(artists: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        (artist.get("name") or "").strip().lower(): artist
        for artist in artists
        if artist.get("name")
    }


def _artist_lookup_key(artist_name: str | None) -> str | None:
    if not artist_name:
        return None
    normalized = " ".join(str(artist_name).strip().lower().split())
    return normalized or None


def _normalize_track(track: dict[str, Any]) -> dict[str, Any]:
    album = track.get("album") or {}
    artists = track.get("artists") or []
    external_urls = track.get("external_urls") or {}
    album_external_urls = album.get("external_urls") or {}
    release_date = album.get("release_date")
    return {
        "track_id": track.get("id"),
        "track_name": track.get("name"),
        "artist_name": ", ".join(artist.get("name", "") for artist in artists if artist.get("name")),
        "album_name": album.get("name"),
        "album_release_year": str(release_date)[:4] if release_date else None,
        "uri": track.get("uri"),
        "preview_url": track.get("preview_url"),
        "url": external_urls.get("spotify"),
        "album_url": album_external_urls.get("spotify"),
        "image_url": ((album.get("images") or [{}])[0]).get("url"),
        "album_id": album.get("id"),
        "artists": [
            {
                "artist_id": artist.get("id"),
                "name": artist.get("name"),
            }
            for artist in artists
            if artist.get("name")
        ],
    }


def _album_lookup_key(album_name: str | None, artist_name: str | None) -> str | None:
    if not album_name:
        return None
    album_part = " ".join(str(album_name).strip().lower().split())
    artist_part = " ".join(str(artist_name or "").strip().lower().split())
    if not album_part:
        return None
    return f"{album_part}|||{artist_part}"


def _album_enrichment_lookup(tracks: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for track in tracks:
        album_name = (track.get("album_name") or "").strip()
        artist_name = (track.get("artist_name") or "").strip()
        if not album_name or not artist_name:
            continue
        lookup[(album_name.lower(), artist_name.lower())] = {
            "album_id": track.get("album_id"),
            "url": track.get("album_url") or track.get("url"),
            "image_url": track.get("image_url"),
            "release_year": track.get("album_release_year"),
        }
    return lookup


def _track_identity_key(track_name: str | None, artist_name: str | None) -> str | None:
    if not track_name or not artist_name:
        return None
    track_part = " ".join(str(track_name).strip().lower().split())
    artist_part = " ".join(str(artist_name).strip().lower().split())
    if not track_part or not artist_part:
        return None
    return f"{track_part}|||{artist_part}"


def _track_enrichment_lookup(tracks: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for track in tracks:
        key = _track_identity_key(track.get("track_name"), track.get("artist_name"))
        if not key:
            continue
        lookup[key] = track
    return lookup


def _merge_history_tracks(
    history_tracks: list[dict[str, Any]],
    enrichment_lookup: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for track in history_tracks:
        key = _track_identity_key(track.get("track_name"), track.get("artist_name"))
        enriched = enrichment_lookup.get(key, {}) if key else {}
        results.append(
            {
                **track,
                "track_id": track.get("track_id") or enriched.get("track_id"),
                "album_release_year": track.get("album_release_year") or enriched.get("album_release_year"),
                "url": track.get("url") or enriched.get("url"),
                "album_url": track.get("album_url") or enriched.get("album_url"),
                "image_url": track.get("image_url") or enriched.get("image_url"),
            }
        )
    return results


async def _enrich_tracks_from_spotify(
    access_token: str,
    tracks: list[dict[str, Any]],
    market: str | None = None,
) -> list[dict[str, Any]]:
    track_ids = list(
        {
            str(track.get("track_id"))
            for track in tracks
            if track.get("track_id")
        }
    )
    enrichment_by_id: dict[str, dict[str, Any]] = {}
    for track_id in track_ids:
        cached = _static_metadata_get("tracks_by_id", track_id)
        if isinstance(cached, dict):
            enrichment_by_id[track_id] = cached

    if track_ids:
        for start in range(0, len(track_ids), 50):
            batch = track_ids[start : start + 50]
            batch_missing = [track_id for track_id in batch if track_id not in enrichment_by_id]
            if not batch_missing:
                continue
            params: dict[str, Any] = {"ids": ",".join(batch_missing)}
            if market:
                params["market"] = market
            try:
                payload = await _spotify_get(
                    access_token,
                    "https://api.spotify.com/v1/tracks",
                    params,
                )
            except HTTPException as exc:
                if exc.status_code in {status.HTTP_403_FORBIDDEN, status.HTTP_429_TOO_MANY_REQUESTS}:
                    continue
                raise

            for item in payload.get("tracks") or []:
                if not item or not item.get("id"):
                    continue
                normalized = _normalize_track(item)
                item_id = str(item["id"])
                enrichment_by_id[item_id] = normalized
                _static_metadata_set("tracks_by_id", item_id, normalized)
                key = _track_identity_key(normalized.get("track_name"), normalized.get("artist_name"))
                if key:
                    _static_metadata_set("tracks_by_key", key, normalized)

    results: list[dict[str, Any]] = []
    for track in tracks:
        track_id = str(track.get("track_id") or "")
        track_key = _track_identity_key(track.get("track_name"), track.get("artist_name"))
        enriched_candidate = enrichment_by_id.get(track_id, {}) or (
            _static_metadata_get("tracks_by_key", track_key) if track_key else {}
        )
        enriched = enriched_candidate if isinstance(enriched_candidate, dict) else {}
        results.append(
            {
                **track,
                "track_name": track.get("track_name") or enriched.get("track_name"),
                "artist_name": track.get("artist_name") or enriched.get("artist_name"),
                "album_name": track.get("album_name") or enriched.get("album_name"),
                "album_release_year": track.get("album_release_year") or enriched.get("album_release_year"),
                "url": track.get("url") or enriched.get("url"),
                "album_url": track.get("album_url") or enriched.get("album_url"),
                "image_url": track.get("image_url") or enriched.get("image_url"),
                "album_id": track.get("album_id") or enriched.get("album_id"),
                "uri": track.get("uri") or enriched.get("uri"),
            }
        )

    # Fallback: when ID-based enrichment misses (or no track_id exists), search by
    # track + artist so we can still recover album art and links.
    final_results: list[dict[str, Any]] = []
    for track in results:
        if track.get("image_url"):
            final_results.append(track)
            continue

        track_name = (track.get("track_name") or "").strip()
        artist_name = (track.get("artist_name") or "").strip()
        if not track_name or not artist_name:
            final_results.append(track)
            continue

        cache_key = _track_identity_key(track_name, artist_name)
        cached = _static_metadata_get("tracks_by_key", cache_key) if cache_key else None
        if isinstance(cached, dict):
            final_results.append(
                {
                    **track,
                    "track_id": track.get("track_id") or cached.get("track_id"),
                    "track_name": track.get("track_name") or cached.get("track_name"),
                    "artist_name": track.get("artist_name") or cached.get("artist_name"),
                    "album_name": track.get("album_name") or cached.get("album_name"),
                    "album_release_year": track.get("album_release_year") or cached.get("album_release_year"),
                    "url": track.get("url") or cached.get("url"),
                    "album_url": track.get("album_url") or cached.get("album_url"),
                    "image_url": track.get("image_url") or cached.get("image_url"),
                    "album_id": track.get("album_id") or cached.get("album_id"),
                    "uri": track.get("uri") or cached.get("uri"),
                }
            )
            continue

        query = f'track:"{track_name}" artist:"{artist_name}"'
        params: dict[str, Any] = {"q": query, "type": "track", "limit": 1}
        if market:
            params["market"] = market
        try:
            payload = await _spotify_get(
                access_token,
                "https://api.spotify.com/v1/search",
                params,
            )
        except HTTPException as exc:
            if exc.status_code in {status.HTTP_403_FORBIDDEN, status.HTTP_429_TOO_MANY_REQUESTS}:
                final_results.append(track)
                continue
            raise

        items = ((payload.get("tracks") or {}).get("items")) or []
        if not items:
            final_results.append(track)
            continue

        enriched = _normalize_track(items[0])
        if enriched.get("track_id"):
            _static_metadata_set("tracks_by_id", str(enriched["track_id"]), enriched)
        if cache_key:
            _static_metadata_set("tracks_by_key", cache_key, enriched)
        final_results.append(
            {
                **track,
                "track_id": track.get("track_id") or enriched.get("track_id"),
                "track_name": track.get("track_name") or enriched.get("track_name"),
                "artist_name": track.get("artist_name") or enriched.get("artist_name"),
                "album_name": track.get("album_name") or enriched.get("album_name"),
                "album_release_year": track.get("album_release_year") or enriched.get("album_release_year"),
                "url": track.get("url") or enriched.get("url"),
                "album_url": track.get("album_url") or enriched.get("album_url"),
                "image_url": track.get("image_url") or enriched.get("image_url"),
                "album_id": track.get("album_id") or enriched.get("album_id"),
                "uri": track.get("uri") or enriched.get("uri"),
            }
        )

    _save_static_metadata_cache(_load_static_metadata_cache())

    return final_results


def _apply_track_history_metrics(
    tracks: list[dict[str, Any]],
    metrics_by_uri: dict[str, dict[str, Any]] | None,
    metrics_by_key: dict[str, dict[str, Any]] | None = None,
    *,
    count_key: str = "play_count",
) -> list[dict[str, Any]]:
    if not metrics_by_uri and not metrics_by_key:
        return tracks

    for track in tracks:
        uri = track.get("uri")
        metrics_uri = metrics_by_uri.get(uri) if metrics_by_uri and uri else None
        key = _track_identity_key(track.get("track_name"), track.get("artist_name"))
        metrics_key = metrics_by_key.get(key) if metrics_by_key and key else None
        metrics = metrics_uri
        if metrics_key and (
            metrics is None
            or float(metrics_key.get("longevity_score", 0.0) or 0.0) > float(metrics.get("longevity_score", 0.0) or 0.0)
            or int(metrics_key.get(count_key, 0) or 0) > int(metrics.get(count_key, 0) or 0)
        ):
            metrics = metrics_key
        if not metrics:
            continue
        track["play_count"] = int(metrics.get(count_key, 0) or 0)
        track["all_time_play_count"] = int(metrics.get("play_count", 0) or 0)
        track["recent_play_count"] = int(metrics.get("recent_play_count", 0) or 0)
        track["first_played_at"] = metrics.get("first_played_at")
        track["last_played_at"] = metrics.get("last_played_at")
        track["listening_span_days"] = int(metrics.get("listening_span_days", 0) or 0)
        track["listening_span_years"] = float(metrics.get("listening_span_years", 0.0) or 0.0)
        track["active_months_count"] = int(metrics.get("active_months_count", 0) or 0)
        track["span_months_count"] = int(metrics.get("span_months_count", 0) or 0)
        track["consistency_ratio"] = float(metrics.get("consistency_ratio", 0.0) or 0.0)
        track["longevity_score"] = float(metrics.get("longevity_score", 0.0) or 0.0)
    return tracks


def _normalized_max(value: float, max_value: float) -> float:
    if max_value <= 0:
        return 0.0
    return value / max_value


def _merge_history_artists(
    history_artists: list[dict[str, Any]],
    enrichment_lookup: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for artist in history_artists:
        enriched = enrichment_lookup.get((artist.get("name") or "").strip().lower(), {})
        results.append(
            {
                **artist,
                "artist_id": enriched.get("artist_id", artist.get("artist_id")),
                "followers_total": enriched.get("followers_total", artist.get("followers_total")),
                "genres": enriched.get("genres", artist.get("genres") or []),
                "popularity": enriched.get("popularity", artist.get("popularity")),
                "url": enriched.get("url", artist.get("url")),
                "image_url": enriched.get("image_url", artist.get("image_url")),
            }
        )
    return results


def _merge_history_albums(
    history_albums: list[dict[str, Any]],
    enrichment_lookup: dict[tuple[str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for album in history_albums:
        key = (
            (album.get("name") or "").strip().lower(),
            (album.get("artist_name") or "").strip().lower(),
        )
        enriched = enrichment_lookup.get(key, {})
        results.append(
            {
                **album,
                "album_id": enriched.get("album_id", album.get("album_id")),
                "url": enriched.get("url", album.get("url")),
                "image_url": enriched.get("image_url", album.get("image_url")),
                "release_year": enriched.get("release_year", album.get("release_year")),
            }
        )
    return results


def _hydrate_artists_from_static_cache(artists: list[dict[str, Any]]) -> list[dict[str, Any]]:
    hydrated: list[dict[str, Any]] = []
    for artist in artists:
        artist_key = _artist_lookup_key(artist.get("name"))
        cached = _static_metadata_get("artists_by_name", artist_key) if artist_key else None
        if isinstance(cached, dict):
            hydrated.append(
                {
                    **artist,
                    "artist_id": artist.get("artist_id") or cached.get("artist_id"),
                    "followers_total": artist.get("followers_total") or cached.get("followers_total"),
                    "genres": artist.get("genres") or cached.get("genres") or [],
                    "popularity": artist.get("popularity") if artist.get("popularity") is not None else cached.get("popularity"),
                    "url": artist.get("url") or cached.get("url"),
                    "image_url": artist.get("image_url") or cached.get("image_url"),
                }
            )
        else:
            hydrated.append(artist)
    return hydrated


def _hydrate_albums_from_static_cache(albums: list[dict[str, Any]]) -> list[dict[str, Any]]:
    hydrated: list[dict[str, Any]] = []
    for album in albums:
        album_key = _album_lookup_key(album.get("name"), album.get("artist_name"))
        cached = _static_metadata_get("albums_by_key", album_key) if album_key else None
        if isinstance(cached, dict):
            hydrated.append(
                {
                    **album,
                    "album_id": album.get("album_id") or cached.get("album_id"),
                    "url": album.get("url") or cached.get("url"),
                    "image_url": album.get("image_url") or cached.get("image_url"),
                    "release_year": album.get("release_year") or cached.get("release_year"),
                }
            )
        else:
            hydrated.append(album)
    return hydrated


def _merge_artists_from_snapshot(
    artists: list[dict[str, Any]],
    snapshot_artists: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    if not snapshot_artists:
        return artists
    lookup = {
        _artist_lookup_key(artist.get("name")): artist
        for artist in snapshot_artists
        if _artist_lookup_key(artist.get("name"))
    }
    merged: list[dict[str, Any]] = []
    for artist in artists:
        cached = lookup.get(_artist_lookup_key(artist.get("name")))
        if not isinstance(cached, dict):
            merged.append(artist)
            continue
        merged.append(
            {
                **artist,
                "artist_id": artist.get("artist_id") or cached.get("artist_id"),
                "followers_total": artist.get("followers_total") or cached.get("followers_total"),
                "genres": artist.get("genres") or cached.get("genres") or [],
                "popularity": artist.get("popularity") if artist.get("popularity") is not None else cached.get("popularity"),
                "url": artist.get("url") or cached.get("url"),
                "image_url": artist.get("image_url") or cached.get("image_url"),
            }
        )
    return merged


def _merge_albums_from_snapshot(
    albums: list[dict[str, Any]],
    snapshot_albums: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    if not snapshot_albums:
        return albums
    lookup = {
        _album_lookup_key(album.get("name"), album.get("artist_name")): album
        for album in snapshot_albums
        if _album_lookup_key(album.get("name"), album.get("artist_name"))
    }
    merged: list[dict[str, Any]] = []
    for album in albums:
        cached = lookup.get(_album_lookup_key(album.get("name"), album.get("artist_name")))
        if not isinstance(cached, dict):
            merged.append(album)
            continue
        merged.append(
            {
                **album,
                "album_id": album.get("album_id") or cached.get("album_id"),
                "url": album.get("url") or cached.get("url"),
                "image_url": album.get("image_url") or cached.get("image_url"),
                "release_year": album.get("release_year") or cached.get("release_year"),
            }
        )
    return merged


def _prefer_snapshot_list_if_richer(
    current_items: list[dict[str, Any]],
    snapshot_items: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    if not current_items or not snapshot_items:
        return current_items
    current_image_count = sum(1 for item in current_items if item.get("image_url"))
    snapshot_image_count = sum(1 for item in snapshot_items if item.get("image_url"))
    if snapshot_image_count <= current_image_count:
        return current_items
    if snapshot_image_count < max(1, len(snapshot_items) // 2):
        return current_items
    return list(snapshot_items[: len(current_items)])


async def _enrich_history_artists_from_search(
    access_token: str,
    artists: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for artist in artists:
        if artist.get("image_url") and artist.get("url"):
            results.append(artist)
            continue

        artist_name = (artist.get("name") or "").strip()
        if not artist_name:
            results.append(artist)
            continue
        artist_key = _artist_lookup_key(artist_name)
        cached = _static_metadata_get("artists_by_name", artist_key) if artist_key else None
        if isinstance(cached, dict):
            results.append(
                {
                    **artist,
                    "artist_id": cached.get("artist_id", artist.get("artist_id")),
                    "followers_total": cached.get("followers_total", artist.get("followers_total")),
                    "genres": cached.get("genres") or artist.get("genres") or [],
                    "popularity": cached.get("popularity") if cached.get("popularity") is not None else artist.get("popularity"),
                    "url": cached.get("url") or artist.get("url"),
                    "image_url": cached.get("image_url") or artist.get("image_url"),
                }
            )
            continue

        try:
            payload = await _spotify_get(
                access_token,
                "https://api.spotify.com/v1/search",
                {"q": f'artist:"{artist_name}"', "type": "artist", "limit": 1},
            )
        except HTTPException as exc:
            if exc.status_code in {status.HTTP_403_FORBIDDEN, status.HTTP_429_TOO_MANY_REQUESTS}:
                results.append(artist)
                continue
            raise

        items = ((payload.get("artists") or {}).get("items")) or []
        if not items:
            results.append(artist)
            continue

        match = items[0]
        images = match.get("images") or []
        external_urls = match.get("external_urls") or {}
        followers = match.get("followers") or {}
        enriched = {
            "artist_id": match.get("id") or artist.get("artist_id"),
            "followers_total": followers.get("total") or artist.get("followers_total"),
            "genres": (match.get("genres") or [])[:2] or artist.get("genres") or [],
            "popularity": match.get("popularity") if match.get("popularity") is not None else artist.get("popularity"),
            "url": external_urls.get("spotify") or artist.get("url"),
            "image_url": images[0].get("url") if images else artist.get("image_url"),
        }
        if artist_key:
            _static_metadata_set("artists_by_name", artist_key, enriched)
        results.append({**artist, **enriched})

    _save_static_metadata_cache(_load_static_metadata_cache())
    return results


async def _enrich_history_albums_from_search(
    access_token: str,
    albums: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for album in albums:
        if album.get("image_url") and album.get("url"):
            results.append(album)
            continue

        album_name = (album.get("name") or "").strip()
        artist_name = (album.get("artist_name") or "").strip()
        if not album_name:
            results.append(album)
            continue
        album_key = _album_lookup_key(album_name, artist_name)
        cached = _static_metadata_get("albums_by_key", album_key) if album_key else None
        if isinstance(cached, dict):
            results.append(
                {
                    **album,
                    "album_id": cached.get("album_id", album.get("album_id")),
                    "url": cached.get("url") or album.get("url"),
                    "image_url": cached.get("image_url") or album.get("image_url"),
                    "release_year": cached.get("release_year") or album.get("release_year"),
                }
            )
            continue

        query = f'album:"{album_name}"'
        if artist_name:
            query += f' artist:"{artist_name}"'

        try:
            payload = await _spotify_get(
                access_token,
                "https://api.spotify.com/v1/search",
                {"q": query, "type": "album", "limit": 1},
            )
        except HTTPException as exc:
            if exc.status_code in {status.HTTP_403_FORBIDDEN, status.HTTP_429_TOO_MANY_REQUESTS}:
                results.append(album)
                continue
            raise

        items = ((payload.get("albums") or {}).get("items")) or []
        if not items:
            results.append(album)
            continue

        match = items[0]
        images = match.get("images") or []
        external_urls = match.get("external_urls") or {}
        enriched = {
            "album_id": match.get("id") or album.get("album_id"),
            "url": external_urls.get("spotify") or album.get("url"),
            "image_url": images[0].get("url") if images else album.get("image_url"),
            "release_year": (match.get("release_date") or "")[:4] or album.get("release_year"),
        }
        if album_key:
            _static_metadata_set("albums_by_key", album_key, enriched)
        results.append({**album, **enriched})

    _save_static_metadata_cache(_load_static_metadata_cache())
    return results


async def _backfill_artist_album_images_if_needed(
    access_token: str,
    artists: list[dict[str, Any]],
    albums: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    needs_artist_backfill = any(not (artist.get("image_url") and artist.get("url")) for artist in artists)
    needs_album_backfill = any(not (album.get("image_url") and album.get("url")) for album in albums)
    if not needs_artist_backfill and not needs_album_backfill:
        return artists, albums

    next_artists = artists
    next_albums = albums
    try:
        if needs_artist_backfill:
            next_artists = await _enrich_history_artists_from_search(access_token, artists)
        if needs_album_backfill:
            next_albums = await _enrich_history_albums_from_search(access_token, albums)
    except HTTPException as exc:
        if exc.status_code in {
            status.HTTP_403_FORBIDDEN,
            status.HTTP_429_TOO_MANY_REQUESTS,
            status.HTTP_502_BAD_GATEWAY,
        }:
            return artists, albums
        raise

    return next_artists, next_albums


async def _backfill_album_images_if_needed(
    access_token: str,
    albums: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    needs_album_backfill = any(not (album.get("image_url") and album.get("url")) for album in albums)
    if not needs_album_backfill:
        return albums

    try:
        return await _enrich_history_albums_from_search(access_token, albums)
    except HTTPException as exc:
        if exc.status_code in {
            status.HTTP_403_FORBIDDEN,
            status.HTTP_429_TOO_MANY_REQUESTS,
            status.HTTP_502_BAD_GATEWAY,
        }:
            return albums
        raise


def _normalize_live_top_artists(
    long_term_top_tracks: list[dict[str, Any]],
    recent_top_tracks: list[dict[str, Any]],
    recent_tracks: list[dict[str, Any]],
    liked_tracks: list[dict[str, Any]],
    enrichment_lookup: dict[str, dict[str, Any]],
    mode: str,
) -> list[dict[str, Any]]:
    aggregates: dict[str, dict[str, Any]] = {}

    def ensure_artist(artist_id: str | None, artist_name: str | None) -> dict[str, Any] | None:
        if not artist_name:
            return None
        key = (artist_id or artist_name).strip().lower()
        entry = aggregates.get(key)
        if entry is None:
            entry = {
                "artist_id": artist_id,
                "name": artist_name,
                "long_top_track_ids": set(),
                "recent_top_track_ids": set(),
                "liked_track_ids": set(),
                "recent_track_ids": set(),
                "recent_play_count": 0,
                "long_rank_weight": 0.0,
                "recent_rank_weight": 0.0,
            }
            aggregates[key] = entry
        return entry

    def track_artists(track: dict[str, Any]) -> list[dict[str, Any]]:
        return track.get("artists") or []

    def apply_top_tracks(tracks: list[dict[str, Any]], target_key: str, weight_key: str) -> None:
        total = len(tracks) or 1
        for index, track in enumerate(tracks):
            track_id = track.get("track_id")
            rank_weight = (total - index) / total
            for artist in track_artists(track):
                entry = ensure_artist(artist.get("artist_id"), artist.get("name"))
                if not entry or not track_id:
                    continue
                entry[target_key].add(track_id)
                entry[weight_key] += rank_weight

    apply_top_tracks(long_term_top_tracks, "long_top_track_ids", "long_rank_weight")
    apply_top_tracks(recent_top_tracks, "recent_top_track_ids", "recent_rank_weight")

    for track in liked_tracks:
        track_id = track.get("track_id")
        if not track_id:
            continue
        for artist in track_artists(track):
            entry = ensure_artist(artist.get("artist_id"), artist.get("name"))
            if entry:
                entry["liked_track_ids"].add(track_id)

    for track in recent_tracks:
        track_id = track.get("track_id")
        if not track_id:
            continue
        for artist in track_artists(track):
            entry = ensure_artist(artist.get("artist_id"), artist.get("name"))
            if not entry:
                continue
            entry["recent_track_ids"].add(track_id)
            entry["recent_play_count"] += 1

    if not aggregates:
        return []

    max_long_top = max(len(item["long_top_track_ids"]) for item in aggregates.values())
    max_recent_top = max(len(item["recent_top_track_ids"]) for item in aggregates.values())
    max_liked = max(len(item["liked_track_ids"]) for item in aggregates.values())
    max_recent_distinct = max(len(item["recent_track_ids"]) for item in aggregates.values())
    max_recent_play_count = max(item["recent_play_count"] for item in aggregates.values())
    max_long_rank_weight = max(item["long_rank_weight"] for item in aggregates.values())
    max_recent_rank_weight = max(item["recent_rank_weight"] for item in aggregates.values())

    results: list[dict[str, Any]] = []
    for item in aggregates.values():
        long_top_track_count_norm = _normalized_max(len(item["long_top_track_ids"]), max_long_top)
        recent_top_track_count_norm = _normalized_max(len(item["recent_top_track_ids"]), max_recent_top)
        liked_track_count_norm = _normalized_max(len(item["liked_track_ids"]), max_liked)
        recent_distinct_tracks_norm = _normalized_max(len(item["recent_track_ids"]), max_recent_distinct)
        recent_play_count_norm = _normalized_max(item["recent_play_count"], max_recent_play_count)
        long_rank_weight_norm = _normalized_max(item["long_rank_weight"], max_long_rank_weight)
        recent_rank_weight_norm = _normalized_max(item["recent_rank_weight"], max_recent_rank_weight)

        if mode == "recent":
            score = (
                recent_rank_weight_norm * 0.30
                + recent_distinct_tracks_norm * 0.25
                + recent_play_count_norm * 0.20
                + recent_top_track_count_norm * 0.15
                + liked_track_count_norm * 0.10
            )
        else:
            score = (
                long_rank_weight_norm * 0.32
                + long_top_track_count_norm * 0.23
                + liked_track_count_norm * 0.18
                + recent_rank_weight_norm * 0.12
                + recent_distinct_tracks_norm * 0.10
                + recent_play_count_norm * 0.05
            )

        enriched = enrichment_lookup.get((item["name"] or "").strip().lower(), {})
        results.append(
            {
                "artist_id": enriched.get("artist_id", item["artist_id"]),
                "name": item["name"],
                "followers_total": enriched.get("followers_total"),
                "genres": enriched.get("genres") or [],
                "popularity": enriched.get("popularity"),
                "url": enriched.get("url"),
                "image_url": enriched.get("image_url"),
                "debug": {
                    "source": "live_formula",
                    "score": round(score, 4),
                    "long_top_track_count_norm": round(long_top_track_count_norm, 4),
                    "recent_top_track_count_norm": round(recent_top_track_count_norm, 4),
                    "liked_track_count_norm": round(liked_track_count_norm, 4),
                    "recent_distinct_tracks_norm": round(recent_distinct_tracks_norm, 4),
                    "recent_play_count_norm": round(recent_play_count_norm, 4),
                    "long_rank_weight_norm": round(long_rank_weight_norm, 4),
                    "recent_rank_weight_norm": round(recent_rank_weight_norm, 4),
                },
            }
        )

    return sorted(
        results,
        key=lambda artist: (
            -artist["debug"]["score"],
            -artist["debug"]["recent_distinct_tracks_norm"],
            -artist["debug"]["recent_play_count_norm"],
            artist["name"] or "",
        ),
    )[:SECTION_PREVIEW_LIMIT]


def _normalize_live_top_albums(
    long_term_top_tracks: list[dict[str, Any]],
    recent_top_tracks: list[dict[str, Any]],
    recent_tracks: list[dict[str, Any]],
    liked_tracks: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    aggregates: dict[str, dict[str, Any]] = {}

    def album_track_key(track: dict[str, Any]) -> str | None:
        track_name = track.get("track_name")
        if track_name:
            normalized = " ".join(str(track_name).strip().lower().split())
            if normalized:
                return normalized
        track_id = track.get("track_id")
        return str(track_id) if track_id else None

    def album_key(track: dict[str, Any]) -> str | None:
        album_id = track.get("album_id")
        album_name = track.get("album_name")
        artist_name = track.get("artist_name")
        if album_id:
            return album_id
        if album_name and artist_name:
            return f"{album_name.strip().lower()}::{artist_name.strip().lower()}"
        return None

    def ensure_album(track: dict[str, Any]) -> dict[str, Any] | None:
        key = album_key(track)
        if not key:
            return None
        entry = aggregates.get(key)
        if entry is None:
            entry = {
                "album_id": track.get("album_id"),
                "name": track.get("album_name"),
                "artist_name": track.get("artist_name"),
                "release_year": track.get("album_release_year"),
                "url": track.get("album_url") or track.get("url"),
                "image_url": track.get("image_url"),
                "long_track_ids": set(),
                "recent_track_ids": set(),
                "liked_track_ids": set(),
                "recent_play_count": 0,
                "long_rank_weight": 0.0,
                "recent_rank_weight": 0.0,
                "track_name_scores": {},
            }
            aggregates[key] = entry
        return entry

    def add_top_tracks(tracks: list[dict[str, Any]], target_key: str, weight_key: str) -> None:
        total = len(tracks) or 1
        for index, track in enumerate(tracks):
            track_id = album_track_key(track)
            entry = ensure_album(track)
            if not entry or not track_id:
                continue
            rank_weight = (total - index) / total
            entry[target_key].add(track_id)
            entry[weight_key] += rank_weight
            track_name = track.get("track_name")
            if track_name:
                entry["track_name_scores"][track_name] = entry["track_name_scores"].get(track_name, 0.0) + rank_weight

    add_top_tracks(long_term_top_tracks, "long_track_ids", "long_rank_weight")
    add_top_tracks(recent_top_tracks, "recent_track_ids", "recent_rank_weight")

    for track in liked_tracks:
        track_id = album_track_key(track)
        entry = ensure_album(track)
        if not entry or not track_id:
            continue
        entry["liked_track_ids"].add(track_id)
        track_name = track.get("track_name")
        if track_name:
            entry["track_name_scores"][track_name] = entry["track_name_scores"].get(track_name, 0.0) + 0.5

    for track in recent_tracks:
        track_id = album_track_key(track)
        entry = ensure_album(track)
        if not entry or not track_id:
            continue
        entry["recent_track_ids"].add(track_id)
        entry["recent_play_count"] += 1
        track_name = track.get("track_name")
        if track_name:
            entry["track_name_scores"][track_name] = entry["track_name_scores"].get(track_name, 0.0) + 0.25

    if not aggregates:
        return [], []

    max_long_distinct = max(len(item["long_track_ids"]) for item in aggregates.values())
    max_recent_distinct = max(len(item["recent_track_ids"]) for item in aggregates.values())
    max_liked = max(len(item["liked_track_ids"]) for item in aggregates.values())
    max_long_rank_weight = max(item["long_rank_weight"] for item in aggregates.values())
    max_recent_rank_weight = max(item["recent_rank_weight"] for item in aggregates.values())
    max_recent_play_count = max(item["recent_play_count"] for item in aggregates.values())

    def build_results(mode: str) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for item in aggregates.values():
            long_distinct_norm = _normalized_max(len(item["long_track_ids"]), max_long_distinct)
            recent_distinct_norm = _normalized_max(len(item["recent_track_ids"]), max_recent_distinct)
            liked_norm = _normalized_max(len(item["liked_track_ids"]), max_liked)
            long_rank_norm = _normalized_max(item["long_rank_weight"], max_long_rank_weight)
            recent_rank_norm = _normalized_max(item["recent_rank_weight"], max_recent_rank_weight)
            recent_play_count_norm = _normalized_max(item["recent_play_count"], max_recent_play_count)

            if mode == "recent":
                score = (
                    recent_distinct_norm * 0.34
                    + recent_play_count_norm * 0.24
                    + recent_rank_norm * 0.18
                    + liked_norm * 0.14
                    + long_distinct_norm * 0.10
                )
                track_representation_count = len(item["recent_track_ids"])
            else:
                score = (
                    long_distinct_norm * 0.34
                    + long_rank_norm * 0.24
                    + liked_norm * 0.18
                    + recent_distinct_norm * 0.14
                    + recent_play_count_norm * 0.10
                )
                track_representation_count = len(item["long_track_ids"])

            minimum_distinct_tracks = MIN_RECENT_ALBUM_DISTINCT_TRACKS if mode == "recent" else MIN_ALBUM_DISTINCT_TRACKS
            if track_representation_count < minimum_distinct_tracks:
                continue

            represented_track_names = [
                name
                for name, _weight in sorted(
                    item["track_name_scores"].items(),
                    key=lambda pair: (-pair[1], pair[0].lower()),
                )[:3]
            ]
            results.append(
                {
                    "album_id": item["album_id"],
                    "name": item["name"],
                    "artist_name": item["artist_name"],
                    "release_year": item.get("release_year"),
                    "url": item["url"],
                    "image_url": item["image_url"],
                    "track_representation_count": track_representation_count,
                    "rank_score": round(recent_rank_norm if mode == "recent" else long_rank_norm, 4),
                    "album_score": round(score, 4),
                    "represented_track_names": represented_track_names,
                    "debug": {
                        "source": "live_formula",
                        "score": round(score, 4),
                        "long_distinct_tracks_norm": round(long_distinct_norm, 4),
                        "recent_distinct_tracks_norm": round(recent_distinct_norm, 4),
                        "liked_tracks_on_album_norm": round(liked_norm, 4),
                        "long_rank_weight_on_album_norm": round(long_rank_norm, 4),
                        "recent_rank_weight_on_album_norm": round(recent_rank_norm, 4),
                        "recent_play_count_on_album_norm": round(recent_play_count_norm, 4),
                    },
                }
            )

        ranked_results = sorted(
            results,
            key=lambda album: (
                -album["debug"]["score"],
                -album["track_representation_count"],
                album["name"] or "",
            ),
        )

        if mode == "recent" and len(ranked_results) < min(SECTION_PREVIEW_LIMIT, 3):
            fallback_results: list[dict[str, Any]] = []
            for item in aggregates.values():
                track_representation_count = len(item["recent_track_ids"])
                if track_representation_count < 1:
                    continue
                long_distinct_norm = _normalized_max(len(item["long_track_ids"]), max_long_distinct)
                recent_distinct_norm = _normalized_max(len(item["recent_track_ids"]), max_recent_distinct)
                liked_norm = _normalized_max(len(item["liked_track_ids"]), max_liked)
                recent_rank_norm = _normalized_max(item["recent_rank_weight"], max_recent_rank_weight)
                recent_play_count_norm = _normalized_max(item["recent_play_count"], max_recent_play_count)
                score = (
                    recent_distinct_norm * 0.34
                    + recent_play_count_norm * 0.24
                    + recent_rank_norm * 0.18
                    + liked_norm * 0.14
                    + long_distinct_norm * 0.10
                )
                represented_track_names = [
                    name
                    for name, _weight in sorted(
                        item["track_name_scores"].items(),
                        key=lambda pair: (-pair[1], pair[0].lower()),
                    )[:3]
                ]
                fallback_results.append(
                    {
                        "album_id": item["album_id"],
                        "name": item["name"],
                        "artist_name": item["artist_name"],
                        "release_year": item.get("release_year"),
                        "url": item["url"],
                        "image_url": item["image_url"],
                        "track_representation_count": track_representation_count,
                        "rank_score": round(recent_rank_norm, 4),
                        "album_score": round(score, 4),
                        "represented_track_names": represented_track_names,
                        "debug": {
                            "source": "live_formula",
                            "score": round(score, 4),
                            "long_distinct_tracks_norm": round(long_distinct_norm, 4),
                            "recent_distinct_tracks_norm": round(recent_distinct_norm, 4),
                            "liked_tracks_on_album_norm": round(liked_norm, 4),
                            "recent_rank_weight_on_album_norm": round(recent_rank_norm, 4),
                            "recent_play_count_on_album_norm": round(recent_play_count_norm, 4),
                            "fallback_breadth_threshold": True,
                        },
                    }
                )
            ranked_results = sorted(
                fallback_results,
                key=lambda album: (
                    -album["debug"]["score"],
                    -album["track_representation_count"],
                    album["name"] or "",
                ),
            )

        return ranked_results[:SECTION_PREVIEW_LIMIT]

    return build_results("long_term"), build_results("recent")


async def _fetch_album_track_ids(
    access_token: str,
    album_id: str,
    max_tracks: int = 300,
) -> set[str]:
    offset = 0
    limit = 50
    track_ids: set[str] = set()

    while offset < max_tracks:
        payload = await _spotify_get(
            access_token,
            f"https://api.spotify.com/v1/albums/{album_id}/tracks",
            {"limit": limit, "offset": offset},
        )
        items = payload.get("items") or []
        if not items:
            break

        for item in items:
            track_id = item.get("id")
            if track_id:
                track_ids.add(track_id)

        offset += len(items)
        if len(items) < limit:
            break

    return track_ids


async def _fetch_album_track_refs(
    access_token: str,
    album_id: str,
    max_tracks: int = 50,
    market: str | None = None,
) -> list[dict[str, Any]]:
    offset = 0
    limit = 50
    tracks: list[dict[str, Any]] = []

    while offset < max_tracks:
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if market:
            params["market"] = market
        payload = await _spotify_get(
            access_token,
            f"https://api.spotify.com/v1/albums/{album_id}/tracks",
            params,
        )
        items = payload.get("items") or []
        if not items:
            break

        tracks.extend(items)
        offset += len(items)
        if len(items) < limit:
            break

    return tracks[:max_tracks]


async def _fetch_tracks_by_ids(
    access_token: str,
    track_ids: list[str],
    market: str | None = None,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for start in range(0, len(track_ids), 50):
        batch = [track_id for track_id in track_ids[start:start + 50] if track_id]
        if not batch:
            continue
        params: dict[str, Any] = {"ids": ",".join(batch)}
        if market:
            params["market"] = market
        payload = await _spotify_get(
            access_token,
            "https://api.spotify.com/v1/tracks",
            params,
        )
        results.extend([track for track in (payload.get("tracks") or []) if track])
    return results


def _choose_representative_track(
    tracks: list[dict[str, Any]],
    album_track_numbers: dict[str, int] | None = None,
) -> dict[str, Any] | None:
    if not tracks:
        return None

    def sort_key(track: dict[str, Any]) -> tuple[int, int, int, str]:
        track_id = track.get("id") or ""
        preview_bonus = 1 if track.get("preview_url") else 0
        popularity = int(track.get("popularity") or 0)
        track_number = 9999
        if album_track_numbers:
            track_number = int(album_track_numbers.get(track_id, track.get("track_number") or 9999))
        else:
            track_number = int(track.get("track_number") or 9999)
        return (preview_bonus, popularity, -track_number, track.get("name") or "")

    return sorted(tracks, key=sort_key, reverse=True)[0]


async def _fetch_artist_representative_track(
    access_token: str,
    artist_id: str,
    market: str | None = None,
) -> dict[str, Any] | None:
    params = {"market": market} if market else None
    payload = await _spotify_get(
        access_token,
        f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks",
        params,
    )
    track = _choose_representative_track(payload.get("tracks") or [])
    return _normalize_track(track) if track else None


async def _fetch_album_representative_track(
    access_token: str,
    album_id: str,
    market: str | None = None,
) -> dict[str, Any] | None:
    album_tracks = await _fetch_album_track_refs(access_token, album_id, market=market)
    ordered_ids = [item.get("id") for item in album_tracks if item.get("id")]
    if not ordered_ids:
        return None

    track_number_lookup = {
        item["id"]: int(item.get("track_number") or 9999)
        for item in album_tracks
        if item.get("id")
    }
    full_tracks = await _fetch_tracks_by_ids(access_token, ordered_ids, market=market)
    track = _choose_representative_track(full_tracks, album_track_numbers=track_number_lookup)
    return _normalize_track(track) if track else None


def _track_weight_map(tracks: list[dict[str, Any]]) -> dict[str, float]:
    total = len(tracks)
    if total == 0:
        return {}

    weights: dict[str, float] = {}
    for index, track in enumerate(tracks):
        track_id = track.get("track_id")
        if not track_id:
            continue
        weights[track_id] = max(weights.get(track_id, 0.0), (total - index) / total)
    return weights


def _album_track_name_map(tracks: list[dict[str, Any]]) -> dict[str, list[str]]:
    names_by_album: dict[str, list[str]] = {}
    for track in tracks:
        album_id = track.get("album_id")
        track_name = track.get("track_name")
        if not album_id or not track_name:
            continue
        album_names = names_by_album.setdefault(album_id, [])
        if track_name not in album_names:
            album_names.append(track_name)
    return names_by_album


def _normalize_top_albums_fallback(
    short_term_top_tracks: list[dict[str, Any]],
    long_term_top_tracks: list[dict[str, Any]],
    recent_tracks: list[dict[str, Any]],
    liked_tracks: list[dict[str, Any]],
    mode: str,
) -> list[dict[str, Any]]:
    albums: dict[str, dict[str, Any]] = {}

    weighted_sources: list[tuple[list[dict[str, Any]], float]] = (
        [
            (recent_tracks, 4.0),
            (short_term_top_tracks, 3.0),
            (liked_tracks, 1.5),
            (long_term_top_tracks, 1.0),
        ]
        if mode == "short_term"
        else [
            (long_term_top_tracks, 4.0),
            (liked_tracks, 2.5),
            (short_term_top_tracks, 1.5),
            (recent_tracks, 1.0),
        ]
    )

    minimum_distinct_tracks = MIN_RECENT_ALBUM_DISTINCT_TRACKS if mode == "short_term" else MIN_ALBUM_DISTINCT_TRACKS

    for tracks, source_weight in weighted_sources:
        for index, track in enumerate(tracks):
            album_id = track.get("album_id")
            if not album_id:
                continue

            entry = albums.get(album_id)
            rank_weight = source_weight * max(1, len(tracks) - index)

            if entry is None:
                entry = {
                    "album_id": album_id,
                    "name": track.get("album_name"),
                    "artist_name": track.get("artist_name"),
                    "url": track.get("album_url") or track.get("url"),
                    "image_url": track.get("image_url"),
                    "track_representation_count": 0,
                    "distinct_track_ids": set(),
                    "rank_score": 0,
                    "album_score": 0,
                    "represented_track_names": [],
                    "debug": {
                        "fallback": True,
                        "total_album_tracks": None,
                    },
                }
                albums[album_id] = entry

            distinct_track_key = (
                " ".join(str(track.get("track_name") or "").strip().lower().split())
                or track.get("track_id")
                or f"{track.get('track_name')}::{track.get('artist_name')}"
            )
            entry["distinct_track_ids"].add(distinct_track_key)
            entry["track_representation_count"] = len(entry["distinct_track_ids"])
            entry["rank_score"] += rank_weight
            entry["album_score"] = entry["track_representation_count"] * 1000 + entry["rank_score"]
            track_name = track.get("track_name")
            if track_name and track_name not in entry["represented_track_names"]:
                entry["represented_track_names"].append(track_name)

    return sorted(
        [
            {
                key: value
                for key, value in album.items()
                if key != "distinct_track_ids"
            }
            for album in albums.values()
            if album["track_representation_count"] >= minimum_distinct_tracks
        ],
        key=lambda album: (
            -album["album_score"],
            -album["track_representation_count"],
            -album["rank_score"],
            album["name"] or "",
        ),
    )[:SECTION_PREVIEW_LIMIT]


def _normalize_top_playlists_fallback(playlists: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    def to_result(playlist: dict[str, Any]) -> dict[str, Any]:
        return {
            "playlist_id": playlist.get("playlist_id"),
            "playlist_name": playlist.get("name"),
            "playlist_url": playlist.get("url"),
            "image_url": playlist.get("image_url"),
            "track_count": playlist.get("track_count"),
            "score": 0.0,
            "match_counts": {
                "short_term_top": 0,
                "long_term_top": 0,
                "recently_played": 0,
                "liked": 0,
                "playlist_size": playlist.get("track_count") or 0,
            },
            "fallback": True,
        }

    all_time_results = [
        to_result(playlist)
        for playlist in sorted(
            playlists,
            key=lambda item: (-(item.get("track_count") or 0), item.get("name") or ""),
        )[:SECTION_PREVIEW_LIMIT]
    ]
    recent_results = [
        to_result(playlist)
        for playlist in playlists[:SECTION_PREVIEW_LIMIT]
    ]
    return recent_results, all_time_results


def _rank_album_candidates(
    short_term_top_tracks: list[dict[str, Any]],
    long_term_top_tracks: list[dict[str, Any]],
    recent_tracks: list[dict[str, Any]],
    liked_tracks: list[dict[str, Any]],
) -> list[str]:
    scores: dict[str, float] = {}

    def add_tracks(tracks: list[dict[str, Any]], weight: float) -> None:
        for index, track in enumerate(tracks):
            album_id = track.get("album_id")
            if not album_id:
                continue
            scores[album_id] = scores.get(album_id, 0.0) + weight / (index + 1)

    add_tracks(short_term_top_tracks, 4.0)
    add_tracks(long_term_top_tracks, 3.0)
    add_tracks(recent_tracks, 2.0)
    add_tracks(liked_tracks, 1.5)

    return [
        album_id
        for album_id, _score in sorted(scores.items(), key=lambda item: (-item[1], item[0]))
    ]


def _normalize_top_albums(
    album_metadata: dict[str, dict[str, Any]],
    album_track_ids: dict[str, set[str]],
    short_term_top_tracks: list[dict[str, Any]],
    long_term_top_tracks: list[dict[str, Any]],
    recent_tracks: list[dict[str, Any]],
    liked_tracks: list[dict[str, Any]],
    mode: str,
) -> list[dict[str, Any]]:
    short_top_ids = {track.get("track_id") for track in short_term_top_tracks if track.get("track_id")}
    long_top_ids = {track.get("track_id") for track in long_term_top_tracks if track.get("track_id")}
    recent_ids = [track.get("track_id") for track in recent_tracks if track.get("track_id")]
    recent_id_set = set(recent_ids)
    liked_ids = {track.get("track_id") for track in liked_tracks if track.get("track_id")}
    short_weight_map = _track_weight_map(short_term_top_tracks)
    long_weight_map = _track_weight_map(long_term_top_tracks)
    represented_track_names = _album_track_name_map(short_term_top_tracks + long_term_top_tracks)

    album_stats: list[dict[str, Any]] = []
    max_recent_play_count = 1
    max_short_intensity = 1.0
    max_long_intensity = 1.0

    for album_id, track_ids in album_track_ids.items():
        if not track_ids:
            continue

        metadata = album_metadata.get(album_id) or {}
        total_album_tracks = len(track_ids)
        short_matches = track_ids & short_top_ids
        long_matches = track_ids & long_top_ids
        recent_matches = track_ids & recent_id_set
        liked_matches = track_ids & liked_ids
        combined_long_breadth_ids = track_ids & (long_top_ids | liked_ids)
        recent_play_count = sum(1 for track_id in recent_ids if track_id in track_ids)
        weighted_short_term_top_presence = sum(short_weight_map.get(track_id, 0.0) for track_id in short_matches)
        weighted_long_term_top_presence = sum(long_weight_map.get(track_id, 0.0) for track_id in long_matches)

        max_recent_play_count = max(max_recent_play_count, recent_play_count)
        max_short_intensity = max(max_short_intensity, weighted_short_term_top_presence)
        max_long_intensity = max(max_long_intensity, weighted_long_term_top_presence)

        album_stats.append(
            {
                "album_id": album_id,
                "name": metadata.get("name"),
                "artist_name": metadata.get("artist_name"),
                "release_year": metadata.get("release_year"),
                "url": metadata.get("url"),
                "image_url": metadata.get("image_url"),
                "track_representation_count": len(long_matches if mode == "long_term" else short_matches),
                "represented_track_names": represented_track_names.get(album_id, []),
                "total_album_tracks": total_album_tracks,
                "recent_breadth": len(recent_matches) / total_album_tracks,
                "liked_breadth": len(liked_matches) / total_album_tracks,
                "top_track_breadth_short": len(short_matches) / total_album_tracks,
                "top_track_breadth_long": len(long_matches) / total_album_tracks,
                "recent_play_count_for_album": recent_play_count,
                "weighted_short_term_top_presence": weighted_short_term_top_presence,
                "weighted_long_term_top_presence": weighted_long_term_top_presence,
                "album_completion_bonus": 1.0 if (len(combined_long_breadth_ids) / total_album_tracks) >= 0.8 else 0.0,
                "debug": {
                    "recent_track_matches": len(recent_matches),
                    "liked_track_matches": len(liked_matches),
                    "short_top_track_matches": len(short_matches),
                    "long_top_track_matches": len(long_matches),
                    "total_album_tracks": total_album_tracks,
                },
            }
        )

    results: list[dict[str, Any]] = []
    minimum_distinct_tracks = MIN_RECENT_ALBUM_DISTINCT_TRACKS if mode == "short_term" else MIN_ALBUM_DISTINCT_TRACKS

    for album in album_stats:
        if album["track_representation_count"] < minimum_distinct_tracks:
            continue
        recent_play_count_normalized = album["recent_play_count_for_album"] / max_recent_play_count
        weighted_short_normalized = album["weighted_short_term_top_presence"] / max_short_intensity
        weighted_long_normalized = album["weighted_long_term_top_presence"] / max_long_intensity

        if mode == "short_term":
            album_score = (
                album["recent_breadth"] * 0.45
                + album["top_track_breadth_short"] * 0.20
                + recent_play_count_normalized * 0.25
                + album["liked_breadth"] * 0.10
            )
        else:
            album_score = (
                album["top_track_breadth_long"] * 0.40
                + album["liked_breadth"] * 0.30
                + weighted_long_normalized * 0.20
                + album["album_completion_bonus"] * 0.10
            )

        results.append(
            {
                **album,
                "album_score": round(album_score, 4),
                "rank_score": round(
                    weighted_short_normalized if mode == "short_term" else weighted_long_normalized,
                    4,
                ),
                "debug": {
                    **album["debug"],
                    "recent_play_count_normalized": round(recent_play_count_normalized, 4),
                    "weighted_short_term_top_presence_normalized": round(weighted_short_normalized, 4),
                    "weighted_long_term_top_presence_normalized": round(weighted_long_normalized, 4),
                    "recent_breadth": round(album["recent_breadth"], 4),
                    "liked_breadth": round(album["liked_breadth"], 4),
                    "top_track_breadth_short": round(album["top_track_breadth_short"], 4),
                    "top_track_breadth_long": round(album["top_track_breadth_long"], 4),
                    "album_completion_bonus": album["album_completion_bonus"],
                },
            }
        )

    return sorted(
        results,
        key=lambda album: (
            -album["album_score"],
            -album["track_representation_count"],
            album["name"] or "",
        ),
    )[:SECTION_PREVIEW_LIMIT]


async def _fetch_top_artists(access_token: str, time_range: str, limit: int) -> tuple[list[dict[str, Any]], bool]:
    try:
        payload = await _spotify_get(
            access_token,
            "https://api.spotify.com/v1/me/top/artists",
            {"limit": limit, "time_range": time_range},
        )
    except HTTPException as exc:
        if exc.status_code == status.HTTP_403_FORBIDDEN:
            return [], False
        raise

    artists_payload = payload.get("items") or []
    artists = [_normalize_artist(artist) for artist in artists_payload]
    for artist in artists:
        _remember_artist_metadata(artist)
    _save_static_metadata_cache(_load_static_metadata_cache())
    return artists, True


async def _fetch_top_tracks(access_token: str, time_range: str, limit: int) -> tuple[list[dict[str, Any]], bool]:
    try:
        payload = await _spotify_get(
            access_token,
            "https://api.spotify.com/v1/me/top/tracks",
            {"limit": limit, "time_range": time_range},
        )
    except HTTPException as exc:
        if exc.status_code == status.HTTP_403_FORBIDDEN:
            return [], False
        raise

    tracks = payload.get("items") or []
    normalized_tracks = [_normalize_track(track) for track in tracks]
    for track in normalized_tracks:
        _remember_track_metadata(track)
    _save_static_metadata_cache(_load_static_metadata_cache())
    return normalized_tracks, True


async def _fetch_playlist_track_ids(
    access_token: str,
    playlist_id: str,
    max_tracks: int = 500,
) -> set[str]:
    offset = 0
    limit = 100
    track_ids: set[str] = set()

    while offset < max_tracks:
        payload = await _spotify_get(
            access_token,
            f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks",
            {"limit": limit, "offset": offset},
        )
        items = payload.get("items") or []
        if not items:
            break

        for item in items:
            track = item.get("track") or {}
            track_id = track.get("id")
            if track_id:
                track_ids.add(track_id)

        offset += len(items)
        if len(items) < limit:
            break

    return track_ids


def _normalize_top_playlists(
    playlists: list[dict[str, Any]],
    playlist_track_ids: dict[str, set[str]],
    short_term_top_track_ids: set[str],
    long_term_top_track_ids: set[str],
    recent_track_ids: set[str],
    liked_track_ids: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    recent_results: list[dict[str, Any]] = []
    long_term_results: list[dict[str, Any]] = []

    candidate_sizes = [
        len(track_ids)
        for playlist in playlists
        if playlist.get("playlist_id") and len(playlist_track_ids.get(playlist["playlist_id"], set())) >= 5
        for track_ids in [playlist_track_ids.get(playlist["playlist_id"], set())]
    ]
    max_playlist_size = max(candidate_sizes, default=1)

    for playlist in playlists:
        playlist_id = playlist.get("playlist_id")
        if not playlist_id:
            continue

        track_ids = playlist_track_ids.get(playlist_id, set())
        playlist_size = len(track_ids)
        if playlist_size < 5:
            continue

        short_matches = len(track_ids & short_term_top_track_ids)
        long_matches = len(track_ids & long_term_top_track_ids)
        recent_matches = len(track_ids & recent_track_ids)
        liked_matches = len(track_ids & liked_track_ids)

        normalized_short = short_matches / playlist_size
        normalized_long = long_matches / playlist_size
        normalized_recent = recent_matches / playlist_size
        normalized_liked = liked_matches / playlist_size
        playlist_size_normalized = min(1.0, playlist_size / max_playlist_size)

        recent_score = (
            normalized_short * 0.5
            + normalized_recent * 0.3
            + normalized_liked * 0.2
        )
        long_term_score = (
            normalized_long * 0.6
            + normalized_liked * 0.3
            + playlist_size_normalized * 0.1
        )

        base_result = {
            "playlist_id": playlist_id,
            "playlist_name": playlist.get("name"),
            "playlist_url": playlist.get("url"),
            "image_url": playlist.get("image_url"),
            "track_count": playlist.get("track_count") or playlist_size,
            "match_counts": {
                "short_term_top": short_matches,
                "long_term_top": long_matches,
                "recently_played": recent_matches,
                "liked": liked_matches,
                "playlist_size": playlist_size,
            },
        }
        recent_results.append({**base_result, "score": round(recent_score, 4)})
        long_term_results.append({**base_result, "score": round(long_term_score, 4)})

    recent_results.sort(key=lambda playlist: (-playlist["score"], -(playlist["match_counts"]["short_term_top"]), playlist["playlist_name"] or ""))
    long_term_results.sort(key=lambda playlist: (-playlist["score"], -(playlist["match_counts"]["long_term_top"]), playlist["playlist_name"] or ""))
    return recent_results[:SECTION_PREVIEW_LIMIT], long_term_results[:SECTION_PREVIEW_LIMIT]


def _build_local_profile_payload(
    mode: str,
    recent_range: str,
    analysis_mode: str,
    username: str | None = None,
    display_name: str | None = None,
    email: str | None = None,
    cached_profile_snapshot: dict[str, Any] | None = None,
    progress_hook: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    def _progress(phase: str) -> None:
        if progress_hook:
            progress_hook(phase)

    is_extended = mode == "extended"
    item_limit = SECTION_PREVIEW_LIMIT if is_extended else INITIAL_DASHBOARD_LIMIT
    recent_window_days = 28 if recent_range == "short_term" else 180
    history_track_limit = max(SECTION_PREVIEW_LIMIT, 50)
    _progress("loading local history signature")
    history_signature = get_history_signature(settings.spotify_history_dir)

    _progress("loading local persistent cache")
    persistent_history_sections = _load_persistent_history_cache(history_signature, recent_window_days)
    _progress("loading precomputed local insights")
    history_insights = _load_local_history_insights_cache(
        history_signature,
        recent_window_days,
        history_track_limit,
    )
    if history_signature and history_insights is None:
        _progress("analyzing local history files")
        history_insights = load_history_insights(
            settings.spotify_history_dir,
            history_track_limit,
            recent_window_days=recent_window_days,
        )
        if history_insights:
            _progress("loading local analysis cache write")
            _store_local_history_insights_cache(
                history_signature,
                recent_window_days,
                history_track_limit,
                history_insights,
            )

    top_tracks_all_time: list[dict[str, Any]] = []
    top_tracks_recent: list[dict[str, Any]] = []
    top_artists_all_time: list[dict[str, Any]] = []
    top_artists_recent: list[dict[str, Any]] = []
    top_albums_all_time: list[dict[str, Any]] = []
    top_albums_recent: list[dict[str, Any]] = []

    _progress("loading local source selection")
    if persistent_history_sections:
        top_tracks_all_time = persistent_history_sections.get("tracks_all_time", [])[:HISTORY_TRACKS_DISPLAY_LIMIT]
        top_tracks_recent = persistent_history_sections.get("tracks_recent", [])[:item_limit]
        top_artists_all_time = persistent_history_sections.get("artists_all_time", [])[:item_limit]
        top_artists_recent = persistent_history_sections.get("artists_recent", [])[:item_limit]
        top_albums_all_time = persistent_history_sections.get("albums_all_time", [])[:item_limit]
        top_albums_recent = persistent_history_sections.get("albums_recent", [])[:item_limit]
    elif history_insights:
        top_tracks_all_time = history_insights.get("tracks_all_time", [])[:HISTORY_TRACKS_DISPLAY_LIMIT]
        top_tracks_recent = history_insights.get("tracks_recent", [])[:item_limit]
        top_artists_all_time = history_insights.get("artists_all_time", [])[:item_limit]
        top_artists_recent = history_insights.get("artists_recent", [])[:item_limit]
        top_albums_all_time = history_insights.get("albums_all_time", [])[:item_limit]
        top_albums_recent = history_insights.get("albums_recent", [])[:item_limit]

    if history_insights:
        fresh_artists_all_time = history_insights.get("artists_all_time", [])[:item_limit]
        fresh_artists_recent = history_insights.get("artists_recent", [])[:item_limit]
        fresh_albums_all_time = history_insights.get("albums_all_time", [])[:item_limit]
        fresh_albums_recent = history_insights.get("albums_recent", [])[:item_limit]
        if len(top_artists_all_time) < len(fresh_artists_all_time):
            top_artists_all_time = fresh_artists_all_time
        if len(top_artists_recent) < len(fresh_artists_recent):
            top_artists_recent = fresh_artists_recent
        if len(top_albums_all_time) < len(fresh_albums_all_time):
            top_albums_all_time = fresh_albums_all_time
        if len(top_albums_recent) < max(2, min(item_limit, len(fresh_albums_recent))):
            top_albums_recent = fresh_albums_recent

    if history_insights:
        _progress("analyzing local track metrics")
        top_tracks_all_time = _apply_track_history_metrics(
            top_tracks_all_time,
            history_insights.get("track_history_metrics"),
            history_insights.get("track_history_metrics_by_key"),
            count_key="play_count",
        )
        top_tracks_recent = _apply_track_history_metrics(
            top_tracks_recent,
            history_insights.get("track_history_metrics"),
            history_insights.get("track_history_metrics_by_key"),
            count_key="recent_play_count",
        )

    top_artists_all_time = _hydrate_artists_from_static_cache(top_artists_all_time)
    top_artists_recent = _hydrate_artists_from_static_cache(top_artists_recent)
    top_albums_all_time = _hydrate_albums_from_static_cache(top_albums_all_time)
    top_albums_recent = _hydrate_albums_from_static_cache(top_albums_recent)
    top_artists_all_time = _merge_artists_from_snapshot(
        top_artists_all_time,
        list((cached_profile_snapshot or {}).get("followed_artists") or []),
    )
    top_artists_recent = _merge_artists_from_snapshot(
        top_artists_recent,
        list((cached_profile_snapshot or {}).get("recent_top_artists") or []),
    )
    top_albums_all_time = _merge_albums_from_snapshot(
        top_albums_all_time,
        list((cached_profile_snapshot or {}).get("top_albums") or []),
    )
    top_albums_recent = _merge_albums_from_snapshot(
        top_albums_recent,
        list((cached_profile_snapshot or {}).get("recent_top_albums") or []),
    )
    top_artists_all_time = _prefer_snapshot_list_if_richer(
        top_artists_all_time,
        list((cached_profile_snapshot or {}).get("followed_artists") or []),
    )
    top_artists_recent = _prefer_snapshot_list_if_richer(
        top_artists_recent,
        list((cached_profile_snapshot or {}).get("recent_top_artists") or []),
    )
    top_albums_all_time = _prefer_snapshot_list_if_richer(
        top_albums_all_time,
        list((cached_profile_snapshot or {}).get("top_albums") or []),
    )
    top_albums_recent = _prefer_snapshot_list_if_richer(
        top_albums_recent,
        list((cached_profile_snapshot or {}).get("recent_top_albums") or []),
    )

    _progress("loading local payload assembly")
    history_insights_available = bool(history_insights)
    followed_total = len(top_artists_all_time) if top_artists_all_time else None
    stale_sections: list[str] = []
    owned_playlists = list((cached_profile_snapshot or {}).get("owned_playlists") or [])
    owned_playlists_available = bool((cached_profile_snapshot or {}).get("owned_playlists_available")) and bool(owned_playlists)
    recent_likes_tracks = list((cached_profile_snapshot or {}).get("recent_likes_tracks") or [])
    recent_likes_available = bool((cached_profile_snapshot or {}).get("recent_likes_available")) and bool(recent_likes_tracks)
    top_playlists_recent = list((cached_profile_snapshot or {}).get("top_playlists_recent") or [])
    top_playlists_all_time = list((cached_profile_snapshot or {}).get("top_playlists_all_time") or [])
    top_playlists_available = bool((cached_profile_snapshot or {}).get("top_playlists_available")) and bool(
        top_playlists_recent or top_playlists_all_time
    )
    local_last_synced_at = (cached_profile_snapshot or {}).get("_stored_at")
    if owned_playlists_available:
        stale_sections.append("playlists")
    if recent_likes_available:
        stale_sections.append("recent_likes")
    if top_playlists_available:
        stale_sections.append("top_playlists")

    return {
        "id": "local-history",
        "display_name": display_name or "Local History",
        "email": email,
        "product": None,
        "country": None,
        "username": username or "listener",
        "followers_total": None,
        "followed_artists_total": followed_total,
        "followed_artists_available": bool(top_artists_all_time),
        "followed_artists": top_artists_all_time,
        "followed_artists_list_available": bool(top_artists_all_time),
        "recent_top_artists": top_artists_recent,
        "recent_top_artists_available": bool(top_artists_recent),
        "top_tracks": top_tracks_all_time[:item_limit],
        "top_tracks_available": bool(top_tracks_all_time),
        "recent_top_tracks": top_tracks_recent[:item_limit],
        "recent_top_tracks_available": bool(top_tracks_recent),
        "top_albums": top_albums_all_time,
        "top_albums_available": bool(top_albums_all_time),
        "recent_top_albums": top_albums_recent,
        "recent_top_albums_available": bool(top_albums_recent),
        "analysis_mode": analysis_mode,
        "experience_mode": "local",
        "recent_range": recent_range,
        "recent_window_days": recent_window_days,
        "history_insights_available": history_insights_available,
        "history_first_played_at": history_insights.get("first_played_at") if history_insights else None,
        "history_last_played_at": history_insights.get("last_played_at") if history_insights else None,
        "history_total_listen_ms": history_insights.get("total_listen_ms") if history_insights else None,
        "history_total_play_count": history_insights.get("total_play_count") if history_insights else None,
        "extended_loaded": is_extended,
        "top_playlists_recent": top_playlists_recent,
        "top_playlists_all_time": top_playlists_all_time,
        "top_playlists_available": top_playlists_available,
        "profile_url": None,
        "image_url": None,
        "recent_tracks": top_tracks_recent[:item_limit],
        "recent_tracks_available": bool(top_tracks_recent),
        "owned_playlists": owned_playlists,
        "owned_playlists_available": owned_playlists_available,
        "recent_likes_tracks": recent_likes_tracks,
        "recent_likes_available": recent_likes_available,
        "stale_sections": stale_sections,
        "local_last_synced_at": local_last_synced_at,
    }


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/auth/login")
async def auth_login(
    request: Request,
    mode: str | None = None,
) -> RedirectResponse:
    if not _is_configured():
        raise HTTPException(status_code=500, detail="Spotify OAuth is not configured.")

    oauth_mode = "recent_ingest" if mode == "recent_ingest" else "default"
    oauth_scope = "user-read-recently-played" if oauth_mode == "recent_ingest" else settings.spotify_scope
    state = secrets.token_urlsafe(32)
    code_verifier = secrets.token_urlsafe(64)
    code_challenge = _pkce_code_challenge(code_verifier)
    request.session["oauth_state"] = state
    request.session["oauth_mode"] = oauth_mode
    request.session["oauth_code_verifier"] = code_verifier

    query = urlencode(
        {
            "client_id": settings.spotify_client_id,
            "response_type": "code",
            "redirect_uri": settings.spotify_redirect_uri,
            "scope": oauth_scope,
            "state": state,
            "code_challenge_method": "S256",
            "code_challenge": code_challenge,
        }
    )

    return RedirectResponse(url=f"{settings.spotify_authorize_url}?{query}", status_code=302)


@app.get("/auth/callback")
async def auth_callback(request: Request, code: str | None = None, state: str | None = None) -> RedirectResponse:
    expected_state = request.session.get("oauth_state")
    if not code or not state or state != expected_state:
        logger.warning("Spotify callback state validation failed.")
        return RedirectResponse(url=_callback_redirect_url("state_error"), status_code=302)

    oauth_mode = str(request.session.get("oauth_mode") or "default")
    code_verifier = request.session.get("oauth_code_verifier")

    token_request_data: dict[str, str] = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": settings.spotify_client_id,
        "redirect_uri": settings.spotify_redirect_uri,
    }
    if code_verifier:
        token_request_data["code_verifier"] = str(code_verifier)

    async with httpx.AsyncClient(timeout=15.0) as client:
        token_response = await client.post(
            settings.spotify_token_url,
            data=token_request_data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

    if token_response.status_code >= 400:
        detail = ""
        try:
            payload = token_response.json()
            detail = payload.get("error_description") or payload.get("error") or ""
        except ValueError:
            detail = token_response.text[:120]

        logger.warning(
            "Spotify token exchange failed with status %s: %s",
            token_response.status_code,
            detail or "<no detail>",
        )
        return RedirectResponse(
            url=_callback_redirect_url("token_error", detail or f"http_{token_response.status_code}"),
            status_code=302,
        )

    token_data = token_response.json()
    access_token = token_data.get("access_token")
    if not access_token:
        logger.warning("Spotify token exchange succeeded without an access token.")
        return RedirectResponse(url=_callback_redirect_url("token_missing"), status_code=302)

    refresh_token = str(token_data.get("refresh_token") or "").strip()
    if not refresh_token:
        logger.warning("Spotify token exchange succeeded without a refresh token.")
        return RedirectResponse(url=_callback_redirect_url("token_missing_refresh"), status_code=302)

    expires_at = _expires_at_from_expires_in(token_data.get("expires_in"))
    scopes = str(token_data.get("scope") or ("user-read-recently-played" if oauth_mode == "recent_ingest" else settings.spotify_scope))

    try:
        profile = await _fetch_spotify_profile(access_token)
    except HTTPException as exc:
        logger.warning("Spotify profile fetch failed after token exchange: %s", exc.detail)
        return RedirectResponse(url=_callback_redirect_url("profile_error"), status_code=302)

    spotify_user_id = str(profile.get("id") or "").strip()
    if not spotify_user_id:
        logger.warning("Spotify profile fetch after token exchange returned no user id.")
        return RedirectResponse(url=_callback_redirect_url("profile_missing_id"), status_code=302)

    try:
        upsert_spotify_tokens(
            user_id=spotify_user_id,
            spotify_user_id=spotify_user_id,
            access_token=str(access_token),
            refresh_token=refresh_token,
            expires_at=expires_at,
            scopes=scopes,
        )
    except RuntimeError as exc:
        logger.warning("Failed to persist Spotify tokens after OAuth callback: %s", exc)
        return RedirectResponse(url=_callback_redirect_url("token_store_error"), status_code=302)

    request.session.pop("oauth_state", None)
    request.session.pop("oauth_mode", None)
    request.session.pop("oauth_code_verifier", None)
    request.session["user_id"] = spotify_user_id
    request.session["token_type"] = token_data.get("token_type") or "Bearer"
    request.session["expires_in"] = int(token_data.get("expires_in") or 0)
    request.session["spotify_user"] = {
        "id": spotify_user_id,
        "display_name": profile.get("display_name"),
        "email": profile.get("email"),
    }

    if oauth_mode == "recent_ingest":
        ingest_result: dict[str, Any] = {
            "flow": "recent_ingest",
            "auth_succeeded": True,
            "ingest_succeeded": False,
            "error": None,
            "row_count": 0,
            "earliest_api_played_at": None,
            "latest_api_played_at": None,
        }
        try:
            summary = await sync_spotify_recent_plays(
                access_token,
                source_ref="oauth_recent_ingest",
                limit=50,
            )
            ingest_result.update(
                {
                    "ingest_succeeded": True,
                    "row_count": int(summary.get("row_count") or 0),
                    "inserted_count": int(summary.get("inserted_count") or 0),
                    "duplicate_count": int(summary.get("duplicate_count") or 0),
                    "already_seen_source_row_count": int(summary.get("already_seen_source_row_count") or 0),
                    "merged_duplicate_row_count": int(summary.get("merged_duplicate_row_count") or 0),
                    "earliest_api_played_at": summary.get("earliest_played_at"),
                    "latest_api_played_at": summary.get("latest_played_at"),
                }
            )
        except Exception as exc:
            ingest_result["error"] = str(exc)

        request.session["recent_ingest_result"] = ingest_result
        return RedirectResponse(
            url=_callback_redirect_url("success", extra={"flow": "recent_ingest"}),
            status_code=302,
        )

    return RedirectResponse(url=_callback_redirect_url("success"), status_code=302)


@app.get("/auth/recent-ingest/result")
async def auth_recent_ingest_result(request: Request) -> dict[str, Any]:
    payload = request.session.pop("recent_ingest_result", None)
    if not isinstance(payload, dict):
        return {"has_result": False}
    return {"has_result": True, **payload}


@app.get("/auth/recent-ingest/probe-before")
async def auth_recent_ingest_probe_before(
    request: Request,
    days: int = 90,
    limit: int = 50,
) -> dict[str, Any]:
    bounded_days = max(1, min(days, 365))
    bounded_limit = max(1, min(limit, 50))
    before_iso = (datetime.now(UTC) - timedelta(days=bounded_days)).isoformat().replace("+00:00", "Z")
    before_millis = int(datetime.fromisoformat(before_iso.replace("Z", "+00:00")).timestamp() * 1000)

    token_source = "token_store"
    token = _require_token(request)

    try:
        page = await fetch_spotify_recent_play_page(
            str(token),
            before_cursor=str(before_millis),
            limit=bounded_limit,
        )
    except RuntimeError as exc:
        detail = str(exc)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=detail) from exc

    items = page.get("items") or []
    played_values = sorted(
        str(item.get("played_at"))
        for item in items
        if isinstance(item, dict) and item.get("played_at") is not None
    )

    return {
        "ok": True,
        "token_source": token_source,
        "days": bounded_days,
        "limit": bounded_limit,
        "before_iso": before_iso,
        "before_millis": before_millis,
        "returned_items": len(items),
        "earliest_played_at": played_values[0] if played_values else None,
        "latest_played_at": played_values[-1] if played_values else None,
    }


@app.get("/auth/recent-ingest/probe-backfill")
async def auth_recent_ingest_probe_backfill(
    request: Request,
    limit: int = 50,
    max_pages: int = 10,
) -> dict[str, Any]:
    bounded_limit = max(1, min(limit, 50))
    bounded_pages = max(1, min(max_pages, 50))

    token_source = "token_store"
    token = _require_token(request)

    before_cursor: str | None = None
    page_summaries: list[dict[str, Any]] = []
    all_played_at: list[str] = []
    total_items = 0

    for page_index in range(1, bounded_pages + 1):
        try:
            page = await fetch_spotify_recent_play_page(
                str(token),
                before_cursor=before_cursor,
                limit=bounded_limit,
            )
        except RuntimeError as exc:
            detail = str(exc)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=detail) from exc

        items = page.get("items") or []
        item_count = len(items)
        total_items += item_count
        played_values = sorted(
            str(item.get("played_at"))
            for item in items
            if isinstance(item, dict) and item.get("played_at") is not None
        )
        all_played_at.extend(played_values)
        page_summaries.append(
            {
                "page": page_index,
                "item_count": item_count,
                "earliest_played_at": played_values[0] if played_values else None,
                "latest_played_at": played_values[-1] if played_values else None,
            }
        )

        next_before = page.get("before_cursor")
        if item_count == 0 or next_before is None:
            break
        before_cursor = str(next_before)

    all_played_at.sort()
    return {
        "ok": True,
        "token_source": token_source,
        "limit": bounded_limit,
        "max_pages": bounded_pages,
        "pages_fetched": len(page_summaries),
        "total_items": total_items,
        "earliest_played_at": all_played_at[0] if all_played_at else None,
        "latest_played_at": all_played_at[-1] if all_played_at else None,
        "page_summaries": page_summaries,
    }


@app.get("/auth/session")
async def auth_session(request: Request) -> dict[str, Any]:
    user_id = _session_user_id(request) or _restore_session_user_from_token_store(request)
    user = request.session.get("spotify_user") or {}
    token_state = get_spotify_tokens(user_id) if user_id else None
    authenticated = bool(token_state and not token_state.get("reauth_required"))

    return {
        "authenticated": authenticated,
        "display_name": user.get("display_name"),
        "spotify_user_id": user.get("id") or (str(token_state.get("spotify_user_id")) if token_state else None),
        "email": user.get("email"),
    }


@app.get("/auth/current-playback")
async def auth_current_playback(request: Request) -> dict[str, Any]:
    user_id = _require_user_id(request)
    return await get_current_playback_for_user(user_id)


@app.post("/auth/recent-ingest/poll-now")
async def auth_recent_ingest_poll_now(request: Request) -> dict[str, Any]:
    user_id = _require_user_id(request)
    return await poll_recent_for_user(user_id)


@app.get("/auth/full-availability")
async def auth_full_availability(request: Request) -> dict[str, Any]:
    user_id = _session_user_id(request)
    if not user_id:
        return {
            "available": False,
            "blocked": False,
            "reason": "not_authenticated",
            "detail": "Spotify is not connected for this session.",
            "retry_after_seconds": None,
        }

    token_state = get_spotify_tokens(user_id)
    if token_state is None:
        return {
            "available": False,
            "blocked": False,
            "reason": "not_authenticated",
            "detail": "Spotify is not connected for this session.",
            "retry_after_seconds": None,
        }
    if token_state.get("reauth_required"):
        return {
            "available": False,
            "blocked": False,
            "reason": "reauth_required",
            "detail": str(token_state.get("reauth_reason") or "Spotify reauthorization is required."),
            "retry_after_seconds": None,
        }

    remaining = _spotify_cooldown_seconds_remaining()
    if remaining > 0:
        return {
            "available": False,
            "blocked": True,
            "reason": "cooldown_active",
            "detail": _spotify_rate_limit_detail("Spotify is rate-limiting requests right now."),
            "retry_after_seconds": remaining,
        }

    try:
        token = _require_token(request)
        await _fetch_spotify_profile(token)
    except HTTPException as exc:
        if exc.status_code == status.HTTP_401_UNAUTHORIZED:
            try:
                refreshed = await _refresh_spotify_access_token(request)
                await _fetch_spotify_profile(refreshed)
            except HTTPException as retry_exc:
                if retry_exc.status_code == status.HTTP_429_TOO_MANY_REQUESTS:
                    retry_after = _spotify_cooldown_seconds_remaining()
                    return {
                        "available": False,
                        "blocked": True,
                        "reason": "rate_limited",
                        "detail": retry_exc.detail,
                        "retry_after_seconds": retry_after,
                    }
                return {
                    "available": False,
                    "blocked": False,
                    "reason": "unauthorized",
                    "detail": retry_exc.detail,
                    "retry_after_seconds": None,
                }
            else:
                return {
                    "available": True,
                    "blocked": False,
                    "reason": "ok",
                    "detail": "Full Spotify experience is available.",
                    "retry_after_seconds": None,
                }

        if exc.status_code == status.HTTP_429_TOO_MANY_REQUESTS:
            retry_after = _spotify_cooldown_seconds_remaining()
            return {
                "available": False,
                "blocked": True,
                "reason": "rate_limited",
                "detail": exc.detail,
                "retry_after_seconds": retry_after,
            }
        if exc.status_code == status.HTTP_403_FORBIDDEN:
            return {
                "available": False,
                "blocked": False,
                "reason": "missing_scope",
                "detail": exc.detail,
                "retry_after_seconds": None,
            }
        return {
            "available": False,
            "blocked": False,
            "reason": "spotify_unavailable",
            "detail": exc.detail,
            "retry_after_seconds": None,
        }

    return {
        "available": True,
        "blocked": False,
        "reason": "ok",
        "detail": "Full Spotify experience is available.",
        "retry_after_seconds": None,
    }


@app.get("/auth/token")
async def auth_token(request: Request) -> dict[str, Any]:
    _require_user_id(request)

    # Return a freshly refreshed token for playback/API clients so we don't hand
    # out an expired session token.
    try:
        token = await _refresh_spotify_access_token(request)
    except HTTPException as exc:
        if exc.status_code == status.HTTP_401_UNAUTHORIZED:
            request.session.clear()
        raise

    return {
        "access_token": token,
        "token_type": request.session.get("token_type") or "Bearer",
        "expires_in": request.session.get("expires_in"),
    }


@app.get("/preview/representative")
async def preview_representative(
    request: Request,
    kind: str,
    spotify_id: str,
) -> dict[str, Any]:
    token = _require_token(request)

    market: str | None = None
    try:
        profile = await _fetch_spotify_profile(token)
        market = profile.get("country")
    except HTTPException:
        market = None

    try:
        if kind == "artist":
            track = await _fetch_artist_representative_track(token, spotify_id, market=market)
        elif kind == "album":
            track = await _fetch_album_representative_track(token, spotify_id, market=market)
        else:
            raise HTTPException(status_code=400, detail="Unsupported preview kind.")
    except HTTPException as exc:
        if exc.status_code == status.HTTP_403_FORBIDDEN:
            return {"track": None, "reason": "spotify_rejected_lookup"}
        if exc.status_code == status.HTTP_404_NOT_FOUND:
            return {"track": None, "reason": "item_not_found"}
        if exc.status_code == status.HTTP_429_TOO_MANY_REQUESTS:
            return {"track": None, "reason": "rate_limited"}
        if exc.status_code == status.HTTP_502_BAD_GATEWAY:
            return {"track": None, "reason": "spotify_lookup_failed"}
        raise

    if not track:
        return {"track": None, "reason": "no_representative_track"}

    return {"track": track, "reason": "ok"}


@app.post("/auth/logout")
async def auth_logout(request: Request) -> dict[str, str]:
    request.session.clear()
    return {"status": "logged_out"}


@app.post("/cache/rebuild")
async def cache_rebuild(request: Request) -> dict[str, str]:
    _clear_dashboard_caches()
    history_signature = get_history_signature(settings.spotify_history_dir)
    if history_signature:
        for recent_window_days in (28, 180):
            history_insights = load_history_insights(
                settings.spotify_history_dir,
                max(SECTION_PREVIEW_LIMIT, 50),
                recent_window_days=recent_window_days,
            )
            if not history_insights:
                continue
            _store_local_history_insights_cache(
                history_signature,
                recent_window_days,
                max(SECTION_PREVIEW_LIMIT, 50),
                history_insights,
            )
            history_sections_with_tracks = {
                "tracks_all_time": history_insights.get("tracks_all_time", [])[:HISTORY_TRACKS_DISPLAY_LIMIT],
                "tracks_recent": history_insights.get("tracks_recent", [])[:SECTION_PREVIEW_LIMIT],
                "artists_all_time": history_insights.get("artists_all_time", [])[:SECTION_PREVIEW_LIMIT],
                "artists_recent": history_insights.get("artists_recent", [])[:SECTION_PREVIEW_LIMIT],
                "albums_all_time": history_insights.get("albums_all_time", [])[:SECTION_PREVIEW_LIMIT],
                "albums_recent": history_insights.get("albums_recent", [])[:SECTION_PREVIEW_LIMIT],
            }
            _store_persistent_history_cache(
                history_signature,
                recent_window_days,
                history_sections_with_tracks,
            )
    return {"status": "cache_rebuilt"}


@app.get("/me/progress")
async def me_progress(request: Request) -> dict[str, Any]:
    key = _progress_key(request)
    if not key:
        return {"active": False, "phase": None, "elapsed_seconds": 0.0}

    progress = LOAD_PROGRESS.get(key)
    if not progress:
        return {"active": False, "phase": None, "elapsed_seconds": 0.0}

    return {
        "active": True,
        "phase": progress.get("phase"),
        "elapsed_seconds": round(time.perf_counter() - float(progress.get("started_at", time.perf_counter())), 1),
        "events": progress.get("events", []),
    }


@app.get("/me/local/recent")
async def me_local_recent(
    request: Request,
    recent_range: str = "short_term",
    limit: int = SECTION_PREVIEW_LIMIT,
) -> dict[str, Any]:
    if recent_range not in {"short_term", "medium_term"}:
        raise HTTPException(status_code=400, detail="Unsupported recent range.")
    item_limit = max(1, min(int(limit), SECTION_PREVIEW_LIMIT))
    _set_load_progress(request, "recent refresh", mode="local-recent")
    try:
        payload = _build_local_profile_payload(
            mode="extended",
            recent_range=recent_range,
            analysis_mode="full",
            progress_hook=lambda phase: _set_load_progress(request, phase),
        )
        _set_load_progress(request, "finishing")
        return {
            "recent_range": payload.get("recent_range"),
            "recent_window_days": payload.get("recent_window_days"),
            "recent_top_artists": (payload.get("recent_top_artists") or [])[:item_limit],
            "recent_top_artists_available": bool((payload.get("recent_top_artists") or [])[:item_limit]),
            "recent_top_tracks": (payload.get("recent_top_tracks") or [])[:item_limit],
            "recent_top_tracks_available": bool((payload.get("recent_top_tracks") or [])[:item_limit]),
            "recent_top_albums": (payload.get("recent_top_albums") or [])[:item_limit],
            "recent_top_albums_available": bool((payload.get("recent_top_albums") or [])[:item_limit]),
            "recent_tracks": (payload.get("recent_tracks") or [])[:item_limit],
            "recent_tracks_available": bool((payload.get("recent_tracks") or [])[:item_limit]),
            "recent_likes_tracks": [],
            "recent_likes_available": False,
        }
    finally:
        _clear_load_progress(request)


@app.get("/me/local")
async def me_local(
    request: Request,
    mode: str = "initial",
    recent_range: str = "short_term",
    analysis_mode: str = "quick",
) -> dict[str, Any]:
    if recent_range not in {"short_term", "medium_term"}:
        raise HTTPException(status_code=400, detail="Unsupported recent range.")
    if analysis_mode not in {"quick", "full"}:
        raise HTTPException(status_code=400, detail="Unsupported analysis mode.")
    if mode not in {"initial", "extended"}:
        raise HTTPException(status_code=400, detail="Unsupported mode.")
    _set_load_progress(request, "profile", mode="local")
    try:
        session_user = request.session.get("spotify_user") or {}
        cached_profile_snapshot = _load_user_profile_snapshot(session_user.get("id"))
        payload = _build_local_profile_payload(
            mode=mode,
            recent_range=recent_range,
            analysis_mode=analysis_mode,
            username=session_user.get("id"),
            display_name=session_user.get("display_name"),
            email=session_user.get("email"),
            cached_profile_snapshot=cached_profile_snapshot,
            progress_hook=lambda phase: _set_load_progress(request, phase),
        )
        _set_load_progress(request, "finishing")
        return payload
    finally:
        _clear_load_progress(request)


@app.get("/me/recent")
async def me_recent(
    request: Request,
    recent_range: str = "short_term",
    limit: int = SECTION_PREVIEW_LIMIT,
) -> dict[str, Any]:
    token = _require_token(request)
    if recent_range not in {"short_term", "medium_term"}:
        raise HTTPException(status_code=400, detail="Unsupported recent range.")
    item_limit = max(1, min(int(limit), SECTION_PREVIEW_LIMIT))
    recent_window_days = 28 if recent_range == "short_term" else 180
    _set_load_progress(request, "recent refresh", mode="full-recent")
    try:
        try:
            profile = await _fetch_spotify_profile(token)
        except HTTPException as exc:
            if exc.status_code != status.HTTP_401_UNAUTHORIZED:
                raise
            token = await _refresh_spotify_access_token(request)
            profile = await _fetch_spotify_profile(token)
        user_id = profile.get("id")

        try:
            _set_load_progress(request, "recent listening")
            recent_tracks, recent_tracks_available = await _fetch_recent_tracks(token, item_limit)

            _set_load_progress(request, "liked tracks")
            recent_likes_tracks, recent_likes_available = await _fetch_recent_liked_tracks(token, item_limit)

            cached_top_tracks_recent = _get_short_cache(f"top_tracks_{recent_range}", user_id, item_limit)
            _set_load_progress(
                request,
                "top tracks recent (cache hit)" if cached_top_tracks_recent is not None else "top tracks recent (fresh)",
            )
            if cached_top_tracks_recent is not None:
                top_tracks_recent, top_tracks_recent_available = cached_top_tracks_recent
            else:
                top_tracks_recent, top_tracks_recent_available = await _fetch_top_tracks(token, recent_range, item_limit)
                _set_short_cache(
                    f"top_tracks_{recent_range}",
                    user_id,
                    item_limit,
                    (top_tracks_recent, top_tracks_recent_available),
                )

            cached_top_artists_recent = _get_short_cache(f"top_artists_{recent_range}", user_id, item_limit)
            _set_load_progress(
                request,
                "top artists recent (cache hit)" if cached_top_artists_recent is not None else "top artists recent (fresh)",
            )
            if cached_top_artists_recent is not None:
                top_artists_recent, top_artists_recent_available = cached_top_artists_recent
            else:
                top_artists_recent, top_artists_recent_available = await _fetch_top_artists(token, recent_range, item_limit)
                _set_short_cache(
                    f"top_artists_{recent_range}",
                    user_id,
                    item_limit,
                    (top_artists_recent, top_artists_recent_available),
                )
        except HTTPException as exc:
            if exc.status_code != status.HTTP_429_TOO_MANY_REQUESTS:
                raise
            cached_snapshot = _load_user_recent_snapshot(user_id, recent_range)
            if cached_snapshot:
                _set_load_progress(request, "loading per-user cached recent sections")
                _set_load_progress(request, "finishing")
                return cached_snapshot
            raise

        _set_load_progress(request, "recent album formulas")
        _, top_albums_recent = _normalize_live_top_albums(
            long_term_top_tracks=[],
            recent_top_tracks=top_tracks_recent,
            recent_tracks=recent_tracks,
            liked_tracks=recent_likes_tracks,
        )
        top_albums_recent_available = bool(top_albums_recent)

        payload = {
            "recent_range": recent_range,
            "recent_window_days": recent_window_days,
            "recent_top_artists": top_artists_recent,
            "recent_top_artists_available": top_artists_recent_available,
            "recent_top_tracks": top_tracks_recent,
            "recent_top_tracks_available": top_tracks_recent_available,
            "recent_top_albums": top_albums_recent,
            "recent_top_albums_available": top_albums_recent_available,
            "recent_tracks": recent_tracks,
            "recent_tracks_available": recent_tracks_available,
            "recent_likes_tracks": recent_likes_tracks,
            "recent_likes_available": recent_likes_available,
        }
        _store_user_recent_snapshot(user_id, recent_range, payload)
        _store_user_profile_snapshot(
            user_id,
            {
                "recent_likes_tracks": recent_likes_tracks,
                "recent_likes_available": recent_likes_available,
            },
        )
        _set_load_progress(request, "finishing")
        return payload
    finally:
        _clear_load_progress(request)


@app.get("/me")
async def me(
    request: Request,
    mode: str = "initial",
    recent_range: str = "short_term",
    analysis_mode: str = "quick",
) -> dict[str, Any]:
    token = _require_token(request)
    is_extended = mode == "extended"
    is_full_analysis = analysis_mode == "full"
    if recent_range not in {"short_term", "medium_term"}:
        raise HTTPException(status_code=400, detail="Unsupported recent range.")
    if analysis_mode not in {"quick", "full"}:
        raise HTTPException(status_code=400, detail="Unsupported analysis mode.")
    recent_window_days = 28 if recent_range == "short_term" else 180
    item_limit = SECTION_PREVIEW_LIMIT if is_extended else INITIAL_DASHBOARD_LIMIT
    playlist_limit = None if is_extended else INITIAL_DASHBOARD_LIMIT
    playlist_cache_limit = playlist_limit if playlist_limit is not None else -1
    _set_load_progress(request, "profile", mode="full")
    try:
        try:
            profile = await _fetch_spotify_profile(token)
        except HTTPException as exc:
            if exc.status_code != status.HTTP_401_UNAUTHORIZED:
                raise
            token = await _refresh_spotify_access_token(request)
            profile = await _fetch_spotify_profile(token)
        user_id = profile.get("id")
        images = profile.get("images") or []
        external_urls = profile.get("external_urls") or {}
        followers = profile.get("followers") or {}

        if not is_full_analysis:
            quick_item_limit = SECTION_PREVIEW_LIMIT
            quick_playlist_limit = SECTION_PREVIEW_LIMIT
            cached_playlists = _get_short_cache("owned_playlists", user_id, quick_playlist_limit)
            if cached_playlists is not None and not _playlist_cache_needs_refresh(cached_playlists[0]):
                playlists, owned_playlists_available = cached_playlists
            else:
                _set_load_progress(request, "playlists (quick)")
                playlists, owned_playlists_available = await _fetch_owned_playlists(token, user_id, quick_playlist_limit)
                _set_short_cache(
                    "owned_playlists",
                    user_id,
                    quick_playlist_limit,
                    (playlists, owned_playlists_available),
                )

            cached_followed_total = _get_short_cache("followed_artists_total", user_id, 1)
            if cached_followed_total is not None:
                followed_artists_total, followed_artists_available = cached_followed_total
            else:
                _set_load_progress(request, "followed artist count (quick)")
                followed_artists_total, followed_artists_available = await _fetch_followed_artists_total(token)
                _set_short_cache(
                    "followed_artists_total",
                    user_id,
                    1,
                    (followed_artists_total, followed_artists_available),
                )

            cached_top_artists_all_time = _get_short_cache("top_artists_long_term", user_id, quick_item_limit)
            if cached_top_artists_all_time is not None:
                top_artists_all_time, top_artists_all_time_available = cached_top_artists_all_time
            else:
                _set_load_progress(request, "top artists all time (quick)")
                top_artists_all_time, top_artists_all_time_available = await _fetch_top_artists(
                    token,
                    "long_term",
                    quick_item_limit,
                )
                _set_short_cache(
                    "top_artists_long_term",
                    user_id,
                    quick_item_limit,
                    (top_artists_all_time, top_artists_all_time_available),
                )

            cached_top_tracks_all_time = _get_short_cache("top_tracks_long_term", user_id, quick_item_limit)
            if cached_top_tracks_all_time is not None:
                top_tracks_all_time, top_tracks_all_time_available = cached_top_tracks_all_time
            else:
                _set_load_progress(request, "top tracks all time (quick)")
                top_tracks_all_time, top_tracks_all_time_available = await _fetch_top_tracks(
                    token,
                    "long_term",
                    quick_item_limit,
                )
                _set_short_cache(
                    "top_tracks_long_term",
                    user_id,
                    quick_item_limit,
                    (top_tracks_all_time, top_tracks_all_time_available),
                )
            spotify_top_tracks_all_time = list(top_tracks_all_time)

            if top_tracks_all_time_available:
                top_albums_all_time, _ = _normalize_live_top_albums(
                    long_term_top_tracks=top_tracks_all_time,
                    recent_top_tracks=[],
                    recent_tracks=[],
                    liked_tracks=[],
                )
                top_albums_all_time_available = bool(top_albums_all_time)
            else:
                top_albums_all_time = []
                top_albums_all_time_available = False

            # Quick load should reflect the latest history-backed recompute when available.
            # All-time sections intentionally ignore the current recent-window toggle.
            history_signature = get_history_signature(settings.spotify_history_dir)
            persistent_history_sections_any_window = _load_persistent_history_cache_any_window(history_signature)
            if persistent_history_sections_any_window:
                cached_tracks_all_time = persistent_history_sections_any_window.get("tracks_all_time", [])[:quick_item_limit]
                if cached_tracks_all_time:
                    top_tracks_all_time = _merge_history_tracks(
                        cached_tracks_all_time,
                        _track_enrichment_lookup(spotify_top_tracks_all_time),
                    )
                    if any(not track.get("image_url") for track in top_tracks_all_time):
                        top_tracks_all_time = await _enrich_tracks_from_spotify(
                            token,
                            top_tracks_all_time,
                            market=profile.get("country"),
                        )
                    top_tracks_all_time_available = True

                cached_artists_all_time = persistent_history_sections_any_window.get("artists_all_time", [])[:quick_item_limit]
                if cached_artists_all_time:
                    top_artists_all_time = cached_artists_all_time
                    top_artists_all_time_available = True

                cached_albums_all_time = persistent_history_sections_any_window.get("albums_all_time", [])[:quick_item_limit]
                if cached_albums_all_time:
                    top_albums_all_time = cached_albums_all_time
                    top_albums_all_time_available = True

            top_artists_all_time = _hydrate_artists_from_static_cache(top_artists_all_time)
            top_albums_all_time = _hydrate_albums_from_static_cache(top_albums_all_time)
            top_artists_all_time, top_albums_all_time = await _backfill_artist_album_images_if_needed(
                token,
                top_artists_all_time,
                top_albums_all_time,
            )
            top_albums_all_time = await _backfill_album_images_if_needed(token, top_albums_all_time)

            payload = {
                "id": profile.get("id"),
                "display_name": profile.get("display_name"),
                "email": profile.get("email"),
                "product": profile.get("product"),
                "country": profile.get("country"),
                "username": profile.get("id"),
                "followers_total": followers.get("total"),
                "followed_artists_total": followed_artists_total,
                "followed_artists_available": followed_artists_available,
                "followed_artists": top_artists_all_time,
                "followed_artists_list_available": top_artists_all_time_available,
                "recent_top_artists": [],
                "recent_top_artists_available": False,
                "top_tracks": top_tracks_all_time,
                "top_tracks_available": top_tracks_all_time_available,
                "recent_top_tracks": [],
                "recent_top_tracks_available": False,
                "top_albums": top_albums_all_time,
                "top_albums_available": top_albums_all_time_available,
                "recent_top_albums": [],
                "recent_top_albums_available": False,
                "analysis_mode": analysis_mode,
                "recent_range": recent_range,
                "recent_window_days": recent_window_days,
                "history_insights_available": False,
                "history_first_played_at": None,
                "history_last_played_at": None,
                "history_total_listen_ms": None,
                "history_total_play_count": None,
                "extended_loaded": False,
                "top_playlists_recent": [],
                "top_playlists_all_time": [],
                "top_playlists_available": owned_playlists_available,
                "profile_url": external_urls.get("spotify"),
                "image_url": images[0].get("url") if images else None,
                "recent_tracks": [],
                "recent_tracks_available": False,
                "owned_playlists": playlists,
                "owned_playlists_available": owned_playlists_available,
                "recent_likes_tracks": [],
                "recent_likes_available": False,
            }
            _store_user_profile_snapshot(
                user_id,
                {
                    "followed_artists": top_artists_all_time,
                    "recent_top_artists": [],
                    "top_albums": top_albums_all_time,
                    "recent_top_albums": [],
                    "owned_playlists": playlists,
                    "owned_playlists_available": owned_playlists_available,
                    "top_playlists_recent": [],
                    "top_playlists_all_time": [],
                    "top_playlists_available": owned_playlists_available,
                },
            )
            _set_load_progress(request, "finishing")
            return payload

        if is_full_analysis:
            _set_load_progress(request, "recent listening")
            recent_tracks, recent_tracks_available = await _fetch_recent_tracks(token, item_limit)
        else:
            recent_tracks, recent_tracks_available = [], False
        cached_playlists = _get_short_cache("owned_playlists", user_id, playlist_cache_limit)
        _set_load_progress(
            request,
            "playlists (cache hit)" if cached_playlists is not None else "playlists (fresh)",
        )
        if cached_playlists is not None and not _playlist_cache_needs_refresh(cached_playlists[0]):
            playlists, owned_playlists_available = cached_playlists
        else:
            playlists, owned_playlists_available = await _fetch_owned_playlists(token, user_id, playlist_limit)
            _set_short_cache(
                "owned_playlists",
                user_id,
                playlist_cache_limit,
                (playlists, owned_playlists_available),
            )
        if is_full_analysis:
            _set_load_progress(request, "liked tracks")
            recent_likes_tracks, recent_likes_available = await _fetch_recent_liked_tracks(token, item_limit)
        else:
            recent_likes_tracks, recent_likes_available = [], False
        cached_followed_total = _get_short_cache("followed_artists_total", user_id, 1)
        _set_load_progress(
            request,
            "followed artist count (cache hit)" if cached_followed_total is not None else "followed artist count (fresh)",
        )
        if cached_followed_total is not None:
            followed_artists_total, followed_artists_available = cached_followed_total
        else:
            followed_artists_total, followed_artists_available = await _fetch_followed_artists_total(token)
            _set_short_cache(
                "followed_artists_total",
                user_id,
                1,
                (followed_artists_total, followed_artists_available),
            )
        cached_top_artists_all_time = _get_short_cache("top_artists_long_term", user_id, item_limit)
        _set_load_progress(
            request,
            "top artists all time (cache hit)" if cached_top_artists_all_time is not None else "top artists all time (fresh)",
        )
        if cached_top_artists_all_time is not None:
            top_artists_all_time, top_artists_all_time_available = cached_top_artists_all_time
        else:
            top_artists_all_time, top_artists_all_time_available = await _fetch_top_artists(token, "long_term", item_limit)
            _set_short_cache(
                "top_artists_long_term",
                user_id,
                item_limit,
                (top_artists_all_time, top_artists_all_time_available),
            )
        if is_full_analysis:
            cached_top_artists_recent = _get_short_cache(f"top_artists_{recent_range}", user_id, item_limit)
            _set_load_progress(
                request,
                "top artists recent (cache hit)" if cached_top_artists_recent is not None else "top artists recent (fresh)",
            )
            if cached_top_artists_recent is not None:
                top_artists_recent, top_artists_recent_available = cached_top_artists_recent
            else:
                top_artists_recent, top_artists_recent_available = await _fetch_top_artists(token, recent_range, item_limit)
                _set_short_cache(
                    f"top_artists_{recent_range}",
                    user_id,
                    item_limit,
                    (top_artists_recent, top_artists_recent_available),
                )
        else:
            top_artists_recent, top_artists_recent_available = [], False
        cached_top_tracks_all_time = _get_short_cache("top_tracks_long_term", user_id, item_limit)
        _set_load_progress(
            request,
            "top tracks all time (cache hit)" if cached_top_tracks_all_time is not None else "top tracks all time (fresh)",
        )
        if cached_top_tracks_all_time is not None:
            top_tracks_all_time, top_tracks_all_time_available = cached_top_tracks_all_time
        else:
            top_tracks_all_time, top_tracks_all_time_available = await _fetch_top_tracks(token, "long_term", item_limit)
            _set_short_cache(
                "top_tracks_long_term",
                user_id,
                item_limit,
                (top_tracks_all_time, top_tracks_all_time_available),
            )
        if is_full_analysis:
            cached_top_tracks_recent = _get_short_cache(f"top_tracks_{recent_range}", user_id, item_limit)
            _set_load_progress(
                request,
                "top tracks recent (cache hit)" if cached_top_tracks_recent is not None else "top tracks recent (fresh)",
            )
            if cached_top_tracks_recent is not None:
                top_tracks_recent, top_tracks_recent_available = cached_top_tracks_recent
            else:
                top_tracks_recent, top_tracks_recent_available = await _fetch_top_tracks(token, recent_range, item_limit)
                _set_short_cache(
                    f"top_tracks_{recent_range}",
                    user_id,
                    item_limit,
                    (top_tracks_recent, top_tracks_recent_available),
                )
        else:
            top_tracks_recent, top_tracks_recent_available = [], False
        top_albums_all_time: list[dict[str, Any]] = []
        top_albums_recent: list[dict[str, Any]] = []
        live_formula_available = is_full_analysis and any(
            [
                top_tracks_all_time_available,
                top_tracks_recent_available,
                recent_tracks_available,
                recent_likes_available,
            ]
        )
        top_albums_all_time_available = live_formula_available
        top_albums_recent_available = top_albums_all_time_available
        top_playlists_recent: list[dict[str, Any]] = []
        top_playlists_all_time: list[dict[str, Any]] = []
        top_playlists_available = owned_playlists_available
        history_signature = get_history_signature(settings.spotify_history_dir)
        persistent_history_sections = _load_persistent_history_cache(history_signature, recent_window_days)
        history_insights_available = False
        history_insights = None
        used_history_backed_results = False

        if is_full_analysis and persistent_history_sections:
            _set_load_progress(request, "history favorites (persistent cache hit)")
            cached_tracks_all_time = persistent_history_sections.get("tracks_all_time", [])[:HISTORY_TRACKS_DISPLAY_LIMIT]
            if cached_tracks_all_time:
                top_tracks_all_time = cached_tracks_all_time
                top_tracks_all_time_available = True
            cached_tracks_recent = persistent_history_sections.get("tracks_recent", [])[:item_limit]
            if cached_tracks_recent:
                top_tracks_recent = cached_tracks_recent
                top_tracks_recent_available = True
            top_artists_all_time = persistent_history_sections.get("artists_all_time", [])[:item_limit]
            top_artists_recent = persistent_history_sections.get("artists_recent", [])[:item_limit]
            top_albums_all_time = persistent_history_sections.get("albums_all_time", [])[:item_limit]
            top_albums_recent = persistent_history_sections.get("albums_recent", [])[:item_limit]
            top_artists_all_time_available = bool(top_artists_all_time)
            top_artists_recent_available = bool(top_artists_recent)
            top_albums_all_time_available = bool(top_albums_all_time)
            top_albums_recent_available = bool(top_albums_recent)
            history_insights_available = True
            used_history_backed_results = True
            if (
                any(not (artist.get("image_url") and artist.get("url")) for artist in top_artists_all_time + top_artists_recent)
                or any(not (album.get("image_url") and album.get("url")) for album in top_albums_all_time + top_albums_recent)
            ):
                _set_load_progress(request, "history favorites (persistent cache enrich)")
                top_artists_all_time = await _enrich_history_artists_from_search(token, top_artists_all_time)
                top_artists_recent = await _enrich_history_artists_from_search(token, top_artists_recent)
                top_albums_all_time = await _enrich_history_albums_from_search(token, top_albums_all_time)
                top_albums_recent = await _enrich_history_albums_from_search(token, top_albums_recent)
                top_artists_all_time_available = bool(top_artists_all_time)
                top_artists_recent_available = bool(top_artists_recent)
                top_albums_all_time_available = bool(top_albums_all_time)
                top_albums_recent_available = bool(top_albums_recent)
        elif live_formula_available:
            cached_live_favorites = _get_short_cache(f"live_favorites_{recent_range}", user_id, item_limit)
            _set_load_progress(
                request,
                "live artist and album formulas (cache hit)"
                if cached_live_favorites is not None
                else "live artist and album formulas (fresh)",
            )
            if cached_live_favorites is not None:
                (
                    top_artists_all_time,
                    top_artists_recent,
                    top_albums_all_time,
                    top_albums_recent,
                ) = cached_live_favorites
            else:
                artist_lookup = _artist_enrichment_lookup(top_artists_all_time + top_artists_recent)
                top_artists_all_time = _normalize_live_top_artists(
                    long_term_top_tracks=top_tracks_all_time,
                    recent_top_tracks=top_tracks_recent,
                    recent_tracks=recent_tracks,
                    liked_tracks=recent_likes_tracks,
                    enrichment_lookup=artist_lookup,
                    mode="all_time",
                )
                top_artists_recent = _normalize_live_top_artists(
                    long_term_top_tracks=top_tracks_all_time,
                    recent_top_tracks=top_tracks_recent,
                    recent_tracks=recent_tracks,
                    liked_tracks=recent_likes_tracks,
                    enrichment_lookup=artist_lookup,
                    mode="recent",
                )
                top_albums_all_time, top_albums_recent = _normalize_live_top_albums(
                    long_term_top_tracks=top_tracks_all_time,
                    recent_top_tracks=top_tracks_recent,
                    recent_tracks=recent_tracks,
                    liked_tracks=recent_likes_tracks,
                )
                _set_short_cache(
                    f"live_favorites_{recent_range}",
                    user_id,
                    item_limit,
                    (
                        top_artists_all_time,
                        top_artists_recent,
                        top_albums_all_time,
                        top_albums_recent,
                    ),
                )
            top_artists_all_time_available = True
            top_artists_recent_available = True
            top_albums_all_time_available = True
            top_albums_recent_available = True
        elif not is_full_analysis and top_tracks_all_time_available:
            top_albums_all_time, _ = _normalize_live_top_albums(
                long_term_top_tracks=top_tracks_all_time,
                recent_top_tracks=[],
                recent_tracks=[],
                liked_tracks=[],
            )
            top_albums_all_time_available = bool(top_albums_all_time)
            top_albums_recent = []
            top_albums_recent_available = False

        if is_full_analysis and is_extended and history_signature and not persistent_history_sections:
            _set_load_progress(request, "history calibration (rebuild)")
            history_insights = load_history_insights(
                settings.spotify_history_dir,
                SECTION_PREVIEW_LIMIT,
                recent_window_days=recent_window_days,
            )
            if history_insights:
                _set_load_progress(request, "history favorites rebuild (search enrichment)")
                artist_lookup = _artist_enrichment_lookup(top_artists_all_time + top_artists_recent)
                album_lookup = _album_enrichment_lookup(
                    top_tracks_all_time + top_tracks_recent + recent_tracks + recent_likes_tracks
                )
                try:
                    history_sections = {
                        "artists_all_time": await _enrich_history_artists_from_search(
                            token,
                            _merge_history_artists(history_insights["artists_all_time"], artist_lookup),
                        ),
                        "artists_recent": await _enrich_history_artists_from_search(
                            token,
                            _merge_history_artists(history_insights["artists_recent"], artist_lookup),
                        ),
                        "albums_all_time": await _enrich_history_albums_from_search(
                            token,
                            _merge_history_albums(history_insights["albums_all_time"], album_lookup),
                        ),
                        "albums_recent": await _enrich_history_albums_from_search(
                            token,
                            _merge_history_albums(history_insights["albums_recent"], album_lookup),
                        ),
                    }
                except HTTPException as exc:
                    if exc.status_code != status.HTTP_502_BAD_GATEWAY:
                        raise
                    _set_load_progress(request, "history favorites rebuild skipped (spotify unavailable)")
                else:
                    _store_persistent_history_cache(history_signature, recent_window_days, history_sections)
                    top_artists_all_time = history_sections["artists_all_time"][:item_limit]
                    top_artists_recent = history_sections["artists_recent"][:item_limit]
                    top_albums_all_time = history_sections["albums_all_time"][:item_limit]
                    top_albums_recent = history_sections["albums_recent"][:item_limit]
                    top_artists_all_time_available = True
                    top_artists_recent_available = True
                    top_albums_all_time_available = True
                    top_albums_recent_available = True
                    history_insights_available = True
                    used_history_backed_results = True

        if is_full_analysis:
            if history_insights is None:
                history_insights = load_history_insights(
                    settings.spotify_history_dir,
                    SECTION_PREVIEW_LIMIT,
                    recent_window_days=recent_window_days,
                )
            if history_insights:
                track_lookup = _track_enrichment_lookup(
                    top_tracks_all_time + top_tracks_recent + recent_tracks + recent_likes_tracks
                )
                history_all_time_tracks = _merge_history_tracks(
                    history_insights.get("tracks_all_time", []),
                    track_lookup,
                )[:HISTORY_TRACKS_DISPLAY_LIMIT]
                if history_all_time_tracks:
                    history_all_time_tracks = await _enrich_tracks_from_spotify(
                        token,
                        history_all_time_tracks,
                        market=profile.get("country"),
                    )
                    top_tracks_all_time = history_all_time_tracks
                    top_tracks_all_time_available = True
                history_recent_tracks = _merge_history_tracks(
                    history_insights.get("tracks_recent", []),
                    track_lookup,
                )[:item_limit]
                if history_recent_tracks:
                    history_recent_tracks = await _enrich_tracks_from_spotify(
                        token,
                        history_recent_tracks,
                        market=profile.get("country"),
                    )
                    top_tracks_recent = history_recent_tracks
                    top_tracks_recent_available = True
                top_tracks_all_time = _apply_track_history_metrics(
                    top_tracks_all_time,
                    history_insights.get("track_history_metrics"),
                    history_insights.get("track_history_metrics_by_key"),
                    count_key="play_count",
                )
                top_tracks_recent = _apply_track_history_metrics(
                    top_tracks_recent,
                    history_insights.get("track_history_metrics"),
                    history_insights.get("track_history_metrics_by_key"),
                    count_key="recent_play_count",
                )
                used_history_backed_results = True

        if (
            is_full_analysis
            and is_extended
            and history_signature
            and used_history_backed_results
        ):
            history_sections_with_tracks = {
                "tracks_all_time": top_tracks_all_time[:HISTORY_TRACKS_DISPLAY_LIMIT],
                "tracks_recent": top_tracks_recent[:item_limit],
                "artists_all_time": top_artists_all_time[:item_limit],
                "artists_recent": top_artists_recent[:item_limit],
                "albums_all_time": top_albums_all_time[:item_limit],
                "albums_recent": top_albums_recent[:item_limit],
            }
            _store_persistent_history_cache(history_signature, recent_window_days, history_sections_with_tracks)

        top_artists_all_time = _hydrate_artists_from_static_cache(top_artists_all_time)
        top_artists_recent = _hydrate_artists_from_static_cache(top_artists_recent)
        top_albums_all_time = _hydrate_albums_from_static_cache(top_albums_all_time)
        top_albums_recent = _hydrate_albums_from_static_cache(top_albums_recent)
        top_artists_all_time, top_albums_all_time = await _backfill_artist_album_images_if_needed(
            token,
            top_artists_all_time,
            top_albums_all_time,
        )
        top_albums_all_time = await _backfill_album_images_if_needed(token, top_albums_all_time)
        if is_full_analysis:
            top_artists_recent, top_albums_recent = await _backfill_artist_album_images_if_needed(
                token,
                top_artists_recent,
                top_albums_recent,
            )
            top_albums_recent = await _backfill_album_images_if_needed(token, top_albums_recent)

        payload = {
            "id": profile.get("id"),
            "display_name": profile.get("display_name"),
            "email": profile.get("email"),
            "product": profile.get("product"),
            "country": profile.get("country"),
            "username": profile.get("id"),
            "followers_total": followers.get("total"),
            "followed_artists_total": followed_artists_total,
            "followed_artists_available": followed_artists_available,
            "followed_artists": top_artists_all_time,
            "followed_artists_list_available": top_artists_all_time_available,
            "recent_top_artists": top_artists_recent,
            "recent_top_artists_available": top_artists_recent_available,
            "top_tracks": top_tracks_all_time,
            "top_tracks_available": top_tracks_all_time_available,
            "recent_top_tracks": top_tracks_recent,
            "recent_top_tracks_available": top_tracks_recent_available,
            "top_albums": top_albums_all_time,
            "top_albums_available": top_albums_all_time_available,
            "recent_top_albums": top_albums_recent,
            "recent_top_albums_available": top_albums_recent_available,
            "analysis_mode": analysis_mode,
            "experience_mode": "full",
            "recent_range": recent_range,
            "recent_window_days": recent_window_days,
            "history_insights_available": history_insights_available,
            "history_first_played_at": history_insights.get("first_played_at") if is_extended and history_insights else None,
            "history_last_played_at": history_insights.get("last_played_at") if is_extended and history_insights else None,
            "history_total_listen_ms": history_insights.get("total_listen_ms") if is_extended and history_insights else None,
            "history_total_play_count": history_insights.get("total_play_count") if is_extended and history_insights else None,
            "extended_loaded": is_extended,
            "top_playlists_recent": top_playlists_recent,
            "top_playlists_all_time": top_playlists_all_time,
            "top_playlists_available": top_playlists_available,
            "profile_url": external_urls.get("spotify"),
            "image_url": images[0].get("url") if images else None,
            "recent_tracks": recent_tracks,
            "recent_tracks_available": recent_tracks_available,
            "owned_playlists": playlists,
            "owned_playlists_available": owned_playlists_available,
            "recent_likes_tracks": recent_likes_tracks,
            "recent_likes_available": recent_likes_available,
        }
        _store_user_profile_snapshot(
            user_id,
            {
                "followed_artists": top_artists_all_time,
                "recent_top_artists": top_artists_recent,
                "top_albums": top_albums_all_time,
                "recent_top_albums": top_albums_recent,
                "owned_playlists": playlists,
                "owned_playlists_available": owned_playlists_available,
                "recent_likes_tracks": recent_likes_tracks,
                "recent_likes_available": recent_likes_available,
                "top_playlists_recent": top_playlists_recent,
                "top_playlists_all_time": top_playlists_all_time,
                "top_playlists_available": top_playlists_available,
            },
        )
        _set_load_progress(request, "finishing")

        return payload
    finally:
        _clear_load_progress(request)
