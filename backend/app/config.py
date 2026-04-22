from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BACKEND_DIR / ".env")


@dataclass(frozen=True)
class Settings:
    spotify_client_id: str
    spotify_client_secret: str
    spotify_redirect_uri: str
    listenlab_token_encryption_key: str
    frontend_url: str
    session_secret: str
    allowed_origin: str
    allowed_origins: list[str]
    spotify_history_dir: str
    cache_dir: str
    sqlite_db_path: str
    spotify_recent_full_page_mode: bool = True
    spotify_scope: str = (
        "user-read-email user-read-private user-read-recently-played playlist-read-private "
        "user-follow-read user-library-read user-top-read streaming user-modify-playback-state "
        "user-read-playback-state user-read-currently-playing"
    )

    @property
    def spotify_authorize_url(self) -> str:
        return "https://accounts.spotify.com/authorize"

    @property
    def spotify_token_url(self) -> str:
        return "https://accounts.spotify.com/api/token"

    @property
    def spotify_me_url(self) -> str:
        return "https://api.spotify.com/v1/me"


def _read_env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _read_env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    normalized = str(raw).strip().lower()
    if normalized in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "f", "no", "n", "off"}:
        return False
    return bool(default)


def get_settings() -> Settings:
    default_origin = _read_env("ALLOWED_ORIGIN", "http://127.0.0.1:5173")
    configured_origins = _read_env("ALLOWED_ORIGINS", "")
    if configured_origins:
        allowed_origins = [origin.strip() for origin in configured_origins.split(",") if origin.strip()]
    else:
        allowed_origins = [default_origin, "http://localhost:5173"]
    if default_origin not in allowed_origins:
        allowed_origins.insert(0, default_origin)

    return Settings(
        spotify_client_id=_read_env("SPOTIFY_CLIENT_ID"),
        spotify_client_secret=_read_env("SPOTIFY_CLIENT_SECRET"),
        spotify_redirect_uri=_read_env("SPOTIFY_REDIRECT_URI", "http://127.0.0.1:8000/auth/callback"),
        listenlab_token_encryption_key=_read_env("LISTENLAB_TOKEN_ENCRYPTION_KEY"),
        frontend_url=_read_env("FRONTEND_URL", "http://127.0.0.1:5173"),
        session_secret=_read_env("SESSION_SECRET", "change-me"),
        allowed_origin=default_origin,
        allowed_origins=allowed_origins,
        spotify_history_dir=_read_env(
            "SPOTIFY_HISTORY_DIR",
            "C:\\Users\\kahnt\\OneDrive\\Programming\\Projects\\ListenLab\\Spotify Extended Streaming History",
        ),
        cache_dir=_read_env("CACHE_DIR", str(BACKEND_DIR / "data" / "cache")),
        sqlite_db_path=_read_env("SQLITE_DB_PATH", str(BACKEND_DIR / "data" / "listenlabs.sqlite3")),
        spotify_recent_full_page_mode=_read_env_bool("SPOTIFY_RECENT_FULL_PAGE_MODE", True),
    )
