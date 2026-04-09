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
    frontend_url: str
    session_secret: str
    allowed_origin: str
    spotify_scope: str = "user-read-email user-read-private"

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


def get_settings() -> Settings:
    return Settings(
        spotify_client_id=_read_env("SPOTIFY_CLIENT_ID"),
        spotify_client_secret=_read_env("SPOTIFY_CLIENT_SECRET"),
        spotify_redirect_uri=_read_env("SPOTIFY_REDIRECT_URI", "http://localhost:8000/auth/callback"),
        frontend_url=_read_env("FRONTEND_URL", "http://localhost:5173"),
        session_secret=_read_env("SESSION_SECRET", "change-me"),
        allowed_origin=_read_env("ALLOWED_ORIGIN", "http://localhost:5173"),
    )
