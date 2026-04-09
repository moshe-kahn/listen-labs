from __future__ import annotations

import base64
import logging
import secrets
from typing import Any
from urllib.parse import urlencode

import httpx
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from starlette.middleware.sessions import SessionMiddleware

from app.config import get_settings

settings = get_settings()
logger = logging.getLogger("listenlab.auth")

app = FastAPI(title="ListenLab API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.allowed_origin],
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


def _is_configured() -> bool:
    return bool(
        settings.spotify_client_id
        and settings.spotify_client_secret
        and settings.spotify_redirect_uri
        and settings.session_secret
    )


def _require_token(request: Request) -> str:
    token = request.session.get("access_token")
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated with Spotify.",
        )
    return token


def _callback_redirect_url(reason: str, detail: str | None = None) -> str:
    query = {"status": reason}
    if detail:
        query["detail"] = detail
    return f"{settings.frontend_url}/auth/callback?{urlencode(query)}"


async def _fetch_spotify_profile(access_token: str) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(
            settings.spotify_me_url,
            headers={"Authorization": f"Bearer {access_token}"},
        )

    if response.status_code == status.HTTP_401_UNAUTHORIZED:
        raise HTTPException(status_code=401, detail="Spotify access token is no longer valid.")
    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail="Failed to fetch Spotify profile.")

    return response.json()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/auth/login")
async def auth_login(request: Request) -> RedirectResponse:
    if not _is_configured():
        raise HTTPException(status_code=500, detail="Spotify OAuth is not configured.")

    state = secrets.token_urlsafe(32)
    request.session["oauth_state"] = state

    query = urlencode(
        {
            "client_id": settings.spotify_client_id,
            "response_type": "code",
            "redirect_uri": settings.spotify_redirect_uri,
            "scope": settings.spotify_scope,
            "state": state,
            "show_dialog": "true",
        }
    )

    return RedirectResponse(url=f"{settings.spotify_authorize_url}?{query}", status_code=302)


@app.get("/auth/callback")
async def auth_callback(request: Request, code: str | None = None, state: str | None = None) -> RedirectResponse:
    expected_state = request.session.get("oauth_state")
    if not code or not state or state != expected_state:
        logger.warning("Spotify callback state validation failed.")
        return RedirectResponse(url=_callback_redirect_url("state_error"), status_code=302)

    credentials = f"{settings.spotify_client_id}:{settings.spotify_client_secret}".encode("utf-8")
    basic_auth = base64.b64encode(credentials).decode("utf-8")

    async with httpx.AsyncClient(timeout=15.0) as client:
        token_response = await client.post(
            settings.spotify_token_url,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": settings.spotify_redirect_uri,
            },
            headers={
                "Authorization": f"Basic {basic_auth}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
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

    request.session.pop("oauth_state", None)
    request.session["access_token"] = access_token
    request.session["refresh_token"] = token_data.get("refresh_token")
    request.session["token_type"] = token_data.get("token_type")
    request.session["expires_in"] = token_data.get("expires_in")

    try:
        profile = await _fetch_spotify_profile(access_token)
    except HTTPException:
        logger.warning("Spotify profile fetch failed after token exchange.")
        return RedirectResponse(url=_callback_redirect_url("profile_error"), status_code=302)

    request.session["spotify_user"] = {
        "id": profile.get("id"),
        "display_name": profile.get("display_name"),
        "email": profile.get("email"),
    }

    return RedirectResponse(url=_callback_redirect_url("success"), status_code=302)


@app.get("/auth/session")
async def auth_session(request: Request) -> dict[str, Any]:
    user = request.session.get("spotify_user") or {}
    authenticated = bool(request.session.get("access_token"))
    return {
        "authenticated": authenticated,
        "display_name": user.get("display_name"),
        "spotify_user_id": user.get("id"),
        "email": user.get("email"),
    }


@app.post("/auth/logout")
async def auth_logout(request: Request) -> dict[str, str]:
    request.session.clear()
    return {"status": "logged_out"}


@app.get("/me")
async def me(request: Request) -> dict[str, Any]:
    token = _require_token(request)
    profile = await _fetch_spotify_profile(token)
    return {
        "id": profile.get("id"),
        "display_name": profile.get("display_name"),
        "email": profile.get("email"),
        "product": profile.get("product"),
        "country": profile.get("country"),
    }
