# Authenticated Dashboard Run Notes

## What this milestone now includes
- React frontend dashboard with Spotify login
- frontend callback handling
- frontend loading screen after successful Spotify auth
- sticky dashboard navigation with section jump controls
- local/full/test mode controls for Spotify availability and rate-limit fallback
- playback controls and player-state UI
- FastAPI backend with Spotify OAuth endpoints
- backend token exchange and session storage
- authenticated `GET /me` snapshot endpoint
- authenticated `GET /me/progress` timing endpoint for load debugging
- `POST /cache/rebuild` endpoint used by reconnect to force a cold rebuild
- dashboard sections for playlists, recent activity, top tracks, top artists, and top albums
- optional local history calibration for artist and album rankings
- section-level caching for live sections and persistent history-derived favorites
- saved local snapshots for Spotify-only sections plus shared static metadata cache for artwork and URLs
- local SQLite raw-ingest foundation for recent-play sync, history import, and sync-state tracking

## Status
- Implemented
- Manually verified locally against Spotify OAuth
- Uses `127.0.0.1` consistently for backend, frontend, and Spotify redirect configuration

## Backend setup
1. Copy `backend/.env.example` to `backend/.env`.
2. Fill in Spotify app credentials.
3. Install dependencies:

```bash
py -m pip install -r backend/requirements.txt
```

4. Run the API:

```bash
py -m uvicorn app.main:app --reload --app-dir backend
```

## Frontend setup
1. Copy `frontend/.env.example` to `frontend/.env`.
2. Install dependencies:

```bash
npm install --prefix frontend
```

3. Run the app:

```bash
npm run dev --prefix frontend
```

## Spotify app settings
Set the Spotify redirect URI to:

```text
http://127.0.0.1:8000/auth/callback
```

## Manual verification flow
1. Open the frontend at `http://127.0.0.1:5173`.
2. Click `Log in with Spotify`.
3. Complete Spotify authorization.
4. Confirm you return to a loading screen rather than the generic signed-out dashboard shell.
5. Confirm the dashboard loads profile data from Spotify after the loading handoff.
6. Confirm playlists, recent activity, and top sections appear without repeated auth errors.
7. If local history calibration is configured, confirm top artists and albums reflect exported listening history.
8. If reconnect is used, confirm the dashboard rebuilds from a cleared cache.

## Optional local history calibration
If you have a Spotify extended streaming history export locally, set:

```text
SPOTIFY_HISTORY_DIR=C:\path\to\Spotify Extended Streaming History
```

This is optional and intended for local calibration and richer artist/album ranking. The product should still work for users who only provide live Spotify API access.

## Runtime cache notes
- Short-lived dashboard caches and persistent history-ranked favorites are stored under `backend/data/cache/` at runtime.
- `history_sections.json` stores the final cached history-ranked payload, not just raw inputs.
- `local_history_insights.json` stores precomputed local-analysis payloads keyed by history signature and window.
- `user_recent_sections.json` stores per-user recent-section snapshots used for fallback behavior.
- `user_profile_snapshots.json` stores selected saved Spotify-only sections for local-mode browsing.
- `spotify_static_metadata.json` stores shared static artist, album, and track metadata for reuse across users.
- `dashboard-progress.log` records load timing phases for debugging local performance.
- These files are runtime artifacts and should not be committed.

## Known next step
- turn the current snapshot dashboard into the final overlooked-artist analysis experience with explanation-first ranking
- fix outstanding album-ranking and local-image hydration bugs uncovered during the dashboard expansion work
- continue improving raw ingest performance and use the resulting SQLite data as the base for later scoring work
