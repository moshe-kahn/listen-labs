# ListenLab Architecture

## Purpose
This document is the implementation-oriented technical source of truth for the ListenLab MVP. It is written for AI coding agents first and human collaborators second.

## Current State vs Target State
### Current state
- The repository includes a React dashboard and FastAPI backend running locally.
- Spotify OAuth login, callback handling, session persistence, and authenticated `GET /me` profile loading are implemented.
- The dashboard currently renders profile identity, playlists, recent listening, liked tracks, top tracks, top artists, and top albums.
- The dashboard also includes playback controls plus a local/full/test mode model for working through Spotify rate limits and local-only sessions.
- The auth layer now also supports a dedicated recent-ingest OAuth path with PKCE plus probe and poll-now endpoints for recent-play API debugging.
- A local exported-history analyzer can calibrate artist and album rankings from Spotify extended streaming history when a history directory is configured.
- The dashboard uses a dedicated post-login loading screen, then swaps into a sticky-navigation dashboard shell.
- The frontend now includes a tracks-only comparison page for evaluating current vs new all-time track ranking formulas.
- Backend section-level caching is implemented for moderate-freshness live sections, long-lived history-derived favorites, shared static Spotify metadata, and saved user snapshot sections for local mode.
- A local SQLite database now stores raw play events, live playback observations, ingest runs, and Spotify recent-sync state.
- Encrypted Spotify token persistence now supports token-backed session restore for returning users.
- Spotify recent-play API ingest is implemented with replay overlap handling, conservative early-stop paging, and batch chronology-based `ms_played` upgrades.
- Spotify extended history JSON ingest is implemented into the same raw-play table, with cross-source upgrade support from API-estimated rows to source-truth rows.
- Raw duplicate-member tracking, ingest-run cleanup helpers, current-playback observation, and unified top-track SQLite queries are now implemented on top of the ingest foundation.
- The core overlooked-artist analysis flow and playlist creation flow are still not implemented.

### Target MVP state
- A React single-page app handles login, analysis actions, result display, and playlist creation controls.
- A FastAPI backend handles Spotify OAuth, Spotify API orchestration, aggregation, scoring, explanation generation, and playlist creation.
- Spotify is the source of truth for user library and listening-related signals.
- Analysis is computed on demand for the active session.
- A local SQLite database may persist raw ingest and sync-state data for analysis and calibration support.
- Local development is the primary target, with a later path to simple cloud hosting for the frontend and a single backend service.

## Architectural Principles
- Use real listening evidence, not recommendation inference.
- Keep scoring modular and configurable.
- Separate Spotify data access from aggregation and scoring logic.
- Always return an explanation for each surfaced result.
- Prefer a small number of reliable system parts over early optimization.
- Design for degraded-but-useful results when Spotify signal availability is limited.
- Preserve provenance for raw listening data and make upgrades explicit when better source quality arrives later.

## MVP System Overview
### Runtime shape
- Browser client: React SPA
- API server: FastAPI application
- External dependency: Spotify Web API
- State model: session-based auth, on-demand computation, plus local SQLite/raw-cache persistence

### Implemented today
- frontend authenticated dashboard shell
- frontend callback handling
- frontend loading handoff after Spotify auth
- frontend persistent sticky navigation with project/account popovers
- frontend playback controls and player state presentation
- frontend local/full/test mode controls with cached-state indicators
- frontend tracks-only comparison page for current vs new all-time ranking formulas
- frontend recent-ingest controls for connect+ingest, before-cursor probe, backfill probe, and post-track-end polling
- backend OAuth endpoints
- backend token exchange and session storage
- backend encrypted Spotify token persistence and token-backed session restore
- backend PKCE code-challenge handling for Spotify auth flows
- authenticated `GET /me` snapshot endpoint
- authenticated `GET /me/progress` timing endpoint for debugging load phases
- authenticated recent-ingest result, probe, and poll-now endpoints
- authenticated `POST /cache/rebuild` endpoint for clearing dashboard caches before reconnect
- best-effort live Spotify data fetches for profile, playlists, recent listening, liked tracks, top tracks, top artists, and top albums
- optional local history-based artist and album ranking calibration
- section-level caching for live sections and persistent history-based favorites
- on-disk local analysis cache, per-user snapshot cache, and shared static metadata cache for artists, albums, and tracks
- SQLite schema migrations and `raw_play_event`, `raw_play_event_membership`, `live_playback_event`, `ingest_run`, and `spotify_sync_state` tables
- raw ingest helpers with source-row dedupe plus conservative cross-source upgrade matching
- ingest-run listing, fetch, and deletion helpers plus a unified top-track query on raw data
- history JSON file loader and batch history ingest path
- backend scripts for data-foundation validation, ingest-run hygiene smoke checks, current-playback polling, and recent-play probing

### High-level flow
1. The user opens the React app and starts Spotify login.
2. The backend runs OAuth with Spotify and stores session state for the logged-in user.
3. The frontend calls `GET /auth/session` and `GET /me`.
4. The backend fetches fresh recent sections, short-cache live ranking sections, and optionally serves persistent history-derived favorites or local snapshot sections when valid.
5. The frontend first renders a loading handoff, then the dashboard snapshot, then optionally fills in the extended view.
6. When Spotify is unavailable or the user explicitly chooses local mode, the backend returns a locally assembled payload backed by history and cached Spotify-derived metadata.
7. A later milestone will add `POST /analysis` and `POST /playlist`.

## Raw Ingest Architecture
### Current raw tables
- `ingest_run`
  - records run lifecycle, row counts, inserted counts, duplicate counts, and error counts
- `spotify_sync_state`
  - stores the recent-sync watermark, overlap lookback, and current/latest sync run metadata
- `raw_play_event`
  - stores the raw play event plus ingest provenance and duration quality method
- `raw_play_event_membership`
  - stores every seen `source_row_key` that maps to a canonical `raw_play_event`
- `live_playback_event`
  - stores observational current-playback snapshots separately from durable canonical play history

### `raw_play_event` design
Important fields currently include:
- source identity
  - `source_type`
  - `source_event_id`
  - `source_row_key`
  - `cross_source_event_key`
- play timing and duration
  - `played_at`
  - `ms_played`
  - `ms_played_method`
  - `duplicate_row_count`
  - `duplicate_merge_strategy`
  - `track_duration_ms`
- raw context
  - `reason_start`
  - `reason_end`
  - `skipped`
  - `platform`
  - `shuffle`
  - `offline`
  - `conn_country`
- Spotify metadata
  - `spotify_track_uri`
  - `spotify_track_id`
  - `spotify_album_id`
  - `spotify_artist_ids_json`
  - `track_name_raw`
  - `artist_name_raw`
  - `album_name_raw`
- payload retention
  - `raw_payload_json`

### Raw duration provenance
`ms_played_method` currently allows:
- `history_source`
- `api_chronology`
- `default_guess`

Precedence:
- `history_source > api_chronology > default_guess`

Upgrade behavior:
- exact `source_row_key` membership match first
- if no exact source-row match, try `cross_source_event_key`
- upgrade the existing row only when the incoming method outranks the stored method
- when the same logical play is seen from another source, attach a membership row and keep one canonical raw event

### Conservative cross-source key
The current `cross_source_event_key` is built from:
- canonical UTC `played_at`
- `spotify_track_id` if available
- otherwise `spotify_track_uri`

This is intentionally conservative and avoids fuzzy title/artist matching for now.

## Recommended Project Layout
This layout should be used when scaffolding the repository:

```text
backend/
  app/
    api/
    clients/
    core/
    models/
    schemas/
    services/
    main.py
frontend/
  src/
    app/
    api/
    features/
      analysis/
      auth/
      playlist/
    types/
docs/
```

## Frontend Responsibilities
The React SPA is responsible for:
- showing authenticated vs unauthenticated states
- starting the Spotify login flow
- showing a dedicated loading state while the first dashboard snapshot is computed
- providing an "Analyze my listening" action
- rendering ranked overlooked artists
- rendering explanation text and signal breakdowns
- allowing artist selection for playlist generation
- handling loading, empty, and degraded-signal states cleanly

### Frontend constraints
- The frontend must not implement scoring logic.
- The frontend should treat backend responses as the source of ranking truth.
- MVP should use a simple route structure and minimal global state.
- Results should be easy to scan and explanation-first.

## Backend Responsibilities
The FastAPI backend is responsible for:
- Spotify OAuth start and callback handling
- secure session management for the active user
- fetching Spotify data from required and optional endpoints
- splitting dashboard sections by freshness and cache policy
- normalizing source data into artist-level aggregates
- computing engagement scores through a dedicated scoring service
- generating plain-language explanations
- creating playlists from selected artists

### Backend constraints
- Keep services composable and testable in isolation.
- Keep Spotify API access behind client or adapter interfaces.
- Return stable response shapes even when some signals are unavailable.
- Keep raw ingest semantics explicit: source-row idempotency first, cross-source upgrades second.

## Session and Persistence Model
### MVP choice
- Use server-side in-memory session storage keyed by a signed session cookie.
- Store Spotify access token, refresh token, expiry metadata, and minimal user identity in the session.
- Assume a single backend instance for MVP and early cloud deployment.
- Use local filesystem cache files only for dashboard caches, shared static metadata, local analysis payloads, and user snapshot fallbacks, not as a general-purpose user database.
- Use the local SQLite database for raw play events and sync-state persistence.

### Implications
- Sessions may be lost on server restart in local development.
- Horizontal scaling is out of scope for MVP.
- Moving to Redis or a database-backed session store is a future enhancement, not an MVP requirement.
- Persistent dashboard caches should be treated as disposable runtime artifacts and can be rebuilt.

## Dashboard Cache Strategy
### Fresh sections
- recently played tracks
- recently liked tracks
- other current-activity style sections

### Short-cache sections
- live top tracks
- live top artists
- live top albums
- playlist summaries
- followed-artist totals

### Persistent cache sections
- history-calibrated artist favorites
- history-calibrated album favorites
- stable image and URL enrichment for those history-ranked results
- local history insights keyed by history signature and time window
- per-user saved Spotify-only sections used by local mode
- shared static Spotify metadata for artists, albums, and tracks

### Invalidation
- Short-cache entries expire by TTL.
- Persistent history cache is invalidated when the Spotify exported-history file signature changes or when `POST /cache/rebuild` is called.
- Shared static metadata is schema-versioned, fail-safe on corrupt JSON, and bounded per bucket with deterministic trim rules.
- Generated cache files under `backend/data/cache/` are runtime artifacts and should not be committed.
- Validation databases under `backend/data/validation/` and SQLite copy files under `backend/data/` are also local-only runtime artifacts.

## Internal Service Boundaries
### Spotify client or adapters
- Responsible only for calling Spotify endpoints, handling pagination, refreshing tokens, and normalizing low-level API errors.
- Must not contain scoring or business rules.

### Raw ingest services
- Map source-specific payloads into the raw-play shape.
- Preserve canonical UTC `played_at` formatting across sources.
- Keep batch chronology inference in orchestration code, not one-item mappers.
- Keep source-specific idempotency and cross-source upgrade rules in DB helpers.

### Aggregation pipeline
- Converts liked tracks, saved albums, followed artists, and listening proxies into artist-level records.
- Produces `ArtistProfile` objects with raw counts and derived signal fields.

### Scoring service
- Accepts normalized artist profiles and scoring configuration.
- Computes engagement scores independently of fetch logic.
- Supports rich-signal and fallback modes without changing the public result shape.

### Explanation builder
- Converts scoring inputs and filtered results into concise evidence-based text.
- Explains why an artist was surfaced using the strongest available signals.

### Playlist service
- Takes selected artist IDs, fetches top tracks, deduplicates tracks, and creates the Spotify playlist.

## API Surface
The MVP backend should expose the following endpoints.

### `GET /health`
Purpose:
- Simple local and deployment health check.

Response:
- `200 OK` with a minimal status payload.

### `GET /auth/login`
Purpose:
- Start Spotify OAuth.

Behavior:
- Generate OAuth state.
- Generate PKCE verifier/challenge material.
- Accept an optional recent-ingest mode with a narrower scope.
- Redirect the browser to Spotify authorization.

### `GET /auth/callback`
Purpose:
- Complete Spotify OAuth.

Behavior:
- Validate OAuth state.
- Exchange the authorization code for tokens.
- Create or update the server-side session.
- In recent-ingest mode, run a recent-play sync immediately and stash the result in session for the frontend.
- Redirect back to the frontend app.

### `GET /auth/recent-ingest/result`
Purpose:
- Return the one-shot result of the recent-ingest auth flow.

### `GET /auth/recent-ingest/probe-before`
Purpose:
- Probe Spotify recently-played with a bounded `before` cursor for API-behavior debugging.

### `GET /auth/recent-ingest/probe-backfill`
Purpose:
- Walk multiple recently-played pages and summarize cursor behavior during backfill debugging.

### `POST /auth/recent-ingest/poll-now`
Purpose:
- Trigger a one-shot recent-play poll after live playback UI detection suggests a track just ended.

### `GET /auth/session`
Purpose:
- Return current authentication state for the SPA.

Response shape:
- `authenticated: boolean`
- `display_name: string | null`
- `spotify_user_id: string | null`

### `POST /auth/logout`
Purpose:
- Clear the active session.

### `GET /me`
Purpose:
- Return the authenticated dashboard snapshot for the current user.

Implemented data today:
- profile identity and Spotify profile URL
- playlists owned by the user and marked public by Spotify
- recently played tracks
- recently liked tracks
- top tracks
- top artists
- top albums
- optional `history_insights_available` flag when local exported history is being used to rank artists and albums
- optional local-mode metadata such as cached-section status and last-sync timestamps
- `mode=initial` for first-paint snapshot and `mode=extended` for larger background payloads

### Raw ingest operations
These are currently internal helpers, not stable public API endpoints.

Implemented internal flows:
- Spotify recent-play sync
- Spotify history JSON file ingest

Current internal guarantees:
- recent-play sync replays an overlap window
- known rows inside the overlap window are still processed so `default_guess -> api_chronology` upgrades can happen
- older known rows beyond the overlap cutoff stop paging
- history ingest uses the same raw upsert/upgrade rules as recent-play ingest

### `GET /me/progress`
Purpose:
- Return in-flight dashboard load timing information for the active session.

Notes:
- Used for debugging and load instrumentation.
- Progress is also appended to a local runtime log file under `backend/data/cache/dashboard-progress.log`.

### `POST /cache/rebuild`
Purpose:
- Clear short-lived dashboard caches and persistent history-derived caches.

Notes:
- Used by the reconnect flow to force a cold-cache dashboard rebuild.

### `POST /analysis`
Purpose:
- Run the full ListenLab analysis for the current user.

Status:
- Planned, not implemented yet.

Request body:
```json
{
  "limit": 25
}
```

Response shape:
```json
{
  "generated_at": "2026-04-08T00:00:00Z",
  "scoring_mode": "rich_signals",
  "results": []
}
```

Notes:
- `limit` is optional and should default to a sensible UI count such as 25.
- Scoring weights are configured server-side in MVP and are not a public API surface yet.

### `POST /playlist`
Purpose:
- Create a playlist from selected surfaced artists.

Status:
- Planned, not implemented yet.

Request body:
```json
{
  "name": "Overlooked Favorites",
  "artist_ids": ["artist_1", "artist_2"],
  "tracks_per_artist": 2
}
```

Response shape:
```json
{
  "playlist_id": "spotify_playlist_id",
  "playlist_name": "Overlooked Favorites",
  "playlist_url": "https://open.spotify.com/playlist/...",
  "added_track_count": 4,
  "skipped_artist_ids": []
}
```

## Core Data Contracts
### `ArtistProfile`
Internal artist-level aggregate used before and after scoring.

Fields:
- `artist_id: string`
- `artist_name: string`
- `is_followed: boolean`
- `liked_track_count: int`
- `saved_album_count: int`
- `play_count: int`
- `listening_minutes: float`
- `recent_play_count: int`
- `recent_listening_minutes: float`
- `engagement_score: float | null`

Rules:
- Numeric counts should default to `0`, not `null`.
- When a signal is unavailable, the profile still exists with zero-value fields; availability is explained through the breakdown contract.

### `EngagementSignalBreakdown`
Structured explanation of how the score was formed.

Fields:
- `formula_version: string`
- `listening_minutes: { raw: float, weight: float, weighted: float, status: string }`
- `play_count: { raw: int, weight: float, weighted: float, status: string }`
- `liked_track_count: { raw: int, weight: float, weighted: float, status: string }`
- `saved_album_count: { raw: int, weight: float, weighted: float, status: string }`
- `recency_bonus: { raw: float, weight: float, weighted: float, status: string }`
- `total_score: float`

Status values:
- `observed`
- `estimated`
- `derived`
- `unavailable`

### `OverlookedArtistResult`
Public analysis result returned to the frontend.

Fields:
- `artist_id: string`
- `artist_name: string`
- `engagement_score: float`
- `explanation: string`
- `breakdown: EngagementSignalBreakdown`

Rules:
- Only artists with `is_followed = false` are returned.
- Explanation text must cite the strongest available signals and remain evidence-based.

### `PlaylistCreationRequest`
Payload sent from the frontend to the backend.

Fields:
- `name: string`
- `artist_ids: string[]`
- `tracks_per_artist: int`

Rules:
- `artist_ids` should come from the current analysis result set.
- `tracks_per_artist` should default to `2` in the UI.

### `PlaylistCreationResult`
Result returned after successful playlist creation.

Fields:
- `playlist_id: string`
- `playlist_name: string`
- `playlist_url: string`
- `added_track_count: int`
- `skipped_artist_ids: string[]`

## Spotify Data Strategy
### Required Spotify data
- liked tracks from `/me/tracks`
- followed artists from `/me/following`
- recent listening from `/me/player/recently-played`
- top artists from `/me/top/artists`
- top tracks from `/me/top/tracks`
- playlists from `/me/playlists`
- top tracks for playlist creation from `/artists/{id}/top-tracks`

### Optional or best-effort Spotify data
- saved albums from `/me/albums` in later milestones
- local extended streaming history export for calibration and richer artist/album ranking
- local extended streaming history export for raw event ingestion and cross-source upgrades
- best-effort album enrichment through lightweight Spotify album search when history-ranked albums need images and URLs
- playback state and related controls when Spotify allows active player access

### Local history calibration path
- When `SPOTIFY_HISTORY_DIR` points to a valid Spotify extended streaming history export, the backend loads the local JSON files and derives artist and album rankings from them.
- This path is meant to calibrate formulas and support power-user local development.
- It must not become a hard dependency for the MVP, because most users will only provide live Spotify API access.
- Final history-ranked sections are cached as payloads so they can be reused without reparsing the export on every load.
- The same local-history path now also supports restricted local mode, where navigation stays available without making new Spotify requests.

## Known Open Issues
- Album breadth and eligibility still need refinement; some albums remain undercounted or overcounted depending on the path.
- The history-derived count for "Chronicles of a Diamond" is still incorrect and must be fixed before album rankings are considered reliable.
- Local mode still loses too many artist and album images on some transitions, which means snapshot and static-cache hydration is incomplete.
- Recent album lists for 4-week and 6-month views can still collapse to only one item for some accounts.

### Two scoring paths
#### Rich-signal path
- Use observed listening minutes, recent activity, and play-based signals when available.

#### Fallback path
- Use proxies such as play frequency indicators, liked tracks, saved albums, and recency heuristics.

Requirement:
- Both paths must produce the same public result schema.

## Filtering and Ranking Rules
- Aggregate all artist signals before filtering.
- Exclude followed artists from surfaced overlooked-artist results.
- Rank remaining artists by engagement score descending.
- Break score ties by stronger listening-minute signal, then stronger play-count signal, then artist name.

## Error Handling Expectations
- Unauthenticated requests to `POST /analysis` and `POST /playlist` should return `401`.
- Spotify API failures should be translated into clear backend errors without leaking raw provider payloads.
- Partial data availability should degrade results, not crash analysis.
- The frontend should receive stable response shapes even when zero results are returned.

## Deployment Model
### Local development
- Run the React app locally.
- Run the FastAPI server locally.
- Use Spotify developer app credentials via environment variables.

### Simple cloud target
- Host the React app as a static SPA.
- Host the FastAPI app as a single web service.
- Keep one backend instance for MVP so in-memory sessions remain valid.

## Non-Goals for MVP
- no recommendation engine based on similarity or genre
- no background jobs or scheduled sync
- no multi-user admin features
- no concert, album completion, or social features

## Implementation Defaults
- Frontend framework: React
- Backend framework: FastAPI
- Auth provider: Spotify OAuth
- Persistence: in-memory session storage plus local SQLite/raw-cache persistence
- Ranking logic owner: backend scoring service
- Explanation owner: backend explanation service
