# ListenLab Roadmap

## Purpose
This roadmap translates the product brief into buildable engineering milestones. It is intentionally implementation-oriented and should be used alongside `docs/architecture.md` and `docs/context.md`.

## Planning Defaults
- The repository now has a working authenticated dashboard foundation.
- MVP uses a React frontend and FastAPI backend.
- Spotify is the source of truth.
- MVP now uses a local SQLite database for raw ingest and sync-state persistence.
- Local development comes first, with a simple cloud deployment path later.
- The current dashboard also supports local cached operation when Spotify is rate-limited or intentionally disabled.

## MVP Boundary
The MVP includes:
- Spotify authentication
- artist-level data aggregation
- engagement scoring with fallback behavior
- overlooked artist ranking and explanations
- playlist creation from surfaced artists

The MVP does not include:
- album completion detection
- listening-insights dashboards
- concert integrations
- advanced personalization controls

## Milestone 0 - Scope Lock and Project Foundation
### Purpose
Lock the MVP shape and establish the initial project structure so implementation can begin cleanly.

### Dependencies
- none

### Deliverables
- architecture, context, and roadmap docs
- agreed frontend/backend split
- initial repository layout for `frontend/` and `backend/`
- environment variable plan for Spotify credentials and app URLs

### Completion criteria
- implementers do not need to guess the stack, service boundaries, or MVP scope
- local project layout is ready for scaffolding work

## Milestone 1 - OAuth and Spotify Connectivity
### Purpose
Enable a user to sign in with Spotify and give the backend a valid authenticated session.

### Dependencies
- Milestone 0

### Deliverables
- Spotify app configuration documented in local setup
- `GET /auth/login`, `GET /auth/callback`, `GET /auth/session`, and `POST /auth/logout`
- backend token refresh handling
- frontend auth state handling and login entry point

### Completion criteria
- a user can log in locally through Spotify
- the frontend can confirm authenticated state through the backend
- the backend can make authenticated Spotify API calls on behalf of the user

### Current status
- Implemented and manually verified in local development.

## Milestone 2 - Authenticated Snapshot Dashboard
### Purpose
Ship a trustworthy authenticated dashboard that exposes the raw listening signals we plan to build analysis on top of.

### Current status
- Implemented locally and iterated beyond the original auth milestone.

### Dependencies
- Milestone 1

### Deliverables
- connected Spotify profile snapshot
- recent listening and liked tracks sections
- playlist section for owned public playlists
- top artists, top tracks, and top albums sections
- resilient fallback behavior for missing Spotify scopes and rate limits

### Completion criteria
- a user can log in and see a stable dashboard without using developer tools
- UI layout remains aligned even with variable content lengths
- the dashboard gracefully degrades when Spotify data is partial

### Implemented additions beyond the original milestone
- local mode, full mode, and test-mode switching for Spotify availability handling
- local snapshot fallback for selected Spotify-only sections
- on-disk cache layers for local history insights, per-user recent sections, and shared static metadata
- playback controls and related player UI states
- dedicated recent-ingest auth/probe controls for validating recently-played API behavior
- a tracks-only comparison page for current vs new all-time ranking formulas
- multiple dashboard and popover usability refinements

## Milestone 3 - Data Collection and Artist Aggregation
### Purpose
Collect the core library signals and normalize them into artist-level records.

### Current status
- Partially started through dashboard-oriented fetches.
- Raw listening ingest is now substantially further along:
  - recent-play API ingest exists
  - history-dump ingest exists
  - same-source and cross-source raw upgrades exist
- The true artist aggregation pipeline for overlooked-artist analysis is still not implemented yet.

### Dependencies
- Milestone 2

### Deliverables
- Spotify client support for liked tracks, saved albums, and followed artists
- pagination handling for required Spotify endpoints
- raw event ingestion from Spotify recent-play API into SQLite
- raw event ingestion from Spotify extended streaming history into SQLite
- conservative replay overlap and early-stop paging for recent-play sync
- source-row idempotency plus conservative cross-source upgrade matching
- canonical-event membership tracking for duplicate source rows
- live playback observation capture as a separate evidence layer
- aggregation pipeline that produces `ArtistProfile` records
- unit-tested normalization logic for multi-artist tracks and album relationships

### Completion criteria
- recent-play sync can ingest new rows and safely replay overlap windows
- history-dump import can upgrade weaker recent-play estimates into source-truth rows
- the backend can build a complete artist-level aggregate from the required MVP data
- followed state, liked tracks, and saved album counts are reflected correctly per artist

## Milestone 4 - Listening Signals and Fallback Model
### Purpose
Add listening-behavior signals while preserving useful output when rich data is unavailable.

### Dependencies
- Milestone 2

### Deliverables
- best-effort support for recent listening or play-based proxy data
- derived listening-minute and play-count fields on `ArtistProfile`
- explicit scoring-mode selection between rich-signal and fallback execution
- tests covering partial signal availability
- chronology-based `ms_played` improvement for recent-play API rows
- explicit duration provenance with method precedence:
  - `history_source > api_chronology > default_guess`

### Completion criteria
- the analysis pipeline can run with rich signals when available
- the same pipeline can still return valid ranked results when those signals are limited

## Milestone 5 - Scoring, Filtering, and Explanation Generation
### Purpose
Turn aggregated artist data into ranked overlooked-artist results.

### Dependencies
- Milestone 3

### Deliverables
- modular scoring service with configurable weights
- followed-artist filtering
- ranking logic and deterministic tie-breaks
- explanation builder that produces user-facing reasoning
- `POST /analysis` response shape finalized

### Completion criteria
- analysis returns only non-followed artists
- results are ranked by engagement score
- each surfaced artist includes a clear explanation and structured breakdown

## Milestone 6 - Frontend MVP Experience
### Purpose
Ship the core user experience for running analysis and reading results.

### Dependencies
- Milestone 4

### Deliverables
- authenticated app shell
- analyze action and loading states
- ranked results view for overlooked artists
- explanation and signal breakdown display
- empty-state and error-state handling

### Completion criteria
- a user can log in, run analysis, and understand the results without using developer tools
- the frontend behavior stays stable across loading, empty, and degraded-signal states

## Milestone 7 - Playlist Creation
### Purpose
Turn useful analysis into an immediate action inside Spotify.

### Dependencies
- Milestone 5

### Deliverables
- UI for selecting surfaced artists
- `POST /playlist` backend endpoint
- Spotify top-track fetch logic per selected artist
- playlist creation success and failure handling

### Completion criteria
- a user can create a Spotify playlist from surfaced artists
- the playlist result includes a usable Spotify URL
- duplicate or unavailable track scenarios are handled safely

## Milestone 8 - Refinement and Launch Readiness
### Purpose
Improve quality until the MVP feels obviously trustworthy on real accounts.

### Dependencies
- Milestone 6

### Deliverables
- score tuning against real-user test accounts
- formula calibration using exported Spotify extended streaming history when available
- documented live-only scoring formulas derived from calibration work
- copy improvements for explanations and UI labels
- bug fixes for edge cases in data aggregation and session handling
- local deployment checklist and cloud deployment notes

### Completion criteria
- results feel consistently credible across multiple accounts
- explanation text reads clearly
- the team has enough setup guidance to run and demo the app reliably

## Immediate Follow-Ups
These are active issues discovered during current dashboard work and should be treated as near-term tasks before more product expansion.

- Fix album ranking so recent 4-week and 6-month album lists do not collapse to a single entry when broader listening exists.
- Fix album breadth counting so duplicate track variants do not inflate album track totals.
- Specifically fix the incorrect history count still shown for "Chronicles of a Diamond."
- Improve local-mode image persistence so artist and album artwork remains available after switching out of full mode.
- Continue tightening documentation and instrumentation around cache behavior and local-mode freshness.
- Continue tightening documentation and instrumentation around raw ingest performance and batch import timing.

## Current Raw Ingest Status
- `raw_play_event`, `ingest_run`, and `spotify_sync_state` are implemented in SQLite.
- `ms_played_method` is implemented with:
  - `history_source`
  - `api_chronology`
  - `default_guess`
- recent-play API rows can upgrade from `default_guess` to `api_chronology`
- history-dump rows can upgrade API rows to `history_source`
- duplicate source rows can be attached to one canonical event through `raw_play_event_membership`
- current conservative cross-source identity is:
  - canonical UTC `played_at`
  - plus `spotify_track_id` if available, otherwise `spotify_track_uri`
- current history-file import path has instrumentation for:
  - file discovery
  - per-file read and parse time
  - mapping time
  - DB ingest time
  - total elapsed time
- current supporting utilities also include:
  - ingest-run hygiene smoke testing
  - isolated data-foundation validation runs
  - current-playback polling and per-user diagnostics
  - recent-play API probe scripts

## Post-MVP Backlog
These items are explicitly deferred until after the MVP is stable.

### Phase 2.1 - Album Completion Detector
- Surface albums where the user has partial listening evidence but has not saved the album.

### Phase 2.2 - Listening Behavior Insights
- Add secondary views for patterns such as all-time vs recent behavior or high-play low-interaction artists.

### Phase 2.3 - Concert Awareness
- Explore third-party integrations for concert discovery near the user.

## Roadmap Validation Checklist
The roadmap is ready to execute when:
- each milestone maps cleanly to the architecture doc
- each milestone has clear exit criteria
- no MVP milestone requires adding a database
- post-MVP items remain clearly outside the initial delivery scope
