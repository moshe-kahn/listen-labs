# ListenLab Roadmap

## Purpose
This roadmap translates the product brief into buildable engineering milestones. It is intentionally implementation-oriented and should be used alongside `docs/architecture.md` and `docs/context.md`.

## Planning Defaults
- The repository now has a working auth-only foundation.
- MVP uses a React frontend and FastAPI backend.
- Spotify is the source of truth.
- MVP uses no persistent database.
- Local development comes first, with a simple cloud deployment path later.

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

## Milestone 2 - Data Collection and Artist Aggregation
### Purpose
Collect the core library signals and normalize them into artist-level records.

### Current status
- Not started.

### Dependencies
- Milestone 1

### Deliverables
- Spotify client support for liked tracks, saved albums, and followed artists
- pagination handling for required Spotify endpoints
- aggregation pipeline that produces `ArtistProfile` records
- unit-tested normalization logic for multi-artist tracks and album relationships

### Completion criteria
- the backend can build a complete artist-level aggregate from the required MVP data
- followed state, liked tracks, and saved album counts are reflected correctly per artist

## Milestone 3 - Listening Signals and Fallback Model
### Purpose
Add listening-behavior signals while preserving useful output when rich data is unavailable.

### Dependencies
- Milestone 2

### Deliverables
- best-effort support for recent listening or play-based proxy data
- derived listening-minute and play-count fields on `ArtistProfile`
- explicit scoring-mode selection between rich-signal and fallback execution
- tests covering partial signal availability

### Completion criteria
- the analysis pipeline can run with rich signals when available
- the same pipeline can still return valid ranked results when those signals are limited

## Milestone 4 - Scoring, Filtering, and Explanation Generation
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

## Milestone 5 - Frontend MVP Experience
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

## Milestone 6 - Playlist Creation
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

## Milestone 7 - Refinement and Launch Readiness
### Purpose
Improve quality until the MVP feels obviously trustworthy on real accounts.

### Dependencies
- Milestone 6

### Deliverables
- score tuning against real-user test accounts
- copy improvements for explanations and UI labels
- bug fixes for edge cases in data aggregation and session handling
- local deployment checklist and cloud deployment notes

### Completion criteria
- results feel consistently credible across multiple accounts
- explanation text reads clearly
- the team has enough setup guidance to run and demo the app reliably

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
