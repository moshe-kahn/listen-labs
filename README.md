# ListenLab

ListenLab is a Spotify web app that analyzes real listening behavior to surface overlooked artists, music, and actionable insights.

The core idea is simple: use what you actually listen to, not generic recommendations, to help you rediscover what you already care about.

---

## MVP

The initial version focuses on one primary feature:
- surfacing artists you clearly engage with but have not followed

Planned MVP components:
- React frontend
- FastAPI backend
- Spotify OAuth
- engagement-based scoring
- explanation-first results
- optional playlist generation

---

## Status

This repository now includes:
- React dashboard for authenticated Spotify snapshots
- FastAPI backend with Spotify OAuth, encrypted token persistence, and token-backed session restore
- live profile, playlists, recent listening, liked tracks, top tracks, top artists, and top albums views
- playback controls and session-aware player UI
- a track-detail overlay with in-place same-album switching, clickable artist/album links, preview snippets, and richer top-player controls (pause/resume, progress, and seek)
- a recent debug page for sessionized recent-play inspection plus DB archive paging
- restricted local mode, full mode, and a test path for probing Spotify availability
- local history-based artist and album ranking calibration using exported Spotify extended streaming history
- section-level caching for live data, history-derived favorites, history enrichment, and saved Spotify-only sections
- on-disk local analysis cache plus shared static metadata cache for artist and album artwork
- SQLite-backed raw play-event ingestion with schema migrations
- Spotify recent-play ingest with replay overlap, conservative early-stop paging, chronology-based `ms_played` estimation, and a dedicated connect-and-ingest OAuth path
- Spotify history-dump ingest with cross-source upgrade support into the same raw-play table
- duplicate-member tracking for canonical raw events, ingest-run hygiene helpers, and a unified top-track query over the raw SQLite store
- observational live playback snapshot capture into a separate SQLite evidence table (`live_playback_event`)
- encrypted Spotify token persistence, token-backed session restore, and backend helpers for current-playback capture plus recent-play polling
- backend validation, polling, and ingest-debug scripts for merge behavior, run cleanup, playback observation, and recent-play API probing
- a post-login loading handoff plus sticky dashboard navigation, account/project popovers, a track-formula comparison page, and multiple dashboard UI polish passes
- Vite `/api` proxying so local frontend development can use relative API paths instead of hard-coded backend origins

The core overlooked-artist analysis flow and playlist generation are still not implemented.

Known gaps still being worked:
- album ranking and album breadth counts still need correction in some cases
- some local-mode artist and album images still fail to persist or hydrate reliably
- recent album lists can still become too sparse for certain accounts and windows
- offline/downloaded playlist playback on phone can produce delayed or missing recently-played API visibility, which can leave temporary gaps in `raw_spotify_recent`

---

## Project Direction

ListenLab is built around **"signal over suggestion"**:
- prioritize real listening behavior over inferred taste
- combine multiple engagement signals such as listening, likes, and saves
- explain why results are surfaced
- avoid black-box recommendations

---

## Docs

- [Architecture](docs/architecture.md)
- [Context](docs/context.md)
- [Roadmap](docs/roadmap.md)
- [Raw Ingest](docs/raw-ingest.md)
- [Formula Calibration](docs/formula-calibration.md)
- [Auth Milestone Notes](docs/auth-milestone.md)
- [Track Section Migration](docs/track-section-migration.md)

---

## Current Product Direction

Build toward a web app that:
- connects to a user's Spotify account
- builds reliable artist and album signals from live Spotify data
- persists raw play events from both Spotify recent-play API data and Spotify extended streaming history
- upgrades weaker raw duration estimates when better source data arrives later
- calibrates scoring heuristics against exported listening history when available
- remains usable in a local cached mode when Spotify is unavailable or rate-limited
- eventually ranks overlooked artists by actual engagement
- explains why each result was surfaced
- optionally creates a playlist from those artists

---

## Implementation Defaults

- Spotify is the source of truth for live account data
- local exported history can be used for scoring calibration and local-mode fallback, not as a required product dependency
- recent sections should stay fresh while stable favorites and saved Spotify-only sections can come from cache
- analysis runs on demand
- use a local SQLite database for raw event ingestion, sync state, and ingest runs
- local development first, simple cloud deployment later

## Current Ingest Design

- `raw_play_event` stores source-faithful play rows from Spotify recent-play API ingest and Spotify history-dump ingest
- `raw_play_event_membership` links multiple source rows to one canonical raw event when cross-source duplicates are merged
- `ingest_run` tracks each import/sync run
- `spotify_sync_state` tracks recent-play replay boundaries and sync lifecycle metadata
- `live_playback_event` stores observational current-playback snapshots as a separate evidence layer (not canonical play history)
- recent-play rows start with `ms_played_method = default_guess` and can improve to `api_chronology`
- history-dump rows ingest with `ms_played_method = history_source`
- method precedence is:
  - `history_source > api_chronology > default_guess`
- dedupe and upgrade matching is:
  - exact `source_row_key` membership first
  - then conservative `cross_source_event_key`
- `cross_source_event_key` currently uses canonical UTC `played_at` plus Spotify track identity
- end-of-track UI detection can trigger a one-shot recent-play poll (`POST /auth/recent-ingest/poll-now`) so durable rows still come through the existing recently-played ingest pipeline
- merged duplicate source rows increment duplicate counters instead of creating extra canonical rows

## Local Safety Notes

- local runtime artifacts under `backend/data/` are disposable and should stay uncommitted
- validation databases and SQLite copy files are local-only debug outputs
- `backend/.env` and `frontend/.env` should stay ignored
