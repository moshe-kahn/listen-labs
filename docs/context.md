# ListenLab Context

## Purpose
This document explains the product intent, domain language, and scope boundaries for ListenLab. It is meant to give implementers enough shared context to make consistent decisions while building the MVP.

## Product Thesis
ListenLab turns real listening behavior into actionable insight.

The product is based on one belief: users already show what they care about through their listening patterns, even when they do not explicitly like tracks, save albums, or follow artists. ListenLab should surface those existing signals clearly instead of trying to predict taste through generic recommendations.

## Core Product Principle
Signal over suggestion.

The app should prioritize observed behavior over inferred similarity. Every surfaced result should be traceable to concrete evidence from the user's Spotify activity and library state.

## User Problem
Spotify users often:
- listen heavily to certain artists
- forget to follow artists they repeatedly return to
- engage with tracks without organizing their library
- lose track of albums or artists they already value

This creates a gap between actual listening behavior and explicit user actions. ListenLab closes that gap by showing what the user is already demonstrating through use.

## Target User
The MVP is optimized for an individual Spotify user who:
- listens regularly
- has enough history or library activity to generate signals
- wants help noticing artists they already care about
- values explanations more than black-box recommendations

## Desired User Outcome
After analysis, the user should feel:
- "These results are obviously based on my real behavior."
- "I can see why each artist was surfaced."
- "I want to follow these artists or save this playlist."

## In-Scope MVP Outcome
The MVP should help the user:
- authenticate with Spotify
- inspect their own listening-related signals in a dashboard
- build toward a ranked list of overlooked artists
- understand why artists or albums are surfaced
- optionally create a playlist from selected artists in a later milestone

## Out-of-Scope for MVP
The MVP should not include:
- AI recommendation features
- similarity, genre, or collaborative filtering logic
- social sharing or multi-user features
- complex personalization settings
- album completion detection
- concert integrations

Those items belong to later phases only.

Current note:
- the repository already includes a listening-insights style dashboard for profile, tracks, albums, artists, playlists, and recent activity
- that dashboard is being used as product calibration and UX groundwork, not as the final overlooked-artist MVP experience
- the dashboard now includes a loading handoff after Spotify auth plus a persistent top navigation bar for jumping between sections
- the dashboard also supports a restricted local mode so saved history- and cache-backed sections remain usable when Spotify is unavailable or rate-limited
- the backend now also persists raw play events from both Spotify recent-play API data and Spotify extended streaming history in a local SQLite database
- the current calibration workflow also includes recent-ingest probe/debug flows, live playback observation, and a dedicated tracks comparison page for testing ranking formulas against the same data
- track identity work now also includes a conservative three-layer model:
  - `source_track`
  - `release_track`
  - `analysis_track`
- track variant grouping is now policy-driven, with an explicit ambiguous-review queue generated after refresh passes so borderline title families can be inspected with artist context

## Domain Vocabulary
### Overlooked artist
An artist the user clearly engages with but does not currently follow. This is the primary surfaced entity for the MVP.

### Engagement signal
A measurable piece of user behavior or library state that contributes to artist ranking. Examples include listening minutes, play count, liked track count, saved album count, and recency.

### Raw play event
The source-faithful unit of listening ingestion stored before higher-level artist or album aggregation. Raw play events may start with an estimated duration and later be upgraded when a better source arrives.

### Source-row idempotency
The rule that the exact same source event should not be inserted twice. This is currently enforced with `source_row_key`.

### Cross-source upgrade
The rule that the same logical listen can be improved by a better source later. This is currently handled conservatively through `cross_source_event_key`.

### Canonical play event
The single logical listen stored in `fact_play_event`, with provenance links back to `raw_spotify_recent` and/or `raw_spotify_history`.

### Live playback observation
An observational current-playback snapshot captured for debugging and transition analysis. It is useful evidence, but it is not canonical durable play history by itself.

### `ArtistProfile`
The internal artist-level aggregate that combines all gathered signals into one normalized record. This is the canonical unit for scoring.

### `EngagementSignalBreakdown`
A structured record of how the final score was computed, including raw values, weights, weighted contributions, and signal availability status.

### Explanation payload
The user-facing explanation content that tells the user why an artist appeared in the ranked list. It must be evidence-based and easy to read.

### Playlist generation input
The selected set of surfaced artist IDs plus playlist settings such as name and tracks per artist. This input is used to create a Spotify playlist from the analysis results.

## Product Rules
- Do not surface artists using genre or similarity assumptions.
- Do not hide why an artist was ranked.
- Do not rely on liked tracks alone.
- Treat listening behavior as a first-class signal whenever available.
- Provide a useful fallback when listening-time data is incomplete.
- Preserve the difference between estimated play duration and source-truth play duration.

## Why Explanations Matter
Explanation is not decorative UI. It is part of the product value.

Users need to trust that ListenLab is reflecting their own behavior rather than inventing recommendations. The explanation layer makes results legible and credible, especially when scores are built from mixed signal quality.

Bad result:
- "Recommended artist"

Good result:
- "You played 18 tracks and spent 120 minutes listening to this artist, but you do not follow them."

## Primary User Flow
1. User logs in with Spotify.
2. User sees a short loading handoff while the first dashboard snapshot is prepared.
3. User lands in a listening snapshot dashboard with sticky navigation and section-level browsing.
4. Backend gathers available Spotify signals and, when available locally, compares them to exported listening history for calibration.
5. Frontend shows ranked artists, tracks, albums, playlists, recent activity, and playback controls where available.
6. If Spotify is limited or intentionally disabled, the frontend can fall back to local cached sections and clearly indicate when cached data may be stale.
7. Raw event ingestion can continue through recent-play sync and history-dump import paths.
8. A later milestone will add overlooked-artist analysis and playlist generation.

## Data Availability Constraints
Spotify may not expose every signal needed for perfect behavioral measurement.

Implementation should therefore assume:
- some users will have richer listening history than others
- listening minutes may be partial, approximated, or later upgraded
- play counts may need proxy or recent-history logic
- results still need to feel correct even under fallback conditions

Additional implementation rule:
- exported Spotify extended streaming history can be used as a calibration aid for development and power users, but formulas must remain reliable for users who only grant live Spotify API access
- stable favorites and history-enriched sections may be cached, but recent activity should stay fresh enough to reflect current listening
- once static artist or album metadata such as names, covers, and Spotify URLs is observed, it should be reusable from shared cache rather than treated as per-user state
- local validation databases, debug logs, and SQLite copies are disposable runtime artifacts, not product data that should be committed

## UX Priorities
- Make the main call to action obvious.
- Show ranked results quickly once analysis completes.
- Use staged loading when it helps first paint happen faster.
- Put explanation text near the artist name and score.
- Keep the interface simple enough that the insight feels immediate.
- Prefer clarity over dashboard complexity.

## Success Criteria
The MVP succeeds when:
- users can authenticate without confusion
- surfaced artists feel obviously correct
- the ranking still works when some signals are missing
- explanations make sense at a glance
- playlist creation feels like a natural follow-up action

## Defaults for Implementers
- Optimize for one user analyzing their own account.
- Keep scoring weights configurable in code, not in the public UI.
- Keep the backend as the owner of ranking, filtering, and explanation generation.
- Keep Spotify as the source of truth rather than copying user data into app storage.
- Use local runtime caches and snapshot files to preserve usability during rate limits or local-only sessions.
- Use the local SQLite store for raw ingest, sync state, and ingest-run bookkeeping.

## Current Follow-Ups
- Fix album breadth/counting so albums do not overcount duplicate or alternate track variants.
- Fix the incorrect track count still shown for "Chronicles of a Diamond."
- Improve local-mode image persistence and hydration so artist and album artwork survives mode switches more reliably.
- Improve recent album ranking so 4-week and 6-month windows do not collapse to overly sparse results.
- Use the new track-formula comparison page, live-playback observations, and raw-data validation scripts to tighten ranking confidence before broader product expansion.
- Decide how canonical Spotify-backed winner selection should work when multiple `source_track` rows are merged into one `release_track`.
- Review the ambiguous track-variant queue and tighten policy family-by-family instead of adding more hardcoded title rules.
