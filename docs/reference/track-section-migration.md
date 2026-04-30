# Track Section Migration

## Purpose
This note defines the first backend migration target for DB-backed track sections so implementation can proceed in small, explicit steps.

## Canonical Track Section DTO
The backend should treat one normalized track-section item shape as the contract for both DB-backed and fallback-backed track sections.

Implementation contract:
- use `CanonicalTrackSectionItem` and `CanonicalTrackSectionPayload` from `backend/app/track_sections.py`
- keep the item fields aligned with the current frontend `RecentTrack` expectations so the migration does not require a frontend rewrite
- allow sparse fields during the transition; missing enrichment is acceptable, schema drift is not

DTO rules:
- `track_id`, `track_name`, and `artist_name` are the minimum identity fields
- `play_count`, `all_time_play_count`, `recent_play_count`, `first_played_at`, and `last_played_at` are the core DB-backed metrics we are migrating toward
- playback-context fields such as `spotify_played_at`, `spotify_context_*`, and `estimated_*` may be null for top-track sections that come from aggregated queries
- `debug.source` is temporary migration instrumentation and should be populated by section assemblers while sources are mixed

## First `/me/recent` Migration Target
Migrate `recent_tracks` first.

Why this section goes first:
- it already has a clear DB-backed analogue through ingested recent-play rows
- it is narrower than `recent_top_tracks`
- it is easier to validate chronologically against archive and debug views
- it lets us prove the DB-first assembler pattern before introducing ranking logic

## Exact `recent_tracks` Semantics
One `recent_tracks` item represents one recent play event, not a deduplicated track aggregate.

Why:
- the current UI presents `recent_tracks` as a chronological "Recently played" feed
- the current debug/archive views are event-oriented
- repeated plays of the same song should appear as repeated items

For the first DB-backed implementation:
- source table: `raw_spotify_recent`
- row identity: one item per `raw_spotify_recent.source_row_key`
- sort key: `played_at DESC, id DESC`
- duplicate handling: do not deduplicate repeated plays across events; rely on `raw_spotify_recent.source_row_key` uniqueness to avoid replay duplicates of the same source row
- canonical timestamp field: `raw_spotify_recent.played_at`, mapped to `spotify_played_at`
- gap calculation: `played_at_gap_ms` is the positive difference between the current row's `played_at` and the next older row's `played_at`
- estimated played duration: use `raw_spotify_recent.ms_played_estimate`
- metadata source: prefer `raw_spotify_recent` columns, then parse optional details from `raw_payload_json`
- missing metadata behavior: return null for enrichment-style fields such as artwork, URLs, release year, album type, and artist list when unavailable; do not perform Spotify enrichment or fallback in this layer

## Exact `recent_top_tracks` Semantics
One `recent_top_tracks` item represents one deduplicated track aggregate inside the requested recent window.

Why:
- the current live route gets this section from Spotify `me/top/tracks`, which is aggregate-oriented rather than event-oriented
- the DB-backed replacement needs explicit ranking semantics before route wiring so comparisons stay interpretable
- this section should answer "what rose to the top lately", not "what happened most recently"

For the first DB-backed implementation:
- source view: `v_fact_play_event_with_sources`
- row identity: one item per canonical track identity using `spotify_track_id`, then `spotify_track_uri`, then a normalized `track_name + artist_name` fallback key
- window filter: include only tracks with at least one play event whose `canonical_ended_at` falls inside the requested recent window
- ranking sort: `recent_play_count DESC, last_played_at DESC, all_time_play_count DESC, track_identity ASC`
- count semantics:
  - `play_count` = `recent_play_count` for this section
  - `recent_play_count` = play events inside the requested recent window
  - `all_time_play_count` = all play events across the unified fact table for the same track identity
- first/last timestamps: `first_played_at` and `last_played_at` come from the unified fact history for that track identity
- metadata source: use the most recent fact row for canonical names and provider IDs/URIs; do not perform Spotify enrichment or fallback in this layer
- missing metadata behavior: sparse fields such as artwork, preview URL, popularity, album release year, and artist list may be null

## Deferred `/me/recent` Track Sections
Do not migrate these in the current pass:
- `recent_likes_tracks`

Reason:
- `recent_likes_tracks` is library-state data, not raw play-event data, so it should remain Spotify-backed for now

## Planned Order
1. Define canonical track DTO.
2. Migrate `/me/recent.recent_tracks` to a DB-first section assembler.
3. Define and validate `/me/recent.recent_top_tracks` DB ranking semantics in an isolated helper.
4. Add side-by-side comparison or debug wiring before replacing live route output.
5. Reuse the same DTO and assembler pattern for the track portions of `/me`.

## Current Migration Read
Fresh live compare outcome:
- `recent_tracks` appears ready for the first narrow route migration
- `recent_top_tracks` does not have semantic parity with the current route and should stay off the main path for now

Current interpretation:
- `recent_tracks` is primarily an implementation migration
- `recent_top_tracks` is now a product semantics decision plus a metadata-completeness task

Current implementation status:
- `/me/recent.recent_tracks` is now migrated to the DB-backed builder
- route-boundary normalization preserves the existing frontend-facing shape for:
  - `spotify_played_at`
  - `artists`
- `GET /debug/me/recent/compare` is still retained temporarily as a verification path

Known small contract drift:
- `spotify_available_markets_count` may now be `null` on DB-backed `recent_tracks` rows where the previous live Spotify path often returned `0`
- this appears frontend-safe, but it is a documented minor drift rather than an intentional product change

Not yet migrated:
- `/me/recent.recent_top_tracks`

Recommended next step:
1. Commit the narrow `recent_tracks` migration.
2. Or do a short manual frontend smoke check for the Recent Tracks UI before committing.

Important clarification:
- the next clean data-model step after this migration is not to keep blending separate frontend recent/history lists
- it is to build a track-level aggregate on top of the merged play-event layer for provenance-aware filtering and inspection

Deferred unrelated cleanup:
- recent archive/load-more totals in the debug surface may look confusing until timestamp normalization is aligned across route outputs
- that is a later debug-surface cleanup, not the main follow-up to this migration
