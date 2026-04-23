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

## Deferred `/me/recent` Track Sections
Do not migrate these in the first pass:
- `recent_top_tracks`
- `recent_likes_tracks`

Reason:
- `recent_top_tracks` needs aggregated ranking semantics and will benefit from the same DTO after `recent_tracks` is stable
- `recent_likes_tracks` is library-state data, not raw play-event data, so it should remain Spotify-backed for now

## Planned Order
1. Define canonical track DTO.
2. Migrate `/me/recent.recent_tracks` to a DB-first section assembler.
3. Migrate `/me/recent.recent_top_tracks` after the DB query helpers are validated.
4. Reuse the same DTO and assembler pattern for the track portions of `/me`.
