# Spotify Catalog Backfill

## Purpose
Spotify catalog backfill enriches existing local identities with Spotify catalog metadata.

It is enrichment-only. It must not create, merge, promote, or repair ListenLab identity rows.

## Tables
- `spotify_track_catalog`
- `spotify_album_catalog`
- `spotify_album_track`
- `spotify_catalog_backfill_run`
- `spotify_catalog_backfill_queue`

## Behavior
- Discovers known Spotify track and album IDs from source mappings and raw play rows.
- Uses representative Spotify IDs for release-level lookup rows, preferring most-listened evidence where multiple candidates exist.
- Fetches track metadata, album metadata, and album tracklists.
- Skips complete non-error catalog rows unless `force_refresh=true`.
- Retries error rows on later runs.
- Resumes incomplete album tracklists by using the existing stored track count as the next offset.
- Applies album tracklist page caps per album, not as a global run stop.
- Processes queue-first work before broader discovered work.

## Request Controls
- `limit`
- `offset`
- `market`
- `include_albums`
- `force_refresh`
- `request_delay_seconds`
- `max_runtime_seconds`
- `max_requests`
- `max_errors`
- `max_429`
- `max_album_tracks_pages_per_album`
- `album_tracklist_policy`

Album tracklist policies:
- `all`
- `relevant_albums`
- `priority_only`
- `none`

## Reliability
- Handles Spotify 429 with `Retry-After` when available.
- Stops as partial with `stop_reason = "rate_limited"` after repeated 429s.
- Falls back from forbidden batch track/album endpoints to single-item requests.
- Stores compact error diagnostics without token/header leakage.
- Keeps run telemetry for request counts, warning counts, skip counts, and retry-after timing.

## Frontend
The Catalog Backfill page includes:
- run controls
- recent runs tab
- queue tab
- queue status filter
- queue repair button
- latest result summary

Search / Lookup includes:
- Album Catalog Lookup
- Track Catalog Lookup
- duplicate album diagnostics
- queue-aware statuses
- manual prioritize actions for visible incomplete albums/tracks

## Invariants
- No identity tables are mutated by catalog backfill.
- No `analysis_track_map` rows are mutated.
- No merge/apply behavior belongs in catalog backfill.
- Catalog rows are metadata evidence, not identity decisions by themselves.

## Verification
Common checks:
- `python3 -m unittest backend.tests.test_spotify_catalog_backfill`
- `python3 -m py_compile backend/app/main.py backend/app/spotify_catalog_backfill.py`
- `npm run build`
