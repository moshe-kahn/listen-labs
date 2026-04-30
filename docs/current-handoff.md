# Current Handoff

## Read First
For a new chat, start here, then read only the topic docs needed for the task.

Recommended docs:
- `docs/reference/drafts/entity-model-draft.md` for release/source/analysis identity, duplicate diagnostics, and merge preview/dry-run.
- `docs/reference/spotify-catalog-backfill.md` for catalog enrichment, queue behavior, lookup, and backfill invariants.
- `docs/reference/raw-ingest.md` for raw play events, recent/history ingest, and fallback history text.
- `docs/reference/drafts/identity-audit-submission-contract.md` only when working on saved track-audit submissions.
- `docs/reference/album-family-review-policy.md` only when working on album-family candidate review.

Avoid asking future agents to read every doc by default. The repo docs include historical and product planning material that is not always relevant.

## Current Active Area
The active work is release-album duplicate review and future repair planning.

Current Release tab diagnostics:
- Duplicate Albums by same resolved Spotify album ID.
- Duplicate Albums by normalized album name + normalized primary artist.
- Duplicate Tracks by same resolved Spotify track ID.
- Release Track Split Signals.

All duplicate diagnostics are read-only.

## Current Merge Tooling
Read-only release album merge tooling exists:
- `POST /debug/identity/release-albums/merge-preview`
- `POST /debug/identity/release-albums/merge-dry-run`

Preview:
- chooses a deterministic survivor
- returns warnings and affected counts
- classifies readiness as `safe_candidate`, `needs_review`, or `unsafe`

Dry run:
- reuses preview/readiness
- blocks `unsafe`
- blocks survivor mismatch
- allows `safe_candidate` and `needs_review`
- returns exact row-level plan sections

No apply/merge endpoint exists yet.

Important schema rule:
- `release_track` has no `release_album_id`
- album membership lives in `album_track`
- album merge would repoint `album_track.release_album_id`
- `release_track` rows are not changed directly

## Current Invariants
- Catalog backfill is enrichment-only.
- Catalog backfill must not mutate identity or analysis mapping tables.
- Duplicate diagnostics must not call Spotify.
- Merge preview and dry-run must not write.
- `analysis_track_map` must not mutate in release-album preview/dry-run paths.
- Spotify catalog tables are metadata evidence, not merge decisions by themselves.

## Recent Prevention Fix
Fallback/history text entity keys now normalize artist text with `_normalize_fallback_artist_text(...)`.

This prevents new fallback splits like:
- `Telekinesis`
- `Telekinesis, Telekinesis`

Scope:
- applies only to fallback/history text keys
- preserves raw artist text for display/raw fields
- does not affect Spotify-ID identity paths
- does not repair existing duplicate rows

## Catalog Backfill Current State
Catalog backfill supports:
- track catalog enrichment
- album catalog enrichment
- album tracklist enrichment
- queue-first processing
- queue repair
- recent runs and queue tabs in the frontend
- album tracklist policies: `all`, `relevant_albums`, `priority_only`, `none`

Track variant runtime config moved to `docs/config/track-variant-policy.json`.
Run `python3 -m unittest backend.tests.test_track_variant_policy` after changes touching that config or its loader.

See `docs/reference/spotify-catalog-backfill.md` for catalog details.

## Next Likely Task
Inspect real merge preview/dry-run output for duplicate release-album groups.

If the plans look correct, design an apply endpoint guarded to `safe_candidate` first.

Future apply requirements:
- transaction
- idempotency
- explicit `rows_affected`
- no direct `release_track` mutation
- no `analysis_track_map` mutation
- focused tests for all touched tables

## Verification Commands
Common checks for the current work area:

```bash
python3 -m unittest backend.tests.test_spotify_catalog_backfill
python3 -m unittest backend.tests.test_entity_backfill
python3 -m py_compile backend/app/main.py backend/app/spotify_catalog_backfill.py backend/app/db.py
cd frontend && npm run build
```
