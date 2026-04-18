# ListenLabs Raw Ingest

## Purpose
This document is the focused source of truth for the current raw listening ingest design.

It covers:
- raw SQLite tables
- canonical-event membership for duplicate source rows
- source-row and cross-source identity
- `ms_played` method precedence
- Spotify recent-play ingest behavior
- Spotify history-dump ingest behavior
- live playback observational evidence capture
- ingest-run utilities, validation scripts, and current performance notes

## Current Scope
The raw ingest layer is responsible for:
- storing source-faithful play events before higher-level scoring
- preserving enough provenance to improve a row when better source data arrives later
- tracking ingest runs
- tracking Spotify recent-sync replay state

The raw ingest layer is not yet responsible for:
- artist-level aggregation
- final ranking/scoring
- fact/dimension modeling
- fuzzy matching
- canonical song clustering

The live playback evidence layer is responsible for:
- capturing current-playback observations for debugging and skip/transition analysis
- staying separate from canonical confirmed play history

## SQLite Tables

### `ingest_run`
Tracks one import or sync run.

Key fields:
- `id`
- `source_type`
- `source_ref`
- `started_at`
- `completed_at`
- `status`
- `row_count`
- `inserted_count`
- `duplicate_count`
- `error_count`

### `spotify_sync_state`
Tracks the recent-play replay boundary and sync lifecycle metadata.

Key fields:
- `last_successful_played_at`
- `overlap_lookback_seconds`
- `last_started_at`
- `last_completed_at`
- `last_run_id`

### `raw_play_event`
Stores one raw logical play row.

Key fields:
- identity
  - `source_type`
  - `source_event_id`
  - `source_row_key`
  - `cross_source_event_key`
- timing and duration
  - `played_at`
  - `ms_played`
  - `ms_played_method`
  - `duplicate_row_count`
  - `duplicate_merge_strategy`
  - `track_duration_ms`
- history/raw context
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

### `raw_play_event_membership`
Stores every observed source row that belongs to a canonical raw event.

Key fields:
- `canonical_event_id`
- `source_row_key`
- `source_type`

### `live_playback_event`
Stores observational playback snapshots from the current-playback endpoint.

Key fields:
- observation
  - `observed_at`
  - `user_id`
  - `spotify_user_id`
  - `source`
  - `has_playback`
- playback snapshot
  - `item_type`
  - `item_id`
  - `item_name`
  - `spotify_track_uri`
  - `artist_names_json`
  - `album_name`
  - `progress_ms`
  - `duration_ms`
  - `is_playing`
  - `device_id`
  - `device_name`
  - `device_type`
  - `spotify_timestamp_ms`
- payload retention
  - `raw_payload_json`

## Identity Model

### `source_row_key`
`source_row_key` is source-specific.

It is used for:
- exact source-row idempotency
- same-source reprocessing
- same-source method upgrades

It should answer:
- “have we already seen this exact source row?”

It should not be treated as a universal cross-source identity.

### `cross_source_event_key`
`cross_source_event_key` is the conservative cross-source logical-play key.

It is used for:
- API row -> history row upgrades
- cross-source improvement of `ms_played` quality

It should answer:
- “does this look like the same logical play across different sources?”

Current rule:
- use canonical UTC `played_at`
- plus `spotify_track_id` if available
- otherwise `spotify_track_uri`
- otherwise `NULL`

No fuzzy title/artist matching is used yet.

## Canonical `played_at`
All cross-source identity logic depends on canonical UTC `played_at`.

Current rule:
- parse source timestamp
- convert to UTC
- serialize as ISO8601 with trailing `Z`

Example:
- `2026-04-16T12:00:00+00:00`
- becomes `2026-04-16T12:00:00Z`

This matters because:
- ordering
- replay overlap logic
- cross-source hashes
- chronology estimation

## `ms_played` Design

### Current fields
- `ms_played INTEGER NOT NULL`
- `ms_played_method TEXT NOT NULL`
- `track_duration_ms INTEGER NULL`

### Allowed methods
- `history_source`
- `api_chronology`
- `default_guess`

### Precedence
- `history_source > api_chronology > default_guess`

### Meaning
- `history_source`
  - true source-provided `ms_played` from Spotify history export
- `api_chronology`
  - inferred from adjacent `played_at` timestamps in recent-play API batches
- `default_guess`
  - fallback estimate when chronology inference is not available

### Current default guess
For Spotify recent-play API rows:
- `ms_played = int(track_duration_ms * 0.65)`
- `ms_played_method = 'default_guess'`

This is intentionally a fallback, not source truth.

## Upgrade Rules
Current upgrade matching is two-stage:

1. exact `source_row_key` membership
2. if no exact source-row match and `cross_source_event_key` is present:
   exact `cross_source_event_key`

If a matching row is found:
- compare incoming `ms_played_method` rank
- upgrade only if the incoming method is better
- attach the new `source_row_key` into `raw_play_event_membership` when it is a cross-source duplicate member

Current upgrade behavior:
- update `ms_played`
- update `ms_played_method`
- increment `duplicate_row_count` when a new duplicate member is attached
- record `duplicate_merge_strategy`
- preserve/upgrade `track_duration_ms`
- preserve/upgrade raw context fields with `COALESCE(...)`

## Spotify Recent-Play Ingest

### Flow
1. compute sync replay boundary from `spotify_sync_state`
2. fetch recent-play API pages
3. map raw API items into raw row shape
4. explicitly sort by canonical `played_at`
5. apply conservative early-stop paging logic
6. apply chronology estimation on kept rows only
7. ingest rows through raw upsert/upgrade logic

### Early-stop paging
Rules:
- do not trust raw API order
- map first
- sort newest -> oldest for page scan logic
- stop only when encountering a known row older than the overlap cutoff
- keep known rows inside overlap so upgrades can still happen

### Chronology estimation
Current assumption:
- treat `played_at` as end-of-play timestamp for the event

For a sorted batch:
- estimate row duration from:
  - `current.played_at - previous.played_at`
- only accept if:
  - gap is `> 0`
  - gap is `<= track_duration_ms`
- otherwise keep `default_guess`

This means:
- first row in a batch usually stays `default_guess`
- plausible adjacent rows can improve to `api_chronology`
- sync summaries now also report earliest/latest `played_at`, already-seen source rows, and merged duplicate members

## Spotify History-Dump Ingest

### Flow
1. discover `Streaming_History_Audio_*.json`
2. read and parse rows
3. map history rows into raw row shape
4. ingest through the same raw upsert/upgrade helper

### History mapping behavior
History rows provide:
- true `ms_played`
- `ms_played_method = 'history_source'`
- Spotify URI-based track identity
- raw context fields such as:
  - `reason_start`
  - `reason_end`
  - `skipped`
  - `platform`
  - `shuffle`
  - `offline`
  - `conn_country`

### Cross-source upgrade behavior
If a history row matches a prior API row by:
- same `source_row_key`
or
- same `cross_source_event_key`

then the history row can upgrade:
- `default_guess -> history_source`
or
- `api_chronology -> history_source`

## Performance Notes

### History ingest instrumentation
The current history ingest path logs:
- file discovery time
- per-file read time
- per-file JSON parse time
- total mapping time
- total DB ingest time
- total elapsed time

### Current batch transaction behavior
History ingest now uses:
- one SQLite connection for the batch
- one transaction for the batch

This is important because the earlier per-row connection/commit path was the likely bottleneck.

### Current observed narrow sample
On a narrow 500-row sample after the batch transaction refactor:
- file read: about `104 ms`
- parse: about `139 ms`
- mapping: about `19 ms`
- DB ingest: about `62 ms`
- total: about `328 ms`

This is only a narrow sample, not a full-history benchmark.

## Current Limitations
- no fuzzy cross-source matching
- no artist/album aggregation on top of raw rows yet
- no final analytics model consuming the raw DB as the main source yet
- chronology estimation is heuristic, not source truth
- live playback observations are not yet promoted directly to canonical plays
- full-history import benchmarking still needs a controlled larger sample after the migration recovery and transaction refactor
- real history/API overlap validation is still pending on a dataset with substantial real `spotify_recent` rows; exact end-time equality has not yet been validated on real overlapping data and should be revisited after a fresh overlap-producing ingest

## Live Playback + Durable Ingest Boundary
- `live_playback_event` is observational evidence, not canonical play history
- canonical durable rows still come from the existing recent-play/history ingest pipeline
- when the UI detects a local track end, it can trigger one delayed `POST /auth/recent-ingest/poll-now`
- this keeps durable inserts in one place (recent-play ingest and dedupe logic) while still capturing richer live context

## Utilities and Validation Scripts

### `backend/scripts/validate_data_foundation.py`
Purpose:
- build an isolated validation SQLite database
- ingest history files with `continue_on_error=True`
- run a merge-validation matrix
- print a unified top-track sample derived from raw data

### `backend/scripts/db_ingest_run_hygiene_smoke.py`
Purpose:
- smoke-test ingest-run creation, listing, lookup, and deletion with dependent raw rows

### `backend/scripts/probe_spotify_recent_before.py`
Purpose:
- probe Spotify recently-played behavior with an optional `before` cursor outside the main app flow

### `backend/scripts/get_current_playback_for_user.py` and `poll_recent_for_user.py`
Purpose:
- inspect current-playback behavior and one-shot recent-play polling for a stored user token during local debugging

## Duplicate Event Semantics TODO
- distinct duplicate-member rows have been observed for the same logical event with differing `reason_start`, `reason_end`, `offline`, `platform`, and occasionally `skipped`
- current merge rule remains:
  - keep the best canonical `ms_played`
  - do not sum `ms_played`
- future investigation:
  - determine whether any duplicate-member rows represent truly non-overlapping resumed segments that would justify additive `ms_played`
  - do not change current merge logic until that is demonstrated with real evidence

## Recommended Next Use
This raw ingest layer is ready to support:
- controlled larger history-import timing tests
- artist/album aggregation built from raw rows
- later scoring work that can distinguish `history_source` from estimated durations
