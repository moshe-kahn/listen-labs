# ListenLab Formula Calibration

## Purpose
This document captures what the exported Spotify extended streaming history teaches us about believable artist and album ranking, and turns that into proposed formulas for users who only provide live Spotify API access.

## Why This Exists
Most users will not have exported Spotify history.

That means the production formulas cannot depend on:
- full historical `ms_played`
- historical play events over many years
- exported skip reasons and raw timestamps

But we can still use exported history during development to answer a more important question:

What patterns make a ranking feel obviously correct?

## Calibration Inputs Used
The local Spotify extended streaming history export provides:
- `ts`
- `ms_played`
- `master_metadata_track_name`
- `master_metadata_album_artist_name`
- `master_metadata_album_album_name`
- `spotify_track_uri`
- `reason_start`
- `reason_end`
- `skipped`
- `shuffle`
- `platform`
- `offline`
- `conn_country`

This is enough to build strong all-time and recent artist/album rankings for calibration.

Important limitation:
- the export does not include playlist membership context for each play

Current raw-ingest note:
- the backend now persists these raw fields in `raw_play_event`
- recent-play API rows do not have equivalent source-truth values for many of them and currently store `NULL`

## Ground-Truth Observations From History
### Artists
The export shows that believable top-artist ranking is not just about one dominant track.

The strongest signals were:
- total listening time
- repeated play count
- breadth across distinct tracks

Examples from the calibration run:
- `Black Pumas`: very high listening time and very high repeat count
- `Radiohead`: lower time than `Black Pumas`, but much broader track coverage
- `Mark Pritchard`: broad catalog coverage matters even when one-track dominance is lower

Conclusion:
- artist formulas must reward both depth and breadth
- distinct tracks should not be a tiny tie-breaker; it is a core part of the score

### Albums
Albums felt correct when both of these were true:
- many plays happened on the album
- multiple distinct songs from the album were represented

Examples from the calibration run:
- `Black Pumas` and `Chronicles of a Diamond` rank highly because both total listening time and multi-track coverage are strong
- recent albums such as `Kid A` feel correct because several tracks from the album reappear, not because of one isolated song

Conclusion:
- album ranking should prioritize distinct-track breadth more heavily than single-track dominance
- recent album formulas should emphasize recent breadth and recent repeated listening together
- duplicate track variants and alternate Spotify track IDs must not inflate album breadth counts

### Recent Windows
For exported history, `recent` should be anchored to the latest timestamp in the export, not the current machine date.

Conclusion:
- for live API scoring, recent windows should always be relative to observed user activity

## Production Constraint
The export is a calibration source, not a product dependency.

The live-only formulas below are intended for all users and should use only signals that the live Spotify app already collects:
- top tracks
- top artists
- recently played tracks
- liked tracks
- followed artists
- owned playlists

## Proposed Live-Only Artist Formula
### Available live signals per artist
- `long_top_track_count`
- `recent_top_track_count`
- `long_rank_weight`
- `recent_rank_weight`
- `liked_track_count`
- `recent_play_count`
- `recent_distinct_tracks`

### Normalization
All count-like signals should be normalized against the max observed value in the current user session:
- divide by the maximum non-zero value for that signal across candidate artists
- keep missing signals at `0`

### Proposed all-time artist score
```text
artist_score_all_time =
  (long_rank_weight_norm * 0.32) +
  (long_top_track_count_norm * 0.23) +
  (liked_track_count_norm * 0.18) +
  (recent_rank_weight_norm * 0.12) +
  (recent_distinct_tracks_norm * 0.10) +
  (recent_play_count_norm * 0.05)
```

### Proposed recent artist score
```text
artist_score_recent =
  (recent_rank_weight_norm * 0.30) +
  (recent_distinct_tracks_norm * 0.25) +
  (recent_play_count_norm * 0.20) +
  (recent_top_track_count_norm * 0.15) +
  (liked_track_count_norm * 0.10)
```

### Artist tie-breaks
1. higher distinct-track count
2. higher recent play count
3. higher liked-track count
4. alphabetical artist name

### Why This Matches Calibration
- total weight from top-track strength preserves replay intensity
- distinct-track count preserves breadth
- liked tracks remain a useful confirmation signal
- recent activity contributes without fully overriding established favorites

## Proposed Live-Only Album Formula
### Available live signals per album
- `long_distinct_tracks_on_album`
- `recent_distinct_tracks_on_album`
- `liked_tracks_on_album`
- `long_rank_weight_on_album`
- `recent_rank_weight_on_album`
- `recent_play_count_on_album`

### Normalization
Normalize each signal across candidate albums for the current session.

### Proposed all-time album score
```text
album_score_all_time =
  (long_distinct_tracks_on_album_norm * 0.34) +
  (long_rank_weight_on_album_norm * 0.24) +
  (liked_tracks_on_album_norm * 0.18) +
  (recent_distinct_tracks_on_album_norm * 0.14) +
  (recent_play_count_on_album_norm * 0.10)
```

### Proposed recent album score
```text
album_score_recent =
  (recent_distinct_tracks_on_album_norm * 0.34) +
  (recent_play_count_on_album_norm * 0.24) +
  (recent_rank_weight_on_album_norm * 0.18) +
  (liked_tracks_on_album_norm * 0.14) +
  (long_distinct_tracks_on_album_norm * 0.10)
```

### Album tie-breaks
1. higher distinct-track count on the album
2. higher recent play count on the album
3. higher liked-track count on the album
4. alphabetical album name

### Why This Matches Calibration
- distinct-track count is the strongest album signal because it captures breadth
- rank-weight and recent play count preserve intensity
- liked tracks provide a stabilizing signal without dominating
- one-song albums should not outrank albums with broad multi-track listening

## What To Avoid
Do not use these as primary ranking signals:
- a single recent play
- a single high-ranked top track without breadth
- playlist count alone
- followed status alone

These can still be supporting signals, but not primary drivers.

## Recommended Implementation Plan
### Phase 1
- implement the live-only artist formula above
- implement the live-only album formula above
- add debug breakdown fields for normalized inputs and final score

### Phase 2
- compare live-only rankings against history-derived rankings on calibration accounts
- tune weights until the top 5 to top 10 results feel directionally aligned

### Phase 3
- use the same scoring framework for overlooked-artist ranking
- apply followed-artist filtering after artist scoring

## Current Decision
Use exported history for:
- development calibration
- local debug validation
- optional power-user local ranking mode

Use live-only formulas for:
- the actual product path
- all default user scoring
- future overlooked-artist ranking

## Active Calibration Gaps
- The current album implementation still has correctness gaps in breadth counting for some albums.
- "Chronicles of a Diamond" is still a known bad example where album track breadth is overstated.
- Recent album ranking for 4-week and 6-month windows can still become too sparse and needs another calibration pass.
