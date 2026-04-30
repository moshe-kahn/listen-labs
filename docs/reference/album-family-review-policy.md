# Album-Family Review Policy

This policy defines how album-family grouping candidates are reviewed before any future mutation/import path exists.

## Scope
- Applies to suggested album-family candidates produced by:
  - `./.venv/bin/python backend/scripts/run_album_family_candidate_report.py`
- This is a manual-review policy only.
- No apply/promotion behavior is implemented here.

## Candidate Review Criteria

A candidate can be marked `accept` only when releases clearly represent variants of the same album family.

Accept signals:
- Same primary artist or clearly equivalent artist credit.
- Same core album title after removing edition/version suffixes.
- Release years are identical or close enough to support variant status.
- Track overlap is strong, if track-level evidence is available.
- Suffixes indicate edition/variant relationship, such as:
  - Deluxe
  - Expanded
  - Remastered
  - Anniversary Edition
  - Bonus Track Version
  - Explicit/Clean variants
  - Regional variants

Reject signals:
- Different primary artist.
- Same/similar title but unrelated release.
- Compilation vs original album unless explicitly intended.
- Soundtrack/score/playlist/artist-mix ambiguity.
- Weak title similarity with no artist/year/track support.
- Candidate would merge albums that should remain independently visible.

Needs-more-evidence signals:
- Same title and artist, but unclear whether album, single, EP, compilation, or live release.
- Same title with different release years and weak track overlap.
- Remaster/deluxe wording exists, but artist metadata is incomplete.
- Missing track-count or release-year evidence.
- Multiple possible canonical/default releases.

## Required Evidence Fields In Candidate Reports

Each candidate row/group should include:
- `candidate_status` (always `suggested_only`)
- `candidate_group_key` or stable candidate ID
- involved `release_album_id` values
- involved current `album_family_id` values
- album names / normalized names
- primary artist names / IDs if available
- release years
- track counts if available
- title similarity score or title-match reason
- artist match signal
- year proximity signal
- suffix/version signal
- confidence score
- explanation string
- explicit warning flags (examples):
  - `different_artist`
  - `large_year_gap`
  - `weak_title_match`
  - `missing_track_evidence`
  - `compilation_risk`
  - `soundtrack_risk`
- `recommended_decision`:
  - `accept`
  - `reject`
  - `needs_more_evidence`

If evidence is unavailable, fields should still be present with safe empty values.

## Manual Promotion Process (Defined, Not Implemented)

1. Generate a suggested-only candidate report.
2. Human reviewer assigns per candidate:
  - `accept`
  - `reject`
  - `needs_more_evidence`
3. For `accept`, reviewer must specify:
  - target `album_family_id`
  - `release_album_id` values to move
  - canonical/default `release_album_id` if known
  - reviewer rationale note
4. `reject` decisions are recorded later (no code path yet).
5. `needs_more_evidence` remains unapplied and can be re-reviewed later.
6. Only after this policy is in place should an explicit promotion/import path be designed.
7. Any future promotion path must:
  - require human-reviewed input
  - never infer acceptance from confidence alone
  - preserve an audit trail
  - support dry-run
  - validate no dangling references
  - validate no accidental many-family assignment for one `release_album`
  - avoid ranking/query behavior changes unless explicitly requested

## Non-Goals / Safety Constraints

Do not add as part of this policy:
- promotion endpoint
- apply command
- accepted `album_family_map` mutation
- auto-merge behavior
- UI changes
- ranking/query behavior changes
- `album_relationship`

## Implementation Note

Current report tooling is intentionally suggested-only and read-only. Evidence fields may be expanded in future report-only iterations to fully match this policy before any promotion/import path exists.
