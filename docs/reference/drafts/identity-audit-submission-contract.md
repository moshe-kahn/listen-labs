# Identity Audit Submission Contract (Draft)

## 1) Current State

- Ambiguous Review decisions are local-only in the frontend until explicitly validated/saved.
- Submission Preview is generated client-side.
- Backend currently supports:
  - read-only preview validation
  - save-only submission persistence
  - read-only saved-submission list/read
  - read-only submission dry-run
- No promotion/apply endpoint exists yet.

## 2) Decision Types

### Group decisions
- approve
- reject
- skip

### Track decisions
- approve
- reject
- skip

## 3) Intended Future Semantics

### Group decisions
- approve: propose accepting the suggested composition/family grouping.
- reject: propose keeping the suggested group separate.
- skip: defer/no-op.

### Track decisions
- approve: propose accepting the track identity mapping/canonicalization candidate.
- reject: propose rejecting that candidate mapping.
- skip: defer/no-op.

## 4) Non-Goals / Boundaries

- No direct mutation of `analysis_track_map` from the preview UI.
- No Formula Lab ranking coupling.
- No learned-rule generation in the initial submission path unless explicitly added later.
- No automatic application from preview export alone.

## 5) Proposed Future API Shape (Sketch Only)

Do not implement in current pass.

- `POST /debug/tracks/identity-audit/submission-preview/validate`
  - read-only validation of a proposed JSON payload.
- `POST /debug/tracks/identity-audit/submissions`
  - future persistence path, gated separately.

## 6) Proposed Payload Shape

Mirror current client-side JSON preview:

- `generated_at`
- `summary`
- `decisions.groups.approved`
- `decisions.groups.rejected`
- `decisions.groups.skipped`
- `decisions.tracks.approved`
- `decisions.tracks.rejected`
- `decisions.tracks.skipped`

## 7) Validation Rules for Future Backend

- Stable keys are required.
- Item IDs are required.
- `decision` must be one of: `approve`, `reject`, `skip`.
- Unknown/missing source items should produce warnings, not hard crashes.
- Payload validation should be idempotent.
- Empty submissions are allowed and should return a no-op summary.

## 8) Safety Principles

- Validate before apply.
- Apply step must be separate from preview/validate.
- Any future write endpoint must explicitly report rows affected.
- Any future mutation path must include focused tests proving unrelated tables are untouched.

## 9) Persistence Phases

Future persistence should be introduced in three distinct phases.

### Phase 1: Save submission

- Store the reviewed preview payload as an audit artifact.
- Do not mutate `analysis_track_map`.
- Do not generate learned rules.
- Return a `submission_id`.
- Primary purpose: review/history/auditing.

### Phase 2: Promote submission

- Input: saved `submission_id`.
- Apply approved decisions only.
- Run only after validation.
- Require a dry-run mode before actual promotion.
- Response must explicitly report intended rows and actual rows affected.

### Phase 3: Generate learned rules

- Separate, later workflow.
- Source material: accepted/promoted decisions.
- Not part of initial save/promote path.

## 10) Additional Safety Rules

- Preview, validate, save, and promote must remain distinct operations.
- No endpoint should both save and mutate mappings.
- Promotion must be idempotent or explicitly detect duplicate application.
- Every write endpoint must have focused tests proving unrelated tables are untouched.
- Every write response must include rows affected and warnings.

## 11) Proposed Future Endpoints (Sketch Only)

Do not implement in current pass.

- `POST /debug/tracks/identity-audit/submissions/validate`
- `POST /debug/tracks/identity-audit/submissions`
- `POST /debug/tracks/identity-audit/submissions/{submission_id}/dry-run`
- `POST /debug/tracks/identity-audit/submissions/{submission_id}/promote`

## 12) Promotion Semantics (Design Only; No Implementation Yet)

### 12.1 Separation of Concerns

- `save` stores artifacts only.
- `dry-run` computes plan only.
- `promote` is the only step allowed to mutate mapping tables.
- `learned-rules` is a later, separate pipeline and must not run inside promote.

### 12.2 Approved Group Decision Mutation Semantics

For each approved group decision:
- Target entity: suggested composition-family candidate keyed by `analysis_track_id` (or stable decision key fallback).
- Intended mutation:
  - mark candidate mapping rows as accepted/confirmed in the composition/analysis mapping surface;
  - if the same candidate rows are already accepted, treat as idempotent no-op.
- Conservative rule:
  - do not create speculative new rows from missing inputs during first promote version;
  - only update existing candidate rows that are unambiguously identified by the saved decision and current validated context.

### 12.3 Approved Track Decision Mutation Semantics

For each approved track decision:
- Target entity: track identity mapping/canonicalization candidate keyed by `release_track_id` and/or stable track decision key.
- Intended mutation:
  - mark the referenced candidate identity mapping as accepted/confirmed;
  - preserve source evidence/explanation fields where present;
  - if already accepted, treat as idempotent no-op.
- Conservative rule:
  - do not infer new canonical entities during initial promote;
  - only promote currently identifiable candidate mappings.

### 12.4 Allowed Tables to Change During Promote

Only tables directly representing identity/grouping decisions may change, specifically:
- `analysis_track_map` (primary promote target for accepted mapping state transitions)
- optional future promote metadata table(s), for example:
  - `identity_audit_promotion_log`
  - `identity_audit_submission` (`status`, `promoted_at`, promotion metadata fields)

Any additional mutable table must be explicitly added to this contract before implementation.

### 12.5 Tables That Must Never Change During Promote

Promotion must not mutate raw ingestion, listening history, ranking, or unrelated model tables, including:
- `raw_*` ingestion tables
- `fact_play_event*` tables/views
- Formula Lab / ranking materializations and caches
- unrelated artist/album/link tables not required for mapping-state transition
- filesystem artifacts (audit text files, review logs, exports)

### 12.6 Rejected and Skipped Decisions

- `reject`: explicit no-op for data mutation; may be recorded in promotion log/metrics only.
- `skip`: explicit no-op/deferred; may be recorded in promotion log/metrics only.
- Neither `reject` nor `skip` may modify mapping status in the first promote version.

### 12.7 Unknown/Stale Item Handling at Promote Time

Promote must re-run validation against current state.
- If item is now unknown/stale:
  - do not apply it;
  - surface as warning and per-item no-op reason (`unknown_group` / `unknown_track` / `stale_reference`).
- Promotion should continue best-effort for valid approved items unless strict mode is explicitly requested later.

### 12.8 Idempotency Rules

Promote must be idempotent by default.
- Re-promoting the same `submission_id` must not duplicate effects.
- Already-applied approved items must be counted as no-ops with explicit reason (`already_applied`).
- If duplicate promotion detection is implemented, response must clearly indicate duplicate/no-op execution.

### 12.9 Required Promote Response Counters (`rows_affected`)

Promote response must include both intent and actual effect counters:
- `rows_affected.analysis_track_map_updated`
- `rows_affected.analysis_track_map_inserted` (if inserts are ever allowed)
- `rows_affected.submission_status_updated`
- `rows_affected.promotion_log_inserted`
- `rows_affected.total_mutated_rows`

And decision-level rollups:
- `decisions.approved_groups_total`
- `decisions.approved_tracks_total`
- `decisions.approved_groups_applied`
- `decisions.approved_tracks_applied`
- `decisions.rejected_noop`
- `decisions.skipped_noop`
- `decisions.unknown_or_stale_noop`

### 12.10 Required Safety Tests for Future Promote Implementation

At minimum:
1. promote applies approved group decisions only.
2. promote applies approved track decisions only.
3. rejected/skipped are no-op mutations.
4. unknown/stale approved items are warning no-ops.
5. repeated promote is idempotent (`rows_affected.total_mutated_rows` becomes 0 on replay or duplicate-safe equivalent).
6. non-mutation guard tests prove unrelated tables remain unchanged (row counts/checksums).
7. dry-run vs promote parity test: plan counts match promote intent counts.
8. promote response includes complete `rows_affected` and warning fields.
