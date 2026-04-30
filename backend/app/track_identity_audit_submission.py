from __future__ import annotations

import json
from datetime import UTC, datetime
import sqlite3
from typing import Any

from backend.app.db import sqlite_connection
from backend.app.track_identity_audit import query_ambiguous_review_queue


DECISION_BUCKETS = ("approved", "rejected", "skipped")
DECISION_VALUE_BY_BUCKET = {
    "approved": "approve",
    "rejected": "reject",
    "skipped": "skip",
}


def _empty_decision_shape() -> dict[str, list[dict[str, Any]]]:
    return {
        "approved": [],
        "rejected": [],
        "skipped": [],
    }


def _empty_response() -> dict[str, Any]:
    return {
        "ok": True,
        "summary": {
            "total_decisions": 0,
            "group_decisions": 0,
            "track_decisions": 0,
            "approved": 0,
            "rejected": 0,
            "skipped": 0,
            "unknown_groups": 0,
            "unknown_tracks": 0,
            "warnings": 0,
        },
        "warnings": [],
        "unknown_items": {
            "groups": [],
            "tracks": [],
        },
        "validated": {
            "groups": _empty_decision_shape(),
            "tracks": _empty_decision_shape(),
        },
    }


def _current_suggested_group_ids() -> set[int]:
    with sqlite_connection() as connection:
        rows = connection.execute(
            """
            SELECT DISTINCT analysis_track_id
            FROM analysis_track_map
            WHERE status = 'suggested'
            """
        ).fetchall()
    return {int(row[0]) for row in rows if row and row[0] is not None}


def _current_ambiguous_track_refs() -> tuple[set[str], set[int]]:
    payload = query_ambiguous_review_queue(limit=5000, offset=0)
    entry_keys: set[str] = set()
    release_track_ids: set[int] = set()
    for item in payload.get("items", []):
        if not isinstance(item, dict):
            continue
        raw_entry_id = item.get("entry_id")
        if isinstance(raw_entry_id, str) and raw_entry_id.strip():
            entry_key = f"track:{raw_entry_id.strip()}"
            entry_keys.add(entry_key)
        raw_release_track_id = item.get("release_track_id")
        if isinstance(raw_release_track_id, int):
            release_track_ids.add(raw_release_track_id)
    return entry_keys, release_track_ids


def _normalized_item(
    raw_item: Any,
    *,
    item_type: str,
    bucket: str,
    index: int,
    warnings: list[str],
    seen_keys: set[str],
) -> dict[str, Any] | None:
    if not isinstance(raw_item, dict):
        warnings.append(f"{item_type}.{bucket}[{index}] is not an object; skipped.")
        return None

    raw_key = raw_item.get("decision_key")
    if isinstance(raw_key, str) and raw_key.strip():
        decision_key = raw_key.strip()
    else:
        fallback_id = raw_item.get("id")
        if isinstance(fallback_id, (str, int)):
            decision_key = f"{item_type}:{fallback_id}"
        else:
            decision_key = f"{item_type}:{bucket}:{index}"
            warnings.append(f"{item_type}.{bucket}[{index}] missing stable decision key.")

    if decision_key in seen_keys:
        warnings.append(f"Duplicate decision_key detected: {decision_key}")
    else:
        seen_keys.add(decision_key)

    item_id = raw_item.get("id")
    if item_id is None:
        warnings.append(f"{decision_key} missing id.")

    decision_value = DECISION_VALUE_BY_BUCKET[bucket]
    raw_decision = raw_item.get("decision")
    if isinstance(raw_decision, str) and raw_decision.strip():
        normalized_decision = raw_decision.strip().lower()
        if normalized_decision != decision_value:
            warnings.append(
                f"{decision_key} decision '{normalized_decision}' mismatches bucket '{bucket}'; bucket wins."
            )

    label = raw_item.get("label") or raw_item.get("title") or raw_item.get("name")
    if not isinstance(label, str) or not label.strip():
        if isinstance(item_id, (str, int)):
            label = f"{item_type} {item_id}"
        else:
            label = decision_key

    family = raw_item.get("family")
    bucket_label = raw_item.get("bucket")
    source = raw_item.get("source")
    compact_source = source if isinstance(source, dict) else None

    return {
        "decision_key": decision_key,
        "id": item_id,
        "decision": decision_value,
        "label": label,
        "family": family if isinstance(family, str) else None,
        "bucket": bucket_label if isinstance(bucket_label, str) else None,
        "source": compact_source,
    }


def validate_identity_audit_submission_preview(
    payload: dict[str, Any],
    *,
    known_group_ids: set[int] | None = None,
    known_track_entry_keys: set[str] | None = None,
    known_track_ids: set[int] | None = None,
) -> dict[str, Any]:
    response = _empty_response()
    warnings: list[str] = []
    seen_keys: set[str] = set()

    decisions = payload.get("decisions")
    if not isinstance(decisions, dict):
        warnings.append("Missing decisions object; defaulting to empty decisions.")
        decisions = {}

    groups = decisions.get("groups")
    if not isinstance(groups, dict):
        warnings.append("Missing decisions.groups object; defaulting to empty.")
        groups = {}

    tracks = decisions.get("tracks")
    if not isinstance(tracks, dict):
        warnings.append("Missing decisions.tracks object; defaulting to empty.")
        tracks = {}

    validated_groups = _empty_decision_shape()
    validated_tracks = _empty_decision_shape()

    for bucket in DECISION_BUCKETS:
        group_items = groups.get(bucket, [])
        if not isinstance(group_items, list):
            warnings.append(f"decisions.groups.{bucket} must be an array; treated as empty.")
            group_items = []
        for index, item in enumerate(group_items):
            normalized = _normalized_item(
                item,
                item_type="group",
                bucket=bucket,
                index=index,
                warnings=warnings,
                seen_keys=seen_keys,
            )
            if normalized is not None:
                validated_groups[bucket].append(normalized)

        track_items = tracks.get(bucket, [])
        if not isinstance(track_items, list):
            warnings.append(f"decisions.tracks.{bucket} must be an array; treated as empty.")
            track_items = []
        for index, item in enumerate(track_items):
            normalized = _normalized_item(
                item,
                item_type="track",
                bucket=bucket,
                index=index,
                warnings=warnings,
                seen_keys=seen_keys,
            )
            if normalized is not None:
                validated_tracks[bucket].append(normalized)

    if known_group_ids is None:
        known_group_ids = _current_suggested_group_ids()
    if known_track_entry_keys is None or known_track_ids is None:
        current_keys, current_ids = _current_ambiguous_track_refs()
        known_track_entry_keys = current_keys if known_track_entry_keys is None else known_track_entry_keys
        known_track_ids = current_ids if known_track_ids is None else known_track_ids

    unknown_groups: list[dict[str, Any]] = []
    for bucket in DECISION_BUCKETS:
        for item in validated_groups[bucket]:
            item_id = item.get("id")
            group_id: int | None = None
            if isinstance(item_id, int):
                group_id = item_id
            elif isinstance(item_id, str) and item_id.isdigit():
                group_id = int(item_id)
            if group_id is None or group_id not in known_group_ids:
                unknown_groups.append(
                    {
                        "decision_key": item.get("decision_key"),
                        "id": item.get("id"),
                        "decision": item.get("decision"),
                        "label": item.get("label"),
                    }
                )

    unknown_tracks: list[dict[str, Any]] = []
    for bucket in DECISION_BUCKETS:
        for item in validated_tracks[bucket]:
            decision_key = item.get("decision_key")
            item_id = item.get("id")
            id_match = False
            if isinstance(item_id, int):
                id_match = item_id in known_track_ids
            elif isinstance(item_id, str) and item_id.isdigit():
                id_match = int(item_id) in known_track_ids
            key_match = isinstance(decision_key, str) and decision_key in known_track_entry_keys
            if not id_match and not key_match:
                unknown_tracks.append(
                    {
                        "decision_key": decision_key,
                        "id": item.get("id"),
                        "decision": item.get("decision"),
                        "label": item.get("label"),
                    }
                )

    total_group_decisions = sum(len(validated_groups[bucket]) for bucket in DECISION_BUCKETS)
    total_track_decisions = sum(len(validated_tracks[bucket]) for bucket in DECISION_BUCKETS)
    total_decisions = total_group_decisions + total_track_decisions
    if total_decisions == 0:
        warnings.append("No decisions to validate.")

    response["validated"]["groups"] = validated_groups
    response["validated"]["tracks"] = validated_tracks
    response["unknown_items"]["groups"] = unknown_groups
    response["unknown_items"]["tracks"] = unknown_tracks
    response["warnings"] = warnings
    response["summary"] = {
        "total_decisions": total_decisions,
        "group_decisions": total_group_decisions,
        "track_decisions": total_track_decisions,
        "approved": len(validated_groups["approved"]) + len(validated_tracks["approved"]),
        "rejected": len(validated_groups["rejected"]) + len(validated_tracks["rejected"]),
        "skipped": len(validated_groups["skipped"]) + len(validated_tracks["skipped"]),
        "unknown_groups": len(unknown_groups),
        "unknown_tracks": len(unknown_tracks),
        "warnings": len(warnings),
    }
    return response


def save_identity_audit_submission(payload: dict[str, Any]) -> dict[str, Any]:
    validation = validate_identity_audit_submission_preview(payload)
    created_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    payload_json = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    validation_json = json.dumps(validation, ensure_ascii=True, separators=(",", ":"), sort_keys=True)

    with sqlite_connection(write=True) as connection:
        cursor = connection.execute(
            """
            INSERT INTO identity_audit_submission (
              created_at,
              payload_json,
              validation_json,
              status,
              promoted_at,
              notes
            ) VALUES (?, ?, ?, 'saved', NULL, NULL)
            """,
            (created_at, payload_json, validation_json),
        )
        submission_id = int(cursor.lastrowid)

    return {
        "ok": True,
        "submission_id": submission_id,
        "status": "saved",
        "created_at": created_at,
        "summary": dict(validation.get("summary") or {}),
        "warnings": list(validation.get("warnings") or []),
        "unknown_items": {
            "groups": list((validation.get("unknown_items") or {}).get("groups") or []),
            "tracks": list((validation.get("unknown_items") or {}).get("tracks") or []),
        },
    }


def _load_json_object(raw_json: str | None) -> dict[str, Any]:
    if not raw_json:
        return {}
    try:
        parsed = json.loads(raw_json)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def list_identity_audit_submissions(*, limit: int = 20, offset: int = 0) -> dict[str, Any]:
    bounded_limit = max(1, min(int(limit), 100))
    bounded_offset = max(0, int(offset))

    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        total = int(connection.execute("SELECT count(*) FROM identity_audit_submission").fetchone()[0])
        rows = connection.execute(
            """
            SELECT
              id,
              created_at,
              status,
              validation_json,
              notes
            FROM identity_audit_submission
            ORDER BY id DESC
            LIMIT ?
            OFFSET ?
            """,
            (bounded_limit, bounded_offset),
        ).fetchall()

    items: list[dict[str, Any]] = []
    for row in rows:
        validation = _load_json_object(str(row["validation_json"] or ""))
        summary = dict(validation.get("summary") or {})
        warnings = list(validation.get("warnings") or [])
        unknown_items = dict(validation.get("unknown_items") or {})
        unknown_groups = list(unknown_items.get("groups") or [])
        unknown_tracks = list(unknown_items.get("tracks") or [])
        items.append(
            {
                "id": int(row["id"]),
                "created_at": str(row["created_at"] or ""),
                "status": str(row["status"] or "saved"),
                "summary": summary,
                "warnings_count": len(warnings),
                "unknown_groups": len(unknown_groups),
                "unknown_tracks": len(unknown_tracks),
                "notes": row["notes"] if row["notes"] is None else str(row["notes"]),
            }
        )

    return {
        "ok": True,
        "items": items,
        "total": total,
    }


def get_identity_audit_submission(submission_id: int) -> dict[str, Any] | None:
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        row = connection.execute(
            """
            SELECT
              id,
              created_at,
              status,
              payload_json,
              validation_json,
              notes,
              promoted_at
            FROM identity_audit_submission
            WHERE id = ?
            LIMIT 1
            """,
            (int(submission_id),),
        ).fetchone()

    if row is None:
        return None

    return {
        "ok": True,
        "item": {
            "id": int(row["id"]),
            "created_at": str(row["created_at"] or ""),
            "status": str(row["status"] or "saved"),
            "payload": _load_json_object(str(row["payload_json"] or "")),
            "validation": _load_json_object(str(row["validation_json"] or "")),
            "notes": row["notes"] if row["notes"] is None else str(row["notes"]),
            "promoted_at": row["promoted_at"] if row["promoted_at"] is None else str(row["promoted_at"]),
        },
    }


def dry_run_identity_audit_submission(submission_id: int) -> dict[str, Any] | None:
    submission = get_identity_audit_submission(submission_id)
    if submission is None:
        return None

    submission_item = dict(submission.get("item") or {})
    payload = dict(submission_item.get("payload") or {})
    validation = validate_identity_audit_submission_preview(payload)
    validated = dict(validation.get("validated") or {})
    validated_groups = dict(validated.get("groups") or {})
    validated_tracks = dict(validated.get("tracks") or {})

    approved_group_items = list(validated_groups.get("approved") or [])
    approved_track_items = list(validated_tracks.get("approved") or [])
    rejected_items = [
        *list(validated_groups.get("rejected") or []),
        *list(validated_tracks.get("rejected") or []),
    ]
    skipped_items = [
        *list(validated_groups.get("skipped") or []),
        *list(validated_tracks.get("skipped") or []),
    ]

    plan_groups = [
        {
            "decision_key": item.get("decision_key"),
            "id": item.get("id"),
            "label": item.get("label"),
            "family": item.get("family"),
            "bucket": item.get("bucket"),
            "action": "would_accept_group",
            "source": item.get("source") if isinstance(item.get("source"), dict) else {},
        }
        for item in approved_group_items
    ]
    plan_tracks = [
        {
            "decision_key": item.get("decision_key"),
            "id": item.get("id"),
            "label": item.get("label"),
            "family": item.get("family"),
            "bucket": item.get("bucket"),
            "action": "would_accept_track_mapping",
            "source": item.get("source") if isinstance(item.get("source"), dict) else {},
        }
        for item in approved_track_items
    ]
    noop_rejected = [
        {
            "decision_key": item.get("decision_key"),
            "id": item.get("id"),
            "label": item.get("label"),
            "decision": "reject",
            "noop_reason": "rejected",
        }
        for item in rejected_items
    ]
    noop_skipped = [
        {
            "decision_key": item.get("decision_key"),
            "id": item.get("id"),
            "label": item.get("label"),
            "decision": "skip",
            "noop_reason": "skipped",
        }
        for item in skipped_items
    ]

    summary = dict(validation.get("summary") or {})
    warnings = list(validation.get("warnings") or [])

    return {
        "ok": True,
        "submission_id": int(submission_item.get("id") or submission_id),
        "status": "dry_run",
        "validation": validation,
        "summary": {
            "approved_groups": len(plan_groups),
            "approved_tracks": len(plan_tracks),
            "rejected": len(noop_rejected),
            "skipped": len(noop_skipped),
            "would_apply": len(plan_groups) + len(plan_tracks),
            "warnings": int(summary.get("warnings") or 0),
            "unknown_groups": int(summary.get("unknown_groups") or 0),
            "unknown_tracks": int(summary.get("unknown_tracks") or 0),
        },
        "plan": {
            "groups": plan_groups,
            "tracks": plan_tracks,
        },
        "noops": {
            "rejected": noop_rejected,
            "skipped": noop_skipped,
        },
        "warnings": warnings,
    }
