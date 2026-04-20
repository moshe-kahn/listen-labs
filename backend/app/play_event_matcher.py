from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


def _parse_iso_z(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _match_key(row: dict[str, Any]) -> tuple[str, str] | None:
    track_id = row.get("spotify_track_id")
    if isinstance(track_id, str) and track_id.strip():
        return ("track_id", track_id.strip())
    track_uri = row.get("spotify_track_uri")
    if isinstance(track_uri, str) and track_uri.strip():
        return ("track_uri", track_uri.strip())
    return None


@dataclass(frozen=True)
class MatchPair:
    recent_id: int
    history_id: int
    delta_ms: int
    match_tier: str


@dataclass(frozen=True)
class MatchResult:
    pairs: list[MatchPair]
    unmatched_recent_ids: list[int]
    unmatched_history_ids: list[int]
    tight_10s_count: int
    wide_30s_count: int


def _build_candidates(
    *,
    recent_rows: list[dict[str, Any]],
    history_rows: list[dict[str, Any]],
) -> tuple[dict[tuple[str, str], list[dict[str, Any]]], dict[tuple[str, str], list[dict[str, Any]]]]:
    grouped_recent: dict[tuple[str, str], list[dict[str, Any]]] = {}
    grouped_history: dict[tuple[str, str], list[dict[str, Any]]] = {}

    for row in recent_rows:
        key = _match_key(row)
        if key is None:
            continue
        grouped_recent.setdefault(key, []).append(row)
    for row in history_rows:
        key = _match_key(row)
        if key is None:
            continue
        grouped_history.setdefault(key, []).append(row)

    for key in grouped_recent:
        grouped_recent[key].sort(key=lambda r: (_parse_iso_z(str(r["played_at"])), int(r["id"])))
    for key in grouped_history:
        grouped_history[key].sort(key=lambda r: (_parse_iso_z(str(r["played_at"])), int(r["id"])))
    return grouped_recent, grouped_history


def _greedy_non_crossing_pass(
    *,
    recent_rows: list[dict[str, Any]],
    history_rows: list[dict[str, Any]],
    used_recent_ids: set[int],
    used_history_ids: set[int],
    lower_bound_ms: int,
    upper_bound_ms: int,
    tier_name: str,
) -> list[MatchPair]:
    pairs: list[MatchPair] = []
    last_history_dt: datetime | None = None

    for recent in recent_rows:
        recent_id = int(recent["id"])
        if recent_id in used_recent_ids:
            continue
        recent_dt = _parse_iso_z(str(recent["played_at"]))

        ranked_candidates: list[tuple[int, dict[str, Any], datetime]] = []
        for history in history_rows:
            history_id = int(history["id"])
            if history_id in used_history_ids:
                continue
            history_dt = _parse_iso_z(str(history["played_at"]))
            if last_history_dt is not None and history_dt < last_history_dt:
                continue
            delta_ms = abs(int((recent_dt - history_dt).total_seconds() * 1000))
            if lower_bound_ms < delta_ms <= upper_bound_ms:
                ranked_candidates.append((delta_ms, history, history_dt))

        if not ranked_candidates:
            continue

        ranked_candidates.sort(key=lambda item: (item[0], int(item[1]["id"])))

        # Conservative ambiguity guard for repeated same-track sequences:
        # if top two candidates are very close, skip matching this row.
        if len(ranked_candidates) > 1:
            if abs(ranked_candidates[1][0] - ranked_candidates[0][0]) <= 1000:
                continue

        chosen_delta_ms, chosen_history, chosen_history_dt = ranked_candidates[0]
        chosen_history_id = int(chosen_history["id"])

        used_recent_ids.add(recent_id)
        used_history_ids.add(chosen_history_id)
        last_history_dt = chosen_history_dt
        pairs.append(
            MatchPair(
                recent_id=recent_id,
                history_id=chosen_history_id,
                delta_ms=chosen_delta_ms,
                match_tier=tier_name,
            )
        )

    return pairs


def match_recent_history_rows(
    *,
    recent_rows: list[dict[str, Any]],
    history_rows: list[dict[str, Any]],
    tight_seconds: int = 10,
    wide_seconds: int = 30,
) -> MatchResult:
    grouped_recent, grouped_history = _build_candidates(
        recent_rows=recent_rows,
        history_rows=history_rows,
    )
    keys = sorted(set(grouped_recent.keys()) & set(grouped_history.keys()))

    used_recent_ids: set[int] = set()
    used_history_ids: set[int] = set()
    all_pairs: list[MatchPair] = []

    tight_ms = int(tight_seconds * 1000)
    wide_ms = int(wide_seconds * 1000)

    for key in keys:
        recent_group = grouped_recent[key]
        history_group = grouped_history[key]
        all_pairs.extend(
            _greedy_non_crossing_pass(
                recent_rows=recent_group,
                history_rows=history_group,
                used_recent_ids=used_recent_ids,
                used_history_ids=used_history_ids,
                lower_bound_ms=0,
                upper_bound_ms=tight_ms,
                tier_name="tight_10s",
            )
        )

    for key in keys:
        recent_group = grouped_recent[key]
        history_group = grouped_history[key]
        all_pairs.extend(
            _greedy_non_crossing_pass(
                recent_rows=recent_group,
                history_rows=history_group,
                used_recent_ids=used_recent_ids,
                used_history_ids=used_history_ids,
                lower_bound_ms=tight_ms,
                upper_bound_ms=wide_ms,
                tier_name="wide_30s",
            )
        )

    unmatched_recent_ids = sorted(
        int(row["id"]) for row in recent_rows if int(row["id"]) not in used_recent_ids
    )
    unmatched_history_ids = sorted(
        int(row["id"]) for row in history_rows if int(row["id"]) not in used_history_ids
    )

    tight_count = sum(1 for pair in all_pairs if pair.match_tier == "tight_10s")
    wide_count = sum(1 for pair in all_pairs if pair.match_tier == "wide_30s")
    return MatchResult(
        pairs=all_pairs,
        unmatched_recent_ids=unmatched_recent_ids,
        unmatched_history_ids=unmatched_history_ids,
        tight_10s_count=tight_count,
        wide_30s_count=wide_count,
    )

