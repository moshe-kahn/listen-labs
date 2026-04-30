from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Literal, TypedDict

from backend.app.db import sqlite_connection


MergedTrackSourceFilter = Literal["all", "recent", "history", "both"]
MergedTrackSourceLabel = Literal["recent", "history", "both"]


class MergedTrackAggregateItem(TypedDict, total=False):
    track_identity: str
    track_id: str | None
    uri: str | None
    album_id: str | None
    track_name: str | None
    artist_name: str | None
    album_name: str | None
    play_count: int
    all_time_play_count: int
    recent_play_count: int
    first_played_at: str | None
    last_played_at: str | None
    listening_span_days: int
    listening_span_years: float
    active_months_count: int
    span_months_count: int
    consistency_ratio: float
    longevity_score: float
    has_recent_source: bool
    has_history_source: bool
    source_label: MergedTrackSourceLabel
    recent_source_event_count: int
    history_source_event_count: int
    matched_source_event_count: int


class MergedTrackAggregateResult(TypedDict):
    items: list[MergedTrackAggregateItem]
    excluded_unknown_identity_count: int


def _track_longevity_score(
    *,
    span_days: int,
    active_months_count: int,
    span_months_count: int,
    play_count: int,
) -> float:
    if span_days <= 0 or active_months_count <= 0 or span_months_count <= 0 or play_count <= 0:
        return 0.0

    span_years = span_days / 365.25
    consistency_ratio = min(1.0, active_months_count / span_months_count)
    active_months_factor = active_months_count ** 0.75
    consistency_factor = consistency_ratio ** 1.15
    span_factor = max(span_years, 0.0) ** 2.7
    play_factor = max(play_count, 1) ** 0.02
    return active_months_factor * consistency_factor * span_factor * play_factor


def get_merged_track_aggregate(
    *,
    limit: int = 200,
    recent_window_days: int = 28,
    source_filter: MergedTrackSourceFilter = "all",
    as_of_iso: str | None = None,
) -> MergedTrackAggregateResult:
    bounded_limit = max(1, int(limit))
    bounded_window_days = max(0, int(recent_window_days))
    normalized_source_filter: MergedTrackSourceFilter = (
        source_filter if source_filter in {"all", "recent", "history", "both"} else "all"
    )
    as_of_dt = (
        datetime.fromisoformat(as_of_iso.replace("Z", "+00:00"))
        if as_of_iso
        else datetime.now(UTC)
    )
    recent_cutoff_iso = (
        (as_of_dt - timedelta(days=bounded_window_days))
        .astimezone(UTC)
        .isoformat()
        .replace("+00:00", "Z")
    )

    source_filter_clause = ""
    if normalized_source_filter == "recent":
        source_filter_clause = "AND agg.recent_source_event_count > 0 AND agg.history_source_event_count = 0"
    elif normalized_source_filter == "history":
        source_filter_clause = "AND agg.history_source_event_count > 0 AND agg.recent_source_event_count = 0"
    elif normalized_source_filter == "both":
        source_filter_clause = "AND agg.history_source_event_count > 0 AND agg.recent_source_event_count > 0"

    with sqlite_connection() as connection:
        connection.row_factory = None
        base_cte = """
            WITH normalized AS (
              SELECT
                id,
                canonical_ended_at AS played_at,
                CASE
                  WHEN spotify_track_id IS NOT NULL AND spotify_track_id != '' THEN spotify_track_id
                  WHEN spotify_track_uri IS NOT NULL AND spotify_track_uri != '' THEN spotify_track_uri
                  ELSE '__unknown__:' || LOWER(TRIM(COALESCE(track_name_canonical, ''))) || ':' || LOWER(TRIM(COALESCE(artist_name_canonical, '')))
                END AS track_identity,
                CASE
                  WHEN (spotify_track_id IS NOT NULL AND spotify_track_id != '')
                    OR (spotify_track_uri IS NOT NULL AND spotify_track_uri != '')
                    OR (
                      LENGTH(TRIM(COALESCE(track_name_canonical, ''))) > 0
                      AND LENGTH(TRIM(COALESCE(artist_name_canonical, ''))) > 0
                    )
                  THEN 0
                  ELSE 1
                END AS is_unknown_identity,
                spotify_track_id,
                spotify_track_uri,
                spotify_album_id,
                track_name_canonical AS track_name_raw,
                artist_name_canonical AS artist_name_raw,
                album_name_canonical AS album_name_raw,
                CASE WHEN raw_spotify_recent_id IS NOT NULL THEN 1 ELSE 0 END AS has_recent,
                CASE WHEN raw_spotify_history_id IS NOT NULL THEN 1 ELSE 0 END AS has_history,
                SUBSTR(canonical_ended_at, 1, 7) AS active_month
              FROM v_fact_play_event_with_sources
              WHERE canonical_ended_at IS NOT NULL
            ),
            ranked AS (
              SELECT
                normalized.*,
                ROW_NUMBER() OVER (
                  PARTITION BY track_identity
                  ORDER BY played_at DESC, id DESC
                ) AS latest_rank
              FROM normalized
            ),
            agg AS (
              SELECT
                track_identity,
                MAX(is_unknown_identity) AS is_unknown_identity,
                COUNT(*) AS total_play_count,
                SUM(CASE WHEN played_at >= ? THEN 1 ELSE 0 END) AS recent_play_count,
                MIN(played_at) AS first_played_at,
                MAX(played_at) AS last_played_at,
                COUNT(DISTINCT active_month) AS active_months_count,
                SUM(has_recent) AS recent_source_event_count,
                SUM(has_history) AS history_source_event_count,
                SUM(CASE WHEN has_recent = 1 AND has_history = 1 THEN 1 ELSE 0 END) AS matched_source_event_count
              FROM normalized
              GROUP BY track_identity
            )
        """
        excluded_unknown_identity_count = int(
            connection.execute(
                f"""
                {base_cte}
                SELECT COALESCE(SUM(agg.total_play_count), 0)
                FROM agg
                WHERE agg.is_unknown_identity > 0
                {source_filter_clause}
                """,
                (recent_cutoff_iso,),
            ).fetchone()[0]
            or 0
        )
        rows = connection.execute(
            f"""
            {base_cte}
            SELECT
              agg.track_identity,
              agg.is_unknown_identity,
              latest.spotify_track_id,
              latest.spotify_track_uri,
              latest.spotify_album_id,
              latest.track_name_raw,
              latest.artist_name_raw,
              latest.album_name_raw,
              agg.total_play_count,
              agg.recent_play_count,
              agg.first_played_at,
              agg.last_played_at,
              agg.active_months_count,
              agg.recent_source_event_count,
              agg.history_source_event_count,
              agg.matched_source_event_count
            FROM agg
            JOIN ranked latest
              ON latest.track_identity = agg.track_identity
             AND latest.latest_rank = 1
            WHERE agg.is_unknown_identity = 0
            {source_filter_clause}
            ORDER BY
              agg.total_play_count DESC,
              agg.last_played_at DESC,
              agg.track_identity ASC
            LIMIT ?
            """,
            (recent_cutoff_iso, bounded_limit),
        ).fetchall()

    items: list[MergedTrackAggregateItem] = []
    for row in rows:
        (
            track_identity,
            _is_unknown_identity,
            spotify_track_id,
            spotify_track_uri,
            spotify_album_id,
            track_name_raw,
            artist_name_raw,
            album_name_raw,
            total_play_count,
            recent_play_count,
            first_played_at,
            last_played_at,
            active_months_count,
            recent_source_event_count,
            history_source_event_count,
            matched_source_event_count,
        ) = row

        first_dt = datetime.fromisoformat(str(first_played_at).replace("Z", "+00:00")) if first_played_at else None
        last_dt = datetime.fromisoformat(str(last_played_at).replace("Z", "+00:00")) if last_played_at else None
        span_days = max(0, int((last_dt - first_dt).total_seconds() // 86_400)) if first_dt and last_dt else 0
        span_months_count = max(1, int(span_days // 30) + 1) if span_days > 0 else 0
        active_months = int(active_months_count or 0)
        play_count = int(total_play_count or 0)
        consistency_ratio = (
            min(1.0, active_months / span_months_count)
            if span_months_count > 0 and active_months > 0
            else 0.0
        )
        has_recent_source = int(recent_source_event_count or 0) > 0
        has_history_source = int(history_source_event_count or 0) > 0
        if has_recent_source and has_history_source:
            source_label: MergedTrackSourceLabel = "both"
        elif has_recent_source:
            source_label = "recent"
        else:
            source_label = "history"

        items.append(
            {
                "track_identity": str(track_identity),
                "track_id": spotify_track_id or str(track_identity),
                "uri": spotify_track_uri,
                "album_id": spotify_album_id,
                "track_name": track_name_raw,
                "artist_name": artist_name_raw,
                "album_name": album_name_raw,
                "play_count": play_count,
                "all_time_play_count": play_count,
                "recent_play_count": int(recent_play_count or 0),
                "first_played_at": first_played_at,
                "last_played_at": last_played_at,
                "listening_span_days": span_days,
                "listening_span_years": round(span_days / 365.25, 3) if span_days > 0 else 0.0,
                "active_months_count": active_months,
                "span_months_count": span_months_count,
                "consistency_ratio": round(consistency_ratio, 4),
                "longevity_score": round(
                    _track_longevity_score(
                        span_days=span_days,
                        active_months_count=active_months,
                        span_months_count=span_months_count,
                        play_count=play_count,
                    ),
                    4,
                ),
                "has_recent_source": has_recent_source,
                "has_history_source": has_history_source,
                "source_label": source_label,
                "recent_source_event_count": int(recent_source_event_count or 0),
                "history_source_event_count": int(history_source_event_count or 0),
                "matched_source_event_count": int(matched_source_event_count or 0),
            }
        )

    return {
        "items": items,
        "excluded_unknown_identity_count": excluded_unknown_identity_count,
    }


def list_merged_track_aggregate(
    *,
    limit: int = 200,
    recent_window_days: int = 28,
    source_filter: MergedTrackSourceFilter = "all",
    as_of_iso: str | None = None,
) -> list[MergedTrackAggregateItem]:
    return get_merged_track_aggregate(
        limit=limit,
        recent_window_days=recent_window_days,
        source_filter=source_filter,
        as_of_iso=as_of_iso,
    )["items"]
