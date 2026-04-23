from __future__ import annotations

from typing import Literal, TypedDict


TrackSectionSource = Literal["db", "spotify", "history_cache", "user_cache", "short_cache", "mixed"]
TrackSectionKind = Literal["recent_tracks", "top_tracks"]
TrackSectionWindow = Literal["recent", "all_time"]


class CanonicalTrackSectionDebug(TypedDict, total=False):
    source: TrackSectionSource
    primary_source: TrackSectionSource
    fallback_source: TrackSectionSource | None
    section_kind: TrackSectionKind
    section_window: TrackSectionWindow


class CanonicalTrackSectionItem(TypedDict, total=False):
    track_id: str | None
    track_name: str | None
    artist_name: str | None
    album_name: str | None
    album_release_year: str | None
    artists: list[dict[str, str | None]] | None
    duration_ms: int | None
    duration_seconds: float | None
    uri: str | None
    preview_url: str | None
    url: str | None
    image_url: str | None
    album_id: str | None
    album_url: str | None
    spotify_played_at: str | None
    spotify_played_at_unix_ms: int | None
    spotify_context_type: str | None
    spotify_context_uri: str | None
    spotify_context_url: str | None
    spotify_context_href: str | None
    spotify_is_local: bool | None
    spotify_track_type: str | None
    spotify_track_number: int | None
    spotify_disc_number: int | None
    spotify_explicit: bool | None
    spotify_popularity: int | None
    spotify_album_type: str | None
    spotify_album_total_tracks: int | None
    spotify_available_markets_count: int | None
    played_at_gap_ms: int | None
    estimated_played_ms: int | None
    estimated_played_seconds: float | None
    estimated_completion_ratio: float | None
    play_count: int | None
    all_time_play_count: int | None
    recent_play_count: int | None
    first_played_at: str | None
    last_played_at: str | None
    listening_span_days: float | None
    listening_span_years: float | None
    active_months_count: int | None
    span_months_count: int | None
    consistency_ratio: float | None
    longevity_score: float | None
    debug: CanonicalTrackSectionDebug


class CanonicalTrackSectionPayload(TypedDict):
    items: list[CanonicalTrackSectionItem]
    available: bool
    recent_range: Literal["short_term", "medium_term"] | None
    recent_window_days: int | None
    debug: CanonicalTrackSectionDebug
