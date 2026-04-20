from __future__ import annotations

from backend.app.sync_state import _annotate_recent_fallback_sequences


def test_default_guess_routes_to_fallback_likely_complete() -> None:
    rows = [
        {
            "source_row_key": "row1",
            "played_at": "2026-04-17T00:00:00Z",
            "ms_played_method": "default_guess",
            "ms_played": 210000,
            "spotify_track_id": "track-a",
        }
    ]

    annotated = _annotate_recent_fallback_sequences(rows)

    assert annotated[0]["ms_played_confidence"] == "low"
    assert annotated[0]["ms_played_fallback_class"] == "fallback_likely_complete"


def test_repeat_sequence_routes_to_fallback_short_transition() -> None:
    rows = [
        {
            "source_row_key": "row1",
            "played_at": "2026-04-17T00:00:00Z",
            "ms_played_method": "default_guess",
            "ms_played": 220000,
            "spotify_track_id": "track-a",
        },
        {
            "source_row_key": "row2",
            "played_at": "2026-04-17T00:10:01Z",
            "ms_played_method": "default_guess",
            "ms_played": 220000,
            "track_duration_ms": 120000,
            "spotify_track_id": "track-a",
        },
    ]

    annotated = _annotate_recent_fallback_sequences(rows)

    assert annotated[1]["ms_played_fallback_class"] == "fallback_short_transition"


def test_tiny_play_keeps_tiny_or_skip_class() -> None:
    rows = [
        {
            "source_row_key": "row1",
            "played_at": "2026-04-17T00:00:00Z",
            "ms_played_method": "default_guess",
            "ms_played": 12000,
            "spotify_track_id": "track-a",
        }
    ]

    annotated = _annotate_recent_fallback_sequences(rows)

    assert annotated[0]["ms_played_fallback_class"] == "fallback_likely_complete"
