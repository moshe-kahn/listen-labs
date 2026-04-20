from __future__ import annotations

import unittest

from backend.app.play_event_matcher import match_recent_history_rows


class PlayEventMatcherTests(unittest.TestCase):
    def test_tight_and_wide_matching_tiers(self) -> None:
        recent_rows = [
            {
                "id": 1,
                "played_at": "2026-04-17T19:00:10.000Z",
                "spotify_track_id": "track-a",
                "spotify_track_uri": "spotify:track:track-a",
            },
            {
                "id": 2,
                "played_at": "2026-04-17T19:10:25.000Z",
                "spotify_track_id": "track-b",
                "spotify_track_uri": "spotify:track:track-b",
            },
        ]
        history_rows = [
            {
                "id": 11,
                "played_at": "2026-04-17T19:00:02Z",
                "spotify_track_id": "track-a",
                "spotify_track_uri": "spotify:track:track-a",
            },
            {
                "id": 12,
                "played_at": "2026-04-17T19:10:08Z",
                "spotify_track_id": "track-b",
                "spotify_track_uri": "spotify:track:track-b",
            },
        ]

        result = match_recent_history_rows(
            recent_rows=recent_rows,
            history_rows=history_rows,
            tight_seconds=10,
            wide_seconds=30,
        )

        self.assertEqual(2, len(result.pairs))
        self.assertEqual(1, result.tight_10s_count)
        self.assertEqual(1, result.wide_30s_count)
        self.assertEqual([], result.unmatched_recent_ids)
        self.assertEqual([], result.unmatched_history_ids)

    def test_ambiguous_repeated_same_track_is_conservative(self) -> None:
        recent_rows = [
            {
                "id": 1,
                "played_at": "2026-04-17T19:00:10.000Z",
                "spotify_track_id": "track-a",
                "spotify_track_uri": "spotify:track:track-a",
            }
        ]
        history_rows = [
            {
                "id": 11,
                "played_at": "2026-04-17T19:00:05Z",
                "spotify_track_id": "track-a",
                "spotify_track_uri": "spotify:track:track-a",
            },
            {
                "id": 12,
                "played_at": "2026-04-17T19:00:06Z",
                "spotify_track_id": "track-a",
                "spotify_track_uri": "spotify:track:track-a",
            },
        ]

        result = match_recent_history_rows(
            recent_rows=recent_rows,
            history_rows=history_rows,
            tight_seconds=10,
            wide_seconds=30,
        )

        self.assertEqual(0, len(result.pairs))
        self.assertEqual([1], result.unmatched_recent_ids)
        self.assertEqual([11, 12], result.unmatched_history_ids)


if __name__ == "__main__":
    unittest.main()

