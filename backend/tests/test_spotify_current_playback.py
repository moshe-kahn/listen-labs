from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

from backend.app.spotify_current_playback import get_current_playback_for_user


class SpotifyCurrentPlaybackTests(unittest.TestCase):
    def test_missing_auth_row_returns_skipped(self) -> None:
        with patch("backend.app.spotify_current_playback.get_spotify_auth_record", return_value=None):
            result = asyncio.run(get_current_playback_for_user("user-1"))

        self.assertEqual("skipped", result["status"])
        self.assertEqual("not_found", result["error_type"])
        self.assertIsNone(result["snapshot"])

    def test_missing_scope_marks_reauth(self) -> None:
        with patch(
            "backend.app.spotify_current_playback.get_spotify_auth_record",
            return_value={"spotify_user_id": "sp-1", "scopes": "user-read-recently-played"},
        ), patch("backend.app.spotify_current_playback.mark_spotify_reauth_required") as mark_mock:
            result = asyncio.run(get_current_playback_for_user("user-1"))

        self.assertEqual("failed", result["status"])
        self.assertEqual("missing_scope", result["error_type"])
        self.assertTrue(result["reauth_marked"])
        mark_mock.assert_called_once()

    def test_playback_204_returns_ok_without_snapshot(self) -> None:
        with patch(
            "backend.app.spotify_current_playback.get_spotify_auth_record",
            return_value={"spotify_user_id": "sp-1", "scopes": "user-read-playback-state"},
        ), patch(
            "backend.app.spotify_current_playback.refresh_access_token_if_needed",
            return_value={"access_token": "token"},
        ), patch(
            "backend.app.spotify_current_playback.fetch_spotify_current_playback_state",
            return_value=None,
        ):
            result = asyncio.run(get_current_playback_for_user("user-1"))

        self.assertEqual("ok", result["status"])
        self.assertTrue(result["scope_ok"])
        self.assertFalse(result["has_playback"])
        self.assertIsNone(result["snapshot"])

    def test_playback_snapshot_is_normalized(self) -> None:
        payload = {
            "is_playing": True,
            "progress_ms": 12345,
            "timestamp": 1713410000000,
            "currently_playing_type": "track",
            "device": {"name": "Kitchen speaker", "type": "Speaker"},
            "item": {
                "id": "track-123",
                "name": "Song A",
                "duration_ms": 210000,
                "type": "track",
                "album": {"name": "Album A"},
                "artists": [{"name": "Artist A"}, {"name": "Artist B"}],
            },
        }
        with patch(
            "backend.app.spotify_current_playback.get_spotify_auth_record",
            return_value={"spotify_user_id": "sp-1", "scopes": "user-read-playback-state"},
        ), patch(
            "backend.app.spotify_current_playback.refresh_access_token_if_needed",
            return_value={"access_token": "token"},
        ), patch(
            "backend.app.spotify_current_playback.fetch_spotify_current_playback_state",
            return_value=payload,
        ):
            result = asyncio.run(get_current_playback_for_user("user-1"))

        self.assertEqual("ok", result["status"])
        self.assertTrue(result["has_playback"])
        snapshot = result["snapshot"]
        assert snapshot is not None
        self.assertEqual("track", snapshot["item_type"])
        self.assertEqual("track-123", snapshot["item_id"])
        self.assertEqual("Song A", snapshot["name"])
        self.assertEqual(["Artist A", "Artist B"], snapshot["artist_names"])
        self.assertEqual("Album A", snapshot["album_name"])
        self.assertEqual(12345, snapshot["progress_ms"])
        self.assertEqual(210000, snapshot["duration_ms"])
        self.assertTrue(snapshot["is_playing"])
        self.assertEqual("Kitchen speaker", snapshot["device_name"])
        self.assertEqual("Speaker", snapshot["device_type"])
        self.assertEqual(1713410000000, snapshot["timestamp"])

    def test_playback_401_marks_reauth(self) -> None:
        with patch(
            "backend.app.spotify_current_playback.get_spotify_auth_record",
            return_value={"spotify_user_id": "sp-1", "scopes": "user-read-playback-state"},
        ), patch(
            "backend.app.spotify_current_playback.refresh_access_token_if_needed",
            return_value={"access_token": "token"},
        ), patch(
            "backend.app.spotify_current_playback.fetch_spotify_current_playback_state",
            side_effect=RuntimeError("Spotify access token is no longer valid."),
        ), patch("backend.app.spotify_current_playback.mark_spotify_reauth_required") as mark_mock:
            result = asyncio.run(get_current_playback_for_user("user-1"))

        self.assertEqual("failed", result["status"])
        self.assertEqual("playback_fetch", result["error_type"])
        self.assertTrue(result["reauth_marked"])
        mark_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
