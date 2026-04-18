from __future__ import annotations

import os
import unittest
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock, patch

from cryptography.fernet import Fernet

from backend.app.db import apply_pending_migrations, ensure_sqlite_db
from backend.app.spotify_token_store import (
    SpotifyTokenStoreError,
    get_spotify_tokens,
    refresh_access_token_if_needed,
    upsert_spotify_tokens,
)


class SpotifyTokenStoreRefreshTests(unittest.TestCase):
    def setUp(self) -> None:
        self.db_path = os.path.join(
            os.getcwd(),
            "backend",
            "tests",
            "_tmp_token_store.sqlite3",
        )
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        os.environ["SQLITE_DB_PATH"] = self.db_path
        os.environ["LISTENLAB_TOKEN_ENCRYPTION_KEY"] = Fernet.generate_key().decode("utf-8")
        os.environ["SPOTIFY_CLIENT_ID"] = "test_client_id"
        os.environ["SPOTIFY_REDIRECT_URI"] = "http://127.0.0.1:8000/auth/callback"
        ensure_sqlite_db()
        apply_pending_migrations()

    def tearDown(self) -> None:
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def _insert_token_row(
        self,
        *,
        access_token: str = "access_old",
        refresh_token: str = "refresh_old",
        expires_delta_seconds: int = 3600,
        scopes: str = "user-read-recently-played",
    ) -> None:
        expires_at = (
            datetime.now(UTC) + timedelta(seconds=expires_delta_seconds)
        ).isoformat().replace("+00:00", "Z")
        upsert_spotify_tokens(
            user_id="user-1",
            spotify_user_id="spotify-user-1",
            access_token=access_token,
            refresh_token=refresh_token,
            expires_at=expires_at,
            scopes=scopes,
        )

    @patch("backend.app.spotify_token_store.httpx.post")
    def test_still_valid_token_skips_refresh(self, mock_post: Mock) -> None:
        self._insert_token_row(expires_delta_seconds=7200)

        row = refresh_access_token_if_needed("user-1")

        self.assertEqual("access_old", row["access_token"])
        self.assertEqual("refresh_old", row["refresh_token"])
        mock_post.assert_not_called()

    @patch("backend.app.spotify_token_store.httpx.post")
    def test_expired_token_refresh_success_updates_access_token(self, mock_post: Mock) -> None:
        self._insert_token_row(expires_delta_seconds=-10)
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "access_new",
            "expires_in": 3600,
            "scope": "user-read-recently-played",
        }
        mock_post.return_value = mock_response

        row = refresh_access_token_if_needed("user-1")

        self.assertEqual("access_new", row["access_token"])
        self.assertEqual("refresh_old", row["refresh_token"])
        kwargs = mock_post.call_args.kwargs
        self.assertEqual("refresh_token", kwargs["data"]["grant_type"])
        self.assertEqual("refresh_old", kwargs["data"]["refresh_token"])
        self.assertEqual("test_client_id", kwargs["data"]["client_id"])

    @patch("backend.app.spotify_token_store.httpx.post")
    def test_refresh_success_rotates_refresh_token_when_returned(self, mock_post: Mock) -> None:
        self._insert_token_row(expires_delta_seconds=-10)
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "access_newer",
            "refresh_token": "refresh_rotated",
            "expires_in": 1200,
            "scope": "user-read-recently-played",
        }
        mock_post.return_value = mock_response

        row = refresh_access_token_if_needed("user-1")

        self.assertEqual("access_newer", row["access_token"])
        self.assertEqual("refresh_rotated", row["refresh_token"])

    @patch("backend.app.spotify_token_store.httpx.post")
    def test_permanent_auth_failure_marks_reauth_required(self, mock_post: Mock) -> None:
        self._insert_token_row(expires_delta_seconds=-10)
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.json.return_value = {"error": "invalid_grant"}
        mock_post.return_value = mock_response

        with self.assertRaises(SpotifyTokenStoreError) as exc:
            refresh_access_token_if_needed("user-1")

        self.assertTrue(exc.exception.permanent)
        row = get_spotify_tokens("user-1")
        assert row is not None
        self.assertTrue(row["reauth_required"])
        self.assertIn("Spotify refresh failed", str(row["reauth_reason"]))
        self.assertEqual("access_old", row["access_token"])
        self.assertEqual("refresh_old", row["refresh_token"])

    @patch("backend.app.spotify_token_store.httpx.post")
    def test_transient_failure_does_not_destroy_tokens_or_mark_reauth(self, mock_post: Mock) -> None:
        self._insert_token_row(expires_delta_seconds=-10)
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.json.return_value = {"error": "server_error"}
        mock_post.return_value = mock_response

        with self.assertRaises(SpotifyTokenStoreError) as exc:
            refresh_access_token_if_needed("user-1")

        self.assertFalse(exc.exception.permanent)
        row = get_spotify_tokens("user-1")
        assert row is not None
        self.assertFalse(row["reauth_required"])
        self.assertEqual("access_old", row["access_token"])
        self.assertEqual("refresh_old", row["refresh_token"])


if __name__ == "__main__":
    unittest.main()
