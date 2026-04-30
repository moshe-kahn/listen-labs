from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from typing import Any, Callable
from unittest.mock import patch

from backend.app.db import apply_pending_migrations, ensure_sqlite_db, insert_raw_play_event
from backend.app.main import app
from fastapi import HTTPException
from fastapi.testclient import TestClient
from backend.app.spotify_catalog_backfill import (
    enqueue_spotify_catalog_backfill_items,
    dry_run_release_album_merge,
    list_spotify_catalog_backfill_queue,
    preview_release_album_merge,
    repair_spotify_catalog_backfill_queue_statuses,
    search_album_catalog_duplicate_by_name_identities,
    search_album_catalog_duplicate_spotify_identities,
    search_album_catalog_lookup,
    search_track_catalog_duplicate_spotify_identities,
    search_track_catalog_lookup,
    run_spotify_catalog_backfill,
)


def _track_payload(track_id: str, album_id: str) -> dict[str, Any]:
    return {
        "id": track_id,
        "name": f"Track {track_id}",
        "duration_ms": 123000,
        "explicit": False,
        "disc_number": 1,
        "track_number": 1,
        "album": {"id": album_id},
        "artists": [{"id": "artist-1", "name": "Artist 1"}],
    }


def _album_payload(album_id: str) -> dict[str, Any]:
    return {
        "id": album_id,
        "name": f"Album {album_id}",
        "album_type": "album",
        "release_date": "2024-01-01",
        "release_date_precision": "day",
        "total_tracks": 2,
        "artists": [{"id": "artist-1", "name": "Artist 1"}],
        "images": [{"url": "https://image.test/1.jpg"}],
    }


class SpotifyCatalogBackfillTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self._tmp_dir.name) / "spotify_catalog_backfill.sqlite3"
        os.environ["SQLITE_DB_PATH"] = str(self.db_path)
        ensure_sqlite_db()
        apply_pending_migrations()

    def tearDown(self) -> None:
        self._tmp_dir.cleanup()

    def _seed_source_tracks(self, track_ids: list[str]) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            for track_id in track_ids:
                connection.execute(
                    """
                    INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    ("spotify", track_id, f"spotify:track:{track_id}", f"Track {track_id}", "{}"),
                )
            connection.commit()

    @staticmethod
    def _analysis_track_map_digest(connection: sqlite3.Connection) -> tuple[int, str]:
        rows = connection.execute(
            """
            SELECT
              release_track_id,
              analysis_track_id,
              match_method,
              confidence,
              status,
              is_user_confirmed,
              explanation
            FROM analysis_track_map
            ORDER BY release_track_id, analysis_track_id
            """
        ).fetchall()
        encoded = json.dumps([tuple(row) for row in rows], ensure_ascii=True, separators=(",", ":")).encode("utf-8")
        return len(rows), hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _identity_album_digest(connection: sqlite3.Connection) -> str:
        payload = {}
        for table in ("release_album", "source_album_map", "album_artist", "album_track", "analysis_track_map"):
            rows = connection.execute(f"SELECT * FROM {table} ORDER BY id").fetchall()
            payload[table] = [tuple(row) for row in rows]
        encoded = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), default=str).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def test_migration_creates_catalog_tables(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            rows = connection.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type='table'
                AND name IN (
                  'spotify_track_catalog',
                  'spotify_album_catalog',
                  'spotify_album_track',
                  'spotify_catalog_backfill_run'
                )
                ORDER BY name
                """
            ).fetchall()
        self.assertEqual(
            [
                ("spotify_album_catalog",),
                ("spotify_album_track",),
                ("spotify_catalog_backfill_run",),
                ("spotify_track_catalog",),
            ],
            rows,
        )

    def test_track_album_album_tracks_upsert(self) -> None:
        self._seed_source_tracks(["t1", "t2"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            self.assertEqual("token", access_token)
            if url.endswith("/v1/tracks"):
                ids = str(params.get("ids") or "").split(",")
                self.assertLessEqual(len(ids), 50)
                self.assertEqual("US", params.get("market"))
                self.assertTrue(params.get("ids"))
                return 200, {}, {"tracks": [_track_payload(track_id, f"a{track_id}") for track_id in ids]}, None
            if url.endswith("/v1/albums"):
                ids = str(params.get("ids") or "").split(",")
                return 200, {}, {"albums": [_album_payload(album_id) for album_id in ids]}, None
            if "/v1/albums/" in url and url.endswith("/tracks"):
                album_id = url.split("/v1/albums/")[1].split("/tracks")[0]
                return 200, {}, {"items": [_track_payload(f"{album_id}-x", album_id), _track_payload(f"{album_id}-y", album_id)], "next": None}, None
            raise AssertionError(f"Unexpected URL {url}")

        result = run_spotify_catalog_backfill(
            access_token="token",
            limit=200,
            offset=0,
            market="US",
            include_albums=True,
            request_delay_seconds=0.20,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(2, result["tracks_upserted"])
        self.assertEqual(2, result["albums_fetched"])
        self.assertEqual(4, result["album_tracks_upserted"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            track_row = connection.execute(
                "SELECT duration_ms, album_id, market, last_status, last_error FROM spotify_track_catalog WHERE spotify_track_id = ?",
                ("t1",),
            ).fetchone()
            album_row = connection.execute(
                "SELECT name, album_type, market, last_status FROM spotify_album_catalog WHERE spotify_album_id = ?",
                ("at1",),
            ).fetchone()
            album_track_count = int(connection.execute("SELECT count(*) FROM spotify_album_track").fetchone()[0])
        self.assertEqual((123000, "at1", "US", "ok", None), track_row)
        self.assertEqual(("Album at1", "album", "US", "ok"), album_row)
        self.assertEqual(4, album_track_count)

    def test_album_tracklist_policy_none_fetches_no_album_tracks(self) -> None:
        self._seed_source_tracks(["t1"])
        album_tracks_calls = 0

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            nonlocal album_tracks_calls
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums"):
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if "/v1/albums/" in url and url.endswith("/tracks"):
                album_tracks_calls += 1
                return 200, {}, {"items": [_track_payload("a1-t1", "a1")], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            album_tracklist_policy="none",
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(0, album_tracks_calls)
        self.assertEqual(0, result["album_tracks_upserted"])
        self.assertEqual(1, result["album_tracklists_seen"])
        self.assertEqual(1, result["album_tracklists_skipped_by_policy"])
        self.assertEqual(0, result["album_tracklists_fetched"])

    def test_album_tracklist_policy_priority_only_fetches_queued_albums(self) -> None:
        self._seed_source_tracks(["t1", "t2"])
        enqueue_spotify_catalog_backfill_items(
            items=[{"entity_type": "album", "spotify_id": "a1", "reason": "test-priority", "priority": 80}]
        )
        fetched_album_track_ids: list[str] = []

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                payloads = {
                    "t1": _track_payload("t1", "a1"),
                    "t2": _track_payload("t2", "a2"),
                }
                return 200, {}, {"tracks": [payloads[track_id] for track_id in ids]}, None
            if url.endswith("/v1/albums"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                return 200, {}, {"albums": [_album_payload(album_id) for album_id in ids]}, None
            if "/v1/albums/" in url and "/tracks" not in url:
                album_id = url.split("/v1/albums/")[1]
                return 200, {}, _album_payload(album_id), None
            if "/v1/albums/" in url and url.endswith("/tracks"):
                album_id = url.split("/v1/albums/")[1].split("/tracks")[0]
                fetched_album_track_ids.append(album_id)
                return 200, {}, {"items": [_track_payload(f"{album_id}-t1", album_id)], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            album_tracklist_policy="priority_only",
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(["a1"], fetched_album_track_ids)
        self.assertEqual(2, result["album_tracklists_seen"])
        self.assertEqual(1, result["album_tracklists_skipped_by_policy"])
        self.assertEqual(1, result["album_tracklists_fetched"])

    def test_album_tracklist_policy_relevant_albums_fetches_high_relevance_album(self) -> None:
        self._seed_source_tracks(["t1"])
        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="rel-high-1",
            played_at="2026-04-28T12:00:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json="{}",
            spotify_track_id="play-track-1",
            spotify_album_id="a1",
        )
        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="rel-high-2",
            played_at="2026-04-28T12:01:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json="{}",
            spotify_track_id="play-track-2",
            spotify_album_id="a1",
        )
        fetched_album_track_ids: list[str] = []

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums"):
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if "/v1/albums/" in url and url.endswith("/tracks"):
                album_id = url.split("/v1/albums/")[1].split("/tracks")[0]
                fetched_album_track_ids.append(album_id)
                return 200, {}, {"items": [_track_payload("a1-t1", "a1")], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            album_tracklist_policy="relevant_albums",
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(["a1"], fetched_album_track_ids)
        self.assertEqual(1, result["album_tracklists_fetched"])
        self.assertEqual(0, result["album_tracklists_skipped_by_policy"])

    def test_album_tracklist_policy_relevant_albums_skips_low_relevance_album(self) -> None:
        self._seed_source_tracks(["t1"])
        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="rel-low-1",
            played_at="2026-04-28T12:02:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json="{}",
            spotify_track_id="play-track-1",
            spotify_album_id="a1",
        )
        album_tracks_calls = 0

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            nonlocal album_tracks_calls
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums"):
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if "/v1/albums/" in url and url.endswith("/tracks"):
                album_tracks_calls += 1
                return 200, {}, {"items": [_track_payload("a1-t1", "a1")], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            album_tracklist_policy="relevant_albums",
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(0, album_tracks_calls)
        self.assertEqual(1, result["album_tracklists_seen"])
        self.assertEqual(1, result["album_tracklists_skipped_by_policy"])
        self.assertEqual(0, result["album_tracklists_fetched"])

    def test_album_tracklist_policy_all_preserves_tracklist_fetch_behavior(self) -> None:
        self._seed_source_tracks(["t1"])
        fetched_album_track_ids: list[str] = []

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums"):
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if "/v1/albums/" in url and url.endswith("/tracks"):
                album_id = url.split("/v1/albums/")[1].split("/tracks")[0]
                fetched_album_track_ids.append(album_id)
                return 200, {}, {"items": [_track_payload("a1-t1", "a1")], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            album_tracklist_policy="all",
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(["a1"], fetched_album_track_ids)
        self.assertEqual(1, result["album_tracklists_seen"])
        self.assertEqual(0, result["album_tracklists_skipped_by_policy"])
        self.assertEqual(1, result["album_tracklists_fetched"])

    def test_enqueue_skips_already_complete_catalog_rows(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, duration_ms, album_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("t-complete", 123000, "a1", "US", "2026-04-27T10:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a-complete", 1, "US", "2026-04-27T10:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a-complete", "t-a-complete", "US", "2026-04-27T10:00:00Z", "ok", None),
            )
            connection.commit()

        result = enqueue_spotify_catalog_backfill_items(
            items=[
                {"entity_type": "track", "spotify_id": "t-complete", "reason": "ui", "priority": 10},
                {"entity_type": "album", "spotify_id": "a-complete", "reason": "ui", "priority": 10},
            ]
        )
        self.assertTrue(result["ok"])
        self.assertEqual(2, result["received"])
        self.assertEqual(2, result["already_complete"])
        self.assertEqual(0, result["enqueued"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            queue_count = int(connection.execute("SELECT count(*) FROM spotify_catalog_backfill_queue").fetchone()[0])
        self.assertEqual(0, queue_count)

    def test_enqueue_dedupes_input(self) -> None:
        result = enqueue_spotify_catalog_backfill_items(
            items=[
                {"entity_type": "track", "spotify_id": "t1", "reason": "first", "priority": 10},
                {"entity_type": "track", "spotify_id": "t1", "reason": "second", "priority": 30},
            ]
        )
        self.assertTrue(result["ok"])
        self.assertEqual(2, result["received"])
        self.assertEqual(1, result["enqueued"])
        self.assertEqual(0, result["invalid"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT entity_type, spotify_id, priority, reason FROM spotify_catalog_backfill_queue"
            ).fetchone()
        self.assertEqual(("track", "t1", 30, "first | second"), row)

    def test_existing_queued_item_priority_increases(self) -> None:
        enqueue_spotify_catalog_backfill_items(
            items=[{"entity_type": "track", "spotify_id": "t1", "reason": "seed", "priority": 20}]
        )
        result = enqueue_spotify_catalog_backfill_items(
            items=[{"entity_type": "track", "spotify_id": "t1", "reason": "urgent", "priority": 80}]
        )
        self.assertTrue(result["ok"])
        self.assertEqual(1, result["updated"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT priority, reason, status FROM spotify_catalog_backfill_queue WHERE entity_type = 'track' AND spotify_id = 't1'"
            ).fetchone()
        self.assertEqual(80, int(row[0]))
        self.assertIn("seed", str(row[1]))
        self.assertIn("urgent", str(row[1]))
        self.assertEqual("pending", str(row[2]))

    def test_track_seed_prefers_most_listened_spotify_id_per_release_track(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Representative Track", "representative track"),
                ).lastrowid
            )
            source_track_low_id = int(
                connection.execute(
                    """
                    INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    ("spotify", "t-low", "spotify:track:t-low", "Track low", "{}"),
                ).lastrowid
            )
            source_track_high_id = int(
                connection.execute(
                    """
                    INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    ("spotify", "t-high", "spotify:track:t-high", "Track high", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_track_map (
                  source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_track_low_id, release_track_id),
            )
            connection.execute(
                """
                INSERT INTO source_track_map (
                  source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_track_high_id, release_track_id),
            )
            connection.commit()

        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="listen-low-1",
            played_at="2026-04-20T12:00:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json="{}",
            spotify_track_id="t-low",
            spotify_track_uri="spotify:track:t-low",
        )
        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="listen-high-1",
            played_at="2026-04-20T12:01:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json="{}",
            spotify_track_id="t-high",
            spotify_track_uri="spotify:track:t-high",
        )
        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="listen-high-2",
            played_at="2026-04-20T12:02:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json="{}",
            spotify_track_id="t-high",
            spotify_track_uri="spotify:track:t-high",
        )

        captured_track_ids: list[str] = []

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                captured_track_ids.extend(ids)
                return 200, {}, {"tracks": [_track_payload(track_id, "album-x") for track_id in ids]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(["t-high"], captured_track_ids)
        self.assertEqual(1, result["tracks_seen"])

    def test_album_fetch_prefers_most_listened_spotify_id_per_release_album(self) -> None:
        self._seed_source_tracks(["t1", "t2"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Representative Album", "representative album"),
                ).lastrowid
            )
            source_album_low_id = int(
                connection.execute(
                    """
                    INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    ("spotify", "a-low", "spotify:album:a-low", "Album low", "{}"),
                ).lastrowid
            )
            source_album_high_id = int(
                connection.execute(
                    """
                    INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    ("spotify", "a-high", "spotify:album:a-high", "Album high", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_album_map (
                  source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_album_low_id, release_album_id),
            )
            connection.execute(
                """
                INSERT INTO source_album_map (
                  source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_album_high_id, release_album_id),
            )
            connection.commit()

        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="album-low-1",
            played_at="2026-04-20T13:00:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json="{}",
            spotify_album_id="a-low",
        )
        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="album-high-1",
            played_at="2026-04-20T13:01:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json="{}",
            spotify_album_id="a-high",
        )
        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="album-high-2",
            played_at="2026-04-20T13:02:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json="{}",
            spotify_album_id="a-high",
        )

        captured_album_batches: list[list[str]] = []

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                payload_by_track = {
                    "t1": _track_payload("t1", "a-low"),
                    "t2": _track_payload("t2", "a-high"),
                }
                return 200, {}, {"tracks": [payload_by_track[track_id] for track_id in ids]}, None
            if url.endswith("/v1/albums"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                captured_album_batches.append(ids)
                return 200, {}, {"albums": [_album_payload(album_id) for album_id in ids]}, None
            if "/v1/albums/" in url and url.endswith("/tracks"):
                album_id = url.split("/v1/albums/")[1].split("/tracks")[0]
                return 200, {}, {"items": [_track_payload(f"{album_id}-x", album_id)], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(1, result["albums_seen"])
        self.assertEqual([["a-high"]], captured_album_batches)

    def test_idempotent_rerun_no_duplicates(self) -> None:
        self._seed_source_tracks(["t1"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums"):
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if url.endswith("/v1/albums/a1/tracks"):
                return 200, {}, {"items": [_track_payload("a1-x", "a1")], "next": None}, None
            raise AssertionError(url)

        run_spotify_catalog_backfill(access_token="token", include_albums=True, sleeper=lambda _: None, fetcher=fetcher)
        run_spotify_catalog_backfill(access_token="token", include_albums=True, sleeper=lambda _: None, fetcher=fetcher)

        with closing(sqlite3.connect(self.db_path)) as connection:
            self.assertEqual(1, int(connection.execute("SELECT count(*) FROM spotify_track_catalog").fetchone()[0]))
            self.assertEqual(1, int(connection.execute("SELECT count(*) FROM spotify_album_catalog").fetchone()[0]))
            self.assertEqual(1, int(connection.execute("SELECT count(*) FROM spotify_album_track").fetchone()[0]))

    def test_existing_complete_track_row_is_skipped_no_track_request(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, duration_ms, album_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("t1", 123000, "a1", "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            raise AssertionError(f"Unexpected Spotify request: {url}")

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(0, result["requests_total"])
        self.assertEqual(0, result["tracks_fetched"])
        self.assertEqual(1, result["skipped"])

    def test_existing_incomplete_track_row_is_fetched(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, duration_ms, album_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("t1", None, "a1", "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(1, result["requests_total"])
        self.assertEqual(1, result["tracks_fetched"])
        self.assertEqual(0, result["skipped"])

    def test_existing_error_track_row_is_fetched(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, duration_ms, album_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("t1", 123000, "a1", "US", "2026-04-25T00:00:00Z", "error", "prior error"),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(1, result["requests_total"])
        self.assertEqual(1, result["tracks_fetched"])
        self.assertEqual(0, result["skipped"])

    def test_existing_complete_album_row_is_skipped(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", 2, "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-x", "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-y", "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                raise AssertionError("Album batch should be skipped for complete catalog row")
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(1, result["albums_seen"])
        self.assertEqual(0, result["albums_fetched"])
        self.assertEqual(1, result["skipped"])
        self.assertEqual(1, result["requests_total"])

    def test_existing_complete_album_tracklist_is_skipped(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-x", "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-y", "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if url.endswith("/v1/albums/a1/tracks"):
                raise AssertionError("Album tracks should be skipped when complete tracklist exists")
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(1, result["albums_fetched"])
        self.assertEqual(0, result["album_tracks_upserted"])
        self.assertEqual(1, result["skipped"])
        self.assertEqual(2, result["requests_total"])

    def test_complete_album_metadata_with_incomplete_tracklist_triggers_track_fetch(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", 2, "US", "2026-04-27T00:00:00Z", "ok", None),
            )
            connection.commit()

        called_album_tracks = {"value": False}

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                raise AssertionError("Album metadata should be skipped when already complete")
            if url.endswith("/v1/albums/a1/tracks"):
                called_album_tracks["value"] = True
                return 200, {}, {"items": [_track_payload("a1-x", "a1"), _track_payload("a1-y", "a1")], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertTrue(called_album_tracks["value"])
        self.assertEqual(2, result["album_tracks_upserted"])

    def test_complete_album_metadata_with_complete_tracklist_skips_track_fetch(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", 2, "US", "2026-04-27T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-x", "US", "2026-04-27T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-y", "US", "2026-04-27T00:00:00Z", "ok", None),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums/a1/tracks"):
                raise AssertionError("Album tracks should be skipped for complete tracklist")
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                raise AssertionError("Album metadata should be skipped when already complete")
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(0, result["album_tracks_upserted"])

    def test_album_track_error_row_triggers_tracklist_refetch(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", 2, "US", "2026-04-27T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-x", "US", "2026-04-27T00:00:00Z", "error", "prior"),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-y", "US", "2026-04-27T00:00:00Z", "ok", None),
            )
            connection.commit()

        called_album_tracks = {"value": False}

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums/a1/tracks"):
                called_album_tracks["value"] = True
                return 200, {}, {"items": [_track_payload("a1-x", "a1"), _track_payload("a1-y", "a1")], "next": None}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                raise AssertionError("Album metadata should be skipped when already complete")
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertTrue(called_album_tracks["value"])
        self.assertEqual(2, result["album_tracks_upserted"])

    def test_partial_prior_album_tracklist_resumes_without_force_refresh(self) -> None:
        self._seed_source_tracks(["t1"])

        def first_fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if url.endswith("/v1/albums/a1/tracks"):
                return 200, {}, {"items": [_track_payload("a1-x", "a1")], "next": "https://api.spotify.com/v1/albums/a1/tracks?offset=50"}, None
            raise AssertionError(url)

        first_result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            max_album_tracks_pages_per_album=1,
            sleeper=lambda _: None,
            fetcher=first_fetcher,
        )
        self.assertEqual("ok", first_result["status"])
        self.assertEqual(1, first_result["album_tracklists_capped"])

        album_batch_called = {"value": False}
        second_page_called = {"value": False}

        def second_fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                album_batch_called["value"] = True
                raise AssertionError("Second run should resume without album metadata fetch")
            if url.endswith("/v1/albums/a1/tracks"):
                return 200, {}, {"items": [_track_payload("a1-x", "a1")], "next": "https://api.spotify.com/v1/albums/a1/tracks?offset=50"}, None
            if "/v1/albums/a1/tracks?offset=50" in url:
                second_page_called["value"] = True
                return 200, {}, {"items": [_track_payload("a1-y", "a1")], "next": None}, None
            raise AssertionError(url)

        second_result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=second_fetcher,
        )
        self.assertEqual("ok", second_result["status"])
        self.assertFalse(album_batch_called["value"])
        self.assertTrue(second_page_called["value"])

    def test_album_track_resume_uses_existing_count_offset(self) -> None:
        self._seed_source_tracks(["t1"])

        first_page_requested = {"value": False}

        def first_fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if url.endswith("/v1/albums/a1/tracks"):
                first_page_requested["value"] = True
                self.assertEqual(0, int(params.get("offset", 0)))
                return 200, {}, {"items": [_track_payload("a1-x", "a1")], "next": "https://api.spotify.com/v1/albums/a1/tracks?offset=50"}, None
            raise AssertionError(url)

        first_result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            max_album_tracks_pages_per_album=1,
            sleeper=lambda _: None,
            fetcher=first_fetcher,
        )
        self.assertEqual("ok", first_result["status"])
        self.assertTrue(first_page_requested["value"])

        second_run_first_offset = {"value": None}

        def second_fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                raise AssertionError("Album metadata should be skipped on resume")
            if url.endswith("/v1/albums/a1/tracks"):
                second_run_first_offset["value"] = int(params.get("offset", 0))
                self.assertEqual(1, second_run_first_offset["value"])
                return 200, {}, {"items": [_track_payload("a1-y", "a1")], "next": None}, None
            raise AssertionError(url)

        second_result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=second_fetcher,
        )
        self.assertEqual("ok", second_result["status"])
        self.assertEqual(1, second_run_first_offset["value"])

    def test_album_track_resume_force_refresh_starts_at_zero(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", 2, "US", "2026-04-27T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-x", "US", "2026-04-27T00:00:00Z", "ok", None),
            )
            connection.commit()

        requested_offset = {"value": None}

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if url.endswith("/v1/albums/a1/tracks"):
                requested_offset["value"] = int(params.get("offset", 0))
                return 200, {}, {"items": [_track_payload("a1-x", "a1"), _track_payload("a1-y", "a1")], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            force_refresh=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(0, requested_offset["value"])

    def test_album_track_resume_error_row_restarts_at_zero(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", 2, "US", "2026-04-27T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-x", "US", "2026-04-27T00:00:00Z", "error", "prior"),
            )
            connection.commit()

        requested_offset = {"value": None}

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                raise AssertionError("Album metadata should be skipped when already complete")
            if url.endswith("/v1/albums/a1/tracks"):
                requested_offset["value"] = int(params.get("offset", 0))
                return 200, {}, {"items": [_track_payload("a1-x", "a1"), _track_payload("a1-y", "a1")], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(0, requested_offset["value"])

    def test_capped_album_eventually_completes_over_multiple_runs(self) -> None:
        self._seed_source_tracks(["t1"])

        def make_fetcher() -> Callable[[str, dict[str, Any], str], tuple[int, dict[str, str], dict[str, Any], str | None]]:
            def fetcher(
                url: str, params: dict[str, Any], access_token: str
            ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
                if url.endswith("/v1/tracks"):
                    return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
                if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                    payload = _album_payload("a1")
                    payload["total_tracks"] = 3
                    return 200, {}, {"albums": [payload]}, None
                if url.endswith("/v1/albums/a1/tracks"):
                    offset = int(params.get("offset", 0))
                    if offset == 0:
                        return 200, {}, {"items": [_track_payload("a1-x", "a1")], "next": "https://api.spotify.com/v1/albums/a1/tracks?offset=50"}, None
                    if offset == 1:
                        return 200, {}, {"items": [_track_payload("a1-y", "a1")], "next": "https://api.spotify.com/v1/albums/a1/tracks?offset=50"}, None
                    if offset == 2:
                        return 200, {}, {"items": [_track_payload("a1-z", "a1")], "next": None}, None
                    raise AssertionError(f"unexpected offset {offset}")
                raise AssertionError(url)
            return fetcher

        first = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            max_album_tracks_pages_per_album=1,
            sleeper=lambda _: None,
            fetcher=make_fetcher(),
        )
        second = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            max_album_tracks_pages_per_album=1,
            sleeper=lambda _: None,
            fetcher=make_fetcher(),
        )
        third = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            max_album_tracks_pages_per_album=1,
            sleeper=lambda _: None,
            fetcher=make_fetcher(),
        )
        self.assertEqual("ok", first["status"])
        self.assertEqual("ok", second["status"])
        self.assertEqual("ok", third["status"])
        self.assertEqual(1, first["album_tracklists_capped"])
        self.assertEqual(1, second["album_tracklists_capped"])
        self.assertEqual(0, third["album_tracklists_capped"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            final_count = int(connection.execute("SELECT count(*) FROM spotify_album_track WHERE spotify_album_id = ?", ("a1",)).fetchone()[0])
        self.assertEqual(3, final_count)

    def test_album_track_resume_does_not_mutate_analysis_track_map(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(connection.execute("INSERT INTO analysis_track (primary_name) VALUES (?)", ("Track A",)).lastrowid)
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id, analysis_track_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if url.endswith("/v1/albums/a1/tracks"):
                return 200, {}, {"items": [_track_payload("a1-x", "a1")], "next": "https://api.spotify.com/v1/albums/a1/tracks?offset=50"}, None
            raise AssertionError(url)

        _ = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            max_album_tracks_pages_per_album=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_force_refresh_still_fetches_album_tracklist(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", 1, "US", "2026-04-27T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-x", "US", "2026-04-27T00:00:00Z", "ok", None),
            )
            connection.commit()

        called_album_batch = {"value": False}
        called_album_tracks = {"value": False}

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                called_album_batch["value"] = True
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if url.endswith("/v1/albums/a1/tracks"):
                called_album_tracks["value"] = True
                return 200, {}, {"items": [_track_payload("a1-x", "a1")], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            force_refresh=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertTrue(called_album_batch["value"])
        self.assertTrue(called_album_tracks["value"])

    def test_force_refresh_fetches_despite_existing_rows(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, duration_ms, album_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("t1", 123000, "a1", "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            force_refresh=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(1, result["requests_total"])
        self.assertEqual(1, result["tracks_fetched"])
        self.assertEqual(0, result["skipped"])

    def test_skip_existing_increments_skipped_counter(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, duration_ms, album_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("t1", 123000, "a1", "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", 2, "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-x", "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-y", "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            raise AssertionError(f"Unexpected Spotify request: {url}")

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(0, result["requests_total"])
        self.assertEqual(2, result["skipped"])

    def test_skip_existing_does_not_mutate_analysis_track_map(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track A",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, duration_ms, album_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("t1", 123000, "a1", "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            sleeper=lambda _: None,
            fetcher=lambda *_: (_ for _ in ()).throw(AssertionError("No Spotify request expected")),
        )
        self.assertEqual("ok", result["status"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_bounded_run_returns_has_more(self) -> None:
        self._seed_source_tracks(["t1", "t2", "t3"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                ids = str(params.get("ids") or "").split(",")
                return 200, {}, {"tracks": [_track_payload(track_id, f"a{track_id}") for track_id in ids]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            limit=2,
            include_albums=False,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertTrue(result["has_more"])
        self.assertEqual(2, result["tracks_seen"])

    def test_runner_processes_queued_item_before_bulk_backlog(self) -> None:
        self._seed_source_tracks(["t-bulk"])
        enqueue_spotify_catalog_backfill_items(
            items=[{"entity_type": "track", "spotify_id": "t-queue", "reason": "visible", "priority": 80}]
        )
        call_urls: list[str] = []

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            call_urls.append(url)
            if url.endswith("/v1/tracks/t-queue"):
                return 200, {}, _track_payload("t-queue", "a-q"), None
            if url.endswith("/v1/tracks"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                self.assertIn("t-bulk", ids)
                return 200, {}, {"tracks": [_track_payload(track_id, "a-b") for track_id in ids]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            limit=2,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertGreaterEqual(len(call_urls), 2)
        self.assertTrue(call_urls[0].endswith("/v1/tracks/t-queue"))

    def test_queue_item_marked_done_on_successful_fetch(self) -> None:
        enqueue_spotify_catalog_backfill_items(
            items=[{"entity_type": "track", "spotify_id": "t1", "reason": "visible", "priority": 80}]
        )

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks/t1"):
                return 200, {}, _track_payload("t1", "a1"), None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            limit=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT status, attempts, last_error FROM spotify_catalog_backfill_queue WHERE entity_type = 'track' AND spotify_id = 't1'"
            ).fetchone()
        self.assertEqual("done", str(row[0]))
        self.assertEqual(0, int(row[1] or 0))
        self.assertIsNone(row[2])

    def test_queue_item_marked_error_on_failure(self) -> None:
        enqueue_spotify_catalog_backfill_items(
            items=[{"entity_type": "track", "spotify_id": "t1", "reason": "visible", "priority": 80}]
        )

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks/t1"):
                return 500, {}, {"error": {"status": 500, "message": "boom"}}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            limit=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertIn(result["status"], {"ok", "partial"})
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT status, attempts, last_error FROM spotify_catalog_backfill_queue WHERE entity_type = 'track' AND spotify_id = 't1'"
            ).fetchone()
        self.assertEqual("error", str(row[0]))
        self.assertEqual(1, int(row[1] or 0))
        self.assertIn("status 500", str(row[2]))

    def test_max_requests_stops_run_with_partial_status(self) -> None:
        self._seed_source_tracks([f"t{i}" for i in range(60)])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                ids = [item for item in str(params.get("ids") or "").split(",") if item]
                return 200, {}, {"tracks": [_track_payload(track_id, f"a{track_id}") for track_id in ids]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            limit=60,
            max_requests=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("partial", result["status"])
        self.assertTrue(result["partial"])
        self.assertEqual("max_requests", result["stop_reason"])
        self.assertGreater(result["tracks_upserted"], 0)

    def test_max_errors_stops_run_with_partial_status(self) -> None:
        self._seed_source_tracks(["t1", "t2", "t3"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks") and "/v1/tracks/" not in url:
                return 403, {}, {"error": {"status": 403, "message": "forbidden batch"}}, None
            if "/v1/tracks/" in url:
                return 403, {}, {"error": {"status": 403, "message": "forbidden single"}}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            max_errors=2,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("partial", result["status"])
        self.assertTrue(result["partial"])
        self.assertEqual("max_errors", result["stop_reason"])
        self.assertGreaterEqual(result["errors"], 2)

    def test_album_track_page_cap_stops_with_partial_status(self) -> None:
        self._seed_source_tracks(["t1"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks") and "/v1/tracks/" not in url:
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if url.endswith("/v1/albums/a1/tracks"):
                return 200, {}, {"items": [_track_payload("a1-x", "a1")], "next": "https://api.spotify.com/v1/albums/a1/tracks?offset=50"}, None
            if "/v1/albums/a1/tracks?offset=50" in url:
                raise AssertionError("Album track second page should not be requested when page cap is reached")
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            max_album_tracks_pages_per_album=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertFalse(result["partial"])
        self.assertIsNone(result["stop_reason"])
        self.assertEqual(1, result["albums_fetched"])
        self.assertEqual(1, result["album_tracks_upserted"])
        self.assertEqual(1, result["album_tracklists_capped"])
        self.assertIn("album track pagination capped for a1", result["warnings"])

    def test_album_track_page_cap_is_local_and_runner_continues_to_next_album(self) -> None:
        self._seed_source_tracks(["t1", "t2"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks") and "/v1/tracks/" not in url:
                ids = [item for item in str(params.get("ids") or "").split(",") if item]
                payloads = []
                for track_id in ids:
                    album_id = "a1" if track_id == "t1" else "a2"
                    payloads.append(_track_payload(track_id, album_id))
                return 200, {}, {"tracks": payloads}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                ids = [item for item in str(params.get("ids") or "").split(",") if item]
                return 200, {}, {"albums": [_album_payload(album_id) for album_id in ids]}, None
            if url.endswith("/v1/albums/a1/tracks"):
                return 200, {}, {"items": [_track_payload("a1-x", "a1")], "next": "https://api.spotify.com/v1/albums/a1/tracks?offset=50"}, None
            if "/v1/albums/a1/tracks?offset=50" in url:
                raise AssertionError("Capped album should not request second page")
            if url.endswith("/v1/albums/a2/tracks"):
                return 200, {}, {"items": [_track_payload("a2-x", "a2")], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            max_album_tracks_pages_per_album=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertFalse(result["partial"])
        self.assertEqual(2, result["albums_fetched"])
        self.assertEqual(2, result["album_tracks_upserted"])
        self.assertEqual(1, result["album_tracklists_capped"])
        self.assertIn("album track pagination capped for a1", result["warnings"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            a1_track_rows = int(connection.execute("SELECT count(*) FROM spotify_album_track WHERE spotify_album_id = ?", ("a1",)).fetchone()[0])
            a2_track_rows = int(connection.execute("SELECT count(*) FROM spotify_album_track WHERE spotify_album_id = ?", ("a2",)).fetchone()[0])
            a1_total_tracks = int(connection.execute("SELECT total_tracks FROM spotify_album_catalog WHERE spotify_album_id = ?", ("a1",)).fetchone()[0])
        self.assertEqual(1, a1_track_rows)
        self.assertEqual(1, a2_track_rows)
        self.assertLess(a1_track_rows, a1_total_tracks)

    def test_partial_result_persists_run_telemetry(self) -> None:
        self._seed_source_tracks([f"t{i}" for i in range(60)])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                ids = [item for item in str(params.get("ids") or "").split(",") if item]
                return 200, {}, {"tracks": [_track_payload(track_id, f"a{track_id}") for track_id in ids]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            limit=60,
            max_requests=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("partial", result["status"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT status, tracks_upserted, requests_total, last_error FROM spotify_catalog_backfill_run ORDER BY id DESC LIMIT 1"
            ).fetchone()
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual("partial", str(row[0]))
        self.assertGreater(int(row[1] or 0), 0)
        self.assertEqual(1, int(row[2] or 0))
        self.assertIn("max_requests", str(row[3] or ""))

    def test_error_status_and_last_error_stored(self) -> None:
        self._seed_source_tracks(["t1"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            return 500, {}, {"error": {"status": 500, "message": "bad request body"}}, None

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("failed", result["status"])
        self.assertGreater(result["errors"], 0)
        with closing(sqlite3.connect(self.db_path)) as connection:
            run_row = connection.execute(
                "SELECT status, last_error FROM spotify_catalog_backfill_run ORDER BY id DESC LIMIT 1"
            ).fetchone()
        self.assertEqual("failed", str(run_row[0]))
        self.assertIn("status 500", str(run_row[1]))
        self.assertIn("tracks_batch", str(run_row[1]))
        self.assertIn("bad request body", str(run_row[1]))
        self.assertNotIn("token", str(run_row[1]).lower())

    def test_429_retry_behavior_with_fake_sleeper(self) -> None:
        self._seed_source_tracks(["t1"])
        state = {"calls": 0}
        sleep_calls: list[float] = []

        def sleeper(seconds: float) -> None:
            sleep_calls.append(seconds)

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                state["calls"] += 1
                if state["calls"] == 1:
                    return 429, {"Retry-After": "1"}, {}, None
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            request_delay_seconds=0.20,
            sleeper=sleeper,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(1, result["requests_429"])
        self.assertGreaterEqual(result["last_retry_after_seconds"], 1.0)
        self.assertGreaterEqual(result["max_retry_after_seconds"], 1.0)
        self.assertTrue(any(call >= 1.25 for call in sleep_calls))

    def test_429_without_retry_after_uses_fallback_cooldown_warning(self) -> None:
        self._seed_source_tracks(["t1"])
        state = {"calls": 0}
        sleep_calls: list[float] = []

        def sleeper(seconds: float) -> None:
            sleep_calls.append(seconds)

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                state["calls"] += 1
                if state["calls"] == 1:
                    return 429, {}, {}, None
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            request_delay_seconds=0.20,
            sleeper=sleeper,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(1, result["requests_429"])
        self.assertEqual(0.0, result["last_retry_after_seconds"])
        self.assertIn("429 without valid Retry-After; used fallback cooldown", result["warnings"])
        self.assertTrue(any(call >= 5.0 for call in sleep_calls))

    def test_repeated_429_stops_partial_with_rate_limited_reason(self) -> None:
        self._seed_source_tracks(["t1"])
        sleep_calls: list[float] = []

        def sleeper(seconds: float) -> None:
            sleep_calls.append(seconds)

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 429, {"Retry-After": "2"}, {}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            max_429=2,
            request_delay_seconds=0.20,
            sleeper=sleeper,
            fetcher=fetcher,
        )
        self.assertEqual("partial", result["status"])
        self.assertTrue(result["partial"])
        self.assertEqual("rate_limited", result["stop_reason"])
        self.assertTrue(result["has_more"])
        self.assertEqual("Stopped early due to rate_limited", result["last_error"])
        self.assertEqual(2, result["requests_429"])
        self.assertGreaterEqual(result["max_retry_after_seconds"], 2.0)
        self.assertTrue(any(call >= 2.25 for call in sleep_calls))

    def test_analysis_track_map_unchanged(self) -> None:
        self._seed_source_tracks(["t1"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track A",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            raise AssertionError(url)

        run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_partial_stop_does_not_mutate_analysis_track_map(self) -> None:
        self._seed_source_tracks([f"t{i}" for i in range(60)])
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track A",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                ids = [item for item in str(params.get("ids") or "").split(",") if item]
                return 200, {}, {"tracks": [_track_payload(track_id, f"a{track_id}") for track_id in ids]}, None
            raise AssertionError(url)

        run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            limit=60,
            max_requests=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_batch_403_triggers_single_track_fallback(self) -> None:
        self._seed_source_tracks(["t1", "t2"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks") and "/v1/tracks/" not in url:
                return 403, {}, {"error": {"status": 403, "message": "forbidden batch"}}, None
            if url.endswith("/v1/tracks/t1"):
                return 200, {}, _track_payload("t1", "a1"), None
            if url.endswith("/v1/tracks/t2"):
                return 200, {}, _track_payload("t2", "a2"), None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            max_429=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertFalse(result["partial"])
        self.assertIsNone(result["stop_reason"])
        self.assertEqual(0, result["requests_429"])
        self.assertEqual(2, result["tracks_upserted"])
        self.assertEqual(3, result["requests_total"])
        self.assertIn("track batch endpoint forbidden; used single-track fallback", result["warnings"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            row_t1 = connection.execute(
                "SELECT duration_ms, album_id, last_status FROM spotify_track_catalog WHERE spotify_track_id = ?",
                ("t1",),
            ).fetchone()
            run_row = connection.execute(
                "SELECT last_error, warnings_json FROM spotify_catalog_backfill_run ORDER BY id DESC LIMIT 1"
            ).fetchone()
        self.assertEqual((123000, "a1", "ok"), row_t1)
        self.assertIsNone(run_row[0])
        self.assertIn(
            "track batch endpoint forbidden; used single-track fallback",
            json.loads(str(run_row[1] or "[]")),
        )

    def test_batch_403_fallback_single_failures_store_per_item_error(self) -> None:
        self._seed_source_tracks(["t1"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks") and "/v1/tracks/" not in url:
                return 403, {}, {"error": {"status": 403, "message": "forbidden batch"}}, None
            if url.endswith("/v1/tracks/t1"):
                return 403, {}, {"error": {"status": 403, "message": "forbidden single"}}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(0, result["tracks_fetched"])
        self.assertEqual(1, result["errors"])
        self.assertEqual(2, result["requests_total"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT last_status, last_error FROM spotify_track_catalog WHERE spotify_track_id = ?",
                ("t1",),
            ).fetchone()
        self.assertEqual("error", str(row[0]))
        self.assertIn("tracks_single_fallback", str(row[1]))

    def test_batch_403_fallback_does_not_mutate_analysis_track_map(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track A",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks") and "/v1/tracks/" not in url:
                return 403, {}, {"error": {"status": 403, "message": "forbidden batch"}}, None
            if url.endswith("/v1/tracks/t1"):
                return 200, {}, _track_payload("t1", "a1"), None
            raise AssertionError(url)

        run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_album_batch_403_triggers_single_album_fallback(self) -> None:
        self._seed_source_tracks(["t1"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks") and "/v1/tracks/" not in url:
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                return 403, {}, {"error": {"status": 403, "message": "forbidden album batch"}}, None
            if url.endswith("/v1/albums/a1"):
                return 200, {}, _album_payload("a1"), None
            if url.endswith("/v1/albums/a1/tracks"):
                return 200, {}, {"items": [_track_payload("a1-x", "a1"), _track_payload("a1-y", "a1")], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(1, result["albums_fetched"])
        self.assertEqual(2, result["album_tracks_upserted"])
        self.assertIn("album batch endpoint forbidden; used single-album fallback", result["warnings"])
        self.assertEqual(4, result["requests_total"])  # tracks batch + album batch + album single + album tracks

        with closing(sqlite3.connect(self.db_path)) as connection:
            album_row = connection.execute(
                "SELECT name, album_type, last_status FROM spotify_album_catalog WHERE spotify_album_id = ?",
                ("a1",),
            ).fetchone()
            run_row = connection.execute(
                "SELECT last_error, warnings_json FROM spotify_catalog_backfill_run ORDER BY id DESC LIMIT 1"
            ).fetchone()
        self.assertEqual(("Album a1", "album", "ok"), album_row)
        self.assertIsNone(run_row[0])
        self.assertIn(
            "album batch endpoint forbidden; used single-album fallback",
            json.loads(str(run_row[1] or "[]")),
        )

    def test_album_batch_403_single_album_failure_stores_per_album_error(self) -> None:
        self._seed_source_tracks(["t1"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks") and "/v1/tracks/" not in url:
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                return 403, {}, {"error": {"status": 403, "message": "forbidden album batch"}}, None
            if url.endswith("/v1/albums/a1"):
                return 403, {}, {"error": {"status": 403, "message": "forbidden album single"}}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(0, result["albums_fetched"])
        self.assertEqual(1, result["errors"])
        self.assertEqual(3, result["requests_total"])  # tracks batch + album batch + album single

        with closing(sqlite3.connect(self.db_path)) as connection:
            album_row = connection.execute(
                "SELECT last_status, last_error FROM spotify_album_catalog WHERE spotify_album_id = ?",
                ("a1",),
            ).fetchone()
        self.assertEqual("error", str(album_row[0]))
        self.assertIn("album_single_fallback", str(album_row[1]))

    def test_album_batch_403_fallback_does_not_mutate_analysis_track_map(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track A",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks") and "/v1/tracks/" not in url:
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                return 403, {}, {"error": {"status": 403, "message": "forbidden album batch"}}, None
            if url.endswith("/v1/albums/a1"):
                return 200, {}, _album_payload("a1"), None
            if url.endswith("/v1/albums/a1/tracks"):
                return 200, {}, {"items": [_track_payload("a1-x", "a1")], "next": None}, None
            raise AssertionError(url)

        run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_catalog_backfill_unauthenticated_returns_401_shape(self) -> None:
        with patch(
            "backend.app.main._require_local_data_session",
            side_effect=HTTPException(status_code=401, detail="Not authenticated with Spotify."),
        ), patch("backend.app.main.run_spotify_catalog_backfill") as run_mock:
            client = TestClient(app)
            response = client.post("/debug/spotify/catalog-backfill", json={"limit": 25, "market": "US", "include_albums": True})
        self.assertEqual(401, response.status_code)
        body = response.json()
        self.assertFalse(body["ok"])
        self.assertEqual("unauthenticated", body["status"])
        self.assertEqual("spotify_not_authenticated", body["error"]["code"])
        self.assertEqual("Not authenticated with Spotify.", body["error"]["message"])
        run_mock.assert_not_called()

    def test_catalog_backfill_unauthenticated_no_run_row_inserted(self) -> None:
        with patch(
            "backend.app.main._require_local_data_session",
            side_effect=HTTPException(status_code=401, detail="Not authenticated with Spotify."),
        ):
            client = TestClient(app)
            response = client.post("/debug/spotify/catalog-backfill", json={"limit": 25, "market": "US", "include_albums": True})
        self.assertEqual(401, response.status_code)
        with closing(sqlite3.connect(self.db_path)) as connection:
            run_count = int(connection.execute("SELECT count(*) FROM spotify_catalog_backfill_run").fetchone()[0])
        self.assertEqual(0, run_count)

    def test_catalog_backfill_unauthenticated_no_spotify_request_made(self) -> None:
        with patch(
            "backend.app.main._require_local_data_session",
            side_effect=HTTPException(status_code=401, detail="Not authenticated with Spotify."),
        ), patch("backend.app.main.refresh_access_token_if_needed") as refresh_mock, patch(
            "backend.app.main.run_spotify_catalog_backfill"
        ) as run_mock:
            client = TestClient(app)
            response = client.post("/debug/spotify/catalog-backfill", json={"limit": 25, "market": "US", "include_albums": True})
        self.assertEqual(401, response.status_code)
        refresh_mock.assert_not_called()
        run_mock.assert_not_called()

    def test_catalog_backfill_runs_endpoint_empty(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            response = client.get("/debug/spotify/catalog-backfill/runs")
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual([], body["items"])
        self.assertEqual(0, body["total"])

    def test_catalog_backfill_queue_endpoint_empty(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            response = client.get("/debug/spotify/catalog-backfill/queue")
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual([], body["items"])
        self.assertEqual(0, body["total"])
        self.assertEqual({"pending": 0, "done": 0, "error": 0}, body["counts"])

    def test_catalog_backfill_queue_list_filters_by_status(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("track", "t-p", "pending", 10, "pending", "2026-04-27T12:00:00Z", 0),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("album", "a-d", "done", 5, "done", "2026-04-27T12:01:00Z", 1),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("track", "t-e", "error", 8, "error", "2026-04-27T12:02:00Z", 2, "boom"),
            )
            connection.commit()

        pending_payload = list_spotify_catalog_backfill_queue(status_filter="pending", limit=50, offset=0)
        self.assertTrue(pending_payload["ok"])
        self.assertEqual(1, pending_payload["total"])
        self.assertEqual(1, len(pending_payload["items"]))
        self.assertEqual("pending", pending_payload["items"][0]["status"])

        with patch("backend.app.main._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            response = client.get("/debug/spotify/catalog-backfill/queue?status=error&limit=50&offset=0")
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual(1, body["total"])
        self.assertEqual(1, len(body["items"]))
        self.assertEqual("error", body["items"][0]["status"])

    def test_catalog_backfill_queue_counts_by_status(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts) VALUES (?, ?, ?, ?, ?, ?)",
                ("track", "t1", 1, "pending", "2026-04-27T11:00:00Z", 0),
            )
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts) VALUES (?, ?, ?, ?, ?, ?)",
                ("track", "t2", 1, "pending", "2026-04-27T11:01:00Z", 0),
            )
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts) VALUES (?, ?, ?, ?, ?, ?)",
                ("album", "a1", 1, "done", "2026-04-27T11:02:00Z", 1),
            )
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts, last_error) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("album", "a2", 1, "error", "2026-04-27T11:03:00Z", 2, "err"),
            )
            connection.commit()

        with patch("backend.app.main._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            response = client.get("/debug/spotify/catalog-backfill/queue")
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual({"pending": 2, "done": 1, "error": 1}, body["counts"])

    def test_catalog_backfill_queue_endpoint_does_not_call_spotify(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed"
        ) as refresh_mock, patch("backend.app.main.run_spotify_catalog_backfill") as run_mock:
            client = TestClient(app)
            response = client.get("/debug/spotify/catalog-backfill/queue")
        self.assertEqual(200, response.status_code)
        refresh_mock.assert_not_called()
        run_mock.assert_not_called()

    def test_queue_list_does_not_mutate_analysis_track_map(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track A",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts) VALUES (?, ?, ?, ?, ?, ?)",
                ("track", "t1", 1, "pending", "2026-04-27T11:00:00Z", 0),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        _ = list_spotify_catalog_backfill_queue(status_filter=None, limit=50, offset=0)

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_queue_album_metadata_complete_but_tracklist_incomplete_stays_pending(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-q1", 2, "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-q1", "t1", "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("album", "alb-q1", "seed", 80, "pending", "2026-04-28T00:00:00Z", 0),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": []}, None
            if url.endswith("/v1/albums/alb-q1"):
                return 200, {}, _album_payload("alb-q1"), None
            if url.endswith("/v1/albums/alb-q1/tracks"):
                return 200, {}, {"items": [], "next": None}, None
            raise AssertionError(url)

        _ = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            limit=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT status FROM spotify_catalog_backfill_queue WHERE entity_type = 'album' AND spotify_id = ?",
                ("alb-q1",),
            ).fetchone()
        self.assertEqual("pending", str(row[0]))

    def test_queue_album_marked_done_after_tracklist_complete(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-q2", 2, "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-q2", "t1", "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("album", "alb-q2", "seed", 80, "pending", "2026-04-28T00:00:00Z", 0),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": []}, None
            if url.endswith("/v1/albums/alb-q2"):
                return 200, {}, _album_payload("alb-q2"), None
            if url.endswith("/v1/albums/alb-q2/tracks"):
                return 200, {}, {"items": [_track_payload("t2", "alb-q2")], "next": None}, None
            raise AssertionError(url)

        _ = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            limit=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT status FROM spotify_catalog_backfill_queue WHERE entity_type = 'album' AND spotify_id = ?",
                ("alb-q2",),
            ).fetchone()
        self.assertEqual("done", str(row[0]))

    def test_queue_track_not_done_until_duration_and_album_present(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("track", "trk-q1", "seed", 80, "pending", "2026-04-28T00:00:00Z", 0),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks/trk-q1"):
                payload = _track_payload("trk-q1", "")
                payload["duration_ms"] = None
                payload["album"] = {}
                return 200, {}, payload, None
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": []}, None
            raise AssertionError(url)

        _ = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            limit=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT status FROM spotify_catalog_backfill_queue WHERE entity_type = 'track' AND spotify_id = ?",
                ("trk-q1",),
            ).fetchone()
        self.assertEqual("pending", str(row[0]))

    def test_pending_already_complete_queue_item_marked_done_without_spotify_call(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, duration_ms, album_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("trk-complete", 180000, "alb-complete", "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("track", "trk-complete", "seed", 80, "pending", "2026-04-28T00:00:00Z", 0),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            raise AssertionError("No Spotify calls should be made for already-complete pending queue items")

        _ = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            limit=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT status FROM spotify_catalog_backfill_queue WHERE entity_type = 'track' AND spotify_id = ?",
                ("trk-complete",),
            ).fetchone()
        self.assertEqual("done", str(row[0]))

    def test_done_but_incomplete_item_reopens_on_enqueue(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("album", "alb-reopen", "seed", 10, "done", "2026-04-28T00:00:00Z", 0),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-reopen", 2, "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-reopen", "t1", "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.commit()

        result = enqueue_spotify_catalog_backfill_items(
            items=[{"entity_type": "album", "spotify_id": "alb-reopen", "reason": "again", "priority": 80}]
        )
        self.assertTrue(result["ok"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT status, priority FROM spotify_catalog_backfill_queue WHERE entity_type = 'album' AND spotify_id = ?",
                ("alb-reopen",),
            ).fetchone()
        self.assertEqual("pending", str(row[0]))
        self.assertEqual(80, int(row[1]))

    def test_repair_queue_done_incomplete_album_becomes_pending(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-repair-pending", 2, "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-repair-pending", "track-1", "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("album", "alb-repair-pending", "seed", 50, "done", "2026-04-28T00:00:00Z", 0),
            )
            connection.commit()

        result = repair_spotify_catalog_backfill_queue_statuses()
        self.assertTrue(result["ok"])
        self.assertEqual(1, result["repaired_to_pending"])
        self.assertEqual(0, result["repaired_to_done"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT status FROM spotify_catalog_backfill_queue WHERE entity_type = 'album' AND spotify_id = ?",
                ("alb-repair-pending",),
            ).fetchone()
        self.assertEqual("pending", str(row[0]))

    def test_repair_queue_pending_complete_album_becomes_done(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-repair-done", 2, "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-repair-done", "track-1", "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-repair-done", "track-2", "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("album", "alb-repair-done", "seed", 50, "pending", "2026-04-28T00:00:00Z", 0),
            )
            connection.commit()

        result = repair_spotify_catalog_backfill_queue_statuses()
        self.assertTrue(result["ok"])
        self.assertEqual(0, result["repaired_to_pending"])
        self.assertEqual(1, result["repaired_to_done"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT status FROM spotify_catalog_backfill_queue WHERE entity_type = 'album' AND spotify_id = ?",
                ("alb-repair-done",),
            ).fetchone()
        self.assertEqual("done", str(row[0]))

    def test_skip_existing_uses_album_completeness_helper(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", 2, "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-x", "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.commit()

        album_batch_called = {"value": False}

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                album_batch_called["value"] = True
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if url.endswith("/v1/albums/a1/tracks"):
                return 200, {}, {"items": [_track_payload("a1-y", "a1")], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertFalse(album_batch_called["value"])

    def test_search_albums_not_backfilled_album(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Unmapped Album", "unmapped album"),
                ).lastrowid
            )
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Artist U",)).lastrowid)
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (release_album_id, artist_id),
            )
            connection.commit()

        with patch("backend.app.main._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            response = client.get("/debug/search/albums?catalog_status=not_backfilled&limit=50&offset=0")
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual(1, body["total"])
        self.assertEqual("Unmapped Album", body["items"][0]["release_album_name"])
        self.assertIsNone(body["items"][0]["spotify_album_id"])

    def test_search_albums_backfilled_tracklist_complete_and_incomplete(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            ra_complete = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Complete Album", "complete album")).lastrowid)
            ra_incomplete = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Incomplete Album", "incomplete album")).lastrowid)
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Artist A",)).lastrowid)
            connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)", (ra_complete, artist_id))
            connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)", (ra_incomplete, artist_id))

            sa_complete = int(
                connection.execute(
                    "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "alb-complete", "spotify:album:alb-complete", "Complete Album", "{}"),
                ).lastrowid
            )
            sa_incomplete = int(
                connection.execute(
                    "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "alb-incomplete", "spotify:album:alb-incomplete", "Incomplete Album", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_album_map (
                  source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (sa_complete, ra_complete),
            )
            connection.execute(
                """
                INSERT INTO source_album_map (
                  source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (sa_incomplete, ra_incomplete),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, total_tracks, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?)
                """,
                ("alb-complete", "Spotify Complete Album", 2, "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, total_tracks, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?)
                """,
                ("alb-incomplete", "Spotify Incomplete Album", 3, "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                "INSERT INTO spotify_album_track (spotify_album_id, spotify_track_id, fetched_at, last_status) VALUES (?, ?, ?, ?)",
                ("alb-complete", "c1", "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                "INSERT INTO spotify_album_track (spotify_album_id, spotify_track_id, fetched_at, last_status) VALUES (?, ?, ?, ?)",
                ("alb-complete", "c2", "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                "INSERT INTO spotify_album_track (spotify_album_id, spotify_track_id, fetched_at, last_status) VALUES (?, ?, ?, ?)",
                ("alb-incomplete", "i1", "2026-04-27T12:00:00Z", "ok"),
            )
            connection.commit()

        complete_payload = search_album_catalog_lookup(catalog_status="tracklist_complete", limit=50, offset=0)
        self.assertTrue(complete_payload["ok"])
        self.assertEqual(1, complete_payload["total"])
        self.assertEqual("Complete Album", complete_payload["items"][0]["release_album_name"])
        self.assertTrue(complete_payload["items"][0]["tracklist_complete"])

        incomplete_payload = search_album_catalog_lookup(catalog_status="tracklist_incomplete", limit=50, offset=0)
        self.assertTrue(incomplete_payload["ok"])
        self.assertEqual(1, incomplete_payload["total"])
        self.assertEqual("Incomplete Album", incomplete_payload["items"][0]["release_album_name"])
        self.assertFalse(incomplete_payload["items"][0]["tracklist_complete"])

    def test_search_albums_status_filters_and_q_filter(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            ra_error = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Error Album", "error album")).lastrowid)
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Artist E",)).lastrowid)
            connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)", (ra_error, artist_id))
            sa_error = int(
                connection.execute(
                    "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "alb-error", "spotify:album:alb-error", "Error Album", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_album_map (
                  source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (sa_error, ra_error),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, total_tracks, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-error", "Spotify Error Album", 2, "2026-04-27T12:00:00Z", "error", "failed fetch"),
            )
            connection.commit()

        with patch("backend.app.main._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            error_response = client.get("/debug/search/albums?catalog_status=error")
            q_response = client.get("/debug/search/albums?q=alb-error")
        self.assertEqual(200, error_response.status_code)
        error_body = error_response.json()
        self.assertEqual(1, error_body["total"])
        self.assertEqual("Error Album", error_body["items"][0]["release_album_name"])
        self.assertEqual(200, q_response.status_code)
        q_body = q_response.json()
        self.assertEqual(1, q_body["total"])
        self.assertEqual("alb-error", q_body["items"][0]["spotify_album_id"])

    def test_search_albums_resolves_spotify_id_from_raw_play_event_track_path(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Witch: We Intend to Cause Havoc!", "witch: we intend to cause havoc!"),
                ).lastrowid
            )
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("WITCH",)).lastrowid)
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (release_album_id, artist_id),
            )
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Lazy Bones!!", "lazy bones!!"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                (release_album_id, release_track_id),
            )
            source_track_id = int(
                connection.execute(
                    """
                    INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    ("spotify", "trk-witch-1", "spotify:track:trk-witch-1", "Lazy Bones!!", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_track_map (
                  source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_track_id, release_track_id),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, total_tracks, artists_json, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "28FR52kMwgdiIINFuzYP1q",
                    "Witch: We Intend to Cause Havoc!",
                    54,
                    json.dumps([{"name": "WITCH"}]),
                    "2026-04-28T00:00:00Z",
                    "ok",
                ),
            )
            connection.execute(
                "INSERT INTO spotify_album_track (spotify_album_id, spotify_track_id, fetched_at, last_status) VALUES (?, ?, ?, ?)",
                ("28FR52kMwgdiIINFuzYP1q", "w1", "2026-04-28T00:00:00Z", "ok"),
            )
            connection.execute(
                "INSERT INTO spotify_album_track (spotify_album_id, spotify_track_id, fetched_at, last_status) VALUES (?, ?, ?, ?)",
                ("28FR52kMwgdiIINFuzYP1q", "w2", "2026-04-28T00:00:00Z", "ok"),
            )
            connection.commit()

        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="witch-listen-1",
            played_at="2026-04-28T01:00:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json="{}",
            spotify_track_id="trk-witch-1",
            spotify_album_id="28FR52kMwgdiIINFuzYP1q",
        )
        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="witch-listen-2",
            played_at="2026-04-28T01:01:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json="{}",
            spotify_track_id="trk-witch-1",
            spotify_album_id="28FR52kMwgdiIINFuzYP1q",
        )

        payload = search_album_catalog_lookup(q="Witch: We Intend to Cause Havoc!", catalog_status="all", limit=50, offset=0)
        self.assertTrue(payload["ok"])
        self.assertEqual(1, payload["total"])
        item = payload["items"][0]
        self.assertEqual("28FR52kMwgdiIINFuzYP1q", item["spotify_album_id"])
        self.assertEqual("Witch: We Intend to Cause Havoc!", item["spotify_album_name"])
        self.assertEqual(54, item["total_tracks"])
        self.assertEqual(2, item["album_track_rows"])
        self.assertFalse(item["tracklist_complete"])

    def test_search_albums_does_not_mutate_analysis_track_map(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track A",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        _ = search_album_catalog_lookup(catalog_status="all", limit=50, offset=0)

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_search_album_duplicates_groups_release_albums_by_resolved_spotify_album_id(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Dup Artist",)).lastrowid)
            duplicate_release_ids: list[int] = []
            for name in ("Dup Album One", "Dup Album Two"):
                release_album_id = int(
                    connection.execute(
                        "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                        (name, name.lower()),
                    ).lastrowid
                )
                duplicate_release_ids.append(release_album_id)
                connection.execute(
                    "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                    (release_album_id, artist_id),
                )
            source_album_id = int(
                connection.execute(
                    "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "alb-dup-1", "spotify:album:alb-dup-1", "Dup Album One", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_album_map (
                  source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_album_id, duplicate_release_ids[0]),
            )
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Dup Track Two", "dup track two"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                (duplicate_release_ids[1], release_track_id),
            )
            source_track_id = int(
                connection.execute(
                    "INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "dup-track-2", "spotify:track:dup-track-2", "Dup Track Two", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_track_map (
                  source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_track_id, release_track_id),
            )
            single_release_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Single Album", "single album"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (single_release_id, artist_id),
            )
            single_source_album_id = int(
                connection.execute(
                    "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "alb-single-1", "spotify:album:alb-single-1", "Single Album", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_album_map (
                  source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (single_source_album_id, single_release_id),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, total_tracks, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?)
                """,
                ("alb-dup-1", "Spotify Duplicate Album", 2, "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, total_tracks, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?)
                """,
                ("alb-single-1", "Spotify Single Album", 10, "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                "INSERT INTO spotify_album_track (spotify_album_id, spotify_track_id, fetched_at, last_status) VALUES (?, ?, ?, ?)",
                ("alb-dup-1", "d1", "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                "INSERT INTO spotify_album_track (spotify_album_id, spotify_track_id, fetched_at, last_status) VALUES (?, ?, ?, ?)",
                ("alb-dup-1", "d2", "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts) VALUES (?, ?, ?, ?, ?, ?)",
                ("album", "alb-dup-1", 80, "pending", "2026-04-27T11:00:00Z", 1),
            )
            connection.commit()

        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="dup-listen-2",
            played_at="2026-04-28T01:00:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json="{}",
            spotify_track_id="dup-track-2",
            spotify_album_id="alb-dup-1",
        )

        payload = search_album_catalog_duplicate_spotify_identities(limit=200, offset=0)
        self.assertTrue(payload["ok"])
        self.assertEqual(1, payload["total"])
        self.assertEqual(1, len(payload["items"]))
        group = payload["items"][0]
        self.assertEqual("alb-dup-1", group["spotify_album_id"])
        self.assertEqual("Spotify Duplicate Album", group["spotify_album_name"])
        self.assertEqual(2, group["duplicate_count"])
        self.assertEqual(2, len(group["release_albums"]))
        grouped_release_ids = {item["release_album_id"] for item in group["release_albums"]}
        self.assertEqual(set(duplicate_release_ids), grouped_release_ids)
        self.assertNotIn(single_release_id, grouped_release_ids)
        self.assertEqual({"pending"}, {item["queue_status"] for item in group["release_albums"]})

    def test_search_album_duplicates_does_not_call_spotify(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed"
        ) as refresh_mock, patch("backend.app.main.run_spotify_catalog_backfill") as run_mock:
            client = TestClient(app)
            response = client.get("/debug/search/albums/duplicates?limit=200&offset=0")
        self.assertEqual(200, response.status_code)
        refresh_mock.assert_not_called()
        run_mock.assert_not_called()

    def test_search_album_duplicates_does_not_mutate_analysis_track_map(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track A",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        _ = search_album_catalog_duplicate_spotify_identities(limit=200, offset=0)

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_search_album_duplicates_by_name_groups_expected_rows(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_tele_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Telekinesis",)).lastrowid)
            artist_tele_variant_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Telekinesis, Telekinesis",)).lastrowid)
            artist_other_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Other Artist",)).lastrowid)

            tele_one_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Telekinesis", "telekinesis")).lastrowid)
            tele_two_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Telekinesis", "telekinesis")).lastrowid)
            tele_other_artist_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Telekinesis", "telekinesis")).lastrowid)
            different_name_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Different Album", "different album")).lastrowid)

            connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)", (tele_one_id, artist_tele_id))
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (tele_two_id, artist_tele_id),
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 2)",
                (tele_two_id, artist_tele_variant_id),
            )
            connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)", (tele_other_artist_id, artist_other_id))
            connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)", (different_name_id, artist_tele_id))

            source_one = int(
                connection.execute(
                    "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "alb-tel-1", "spotify:album:alb-tel-1", "Telekinesis", "{}"),
                ).lastrowid
            )
            source_other_artist = int(
                connection.execute(
                    "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "alb-other-artist", "spotify:album:alb-other-artist", "Telekinesis", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_album_map (
                  source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_one, tele_one_id),
            )
            connection.execute(
                """
                INSERT INTO source_album_map (
                  source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_other_artist, tele_other_artist_id),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, total_tracks, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?)
                """,
                ("alb-tel-1", "Telekinesis", 11, "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, total_tracks, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?)
                """,
                ("alb-other-artist", "Telekinesis", 11, "2026-04-27T12:00:00Z", "ok"),
            )
            connection.commit()

        payload = search_album_catalog_duplicate_by_name_identities(limit=200, offset=0)
        self.assertTrue(payload["ok"])
        self.assertEqual(1, payload["total"])
        self.assertEqual(1, len(payload["items"]))
        group = payload["items"][0]
        self.assertEqual("telekinesis", group["normalized_album_name"])
        self.assertEqual("telekinesis", group["normalized_primary_artist"])
        self.assertEqual(2, group["duplicate_count"])
        grouped_release_ids = {item["release_album_id"] for item in group["release_albums"]}
        self.assertEqual({tele_one_id, tele_two_id}, grouped_release_ids)
        self.assertNotIn(tele_other_artist_id, grouped_release_ids)
        self.assertNotIn(different_name_id, grouped_release_ids)
        self.assertIn("alb-tel-1", group["spotify_album_ids"])

    def test_search_album_duplicates_by_name_does_not_call_spotify(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed"
        ) as refresh_mock, patch("backend.app.main.run_spotify_catalog_backfill") as run_mock:
            client = TestClient(app)
            response = client.get("/debug/search/albums/duplicates-by-name?limit=200&offset=0")
        self.assertEqual(200, response.status_code)
        refresh_mock.assert_not_called()
        run_mock.assert_not_called()

    def test_search_album_duplicates_by_name_does_not_mutate_analysis_track_map(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track A",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        _ = search_album_catalog_duplicate_by_name_identities(limit=200, offset=0)

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_release_album_merge_preview_chooses_deterministic_survivor_and_lists_affected_rows(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Artist A",)).lastrowid)
            album_one_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            album_two_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            album_three_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            track_one_id = int(connection.execute("INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)", ("Track 1", "track 1")).lastrowid)
            track_two_id = int(connection.execute("INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)", ("Track 2", "track 2")).lastrowid)
            for album_id in (album_one_id, album_two_id, album_three_id):
                connection.execute(
                    "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                    (album_id, artist_id),
                )
            connection.execute("INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)", (album_one_id, track_one_id))
            connection.execute("INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)", (album_two_id, track_one_id))
            connection.execute("INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)", (album_three_id, track_two_id))
            source_two_id = int(
                connection.execute(
                    "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "alb-survivor", "spotify:album:alb-survivor", "Album A", "{}"),
                ).lastrowid
            )
            source_three_id = int(
                connection.execute(
                    "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "alb-duplicate", "spotify:album:alb-duplicate", "Album A", "{}"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO source_album_map (source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')",
                (source_two_id, album_two_id),
            )
            connection.execute(
                "INSERT INTO source_album_map (source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation) VALUES (?, ?, 'provider_identity', 1.0, 'suggested', 0, 'seed')",
                (source_three_id, album_three_id),
            )
            connection.execute(
                "INSERT INTO spotify_album_catalog (spotify_album_id, name, fetched_at, last_status) VALUES (?, ?, ?, ?)",
                ("alb-survivor", "Album A", "2026-04-29T12:00:00Z", "ok"),
            )
            connection.commit()
            insert_raw_play_event(
                source_type="spotify_recent",
                source_row_key="merge-preview-row",
                played_at="2026-04-29T12:00:00Z",
                ms_played=1000,
                ms_played_method="history_source",
                raw_payload_json="{}",
                track_name_raw="Track 1",
                artist_name_raw="Artist A",
                album_name_raw="Album A",
                spotify_album_id="alb-survivor",
            )

        payload = preview_release_album_merge([album_one_id, album_two_id, album_three_id])

        self.assertTrue(payload["ok"])
        self.assertEqual(album_two_id, payload["survivor_release_album_id"])
        self.assertEqual([album_one_id, album_three_id], payload["merge_release_album_ids"])
        self.assertEqual(1, payload["affected"]["source_album_map_rows"])
        self.assertEqual(2, payload["affected"]["album_artist_rows"])
        self.assertEqual(2, payload["affected"]["album_track_rows"])
        self.assertEqual(1, payload["affected"]["album_track_conflicts"])
        self.assertEqual(2, payload["affected"]["release_track_rows"])
        self.assertEqual(1, payload["affected"]["raw_play_event_rows"])
        self.assertEqual("needs_review", payload["merge_readiness"])
        self.assertTrue(any("album_track.release_album_id" in operation for operation in payload["proposed_operations"]))
        self.assertTrue(any("release_track rows directly" in operation for operation in payload["proposed_operations"]))

    def test_release_album_merge_preview_safe_candidate(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Artist A",)).lastrowid)
            album_one_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            album_two_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            source_one_id = int(connection.execute("INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)", ("spotify", "alb-safe", "spotify:album:alb-safe", "Album A", "{}")).lastrowid)
            for album_id in (album_one_id, album_two_id):
                connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)", (album_id, artist_id))
            connection.execute("INSERT INTO source_album_map (source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')", (source_one_id, album_one_id))
            connection.execute("INSERT INTO source_album_map (source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')", (source_one_id, album_two_id))
            connection.execute("INSERT INTO spotify_album_catalog (spotify_album_id, name, fetched_at, last_status) VALUES (?, ?, ?, ?)", ("alb-safe", "Album A", "2026-04-29T12:00:00Z", "ok"))
            connection.commit()

        payload = preview_release_album_merge([album_one_id, album_two_id])

        self.assertEqual("safe_candidate", payload["merge_readiness"])
        self.assertEqual(0, payload["affected"]["album_track_conflicts"])

    def test_release_album_merge_preview_multiple_spotify_ids_needs_review(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Artist A",)).lastrowid)
            album_one_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            album_two_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            source_one_id = int(connection.execute("INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)", ("spotify", "alb-one", "spotify:album:alb-one", "Album A", "{}")).lastrowid)
            source_two_id = int(connection.execute("INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)", ("spotify", "alb-two", "spotify:album:alb-two", "Album A", "{}")).lastrowid)
            for album_id in (album_one_id, album_two_id):
                connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)", (album_id, artist_id))
            connection.execute("INSERT INTO source_album_map (source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')", (source_one_id, album_one_id))
            connection.execute("INSERT INTO source_album_map (source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')", (source_two_id, album_two_id))
            connection.commit()

        payload = preview_release_album_merge([album_one_id, album_two_id])

        self.assertEqual("needs_review", payload["merge_readiness"])
        self.assertTrue(any("Multiple distinct Spotify album IDs" in reason for reason in payload["readiness_reasons"]))

    def test_release_album_merge_preview_different_name_or_artist_unsafe(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_one_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Artist A",)).lastrowid)
            artist_two_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Artist B",)).lastrowid)
            album_one_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            album_two_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album B", "album b")).lastrowid)
            connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)", (album_one_id, artist_one_id))
            connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)", (album_two_id, artist_two_id))
            connection.commit()

        payload = preview_release_album_merge([album_one_id, album_two_id])

        self.assertEqual("unsafe", payload["merge_readiness"])
        self.assertTrue(any("different normalized album names" in reason for reason in payload["readiness_reasons"]))
        self.assertTrue(any("different normalized primary artists" in reason for reason in payload["readiness_reasons"]))

    def test_release_album_merge_preview_missing_ids_unsafe(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            album_one_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            album_two_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            connection.commit()

        payload = preview_release_album_merge([album_one_id, album_two_id, 999999])

        self.assertEqual("unsafe", payload["merge_readiness"])
        self.assertTrue(any("not found" in reason for reason in payload["readiness_reasons"]))

    def test_release_album_merge_preview_invalid_single_id_returns_warning(self) -> None:
        payload = preview_release_album_merge([1])

        self.assertFalse(payload["ok"])
        self.assertIsNone(payload["survivor_release_album_id"])
        self.assertEqual("unsafe", payload["merge_readiness"])
        self.assertTrue(payload["warnings"])

    def test_release_album_merge_preview_endpoint_does_not_call_spotify(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed"
        ) as refresh_mock, patch("backend.app.main.run_spotify_catalog_backfill") as run_mock:
            client = TestClient(app)
            response = client.post("/debug/identity/release-albums/merge-preview", json={"release_album_ids": [1]})
        self.assertEqual(200, response.status_code)
        refresh_mock.assert_not_called()
        run_mock.assert_not_called()

    def test_release_album_merge_preview_does_not_write_or_mutate_analysis_track_map(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            album_one_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            album_two_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            release_track_id = int(connection.execute("INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)", ("Track A", "track a")).lastrowid)
            analysis_track_id = int(connection.execute("INSERT INTO analysis_track (primary_name) VALUES (?)", ("Track A",)).lastrowid)
            connection.execute(
                "INSERT INTO analysis_track_map (release_track_id, analysis_track_id, match_method, confidence, status, is_user_confirmed, explanation) VALUES (?, ?, 'seed', 0.5, 'suggested', 0, 'seed')",
                (release_track_id, analysis_track_id),
            )
            connection.commit()
            before_digest = self._identity_album_digest(connection)
            before_analysis_count, before_analysis_digest = self._analysis_track_map_digest(connection)

        _ = preview_release_album_merge([album_one_id, album_two_id])

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_digest = self._identity_album_digest(connection)
            after_analysis_count, after_analysis_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_digest, after_digest)
        self.assertEqual(before_analysis_count, after_analysis_count)
        self.assertEqual(before_analysis_digest, after_analysis_digest)

    def test_release_album_merge_dry_run_blocked_for_unsafe(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_one_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Artist A",)).lastrowid)
            artist_two_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Artist B",)).lastrowid)
            album_one_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            album_two_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album B", "album b")).lastrowid)
            connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)", (album_one_id, artist_one_id))
            connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)", (album_two_id, artist_two_id))
            connection.commit()

        payload = dry_run_release_album_merge([album_one_id, album_two_id], survivor_release_album_id=album_one_id)

        self.assertFalse(payload["ok"])
        self.assertTrue(payload["blocked"])
        self.assertEqual("unsafe", payload["merge_readiness"])
        self.assertTrue(payload["blocked_reasons"])

    def test_release_album_merge_dry_run_allowed_for_safe_candidate(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Artist A",)).lastrowid)
            album_one_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            album_two_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            source_id = int(connection.execute("INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)", ("spotify", "alb-safe-dry", "spotify:album:alb-safe-dry", "Album A", "{}")).lastrowid)
            for album_id in (album_one_id, album_two_id):
                connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)", (album_id, artist_id))
                connection.execute("INSERT INTO source_album_map (source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')", (source_id, album_id))
            connection.execute("INSERT INTO spotify_album_catalog (spotify_album_id, name, fetched_at, last_status) VALUES (?, ?, ?, ?)", ("alb-safe-dry", "Album A", "2026-04-29T12:00:00Z", "ok"))
            connection.commit()

        preview = preview_release_album_merge([album_one_id, album_two_id])
        payload = dry_run_release_album_merge(
            [album_one_id, album_two_id],
            survivor_release_album_id=preview["survivor_release_album_id"],
        )

        self.assertTrue(payload["ok"])
        self.assertFalse(payload["blocked"])
        self.assertEqual("safe_candidate", payload["merge_readiness"])
        self.assertEqual(1, payload["rows_affected"]["source_album_map"])
        self.assertEqual(1, payload["rows_affected"]["release_album_retire"])

    def test_release_album_merge_dry_run_includes_album_track_repoints_and_conflicts(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Artist A",)).lastrowid)
            survivor_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            duplicate_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            source_id = int(connection.execute("INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)", ("spotify", "alb-conflict-dry", "spotify:album:alb-conflict-dry", "Album A", "{}")).lastrowid)
            source_duplicate_id = int(connection.execute("INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)", ("spotify", "alb-conflict-dry-other", "spotify:album:alb-conflict-dry-other", "Album A", "{}")).lastrowid)
            conflict_track_id = int(connection.execute("INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)", ("Track 1", "track 1")).lastrowid)
            repoint_track_id = int(connection.execute("INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)", ("Track 2", "track 2")).lastrowid)
            for album_id in (survivor_id, duplicate_id):
                connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)", (album_id, artist_id))
            connection.execute("INSERT INTO source_album_map (source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')", (source_id, survivor_id))
            connection.execute("INSERT INTO source_album_map (source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation) VALUES (?, ?, 'provider_identity', 1.0, 'suggested', 0, 'seed')", (source_duplicate_id, duplicate_id))
            connection.execute("INSERT INTO spotify_album_catalog (spotify_album_id, name, fetched_at, last_status) VALUES (?, ?, ?, ?)", ("alb-conflict-dry", "Album A", "2026-04-29T12:00:00Z", "ok"))
            connection.execute("INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)", (survivor_id, conflict_track_id))
            connection.execute("INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)", (duplicate_id, conflict_track_id))
            connection.execute("INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)", (duplicate_id, repoint_track_id))
            connection.commit()

        preview = preview_release_album_merge([survivor_id, duplicate_id])
        payload = dry_run_release_album_merge(
            [survivor_id, duplicate_id],
            survivor_release_album_id=preview["survivor_release_album_id"],
        )

        self.assertTrue(payload["ok"])
        self.assertEqual("needs_review", payload["merge_readiness"])
        self.assertEqual(1, payload["rows_affected"]["album_track_repoint"])
        self.assertEqual(1, payload["rows_affected"]["album_track_conflict_delete"])
        self.assertEqual(1, len(payload["plan"]["album_track_repoints"]))
        self.assertEqual(1, len(payload["plan"]["album_track_conflicts"]))

    def test_release_album_merge_dry_run_does_not_write_or_mutate_analysis_track_map(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            album_one_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            album_two_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            release_track_id = int(connection.execute("INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)", ("Track A", "track a")).lastrowid)
            analysis_track_id = int(connection.execute("INSERT INTO analysis_track (primary_name) VALUES (?)", ("Track A",)).lastrowid)
            connection.execute(
                "INSERT INTO analysis_track_map (release_track_id, analysis_track_id, match_method, confidence, status, is_user_confirmed, explanation) VALUES (?, ?, 'seed', 0.5, 'suggested', 0, 'seed')",
                (release_track_id, analysis_track_id),
            )
            connection.commit()
            before_digest = self._identity_album_digest(connection)
            before_analysis_count, before_analysis_digest = self._analysis_track_map_digest(connection)

        _ = dry_run_release_album_merge([album_one_id, album_two_id], survivor_release_album_id=album_one_id)

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_digest = self._identity_album_digest(connection)
            after_analysis_count, after_analysis_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_digest, after_digest)
        self.assertEqual(before_analysis_count, after_analysis_count)
        self.assertEqual(before_analysis_digest, after_analysis_digest)

    def test_release_album_merge_dry_run_endpoint_does_not_call_spotify(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed"
        ) as refresh_mock, patch("backend.app.main.run_spotify_catalog_backfill") as run_mock:
            client = TestClient(app)
            response = client.post(
                "/debug/identity/release-albums/merge-dry-run",
                json={"release_album_ids": [1], "survivor_release_album_id": 1},
            )
        self.assertEqual(200, response.status_code)
        refresh_mock.assert_not_called()
        run_mock.assert_not_called()

    def test_search_tracks_not_backfilled_and_backfilled(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Track Artist",)).lastrowid)
            release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track Album", "track album"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (release_album_id, artist_id),
            )

            not_backfilled_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track Missing", "track missing"),
                ).lastrowid
            )
            backfilled_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track Backfilled", "track backfilled"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (not_backfilled_track_id, artist_id),
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (backfilled_track_id, artist_id),
            )
            connection.execute(
                "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                (release_album_id, not_backfilled_track_id),
            )
            connection.execute(
                "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                (release_album_id, backfilled_track_id),
            )

            source_track_id = int(
                connection.execute(
                    "INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "trk-backfilled", "spotify:track:trk-backfilled", "Track Backfilled", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_track_map (
                  source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_track_id, backfilled_track_id),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, album_id, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("trk-backfilled", "Spotify Backfilled", 181000, "alb-1", "2026-04-27T12:00:00Z", "ok"),
            )
            connection.commit()

        not_backfilled_payload = search_track_catalog_lookup(catalog_status="not_backfilled", limit=50, offset=0)
        self.assertTrue(not_backfilled_payload["ok"])
        self.assertEqual(1, not_backfilled_payload["total"])
        self.assertEqual("Track Missing", not_backfilled_payload["items"][0]["release_track_name"])
        self.assertIsNone(not_backfilled_payload["items"][0]["spotify_track_id"])

        backfilled_payload = search_track_catalog_lookup(catalog_status="backfilled", limit=50, offset=0)
        self.assertTrue(backfilled_payload["ok"])
        self.assertEqual(1, backfilled_payload["total"])
        self.assertEqual("Track Backfilled", backfilled_payload["items"][0]["release_track_name"])
        self.assertEqual("trk-backfilled", backfilled_payload["items"][0]["spotify_track_id"])
        self.assertEqual("3:01", backfilled_payload["items"][0]["duration_display"])

    def test_search_tracks_duration_missing_error_and_q_filter(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Track Artist B",)).lastrowid)
            release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Lookup Album", "lookup album"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (release_album_id, artist_id),
            )
            duration_missing_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track No Duration", "track no duration"),
                ).lastrowid
            )
            error_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track Error", "track error"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (duration_missing_track_id, artist_id),
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (error_track_id, artist_id),
            )
            connection.execute(
                "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                (release_album_id, duration_missing_track_id),
            )
            connection.execute(
                "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                (release_album_id, error_track_id),
            )

            source_track_duration_missing = int(
                connection.execute(
                    "INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "trk-no-duration", "spotify:track:trk-no-duration", "Track No Duration", "{}"),
                ).lastrowid
            )
            source_track_error = int(
                connection.execute(
                    "INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "trk-error", "spotify:track:trk-error", "Track Error", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_track_map (
                  source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_track_duration_missing, duration_missing_track_id),
            )
            connection.execute(
                """
                INSERT INTO source_track_map (
                  source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_track_error, error_track_id),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, album_id, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("trk-no-duration", "Spotify No Duration", None, "alb-2", "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, album_id, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("trk-error", "Spotify Error", None, None, "2026-04-27T12:00:00Z", "error", "failed fetch"),
            )
            connection.commit()

        duration_missing_payload = search_track_catalog_lookup(catalog_status="duration_missing", limit=50, offset=0)
        self.assertTrue(duration_missing_payload["ok"])
        self.assertEqual(2, duration_missing_payload["total"])
        self.assertEqual(
            {"Track Error", "Track No Duration"},
            {item["release_track_name"] for item in duration_missing_payload["items"]},
        )

        with patch("backend.app.main._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            error_response = client.get("/debug/search/tracks?catalog_status=error")
            q_response = client.get("/debug/search/tracks?q=trk-error")
        self.assertEqual(200, error_response.status_code)
        error_body = error_response.json()
        self.assertEqual(1, error_body["total"])
        self.assertEqual("Track Error", error_body["items"][0]["release_track_name"])
        self.assertEqual(200, q_response.status_code)
        q_body = q_response.json()
        self.assertEqual(1, q_body["total"])
        self.assertEqual("trk-error", q_body["items"][0]["spotify_track_id"])

    def test_search_tracks_does_not_call_spotify(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed"
        ) as refresh_mock, patch("backend.app.main.run_spotify_catalog_backfill") as run_mock:
            client = TestClient(app)
            response = client.get("/debug/search/tracks?catalog_status=all&queue_status=all")
        self.assertEqual(200, response.status_code)
        refresh_mock.assert_not_called()
        run_mock.assert_not_called()

    def test_search_tracks_does_not_mutate_analysis_track_map(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track A",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        _ = search_track_catalog_lookup(catalog_status="all", limit=50, offset=0)

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_search_track_duplicates_groups_release_tracks_by_resolved_spotify_track_id(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Dup Track Artist",)).lastrowid)
            release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Dup Track Album", "dup track album"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (release_album_id, artist_id),
            )

            duplicate_release_track_ids: list[int] = []
            for name in ("Dup Track One", "Dup Track Two"):
                release_track_id = int(
                    connection.execute(
                        "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                        (name, name.lower()),
                    ).lastrowid
                )
                duplicate_release_track_ids.append(release_track_id)
                connection.execute(
                    "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                    (release_track_id, artist_id),
                )
                connection.execute(
                    "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                    (release_album_id, release_track_id),
                )

            singleton_release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Singleton Track", "singleton track"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (singleton_release_track_id, artist_id),
            )
            connection.execute(
                "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                (release_album_id, singleton_release_track_id),
            )

            duplicate_source_track_id = int(
                connection.execute(
                    "INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "trk-dup-1", "spotify:track:trk-dup-1", "Dup Track One", "{}"),
                ).lastrowid
            )
            for release_track_id in duplicate_release_track_ids:
                connection.execute(
                    """
                    INSERT INTO source_track_map (
                      source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                    ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                    """,
                    (duplicate_source_track_id, release_track_id),
                )

            singleton_source_track_id = int(
                connection.execute(
                    "INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "trk-single-1", "spotify:track:trk-single-1", "Singleton Track", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_track_map (
                  source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (singleton_source_track_id, singleton_release_track_id),
            )

            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, album_id, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("trk-dup-1", "Spotify Duplicate Track", 181000, "alb-dup-1", "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, album_id, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("trk-single-1", "Spotify Singleton Track", 200000, "alb-single-1", "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts) VALUES (?, ?, ?, ?, ?, ?)",
                ("track", "trk-dup-1", 80, "pending", "2026-04-27T11:00:00Z", 1),
            )
            connection.commit()

        payload = search_track_catalog_duplicate_spotify_identities(limit=200, offset=0)
        self.assertTrue(payload["ok"])
        self.assertEqual(1, payload["total"])
        self.assertEqual(1, len(payload["items"]))
        group = payload["items"][0]
        self.assertEqual("trk-dup-1", group["spotify_track_id"])
        self.assertEqual("Spotify Duplicate Track", group["spotify_track_name"])
        self.assertEqual(181000, group["duration_ms"])
        self.assertEqual("3:01", group["duration_display"])
        self.assertEqual(2, group["duplicate_count"])
        self.assertEqual(2, len(group["release_tracks"]))
        grouped_release_track_ids = {item["release_track_id"] for item in group["release_tracks"]}
        self.assertEqual(set(duplicate_release_track_ids), grouped_release_track_ids)
        self.assertNotIn(singleton_release_track_id, grouped_release_track_ids)
        self.assertEqual({"pending"}, {item["queue_status"] for item in group["release_tracks"]})

    def test_search_track_duplicates_does_not_call_spotify(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed"
        ) as refresh_mock, patch("backend.app.main.run_spotify_catalog_backfill") as run_mock:
            client = TestClient(app)
            response = client.get("/debug/search/tracks/duplicates?limit=200&offset=0")
        self.assertEqual(200, response.status_code)
        refresh_mock.assert_not_called()
        run_mock.assert_not_called()

    def test_search_track_duplicates_does_not_mutate_analysis_track_map(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track A",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        _ = search_track_catalog_duplicate_spotify_identities(limit=200, offset=0)

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_search_albums_queue_status_filters_and_combined_filters(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Queue Artist Album",)).lastrowid)

            def _seed_album(name: str, spotify_album_id: str, *, catalog_status: str = "ok", catalog_error: str | None = None) -> None:
                release_album_id = int(
                    connection.execute(
                        "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                        (name, name.lower()),
                    ).lastrowid
                )
                connection.execute(
                    "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                    (release_album_id, artist_id),
                )
                source_album_id = int(
                    connection.execute(
                        "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                        ("spotify", spotify_album_id, f"spotify:album:{spotify_album_id}", name, "{}"),
                    ).lastrowid
                )
                connection.execute(
                    """
                    INSERT INTO source_album_map (
                      source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation
                    ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                    """,
                    (source_album_id, release_album_id),
                )
                connection.execute(
                    """
                    INSERT INTO spotify_album_catalog (
                      spotify_album_id, name, total_tracks, fetched_at, last_status, last_error
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (spotify_album_id, f"Spotify {name}", 2, "2026-04-27T12:00:00Z", catalog_status, catalog_error),
                )

            _seed_album("Album Pending", "alb-pending", catalog_status="error", catalog_error="catalog failed")
            _seed_album("Album Done", "alb-done")
            _seed_album("Album Queue Error", "alb-queue-error")
            _seed_album("Album Not Queued", "alb-not-queued")
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts) VALUES (?, ?, ?, ?, ?, ?)",
                ("album", "alb-pending", 80, "pending", "2026-04-27T11:00:00Z", 1),
            )
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts) VALUES (?, ?, ?, ?, ?, ?)",
                ("album", "alb-done", 40, "done", "2026-04-27T11:00:00Z", 2),
            )
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts, last_error) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("album", "alb-queue-error", 30, "error", "2026-04-27T11:00:00Z", 3, "queue failure"),
            )
            connection.commit()

        pending_payload = search_album_catalog_lookup(catalog_status="all", queue_status="pending", limit=50, offset=0)
        done_payload = search_album_catalog_lookup(catalog_status="all", queue_status="done", limit=50, offset=0)
        error_payload = search_album_catalog_lookup(catalog_status="all", queue_status="error", limit=50, offset=0)
        not_queued_payload = search_album_catalog_lookup(catalog_status="all", queue_status="not_queued", limit=50, offset=0)
        combined_payload = search_album_catalog_lookup(catalog_status="error", queue_status="pending", limit=50, offset=0)

        self.assertEqual({"alb-pending"}, {item["spotify_album_id"] for item in pending_payload["items"]})
        self.assertEqual({"alb-done"}, {item["spotify_album_id"] for item in done_payload["items"]})
        self.assertEqual({"alb-queue-error"}, {item["spotify_album_id"] for item in error_payload["items"]})
        self.assertEqual({"alb-not-queued"}, {item["spotify_album_id"] for item in not_queued_payload["items"]})
        self.assertEqual(1, combined_payload["total"])
        self.assertEqual("alb-pending", combined_payload["items"][0]["spotify_album_id"])
        self.assertEqual("pending", combined_payload["items"][0]["queue_status"])
        self.assertIn("queue_priority", combined_payload["items"][0])
        self.assertIn("queue_requested_at", combined_payload["items"][0])
        self.assertIn("queue_attempts", combined_payload["items"][0])
        self.assertIn("queue_last_error", combined_payload["items"][0])

        with patch("backend.app.main._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            endpoint_response = client.get("/debug/search/albums?catalog_status=all&queue_status=pending")
        self.assertEqual(200, endpoint_response.status_code)
        self.assertEqual(1, endpoint_response.json().get("total"))

    def test_search_albums_sort_recently_backfilled_and_name(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Album Sort Artist",)).lastrowid)
            seeded = [
                ("Beta Album", "alb-sort-beta", "2026-04-27T12:00:01Z"),
                ("Alpha Album", "alb-sort-alpha", "2026-04-27T12:00:03Z"),
                ("Gamma Album", "alb-sort-gamma", "2026-04-27T12:00:02Z"),
            ]
            for release_name, spotify_album_id, fetched_at in seeded:
                release_album_id = int(
                    connection.execute(
                        "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                        (release_name, release_name.lower()),
                    ).lastrowid
                )
                connection.execute(
                    "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                    (release_album_id, artist_id),
                )
                source_album_id = int(
                    connection.execute(
                        "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                        ("spotify", spotify_album_id, f"spotify:album:{spotify_album_id}", release_name, "{}"),
                    ).lastrowid
                )
                connection.execute(
                    """
                    INSERT INTO source_album_map (
                      source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation
                    ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                    """,
                    (source_album_id, release_album_id),
                )
                connection.execute(
                    """
                    INSERT INTO spotify_album_catalog (
                      spotify_album_id, name, total_tracks, fetched_at, last_status
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (spotify_album_id, f"Spotify {release_name}", 2, fetched_at, "ok"),
                )
            connection.commit()

        recent_payload = search_album_catalog_lookup(catalog_status="backfilled", queue_status="all", sort="recently_backfilled", limit=50, offset=0)
        self.assertEqual(
            ["Alpha Album", "Gamma Album", "Beta Album"],
            [item["release_album_name"] for item in recent_payload["items"]],
        )
        name_payload = search_album_catalog_lookup(catalog_status="backfilled", queue_status="all", sort="name", limit=50, offset=0)
        self.assertEqual(
            ["Alpha Album", "Beta Album", "Gamma Album"],
            [item["release_album_name"] for item in name_payload["items"]],
        )
        combined_payload = search_album_catalog_lookup(catalog_status="backfilled", queue_status="not_queued", sort="name", limit=50, offset=0)
        self.assertEqual(3, combined_payload["total"])

    def test_search_tracks_queue_status_filters_and_combined_filters(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Queue Artist Track",)).lastrowid)
            release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Queue Track Album", "queue track album"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (release_album_id, artist_id),
            )

            def _seed_track(name: str, spotify_track_id: str, *, duration_ms: int | None = 180000, last_status: str = "ok", last_error: str | None = None) -> None:
                release_track_id = int(
                    connection.execute(
                        "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                        (name, name.lower()),
                    ).lastrowid
                )
                source_track_id = int(
                    connection.execute(
                        "INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                        ("spotify", spotify_track_id, f"spotify:track:{spotify_track_id}", name, "{}"),
                    ).lastrowid
                )
                connection.execute(
                    "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                    (release_track_id, artist_id),
                )
                connection.execute(
                    "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                    (release_album_id, release_track_id),
                )
                connection.execute(
                    """
                    INSERT INTO source_track_map (
                      source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                    ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                    """,
                    (source_track_id, release_track_id),
                )
                connection.execute(
                    """
                    INSERT INTO spotify_track_catalog (
                      spotify_track_id, name, duration_ms, album_id, fetched_at, last_status, last_error
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (spotify_track_id, f"Spotify {name}", duration_ms, "alb-seeded", "2026-04-27T12:00:00Z", last_status, last_error),
                )

            _seed_track("Track Pending", "trk-pending", duration_ms=None, last_status="error", last_error="catalog fail")
            _seed_track("Track Done", "trk-done")
            _seed_track("Track Queue Error", "trk-queue-error")
            _seed_track("Track Not Queued", "trk-not-queued")
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts) VALUES (?, ?, ?, ?, ?, ?)",
                ("track", "trk-pending", 80, "pending", "2026-04-27T11:00:00Z", 1),
            )
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts) VALUES (?, ?, ?, ?, ?, ?)",
                ("track", "trk-done", 40, "done", "2026-04-27T11:00:00Z", 2),
            )
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts, last_error) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("track", "trk-queue-error", 30, "error", "2026-04-27T11:00:00Z", 3, "queue failure"),
            )
            connection.commit()

        pending_payload = search_track_catalog_lookup(catalog_status="all", queue_status="pending", limit=50, offset=0)
        done_payload = search_track_catalog_lookup(catalog_status="all", queue_status="done", limit=50, offset=0)
        error_payload = search_track_catalog_lookup(catalog_status="all", queue_status="error", limit=50, offset=0)
        not_queued_payload = search_track_catalog_lookup(catalog_status="all", queue_status="not_queued", limit=50, offset=0)
        combined_payload = search_track_catalog_lookup(catalog_status="error", queue_status="pending", limit=50, offset=0)

        self.assertEqual({"trk-pending"}, {item["spotify_track_id"] for item in pending_payload["items"]})
        self.assertEqual({"trk-done"}, {item["spotify_track_id"] for item in done_payload["items"]})
        self.assertEqual({"trk-queue-error"}, {item["spotify_track_id"] for item in error_payload["items"]})
        self.assertEqual({"trk-not-queued"}, {item["spotify_track_id"] for item in not_queued_payload["items"]})
        self.assertEqual(1, combined_payload["total"])
        self.assertEqual("trk-pending", combined_payload["items"][0]["spotify_track_id"])
        self.assertEqual("pending", combined_payload["items"][0]["queue_status"])
        self.assertIn("queue_priority", combined_payload["items"][0])
        self.assertIn("queue_requested_at", combined_payload["items"][0])
        self.assertIn("queue_attempts", combined_payload["items"][0])
        self.assertIn("queue_last_error", combined_payload["items"][0])

        with patch("backend.app.main._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            endpoint_response = client.get("/debug/search/tracks?catalog_status=all&queue_status=pending")
        self.assertEqual(200, endpoint_response.status_code)
        self.assertEqual(1, endpoint_response.json().get("total"))

    def test_search_tracks_sort_recently_backfilled_and_name(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Track Sort Artist",)).lastrowid)
            release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track Sort Album", "track sort album"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (release_album_id, artist_id),
            )

            seeded = [
                ("Beta Track", "trk-sort-beta", "2026-04-27T12:00:01Z"),
                ("Alpha Track", "trk-sort-alpha", "2026-04-27T12:00:03Z"),
                ("Gamma Track", "trk-sort-gamma", "2026-04-27T12:00:02Z"),
            ]
            for release_name, spotify_track_id, fetched_at in seeded:
                release_track_id = int(
                    connection.execute(
                        "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                        (release_name, release_name.lower()),
                    ).lastrowid
                )
                connection.execute(
                    "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                    (release_track_id, artist_id),
                )
                connection.execute(
                    "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                    (release_album_id, release_track_id),
                )
                source_track_id = int(
                    connection.execute(
                        "INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                        ("spotify", spotify_track_id, f"spotify:track:{spotify_track_id}", release_name, "{}"),
                    ).lastrowid
                )
                connection.execute(
                    """
                    INSERT INTO source_track_map (
                      source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                    ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                    """,
                    (source_track_id, release_track_id),
                )
                connection.execute(
                    """
                    INSERT INTO spotify_track_catalog (
                      spotify_track_id, name, duration_ms, album_id, fetched_at, last_status
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (spotify_track_id, f"Spotify {release_name}", 181000, "alb-sort", fetched_at, "ok"),
                )
            connection.commit()

        recent_payload = search_track_catalog_lookup(catalog_status="backfilled", queue_status="all", sort="recently_backfilled", limit=50, offset=0)
        self.assertEqual(
            ["Alpha Track", "Gamma Track", "Beta Track"],
            [item["release_track_name"] for item in recent_payload["items"]],
        )
        name_payload = search_track_catalog_lookup(catalog_status="backfilled", queue_status="all", sort="name", limit=50, offset=0)
        self.assertEqual(
            ["Alpha Track", "Beta Track", "Gamma Track"],
            [item["release_track_name"] for item in name_payload["items"]],
        )
        combined_payload = search_track_catalog_lookup(catalog_status="backfilled", queue_status="not_queued", sort="name", limit=50, offset=0)
        self.assertEqual(3, combined_payload["total"])

    def test_catalog_backfill_enqueue_does_not_call_spotify(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed"
        ) as refresh_mock, patch("backend.app.main.run_spotify_catalog_backfill") as run_mock:
            client = TestClient(app)
            response = client.post(
                "/debug/spotify/catalog-backfill/enqueue",
                json={"items": [{"entity_type": "track", "spotify_id": "t1", "reason": "visible", "priority": 80}]},
            )
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual(1, body["enqueued"])
        refresh_mock.assert_not_called()
        run_mock.assert_not_called()

    def test_catalog_backfill_queue_repair_endpoint_does_not_call_spotify(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-repair-endpoint", 2, "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-repair-endpoint", "track-1", "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("album", "alb-repair-endpoint", "seed", 50, "done", "2026-04-28T00:00:00Z", 0),
            )
            connection.commit()

        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed"
        ) as refresh_mock, patch("backend.app.main.run_spotify_catalog_backfill") as run_mock:
            client = TestClient(app)
            response = client.post("/debug/spotify/catalog-backfill/queue/repair")
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual(1, body["repaired_to_pending"])
        self.assertEqual(0, body["repaired_to_done"])
        refresh_mock.assert_not_called()
        run_mock.assert_not_called()

    def test_enqueue_does_not_mutate_analysis_track_map(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track A",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        enqueue_spotify_catalog_backfill_items(
            items=[{"entity_type": "track", "spotify_id": "t-enqueue", "reason": "visible", "priority": 80}]
        )

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_queue_repair_does_not_mutate_analysis_track_map(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track B", "track b"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track B",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-repair-map", 2, "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-repair-map", "track-1", "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("album", "alb-repair-map", "seed", 50, "done", "2026-04-28T00:00:00Z", 0),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        _ = repair_spotify_catalog_backfill_queue_statuses()

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_catalog_backfill_runs_endpoint_lists_recent_first(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_run (
                  started_at, completed_at, market, status, tracks_seen, tracks_fetched, tracks_upserted,
                  albums_seen, albums_fetched, album_tracks_upserted, skipped, errors, requests_total,
                  requests_success, requests_429, requests_failed, initial_request_delay_seconds,
                  final_request_delay_seconds, effective_requests_per_minute, peak_requests_last_30_seconds,
                  max_retry_after_seconds, has_more, last_error, warnings_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "2026-04-21T12:00:00Z",
                    "2026-04-21T12:01:00Z",
                    "US",
                    "ok",
                    10,
                    10,
                    10,
                    4,
                    4,
                    8,
                    0,
                    0,
                    20,
                    20,
                    0,
                    0,
                    0.5,
                    0.5,
                    120.0,
                    5,
                    0.0,
                    0,
                    None,
                    json.dumps(["track batch endpoint forbidden; used single-track fallback"]),
                ),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_run (
                  started_at, completed_at, market, status, tracks_seen, tracks_fetched, tracks_upserted,
                  albums_seen, albums_fetched, album_tracks_upserted, skipped, errors, requests_total,
                  requests_success, requests_429, requests_failed, initial_request_delay_seconds,
                  final_request_delay_seconds, effective_requests_per_minute, peak_requests_last_30_seconds,
                  max_retry_after_seconds, has_more, last_error, warnings_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "2026-04-21T13:00:00Z",
                    "2026-04-21T13:02:00Z",
                    "US",
                    "failed",
                    15,
                    12,
                    12,
                    6,
                    5,
                    10,
                    2,
                    1,
                    30,
                    25,
                    2,
                    3,
                    0.5,
                    0.9,
                    80.0,
                    8,
                    1.0,
                    1,
                    "failed run",
                    json.dumps([]),
                ),
            )
            connection.commit()

        with patch("backend.app.main._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            response = client.get("/debug/spotify/catalog-backfill/runs?limit=20&offset=0")
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual(2, body["total"])
        self.assertEqual(2, len(body["items"]))
        self.assertEqual("2026-04-21T13:00:00Z", body["items"][0]["started_at"])
        self.assertEqual("failed", body["items"][0]["status"])
        self.assertEqual(0, body["items"][0]["warnings_count"])
        self.assertEqual("2026-04-21T12:00:00Z", body["items"][1]["started_at"])
        self.assertEqual("ok", body["items"][1]["status"])
        self.assertEqual(1, body["items"][1]["warnings_count"])
        self.assertIn("track batch endpoint forbidden; used single-track fallback", body["items"][1]["warnings"])

    def test_catalog_backfill_coverage_counts(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_1 = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track One", "track one"),
                ).lastrowid
            )
            release_track_2 = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track Two", "track two"),
                ).lastrowid
            )
            source_track_1 = int(
                connection.execute(
                    "INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "track-1", "spotify:track:track-1", "Track One", "{}"),
                ).lastrowid
            )
            source_track_2 = int(
                connection.execute(
                    "INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "track-2", "spotify:track:track-2", "Track Two", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_track_map (
                  source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_track_1, release_track_1),
            )
            connection.execute(
                """
                INSERT INTO source_track_map (
                  source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_track_2, release_track_2),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, duration_ms, fetched_at, last_status
                ) VALUES (?, ?, ?, ?)
                """,
                ("track-1", 123000, "2026-04-22T12:00:00Z", "ok"),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, duration_ms, fetched_at, last_status
                ) VALUES (?, ?, ?, ?)
                """,
                ("track-2", None, "2026-04-22T12:00:00Z", "ok"),
            )

            release_album_1 = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Album One", "album one"),
                ).lastrowid
            )
            source_album_1 = int(
                connection.execute(
                    "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "album-1", "spotify:album:album-1", "Album One", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_album_map (
                  source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_album_1, release_album_1),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, fetched_at, last_status
                ) VALUES (?, ?, ?)
                """,
                ("album-1", "2026-04-22T12:00:00Z", "ok"),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, fetched_at, last_status
                ) VALUES (?, ?, ?, ?)
                """,
                ("album-1", "track-1", "2026-04-22T12:00:00Z", "ok"),
            )

            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_run (
                  started_at, completed_at, market, status, tracks_seen, tracks_fetched, tracks_upserted,
                  albums_seen, albums_fetched, album_tracks_upserted, skipped, errors, requests_total,
                  requests_success, requests_429, requests_failed, initial_request_delay_seconds,
                  final_request_delay_seconds, effective_requests_per_minute, peak_requests_last_30_seconds,
                  max_retry_after_seconds, has_more, last_error, warnings_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "2026-04-22T12:00:00Z",
                    "2026-04-22T12:02:00Z",
                    "US",
                    "failed",
                    20,
                    18,
                    18,
                    10,
                    8,
                    30,
                    2,
                    1,
                    40,
                    35,
                    2,
                    3,
                    0.5,
                    1.1,
                    70.0,
                    9,
                    2.0,
                    1,
                    "rate limit",
                    json.dumps([]),
                ),
            )
            connection.commit()

        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed"
        ) as refresh_mock:
            client = TestClient(app)
            response = client.get("/debug/spotify/catalog-backfill/coverage")
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual(2, body["known_release_tracks"])
        self.assertEqual(2, body["track_catalog_rows"])
        self.assertEqual(1, body["track_duration_coverage_count"])
        self.assertEqual(50.0, body["track_duration_coverage_percent"])
        self.assertEqual(1, body["known_release_albums"])
        self.assertEqual(1, body["album_catalog_rows"])
        self.assertEqual(1, body["album_track_rows"])
        self.assertIsInstance(body["latest_run"], dict)
        self.assertEqual("failed", body["latest_run"]["status"])
        self.assertEqual(1, body["recent_errors_count"])
        refresh_mock.assert_not_called()

    def test_ok_run_with_fallback_warnings_persists_warnings_but_no_last_error(self) -> None:
        self._seed_source_tracks(["t1"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks") and "/v1/tracks/" not in url:
                return 403, {}, {"error": {"status": 403, "message": "forbidden batch"}}, None
            if url.endswith("/v1/tracks/t1"):
                return 200, {}, _track_payload("t1", "a1"), None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertIn("track batch endpoint forbidden; used single-track fallback", result["warnings"])
        self.assertIsNone(result["last_error"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT status, last_error, warnings_json FROM spotify_catalog_backfill_run ORDER BY id DESC LIMIT 1"
            ).fetchone()
        self.assertEqual("ok", str(row[0]))
        self.assertIsNone(row[1])
        self.assertIn("track batch endpoint forbidden; used single-track fallback", json.loads(str(row[2] or "[]")))

    def test_recent_errors_count_excludes_ok_warning_only_run(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_run (
                  started_at, completed_at, market, status, tracks_seen, tracks_fetched, tracks_upserted,
                  albums_seen, albums_fetched, album_tracks_upserted, skipped, errors, requests_total,
                  requests_success, requests_429, requests_failed, initial_request_delay_seconds,
                  final_request_delay_seconds, effective_requests_per_minute, peak_requests_last_30_seconds,
                  max_retry_after_seconds, has_more, last_error, warnings_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "2026-04-22T10:00:00Z",
                    "2026-04-22T10:01:00Z",
                    "US",
                    "ok",
                    2,
                    2,
                    2,
                    1,
                    1,
                    2,
                    0,
                    0,
                    5,
                    5,
                    0,
                    0,
                    0.5,
                    0.5,
                    120.0,
                    3,
                    0.0,
                    0,
                    None,
                    json.dumps(["album batch endpoint forbidden; used single-album fallback"]),
                ),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_run (
                  started_at, completed_at, market, status, tracks_seen, tracks_fetched, tracks_upserted,
                  albums_seen, albums_fetched, album_tracks_upserted, skipped, errors, requests_total,
                  requests_success, requests_429, requests_failed, initial_request_delay_seconds,
                  final_request_delay_seconds, effective_requests_per_minute, peak_requests_last_30_seconds,
                  max_retry_after_seconds, has_more, last_error, warnings_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "2026-04-22T11:00:00Z",
                    "2026-04-22T11:01:00Z",
                    "US",
                    "failed",
                    2,
                    1,
                    1,
                    1,
                    0,
                    0,
                    0,
                    1,
                    4,
                    2,
                    0,
                    2,
                    0.5,
                    1.0,
                    60.0,
                    2,
                    0.0,
                    1,
                    "fatal",
                    json.dumps([]),
                ),
            )
            connection.commit()

        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed"
        ) as refresh_mock:
            client = TestClient(app)
            response = client.get("/debug/spotify/catalog-backfill/coverage")
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual(1, body["recent_errors_count"])
        refresh_mock.assert_not_called()

    def test_catalog_access_probe_success_shape(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed",
            return_value={"access_token": "token"},
        ), patch(
            "backend.app.main._spotify_catalog_probe_track_request",
            return_value=(True, 200, "Catalog access succeeded.", {"id": "track-1", "name": "Track 1"}),
        ) as probe_mock:
            client = TestClient(app)
            response = client.post(
                "/debug/spotify/catalog-access-probe",
                json={"spotify_track_id": "track-1", "market": "US"},
            )
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual("track-1", body["spotify_track_id"])
        self.assertEqual("US", body["market"])
        self.assertEqual(200, body["status"])
        self.assertIsInstance(body["body"], dict)
        probe_mock.assert_called_once()
        with closing(sqlite3.connect(self.db_path)) as connection:
            run_count = int(connection.execute("SELECT count(*) FROM spotify_catalog_backfill_run").fetchone()[0])
            track_catalog_count = int(connection.execute("SELECT count(*) FROM spotify_track_catalog").fetchone()[0])
            album_catalog_count = int(connection.execute("SELECT count(*) FROM spotify_album_catalog").fetchone()[0])
            album_track_count = int(connection.execute("SELECT count(*) FROM spotify_album_track").fetchone()[0])
        self.assertEqual(0, run_count)
        self.assertEqual(0, track_catalog_count)
        self.assertEqual(0, album_catalog_count)
        self.assertEqual(0, album_track_count)

    def test_catalog_access_probe_403_shape_includes_body_message(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed",
            return_value={"access_token": "token"},
        ), patch(
            "backend.app.main._spotify_catalog_probe_track_request",
            return_value=(False, 403, "Forbidden by Spotify policy.", {"error": {"status": 403, "message": "Forbidden"}}),
        ):
            client = TestClient(app)
            response = client.post(
                "/debug/spotify/catalog-access-probe",
                json={"spotify_track_id": "track-1", "market": "US"},
            )
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertFalse(body["ok"])
        self.assertEqual(403, body["status"])
        self.assertIn("Forbidden", body["message"])
        self.assertIsInstance(body["body"], dict)
        self.assertIn("error", body["body"])

    def test_catalog_access_probe_missing_auth_returns_stable_401(self) -> None:
        with patch(
            "backend.app.main._require_local_data_session",
            side_effect=HTTPException(status_code=401, detail="Not authenticated with Spotify."),
        ), patch("backend.app.main._spotify_catalog_probe_track_request") as probe_mock:
            client = TestClient(app)
            response = client.post("/debug/spotify/catalog-access-probe", json={"spotify_track_id": "track-1", "market": "US"})
        self.assertEqual(401, response.status_code)
        body = response.json()
        self.assertFalse(body["ok"])
        self.assertEqual("unauthenticated", body["status"])
        self.assertEqual("spotify_not_authenticated", body["error"]["code"])
        self.assertEqual("Not authenticated with Spotify.", body["error"]["message"])
        probe_mock.assert_not_called()

    def test_catalog_access_probe_no_token_leakage(self) -> None:
        secret_token = "super-secret-token-should-not-leak"
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed",
            return_value={"access_token": secret_token},
        ), patch(
            "backend.app.main._spotify_catalog_probe_track_request",
            return_value=(False, 403, "Forbidden by Spotify policy.", {"error": {"status": 403, "message": "Forbidden"}}),
        ):
            client = TestClient(app)
            response = client.post(
                "/debug/spotify/catalog-access-probe",
                json={"spotify_track_id": "track-1", "market": "US"},
            )
        self.assertEqual(200, response.status_code)
        serialized = json.dumps(response.json(), ensure_ascii=True)
        self.assertNotIn(secret_token, serialized)

    def test_catalog_access_probe_batch_constructs_ids_query_param(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed",
            return_value={"access_token": "token"},
        ), patch(
            "backend.app.main.discover_known_spotify_track_ids",
            return_value=["track-1", "track-2", "track-3"],
        ) as discover_mock, patch(
            "backend.app.main._spotify_catalog_probe_tracks_batch_request",
            return_value=(True, 200, "Catalog batch access succeeded.", {"tracks": []}),
        ) as batch_probe_mock:
            client = TestClient(app)
            response = client.post(
                "/debug/spotify/catalog-access-probe",
                json={"mode": "batch", "limit": 3, "market": "US"},
            )
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual(3, body["ids_count"])
        self.assertEqual(["track-1", "track-2", "track-3"], body["ids_sample"])
        discover_mock.assert_called_once()
        batch_probe_mock.assert_called_once()
        called_kwargs = batch_probe_mock.call_args.kwargs
        self.assertEqual(["track-1", "track-2", "track-3"], called_kwargs["spotify_track_ids"])
        self.assertEqual("US", called_kwargs["market"])

    def test_catalog_access_probe_batch_403_returns_body_message(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed",
            return_value={"access_token": "token"},
        ), patch(
            "backend.app.main.discover_known_spotify_track_ids",
            return_value=["track-1", "track-2"],
        ), patch(
            "backend.app.main._spotify_catalog_probe_tracks_batch_request",
            return_value=(False, 403, "Forbidden for batch endpoint.", {"error": {"status": 403, "message": "Forbidden"}}),
        ):
            client = TestClient(app)
            response = client.post(
                "/debug/spotify/catalog-access-probe",
                json={"mode": "batch", "limit": 2, "market": "US"},
            )
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertFalse(body["ok"])
        self.assertEqual(403, body["status"])
        self.assertIn("Forbidden", body["message"])
        self.assertIsInstance(body["body"], dict)
        self.assertEqual(2, body["ids_count"])

    def test_catalog_access_probe_batch_no_catalog_or_run_writes(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed",
            return_value={"access_token": "token"},
        ), patch(
            "backend.app.main.discover_known_spotify_track_ids",
            return_value=["track-1"],
        ), patch(
            "backend.app.main._spotify_catalog_probe_tracks_batch_request",
            return_value=(True, 200, "Catalog batch access succeeded.", {"tracks": []}),
        ):
            client = TestClient(app)
            response = client.post(
                "/debug/spotify/catalog-access-probe",
                json={"mode": "batch", "limit": 1, "market": "US"},
            )
        self.assertEqual(200, response.status_code)
        with closing(sqlite3.connect(self.db_path)) as connection:
            run_count = int(connection.execute("SELECT count(*) FROM spotify_catalog_backfill_run").fetchone()[0])
            track_catalog_count = int(connection.execute("SELECT count(*) FROM spotify_track_catalog").fetchone()[0])
            album_catalog_count = int(connection.execute("SELECT count(*) FROM spotify_album_catalog").fetchone()[0])
            album_track_count = int(connection.execute("SELECT count(*) FROM spotify_album_track").fetchone()[0])
        self.assertEqual(0, run_count)
        self.assertEqual(0, track_catalog_count)
        self.assertEqual(0, album_catalog_count)
        self.assertEqual(0, album_track_count)


if __name__ == "__main__":
    unittest.main()
