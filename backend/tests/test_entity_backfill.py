from __future__ import annotations

import json
import os
import sqlite3
import unittest
from contextlib import closing

from backend.app.db import (
    _normalize_fallback_artist_text,
    apply_pending_migrations,
    backfill_local_text_entities,
    backfill_spotify_source_entities,
    ensure_sqlite_db,
    insert_raw_play_event,
    merge_conservative_same_album_release_track_duplicates,
    refresh_conservative_analysis_track_links,
    suggest_conservative_analysis_track_links,
)


class EntityBackfillTests(unittest.TestCase):
    def setUp(self) -> None:
        self.db_path = os.path.join(
            os.getcwd(),
            "backend",
            "tests",
            "_tmp_entity_backfill.sqlite3",
        )
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        os.environ["SQLITE_DB_PATH"] = self.db_path
        ensure_sqlite_db()
        apply_pending_migrations()

    def tearDown(self) -> None:
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_backfill_creates_exact_spotify_entities_and_links(self) -> None:
        payload = {
            "item": {
                "id": "track-1",
                "name": "Song A",
                "uri": "spotify:track:track-1",
                "artists": [
                    {"id": "artist-1", "name": "Artist A", "uri": "spotify:artist:artist-1"},
                    {"id": "artist-2", "name": "Artist B", "uri": "spotify:artist:artist-2"},
                ],
                "album": {
                    "id": "album-1",
                    "name": "Album A",
                    "uri": "spotify:album:album-1",
                },
            }
        }
        insert_raw_play_event(
            source_type="spotify_recent",
            source_row_key="row-1",
            played_at="2026-04-18T12:00:00Z",
            ms_played=120000,
            ms_played_method="history_source",
            raw_payload_json=json.dumps(payload, sort_keys=True),
            spotify_track_uri="spotify:track:track-1",
            spotify_track_id="track-1",
            track_name_raw="Song A",
            artist_name_raw="Artist A, Artist B",
            album_name_raw="Album A",
            spotify_album_id="album-1",
            spotify_artist_ids_json=json.dumps(["artist-1", "artist-2"]),
            track_duration_ms=180000,
        )
        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="row-2",
            played_at="2026-04-18T13:00:00Z",
            ms_played=180000,
            ms_played_method="history_source",
            raw_payload_json=json.dumps(
                {
                    "master_metadata_track_name": "Song A",
                    "master_metadata_album_artist_name": "Artist A, Artist B",
                    "master_metadata_album_album_name": "Album A",
                },
                sort_keys=True,
            ),
            spotify_track_uri="spotify:track:track-1",
            spotify_track_id="track-1",
            track_name_raw="Song A",
            artist_name_raw="Artist A, Artist B",
            album_name_raw="Album A",
            spotify_album_id="album-1",
            spotify_artist_ids_json=json.dumps(["artist-1", "artist-2"]),
            track_duration_ms=180000,
        )

        result = backfill_spotify_source_entities()

        self.assertEqual(1, result["rows_scanned"])
        self.assertEqual(2, result["artists_created"])
        self.assertEqual(2, result["source_artists_created"])
        self.assertEqual(1, result["release_albums_created"])
        self.assertEqual(1, result["source_albums_created"])
        self.assertEqual(1, result["release_tracks_created"])
        self.assertEqual(1, result["source_tracks_created"])
        self.assertEqual(2, result["album_artist_links_created"])
        self.assertEqual(2, result["track_artist_links_created"])
        self.assertEqual(1, result["album_track_links_created"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_count = int(connection.execute("SELECT count(*) FROM artist").fetchone()[0])
            source_artist_count = int(connection.execute("SELECT count(*) FROM source_artist").fetchone()[0])
            track_count = int(connection.execute("SELECT count(*) FROM release_track").fetchone()[0])
            album_count = int(connection.execute("SELECT count(*) FROM release_album").fetchone()[0])
            track_artist_count = int(connection.execute("SELECT count(*) FROM track_artist").fetchone()[0])
            album_artist_count = int(connection.execute("SELECT count(*) FROM album_artist").fetchone()[0])
            album_track_count = int(connection.execute("SELECT count(*) FROM album_track").fetchone()[0])
            track_artist_rows = connection.execute(
                """
                SELECT role, billing_index, credited_as, match_method, confidence, source_basis
                FROM track_artist
                ORDER BY billing_index ASC, id ASC
                """
            ).fetchall()
            album_artist_rows = connection.execute(
                """
                SELECT role, billing_index, credited_as, match_method, confidence, source_basis
                FROM album_artist
                ORDER BY billing_index ASC, id ASC
                """
            ).fetchall()

        self.assertEqual(2, artist_count)
        self.assertEqual(2, source_artist_count)
        self.assertEqual(1, track_count)
        self.assertEqual(1, album_count)
        self.assertEqual(2, track_artist_count)
        self.assertEqual(2, album_artist_count)
        self.assertEqual(1, album_track_count)
        self.assertTrue(all(row[0] == "primary" for row in track_artist_rows))
        self.assertTrue(all(row[3] == "provider_identity" for row in track_artist_rows))
        self.assertTrue(all(row[5] == "spotify_structured_artist_ids" for row in track_artist_rows))
        self.assertTrue(all(row[2] in {"Artist A", "Artist B"} for row in track_artist_rows))
        self.assertTrue(all(row[0] == "primary" for row in album_artist_rows))
        self.assertTrue(all(row[3] == "provider_identity" for row in album_artist_rows))
        self.assertTrue(all(row[5] == "spotify_structured_artist_ids" for row in album_artist_rows))

    def test_backfill_is_idempotent(self) -> None:
        insert_raw_play_event(
            source_type="spotify_recent",
            source_row_key="row-1",
            played_at="2026-04-18T12:00:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json=json.dumps(
                {
                    "item": {
                        "artists": [{"id": "artist-1", "name": "Artist A"}],
                    }
                },
                sort_keys=True,
            ),
            spotify_track_uri="spotify:track:track-1",
            spotify_track_id="track-1",
            track_name_raw="Song A",
            artist_name_raw="Artist A",
            album_name_raw="Album A",
            spotify_album_id="album-1",
            spotify_artist_ids_json=json.dumps(["artist-1"]),
        )

        first = backfill_spotify_source_entities()
        second = backfill_spotify_source_entities()

        self.assertEqual(1, first["release_tracks_created"])
        self.assertEqual(0, second["release_tracks_created"])
        self.assertEqual(0, second["release_albums_created"])
        self.assertEqual(0, second["artists_created"])
        self.assertEqual(0, second["track_artist_links_created"])
        self.assertEqual(0, second["album_artist_links_created"])

    def test_local_text_backfill_uses_history_names_when_album_and_artist_ids_are_missing(self) -> None:
        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="history-row-1",
            played_at="2026-04-18T12:00:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json=json.dumps(
                {
                    "master_metadata_track_name": "History Song",
                    "master_metadata_album_artist_name": "History Artist",
                    "master_metadata_album_album_name": "History Album",
                    "spotify_track_uri": "spotify:track:history-track-1",
                },
                sort_keys=True,
            ),
            spotify_track_uri="spotify:track:history-track-1",
            spotify_track_id="history-track-1",
            track_name_raw="History Song",
            artist_name_raw="History Artist",
            album_name_raw="History Album",
            spotify_album_id=None,
            spotify_artist_ids_json=None,
        )

        exact = backfill_spotify_source_entities()
        local = backfill_local_text_entities()

        self.assertEqual(0, exact["release_albums_created"])
        self.assertEqual(0, exact["artists_created"])
        self.assertEqual(1, exact["release_tracks_created"])
        self.assertEqual(1, local["release_albums_created"])
        self.assertEqual(1, local["artists_created"])
        self.assertEqual(1, local["album_artist_links_created"])
        self.assertEqual(1, local["track_artist_links_created"])
        self.assertEqual(1, local["album_track_links_created"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            history_album_sources = int(
                connection.execute(
                    "SELECT count(*) FROM source_album WHERE source_name = 'history_raw'"
                ).fetchone()[0]
            )
            history_artist_sources = int(
                connection.execute(
                    "SELECT count(*) FROM source_artist WHERE source_name = 'history_raw'"
                ).fetchone()[0]
            )
            spotify_track_sources = int(
                connection.execute(
                    "SELECT count(*) FROM source_track WHERE source_name = 'spotify'"
                ).fetchone()[0]
            )
            history_track_artist = connection.execute(
                """
                SELECT role, billing_index, credited_as, match_method, confidence, source_basis
                FROM track_artist
                LIMIT 1
                """
            ).fetchone()
            history_album_artist = connection.execute(
                """
                SELECT role, billing_index, credited_as, match_method, confidence, source_basis
                FROM album_artist
                LIMIT 1
                """
            ).fetchone()

        self.assertEqual(1, history_album_sources)
        self.assertEqual(1, history_artist_sources)
        self.assertEqual(1, spotify_track_sources)
        assert history_track_artist is not None
        assert history_album_artist is not None
        self.assertEqual("primary", history_track_artist[0])
        self.assertEqual("History Artist", history_track_artist[2])
        self.assertEqual("history_raw_text", history_track_artist[3])
        self.assertEqual("artist_name_raw", history_track_artist[5])
        self.assertEqual("primary", history_album_artist[0])
        self.assertEqual("History Artist", history_album_artist[2])
        self.assertEqual("history_raw_text", history_album_artist[3])
        self.assertEqual("artist_name_raw", history_album_artist[5])

    def test_normalize_fallback_artist_text_dedupes_comma_artist(self) -> None:
        self.assertEqual("Telekinesis", _normalize_fallback_artist_text("Telekinesis, Telekinesis"))
        self.assertEqual("Telekinesis", _normalize_fallback_artist_text(" Telekinesis ,  Telekinesis "))

    def test_normalize_fallback_artist_text_uses_first_primary_artist(self) -> None:
        self.assertEqual("Brian Eno", _normalize_fallback_artist_text("Brian Eno, David Byrne"))
        self.assertEqual("", _normalize_fallback_artist_text(""))
        self.assertEqual("", _normalize_fallback_artist_text(None))

    def test_local_text_backfill_fallback_stable_key_uses_normalized_artist(self) -> None:
        base_payload = {
            "master_metadata_track_name": "History Song Key",
            "master_metadata_album_album_name": "History Album Key",
        }
        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="history-key-row-1",
            played_at="2026-04-18T12:00:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json=json.dumps(
                {**base_payload, "master_metadata_album_artist_name": "Telekinesis"},
                sort_keys=True,
            ),
            spotify_track_uri=None,
            spotify_track_id=None,
            track_name_raw="History Song Key",
            artist_name_raw="Telekinesis",
            album_name_raw="History Album Key",
            spotify_album_id=None,
            spotify_artist_ids_json=None,
        )
        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="history-key-row-2",
            played_at="2026-04-18T13:00:00Z",
            ms_played=101000,
            ms_played_method="history_source",
            raw_payload_json=json.dumps(
                {**base_payload, "master_metadata_album_artist_name": "Telekinesis, Telekinesis"},
                sort_keys=True,
            ),
            spotify_track_uri=None,
            spotify_track_id=None,
            track_name_raw="History Song Key",
            artist_name_raw="Telekinesis, Telekinesis",
            album_name_raw="History Album Key",
            spotify_album_id=None,
            spotify_artist_ids_json=None,
        )

        local = backfill_local_text_entities()
        self.assertEqual(1, local["release_tracks_created"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            source_track_count = int(
                connection.execute("SELECT count(*) FROM source_track WHERE source_name = 'history_raw'").fetchone()[0]
            )
            source_artist_labels = [
                row[0]
                for row in connection.execute(
                    "SELECT source_name_raw FROM source_artist WHERE source_name = 'history_raw' ORDER BY id ASC"
                ).fetchall()
            ]
            track_artist_credits = [
                row[0]
                for row in connection.execute(
                    "SELECT credited_as FROM track_artist ORDER BY id ASC"
                ).fetchall()
            ]
            raw_artist_values = [
                row[0]
                for row in connection.execute(
                    "SELECT artist_name_raw FROM raw_play_event ORDER BY id ASC"
                ).fetchall()
            ]
        self.assertEqual(1, source_track_count)
        self.assertIn("Telekinesis", source_artist_labels)
        self.assertEqual(1, len(source_artist_labels))
        self.assertIn("Telekinesis", track_artist_credits)
        self.assertIn("Telekinesis, Telekinesis", raw_artist_values)

    def test_spotify_id_path_behavior_unchanged_with_duplicate_artist_text(self) -> None:
        payload = {
            "item": {
                "id": "track-sp-1",
                "name": "Song SP",
                "uri": "spotify:track:track-sp-1",
                "artists": [{"id": "artist-sp-1", "name": "Telekinesis"}],
                "album": {"id": "album-sp-1", "name": "Album SP", "uri": "spotify:album:album-sp-1"},
            }
        }
        insert_raw_play_event(
            source_type="spotify_recent",
            source_row_key="spotify-row-1",
            played_at="2026-04-18T12:00:00Z",
            ms_played=120000,
            ms_played_method="history_source",
            raw_payload_json=json.dumps(payload, sort_keys=True),
            spotify_track_uri="spotify:track:track-sp-1",
            spotify_track_id="track-sp-1",
            track_name_raw="Song SP",
            artist_name_raw="Telekinesis, Telekinesis",
            album_name_raw="Album SP",
            spotify_album_id="album-sp-1",
            spotify_artist_ids_json=json.dumps(["artist-sp-1"]),
            track_duration_ms=180000,
        )

        exact = backfill_spotify_source_entities()
        self.assertEqual(1, exact["release_tracks_created"])
        self.assertEqual(1, exact["artists_created"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_names = [
                row[0]
                for row in connection.execute("SELECT canonical_name FROM artist ORDER BY id ASC").fetchall()
            ]
            analysis_track_map_count = int(connection.execute("SELECT count(*) FROM analysis_track_map").fetchone()[0])
        self.assertEqual(["Telekinesis"], artist_names)
        self.assertEqual(0, analysis_track_map_count)

    def test_exact_spotify_backfill_reuses_release_track_created_from_matching_spotify_uri(self) -> None:
        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="history-uri-row",
            played_at="2026-04-18T10:00:00Z",
            ms_played=90000,
            ms_played_method="history_source",
            raw_payload_json=json.dumps(
                {
                    "master_metadata_track_name": "Shared Song",
                    "spotify_track_uri": "spotify:track:track-shared-1",
                },
                sort_keys=True,
            ),
            spotify_track_uri="spotify:track:track-shared-1",
            spotify_track_id=None,
            track_name_raw="Shared Song",
            artist_name_raw="Shared Artist",
            album_name_raw="Shared Album",
            spotify_album_id=None,
            spotify_artist_ids_json=None,
            track_duration_ms=200000,
        )

        local = backfill_local_text_entities()

        insert_raw_play_event(
            source_type="spotify_recent",
            source_row_key="recent-id-row",
            played_at="2026-04-18T11:00:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json=json.dumps(
                {
                    "item": {
                        "id": "track-shared-1",
                        "name": "Shared Song",
                        "uri": "spotify:track:track-shared-1",
                        "artists": [{"id": "artist-shared-1", "name": "Shared Artist"}],
                    }
                },
                sort_keys=True,
            ),
            spotify_track_uri="spotify:track:track-shared-1",
            spotify_track_id="track-shared-1",
            track_name_raw="Shared Song",
            artist_name_raw="Shared Artist",
            album_name_raw="Shared Album",
            spotify_album_id=None,
            spotify_artist_ids_json=json.dumps(["artist-shared-1"]),
            track_duration_ms=200000,
        )

        exact = backfill_spotify_source_entities()

        self.assertEqual(1, local["release_tracks_created"])
        self.assertEqual(0, exact["release_tracks_created"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_count = int(connection.execute("SELECT count(*) FROM release_track").fetchone()[0])
            source_track_count = int(connection.execute("SELECT count(*) FROM source_track").fetchone()[0])
            track_maps = connection.execute(
                """
                SELECT st.source_name, stm.match_method, stm.confidence, stm.release_track_id
                FROM source_track_map stm
                JOIN source_track st
                  ON st.id = stm.source_track_id
                ORDER BY st.source_name ASC
                """
            ).fetchall()

        self.assertEqual(1, release_track_count)
        self.assertEqual(2, source_track_count)
        self.assertEqual(2, len(track_maps))
        self.assertEqual({row[3] for row in track_maps}, {track_maps[0][3]})
        self.assertEqual(
            {
                (row[0], row[1], row[2])
                for row in track_maps
            },
            {
                ("spotify", "spotify_id_uri_equivalent", 1.0),
                ("spotify_uri", "spotify_track_uri", 1.0),
            },
        )

    def test_local_text_backfill_reuses_exact_spotify_release_track_via_uri_equivalence(self) -> None:
        insert_raw_play_event(
            source_type="spotify_recent",
            source_row_key="recent-row-1",
            played_at="2026-04-18T12:00:00Z",
            ms_played=110000,
            ms_played_method="history_source",
            raw_payload_json=json.dumps(
                {
                    "item": {
                        "id": "track-shared-2",
                        "name": "Shared Song 2",
                        "uri": "spotify:track:track-shared-2",
                        "artists": [{"id": "artist-2", "name": "Artist Two"}],
                    }
                },
                sort_keys=True,
            ),
            spotify_track_uri="spotify:track:track-shared-2",
            spotify_track_id="track-shared-2",
            track_name_raw="Shared Song 2",
            artist_name_raw="Artist Two",
            album_name_raw="Album Two",
            spotify_album_id=None,
            spotify_artist_ids_json=json.dumps(["artist-2"]),
            track_duration_ms=210000,
        )

        exact = backfill_spotify_source_entities()

        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="history-row-2",
            played_at="2026-04-18T13:00:00Z",
            ms_played=95000,
            ms_played_method="history_source",
            raw_payload_json=json.dumps(
                {
                    "master_metadata_track_name": "Shared Song 2",
                    "spotify_track_uri": "spotify:track:track-shared-2",
                },
                sort_keys=True,
            ),
            spotify_track_uri="spotify:track:track-shared-2",
            spotify_track_id=None,
            track_name_raw="Shared Song 2",
            artist_name_raw="Artist Two",
            album_name_raw="Album Two",
            spotify_album_id=None,
            spotify_artist_ids_json=None,
            track_duration_ms=210000,
        )

        local = backfill_local_text_entities()

        self.assertEqual(1, exact["release_tracks_created"])
        self.assertEqual(0, local["release_tracks_created"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_count = int(connection.execute("SELECT count(*) FROM release_track").fetchone()[0])
            source_track_count = int(connection.execute("SELECT count(*) FROM source_track").fetchone()[0])
            track_maps = connection.execute(
                """
                SELECT st.source_name, stm.match_method, stm.release_track_id
                FROM source_track_map stm
                JOIN source_track st
                  ON st.id = stm.source_track_id
                ORDER BY st.source_name ASC
                """
            ).fetchall()

        self.assertEqual(1, release_track_count)
        self.assertEqual(2, source_track_count)
        self.assertEqual(
            {
                (row[0], row[1])
                for row in track_maps
            },
            {
                ("spotify", "provider_identity"),
                ("spotify_uri", "spotify_id_uri_equivalent"),
            },
        )
        self.assertEqual({row[2] for row in track_maps}, {track_maps[0][2]})

    def test_same_title_different_spotify_ids_stay_separate_release_tracks(self) -> None:
        for row_key, track_id in (("row-1", "track-a"), ("row-2", "track-b")):
            insert_raw_play_event(
                source_type="spotify_recent",
                source_row_key=row_key,
                played_at="2026-04-18T12:00:00Z",
                ms_played=100000,
                ms_played_method="history_source",
                raw_payload_json=json.dumps(
                    {
                        "item": {
                            "id": track_id,
                            "name": "Same Title",
                            "uri": f"spotify:track:{track_id}",
                            "artists": [{"id": "artist-same", "name": "Artist Same"}],
                        }
                    },
                    sort_keys=True,
                ),
                spotify_track_uri=f"spotify:track:{track_id}",
                spotify_track_id=track_id,
                track_name_raw="Same Title",
                artist_name_raw="Artist Same",
                album_name_raw="Album Same",
                spotify_album_id=None,
                spotify_artist_ids_json=json.dumps(["artist-same"]),
                track_duration_ms=180000,
            )

        result = backfill_spotify_source_entities()

        self.assertEqual(2, result["release_tracks_created"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_tracks = connection.execute(
                "SELECT id, primary_name, normalized_name FROM release_track ORDER BY id ASC"
            ).fetchall()
            source_track_maps = connection.execute(
                "SELECT count(*) FROM source_track_map"
            ).fetchone()

        self.assertEqual(2, len(release_tracks))
        self.assertEqual({"Same Title"}, {row[1] for row in release_tracks})
        self.assertEqual({"same title"}, {row[2] for row in release_tracks})
        assert source_track_maps is not None
        self.assertEqual(2, int(source_track_maps[0]))

    def test_migrations_create_conservative_track_grouping_tables(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            table_names = {
                row[0]
                for row in connection.execute(
                    """
                    SELECT name
                    FROM sqlite_master
                    WHERE type = 'table'
                    """
                ).fetchall()
            }

        self.assertIn("analysis_track", table_names)
        self.assertIn("analysis_track_map", table_names)
        self.assertIn("track_relationship", table_names)

    def test_suggest_conservative_analysis_track_links_groups_same_title_and_primary_artist(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(
                connection.execute(
                    "INSERT INTO artist (canonical_name, sort_name) VALUES (?, ?)",
                    ("Artist A", "artist a"),
                ).lastrowid
            )
            first_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Song A", "song a"),
                ).lastrowid
            )
            second_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Song A", "song a"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (first_track_id, artist_id),
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (second_track_id, artist_id),
            )
            connection.commit()

        result = suggest_conservative_analysis_track_links()

        self.assertEqual(1, result["groups_considered"])
        self.assertEqual(1, result["groups_suggested"])
        self.assertEqual(1, result["analysis_tracks_created"])
        self.assertEqual(2, result["analysis_track_maps_created"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            analysis_tracks = connection.execute(
                "SELECT primary_name, grouping_note FROM analysis_track"
            ).fetchall()
            analysis_track_maps = connection.execute(
                """
                SELECT match_method, confidence, status, explanation
                FROM analysis_track_map
                ORDER BY release_track_id ASC
                """
            ).fetchall()

        self.assertEqual(1, len(analysis_tracks))
        self.assertEqual("Song A", analysis_tracks[0][0])
        self.assertTrue(str(analysis_tracks[0][1]).startswith("conservative_exact_title_primary_artist:"))
        self.assertIn("|song a", str(analysis_tracks[0][1]))
        self.assertEqual(2, len(analysis_track_maps))
        self.assertTrue(all(row[0] == "song_family_title_primary_artist" for row in analysis_track_maps))
        self.assertTrue(all(float(row[1]) == 0.9 for row in analysis_track_maps))
        self.assertTrue(all(row[2] == "suggested" for row in analysis_track_maps))

    def test_suggest_conservative_analysis_track_links_groups_groupable_variants(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(
                connection.execute(
                    "INSERT INTO artist (canonical_name, sort_name) VALUES (?, ?)",
                    ("Artist B", "artist b"),
                ).lastrowid
            )
            first_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Song B", "song b"),
                ).lastrowid
            )
            second_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Song B - 2015 Remaster", "song b - 2015 remaster"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (first_track_id, artist_id),
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (second_track_id, artist_id),
            )
            connection.commit()

        result = suggest_conservative_analysis_track_links()

        self.assertEqual(1, result["groups_considered"])
        self.assertEqual(1, result["groups_suggested"])
        self.assertEqual(1, result["analysis_tracks_created"])
        self.assertEqual(2, result["analysis_track_maps_created"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            analysis_track_maps = connection.execute(
                "SELECT confidence FROM analysis_track_map ORDER BY release_track_id ASC"
            ).fetchall()

        self.assertTrue(all(float(row[0]) == 0.82 for row in analysis_track_maps))

    def test_suggest_conservative_analysis_track_links_groups_generic_versions(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(
                connection.execute(
                    "INSERT INTO artist (canonical_name, sort_name) VALUES (?, ?)",
                    ("Artist BV", "artist bv"),
                ).lastrowid
            )
            first_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Song BV", "song bv"),
                ).lastrowid
            )
            second_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Song BV - Album Version", "song bv - album version"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (first_track_id, artist_id),
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (second_track_id, artist_id),
            )
            connection.commit()

        result = suggest_conservative_analysis_track_links()

        self.assertEqual(1, result["groups_considered"])
        self.assertEqual(1, result["groups_suggested"])
        self.assertEqual(1, result["analysis_tracks_created"])
        self.assertEqual(2, result["analysis_track_maps_created"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            analysis_track_maps = connection.execute(
                "SELECT confidence FROM analysis_track_map ORDER BY release_track_id ASC"
            ).fetchall()

        self.assertTrue(all(float(row[0]) == 0.8 for row in analysis_track_maps))

    def test_suggest_conservative_analysis_track_links_skips_attributed_edits(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(
                connection.execute(
                    "INSERT INTO artist (canonical_name, sort_name) VALUES (?, ?)",
                    ("Artist BE", "artist be"),
                ).lastrowid
            )
            first_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Song BE", "song be"),
                ).lastrowid
            )
            second_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Song BE - Local Natives Edit", "song be - local natives edit"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (first_track_id, artist_id),
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (second_track_id, artist_id),
            )
            connection.commit()

        result = suggest_conservative_analysis_track_links()

        self.assertEqual(0, result["groups_considered"])
        self.assertEqual(0, result["groups_suggested"])
        self.assertEqual(0, result["analysis_tracks_created"])
        self.assertEqual(0, result["analysis_track_maps_created"])

    def test_suggest_conservative_analysis_track_links_groups_instrumentals(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(
                connection.execute(
                    "INSERT INTO artist (canonical_name, sort_name) VALUES (?, ?)",
                    ("Artist BI", "artist bi"),
                ).lastrowid
            )
            first_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Song BI", "song bi"),
                ).lastrowid
            )
            second_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Song BI - Instrumental", "song bi - instrumental"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (first_track_id, artist_id),
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (second_track_id, artist_id),
            )
            connection.commit()

        result = suggest_conservative_analysis_track_links()

        self.assertEqual(1, result["groups_considered"])
        self.assertEqual(1, result["groups_suggested"])
        self.assertEqual(1, result["analysis_tracks_created"])
        self.assertEqual(2, result["analysis_track_maps_created"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            analysis_track_maps = connection.execute(
                "SELECT confidence FROM analysis_track_map ORDER BY release_track_id ASC"
            ).fetchall()

        self.assertTrue(all(float(row[0]) == 0.77 for row in analysis_track_maps))

    def test_suggest_conservative_analysis_track_links_groups_sessions(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(
                connection.execute(
                    "INSERT INTO artist (canonical_name, sort_name) VALUES (?, ?)",
                    ("Artist BS", "artist bs"),
                ).lastrowid
            )
            first_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Song BS", "song bs"),
                ).lastrowid
            )
            second_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Song BS - Mahogany Sessions", "song bs - mahogany sessions"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (first_track_id, artist_id),
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (second_track_id, artist_id),
            )
            connection.commit()

        result = suggest_conservative_analysis_track_links()

        self.assertEqual(1, result["groups_considered"])
        self.assertEqual(1, result["groups_suggested"])
        self.assertEqual(1, result["analysis_tracks_created"])
        self.assertEqual(2, result["analysis_track_maps_created"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            analysis_track_maps = connection.execute(
                "SELECT confidence FROM analysis_track_map ORDER BY release_track_id ASC"
            ).fetchall()

        self.assertTrue(all(float(row[0]) == 0.77 for row in analysis_track_maps))

    def test_suggest_conservative_analysis_track_links_groups_featured_credit_suffixes(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(
                connection.execute(
                    "INSERT INTO artist (canonical_name, sort_name) VALUES (?, ?)",
                    ("Artist BF", "artist bf"),
                ).lastrowid
            )
            first_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Song BF", "song bf"),
                ).lastrowid
            )
            second_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Song BF (feat. Guest)", "song bf (feat. guest)"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (first_track_id, artist_id),
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (second_track_id, artist_id),
            )
            connection.commit()

        result = suggest_conservative_analysis_track_links()

        self.assertEqual(1, result["groups_considered"])
        self.assertEqual(1, result["groups_suggested"])
        self.assertEqual(1, result["analysis_tracks_created"])
        self.assertEqual(2, result["analysis_track_maps_created"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            analysis_track_maps = connection.execute(
                "SELECT confidence FROM analysis_track_map ORDER BY release_track_id ASC"
            ).fetchall()

        self.assertTrue(all(float(row[0]) == 0.88 for row in analysis_track_maps))

    def test_suggest_conservative_analysis_track_links_skips_remixes(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(
                connection.execute(
                    "INSERT INTO artist (canonical_name, sort_name) VALUES (?, ?)",
                    ("Artist B2", "artist b2"),
                ).lastrowid
            )
            first_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Song B2", "song b2"),
                ).lastrowid
            )
            second_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Song B2 - Remix", "song b2 - remix"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (first_track_id, artist_id),
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (second_track_id, artist_id),
            )
            connection.commit()

        result = suggest_conservative_analysis_track_links()

        self.assertEqual(0, result["groups_considered"])
        self.assertEqual(0, result["groups_suggested"])
        self.assertEqual(0, result["analysis_tracks_created"])
        self.assertEqual(0, result["analysis_track_maps_created"])

    def test_suggest_conservative_analysis_track_links_is_idempotent(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(
                connection.execute(
                    "INSERT INTO artist (canonical_name, sort_name) VALUES (?, ?)",
                    ("Artist C", "artist c"),
                ).lastrowid
            )
            for _ in range(2):
                release_track_id = int(
                    connection.execute(
                        "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                        ("Song C", "song c"),
                    ).lastrowid
                )
                connection.execute(
                    "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                    (release_track_id, artist_id),
                )
            connection.commit()

        first = suggest_conservative_analysis_track_links()
        second = suggest_conservative_analysis_track_links()

        self.assertEqual(1, first["analysis_tracks_created"])
        self.assertEqual(2, first["analysis_track_maps_created"])
        self.assertEqual(0, second["analysis_tracks_created"])
        self.assertEqual(0, second["analysis_track_maps_created"])

    def test_merge_conservative_same_album_release_track_duplicates_merges_safe_case(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(
                connection.execute(
                    "INSERT INTO artist (canonical_name, sort_name) VALUES (?, ?)",
                    ("Artist D", "artist d"),
                ).lastrowid
            )
            album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Album D", "album d"),
                ).lastrowid
            )
            winner_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Song D", "song d"),
                ).lastrowid
            )
            loser_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Song D", "song d"),
                ).lastrowid
            )
            for release_track_id in (winner_track_id, loser_track_id):
                connection.execute(
                    "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                    (release_track_id, artist_id),
                )
                connection.execute(
                    "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                    (album_id, release_track_id),
                )
            source_track_winner_id = int(
                connection.execute(
                    "INSERT INTO source_track (source_name, external_id, source_name_raw) VALUES (?, ?, ?)",
                    ("spotify", "spotify-track-d-1", "Song D"),
                ).lastrowid
            )
            source_track_loser_id = int(
                connection.execute(
                    "INSERT INTO source_track (source_name, external_id, source_name_raw) VALUES (?, ?, ?)",
                    ("spotify", "spotify-track-d-2", "Song D"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_track_map (
                  source_track_id, release_track_id, match_method, confidence, status, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 'Exact Spotify track ID backfill')
                """,
                (source_track_winner_id, winner_track_id),
            )
            connection.execute(
                """
                INSERT INTO source_track_map (
                  source_track_id, release_track_id, match_method, confidence, status, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 'Exact Spotify track ID backfill')
                """,
                (source_track_loser_id, loser_track_id),
            )
            connection.commit()

        result = merge_conservative_same_album_release_track_duplicates()

        self.assertEqual(1, result["groups_considered"])
        self.assertEqual(1, result["groups_merged"])
        self.assertEqual(1, result["release_tracks_deleted"])
        self.assertEqual(1, result["source_track_maps_repointed"])
        self.assertEqual(1, result["merge_logs_created"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            remaining_release_tracks = connection.execute(
                "SELECT id FROM release_track ORDER BY id ASC"
            ).fetchall()
            source_track_maps = connection.execute(
                """
                SELECT source_track_id, release_track_id, match_method, confidence
                FROM source_track_map
                ORDER BY source_track_id ASC
                """
            ).fetchall()
            merge_logs = connection.execute(
                """
                SELECT obsolete_release_track_id, canonical_release_track_id, match_method, confidence, status
                FROM release_track_merge_log
                """
            ).fetchall()

        self.assertEqual([(winner_track_id,)], remaining_release_tracks)
        self.assertEqual(
            [
                (source_track_winner_id, winner_track_id, "provider_identity", 1.0),
                (source_track_loser_id, winner_track_id, "same_album_exact_title_primary_artist", 0.95),
            ],
            source_track_maps,
        )
        self.assertEqual(
            [(loser_track_id, winner_track_id, "same_album_exact_title_primary_artist", 0.95, "accepted")],
            merge_logs,
        )

    def test_merge_conservative_same_album_release_track_duplicates_merges_same_album_variant_titles(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(
                connection.execute(
                    "INSERT INTO artist (canonical_name, sort_name) VALUES (?, ?)",
                    ("Artist E", "artist e"),
                ).lastrowid
            )
            album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Album E", "album e"),
                ).lastrowid
            )
            for external_id in ("spotify-track-e-1", "spotify-track-e-2"):
                if external_id.endswith("-1"):
                    release_track_name = "Song E - Live"
                    normalized_name = "song e - live"
                else:
                    release_track_name = "Song E - Live"
                    normalized_name = "song e - live"
                release_track_id = int(
                    connection.execute(
                        "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                        (release_track_name, normalized_name),
                    ).lastrowid
                )
                source_track_id = int(
                    connection.execute(
                        "INSERT INTO source_track (source_name, external_id, source_name_raw) VALUES (?, ?, ?)",
                        ("spotify", external_id, release_track_name),
                    ).lastrowid
                )
                connection.execute(
                    "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                    (release_track_id, artist_id),
                )
                connection.execute(
                    "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                    (album_id, release_track_id),
                )
                connection.execute(
                    """
                    INSERT INTO source_track_map (
                      source_track_id, release_track_id, match_method, confidence, status, explanation
                    ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 'Exact Spotify track ID backfill')
                    """,
                    (source_track_id, release_track_id),
                )
            connection.commit()

        result = merge_conservative_same_album_release_track_duplicates()

        self.assertEqual(1, result["groups_considered"])
        self.assertEqual(1, result["groups_merged"])
        self.assertEqual(1, result["release_tracks_deleted"])
        self.assertEqual(1, result["merge_logs_created"])

    def test_merge_conservative_same_album_release_track_duplicates_is_idempotent(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(
                connection.execute(
                    "INSERT INTO artist (canonical_name, sort_name) VALUES (?, ?)",
                    ("Artist F", "artist f"),
                ).lastrowid
            )
            album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Album F", "album f"),
                ).lastrowid
            )
            for external_id in ("spotify-track-f-1", "spotify-track-f-2"):
                release_track_id = int(
                    connection.execute(
                        "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                        ("Song F", "song f"),
                    ).lastrowid
                )
                source_track_id = int(
                    connection.execute(
                        "INSERT INTO source_track (source_name, external_id, source_name_raw) VALUES (?, ?, ?)",
                        ("spotify", external_id, "Song F"),
                    ).lastrowid
                )
                connection.execute(
                    "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                    (release_track_id, artist_id),
                )
                connection.execute(
                    "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                    (album_id, release_track_id),
                )
                connection.execute(
                    """
                    INSERT INTO source_track_map (
                      source_track_id, release_track_id, match_method, confidence, status, explanation
                    ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 'Exact Spotify track ID backfill')
                    """,
                    (source_track_id, release_track_id),
                )
            connection.commit()

        first = merge_conservative_same_album_release_track_duplicates()
        second = merge_conservative_same_album_release_track_duplicates()

        self.assertEqual(1, first["groups_merged"])
        self.assertEqual(1, first["release_tracks_deleted"])
        self.assertEqual(0, second["groups_merged"])
        self.assertEqual(0, second["release_tracks_deleted"])

    def test_refresh_conservative_analysis_track_links_rebuilds_stale_suggestions(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(
                connection.execute(
                    "INSERT INTO artist (canonical_name, sort_name) VALUES (?, ?)",
                    ("Artist G", "artist g"),
                ).lastrowid
            )
            first_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Song G", "song g"),
                ).lastrowid
            )
            second_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Song G", "song g"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (first_track_id, artist_id),
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (second_track_id, artist_id),
            )
            connection.commit()

        first = suggest_conservative_analysis_track_links()
        self.assertEqual(1, first["analysis_tracks_created"])
        self.assertEqual(2, first["analysis_track_maps_created"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                "DELETE FROM analysis_track_map WHERE release_track_id = ?",
                (second_track_id,),
            )
            connection.commit()

        refreshed = refresh_conservative_analysis_track_links()

        self.assertEqual(1, refreshed["suggested_maps_deleted"])
        self.assertEqual(1, refreshed["analysis_tracks_deleted"])
        self.assertEqual(1, refreshed["analysis_tracks_created"])
        self.assertEqual(2, refreshed["analysis_track_maps_created"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            analysis_tracks = connection.execute(
                "SELECT count(*) FROM analysis_track"
            ).fetchone()
            analysis_track_maps = connection.execute(
                "SELECT count(*) FROM analysis_track_map"
            ).fetchone()

        assert analysis_tracks is not None
        assert analysis_track_maps is not None
        self.assertEqual(1, int(analysis_tracks[0]))
        self.assertEqual(2, int(analysis_track_maps[0]))


if __name__ == "__main__":
    unittest.main()
