from __future__ import annotations

import os
import sqlite3
import unittest
from contextlib import closing

from backend.app.db import apply_pending_migrations, ensure_sqlite_db, query_album_family_grouping_candidates


class AlbumFamilyGroupingCandidateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.db_path = os.path.join(
            os.getcwd(),
            "backend",
            "tests",
            "_tmp_album_family_grouping_candidates.sqlite3",
        )
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        os.environ["SQLITE_DB_PATH"] = self.db_path
        ensure_sqlite_db()
        apply_pending_migrations()

    def tearDown(self) -> None:
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_candidates_report_is_read_only_and_preserves_accepted_album_family_maps(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(
                connection.execute(
                    "INSERT INTO artist (canonical_name, sort_name) VALUES (?, ?)",
                    ("Artist Candidate", "artist candidate"),
                ).lastrowid
            )
            base_release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name, release_year) VALUES (?, ?, ?)",
                    ("Sunrise", "sunrise", 2020),
                ).lastrowid
            )
            deluxe_release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name, release_year) VALUES (?, ?, ?)",
                    ("Sunrise Deluxe Edition", "sunrise deluxe edition", 2021),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO album_artist (
                  release_album_id, artist_id, role, billing_index, match_method, confidence, source_basis
                ) VALUES (?, ?, 'primary', 0, 'backfill', 1.0, 'test')
                """,
                (base_release_album_id, artist_id),
            )
            connection.execute(
                """
                INSERT INTO album_artist (
                  release_album_id, artist_id, role, billing_index, match_method, confidence, source_basis
                ) VALUES (?, ?, 'primary', 0, 'backfill', 1.0, 'test')
                """,
                (deluxe_release_album_id, artist_id),
            )
            connection.execute(
                """
                INSERT INTO album_family (
                  primary_name, normalized_name, release_year, canonical_release_album_id
                ) VALUES (?, ?, ?, ?)
                """,
                ("Sunrise Family Base", "sunrise family base", 2020, base_release_album_id),
            )
            base_family_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            connection.execute(
                """
                INSERT OR REPLACE INTO album_family_map (
                  release_album_id,
                  album_family_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation,
                  created_at,
                  updated_at
                )
                VALUES (
                  ?, ?, 'manual_override', 1.0, 'accepted', 1, 'test setup',
                  strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                  strftime('%Y-%m-%dT%H:%M:%fZ','now')
                )
                """,
                (base_release_album_id, base_family_id),
            )
            connection.execute(
                """
                INSERT INTO album_family (
                  primary_name, normalized_name, release_year, canonical_release_album_id
                ) VALUES (?, ?, ?, ?)
                """,
                ("Sunrise Family Override", "sunrise family override", 2021, deluxe_release_album_id),
            )
            override_family_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            connection.execute(
                """
                INSERT OR REPLACE INTO album_family_map (
                  release_album_id,
                  album_family_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation,
                  created_at,
                  updated_at
                )
                VALUES (
                  ?, ?, 'manual_override', 1.0, 'accepted', 1, 'test setup',
                  strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                  strftime('%Y-%m-%dT%H:%M:%fZ','now')
                )
                """,
                (deluxe_release_album_id, override_family_id),
            )
            connection.commit()

            accepted_before = connection.execute(
                """
                SELECT
                  release_album_id,
                  album_family_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  COALESCE(explanation, '')
                FROM album_family_map
                WHERE status = 'accepted'
                ORDER BY release_album_id, album_family_id
                """
            ).fetchall()

        payload = query_album_family_grouping_candidates(limit=20, offset=0)

        with closing(sqlite3.connect(self.db_path)) as connection:
            accepted_after = connection.execute(
                """
                SELECT
                  release_album_id,
                  album_family_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  COALESCE(explanation, '')
                FROM album_family_map
                WHERE status = 'accepted'
                ORDER BY release_album_id, album_family_id
                """
            ).fetchall()

        self.assertEqual(accepted_before, accepted_after)
        self.assertEqual(0, payload["summary"]["mutations_applied"])
        self.assertEqual("suggested_only", payload["summary"]["candidate_status"])
        self.assertGreaterEqual(payload["summary"]["total_candidate_groups"], 1)
        self.assertGreaterEqual(payload["pagination"]["returned"], 1)
        self.assertGreaterEqual(payload["items"][0]["distinct_effective_family_count"], 2)
        first_item = payload["items"][0]
        required_fields = {
            "candidate_status",
            "candidate_group_key",
            "release_album_ids",
            "current_album_family_ids",
            "album_names",
            "album_normalized_names",
            "primary_artist_names",
            "primary_artist_ids",
            "release_years",
            "track_counts",
            "title_similarity_score",
            "title_match_reason",
            "artist_match_signal",
            "year_proximity_signal",
            "suffix_version_signal",
            "confidence_score",
            "explanation",
            "warning_flags",
            "recommended_decision",
        }
        self.assertTrue(required_fields.issubset(set(first_item.keys())))

    def test_candidates_include_required_fields_with_safe_defaults_when_evidence_missing(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            album_one_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name, release_year) VALUES (?, ?, ?)",
                    ("Moonlight Deluxe", "moonlight deluxe", None),
                ).lastrowid
            )
            album_two_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name, release_year) VALUES (?, ?, ?)",
                    ("Moonlight Expanded", "moonlight expanded", None),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO album_family (
                  primary_name, normalized_name, release_year, canonical_release_album_id
                ) VALUES (?, ?, ?, ?)
                """,
                ("Moonlight Base Family", "moonlight base family", None, album_one_id),
            )
            connection.execute(
                """
                INSERT INTO album_family (
                  primary_name, normalized_name, release_year, canonical_release_album_id
                ) VALUES (?, ?, ?, ?)
                """,
                ("Moonlight Alt Family", "moonlight alt family", None, album_two_id),
            )
            alt_family_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            connection.execute(
                """
                INSERT OR REPLACE INTO album_family_map (
                  release_album_id,
                  album_family_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation,
                  created_at,
                  updated_at
                )
                VALUES (
                  ?, ?, 'manual_override', 1.0, 'accepted', 1, 'sparse-evidence setup',
                  strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                  strftime('%Y-%m-%dT%H:%M:%fZ','now')
                )
                """,
                (album_two_id, alt_family_id),
            )
            connection.commit()

        payload = query_album_family_grouping_candidates(limit=50, offset=0)
        matching = [
            item
            for item in payload["items"]
            if "moonlight" in str(item.get("candidate_group_key") or "")
        ]
        self.assertTrue(matching)
        item = matching[0]
        self.assertEqual("suggested_only", item.get("candidate_status"))
        self.assertIsInstance(item.get("release_album_ids"), list)
        self.assertIsInstance(item.get("current_album_family_ids"), list)
        self.assertIsInstance(item.get("album_names"), list)
        self.assertIsInstance(item.get("album_normalized_names"), list)
        self.assertIsInstance(item.get("primary_artist_names"), list)
        self.assertIsInstance(item.get("primary_artist_ids"), list)
        self.assertIsInstance(item.get("release_years"), list)
        self.assertIsInstance(item.get("track_counts"), list)
        self.assertIsInstance(item.get("title_similarity_score"), float)
        self.assertIsInstance(item.get("title_match_reason"), str)
        self.assertIsInstance(item.get("artist_match_signal"), str)
        self.assertIsInstance(item.get("year_proximity_signal"), str)
        self.assertIsInstance(item.get("suffix_version_signal"), str)
        self.assertIsInstance(item.get("confidence_score"), float)
        self.assertIsInstance(item.get("explanation"), str)
        self.assertIsInstance(item.get("warning_flags"), list)
        self.assertIn(item.get("recommended_decision"), {"accept", "reject", "needs_more_evidence"})


if __name__ == "__main__":
    unittest.main()
