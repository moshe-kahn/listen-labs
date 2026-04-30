from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import unittest
from contextlib import closing
from pathlib import Path

from backend.app.db import apply_pending_migrations, ensure_sqlite_db


class AlbumFamilyCandidateReportWrapperTests(unittest.TestCase):
    def setUp(self) -> None:
        self.db_path = os.path.join(
            os.getcwd(),
            "backend",
            "tests",
            "_tmp_album_family_candidate_report_wrapper.sqlite3",
        )
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        os.environ["SQLITE_DB_PATH"] = self.db_path
        ensure_sqlite_db()
        apply_pending_migrations()

    def tearDown(self) -> None:
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_wrapper_prints_report_path_and_does_not_mutate_accepted_maps(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name, release_year) VALUES (?, ?, ?)",
                    ("Wrapper Album", "wrapper album", 2025),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO album_family (
                  primary_name,
                  normalized_name,
                  release_year,
                  canonical_release_album_id
                ) VALUES (?, ?, ?, ?)
                """,
                ("Wrapper Family", "wrapper family", 2025, release_album_id),
            )
            family_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
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
                  ?, ?, 'manual_override', 1.0, 'accepted', 1, 'wrapper test',
                  strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                  strftime('%Y-%m-%dT%H:%M:%fZ','now')
                )
                """,
                (release_album_id, family_id),
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

        result = subprocess.run(
            [sys.executable, "backend/scripts/run_album_family_candidate_report.py"],
            cwd=os.getcwd(),
            env=dict(os.environ),
            check=True,
            capture_output=True,
            text=True,
        )
        output_text = result.stdout.strip()

        self.assertIn("Album-family candidate report written to:", output_text)
        report_path_text = output_text.split("Album-family candidate report written to:", 1)[1].strip()
        report_path = Path(report_path_text)
        self.assertTrue(report_path.exists())

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

        if report_path.exists():
            report_path.unlink()


if __name__ == "__main__":
    unittest.main()
