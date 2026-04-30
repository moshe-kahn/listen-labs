from __future__ import annotations

import os
import sqlite3
import unittest
from contextlib import closing

from backend.app.db import (
    MIGRATIONS,
    apply_pending_migrations,
    ensure_sqlite_db,
    execute_sql,
    get_effective_album_family_id,
    get_schema_version,
    set_schema_version,
)


class AlbumFamilyMigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.db_path = os.path.join(
            os.getcwd(),
            "backend",
            "tests",
            "_tmp_album_family_migration.sqlite3",
        )
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        os.environ["SQLITE_DB_PATH"] = self.db_path

    def tearDown(self) -> None:
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def _apply_migrations_through(self, target_version: int) -> None:
        ensure_sqlite_db()
        for version in range(1, target_version + 1):
            execute_sql(MIGRATIONS[version])
            set_schema_version(version)

    def test_album_family_migration_backfills_one_family_per_release_album(self) -> None:
        self._apply_migrations_through(16)

        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name, release_year) VALUES (?, ?, ?)",
                ("Album A", "album a", 2020),
            )
            connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name, release_year) VALUES (?, ?, ?)",
                ("Album B (Deluxe)", "album b deluxe", 2021),
            )
            connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name, release_year) VALUES (?, ?, ?)",
                ("Album C", "album c", None),
            )
            connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name, release_year) VALUES (?, ?, ?)",
                ("", None, None),
            )
            connection.commit()

        execute_sql(MIGRATIONS[17])
        set_schema_version(17)

        with closing(sqlite3.connect(self.db_path)) as connection:
            release_count = int(connection.execute("SELECT count(*) FROM release_album").fetchone()[0])
            family_count = int(connection.execute("SELECT count(*) FROM album_family").fetchone()[0])
            mapped_release_count = int(
                connection.execute(
                    "SELECT count(DISTINCT release_album_id) FROM album_family_map"
                ).fetchone()[0]
            )
            release_without_map = int(
                connection.execute(
                    """
                    SELECT count(*)
                    FROM release_album ra
                    LEFT JOIN album_family_map afm ON afm.release_album_id = ra.id
                    WHERE afm.release_album_id IS NULL
                    """
                ).fetchone()[0]
            )
            release_with_non_singleton_maps = int(
                connection.execute(
                    """
                    SELECT count(*)
                    FROM (
                      SELECT release_album_id
                      FROM album_family_map
                      GROUP BY release_album_id
                      HAVING count(*) != 1
                    )
                    """
                ).fetchone()[0]
            )
            dangling_release_refs = int(
                connection.execute(
                    """
                    SELECT count(*)
                    FROM album_family_map afm
                    LEFT JOIN release_album ra ON ra.id = afm.release_album_id
                    WHERE ra.id IS NULL
                    """
                ).fetchone()[0]
            )
            dangling_family_refs = int(
                connection.execute(
                    """
                    SELECT count(*)
                    FROM album_family_map afm
                    LEFT JOIN album_family af ON af.id = afm.album_family_id
                    WHERE af.id IS NULL
                    """
                ).fetchone()[0]
            )

        self.assertEqual(release_count, family_count)
        self.assertEqual(release_count, mapped_release_count)
        self.assertEqual(0, release_without_map)
        self.assertEqual(0, release_with_non_singleton_maps)
        self.assertEqual(0, dangling_release_refs)
        self.assertEqual(0, dangling_family_refs)

    def test_album_family_migration_handles_empty_release_album_table(self) -> None:
        self._apply_migrations_through(16)

        with closing(sqlite3.connect(self.db_path)) as connection:
            release_count_before = int(connection.execute("SELECT count(*) FROM release_album").fetchone()[0])
        self.assertEqual(0, release_count_before)

        execute_sql(MIGRATIONS[17])
        set_schema_version(17)

        with closing(sqlite3.connect(self.db_path)) as connection:
            release_count = int(connection.execute("SELECT count(*) FROM release_album").fetchone()[0])
            family_count = int(connection.execute("SELECT count(*) FROM album_family").fetchone()[0])
            map_count = int(connection.execute("SELECT count(*) FROM album_family_map").fetchone()[0])

        self.assertEqual(0, release_count)
        self.assertEqual(0, family_count)
        self.assertEqual(0, map_count)

    def test_apply_pending_migrations_is_no_op_at_target_schema_version(self) -> None:
        ensure_sqlite_db()
        apply_pending_migrations()
        target_version = max(MIGRATIONS.keys())
        self.assertEqual(target_version, get_schema_version())

        with closing(sqlite3.connect(self.db_path)) as connection:
            release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name, release_year) VALUES (?, ?, ?)",
                    ("No-op Album", "no-op album", 2022),
                ).lastrowid
            )
            album_family_id = int(
                connection.execute(
                    """
                    INSERT INTO album_family (
                      primary_name,
                      normalized_name,
                      release_year,
                      canonical_release_album_id
                    ) VALUES (?, ?, ?, ?)
                    """,
                    ("No-op Album", "no-op album", 2022, release_album_id),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO album_family_map (
                  release_album_id,
                  album_family_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    release_album_id,
                    album_family_id,
                    "manual_setup",
                    1.0,
                    "accepted",
                    0,
                ),
            )
            connection.commit()

        apply_pending_migrations()
        self.assertEqual(target_version, get_schema_version())

        with closing(sqlite3.connect(self.db_path)) as connection:
            release_count = int(
                connection.execute(
                    "SELECT count(*) FROM release_album WHERE primary_name = 'No-op Album'"
                ).fetchone()[0]
            )
            family_count = int(
                connection.execute(
                    "SELECT count(*) FROM album_family WHERE primary_name = 'No-op Album'"
                ).fetchone()[0]
            )
            map_count = int(
                connection.execute(
                    """
                    SELECT count(*)
                    FROM album_family_map afm
                    JOIN release_album ra ON ra.id = afm.release_album_id
                    WHERE ra.primary_name = 'No-op Album'
                    """
                ).fetchone()[0]
            )

        self.assertEqual(1, release_count)
        self.assertEqual(1, family_count)
        self.assertEqual(1, map_count)

    def test_get_effective_album_family_id_uses_map_then_falls_back_to_release_album_id(self) -> None:
        self._apply_migrations_through(16)

        with closing(sqlite3.connect(self.db_path)) as connection:
            release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name, release_year) VALUES (?, ?, ?)",
                    ("Family Test Album", "family test album", 2023),
                ).lastrowid
            )
            connection.commit()

        execute_sql(MIGRATIONS[17])
        set_schema_version(17)

        with closing(sqlite3.connect(self.db_path)) as connection:
            mapped_family_id = int(
                connection.execute(
                    """
                    INSERT INTO album_family (
                      primary_name,
                      normalized_name,
                      release_year,
                      canonical_release_album_id
                    ) VALUES (?, ?, ?, ?)
                    """,
                    ("Family Test Album (Canonical)", "family test album canonical", 2023, release_album_id),
                ).lastrowid
            )
            connection.execute(
                """
                UPDATE album_family_map
                SET album_family_id = ?,
                    match_method = ?,
                    confidence = ?,
                    status = ?,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE release_album_id = ?
                """,
                (
                    mapped_family_id,
                    "manual_override",
                    1.0,
                    "accepted",
                    release_album_id,
                ),
            )
            connection.commit()

        self.assertEqual(mapped_family_id, get_effective_album_family_id(release_album_id))

        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                "DELETE FROM album_family_map WHERE release_album_id = ?",
                (release_album_id,),
            )
            connection.commit()

        self.assertEqual(release_album_id, get_effective_album_family_id(release_album_id))

    def test_get_effective_album_family_id_returns_none_for_missing_release_album(self) -> None:
        ensure_sqlite_db()
        apply_pending_migrations()
        self.assertIsNone(get_effective_album_family_id(999999))


if __name__ == "__main__":
    unittest.main()
