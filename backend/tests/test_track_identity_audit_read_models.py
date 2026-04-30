from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
import os
import sqlite3
from contextlib import closing

from backend.app.db import apply_pending_migrations, ensure_sqlite_db
from backend.app.track_identity_audit import query_ambiguous_review_queue, query_suggested_analysis_groups
from backend.app.track_identity_audit_submission import (
    dry_run_identity_audit_submission,
    get_identity_audit_submission,
    list_identity_audit_submissions,
    save_identity_audit_submission,
    validate_identity_audit_submission_preview,
)


class TrackIdentityAuditReadModelTests(unittest.TestCase):
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

    def test_ambiguous_review_parser_handles_messy_file_with_partial_results(self) -> None:
        messy_content = """Ambiguous Analysis Review Queue
===============================
Generated at: 2026-04-19T23:52:27.077110Z

Summary
-------
grouped_review_entries: 1
ungrouped_review_entries: 1
this line is malformed

Review Family Counts
--------------------
acoustic: 2
bad family line

Grouped Review Entries
----------------------
[release_track 11] Song One - Acoustic | artist=Artist A | analysis=Song One | song_family_key=song one | confidence=0.77
  review_families=acoustic | base='Song One' | dominant='acoustic'
  components=acoustic:acoustic:arrangement_change:True, malformed_component

Ungrouped Review Entries
------------------------
[release_track 12] Song Two - Demo | artist=Artist B | analysis=(none)
  review_families=demo | base='Song Two' | dominant='demo'
  components=demo:demo:recording_state:True
"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "analysis_ambiguous_review_20260419T235226Z.txt"
            path.write_text(messy_content, encoding="utf-8")
            payload = query_ambiguous_review_queue(log_path=str(path), limit=50, offset=0)

        self.assertEqual(2, payload["summary"]["total_review_entries"])
        self.assertEqual(2, len(payload["items"]))
        self.assertIsInstance(payload["parse_warning"], str)
        self.assertTrue(payload["parse_warning"])
        self.assertEqual([], [item for item in payload["items"] if not isinstance(item.get("components"), list)])

    def test_ambiguous_review_parser_returns_empty_payload_when_file_missing(self) -> None:
        payload = query_ambiguous_review_queue(
            log_path="/tmp/definitely_missing_ambiguous_review_file.txt",
            limit=50,
            offset=0,
        )

        self.assertEqual([], payload["items"])
        self.assertEqual([], payload["family_counts"])
        self.assertEqual(0, payload["summary"]["total_review_entries"])
        self.assertTrue(payload["parse_warning"])

    def test_ambiguous_review_parser_tolerates_unknown_headers_and_empty_sections(self) -> None:
        content = """Ambiguous Analysis Review Queue
===============================
Generated at: 2026-04-20T00:00:00Z

Summary
-------
grouped_review_entries: 0
ungrouped_review_entries: 0

Unknown Strange Header
----------------------
this should not crash parser

Review Family Counts
--------------------
(none)

Grouped Review Entries
----------------------
(none)

Ungrouped Review Entries
------------------------
(none)
"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "analysis_ambiguous_review_20260420T000000Z.txt"
            path.write_text(content, encoding="utf-8")
            payload = query_ambiguous_review_queue(log_path=str(path), limit=20, offset=0)

        self.assertEqual([], payload["items"])
        self.assertEqual([], payload["family_counts"])
        self.assertEqual(0, payload["summary"]["total_review_entries"])
        self.assertIn("parse_warning", payload)

    def test_ambiguous_review_pagination_applied_after_parse(self) -> None:
        content = """Ambiguous Analysis Review Queue
===============================
Generated at: 2026-04-21T00:00:00Z

Summary
-------
grouped_review_entries: 3
ungrouped_review_entries: 0

Review Family Counts
--------------------
acoustic: 3

Grouped Review Entries
----------------------
[release_track 1] Song A - Acoustic | artist=Artist A | analysis=Song A | song_family_key=song a | confidence=0.77
  review_families=acoustic | base='Song A' | dominant='acoustic'
  components=acoustic:acoustic:arrangement_change:True
[release_track 2] Song B - Acoustic | artist=Artist B | analysis=Song B | song_family_key=song b | confidence=0.77
  review_families=acoustic | base='Song B' | dominant='acoustic'
  components=acoustic:acoustic:arrangement_change:True
[release_track 3] Song C - Acoustic | artist=Artist C | analysis=Song C | song_family_key=song c | confidence=0.77
  review_families=acoustic | base='Song C' | dominant='acoustic'
  components=acoustic:acoustic:arrangement_change:True
"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "analysis_ambiguous_review_20260421T000000Z.txt"
            path.write_text(content, encoding="utf-8")
            payload = query_ambiguous_review_queue(log_path=str(path), limit=1, offset=1)

        self.assertEqual(1, payload["pagination"]["returned"])
        self.assertTrue(payload["pagination"]["has_more"])
        self.assertEqual(3, payload["summary"]["total_review_entries"])
        self.assertEqual(2, payload["items"][0]["release_track_id"])

    def test_suggested_groups_shape_unchanged_when_album_family_lookup_is_consumed(self) -> None:
        db_path = os.path.join(
            os.getcwd(),
            "backend",
            "tests",
            "_tmp_track_identity_audit_read_models.sqlite3",
        )
        if os.path.exists(db_path):
            os.remove(db_path)
        os.environ["SQLITE_DB_PATH"] = db_path
        ensure_sqlite_db()
        apply_pending_migrations()

        try:
            with closing(sqlite3.connect(db_path)) as connection:
                artist_id = int(
                    connection.execute(
                        "INSERT INTO artist (canonical_name, sort_name) VALUES (?, ?)",
                        ("Artist A", "artist a"),
                    ).lastrowid
                )
                release_track_id = int(
                    connection.execute(
                        "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                        ("Song A", "song a"),
                    ).lastrowid
                )
                release_album_id = int(
                    connection.execute(
                        "INSERT INTO release_album (primary_name, normalized_name, release_year) VALUES (?, ?, ?)",
                        ("Album A", "album a", 2024),
                    ).lastrowid
                )
                analysis_track_id = int(
                    connection.execute(
                        "INSERT INTO analysis_track (primary_name, grouping_note) VALUES (?, ?)",
                        ("Song A", "conservative_exact_title_primary_artist:hash|song a"),
                    ).lastrowid
                )
                connection.execute(
                    """
                    INSERT INTO track_artist (
                      release_track_id, artist_id, role, billing_index, match_method, confidence, source_basis
                    ) VALUES (?, ?, 'primary', 0, 'backfill', 1.0, 'test')
                    """,
                    (release_track_id, artist_id),
                )
                connection.execute(
                    """
                    INSERT INTO album_artist (
                      release_album_id, artist_id, role, billing_index, match_method, confidence, source_basis
                    ) VALUES (?, ?, 'primary', 0, 'backfill', 1.0, 'test')
                    """,
                    (release_album_id, artist_id),
                )
                connection.execute(
                    "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                    (release_album_id, release_track_id),
                )
                connection.execute(
                    """
                    INSERT INTO analysis_track_map (
                      release_track_id, analysis_track_id, match_method, confidence, status, is_user_confirmed, explanation
                    ) VALUES (?, ?, ?, ?, ?, 0, ?)
                    """,
                    (
                        release_track_id,
                        analysis_track_id,
                        "song_family_title_primary_artist",
                        0.9,
                        "suggested",
                        "test seed",
                    ),
                )
                connection.execute(
                    "DELETE FROM album_family_map WHERE release_album_id = ?",
                    (release_album_id,),
                )
                connection.commit()

            payload = query_suggested_analysis_groups(limit=10, offset=0, status="suggested")

            self.assertEqual(1, payload["summary"]["total_groups"])
            self.assertEqual(1, payload["pagination"]["returned"])
            self.assertEqual("suggested", payload["summary"]["status"])
            item = payload["items"][0]
            self.assertEqual(analysis_track_id, item["analysis_track_id"])
            self.assertEqual(1, item["release_track_count"])
            self.assertEqual(1, len(item["release_tracks"]))
            release_track_item = item["release_tracks"][0]
            self.assertEqual(
                {
                    "release_track_id",
                    "release_track_name",
                    "normalized_name",
                    "primary_artists",
                    "album_names",
                    "source_refs",
                    "source_map_methods",
                },
                set(release_track_item.keys()),
            )
            self.assertEqual("Album A", release_track_item["album_names"])
        finally:
            if os.path.exists(db_path):
                os.remove(db_path)

    def test_submission_validate_empty_payload_is_noop(self) -> None:
        payload = validate_identity_audit_submission_preview(
            {},
            known_group_ids=set(),
            known_track_entry_keys=set(),
            known_track_ids=set(),
        )
        self.assertTrue(payload["ok"])
        self.assertEqual(0, payload["summary"]["total_decisions"])
        self.assertIsInstance(payload["validated"]["groups"]["approved"], list)
        self.assertIsInstance(payload["validated"]["tracks"]["approved"], list)

    def test_submission_validate_valid_payload_counts(self) -> None:
        preview = {
            "generated_at": "2026-04-25T10:00:00Z",
            "summary": {},
            "decisions": {
                "groups": {
                    "approved": [{"decision_key": "group:42", "id": 42, "label": "Group A"}],
                    "rejected": [{"decision_key": "group:44", "id": 44, "label": "Group B"}],
                    "skipped": [{"decision_key": "group:45", "id": 45, "label": "Group C"}],
                },
                "tracks": {
                    "approved": [{"decision_key": "track:grouped:1", "id": 1, "label": "Track A"}],
                    "rejected": [{"decision_key": "track:grouped:2", "id": 2, "label": "Track B"}],
                    "skipped": [{"decision_key": "track:grouped:3", "id": 3, "label": "Track C"}],
                },
            },
        }
        payload = validate_identity_audit_submission_preview(
            preview,
            known_group_ids={42, 44, 45},
            known_track_entry_keys={"track:grouped:1", "track:grouped:2", "track:grouped:3"},
            known_track_ids={1, 2, 3},
        )
        self.assertEqual(6, payload["summary"]["total_decisions"])
        self.assertEqual(3, payload["summary"]["group_decisions"])
        self.assertEqual(3, payload["summary"]["track_decisions"])
        self.assertEqual(2, payload["summary"]["approved"])
        self.assertEqual(2, payload["summary"]["rejected"])
        self.assertEqual(2, payload["summary"]["skipped"])

    def test_submission_validate_unknown_items_reported(self) -> None:
        preview = {
            "decisions": {
                "groups": {"approved": [{"decision_key": "group:999", "id": 999}]},
                "tracks": {"approved": [{"decision_key": "track:missing", "id": 888}]},
            }
        }
        payload = validate_identity_audit_submission_preview(
            preview,
            known_group_ids={42},
            known_track_entry_keys={"track:grouped:1"},
            known_track_ids={1},
        )
        self.assertEqual(1, payload["summary"]["unknown_groups"])
        self.assertEqual(1, payload["summary"]["unknown_tracks"])
        self.assertEqual(1, len(payload["unknown_items"]["groups"]))
        self.assertEqual(1, len(payload["unknown_items"]["tracks"]))

    def test_submission_validate_malformed_bucket_warns_and_stays_stable(self) -> None:
        preview = {
            "decisions": {
                "groups": {"approved": "not-an-array"},
                "tracks": {"approved": []},
            }
        }
        payload = validate_identity_audit_submission_preview(
            preview,
            known_group_ids=set(),
            known_track_entry_keys=set(),
            known_track_ids=set(),
        )
        self.assertGreater(payload["summary"]["warnings"], 0)
        self.assertEqual([], payload["validated"]["groups"]["approved"])
        self.assertIsInstance(payload["warnings"], list)

    def test_submission_validate_duplicate_key_warns(self) -> None:
        preview = {
            "decisions": {
                "groups": {
                    "approved": [
                        {"decision_key": "group:42", "id": 42},
                        {"decision_key": "group:42", "id": 42},
                    ]
                },
                "tracks": {},
            }
        }
        payload = validate_identity_audit_submission_preview(
            preview,
            known_group_ids={42},
            known_track_entry_keys=set(),
            known_track_ids=set(),
        )
        warnings_text = " | ".join(payload["warnings"])
        self.assertIn("Duplicate decision_key", warnings_text)

    def test_submission_validate_decision_mismatch_warns_bucket_wins(self) -> None:
        preview = {
            "decisions": {
                "groups": {
                    "rejected": [{"decision_key": "group:42", "id": 42, "decision": "approve"}]
                },
                "tracks": {},
            }
        }
        payload = validate_identity_audit_submission_preview(
            preview,
            known_group_ids={42},
            known_track_entry_keys=set(),
            known_track_ids=set(),
        )
        warnings_text = " | ".join(payload["warnings"])
        self.assertIn("mismatches bucket", warnings_text)
        self.assertEqual("reject", payload["validated"]["groups"]["rejected"][0]["decision"])

    def test_submission_save_valid_payload_persists_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "audit_submissions.sqlite3"
            os.environ["SQLITE_DB_PATH"] = str(db_path)
            ensure_sqlite_db()
            apply_pending_migrations()

            preview = {
                "generated_at": "2026-04-25T10:00:00Z",
                "summary": {},
                "decisions": {
                    "groups": {"approved": [{"decision_key": "group:1", "id": 1, "label": "Group 1"}]},
                    "tracks": {"skipped": [{"decision_key": "track:2", "id": 2, "label": "Track 2"}]},
                },
            }
            result = save_identity_audit_submission(preview)

            self.assertTrue(result["ok"])
            self.assertGreater(result["submission_id"], 0)
            self.assertEqual("saved", result["status"])

            with closing(sqlite3.connect(db_path)) as connection:
                row = connection.execute(
                    """
                    SELECT payload_json, validation_json, status
                    FROM identity_audit_submission
                    WHERE id = ?
                    """,
                    (result["submission_id"],),
                ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual("saved", row[2])
            payload_json = json.loads(str(row[0]))
            validation_json = json.loads(str(row[1]))
            self.assertEqual(preview["generated_at"], payload_json["generated_at"])
            self.assertIn("summary", validation_json)

    def test_submission_save_empty_payload_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "audit_submissions_empty.sqlite3"
            os.environ["SQLITE_DB_PATH"] = str(db_path)
            ensure_sqlite_db()
            apply_pending_migrations()

            result = save_identity_audit_submission({})
            self.assertTrue(result["ok"])
            self.assertEqual(0, result["summary"]["total_decisions"])
            self.assertIsInstance(result["warnings"], list)

            with closing(sqlite3.connect(db_path)) as connection:
                row_count = int(connection.execute("SELECT count(*) FROM identity_audit_submission").fetchone()[0])
            self.assertEqual(1, row_count)

    def test_submission_save_with_unknowns_and_warnings_is_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "audit_submissions_unknown.sqlite3"
            os.environ["SQLITE_DB_PATH"] = str(db_path)
            ensure_sqlite_db()
            apply_pending_migrations()

            preview = {
                "decisions": {
                    "groups": {"approved": [{"decision_key": "group:999", "id": 999}]},
                    "tracks": {"approved": [{"decision_key": "track:missing", "id": 888}]},
                }
            }
            result = save_identity_audit_submission(preview)
            self.assertTrue(result["ok"])
            self.assertGreaterEqual(result["summary"]["unknown_groups"], 1)
            self.assertGreaterEqual(result["summary"]["unknown_tracks"], 1)

            with closing(sqlite3.connect(db_path)) as connection:
                row = connection.execute(
                    "SELECT validation_json FROM identity_audit_submission WHERE id = ?",
                    (result["submission_id"],),
                ).fetchone()
            self.assertIsNotNone(row)
            validation_json = json.loads(str(row[0]))
            self.assertGreaterEqual(len(validation_json["unknown_items"]["groups"]), 1)
            self.assertGreaterEqual(len(validation_json["unknown_items"]["tracks"]), 1)

    def test_submission_save_does_not_mutate_analysis_track_map(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "audit_submissions_no_mutate.sqlite3"
            os.environ["SQLITE_DB_PATH"] = str(db_path)
            ensure_sqlite_db()
            apply_pending_migrations()

            with closing(sqlite3.connect(db_path)) as connection:
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
                    (
                        release_track_id,
                        analysis_track_id,
                        "seed",
                        0.5,
                        "suggested",
                        0,
                        "seed row",
                    ),
                )
                connection.commit()
                before_count, before_digest = self._analysis_track_map_digest(connection)

            save_identity_audit_submission({})

            with closing(sqlite3.connect(db_path)) as connection:
                after_count, after_digest = self._analysis_track_map_digest(connection)
            self.assertEqual(before_count, after_count)
            self.assertEqual(before_digest, after_digest)

    def test_submission_list_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "audit_submissions_list_empty.sqlite3"
            os.environ["SQLITE_DB_PATH"] = str(db_path)
            ensure_sqlite_db()
            apply_pending_migrations()

            payload = list_identity_audit_submissions()
            self.assertTrue(payload["ok"])
            self.assertEqual([], payload["items"])
            self.assertEqual(0, payload["total"])

    def test_submission_list_after_save_newest_first_and_summary_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "audit_submissions_list.sqlite3"
            os.environ["SQLITE_DB_PATH"] = str(db_path)
            ensure_sqlite_db()
            apply_pending_migrations()

            first = save_identity_audit_submission(
                {
                    "decisions": {
                        "groups": {"approved": [{"decision_key": "group:1", "id": 1}]},
                        "tracks": {},
                    }
                }
            )
            second = save_identity_audit_submission(
                {
                    "decisions": {
                        "groups": {"rejected": [{"decision_key": "group:2", "id": 2}]},
                        "tracks": {"skipped": [{"decision_key": "track:3", "id": 3}]},
                    }
                }
            )
            payload = list_identity_audit_submissions(limit=20, offset=0)

            self.assertTrue(payload["ok"])
            self.assertEqual(2, payload["total"])
            self.assertEqual(2, len(payload["items"]))
            self.assertEqual(second["submission_id"], payload["items"][0]["id"])
            self.assertEqual(first["submission_id"], payload["items"][1]["id"])
            self.assertIn("total_decisions", payload["items"][0]["summary"])
            self.assertIn("warnings_count", payload["items"][0])
            self.assertIn("unknown_groups", payload["items"][0])
            self.assertIn("unknown_tracks", payload["items"][0])

    def test_submission_list_pagination_and_limit_cap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "audit_submissions_list_pagination.sqlite3"
            os.environ["SQLITE_DB_PATH"] = str(db_path)
            ensure_sqlite_db()
            apply_pending_migrations()

            created_ids: list[int] = []
            for index in range(3):
                result = save_identity_audit_submission(
                    {"decisions": {"groups": {"approved": [{"decision_key": f"group:{index}", "id": index}]}, "tracks": {}}}
                )
                created_ids.append(result["submission_id"])

            paged = list_identity_audit_submissions(limit=1, offset=1)
            self.assertEqual(1, len(paged["items"]))
            self.assertEqual(created_ids[1], paged["items"][0]["id"])

            capped = list_identity_audit_submissions(limit=999, offset=0)
            self.assertEqual(3, len(capped["items"]))
            self.assertEqual(3, capped["total"])

    def test_submission_read_saved_submission(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "audit_submissions_read.sqlite3"
            os.environ["SQLITE_DB_PATH"] = str(db_path)
            ensure_sqlite_db()
            apply_pending_migrations()

            preview = {
                "generated_at": "2026-04-25T10:00:00Z",
                "decisions": {
                    "groups": {"approved": [{"decision_key": "group:1", "id": 1}]},
                    "tracks": {"skipped": [{"decision_key": "track:2", "id": 2}]},
                },
            }
            result = save_identity_audit_submission(preview)
            readback = get_identity_audit_submission(result["submission_id"])
            self.assertIsNotNone(readback)
            assert readback is not None
            self.assertTrue(readback["ok"])
            self.assertEqual(result["submission_id"], readback["item"]["id"])
            self.assertEqual(preview["generated_at"], readback["item"]["payload"]["generated_at"])
            self.assertIn("summary", readback["item"]["validation"])

    def test_submission_read_missing_returns_none(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "audit_submissions_read_missing.sqlite3"
            os.environ["SQLITE_DB_PATH"] = str(db_path)
            ensure_sqlite_db()
            apply_pending_migrations()

            readback = get_identity_audit_submission(123456)
            self.assertIsNone(readback)

    def test_submission_read_list_do_not_mutate_analysis_track_map(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "audit_submissions_read_safety.sqlite3"
            os.environ["SQLITE_DB_PATH"] = str(db_path)
            ensure_sqlite_db()
            apply_pending_migrations()

            with closing(sqlite3.connect(db_path)) as connection:
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
                    (
                        release_track_id,
                        analysis_track_id,
                        "seed",
                        0.5,
                        "suggested",
                        0,
                        "seed row",
                    ),
                )
                connection.commit()

            saved = save_identity_audit_submission({})
            with closing(sqlite3.connect(db_path)) as connection:
                before_count, before_digest = self._analysis_track_map_digest(connection)

            list_identity_audit_submissions(limit=20, offset=0)
            get_identity_audit_submission(saved["submission_id"])

            with closing(sqlite3.connect(db_path)) as connection:
                after_count, after_digest = self._analysis_track_map_digest(connection)
            self.assertEqual(before_count, after_count)
            self.assertEqual(before_digest, after_digest)

    def test_submission_dry_run_empty_saved_submission(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "audit_submissions_dry_run_empty.sqlite3"
            os.environ["SQLITE_DB_PATH"] = str(db_path)
            ensure_sqlite_db()
            apply_pending_migrations()

            saved = save_identity_audit_submission({})
            payload = dry_run_identity_audit_submission(saved["submission_id"])
            self.assertIsNotNone(payload)
            assert payload is not None
            self.assertTrue(payload["ok"])
            self.assertEqual(0, payload["summary"]["would_apply"])
            self.assertEqual([], payload["plan"]["groups"])
            self.assertEqual([], payload["plan"]["tracks"])

    def test_submission_dry_run_approved_in_plan_rejected_skipped_in_noops(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "audit_submissions_dry_run_plan.sqlite3"
            os.environ["SQLITE_DB_PATH"] = str(db_path)
            ensure_sqlite_db()
            apply_pending_migrations()

            saved = save_identity_audit_submission(
                {
                    "decisions": {
                        "groups": {
                            "approved": [{"decision_key": "group:42", "id": 42, "label": "Group 42"}],
                            "rejected": [{"decision_key": "group:43", "id": 43, "label": "Group 43"}],
                        },
                        "tracks": {
                            "approved": [{"decision_key": "track:grouped:1", "id": 1, "label": "Track 1"}],
                            "skipped": [{"decision_key": "track:grouped:2", "id": 2, "label": "Track 2"}],
                        },
                    }
                }
            )
            payload = dry_run_identity_audit_submission(saved["submission_id"])
            self.assertIsNotNone(payload)
            assert payload is not None
            self.assertEqual(1, payload["summary"]["approved_groups"])
            self.assertEqual(1, payload["summary"]["approved_tracks"])
            self.assertEqual(2, payload["summary"]["would_apply"])
            self.assertEqual(1, len(payload["plan"]["groups"]))
            self.assertEqual(1, len(payload["plan"]["tracks"]))
            self.assertEqual(1, len(payload["noops"]["rejected"]))
            self.assertEqual(1, len(payload["noops"]["skipped"]))
            self.assertEqual("would_accept_group", payload["plan"]["groups"][0]["action"])
            self.assertEqual("would_accept_track_mapping", payload["plan"]["tracks"][0]["action"])

    def test_submission_dry_run_revalidation_warnings_unknowns_propagated(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "audit_submissions_dry_run_warnings.sqlite3"
            os.environ["SQLITE_DB_PATH"] = str(db_path)
            ensure_sqlite_db()
            apply_pending_migrations()

            saved = save_identity_audit_submission(
                {
                    "decisions": {
                        "groups": {"approved": [{"decision_key": "group:999", "id": 999}]},
                        "tracks": {"approved": [{"decision_key": "track:missing", "id": 888}]},
                    }
                }
            )
            payload = dry_run_identity_audit_submission(saved["submission_id"])
            self.assertIsNotNone(payload)
            assert payload is not None
            self.assertGreaterEqual(payload["summary"]["unknown_groups"], 1)
            self.assertGreaterEqual(payload["summary"]["unknown_tracks"], 1)
            self.assertIsInstance(payload["warnings"], list)
            self.assertIn("validation", payload)

    def test_submission_dry_run_missing_returns_none(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "audit_submissions_dry_run_missing.sqlite3"
            os.environ["SQLITE_DB_PATH"] = str(db_path)
            ensure_sqlite_db()
            apply_pending_migrations()

            payload = dry_run_identity_audit_submission(999999)
            self.assertIsNone(payload)

    def test_submission_dry_run_does_not_mutate_analysis_track_map_or_submission_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "audit_submissions_dry_run_safety.sqlite3"
            os.environ["SQLITE_DB_PATH"] = str(db_path)
            ensure_sqlite_db()
            apply_pending_migrations()

            with closing(sqlite3.connect(db_path)) as connection:
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
                    (
                        release_track_id,
                        analysis_track_id,
                        "seed",
                        0.5,
                        "suggested",
                        0,
                        "seed row",
                    ),
                )
                connection.commit()
                before_count, before_digest = self._analysis_track_map_digest(connection)

            saved = save_identity_audit_submission({})
            payload = dry_run_identity_audit_submission(saved["submission_id"])
            self.assertIsNotNone(payload)

            with closing(sqlite3.connect(db_path)) as connection:
                after_count, after_digest = self._analysis_track_map_digest(connection)
                status_row = connection.execute(
                    "SELECT status FROM identity_audit_submission WHERE id = ?",
                    (saved["submission_id"],),
                ).fetchone()
            self.assertEqual(before_count, after_count)
            self.assertEqual(before_digest, after_digest)
            self.assertIsNotNone(status_row)
            self.assertEqual("saved", str(status_row[0]))


if __name__ == "__main__":
    unittest.main()
