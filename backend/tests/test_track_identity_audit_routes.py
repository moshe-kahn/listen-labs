from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.app.db import apply_pending_migrations, ensure_sqlite_db
from backend.app.main import app


class TrackIdentityAuditRouteTests(unittest.TestCase):
    def test_ambiguous_review_endpoint_empty_result_shape(self) -> None:
        payload = {
            "source": {"kind": "file", "path": "", "generated_at": None},
            "summary": {"grouped_review_entries": 0, "ungrouped_review_entries": 0, "total_review_entries": 0},
            "family_counts": [],
            "pagination": {"limit": 200, "offset": 0, "returned": 0, "has_more": False},
            "filters": {"family": None, "bucket": None},
            "items": [],
            "parse_warning": "",
        }
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.query_ambiguous_review_queue",
            return_value=payload,
        ):
            client = TestClient(app)
            response = client.get("/debug/tracks/identity-audit/ambiguous-review?limit=200&offset=0")

        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertIn("items", body)
        self.assertEqual([], body["items"])
        self.assertIn("family_counts", body)
        self.assertEqual([], body["family_counts"])

    def test_suggested_groups_endpoint_empty_result_shape(self) -> None:
        payload = {
            "summary": {"total_groups": 0, "status": "suggested"},
            "pagination": {"limit": 50, "offset": 0, "returned": 0, "has_more": False},
            "items": [],
        }
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.query_suggested_analysis_groups",
            return_value=payload,
        ):
            client = TestClient(app)
            response = client.get("/debug/tracks/identity-audit/suggested-groups?limit=50&offset=0")

        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertIn("items", body)
        self.assertEqual([], body["items"])
        self.assertEqual(0, body["summary"]["total_groups"])

    def test_submission_preview_validate_endpoint_shape(self) -> None:
        payload = {
            "ok": True,
            "summary": {
                "total_decisions": 0,
                "group_decisions": 0,
                "track_decisions": 0,
                "approved": 0,
                "rejected": 0,
                "skipped": 0,
                "unknown_groups": 0,
                "unknown_tracks": 0,
                "warnings": 0,
            },
            "warnings": [],
            "unknown_items": {"groups": [], "tracks": []},
            "validated": {
                "groups": {"approved": [], "rejected": [], "skipped": []},
                "tracks": {"approved": [], "rejected": [], "skipped": []},
            },
        }
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.validate_identity_audit_submission_preview",
            return_value=payload,
        ):
            client = TestClient(app)
            response = client.post("/debug/tracks/identity-audit/submission-preview/validate", json={})
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual([], body["warnings"])
        self.assertEqual([], body["unknown_items"]["groups"])
        self.assertEqual([], body["validated"]["tracks"]["approved"])

    def test_submission_preview_validate_requires_json_object(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            response = client.post("/debug/tracks/identity-audit/submission-preview/validate", json=[])
        self.assertEqual(400, response.status_code)
        self.assertIn("Payload must be a JSON object", response.json().get("detail", ""))

    def test_submission_save_endpoint_shape(self) -> None:
        payload = {
            "ok": True,
            "submission_id": 123,
            "status": "saved",
            "created_at": "2026-04-25T20:00:00Z",
            "summary": {
                "total_decisions": 0,
                "group_decisions": 0,
                "track_decisions": 0,
                "approved": 0,
                "rejected": 0,
                "skipped": 0,
                "unknown_groups": 0,
                "unknown_tracks": 0,
                "warnings": 0,
            },
            "warnings": [],
            "unknown_items": {"groups": [], "tracks": []},
        }
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.save_identity_audit_submission",
            return_value=payload,
        ):
            client = TestClient(app)
            response = client.post("/debug/tracks/identity-audit/submissions", json={})
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual(123, body["submission_id"])
        self.assertEqual("saved", body["status"])
        self.assertEqual([], body["warnings"])
        self.assertEqual([], body["unknown_items"]["groups"])

    def test_submission_save_requires_json_object(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            response = client.post("/debug/tracks/identity-audit/submissions", json=[])
        self.assertEqual(400, response.status_code)
        self.assertIn("Payload must be a JSON object", response.json().get("detail", ""))

    def test_submission_save_non_object_returns_400_without_writing_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = os.path.join(tmp_dir, "routes_submission_400.sqlite3")
            os.environ["SQLITE_DB_PATH"] = db_path
            ensure_sqlite_db()
            apply_pending_migrations()

            with patch("backend.app.main._require_local_data_session", return_value="user-1"):
                client = TestClient(app)
                response = client.post("/debug/tracks/identity-audit/submissions", json=[])
            self.assertEqual(400, response.status_code)

            with sqlite3.connect(db_path) as connection:
                row_count = int(connection.execute("SELECT count(*) FROM identity_audit_submission").fetchone()[0])
            self.assertEqual(0, row_count)

    def test_submission_list_endpoint_shape(self) -> None:
        payload = {
            "ok": True,
            "items": [
                {
                    "id": 7,
                    "created_at": "2026-04-25T20:10:00Z",
                    "status": "saved",
                    "summary": {"total_decisions": 2},
                    "warnings_count": 0,
                    "unknown_groups": 0,
                    "unknown_tracks": 0,
                    "notes": None,
                }
            ],
            "total": 1,
        }
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.list_identity_audit_submissions",
            return_value=payload,
        ):
            client = TestClient(app)
            response = client.get("/debug/tracks/identity-audit/submissions?limit=20&offset=0")
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual(1, body["total"])
        self.assertEqual(1, len(body["items"]))

    def test_submission_read_endpoint_shape(self) -> None:
        payload = {
            "ok": True,
            "item": {
                "id": 7,
                "created_at": "2026-04-25T20:10:00Z",
                "status": "saved",
                "payload": {"decisions": {}},
                "validation": {"summary": {}},
                "notes": None,
                "promoted_at": None,
            },
        }
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.get_identity_audit_submission",
            return_value=payload,
        ):
            client = TestClient(app)
            response = client.get("/debug/tracks/identity-audit/submissions/7")
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual(7, body["item"]["id"])
        self.assertIn("validation", body["item"])

    def test_submission_read_missing_stable_404_shape(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.get_identity_audit_submission",
            return_value=None,
        ):
            client = TestClient(app)
            response = client.get("/debug/tracks/identity-audit/submissions/999")
        self.assertEqual(404, response.status_code)
        body = response.json()
        self.assertFalse(body["ok"])
        self.assertEqual("submission_not_found", body["error"]["code"])

    def test_submission_dry_run_endpoint_shape(self) -> None:
        payload = {
            "ok": True,
            "submission_id": 7,
            "status": "dry_run",
            "validation": {"summary": {"warnings": 0, "unknown_groups": 0, "unknown_tracks": 0}},
            "summary": {
                "approved_groups": 1,
                "approved_tracks": 1,
                "rejected": 0,
                "skipped": 0,
                "would_apply": 2,
                "warnings": 0,
                "unknown_groups": 0,
                "unknown_tracks": 0,
            },
            "plan": {"groups": [], "tracks": []},
            "noops": {"rejected": [], "skipped": []},
            "warnings": [],
        }
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.dry_run_identity_audit_submission",
            return_value=payload,
        ):
            client = TestClient(app)
            response = client.post("/debug/tracks/identity-audit/submissions/7/dry-run")
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual("dry_run", body["status"])
        self.assertIn("plan", body)
        self.assertIsInstance(body["plan"]["groups"], list)

    def test_submission_dry_run_missing_stable_404_shape(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.dry_run_identity_audit_submission",
            return_value=None,
        ):
            client = TestClient(app)
            response = client.post("/debug/tracks/identity-audit/submissions/999/dry-run")
        self.assertEqual(404, response.status_code)
        body = response.json()
        self.assertFalse(body["ok"])
        self.assertEqual("submission_not_found", body["error"]["code"])


if __name__ == "__main__":
    unittest.main()
