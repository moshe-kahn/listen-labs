from __future__ import annotations

import json
import os
import sqlite3
import time
import unittest
from contextlib import closing

from backend.app.db import (
    apply_pending_migrations,
    ensure_sqlite_db,
    insert_ingest_run,
    insert_raw_spotify_history_observation,
    insert_raw_spotify_recent_observation,
)
from backend.app.play_event_projector import reconcile_fact_play_events_for_ingest_run


class PlayEventProjectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.db_path = os.path.join(
            os.getcwd(),
            "backend",
            "tests",
            "_tmp_play_event_projection.sqlite3",
        )
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        os.environ["SQLITE_DB_PATH"] = self.db_path
        ensure_sqlite_db()
        apply_pending_migrations()

    def tearDown(self) -> None:
        if os.path.exists(self.db_path):
            for _ in range(5):
                try:
                    os.remove(self.db_path)
                    break
                except PermissionError:
                    time.sleep(0.1)

    def test_reconcile_creates_matched_fact_with_history_timing_precedence(self) -> None:
        run_recent = "run-recent-1"
        run_history = "run-history-1"
        insert_ingest_run(
            run_id=run_recent,
            source_type="spotify_recent",
            started_at="2026-04-17T19:00:00Z",
            source_ref="test",
        )
        insert_ingest_run(
            run_id=run_history,
            source_type="export",
            started_at="2026-04-17T19:00:00Z",
            source_ref="test",
        )

        insert_raw_spotify_recent_observation(
            ingest_run_id=run_recent,
            source_row_key="recent-row-1",
            source_event_id=None,
            played_at="2026-04-17T19:52:08.858000Z",
            ms_played_estimate=200000,
            ms_played_method="api_chronology",
            ms_played_confidence="high",
            ms_played_fallback_class=None,
            spotify_track_uri="spotify:track:track-1",
            spotify_track_id="track-1",
            track_name_raw="Song A",
            artist_name_raw="Artist A",
            album_name_raw="Album A",
            spotify_album_id="album-1",
            spotify_artist_ids_json=json.dumps(["artist-1"]),
            track_duration_ms=240000,
            context_type="playlist",
            context_uri="spotify:playlist:abc",
            raw_payload_json="{}",
        )
        insert_raw_spotify_history_observation(
            ingest_run_id=run_history,
            source_row_key="history-row-1",
            played_at="2026-04-17T19:52:01Z",
            ms_played=205600,
            spotify_track_uri="spotify:track:track-1",
            spotify_track_id="track-1",
            track_name_raw="Song A",
            artist_name_raw="Artist A",
            album_name_raw="Album A",
            spotify_album_id="album-1",
            spotify_artist_ids_json=json.dumps(["artist-1"]),
            reason_start="trackdone",
            reason_end="logout",
            skipped=0,
            shuffle=1,
            offline=0,
            platform="not_applicable",
            conn_country="US",
            private_session=0,
            raw_payload_json="{}",
        )

        summary_recent = reconcile_fact_play_events_for_ingest_run(
            source_type="spotify_recent",
            run_id=run_recent,
        )
        summary_history = reconcile_fact_play_events_for_ingest_run(
            source_type="export",
            run_id=run_history,
        )

        self.assertGreaterEqual(summary_recent["facts_touched_count"], 1)
        self.assertGreaterEqual(summary_recent["matched_pairs_count"], 1)
        self.assertGreaterEqual(summary_history["matched_pairs_count"], 0)

        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                """
                SELECT
                  f.timing_source,
                  f.matched_state,
                  f.canonical_ended_at,
                  f.canonical_ms_played,
                  f.canonical_reason_end,
                  f.canonical_context_type,
                  f.canonical_context_uri
                FROM fact_play_event f
                JOIN fact_play_event_recent_link rl
                  ON rl.fact_play_event_id = f.id
                JOIN fact_play_event_history_link hl
                  ON hl.fact_play_event_id = f.id
                LIMIT 1
                """
            ).fetchone()

        assert row is not None
        self.assertEqual("history", row[0])
        self.assertEqual("matched", row[1])
        self.assertEqual("2026-04-17T19:52:01Z", row[2])
        self.assertEqual(205600, row[3])
        self.assertEqual("logout", row[4])
        self.assertEqual("playlist", row[5])
        self.assertEqual("spotify:playlist:abc", row[6])


if __name__ == "__main__":
    unittest.main()
