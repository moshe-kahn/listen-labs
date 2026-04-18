from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

from backend.app.db import (
    apply_pending_migrations,
    delete_ingest_run,
    ensure_sqlite_db,
    get_ingest_run,
    get_raw_play_event_by_source_row_key,
    insert_ingest_run,
    insert_raw_play_event,
    list_ingest_runs,
)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def main() -> None:
    ensure_sqlite_db()
    apply_pending_migrations()

    token = uuid.uuid4().hex[:12]
    run_id = f"hygiene-smoke-{token}"
    source_row_key = f"hygiene-smoke-row-{token}"

    insert_ingest_run(
        run_id=run_id,
        source_type="spotify_recent",
        source_ref="hygiene_smoke",
        started_at=_now_iso(),
        status="running",
    )
    row_id = insert_raw_play_event(
        ingest_run_id=run_id,
        source_type="spotify_recent",
        source_row_key=source_row_key,
        source_event_id=None,
        cross_source_event_key=f"hygiene-smoke-event-{token}",
        played_at=_now_iso(),
        ms_played=1000,
        ms_played_method="default_guess",
        track_duration_ms=180000,
        track_name_raw="Hygiene Smoke Track",
        artist_name_raw="ListenLab",
        raw_payload_json="{}",
    )

    fetched = get_ingest_run(run_id)
    listed = list_ingest_runs(source_ref="hygiene_smoke", limit=5)
    before_delete_row = get_raw_play_event_by_source_row_key(source_row_key)

    delete_summary = delete_ingest_run(run_id=run_id, delete_raw_events=True)
    after_delete_run = get_ingest_run(run_id)
    after_delete_row = get_raw_play_event_by_source_row_key(source_row_key)

    output = {
        "run_id": run_id,
        "inserted_row_id": row_id,
        "fetched_run_exists": fetched is not None,
        "list_contains_run": any(item["id"] == run_id for item in listed),
        "raw_row_exists_before_delete": before_delete_row is not None,
        "delete_summary": delete_summary,
        "run_exists_after_delete": after_delete_run is not None,
        "raw_row_exists_after_delete": after_delete_row is not None,
    }
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
