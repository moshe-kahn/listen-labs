from __future__ import annotations

import argparse
import asyncio
import json

from backend.app.db import apply_pending_migrations, ensure_sqlite_db
from backend.app.spotify_recent_polling import poll_recent_for_user
from backend.app.spotify_token_store import validate_token_encryption_key


async def _run(user_id: str) -> None:
    ensure_sqlite_db()
    apply_pending_migrations()
    validate_token_encryption_key()
    result = await poll_recent_for_user(user_id)
    print(json.dumps(result, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Poll Spotify recently-played data for a single user.")
    parser.add_argument("--user-id", required=True, help="ListenLab user id / Spotify user id key")
    args = parser.parse_args()
    asyncio.run(_run(args.user_id))


if __name__ == "__main__":
    main()
