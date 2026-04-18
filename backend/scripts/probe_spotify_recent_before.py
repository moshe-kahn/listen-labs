from __future__ import annotations

import argparse
import asyncio
import os
from datetime import UTC, datetime, timedelta

import httpx
from dotenv import load_dotenv


def _to_epoch_millis(iso_value: str) -> int:
    parsed = datetime.fromisoformat(iso_value.replace("Z", "+00:00"))
    return int(parsed.timestamp() * 1000)


def _default_before_iso() -> str:
    return (datetime.now(UTC) - timedelta(days=90)).isoformat().replace("+00:00", "Z")


async def _run(token: str, *, limit: int, before_iso: str | None) -> None:
    params: dict[str, int] = {"limit": max(1, min(limit, 50))}
    if before_iso:
        params["before"] = _to_epoch_millis(before_iso)

    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(
            "https://api.spotify.com/v1/me/player/recently-played",
            params=params,
            headers={"Authorization": f"Bearer {token}"},
        )

    print(f"status={response.status_code}")
    print(f"request_limit={params['limit']}")
    print(f"request_before_iso={before_iso or 'none'}")
    print(f"request_before_ms={params.get('before', 'none')}")

    if response.status_code >= 400:
        try:
            print(f"error={response.json()}")
        except ValueError:
            print(f"error_text={response.text[:500]}")
        return

    payload = response.json()
    items = payload.get("items") or []
    print(f"returned_items={len(items)}")

    played_values = [
        str(item.get("played_at"))
        for item in items
        if isinstance(item, dict) and item.get("played_at") is not None
    ]
    played_values.sort()
    print(f"earliest_played_at={played_values[0] if played_values else 'none'}")
    print(f"latest_played_at={played_values[-1] if played_values else 'none'}")

    if items:
        first = items[0]
        last = items[-1]
        first_track = (first.get("track") or {}).get("name")
        last_track = (last.get("track") or {}).get("name")
        print(f"first_item_track={first_track or 'unknown'}")
        print(f"last_item_track={last_track or 'unknown'}")


def main() -> None:
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

    parser = argparse.ArgumentParser(description="Probe Spotify recently-played endpoint with a custom before cursor.")
    parser.add_argument("--token", default=os.getenv("SPOTIFY_ACCESS_TOKEN", "").strip(), help="Spotify access token")
    parser.add_argument("--limit", type=int, default=50, help="Spotify recently-played limit (1-50)")
    parser.add_argument(
        "--before-iso",
        default=_default_before_iso(),
        help="ISO UTC timestamp for the 'before' cursor (default: now-90d)",
    )
    parser.add_argument(
        "--no-before",
        action="store_true",
        help="Do not send a before cursor (for baseline comparison)",
    )
    args = parser.parse_args()

    token = args.token.strip()
    if not token:
        raise RuntimeError("Missing Spotify token. Provide --token or set SPOTIFY_ACCESS_TOKEN in backend/.env.")

    before_iso = None if args.no_before else args.before_iso
    asyncio.run(_run(token, limit=args.limit, before_iso=before_iso))


if __name__ == "__main__":
    main()
