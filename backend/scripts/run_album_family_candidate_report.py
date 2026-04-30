from __future__ import annotations

import sys
from pathlib import Path

# Allow direct execution via:
#   ./.venv/bin/python backend/scripts/run_album_family_candidate_report.py
# from repository root, where `backend` is not always importable by default.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.scripts.report_album_family_candidates import generate_album_family_candidate_report


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    output_path = generate_album_family_candidate_report(limit=500, offset=0)
    print(f"Album-family candidate report written to: {output_path}")


if __name__ == "__main__":
    main()
