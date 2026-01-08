"""
Fetch all Strava athlete activities for a given year and save each page to JSON.

Usage:
  python scripts/extract_year_data.py 2025

Env:
  STRAVA_ACCESS_TOKEN=...  (stored in .env or your shell env)
"""

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Tuple


API_BASE = "https://www.strava.com/api/v3"
REPO_ROOT = Path(__file__).resolve().parents[1]


def load_dotenv(path: Path) -> None:
    if not path.exists() or not path.is_file():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def year_bounds_epoch(year: int) -> Tuple[int, int]:
    start = datetime(year, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end_exclusive = datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    after = int(start.timestamp())
    before = int(end_exclusive.timestamp()) - 1
    return after, before


@dataclass(frozen=True)
class FetchResult:
    payload: Any
    headers: Dict[str, str]
    status: int


def fetch_json(url: str, token: str, timeout_s: int = 60) -> FetchResult:
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
        method="GET",
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            body = resp.read().decode("utf-8")
            headers = {k: v for (k, v) in resp.headers.items()}
            payload = json.loads(body) if body else None
            return FetchResult(payload=payload, headers=headers, status=resp.status)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if e.fp else ""
        headers = dict(e.headers.items()) if e.headers else {}
        try:
            payload = json.loads(body) if body else None
        except json.JSONDecodeError:
            payload = body
        return FetchResult(payload=payload, headers=headers, status=int(e.code))


def build_activities_url(after: int, before: int, page: int, per_page: int) -> str:
    query = urllib.parse.urlencode(
        {
            "after": after,
            "before": before,
            "page": page,
            "per_page": per_page,
        }
    )
    return f"{API_BASE}/athlete/activities?{query}"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch Strava athlete activities for a year, saving each page to JSON."
    )
    parser.add_argument("year", type=int, help="Year to fetch (e.g. 2025).")
    parser.add_argument(
        "--out",
        type=Path,
        default=REPO_ROOT / "data" / "strava",
        help="Output base folder (default: data/strava).",
    )
    parser.add_argument(
        "--per-page",
        type=int,
        default=200,
        help="Items per page (Strava max is 200).",
    )
    parser.add_argument(
        "--start-page",
        type=int,
        default=1,
        help="First page to fetch (default: 1).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="Optional safety cap; 0 means no cap (default: 0).",
    )
    parser.add_argument(
        "--rpm",
        type=int,
        default=15,
        help="Max requests per minute (default: 15).",
    )
    args = parser.parse_args()

    load_dotenv(REPO_ROOT / ".env")
    token = os.environ.get("STRAVA_ACCESS_TOKEN", "").strip()
    if not token:
        raise SystemExit(
            "Missing STRAVA_ACCESS_TOKEN. Put it in .env or set it in your environment."
        )

    if args.per_page < 1 or args.per_page > 200:
        raise SystemExit("--per-page must be between 1 and 200.")
    if args.start_page < 1:
        raise SystemExit("--start-page must be >= 1.")
    if args.rpm < 1:
        raise SystemExit("--rpm must be >= 1.")

    after, before = year_bounds_epoch(args.year)
    out_dir = args.out / str(args.year)
    out_dir.mkdir(parents=True, exist_ok=True)

    min_interval_s = 60.0 / float(args.rpm)
    next_allowed = 0.0

    page = args.start_page
    fetched_pages = 0

    while True:
        if args.max_pages and fetched_pages >= args.max_pages:
            print(f"Reached --max-pages={args.max_pages}; stopping.")
            break

        now = time.monotonic()
        if now < next_allowed:
            time.sleep(next_allowed - now)

        url = build_activities_url(after=after, before=before, page=page, per_page=args.per_page)
        result = fetch_json(url, token=token)
        next_allowed = time.monotonic() + min_interval_s

        if result.status == 401:
            raise SystemExit("401 Unauthorized. Check STRAVA_ACCESS_TOKEN.")

        if result.status == 429:
            retry_after = result.headers.get("Retry-After")
            sleep_s = float(retry_after) if retry_after and retry_after.isdigit() else 10.0
            print(f"429 Rate limited. Sleeping {sleep_s}s then retrying page {page}...")
            time.sleep(sleep_s)
            next_allowed = time.monotonic() + min_interval_s
            continue

        if result.status < 200 or result.status >= 300:
            raise SystemExit(f"HTTP {result.status} fetching page {page}: {result.payload}")

        if not isinstance(result.payload, list):
            raise SystemExit(
                f"Unexpected response type for page {page}: {type(result.payload).__name__}"
            )

        if not result.payload:
            print(f"No more activities. Last fetched page: {page - 1}")
            break

        out_path = out_dir / f"page_{page}.json"
        out_path.write_text(json.dumps(result.payload, ensure_ascii=False, indent=2), encoding="utf-8")

        usage = result.headers.get("X-RateLimit-Usage")
        limit = result.headers.get("X-RateLimit-Limit")
        usage_msg = f" (rate {usage}/{limit})" if usage and limit else ""
        print(f"Wrote {out_path} ({len(result.payload)} activities){usage_msg}")

        page += 1
        fetched_pages += 1

    meta = {
        "year": args.year,
        "after": after,
        "before": before,
        "per_page": args.per_page,
        "start_page": args.start_page,
        "fetched_pages": fetched_pages,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    (out_dir / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
