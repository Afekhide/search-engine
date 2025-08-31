#!/usr/bin/env python3
"""
Fetch content from uncrawled URLs in the database and save to JSONL.
"""

import argparse
import json
from pathlib import Path
from datetime import datetime

from search_engine.fetch_contents import fetch_content_from_database
from search_engine.config import INDEXER_WORKERS
from search_engine.logger import get_logger
from search_engine.url_tracker import get_url_stats


log = get_logger(__name__)


def _default_output_path(output: str | None) -> Path:
    if output:
        p = Path(output)
        p.parent.mkdir(parents=True, exist_ok=True)
        return p
    data_dir = Path("data")
    data_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    return data_dir / f"content-{stamp}.jsonl"


def _write_jsonl(pages, output_path: Path, include_html: bool = False) -> int:
    count = 0
    with output_path.open("w", encoding="utf-8") as f:
        for page in pages:
            obj = {
                "url": page.url,
                "final_url": page.final_url,
                "title": page.title,
                "text": page.text,
            }
            if include_html:
                obj["html"] = page.html
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
            count += 1
    return count


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch content from uncrawled URLs in database")
    parser.add_argument("--batch-size", type=int, default=100, help="Number of URLs to process per batch")
    parser.add_argument("--max-urls", type=int, default=None, help="Maximum number of URLs to fetch (default: all uncrawled)")
    parser.add_argument("--workers", type=int, default=INDEXER_WORKERS, help="Number of worker threads")
    parser.add_argument("--output", default=None, help="Output JSONL path (defaults to data/content-<timestamp>.jsonl)")
    parser.add_argument("--include-html", action="store_true", help="Include raw HTML in output")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    # Show URL statistics before fetching
    stats_before = get_url_stats()
    log.info(f"URL stats before content fetching: {stats_before}")

    if stats_before["uncrawled"] == 0:
        log.warning("No uncrawled URLs found in database. Run the crawler first to discover URLs.")
        return

    output_path = _default_output_path(args.output)
    log.info(f"Fetching content from database: batch_size={args.batch_size} max_urls={args.max_urls} workers={args.workers}")
    log.info(f"Writing output to {output_path}")

    # Fetch content from database
    pages = fetch_content_from_database(
        batch_size=args.batch_size,
        max_urls=args.max_urls,
        max_workers=args.workers
    )

    # Write to JSONL file
    count = _write_jsonl(pages, output_path, include_html=bool(args.include_html))
    log.info(f"Wrote {count} pages to {output_path}")

    # Show URL statistics after fetching
    stats_after = get_url_stats()
    log.info(f"URL stats after content fetching: {stats_after}")


if __name__ == "__main__":
    main() 