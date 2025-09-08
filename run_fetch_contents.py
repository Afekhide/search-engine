#!/usr/bin/env python3
"""
Fetch content from uncrawled URLs in the database and save to JSONL.
"""

import argparse
import json
from pathlib import Path
from datetime import datetime

from search_engine.fetch_contents import fetch_content_batch
from search_engine.config import INDEXER_WORKERS, CRAWL_DELAY_SECS
from search_engine.logger import get_logger
from search_engine.url_tracker import get_url_stats, get_uncrawled_urls, mark_urls_crawled
import time


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


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch content from uncrawled URLs in database")
    parser.add_argument("--batch-size", type=int, default=100, help="Number of URLs to process per batch")
    parser.add_argument("--max-urls", type=int, default=None, help="Maximum number of URLs to fetch (default: all uncrawled)")
    parser.add_argument("--workers", type=int, default=INDEXER_WORKERS, help="Number of worker threads")
    parser.add_argument("--output", default=None, help="Output JSONL path (defaults to data/content-<timestamp>.jsonl)")
    parser.add_argument("--include-html", action="store_true", help="Include raw HTML in output")
    parser.add_argument("--parallel", action="store_true", help="Compatibility flag (fetching already runs in parallel via --workers)")
    return parser.parse_args()


def _write_pages_append(pages, f, include_html: bool) -> int:
    written = 0
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
        written += 1
    f.flush()
    return written


def main() -> None:
    args = _parse_args()

    # Show URL statistics before fetching
    stats_before = get_url_stats()
    log.info(f"URL stats before content fetching: {stats_before}")

    uncrawled = list(get_uncrawled_urls())
    if not uncrawled:
        log.warning("No uncrawled URLs found in database. Run the crawler first to discover URLs.")
        return

    if args.max_urls is not None:
        uncrawled = uncrawled[: args.max_urls]

    output_path = _default_output_path(args.output)
    log.info(f"Fetching content to {output_path} | batch_size={args.batch_size} workers={args.workers} include_html={bool(args.include_html)} parallel_flag={bool(args.parallel)}")

    total_written = 0
    with output_path.open("a", encoding="utf-8") as out_f:
        for i in range(0, len(uncrawled), args.batch_size):
            batch = uncrawled[i : i + args.batch_size]
            log.info(f"Processing batch {i // args.batch_size + 1} with {len(batch)} URLs")

            pages = fetch_content_batch(batch, max_workers=args.workers)
            written = _write_pages_append(pages, out_f, include_html=bool(args.include_html))
            total_written += written
            log.info(f"Wrote {written} pages (cumulative {total_written})")

            if pages:
                mark_urls_crawled([p.url for p in pages], [p.final_url for p in pages])

            if CRAWL_DELAY_SECS > 0 and i + args.batch_size < len(uncrawled):
                time.sleep(CRAWL_DELAY_SECS)

    # Show URL statistics after fetching
    stats_after = get_url_stats()
    log.info(f"URL stats after content fetching: {stats_after}")
    log.info(f"Done. Total pages written: {total_written}")


if __name__ == "__main__":
    main() 