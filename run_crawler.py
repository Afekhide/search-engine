import argparse
import json
from pathlib import Path
from datetime import datetime

from search_engine.crawler import bfs_crawl, bfs_crawl_parallel
from search_engine.config import CRAWLER_WORKERS, SAME_DOMAIN_ONLY_DEFAULT
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
    return data_dir / f"crawl-{stamp}.jsonl"


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
    parser = argparse.ArgumentParser(description="Crawl seed URLs and write pages to JSONL")
    parser.add_argument("--seeds-file", help="Path to a text file with one seed URL per line")
    parser.add_argument("--max-pages", type=int, default=100, help="Maximum number of pages to fetch")
    parser.add_argument("--same-domain-only", action="store_true", help="Restrict crawling to the same domain as seeds")
    parser.add_argument("--parallel", action="store_true", help="Use multi-threaded crawler")
    parser.add_argument("--workers", type=int, default=CRAWLER_WORKERS, help="Number of worker threads when using --parallel")
    parser.add_argument("--output", default=None, help="Output JSONL path (defaults to data/crawl-<timestamp>.jsonl)")
    parser.add_argument("--include-html", action="store_true", help="Include raw HTML in output")
    parser.add_argument("--skip-crawled", action="store_true", default=True, help="Skip URLs that have already been crawled (default: True)")
    parser.add_argument("--no-skip-crawled", action="store_true", help="Disable skipping of already crawled URLs")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    seeds: list[str] = []
    seeds_path = Path(args.seeds_file) if args.seeds_file else None
    if not seeds_path or not seeds_path.exists():
        raise SystemExit(f"Seeds file not found: {seeds_path}")
    with seeds_path.open("r", encoding="utf-8") as sf:
        for line in sf:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            seeds.append(line)

    if not seeds:
        raise SystemExit("Seeds file is empty. Provide at least one URL.")

    output_path = _default_output_path(args.output)
    same_domain = bool(args.same_domain_only or SAME_DOMAIN_ONLY_DEFAULT)
    skip_crawled = args.skip_crawled and not args.no_skip_crawled

    # Show URL statistics before crawling
    stats_before = get_url_stats()
    log.info(f"URL stats before crawl: {stats_before}")

    log.info(f"Loaded {len(seeds)} seed URLs from {seeds_path}")
    log.info(f"Crawling up to {args.max_pages} pages. parallel={args.parallel} workers={args.workers} same_domain_only={same_domain} skip_crawled={skip_crawled}")
    log.info(f"Writing output to {output_path}")

    if args.parallel:
        pages = bfs_crawl_parallel(seeds, max_pages=args.max_pages, same_domain_only=same_domain, max_workers=args.workers, skip_crawled=skip_crawled)
    else:
        pages = bfs_crawl(seeds, max_pages=args.max_pages, same_domain_only=same_domain, skip_crawled=skip_crawled)

    count = _write_jsonl(pages, output_path, include_html=bool(args.include_html))
    log.info(f"Wrote {count} pages to {output_path}")

    # Show URL statistics after crawling
    stats_after = get_url_stats()
    log.info(f"URL stats after crawl: {stats_after}")


if __name__ == "__main__":
    main() 