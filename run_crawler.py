import argparse
from pathlib import Path

from search_engine.crawler import discover_links_from_seeds, discover_links_parallel
from search_engine.config import CRAWLER_WORKERS, SAME_DOMAIN_ONLY_DEFAULT
from search_engine.logger import get_logger
from search_engine.url_tracker import get_url_stats


log = get_logger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Discover links from seed URLs")
    parser.add_argument("--seeds_file", help="Path to a text file with one seed URL per line")
    parser.add_argument("--same-domain-only", action="store_true", help="Restrict link discovery to the same domain as seeds")
    parser.add_argument("--parallel", action="store_true", help="Use multi-threaded link discovery")
    parser.add_argument("--workers", type=int, default=CRAWLER_WORKERS, help="Number of worker threads when using --parallel")
    parser.add_argument("--skip-crawled", action="store_true", default=True, help="Skip URLs that have already been processed (default: True)")
    parser.add_argument("--no-skip-crawled", action="store_true", help="Disable skipping of already processed URLs")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    seeds: list[str] = []
    seeds_path = Path(args.seeds_file)
    if not seeds_path.exists():
        raise SystemExit(f"Seeds file not found: {seeds_path}")
    
    with seeds_path.open("r", encoding="utf-8") as sf:
        for line in sf:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            seeds.append(line)

    if not seeds:
        raise SystemExit("Seeds file is empty. Provide at least one URL.")

    same_domain = bool(args.same_domain_only or SAME_DOMAIN_ONLY_DEFAULT)
    skip_crawled = args.skip_crawled and not args.no_skip_crawled

    # Show URL statistics before link discovery
    stats_before = get_url_stats()
    log.info(f"URL stats before link discovery: {stats_before}")

    log.info(f"Loaded {len(seeds)} seed URLs from {seeds_path}")
    log.info(f"Link discovery: parallel={args.parallel} workers={args.workers} same_domain_only={same_domain} skip_crawled={skip_crawled}")

    # Discover links from seed URLs
    if args.parallel:
        discovered_links = discover_links_parallel(seeds, same_domain_only=same_domain, max_workers=args.workers, skip_crawled=skip_crawled)
    else:
        discovered_links = discover_links_from_seeds(seeds, same_domain_only=same_domain, skip_crawled=skip_crawled)

    log.info(f"Link discovery completed: {len(discovered_links)} unique links discovered")

    # Show URL statistics after link discovery
    stats_after = get_url_stats()
    log.info(f"URL stats after link discovery: {stats_after}")
    
    log.info("Next step: Run 'python run_fetch_contents.py' to fetch content from discovered URLs")


if __name__ == "__main__":
    main() 