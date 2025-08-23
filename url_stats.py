#!/usr/bin/env python3
"""
Utility script to check URL statistics and manage URL tracking.
"""

import argparse
from search_engine.url_tracker import get_url_stats, get_crawled_urls, get_uncrawled_urls
from search_engine.logger import get_logger

log = get_logger(__name__)


def show_stats():
    """Display URL statistics."""
    stats = get_url_stats()
    print("=== URL Statistics ===")
    print(f"Total URLs: {stats['total']}")
    print(f"Crawled URLs: {stats['crawled']}")
    print(f"Uncrawled URLs: {stats['uncrawled']}")
    print(f"Crawl Progress: {stats['crawl_percentage']:.1f}%")


def show_crawled_urls(limit: int = 10):
    """Show recently crawled URLs."""
    crawled = get_crawled_urls()
    print(f"=== Crawled URLs (showing first {limit}) ===")
    for i, url in enumerate(sorted(crawled)[:limit]):
        print(f"{i+1}. {url}")
    if len(crawled) > limit:
        print(f"... and {len(crawled) - limit} more")


def show_uncrawled_urls(limit: int = 10):
    """Show uncrawled URLs."""
    uncrawled = get_uncrawled_urls()
    print(f"=== Uncrawled URLs (showing first {limit}) ===")
    for i, url in enumerate(sorted(uncrawled)[:limit]):
        print(f"{i+1}. {url}")
    if len(uncrawled) > limit:
        print(f"... and {len(uncrawled) - limit} more")


def main():
    parser = argparse.ArgumentParser(description="Check URL tracking statistics")
    parser.add_argument("--crawled", action="store_true", help="Show crawled URLs")
    parser.add_argument("--uncrawled", action="store_true", help="Show uncrawled URLs")
    parser.add_argument("--limit", type=int, default=10, help="Limit number of URLs to show")
    
    args = parser.parse_args()
    
    if args.crawled:
        show_crawled_urls(args.limit)
    elif args.uncrawled:
        show_uncrawled_urls(args.limit)
    else:
        show_stats()


if __name__ == "__main__":
    main() 