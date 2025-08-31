from __future__ import annotations

import re
import time
from collections import deque
from dataclasses import dataclass
from html import unescape
from typing import Deque, Dict, Iterable, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import HTTP_TIMEOUT_SECS, HTTP_MAX_CONTENT_MB, CRAWL_DELAY_SECS
from .logger import get_logger
from .url_tracker import is_url_crawled, add_urls_to_queue, mark_url_crawled, mark_urls_crawled


log = get_logger(__name__)


@dataclass
class LinkDiscoveryResult:
	url: str
	final_url: str
	discovered_links: List[str]
	title: str


def is_same_domain(url_a: str, url_b: str) -> bool:
	def _origin(url: str) -> Tuple[str, str]:
		m = re.match(r"^(https?://)([^/]+)", url)
		return (m.group(1) if m else "", m.group(2).lower() if m else "")

	_, host_a = _origin(url_a)
	_, host_b = _origin(url_b)
	return host_a == host_b


def _clean_title(soup: BeautifulSoup) -> str:
	title = soup.title.string if soup.title and soup.title.string else ""
	return unescape(re.sub(r"\s+", " ", title).strip())


@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=0.5, max=4))
def fetch_url_for_links(url: str) -> Optional[LinkDiscoveryResult]:
	"""Fetch a URL to extract links (not content)."""
	log.debug(f"Fetching URL for link discovery: {url}")
	headers = {"User-Agent": "python-search-engine/0.1"}
	resp = requests.get(url, timeout=HTTP_TIMEOUT_SECS, headers=headers, allow_redirects=True)
	if not resp.ok:
		log.warning(f"Non-OK response for {url}: status={resp.status_code}")
		return None
	content_mb = len(resp.content) / (1024 * 1024)
	if content_mb > HTTP_MAX_CONTENT_MB:
		log.info(f"Skipping large content {url} size={content_mb:.2f}MB")
		return None
	
	html = resp.text
	soup = BeautifulSoup(html, "html.parser")
	title = _clean_title(soup)
	links = extract_links(url, html)
	
	log.debug(f"Discovered {len(links)} links from {url} → {resp.url}")
	return LinkDiscoveryResult(url=url, final_url=str(resp.url), discovered_links=links, title=title)


_link_pattern = re.compile(r"^https?://", re.I)


def extract_links(base_url: str, html: str) -> List[str]:
	soup = BeautifulSoup(html, "html.parser")
	links: List[str] = []
	for a in soup.find_all("a", href=True):
		href: str = a["href"].strip()
		if href.startswith("#"):
			continue
		if href.startswith("/"):
			m = re.match(r"^(https?://[^/]+)", base_url)
			if not m:
				continue
			href = m.group(1) + href
		if _link_pattern.match(href):
			links.append(href)
	return links


def discover_links_from_seeds(seeds: Iterable[str], same_domain_only: bool = True, skip_crawled: bool = True) -> List[str]:
	"""Discover links from seed URLs without following them further."""
	log.info(f"Starting link discovery from seeds: seeds={len(list(seeds))} same_domain_only={same_domain_only} skip_crawled={skip_crawled}")
	
	# Add seed URLs to queue if they haven't been processed for link discovery
	if skip_crawled:
		unprocessed_seeds = [url for url in seeds if not is_url_crawled(url)]
		if unprocessed_seeds:
			add_urls_to_queue(unprocessed_seeds)
			log.info(f"Added {len(unprocessed_seeds)} unprocessed seed URLs to queue")
		else:
			log.info("All seed URLs have already been processed for link discovery")
	
	all_discovered_links: Set[str] = set()
	
	for seed_url in seeds:
		if skip_crawled and is_url_crawled(seed_url):
			log.debug(f"Skipping already processed seed URL: {seed_url}")
			continue
			
		result = fetch_url_for_links(seed_url)
		if result is None:
			continue
		
		# Filter links based on domain restriction
		if same_domain_only:
			filtered_links = [link for link in result.discovered_links if is_same_domain(seed_url, link)]
		else:
			filtered_links = result.discovered_links
		
		# Add discovered links to the set
		all_discovered_links.update(filtered_links)
		log.info(f"Discovered {len(filtered_links)} links from {seed_url} (total unique: {len(all_discovered_links)})")
		
		# Mark seed URL as processed for link discovery
		mark_url_crawled(seed_url, result.final_url)
		
		if CRAWL_DELAY_SECS > 0:
			time.sleep(CRAWL_DELAY_SECS)
	
	# Add all discovered links to the queue for future content fetching
	discovered_list = list(all_discovered_links)
	if discovered_list:
		add_urls_to_queue(discovered_list)
		log.info(f"Added {len(discovered_list)} discovered links to queue for content fetching")
	
	return discovered_list


def discover_links_parallel(seeds: Iterable[str], same_domain_only: bool = True, max_workers: int = 8, skip_crawled: bool = True) -> List[str]:
	"""Discover links from seed URLs in parallel without following them further."""
	log.info(f"Starting parallel link discovery: seeds={len(list(seeds))} workers={max_workers} same_domain_only={same_domain_only} skip_crawled={skip_crawled}")
	
	# Add seed URLs to queue if they haven't been processed
	if skip_crawled:
		unprocessed_seeds = [url for url in seeds if not is_url_crawled(url)]
		if unprocessed_seeds:
			add_urls_to_queue(unprocessed_seeds)
			log.info(f"Added {len(unprocessed_seeds)} unprocessed seed URLs to queue")
		else:
			log.info("All seed URLs have already been processed for link discovery")
	
	all_discovered_links: Set[str] = set()
	processed_urls: List[str] = []
	processed_final_urls: List[str] = []
	
	with ThreadPoolExecutor(max_workers=max_workers) as executor:
		# Submit all seed URLs for processing
		futures = {}
		for seed_url in seeds:
			if skip_crawled and is_url_crawled(seed_url):
				log.debug(f"Skipping already processed seed URL: {seed_url}")
				continue
			future = executor.submit(fetch_url_for_links, seed_url)
			futures[future] = seed_url
		
		# Process results as they complete
		for future in as_completed(futures):
			seed_url = futures[future]
			try:
				result = future.result()
			except Exception as e:
				log.warning(f"Error processing {seed_url}: {e}")
				continue
			
			if result is None:
				continue
			
			# Filter links based on domain restriction
			if same_domain_only:
				filtered_links = [link for link in result.discovered_links if is_same_domain(seed_url, link)]
			else:
				filtered_links = result.discovered_links
			
			# Add discovered links to the set
			all_discovered_links.update(filtered_links)
			log.info(f"Discovered {len(filtered_links)} links from {seed_url} (total unique: {len(all_discovered_links)})")
			
			# Track for batch marking as processed
			processed_urls.append(seed_url)
			processed_final_urls.append(result.final_url)
	
	# Mark all processed URLs in batch
	if processed_urls:
		mark_urls_crawled(processed_urls, processed_final_urls)
	
	# Add all discovered links to the queue for future content fetching
	discovered_list = list(all_discovered_links)
	if discovered_list:
		add_urls_to_queue(discovered_list)
		log.info(f"Added {len(discovered_list)} discovered links to queue for content fetching")
	
	log.info(f"Parallel link discovery completed: processed={len(processed_urls)} discovered={len(discovered_list)}")
	return discovered_list


# Keep the old functions for backward compatibility but mark them as deprecated
def bfs_crawl(seeds: Iterable[str], max_pages: int = 100, same_domain_only: bool = True, skip_crawled: bool = True) -> List[object]:
	"""DEPRECATED: Use discover_links_from_seeds instead."""
	log.warning("bfs_crawl is deprecated. Use discover_links_from_seeds for link discovery.")
	return discover_links_from_seeds(seeds, same_domain_only, skip_crawled)


def bfs_crawl_parallel(seeds: Iterable[str], max_pages: int = 100, same_domain_only: bool = True, max_workers: int = 8, skip_crawled: bool = True) -> List[object]:
	"""DEPRECATED: Use discover_links_parallel instead."""
	log.warning("bfs_crawl_parallel is deprecated. Use discover_links_parallel for link discovery.")
	return discover_links_parallel(seeds, same_domain_only, max_workers, skip_crawled) 