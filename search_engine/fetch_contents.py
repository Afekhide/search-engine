from __future__ import annotations

import json
import time
from dataclasses import dataclass
from html import unescape
from typing import List, Optional

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import HTTP_TIMEOUT_SECS, HTTP_MAX_CONTENT_MB, CRAWL_DELAY_SECS
from .logger import get_logger
from .url_tracker import get_uncrawled_urls, mark_urls_crawled


log = get_logger(__name__)


@dataclass
class FetchedPage:
	url: str
	final_url: str
	title: str
	text: str
	html: str


def _clean_title(soup: BeautifulSoup) -> str:
	title = soup.title.string if soup.title and soup.title.string else ""
	return unescape(title.strip())


def _extract_visible_text(soup: BeautifulSoup) -> str:
	for elem in soup(["script", "style", "noscript"]):
		elem.decompose()
	text = soup.get_text(separator=" ", strip=True)
	text = unescape(text)
	return text


@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=0.5, max=4))
def fetch_url_content(url: str) -> Optional[FetchedPage]:
	"""Fetch content from a URL."""
	log.debug(f"Fetching content from: {url}")
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
	text = _extract_visible_text(soup)
	
	log.debug(f"Fetched content from {url} → {resp.url} title='{title[:60]}' len={len(text)}")
	return FetchedPage(url=url, final_url=str(resp.url), title=title, text=text, html=html)


def fetch_content_batch(urls: List[str], max_workers: int = 8) -> List[FetchedPage]:
	"""Fetch content from a batch of URLs in parallel."""
	if not urls:
		return []
	
	log.info(f"Fetching content from {len(urls)} URLs with {max_workers} workers")
	results: List[FetchedPage] = []
	
	with ThreadPoolExecutor(max_workers=max_workers) as executor:
		futures = {executor.submit(fetch_url_content, url): url for url in urls}
		
		for future in as_completed(futures):
			url = futures[future]
			try:
				page = future.result()
				if page:
					results.append(page)
			except Exception as e:
				log.warning(f"Error fetching content from {url}: {e}")
	
	log.info(f"Successfully fetched content from {len(results)}/{len(urls)} URLs")
	return results


def fetch_content_from_database(batch_size: int = 100, max_urls: Optional[int] = None, max_workers: int = 8) -> List[FetchedPage]:
	"""Fetch content from uncrawled URLs in the database."""
	uncrawled_urls = list(get_uncrawled_urls())
	
	if max_urls:
		uncrawled_urls = uncrawled_urls[:max_urls]
	
	if not uncrawled_urls:
		log.info("No uncrawled URLs found in database")
		return []
	
	log.info(f"Found {len(uncrawled_urls)} uncrawled URLs in database")
	
	all_results: List[FetchedPage] = []
	processed_urls: List[str] = []
	processed_final_urls: List[str] = []
	
	# Process in batches
	for i in range(0, len(uncrawled_urls), batch_size):
		batch = uncrawled_urls[i:i + batch_size]
		log.info(f"Processing batch {i//batch_size + 1}: {len(batch)} URLs")
		
		batch_results = fetch_content_batch(batch, max_workers)
		all_results.extend(batch_results)
		
		# Track successfully processed URLs
		for page in batch_results:
			processed_urls.append(page.url)
			processed_final_urls.append(page.final_url)
		
		# Mark batch as crawled
		if processed_urls:
			mark_urls_crawled(processed_urls, processed_final_urls)
			processed_urls = []
			processed_final_urls = []
		
		# Delay between batches
		if CRAWL_DELAY_SECS > 0:
			time.sleep(CRAWL_DELAY_SECS)
	
	log.info(f"Content fetching completed: {len(all_results)} pages fetched")
	return all_results 