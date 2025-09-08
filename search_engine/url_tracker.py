from __future__ import annotations

from datetime import datetime
from typing import List, Set, Optional

from pymongo import ASCENDING
from pymongo.collection import Collection
from pymongo import UpdateOne

from .db import get_db
from .logger import get_logger


log = get_logger(__name__)


def urls_collection() -> Collection:
	"""Get the URLs tracking collection."""
	return get_db()["urls"]


def _ensure_urls_indexes() -> None:
	"""Ensure indexes exist on the URLs collection."""
	urls = urls_collection()
	# Unique URL index
	urls.create_index([("url", ASCENDING)], unique=True, name="unique_url")
	# Index for querying by crawled status
	urls.create_index([("crawled", ASCENDING)], name="idx_crawled")
	log.debug("URLs collection indexes ensured")


def mark_url_crawled(url: str, final_url: Optional[str] = None) -> None:
	"""Mark a URL as crawled."""
	urls = urls_collection()
	_ensure_urls_indexes()
	
	doc = {
		"url": url,
		"crawled": True,
		"crawled_at": datetime.utcnow(),
		"updated_at": datetime.utcnow(),
	}
	if final_url and final_url != url:
		doc["final_url"] = final_url
	
	try:
		urls.update_one(
			{"url": url},
			{
				"$set": doc,
				"$setOnInsert": {"created_at": datetime.utcnow()}
			},
			upsert=True
		)
		log.debug(f"Marked URL as crawled: {url}")
	except Exception as e:
		log.warning(f"Failed to mark URL as crawled {url}: {e}")


def mark_urls_crawled(urls: List[str], final_urls: Optional[List[str]] = None) -> None:
	"""Mark multiple URLs as crawled in batch."""
	if not urls:
		return
	
	urls_col = urls_collection()
	_ensure_urls_indexes()
	
	now = datetime.utcnow()
	operations: List[UpdateOne] = []
	
	for i, url in enumerate(urls):
		doc = {
			"url": url,
			"crawled": True,
			"crawled_at": now,
			"updated_at": now,
		}
		if final_urls and i < len(final_urls) and final_urls[i] != url:
			doc["final_url"] = final_urls[i]
		
		operations.append(UpdateOne(
			{"url": url},
			{
				"$set": doc,
				"$setOnInsert": {"created_at": now}
			},
			upsert=True
		))
	
	if operations:
		try:
			result = urls_col.bulk_write(operations, ordered=False)
			log.info(f"Marked {len(urls)} URLs as crawled: upserted={result.upserted_count} modified={result.modified_count}")
		except Exception as e:
			log.warning(f"Failed to mark URLs as crawled: {e}")


def is_url_crawled(url: str) -> bool:
	"""Check if a URL has been crawled."""
	urls = urls_collection()
	_ensure_urls_indexes()
	
	doc = urls.find_one({"url": url}, projection={"crawled": 1})
	return bool(doc and doc.get("crawled", False))


def get_crawled_urls() -> Set[str]:
	"""Get all crawled URLs as a set."""
	urls = urls_collection()
	_ensure_urls_indexes()
	
	cursor = urls.find({"crawled": True}, projection={"url": 1})
	return {doc["url"] for doc in cursor}


def get_uncrawled_urls() -> Set[str]:
	"""Get all uncrawled URLs as a set."""
	urls = urls_collection()
	_ensure_urls_indexes()
	
	cursor = urls.find({"crawled": {"$ne": True}}, projection={"url": 1})
	return {doc["url"] for doc in cursor}


def add_urls_to_queue(urls: List[str]) -> None:
	"""Add URLs to the queue (mark as uncrawled)."""
	if not urls:
		return
	
	urls_col = urls_collection()
	_ensure_urls_indexes()
	
	now = datetime.utcnow()
	operations: List[UpdateOne] = []
	
	for url in urls:
		operations.append(UpdateOne(
			{"url": url},
			{
				"$set": {
					"url": url,
					"crawled": False,
					"updated_at": now
				},
				"$setOnInsert": {"created_at": now}
			},
			upsert=True
		))
	
	if operations:
		try:
			result = urls_col.bulk_write(operations, ordered=False)
			log.info(f"Queued {len(urls)} URLs: upserted={result.upserted_count} modified={result.modified_count}")
		except Exception as e:
			log.warning(f"Failed to add URLs to queue: {e}")


def get_url_stats() -> dict:
	"""Get statistics about URLs in the database."""
	urls = urls_collection()
	_ensure_urls_indexes()
	
	total = urls.count_documents({})
	crawled = urls.count_documents({"crawled": True})
	uncrawled = urls.count_documents({"crawled": {"$ne": True}})
	
	return {
		"total": total,
		"crawled": crawled,
		"uncrawled": uncrawled,
		"crawl_percentage": (crawled / total * 100) if total > 0 else 0
	} 