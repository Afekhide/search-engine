from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pymongo import UpdateOne
from concurrent.futures import ThreadPoolExecutor, as_completed

from .fetch_contents import FetchedPage
from .db import documents_collection, upsert_document
from .text import normalize_text_for_index, summarize_text
from .config import INDEX_EXCERPT_MAX_CHARS


def build_document_from_page(page: FetchedPage) -> Dict[str, Any]:
	"""Convert a fetched page into a MongoDB document ready for indexing.

	Fields designed to match the text index created in `db.py` where `title` and
	`index_text` are included in a weighted `$text` index.
	"""
	normalized = normalize_text_for_index(page.text or "")
	excerpt = summarize_text(page.text or "", max_chars=INDEX_EXCERPT_MAX_CHARS)

	doc: Dict[str, Any] = {
		"url": page.url,
		"final_url": page.final_url,
		"title": page.title or "",
		"raw_text": page.text or "",
		"text_excerpt": excerpt,
		"index_text": normalized.joined,
		"content_length": len(page.text or ""),
		"source": "crawler",
		"updated_at": datetime.utcnow(),
	}
	return doc


def index_page(page: FetchedPage) -> None:
	"""Index a single fetched page (upsert)."""
	doc = build_document_from_page(page)
	upsert_document(doc)


def index_pages(pages: Iterable[FetchedPage], batch_size: int = 100) -> Dict[str, int]:
	"""Index many pages efficiently using bulk writes.

	Returns statistics with keys: `attempted`, `upserts_completed`, `batches`.
	"""
	docs_col = documents_collection()
	buffer: List[UpdateOne] = []
	attempted = 0
	batches = 0
	completed = 0

	def _flush() -> int:
		nonlocal batches
		if not buffer:
			return 0
		res = docs_col.bulk_write(buffer, ordered=False)
		buffer.clear()
		batches += 1
		return (res.upserted_count or 0) + (res.modified_count or 0) + (res.matched_count or 0)

	for page in pages:
		attempted += 1
		doc = build_document_from_page(page)
		update = {
			"$set": {k: v for k, v in doc.items() if k not in {"url"}},
			"$setOnInsert": {"created_at": datetime.utcnow()},
		}
		buffer.append(UpdateOne({"url": doc["url"]}, update, upsert=True))
		if len(buffer) >= batch_size:
			completed += _flush()

	completed += _flush()
	return {"attempted": attempted, "upserts_completed": completed, "batches": batches}


def index_pages_parallel(pages: Iterable[FetchedPage], batch_size: int = 200, max_workers: int = 8) -> Dict[str, int]:
	"""Build index documents in parallel threads and batch-write to MongoDB.

	Parallelization focuses on CPU-bound normalization; MongoDB writes remain
	batched and executed on the main thread to avoid lock contention.
	"""
	# Stage 1: materialize/normalize in parallel
	futures = []
	attempted = 0
	documents: List[Dict[str, Any]] = []

	with ThreadPoolExecutor(max_workers=max_workers) as executor:
		for page in pages:
			attempted += 1
			futures.append(executor.submit(build_document_from_page, page))
			if len(futures) >= batch_size:
				for f in as_completed(list(futures)):
					documents.append(f.result())
				futures.clear()
		# drain remaining
		for f in as_completed(list(futures)):
			documents.append(f.result())

	# Stage 2: bulk write
	docs_col = documents_collection()
	buffer: List[UpdateOne] = []
	batches = 0
	completed = 0

	def _flush() -> int:
		nonlocal batches
		if not buffer:
			return 0
		res = docs_col.bulk_write(buffer, ordered=False)
		buffer.clear()
		batches += 1
		return (res.upserted_count or 0) + (res.modified_count or 0) + (res.matched_count or 0)

	for doc in documents:
		update = {
			"$set": {k: v for k, v in doc.items() if k not in {"url"}},
			"$setOnInsert": {"created_at": datetime.utcnow()},
		}
		buffer.append(UpdateOne({"url": doc["url"]}, update, upsert=True))
		if len(buffer) >= batch_size:
			completed += _flush()

	completed += _flush()
	return {"attempted": attempted, "upserts_completed": completed, "batches": batches}


def reindex_documents(query: Optional[Dict[str, Any]] = None, batch_size: int = 500) -> Dict[str, int]:
	"""Recompute `index_text` from stored `raw_text` for existing documents.

	Useful if you change tokenization/normalization rules.
	"""
	docs_col = documents_collection()
	q = query or {}
	cursor = docs_col.find(q, projection={"_id": 1, "url": 1, "title": 1, "raw_text": 1})

	buffer: List[UpdateOne] = []
	total = 0
	updated = 0
	batches = 0

	def _flush() -> int:
		nonlocal batches
		if not buffer:
			return 0
		res = docs_col.bulk_write(buffer, ordered=False)
		buffer.clear()
		batches += 1
		return res.modified_count or 0

	for doc in cursor:
		total += 1
		raw_text = doc.get("raw_text", "") or ""
		normalized = normalize_text_for_index(raw_text)
		excerpt = summarize_text(raw_text, 400)
		update = {
			"$set": {
				"index_text": normalized.joined,
				"text_excerpt": excerpt,
				"updated_at": datetime.utcnow(),
			}
		}
		buffer.append(UpdateOne({"_id": doc["_id"]}, update, upsert=False))
		if len(buffer) >= batch_size:
			updated += _flush()

	updated += _flush()
	return {"matched": total, "updated": updated, "batches": batches} 