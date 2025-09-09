from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pymongo import UpdateOne
from concurrent.futures import ThreadPoolExecutor, as_completed

from .fetch_contents import FetchedPage
from .db import documents_collection, upsert_document, postings_collection, terms_collection
from .text import normalize_text_for_index, summarize_text, tokenize
from .config import INDEX_EXCERPT_MAX_CHARS
from .logger import get_logger


log = get_logger(__name__)


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


def _build_postings(page: FetchedPage) -> Tuple[Dict[str, Any], Dict[str, Any]]:
	"""Build positional postings for a document.

	Returns (term_freqs, term_positions) where:
	- term_freqs: term -> frequency in doc
	- term_positions: term -> list of positions (ints)
	"""
	text_tokens = tokenize((page.text or "").lower())
	norm_tokens = normalize_text_for_index(page.text or "").tokens
	# Map normalized tokens back to positions by aligning raw tokens to normalized pipeline
	# Simpler approach: re-normalize each raw token and track positions of resulting stemmed token
	positions: Dict[str, List[int]] = {}
	freqs: Dict[str, int] = {}
	for idx, raw in enumerate(text_tokens):
		stemmed = normalize_text_for_index(raw).tokens
		if not stemmed:
			continue
		t = stemmed[0]
		freqs[t] = freqs.get(t, 0) + 1
		positions.setdefault(t, []).append(idx)
	return freqs, positions


def _bulk_upsert_postings(url: str, term_freqs: Dict[str, int], term_positions: Dict[str, List[int]]) -> None:
	postings_col = postings_collection()
	terms_col = terms_collection()

	ops: List[UpdateOne] = []
	term_ops: List[UpdateOne] = []
	for term, freq in term_freqs.items():
		ops.append(UpdateOne(
			{"term": term, "doc_url": url},
			{"$set": {"term": term, "doc_url": url, "tf": freq, "positions": term_positions.get(term, [])},
			 "$setOnInsert": {"created_at": datetime.utcnow()}},
			upsert=True
		))
		term_ops.append(UpdateOne(
			{"term": term},
			{"$setOnInsert": {"term": term, "created_at": datetime.utcnow()}, "$set": {"updated_at": datetime.utcnow()}},
			upsert=True
		))

	if ops:
		res = postings_col.bulk_write(ops, ordered=False)
		log.debug(f"Upserted postings: matched={res.matched_count} modified={res.modified_count} upserted={res.upserted_count}")
	if term_ops:
		res2 = terms_col.bulk_write(term_ops, ordered=False)
		log.debug(f"Upserted terms: matched={res2.matched_count} modified={res2.modified_count} upserted={res2.upserted_count}")


def index_page(page: FetchedPage) -> None:
	"""Index a single fetched page (upsert)."""
	doc = build_document_from_page(page)
	log.debug(f"Indexing single page url={doc['url']} title='{doc['title'][:60]}'")
	upsert_document(doc)
	term_freqs, term_positions = _build_postings(page)
	_bulk_upsert_postings(doc["url"], term_freqs, term_positions)


def index_pages(pages: Iterable[FetchedPage], batch_size: int = 100) -> Dict[str, int]:
	"""Index many pages efficiently using bulk writes.

	Returns statistics with keys: `attempted`, `upserts_completed`, `batches`.
	"""
	log.info(f"Starting indexing: batch_size={batch_size}")
	docs_col = documents_collection()
	buffer: List[UpdateOne] = []
	attempted = 0
	batches = 0
	completed = 0

	postings_buffer: List[Tuple[str, Dict[str, int], Dict[str, List[int]]]] = []

	def _flush() -> int:
		nonlocal batches
		if not buffer and not postings_buffer:
			return 0
		res = None
		if buffer:
			res = docs_col.bulk_write(buffer, ordered=False)
			buffer.clear()
		batches += 1
		if res:
			log.debug(f"Flushed docs batch #{batches}: matched={res.matched_count} modified={res.modified_count} upserted={res.upserted_count}")
		# Flush postings per document to keep memory bounded
		for (url, tf, pos) in postings_buffer:
			_bulk_upsert_postings(url, tf, pos)
		postings_buffer.clear()
		return (res.upserted_count or 0) + (res.modified_count or 0) + (res.matched_count or 0) if res else 0

	for page in pages:
		attempted += 1
		doc = build_document_from_page(page)
		update = {
			"$set": {k: v for k, v in doc.items() if k not in {"url"}},
			"$setOnInsert": {"created_at": datetime.utcnow()},
		}
		buffer.append(UpdateOne({"url": doc["url"]}, update, upsert=True))
		# Prepare postings for this page
		term_freqs, term_positions = _build_postings(page)
		postings_buffer.append((doc["url"], term_freqs, term_positions))
		if len(buffer) >= batch_size:
			completed += _flush()

	completed += _flush()
	log.info(f"Indexing completed: attempted={attempted} changed={completed} batches={batches}")
	return {"attempted": attempted, "upserts_completed": completed, "batches": batches}


def index_pages_parallel(pages: Iterable[FetchedPage], batch_size: int = 200, max_workers: int = 8) -> Dict[str, int]:
	"""Build index documents in parallel threads and batch-write to MongoDB.

	Parallelization focuses on CPU-bound normalization; MongoDB writes remain
	batched and executed on the main thread to avoid lock contention.
	"""
	log.info(f"Starting parallel indexing: workers={max_workers} batch_size={batch_size}")
	# Stage 1: materialize/normalize in parallel
	futures = []
	attempted = 0
	documents: List[Dict[str, Any]] = []
	postings_list: List[Tuple[str, Dict[str, int], Dict[str, List[int]]]] = []

	with ThreadPoolExecutor(max_workers=max_workers) as executor:
		for page in pages:
			attempted += 1
			futures.append(executor.submit(lambda p: (build_document_from_page(p), _build_postings(p)), page))
			if len(futures) >= batch_size:
				for f in as_completed(list(futures)):
					doc, (tf, pos) = f.result()
					documents.append(doc)
					postings_list.append((doc["url"], tf, pos))
				futures.clear()
		# drain remaining
		for f in as_completed(list(futures)):
			doc, (tf, pos) = f.result()
			documents.append(doc)
			postings_list.append((doc["url"], tf, pos))

	# Stage 2: bulk write documents, then postings
	docs_col = documents_collection()
	buffer: List[UpdateOne] = []
	batches = 0
	completed = 0

	def _flush_docs() -> int:
		nonlocal batches
		if not buffer:
			return 0
		res = docs_col.bulk_write(buffer, ordered=False)
		buffer.clear()
		batches += 1
		log.debug(f"Flushed docs batch #{batches}: matched={res.matched_count} modified={res.modified_count} upserted={res.upserted_count}")
		return (res.upserted_count or 0) + (res.modified_count or 0) + (res.matched_count or 0)

	for doc in documents:
		update = {
			"$set": {k: v for k, v in doc.items() if k not in {"url"}},
			"$setOnInsert": {"created_at": datetime.utcnow()},
		}
		buffer.append(UpdateOne({"url": doc["url"]}, update, upsert=True))
		if len(buffer) >= batch_size:
			completed += _flush_docs()

	completed += _flush_docs()

	# Upsert postings per document (grouped) to avoid huge bulk sizes
	for url, tf, pos in postings_list:
		_bulk_upsert_postings(url, tf, pos)

	log.info(f"Parallel indexing completed: attempted={attempted} changed={completed} batches={batches}")
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