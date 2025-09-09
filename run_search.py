import argparse
import json
from typing import List, Set, Dict, Tuple

from pymongo import DESCENDING

from search_engine.db import documents_collection, postings_collection
from search_engine.config import DEFAULT_SEARCH_LIMIT, MAX_SEARCH_LIMIT
from search_engine.logger import get_logger
from search_engine.text import normalize_text_for_index


log = get_logger(__name__)


def _parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Run an inverted-index search (BM25) against MongoDB")
	parser.add_argument("--query", required=True, help="Search query text")
	parser.add_argument("--limit", type=int, default=DEFAULT_SEARCH_LIMIT, help="Number of results to return")
	parser.add_argument("--skip", type=int, default=0, help="Number of results to skip")
	parser.add_argument("--json", action="store_true", help="Output results as JSON {urls: [...], count: N}")
	# Compatibility flags (no-ops for BM25)
	parser.add_argument("--min-score", type=float, default=0.0, help="Ignored for BM25")
	parser.add_argument("--min-overlap", type=int, default=1, help="Ignored for BM25")
	return parser.parse_args()


def _get_corpus_stats() -> Tuple[int, float]:
	"""Return (N, avgdl) where N is number of docs and avgdl is average content length."""
	docs = documents_collection()
	N = docs.count_documents({})
	avgdl = 0.0
	if N > 0:
		pipeline = [{"$group": {"_id": None, "avg": {"$avg": "$content_length"}}}]
		agg = list(docs.aggregate(pipeline))
		if agg:
			avgdl = float(agg[0].get("avg", 0.0))
	return N, avgdl


def _bm25_score(tf: int, df: int, dl: int, N: int, avgdl: float, k1: float = 1.5, b: float = 0.75) -> float:
	if tf <= 0 or df <= 0 or N <= 0:
		return 0.0
	# IDF with slight smoothing to avoid negatives for very common terms
	idf = max(0.0, ( (N - df + 0.5) / (df + 0.5) ))
	# Use log on idf ratio for stability
	import math
	idf = math.log(1.0 + idf)
	K = k1 * (1 - b + b * (dl / (avgdl or 1.0)))
	return idf * ((tf * (k1 + 1)) / (tf + K))


def _bm25_search(query: str, limit: int, skip: int = 0) -> List[Dict]:
	norm = normalize_text_for_index(query)
	terms = list(dict.fromkeys(norm.tokens))  # unique order-preserving
	if not terms:
		return []
	N, avgdl = _get_corpus_stats()
	if N == 0:
		return []

	posts = postings_collection()
	required_terms: Set[str] = set(terms)
	doc_scores: Dict[str, float] = {}
	doc_lengths: Dict[str, int] = {}
	doc_matched_terms: Dict[str, Set[str]] = {}

	# If any query term has df==0, no document can contain all terms
	for t in terms:
		df = posts.count_documents({"term": t})
		if df == 0:
			return []
		for p in posts.find({"term": t}, projection={"doc_url": 1, "tf": 1}):
			url = p.get("doc_url")
			tf = int(p.get("tf", 0) or 0)
			if not url or tf <= 0:
				continue
			if url not in doc_lengths:
				doc = documents_collection().find_one({"url": url}, projection={"content_length": 1})
				doc_lengths[url] = int((doc or {}).get("content_length", 0) or 0)
			dl = doc_lengths[url]
			doc_scores[url] = doc_scores.get(url, 0.0) + _bm25_score(tf, df, dl, N, avgdl)
			# Track matched terms per document
			s = doc_matched_terms.get(url)
			if s is None:
				s = set()
				doc_matched_terms[url] = s
			s.add(t)

	# Keep only docs that matched all query terms (conjunctive match)
	conj_urls = [u for u, matched in doc_matched_terms.items() if matched.issuperset(required_terms)]
	if not conj_urls:
		return []

	# Rank and page
	ranked = sorted(((u, doc_scores.get(u, 0.0)) for u in conj_urls), key=lambda kv: kv[1], reverse=True)
	paged = ranked[skip: skip + limit]
	urls = [u for u, _ in paged]
	if not urls:
		return []
	# Fetch document metadata in one go
	docs_map: Dict[str, Dict] = {}
	for d in documents_collection().find({"url": {"$in": urls}}, projection={"url": 1, "final_url": 1, "title": 1, "text_excerpt": 1}):
		docs_map[d["url"]] = d
	results: List[Dict] = []
	for url in urls:
		meta = docs_map.get(url, {"url": url})
		results.append({
			"url": meta.get("final_url") or meta.get("url"),
			"title": meta.get("title") or "",
			"text_excerpt": meta.get("text_excerpt") or "",
			"score": doc_scores.get(url, 0.0),
		})
	return results


def main() -> None:
	args = _parse_args()
	log.info(f"BM25 Search (AND): q='{args.query}' limit={args.limit} skip={args.skip}")
	limit = max(1, min(args.limit, MAX_SEARCH_LIMIT))
	results = _bm25_search(args.query, limit, args.skip)
	if args.json:
		urls = [doc["url"] for doc in results]
		payload = {"urls": urls, "count": len(urls)}
		log.info(f"Found {payload['count']} relevant results (BM25 AND)")
		print(json.dumps(payload, ensure_ascii=False))
		return
	for i, doc in enumerate(results, start=1):
		print(f"[{i}] {doc.get('title') or '(no title)'}")
		print(f"    URL: {doc.get('url')} | score={doc.get('score'):.4f}")
		excerpt = doc.get("text_excerpt") or ""
		if excerpt:
			print(f"    {excerpt[:300]}")
		print()
	log.info(f"Displayed {len(results)} relevant results (BM25 AND)")


if __name__ == "__main__":
	main() 