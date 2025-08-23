import argparse
import json
from typing import List

from pymongo import DESCENDING

from search_engine.db import documents_collection
from search_engine.config import DEFAULT_SEARCH_LIMIT, MAX_SEARCH_LIMIT
from search_engine.logger import get_logger


log = get_logger(__name__)


def _parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Run a full-text search against MongoDB")
	parser.add_argument("--query", required=True, help="Search query text")
	parser.add_argument("--limit", type=int, default=DEFAULT_SEARCH_LIMIT, help="Number of results to return")
	parser.add_argument("--skip", type=int, default=0, help="Number of results to skip")
	parser.add_argument("--json", action="store_true", help="Output results as JSON {urls: [...], count: N}")
	return parser.parse_args()


def search(query: str, limit: int, skip: int = 0):
	limit = max(1, min(limit, MAX_SEARCH_LIMIT))
	docs = documents_collection()
	cursor = docs.find(
		{"$text": {"$search": query}},
		projection={"score": {"$meta": "textScore"}, "url": 1, "final_url": 1, "title": 1, "text_excerpt": 1},
	).sort([("score", {"$meta": "textScore"})]).skip(skip).limit(limit)
	return list(cursor)


def main() -> None:
	args = _parse_args()
	log.info(f"Search: q='{args.query}' limit={args.limit} skip={args.skip}")
	results = search(args.query, args.limit, args.skip)
	if args.json:
		urls = [(doc.get("final_url") or doc.get("url")) for doc in results]
		payload = {"urls": urls, "count": len(urls)}
		log.info(f"Found {payload['count']} results")
		print(json.dumps(payload, ensure_ascii=False))
		return
	for i, doc in enumerate(results, start=1):
		print(f"[{i}] {doc.get('title') or '(no title)'}")
		print(f"    URL: {doc.get('final_url') or doc.get('url')} | score={doc.get('score')}")
		excerpt = doc.get("text_excerpt") or ""
		if excerpt:
			print(f"    {excerpt[:300]}")
		print()
	log.info(f"Displayed {len(results)} results")


if __name__ == "__main__":
	main() 