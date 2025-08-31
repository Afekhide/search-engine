import argparse
import json
from pathlib import Path
from typing import Iterable, List

from search_engine.indexer import index_pages, index_pages_parallel
from search_engine.fetch_contents import FetchedPage
from search_engine.config import INDEXER_WORKERS, INDEX_BULK_BATCH_SIZE
from search_engine.logger import get_logger


log = get_logger(__name__)


def _read_jsonl(path: Path) -> Iterable[FetchedPage]:
	with path.open("r", encoding="utf-8") as f:
		for line in f:
			line = line.strip()
			if not line:
				continue
			obj = json.loads(line)
			yield FetchedPage(
				url=obj.get("url", ""),
				final_url=obj.get("final_url", obj.get("url", "")),
				title=obj.get("title", ""),
				text=obj.get("text", ""),
				html=obj.get("html", ""),
			)


def _parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Index crawled JSONL into MongoDB")
	parser.add_argument("--input", required=True, help="Path to JSONL file produced by run_fetch_contents.py")
	parser.add_argument("--parallel", action="store_true", help="Use multi-threaded indexer")
	parser.add_argument("--workers", type=int, default=INDEXER_WORKERS, help="Number of worker threads when using --parallel")
	parser.add_argument("--batch-size", type=int, default=INDEX_BULK_BATCH_SIZE, help="Bulk write batch size")
	return parser.parse_args()


def main() -> None:
	args = _parse_args()
	path = Path(args.input)
	if not path.exists():
		raise SystemExit(f"Input file not found: {path}")

	log.info(f"Indexing input file: {path}")

	pages_iter = _read_jsonl(path)

	if args.parallel:
		log.info(f"Parallel indexing enabled: workers={args.workers} batch_size={args.batch_size}")
		stats = index_pages_parallel(pages_iter, batch_size=args.batch_size, max_workers=args.workers)
	else:
		log.info(f"Sequential indexing: batch_size={args.batch_size}")
		stats = index_pages(pages_iter, batch_size=args.batch_size)

	log.info(f"Indexing stats: {stats}")


if __name__ == "__main__":
	main() 