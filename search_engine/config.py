import os
from typing import Any, Dict
from pathlib import Path

from dotenv import load_dotenv

try:  
	import tomllib as toml
except Exception:
	import tomli as toml

load_dotenv()

# Resolve config file path
CONFIG_PATH = Path(os.getenv("CONFIG_TOML", "config.toml"))


def _load_toml(path: Path) -> Dict[str, Any]:
	if not path.exists():
		return {}
	with path.open("rb") as f:
		return toml.load(f)


_cfg = _load_toml(CONFIG_PATH)

# Database
_db = _cfg.get("dbconfig", {}) if isinstance(_cfg, dict) else {}
MONGODB_URI = os.getenv("MONGODB_URI", _db.get("uri", "mongodb://localhost:27017"))
MONGODB_DB = os.getenv("MONGODB_DB", _db.get("database", "search_engine"))

# Thread pools
_threads = _cfg.get("threadpoolconfig", {}) if isinstance(_cfg, dict) else {}
CRAWLER_WORKERS = int(os.getenv("CRAWLER_WORKERS", str(_threads.get("crawler_workers", 8))))
INDEXER_WORKERS = int(os.getenv("INDEXER_WORKERS", str(_threads.get("indexer_workers", 8))))

# Indexer
_indexer = _cfg.get("indexerconfig", {}) if isinstance(_cfg, dict) else {}
INDEX_BULK_BATCH_SIZE = int(os.getenv("INDEX_BULK_BATCH_SIZE", str(_indexer.get("bulk_batch_size", 200))))
INDEX_MAX_PAGES_PER_RUN = int(os.getenv("INDEX_MAX_PAGES_PER_RUN", str(_indexer.get("max_pages_per_run", 0))))
INDEX_EXCERPT_MAX_CHARS = int(os.getenv("INDEX_EXCERPT_MAX_CHARS", str(_indexer.get("excerpt_max_chars", 400))))

# Crawler
_crawl = _cfg.get("crawler", {}) if isinstance(_cfg, dict) else {}
HTTP_TIMEOUT_SECS = int(os.getenv("HTTP_TIMEOUT_SECS", str(_crawl.get("http_timeout_secs", 15))))
HTTP_MAX_CONTENT_MB = int(os.getenv("HTTP_MAX_CONTENT_MB", str(_crawl.get("http_max_content_mb", 5))))
CRAWL_DELAY_SECS = float(os.getenv("CRAWL_DELAY_SECS", str(_crawl.get("crawl_delay_secs", 0.0))))
SAME_DOMAIN_ONLY_DEFAULT = bool(_crawl.get("same_domain_only", True))

# Search
_search = _cfg.get("search", {}) if isinstance(_cfg, dict) else {}
DEFAULT_SEARCH_LIMIT = int(os.getenv("DEFAULT_SEARCH_LIMIT", str(_search.get("default_limit", 10))))
MAX_SEARCH_LIMIT = int(os.getenv("MAX_SEARCH_LIMIT", str(_search.get("max_limit", 50)))) 