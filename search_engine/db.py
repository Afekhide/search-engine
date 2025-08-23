from datetime import datetime
from typing import Dict, Any

from pymongo import MongoClient, ASCENDING, TEXT
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError

from .config import MONGODB_URI, MONGODB_DB
from .logger import get_logger


log = get_logger(__name__)
_client: MongoClient | None = None
_db = None


def get_client() -> MongoClient:
	global _client
	if _client is None:
		log.info(f"Connecting to MongoDB at {MONGODB_URI}")
		_client = MongoClient(MONGODB_URI, appname="search-engine")
	return _client


def get_db():
	global _db
	if _db is None:
		_db = get_client()[MONGODB_DB]
		log.info(f"Using database '{MONGODB_DB}'")
		_ensure_collections_and_indexes(_db)
	return _db


def documents_collection() -> Collection:
	return get_db()["documents"]


def _ensure_collections_and_indexes(db) -> None:
	docs = db["documents"]
	log.info("Ensuring indexes on collection 'documents'")
	# Unique URL for dedupe/upserts
	docs.create_index([("url", ASCENDING)], unique=True, name="unique_url")
	# Weighted text index: title boosted relative to index_text
	docs.create_index([
		("title", TEXT),
		("index_text", TEXT),
	],
		name="text_index_title_indextext",
		default_language="english",
		weights={"title": 5, "index_text": 1}
	)
	log.info("Indexes ensured")


def upsert_document(doc: Dict[str, Any]) -> None:
	docs = documents_collection()
	doc["updated_at"] = datetime.utcnow()
	try:
		res = docs.update_one({"url": doc["url"]}, {"$set": doc, "$setOnInsert": {"created_at": datetime.utcnow()}}, upsert=True)
		log.debug(f"Upserted document url={doc['url']} matched={res.matched_count} modified={res.modified_count} upserted_id={res.upserted_id}")
	except DuplicateKeyError:
		# Very rare due to upsert; fallback to a second attempt
		log.warning(f"Duplicate key on url={doc['url']}, retrying without upsert")
		docs.update_one({"url": doc["url"]}, {"$set": doc}) 