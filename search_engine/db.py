from datetime import datetime
from typing import Dict, Any

from pymongo import MongoClient, ASCENDING, TEXT
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError, OperationFailure

from .config import MONGODB_URI, MONGODB_DB
from .logger import get_logger


log = get_logger(__name__)
_client: MongoClient | None = None
_db = None


def get_client() -> MongoClient:
	global _client
	if _client is None:
		log.info(f"Connecting to MongoDB at {MONGODB_URI}")
		_client = MongoClient(MONGODB_URI, appname="python-search-engine")
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


def postings_collection() -> Collection:
	return get_db()["postings"]


def terms_collection() -> Collection:
	return get_db()["terms"]


def _create_text_index(docs: Collection) -> None:
	"""Create the desired text index with robust conflict handling."""
	try:
		docs.create_index([
			("title", TEXT),
			("index_text", TEXT),
		],
			name="text_index_title_indextext",
			default_language="english",
			weights={"title": 8, "index_text": 1}
		)
		log.info("Text index ensured (title weight=8, index_text weight=1)")
	except OperationFailure as e:
		log.warning(f"Text index creation failed, attempting recovery: {e}")
		# Drop any existing text index and recreate
		try:
			for idx in docs.list_indexes():
				keys = [k[0] for k in idx.get("key", {}).items()] if isinstance(idx.get("key"), dict) else [k for k, _ in idx.get("key", [])]
				if any(k in ("_fts", "_ftsx") for k in keys) or idx.get("name") == "text_index_title_indextext":
					log.info(f"Dropping conflicting text index: {idx.get('name')}")
					docs.drop_index(idx.get("name"))
			# Recreate
			docs.create_index([
				("title", TEXT),
				("index_text", TEXT),
			],
				name="text_index_title_indextext",
				default_language="english",
				weights={"title": 8, "index_text": 1}
			)
			log.info("Recreated text index successfully")
		except Exception as e2:
			log.error(f"Failed to recover text index: {e2}")
			raise


def _ensure_collections_and_indexes(db) -> None:
	# Ensure primary documents collection and indexes
	docs = db["documents"]
	log.info("Ensuring indexes on collection 'documents'")
	docs.create_index([("url", ASCENDING)], unique=True, name="unique_url")
	_create_text_index(docs)
	log.info("Indexes ensured")

	# Ensure inverted-index collections exist and have indexes
	if "postings" not in db.list_collection_names():
		try:
			db.create_collection("postings")
			log.info("Created collection 'postings'")
		except Exception:
			pass
	if "terms" not in db.list_collection_names():
		try:
			db.create_collection("terms")
			log.info("Created collection 'terms'")
		except Exception:
			pass

	postings = db["postings"]
	terms = db["terms"]
	postings.create_index([("term", ASCENDING)], name="idx_term")
	postings.create_index([("term", ASCENDING), ("doc_url", ASCENDING)], unique=True, name="unique_term_doc")
	terms.create_index([("term", ASCENDING)], unique=True, name="unique_term")


def upsert_document(doc: Dict[str, Any]) -> None:
	docs = documents_collection()
	doc["updated_at"] = datetime.utcnow()
	try:
		res = docs.update_one({"url": doc["url"]}, {"$set": doc, "$setOnInsert": {"created_at": datetime.utcnow()}}, upsert=True)
		log.debug(f"Upserted document url={doc['url']} matched={res.matched_count} modified={res.modified_count} upserted_id={res.upserted_id}")
	except DuplicateKeyError:
		log.warning(f"Duplicate key on url={doc['url']}, retrying without upsert")
		docs.update_one({"url": doc["url"]}, {"$set": doc}) 