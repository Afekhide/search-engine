from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from typing import Iterable, List

import nltk
from nltk.corpus import stopwords, wordnet
from nltk.stem import PorterStemmer, WordNetLemmatizer

from .logger import get_logger


log = get_logger(__name__)

NLTK_RESOURCES = [
	("corpora", "stopwords"),
	("corpora", "wordnet"),
	("tokenizers", "punkt"),
]

# Fallback stopwords if NLTK fails
FALLBACK_STOPWORDS = {
	"a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "as", "at",
	"be", "because", "been", "before", "being", "below", "between", "both", "but", "by",
	"could", "did", "do", "does", "doing", "down", "during",
	"each", "few", "for", "from", "further",
	"had", "has", "have", "having", "he", "her", "here", "hers", "herself", "him", "himself", "his", "how",
	"i", "if", "in", "into", "is", "it", "its", "itself",
	"just",
	"me", "more", "most", "my", "myself",
	"no", "nor", "not", "now",
	"of", "off", "on", "once", "only", "or", "other", "our", "ours", "ourselves", "out", "over", "own",
	"same", "she", "should", "so", "some", "such",
	"than", "that", "the", "their", "theirs", "them", "themselves", "then", "there", "these", "they", "this", "those", "through", "to", "too",
	"under", "until", "up",
	"very",
	"was", "we", "were", "what", "when", "where", "which", "while", "who", "whom", "why", "will", "with", "would",
	"you", "your", "yours", "yourself", "yourselves"
}


def _ensure_nltk() -> None:
	"""Download NLTK resources if they don't exist."""
	log.info("Ensuring NLTK resources are available")
	for resource_type, resource_name in NLTK_RESOURCES:
		try:
			nltk.data.find(f"{resource_type}/{resource_name}")
			log.debug(f"NLTK resource {resource_name} already available")
		except LookupError:
			log.info(f"Downloading NLTK resource: {resource_name}")
			try:
				nltk.download(resource_name, quiet=True)
				log.info(f"Successfully downloaded NLTK resource: {resource_name}")
			except Exception as e:
				log.error(f"Failed to download NLTK resource {resource_name}: {e}")
				raise


def _ensure_wordnet_loaded() -> None:
	"""Ensure WordNet is fully loaded for multi-threaded use."""
	try:
		wordnet.ensure_loaded()
		log.debug("WordNet ensure_loaded() completed")
	except Exception as e:
		log.warning(f"WordNet ensure_loaded() failed: {e}")


@dataclass
class NormalizedText:
	original_text: str
	tokens: List[str]
	joined: str


def _get_stopwords_set() -> set[str]:
	"""Get English stopwords set, ensuring NLTK resources are available first."""
	_ensure_nltk()
	try:
		# Try multiple approaches to load stopwords
		try:
			# Method 1: Direct access
			stop_words = set(stopwords.words("english"))
			log.debug(f"Loaded {len(stop_words)} stopwords from NLTK")
			return stop_words
		except Exception as e1:
			log.warning(f"Method 1 failed: {e1}")
			try:
				# Method 2: Try downloading again and accessing
				nltk.download("stopwords", quiet=True)
				stop_words = set(stopwords.words("english"))
				log.debug(f"Loaded {len(stop_words)} stopwords from NLTK (retry)")
				return stop_words
			except Exception as e2:
				log.warning(f"Method 2 failed: {e2}")
				# Method 3: Manual access to the file
				try:
					from nltk.data import find
					stopwords_path = find("corpora/stopwords")
					with open(f"{stopwords_path}/english", "r", encoding="utf-8") as f:
						stop_words = set(line.strip() for line in f if line.strip())
					log.debug(f"Loaded {len(stop_words)} stopwords from file")
					return stop_words
				except Exception as e3:
					log.warning(f"Method 3 failed: {e3}")
					raise
	except Exception as e:
		log.error(f"All methods failed to load stopwords: {e}")
		log.info("Using fallback stopwords list")
		return FALLBACK_STOPWORDS


_word_pattern = re.compile(r"[A-Za-z][A-Za-z\-']+")


def tokenize(text: str) -> List[str]:
	# Simple pattern-based tokenizer (faster than full punkt in many cases)
	return _word_pattern.findall(text.lower())


def normalize_text_for_index(text: str) -> NormalizedText:
	"""Normalize text for indexing with stemming and lemmatization."""
	_ensure_nltk()
	_ensure_wordnet_loaded()
	
	stop = _get_stopwords_set()
	lemmatizer = WordNetLemmatizer()
	stemmer = PorterStemmer()

	raw_tokens = tokenize(text)
	filtered_tokens = [t for t in raw_tokens if t not in stop and len(t) > 1]
	lemmatized = [lemmatizer.lemmatize(t) for t in filtered_tokens]
	stemmed = [stemmer.stem(t) for t in lemmatized]
	joined = " ".join(stemmed)
	
	log.debug(f"Normalized text: {len(raw_tokens)} raw tokens → {len(filtered_tokens)} filtered → {len(stemmed)} stemmed")
	return NormalizedText(original_text=text, tokens=stemmed, joined=joined)


def summarize_text(text: str, max_chars: int = 300) -> str:
	clean = re.sub(r"\s+", " ", text).strip()
	if len(clean) <= max_chars:
		return clean
	return clean[: max_chars - 1] + "…" 