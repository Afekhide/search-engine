from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from typing import Iterable, List

import nltk
from nltk.stem import PorterStemmer

from .logger import get_logger


log = get_logger(__name__)

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


@dataclass
class NormalizedText:
	original_text: str
	tokens: List[str]
	joined: str


@lru_cache(maxsize=1)
def _get_stopwords_set() -> set[str]:
	"""Load English stopwords directly from the corpus file; fall back if unavailable."""
	try:
		from nltk.data import find
		path = find("corpora/stopwords/english")
		with open(path, "r", encoding="utf-8") as f:
			words = {line.strip() for line in f if line.strip()}
			if words:
				return words
	except Exception as e:
		log.warning(f"Using built-in stopwords due to error: {e}")
	return FALLBACK_STOPWORDS


_word_pattern = re.compile(r"[A-Za-z][A-Za-z\-']+")


def tokenize(text: str) -> List[str]:
	# Simple pattern-based tokenizer (faster than full punkt in many cases)
	return _word_pattern.findall(text.lower())


@lru_cache(maxsize=50000)
def normalize_token(token: str) -> str:
	"""Normalize a single token quickly using stemming only (no WordNet)."""
	stemmer = PorterStemmer()
	return stemmer.stem(token)


def normalize_text_for_index(text: str) -> NormalizedText:
	"""Normalize text for indexing using stopword removal + stemming (no WordNet)."""
	stop = _get_stopwords_set()
	stemmer = PorterStemmer()

	raw_tokens = tokenize(text)
	filtered_tokens = [t for t in raw_tokens if t not in stop and len(t) > 1]
	stemmed = [stemmer.stem(t) for t in filtered_tokens]
	joined = " ".join(stemmed)
	return NormalizedText(original_text=text, tokens=stemmed, joined=joined)


def summarize_text(text: str, max_chars: int = 300) -> str:
	clean = re.sub(r"\s+", " ", text).strip()
	if len(clean) <= max_chars:
		return clean
	return clean[: max_chars - 1] + "…" 