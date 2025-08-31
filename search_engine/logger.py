import logging
import os
from typing import Optional
from pathlib import Path

try:
	# Python 3.11+ tomllib
	import tomllib as toml
except Exception:  # pragma: no cover
	import tomli as toml  # type: ignore

_configured = False


def _read_level_from_toml() -> str:
	try:
		# Get config path directly without importing config module
		config_path = Path(os.getenv("CONFIG_TOML", "config.toml"))
		if not config_path.exists():
			return ""
		
		with open(config_path, "rb") as f:
			cfg = toml.load(f)
			lvl = ((cfg or {}).get("logging") or {}).get("level")
			if isinstance(lvl, str) and lvl:
				return lvl.upper()
	except Exception:
		return ""
	return ""


def _parse_level(value: Optional[str]) -> int:
	value = (value or "").upper()
	return getattr(logging, value, logging.INFO)


def setup_logging() -> None:
	global _configured
	if _configured:
		return
	# Env overrides TOML
	level_name = os.getenv("LOG_LEVEL") or _read_level_from_toml() or "INFO"
	level = _parse_level(level_name)
	formatter = logging.Formatter(fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
	root = logging.getLogger()
	root.setLevel(level)
	if not root.handlers:
		h = logging.StreamHandler()
		h.setFormatter(formatter)
		root.addHandler(h)
	_configured = True


def get_logger(name: Optional[str] = None) -> logging.Logger:
	setup_logging()
	return logging.getLogger(name or "search_engine") 