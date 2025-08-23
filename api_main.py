from fastapi import FastAPI, Query, HTTPException
from typing import List
import subprocess
import sys
import json

from search_engine.config import DEFAULT_SEARCH_LIMIT, MAX_SEARCH_LIMIT
from search_engine.logger import get_logger

app = FastAPI(title="Python MongoDB Search API", version="0.1.0")
log = get_logger(__name__)


@app.get("/search")
def search_endpoint(
	q: str = Query(..., description="Search query"),
	limit: int = Query(DEFAULT_SEARCH_LIMIT, ge=1, le=MAX_SEARCH_LIMIT),
	skip: int = Query(0, ge=0),
):
	log.info(f"API /search q='{q}' limit={limit} skip={skip}")
	# Delegate to run_search.py to keep a single source of truth for search behavior
	cmd = [sys.executable, "run_search.py", "--query", q, "--limit", str(limit), "--skip", str(skip), "--json"]
	log.debug(f"Running subprocess: {' '.join(cmd)}")
	proc = subprocess.run(cmd, capture_output=True, text=True)
	if proc.returncode != 0:
		log.error(f"Search subprocess failed: code={proc.returncode} stderr={proc.stderr.strip()}")
		raise HTTPException(status_code=500, detail=proc.stderr.strip() or "Search process failed")
	try:
		payload = json.loads(proc.stdout.strip() or "{}")
		if not isinstance(payload, dict) or "urls" not in payload:
			raise ValueError("Malformed search output")
		log.info(f"API /search returning {payload.get('count', 0)} URLs")
		return payload
	except Exception as e:
		log.error(f"Search output parse error: {e}")
		raise HTTPException(status_code=500, detail=f"Invalid search output: {e}")


if __name__ == "__main__":
	import uvicorn
	uvicorn.run("api_main:app", host="0.0.0.0", port=8000, reload=False) 