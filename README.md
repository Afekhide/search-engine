## Python + MongoDB Search Engine (Full-Text, No PageRank)

This project is a minimal, medium-scale search engine built with Python and MongoDB. It uses MongoDB's full-text search and a lightweight indexing pipeline with stemming and lemmatization via NLTK. No PageRank is used.

### Features
- Full-text search using MongoDB `$text`
- Basic token normalization (lowercasing, stopword removal, lemmatization, stemming)
- Simple web crawler (BFS) with domain restriction option
- Weighted text index (`title` > `index_text`)
- REST API via FastAPI (`/search`, `/crawl`)
- CLI for crawling and searching

### Requirements
- Python 3.9+
- MongoDB (local or Atlas)

### Setup
1. Create a virtualenv and install dependencies:
```bash
python -m venv .venv
. .venv/Scripts/Activate.ps1  # PowerShell on Windows
pip install -r requirements.txt
```

2. Ensure MongoDB is running locally or set an environment variable for your connection string. Defaults are:
- `MONGODB_URI`: `mongodb://localhost:27017`
- `MONGODB_DB`: `search_engine`

Optionally create a `.env` file:
```bash
MONGODB_URI=mongodb://localhost:27017
MONGODB_DB=search_engine
```

3. Run the API server:
```bash
python cli.py serve --host 0.0.0.0 --port 8000
```
Open `http://localhost:8000/docs` for Swagger UI.

### CLI Usage
- Crawl and index a few pages:
```bash
python cli.py crawl --seeds https://example.com --max-pages 50 --same-domain-only
```

- Search:
```bash
python cli.py search --query "example term" --limit 10
```

### API
- `GET /search?q=term&limit=10&skip=0`
- `POST /crawl` with JSON body `{ "seeds": ["https://example.com"], "max_pages": 50, "same_domain_only": true }`

### Notes
- On first run, NLTK resources are auto-downloaded programmatically (no manual steps required).
- Indexes are created automatically on startup (`title`, `index_text` with weights).

### License
MIT 
