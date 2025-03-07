# Database of Subscribable Feeds for Feeeed

This repository contains a pipeline for generating a structured feed database for the Feeeed app.

## Pipeline Overview

The feed database is generated through a pipeline that:
1. Collects feeds from various sources (OPML files, curated lists, etc.)
2. Validates and deduplicates feeds
3. Enriches feeds by fetching them (subsequent pipeline runs will use cached data)
4. Labels feeds with categories and quality signals using an LLM (subsequent pipeline runs will use cached data)
5. Organizes feeds into a hierarchical category tree

## Usage

_All steps of the pipeline are designed to be idempotent, and cache expensive computations so they can be-run when something upstrea changes._

1. **Update data sources**
   See various scripts in `ingest-scripts/`

2. **Run the pipeline**:
   ```
   python pipeline.py
   ```

3. **Browse and curate**:
   ```
   python browser.py
   ```
   Access the web interface at http://localhost:5000

4. **Generate category tree**:
   ```
   python make_tree.py
   ```

## Installation

- Requires Python 3
- Install dependencies: `pip install Flask feedparser ollama chromadb`

## Key Files

- `pipeline.py`: Main processing pipeline
- `browser.py`: Web interface for feed curation
- `make_tree.py`: Generates final category tree
- `categories.json`: Defines category hierarchy
- `raw_data/`: Source feed data
- `generated/`: Output files