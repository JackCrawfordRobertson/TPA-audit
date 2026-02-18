# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Purpose

This is a content audit toolset for [thepaymentsassociation.org](https://thepaymentsassociation.org). It crawls the live site and produces CSVs categorising pages as `KEEP`, `DELETE_CANDIDATE`, `DUPLICATE`, or `REVIEW`. Content older than 3 years (`CUTOFF_YEAR = current year - 3`) is flagged for deletion.

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

## Running the scripts

```bash
# Crawl general site pages (uses Playwright headless browser, ~10 concurrent)
python tpa_crawl.py

# Audit articles via XML sitemap (uses aiohttp, ~20 concurrent, no browser)
python tpa_articles.py
```

Output is written to `data/tpa-audit/YYYY-MM-DD/` with the filename `tpa_audit.csv` (crawl) or `tpa_articles_audit.csv` (articles).

## Architecture

**`tpa_crawl.py`** — BFS crawler using Playwright (needed for JS-rendered pages):
- Starts from `BASE_URL`, crawls in batches of 10, capped at 2 path segments deep (`is_top_level`)
- Skips URL patterns like `/events/tag/`, `/page/`, `/author/`, date-based paths, etc.
- Categorises each discovered URL via `categorise()` using path-based heuristics
- Hardcoded `duplicate_pairs` list handles known `/members/*` path duplication

**`tpa_articles.py`** — Article-specific auditor using the site's XML sitemap:
- Phase 1: Fetches `/sitemap.xml`, finds all `post-sitemap*.xml` entries, extracts `/article/` URLs
- Phase 2: Scrapes each article concurrently with aiohttp, no browser required
- Extracts metadata via JSON-LD (`@graph` structure) — title, publish date, author
- Three-strategy fallback for `article_type`: (1) `filter_types-*` CSS class on article container, (2) BreadcrumbList second item from JSON-LD, (3) `category-*` CSS class
- `ARTICLE_TYPE_OVERRIDES` dict normalises raw CSS slug names to display labels

## Data

- `data/RawDB/wp_8i_posts.csv` — raw WordPress posts export (reference data, ~19MB)
- `data/tpa-audit/YYYY-MM-DD/` — dated output directories from each run
