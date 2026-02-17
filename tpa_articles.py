import asyncio
import csv
import json
import os
import re
import urllib.request
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import datetime, timezone

import aiohttp

BASE_URL = "https://thepaymentsassociation.org"
SITEMAP_NS = 'http://www.sitemaps.org/schemas/sitemap/0.9'
CUTOFF_YEAR = datetime.now(timezone.utc).year - 3
semaphore = asyncio.Semaphore(20)

HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}

ARTICLE_TYPE_OVERRIDES = {
    'thought-leadership-quarterly': 'Thought Leadership',
    'thought-leadership': 'Thought Leadership',
    'payments-intelligence': 'Payments Intelligence',
}


def fetch_xml(url, retries=3, backoff=5):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req) as r:
                return ET.fromstring(r.read())
        except Exception as e:
            if attempt < retries - 1:
                wait = backoff * (attempt + 1)
                print(f"  Retrying {url} in {wait}s ({e})")
                import time; time.sleep(wait)
            else:
                raise


def get_article_urls_from_sitemap():
    """Fetch all article URLs directly from the site's XML sitemaps."""
    print("=== Phase 1: Fetching article URLs from sitemap ===")

    root = fetch_xml(BASE_URL + '/sitemap.xml')

    post_sitemaps = [
        loc.text for loc in root.findall(f'.//{{{SITEMAP_NS}}}loc')
        if 'post-sitemap' in (loc.text or '')
    ]
    print(f"Found {len(post_sitemaps)} post sitemaps")

    article_urls = set()
    for sitemap_url in post_sitemaps:
        sm = fetch_xml(sitemap_url)
        for loc in sm.findall(f'.//{{{SITEMAP_NS}}}loc'):
            url = (loc.text or '').rstrip('/')
            if '/article/' in url:
                article_urls.add(url)

    print(f"Found {len(article_urls)} articles total\n")
    return list(article_urls)


def extract_jsonld(html):
    """Return (article_dict, breadcrumb_category) from the page's JSON-LD."""
    for match in re.finditer(
        r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL
    ):
        try:
            data = json.loads(match.group(1))
            if isinstance(data, dict) and '@graph' in data:
                graph = data['@graph']
            elif isinstance(data, list):
                graph = data
            else:
                continue

            article = next((d for d in graph if 'datePublished' in d), None)

            # BreadcrumbList: items[0]=Home, items[1]=section (e.g. "Industry News")
            breadcrumb = next((d for d in graph if d.get('@type') == 'BreadcrumbList'), None)
            category = None
            if breadcrumb:
                items = breadcrumb.get('itemListElement', [])
                if len(items) >= 2:
                    category = items[1].get('name')

            if article:
                return article, category

        except (json.JSONDecodeError, AttributeError):
            continue
    return {}, None


async def scrape_article(url, session, retries=3):
    for attempt in range(retries):
        async with semaphore:
            try:
                timeout = aiohttp.ClientTimeout(total=20)
                async with session.get(url, headers=HEADERS, timeout=timeout) as resp:
                    if resp.status != 200:
                        print(f"✗ {url}: HTTP {resp.status}")
                        return None
                    html = await resp.text()

                data, breadcrumb_category = extract_jsonld(html)

                # Title
                title_match = re.search(r'<title>(.*?)</title>', html)
                title = data.get('headline') or (title_match.group(1) if title_match else 'Unknown')
                title = title.replace(' | The Payments Association', '').strip()

                # Publication date
                date_str = data.get('datePublished', '')
                pub_date = datetime.fromisoformat(date_str) if date_str else None

                # Author
                author_data = data.get('author', {})
                if isinstance(author_data, list):
                    author_data = author_data[0] if author_data else {}
                author = author_data.get('name', 'Unknown') if isinstance(author_data, dict) else 'Unknown'

                # Article type — three-strategy fallback chain
                article_type = 'Unknown'
                container_match = re.search(r'class="([^"]*elementor-location-single[^"]*)"', html)

                # Strategy 1: filter_types-* on the main article container (Thought Leadership etc.)
                if container_match:
                    type_match = re.search(r'filter_types-([a-z0-9-]+)', container_match.group(1))
                    if type_match:
                        raw = type_match.group(1)
                        article_type = ARTICLE_TYPE_OVERRIDES.get(
                            raw, ' '.join(w.capitalize() for w in raw.split('-'))
                        )

                # Strategy 2: BreadcrumbList second item from JSON-LD (free — already parsed)
                if article_type == 'Unknown' and breadcrumb_category:
                    article_type = breadcrumb_category

                # Strategy 3: category-* class on the main article container
                if article_type == 'Unknown' and container_match:
                    cat_match = re.search(r'\bcategory-([a-z0-9-]+)\b', container_match.group(1))
                    if cat_match:
                        article_type = ' '.join(w.capitalize() for w in cat_match.group(1).split('-'))

                # Status
                if pub_date:
                    status = 'DELETE_CANDIDATE' if pub_date.year < CUTOFF_YEAR else 'KEEP'
                    reason = f'Published {pub_date.strftime("%b %Y")}{"  — over 3 years old" if status == "DELETE_CANDIDATE" else ""}'
                else:
                    status = 'REVIEW'
                    reason = 'Could not determine publish date'

                result = {
                    'url': url,
                    'title': title,
                    'author': author,
                    'published_date': pub_date.strftime('%Y-%m-%d') if pub_date else 'Unknown',
                    'article_type': article_type,
                    'status': status,
                    'reason': reason,
                }

                icon = '🔴' if status == 'DELETE_CANDIDATE' else '🟢' if status == 'KEEP' else '🟠'
                print(f"{icon} [{article_type}] {result['published_date']} — {title[:60]}")
                return result

            except (asyncio.TimeoutError, aiohttp.ServerDisconnectedError) as e:
                if attempt < retries - 1:
                    wait = 2 ** attempt
                    print(f"  ⟳ Timeout on attempt {attempt + 1}, retrying in {wait}s — {url[-60:]}")
                    await asyncio.sleep(wait)
                else:
                    print(f"✗ {url}: gave up after {retries} attempts")
                    return None

            except Exception as e:
                print(f"✗ {url}: {type(e).__name__}: {e}")
                return None


async def main():
    article_list = get_article_urls_from_sitemap()
    total = len(article_list)
    completed = 0

    print(f"=== Phase 2: Scraping {total} articles ({semaphore._value} concurrent, no browser) ===\n")

    connector = aiohttp.TCPConnector(limit=20, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:

        async def scrape_and_track(url):
            nonlocal completed
            result = await scrape_article(url, session)
            completed += 1
            if completed % 200 == 0:
                print(f"  ↳ Progress: {completed}/{total} ({completed * 100 // total}%)")
            return result

        raw = await asyncio.gather(*[scrape_and_track(url) for url in article_list])
        all_results = [r for r in raw if r]

    all_results.sort(key=lambda x: x['published_date'])

    date_str = datetime.now().strftime('%Y-%m-%d')
    output_dir = os.path.join('data', 'tpa-audit', date_str)
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'tpa_articles_audit.csv')

    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['url', 'title', 'author', 'published_date', 'article_type', 'status', 'reason'])
        writer.writeheader()
        writer.writerows(all_results)

    counts = Counter(r['status'] for r in all_results)
    type_counts = Counter(r['article_type'] for r in all_results)
    print(f"\n✅ Done! {len(all_results)} articles saved to {output_path}")
    print(f"  🔴 DELETE_CANDIDATE : {counts.get('DELETE_CANDIDATE', 0)}")
    print(f"  🟠 REVIEW           : {counts.get('REVIEW', 0)}")
    print(f"  🟢 KEEP             : {counts.get('KEEP', 0)}")
    print("\nArticle types:")
    for article_type, count in type_counts.most_common():
        print(f"  {article_type:<30} {count}")


asyncio.run(main())
