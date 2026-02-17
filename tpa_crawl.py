import asyncio
from playwright.async_api import async_playwright
from urllib.parse import urlparse
import re
import csv
import os
from datetime import datetime, timezone

BASE_URL = "https://thepaymentsassociation.org"
visited = set()
found_urls = []
semaphore = asyncio.Semaphore(10)

CUTOFF_YEAR = datetime.now(timezone.utc).year - 3

SKIP_PATTERNS = [
    r'/events/tag/',
    r'/events/category/',
    r'/day/',
    r'\d{4}-\d{2}$',
    r'\d{4}-\d{2}-\d{2}',
    r'\?',
    r'/page/',
    r'/feed/',
    r'/author/',
]

def should_skip(url):
    return any(re.search(pattern, url) for pattern in SKIP_PATTERNS)

def is_top_level(url):
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split('/') if p]
    return len(parts) <= 2

def categorise(url):
    path = urlparse(url).path.lower()
    
    # Check for duplicates pattern
    duplicate_pairs = [
        ('/members/become-a-member', '/become-a-member'),
        ('/members/purchase-membership', '/purchase-membership'),
        ('/members/terms-and-conditions', '/terms-and-conditions'),
    ]
    for pair in duplicate_pairs:
        if path == pair[0]:
            return 'DUPLICATE', f'Possible duplicate of {BASE_URL}{pair[1]}'

    # Gallery pages
    if '/gallery/' in path:
        year_match = re.search(r'(\d{4})', path)
        if year_match and int(year_match.group(1)) < CUTOFF_YEAR:
            return 'DELETE_CANDIDATE', f'Old gallery page from {year_match.group(1)}'
        return 'KEEP', 'Recent gallery page'

    # Past event pages
    if '/event/' in path:
        year_match = re.search(r'(\d{4})', path)
        if year_match and int(year_match.group(1)) < CUTOFF_YEAR:
            return 'DELETE_CANDIDATE', f'Past event from {year_match.group(1)}'
        return 'REVIEW', 'Past or upcoming event — check if still relevant'

    # Directory pages
    if '/directory/' in path:
        return 'REVIEW', 'Check if member is still active'

    # Filter/tag category pages
    if '/filter_categories/' in path or '/directory_cat/' in path:
        return 'REVIEW', 'Auto-generated category page — check if needed'

    # Webinar pages
    if '/webinar/' in path:
        year_match = re.search(r'(\d{4})', path)
        if year_match and int(year_match.group(1)) < CUTOFF_YEAR:
            return 'DELETE_CANDIDATE', f'Old webinar from {year_match.group(1)}'
        return 'KEEP', 'Webinar page'

    # Articles and whitepapers
    if '/article/' in path or '/whitepaper/' in path:
        return 'KEEP', 'Content page'

    # Core pages
    return 'KEEP', 'Core site page'

async def crawl_url(url, browser):
    async with semaphore:
        context = await browser.new_context()
        page = await context.new_page()
        new_links = []

        try:
            await page.goto(url, timeout=30000, wait_until="domcontentloaded")
            links = await page.eval_on_selector_all("a[href]", "els => els.map(el => el.href)")

            for link in links:
                parsed = urlparse(link)
                if parsed.netloc == "thepaymentsassociation.org":
                    clean = parsed.scheme + "://" + parsed.netloc + parsed.path.rstrip("/")
                    if not should_skip(clean) and is_top_level(clean):
                        if clean not in visited:
                            new_links.append(clean)

            print(f"✓ {url} — {len(links)} links found")

        except Exception as e:
            print(f"✗ {url}: {e}")
        finally:
            await context.close()

        return new_links

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        to_visit = [BASE_URL]

        while to_visit:
            batch = []
            while to_visit and len(batch) < 10:
                url = to_visit.pop()
                if url not in visited and not should_skip(url):
                    visited.add(url)
                    batch.append(url)

            if not batch:
                break

            print(f"\n--- Batch of {len(batch)} | Visited: {len(visited)} | Queue: {len(to_visit)} ---")

            results = await asyncio.gather(*[crawl_url(url, browser) for url in batch])

            for new_links in results:
                to_visit.extend(new_links)

        await browser.close()

    # Categorise all found URLs
    rows = []
    for url in sorted(visited):
        status, reason = categorise(url)
        rows.append({'url': url, 'status': status, 'reason': reason})

    # Sort by status so DELETE_CANDIDATE and DUPLICATE appear first
    status_order = {'DELETE_CANDIDATE': 0, 'DUPLICATE': 1, 'REVIEW': 2, 'KEEP': 3}
    rows.sort(key=lambda x: status_order.get(x['status'], 99))

    date_str = datetime.now().strftime('%Y-%m-%d')
    output_dir = os.path.join('data', 'tpa-audit', date_str)
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'tpa_audit.csv')

    with open(output_path, "w", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['url', 'status', 'reason'])
        writer.writeheader()
        writer.writerows(rows)

    # Summary
    from collections import Counter
    counts = Counter(r['status'] for r in rows)
    print(f"\nDone! Found {len(rows)} URLs saved to {output_path}")
    print(f"  DELETE_CANDIDATE : {counts.get('DELETE_CANDIDATE', 0)}")
    print(f"  DUPLICATE        : {counts.get('DUPLICATE', 0)}")
    print(f"  REVIEW           : {counts.get('REVIEW', 0)}")
    print(f"  KEEP             : {counts.get('KEEP', 0)}")

asyncio.run(main())
