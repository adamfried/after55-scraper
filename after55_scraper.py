"""
after55.com Active Adult Properties Scraper → Supabase
=======================================================
v4 — Waits properly for JS-rendered listings, takes a debug screenshot,
and tries multiple strategies to find property links.
"""

import re
import time
import logging
import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ── Supabase credentials ──────────────────────────────────────────────────────
SUPABASE_URL = "https://qvaofqcvjrozsrbhixzc.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF2YW9mcWN2anJvenNyYmhpeHpjIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MzAzMTg2MCwiZXhwIjoyMDg4NjA3ODYwfQ.Y92P-jRJPrFfe9ZtDf0hO15qMvFdK0GqPLecDnXrlSE"
TABLE = "properties"

# ── Search URL — full US, Active Adult ───────────────────────────────────────
SEARCH_BASE = (
    "https://www.after55.com/search/specialties-active-adult"
    "?bounds=50.68745,23.1298,-73.54957,-123.77906"
    "&poly=-111.03693|50.16310,-112.39923|50.27557,-114.68439|50.38778"
    ",-117.54083|50.47176,-121.27619|50.52767,-124.44025|50.52767"
    ",-125.62677|50.47176,-125.62677|45.99726,-125.40704|44.29271"
    ",-124.48419|40.06158,-124.13263|38.56568,-123.16583|34.47069"
    ",-122.19904|31.37276,-120.96857|28.28541,-120.70490|27.78115"
    ",-120.13361|26.80484,-119.21076|24.98645,-118.33185|23.30229"
    ",-117.18927|21.84150,-54.43536|21.84150,-50.78790|21.92306"
    ",-47.05255|22.61441,-43.66876|23.70529,-40.68048|25.02627"
    ",-37.60431|27.31359,-35.18732|29.70751,-32.77033|33.11951"
    ",-30.88068|36.96779,-29.60626|40.86400,-29.07892|43.81898"
    ",-28.85919|45.29065,-28.85919|45.90560,-28.94708|46.42301"
    ",-29.21076|46.78531,-29.51837|47.05545,-30.00177|47.41351"
    ",-30.22150|47.59164,-31.53986|48.61867,-32.37482|49.25374"
    ",-33.78107|49.99389,-34.92365|50.58351,-36.06622|51.02785"
    ",-37.29669|51.52268,-38.35138|51.87676,-39.71369|52.25497"
    ",-41.25177|52.57661,-43.71271|52.94890,-46.70099|53.23918"
    ",-53.29279|53.60580,-57.02814|53.73597,-61.33478|53.81388"
    ",-65.55353|53.83982,-70.38751|53.89164,-84.36212|53.89164"
    ",-88.62482|53.86574,-93.10724|53.83982,-96.84259|53.76195"
    ",-100.49005|53.70997,-104.13751|53.63186,-106.64240|53.60580"
    ",-109.23517|53.52750,-110.59747|53.50137,-112.17951|53.44906"
    ",-113.62970|53.44906,-115.03595|53.39669,-116.13458|53.31800"
    ",-117.10138|53.26547,-118.06818|53.18654,-118.77130|53.13385"
    ",-120.52911|52.81630,-121.27619|52.70993,-122.50665|52.49642"
    ",-124.48419|52.12026,-125.31915|51.98514,-125.62677|51.74090"
    ",-125.62677|50.61140"
)

DELAY_SECONDS = 2.5
BATCH_SIZE    = 25
LOG_FILE      = "after55_scraper.log"
BASE_URL      = "https://www.after55.com"

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# ── Supabase ──────────────────────────────────────────────────────────────────
SUPA_HEADERS = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "return=minimal",
}

def insert_rows(rows):
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/{TABLE}",
        headers=SUPA_HEADERS, json=rows, timeout=30
    )
    if r.status_code in (200, 201):
        log.info(f"  ✓ Inserted {len(rows)} rows")
    else:
        log.error(f"  ✗ Insert failed {r.status_code}: {r.text[:300]}")


# ── Wait for JS-rendered listings ────────────────────────────────────────────
def wait_for_listings_to_render(page):
    """
    after55.com is a React app. We need to wait until actual property cards
    appear in the DOM — not just the initial HTML shell.
    Try several selectors that appear only after JS renders listings.
    """
    selectors_to_try = [
        # Property card links — the most direct signal
        "a[href*='/ca/'], a[href*='/tx/'], a[href*='/fl/'], a[href*='/ny/'], a[href*='/wa/']",
        # Heading inside a card
        "h2 a[href], h3 a[href]",
        # Generic: any link whose href is a short path with 7-char ID
        "a[href]",
    ]

    log.info("Waiting for listings to render (up to 30s)...")

    # First just wait for any <h2> to appear (property card titles)
    try:
        page.wait_for_selector("h2", timeout=20000)
        log.info("  <h2> elements detected — page has rendered content")
    except PWTimeout:
        log.warning("  No <h2> found after 20s")

    # Extra wait for React hydration
    page.wait_for_timeout(5000)

    # Log all h2 text to understand what rendered
    h2s = page.eval_on_selector_all("h2", "els => els.map(e => e.innerText.trim()).slice(0,10)")
    log.info(f"  H2 elements on page: {h2s}")

    # Log total anchor count
    n_links = page.eval_on_selector_all("a[href]", "els => els.length")
    log.info(f"  Total <a href> elements: {n_links}")

    # Dump ALL hrefs so we can see exactly what's there
    all_hrefs = page.eval_on_selector_all(
        "a[href]",
        "els => els.map(e => e.getAttribute('href')).filter(h => h && !h.startsWith('#')).slice(0, 50)"
    )
    log.info(f"  First 50 non-anchor hrefs: {all_hrefs}")

    # Save a screenshot for visual debugging
    try:
        page.screenshot(path="debug_screenshot.png", full_page=False)
        log.info("  Screenshot saved: debug_screenshot.png")
    except Exception as e:
        log.warning(f"  Screenshot failed: {e}")

    # Also dump a snippet of page HTML for debugging
    html_snippet = page.eval_on_selector("body", "el => el.innerHTML.slice(0, 3000)")
    log.info(f"  Body HTML snippet:\n{html_snippet}\n")


# ── Extract property URLs from current page ───────────────────────────────────
def extract_property_urls(page):
    all_hrefs = page.eval_on_selector_all(
        "a[href]",
        "els => els.map(e => e.getAttribute('href'))"
    )
    urls = []
    for href in all_hrefs:
        if not href:
            continue
        # Match /st/city/property-slug/7-8charid  e.g. /ca/los-angeles/some-place/abc1234
        if re.match(r"^/[a-z]{2}/[^/]+/[^/]+/[a-z0-9]{6,9}$", href):
            full = BASE_URL + href
            if full not in urls:
                urls.append(full)
    return urls


# ── Collect all listing URLs ──────────────────────────────────────────────────
def collect_listing_urls(page):
    all_urls = []
    search_page = 1

    while True:
        if search_page == 1:
            url = SEARCH_BASE
        else:
            base_path = SEARCH_BASE.split("?")[0]
            query     = SEARCH_BASE.split("?")[1]
            url = f"{base_path}/page-{search_page}?{query}"

        log.info(f"\n--- Search page {search_page} ---")
        try:
            resp = page.goto(url, wait_until="domcontentloaded", timeout=45000)
            log.info(f"Status: {resp.status if resp else '?'}  Final URL: {page.url[:80]}")
        except PWTimeout:
            log.warning("Timeout loading search page. Stopping.")
            break

        # On first page, do full debug logging
        if search_page == 1:
            wait_for_listings_to_render(page)
        else:
            page.wait_for_timeout(7000)

        new_urls = extract_property_urls(page)
        log.info(f"Property URLs found on this page: {len(new_urls)}")
        for u in new_urls[:5]:
            log.info(f"  {u}")

        if not new_urls:
            log.info("No listings found — stopping pagination.")
            break

        all_urls.extend(new_urls)
        log.info(f"Running total: {len(all_urls)}")

        # Check for next page link
        page_nums = []
        for href in page.eval_on_selector_all("a[href*='page-']", "els => els.map(e => e.getAttribute('href'))"):
            m = re.search(r"page-(\d+)", href or "")
            if m:
                page_nums.append(int(m.group(1)))
        log.info(f"Pagination pages found: {sorted(set(page_nums))}")

        if not page_nums or search_page >= max(page_nums):
            log.info("No more pages.")
            break

        search_page += 1
        time.sleep(DELAY_SECONDS)

    return list(dict.fromkeys(all_urls))


# ── Scrape one property page ──────────────────────────────────────────────────
def scrape_listing(page, url):
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(3000)
    except PWTimeout:
        log.warning(f"Timeout: {url}")
        return None

    content = page.content()
    data = {"listing_url": url}

    h1 = page.query_selector("h1")
    data["name"] = h1.inner_text().strip() if h1 else ""

    # Address
    full_addr = ""
    for el in page.query_selector_all("p, div, span"):
        try:
            t = el.inner_text().strip()
            if re.search(r"\d{5}", t) and 10 < len(t) < 90:
                full_addr = t
                break
        except Exception:
            continue
    m = re.match(r"^(.*?),\s*(.*?),\s*([A-Z]{2})\s+(\d{5})", full_addr)
    if m:
        data["street"] = m.group(1).strip()
        data["city"]   = m.group(2).strip()
        data["state"]  = m.group(3).strip()
        data["zip"]    = m.group(4).strip()
    else:
        data["street"] = full_addr
        data["city"] = data["state"] = data["zip"] = ""

    # Rent / beds / baths / sqft
    tbl = page.query_selector("table")
    if tbl:
        cells = [c.inner_text().strip() for c in tbl.query_selector_all("td")]
        data["rent_range"] = cells[0] if len(cells) > 0 else ""
        data["bedrooms"]   = cells[1] if len(cells) > 1 else ""
        data["bathrooms"]  = cells[2] if len(cells) > 2 else ""
        data["sqft_range"] = cells[3] if len(cells) > 3 else ""
    else:
        data.update({"rent_range":"","bedrooms":"","bathrooms":"","sqft_range":""})

    def re_int(pat, s):
        m = re.search(pat, s); return int(m.group(1)) if m else None
    def re_str(pat, s):
        m = re.search(pat, s); return m.group(1).strip() if m else ""

    # Property info
    prop_info = ""
    for el in page.query_selector_all("p, li, div"):
        try:
            t = el.inner_text().strip()
            if "Built in" in t and "units" in t:
                prop_info = t; break
        except Exception:
            continue
    data["year_built"] = re_str(r"Built in (\d{4})", prop_info)
    data["unit_count"] = re_int(r"(\d+)\s+units", prop_info)
    data["stories"]    = re_int(r"(\d+)\s+stor", prop_info)

    data["application_fee"] = re_int(r"Application Fee[^$]*\$(\d+)", content)
    pt = re.search(r"(No Pets Allowed|Dogs Allowed|Cats Allowed|Pets Allowed)", content)
    data["pet_policy"] = pt.group(1) if pt else ""

    lm = re.search(r"Lease Term[^<]*Options.*?<li[^>]*>([^<]+)</li>", content, re.DOTALL)
    data["lease_terms"] = lm.group(1).strip() if lm else ""

    af = re.search(r"Apartment Features.*?<ul[^>]*>(.*?)</ul>", content, re.DOTALL)
    data["apartment_features"] = re.findall(r"<li[^>]*>([^<]+)</li>", af.group(1)) if af else []

    cf = re.search(r"Community Features.*?<ul[^>]*>(.*?)</ul>", content, re.DOTALL)
    data["community_features"] = re.findall(r"<li[^>]*>([^<]+)</li>", cf.group(1)) if cf else []

    hospitals = []
    for name, commute in re.findall(
        r'href="/hospital/[^"]+">([^<]+)</a>.*?<td[^>]*>(Drive:[^<]+)</td>',
        content, re.DOTALL
    )[:5]:
        hospitals.append({"name": name.strip(), "commute": commute.strip()})
    data["hospitals"] = hospitals

    for key, label in [("walk_score","Walk Score"),("transit_score","Transit Score"),
                       ("bike_score","Bike Score"),("sound_score","Soundscore")]:
        data[key] = re_int(rf"{label}[^0-9]*(\d+)\s*/\s*100", content)

    ph = re.search(r'tel:\+1(\d{10})', content)
    data["phone"] = f"({ph.group(1)[:3]}) {ph.group(1)[3:6]}-{ph.group(1)[6:]}" if ph else ""

    return data


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--disable-setuid-sandbox",
                "--window-size=1280,900",
            ]
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
            locale="en-US",
            timezone_id="America/New_York",
        )
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        page = context.new_page()

        log.info("=== Phase 1: Collecting listing URLs ===")
        all_urls = collect_listing_urls(page)
        log.info(f"\nTotal unique listings: {len(all_urls)}")

        if not all_urls:
            log.error("Zero listings found — review the debug output above.")
            browser.close()
            return

        log.info("\n=== Phase 2: Scraping property pages ===")
        batch, total = [], 0
        for i, url in enumerate(all_urls, 1):
            log.info(f"[{i}/{len(all_urls)}] {url}")
            prop = scrape_listing(page, url)
            if prop:
                batch.append(prop)
                log.info(f"  → {prop.get('name','?')} | {prop.get('city','?')}, {prop.get('state','?')}")
            time.sleep(DELAY_SECONDS)
            if len(batch) >= BATCH_SIZE:
                insert_rows(batch)
                total += len(batch)
                batch = []
        if batch:
            insert_rows(batch)
            total += len(batch)

        browser.close()
    log.info(f"\nDone. {total} properties inserted.")


if __name__ == "__main__":
    main()
