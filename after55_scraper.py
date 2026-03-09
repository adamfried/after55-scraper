"""
after55.com Active Adult Properties Scraper → Supabase
=======================================================
Uses Playwright (real Chromium browser) to bypass 403 bot protection.
Scrapes all 750 active adult properties across the US and inserts
them into your Supabase `properties` table.
"""

import re
import time
import json
import logging
import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ── Supabase credentials ──────────────────────────────────────────────────────
SUPABASE_URL = "https://qvaofqcvjrozsrbhixzc.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF2YW9mcWN2anJvenNyYmhpeHpjIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MzAzMTg2MCwiZXhwIjoyMDg4NjA3ODYwfQ.Y92P-jRJPrFfe9ZtDf0hO15qMvFdK0GqPLecDnXrlSE"
TABLE        = "properties"

# ── The exact working search URL (US-wide, Active Adult toggled on) ───────────
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

# ── Configuration ─────────────────────────────────────────────────────────────
DELAY_SECONDS  = 2.0
PAGE_LOAD_WAIT = 5000   # ms to wait for JS to render listings
BATCH_SIZE     = 25
LOG_FILE       = "after55_scraper.log"
BASE_URL       = "https://www.after55.com"

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
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}"
    r = requests.post(url, headers=SUPA_HEADERS, json=rows, timeout=30)
    if r.status_code in (200, 201):
        log.info(f"  ✓ Inserted {len(rows)} rows into Supabase")
    else:
        log.error(f"  ✗ Insert failed {r.status_code}: {r.text[:300]}")


# ── Helpers ───────────────────────────────────────────────────────────────────
def txt(el):
    return el.inner_text().strip() if el else ""

def re_int(pattern, text):
    m = re.search(pattern, text)
    return int(m.group(1)) if m else None

def re_str(pattern, text):
    m = re.search(pattern, text)
    return m.group(1).strip() if m else ""


# ── Collect all listing URLs from paginated search ────────────────────────────
def collect_listing_urls(page):
    """Page through the US-wide active adult search and collect all property URLs."""
    all_urls = []
    search_page = 1

    while True:
        if search_page == 1:
            url = SEARCH_BASE
        else:
            url = SEARCH_BASE + f"&page={search_page}"

        log.info(f"Search page {search_page}: {url[:80]}…")
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(PAGE_LOAD_WAIT)
        except PWTimeout:
            log.warning(f"Timeout loading search page {search_page}, stopping.")
            break

        # Grab all property links on this page
        links = page.query_selector_all("a[href]")
        new_urls = []
        for link in links:
            href = link.get_attribute("href") or ""
            # Property pages: /st/city/property-name/7charID
            if re.match(r"^/[a-z]{2}/[^/]+/[^/]+/[a-z0-9]{7}$", href):
                full = BASE_URL + href
                if full not in all_urls and full not in new_urls:
                    new_urls.append(full)

        if not new_urls:
            log.info(f"No new listings on page {search_page}. Done collecting.")
            break

        all_urls.extend(new_urls)
        log.info(f"  Found {len(new_urls)} listings (total: {len(all_urls)})")

        # Check if there's a next page
        next_btn = page.query_selector("a[href*='page-']:last-of-type, [aria-label='Next page']")
        if not next_btn:
            # Also check for page numbers in the URL pattern
            page_links = page.query_selector_all("a[href*='page-']")
            page_nums = []
            for pl in page_links:
                h = pl.get_attribute("href") or ""
                m = re.search(r"page-(\d+)", h)
                if m:
                    page_nums.append(int(m.group(1)))
            if not page_nums or search_page >= max(page_nums):
                log.info("Reached last search page.")
                break

        search_page += 1
        time.sleep(DELAY_SECONDS)

    return list(dict.fromkeys(all_urls))


# ── Scrape a single property detail page ─────────────────────────────────────
def scrape_listing(page, url):
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(3000)
    except PWTimeout:
        log.warning(f"Timeout loading {url}")
        return None

    content = page.content()
    data = {"listing_url": url}

    # Name
    h1 = page.query_selector("h1")
    data["name"] = txt(h1)

    # Address — find text matching "Street, City, ST 12345"
    full_addr = ""
    for sel in ["p", "div", "span"]:
        for el in page.query_selector_all(sel):
            t = el.inner_text().strip()
            if re.search(r"\d{5}", t) and len(t) < 90:
                full_addr = t
                break
        if full_addr:
            break
    m = re.match(r"^(.*?),\s*(.*?),\s*([A-Z]{2})\s+(\d{5})", full_addr)
    if m:
        data["street"] = m.group(1).strip()
        data["city"]   = m.group(2).strip()
        data["state"]  = m.group(3).strip()
        data["zip"]    = m.group(4).strip()
    else:
        data["street"] = full_addr
        data["city"] = data["state"] = data["zip"] = ""

    # Floor plan summary table
    tbl = page.query_selector("table")
    if tbl:
        cells = [c.inner_text().strip() for c in tbl.query_selector_all("td")]
        data["rent_range"] = cells[0] if len(cells) > 0 else ""
        data["bedrooms"]   = cells[1] if len(cells) > 1 else ""
        data["bathrooms"]  = cells[2] if len(cells) > 2 else ""
        data["sqft_range"] = cells[3] if len(cells) > 3 else ""
    else:
        data.update({"rent_range":"","bedrooms":"","bathrooms":"","sqft_range":""})

    # Property info block — "Built in 2007 · 86 units/5 stories"
    prop_info = ""
    for el in page.query_selector_all("p, li, div"):
        t = el.inner_text().strip()
        if "Built in" in t and "units" in t:
            prop_info = t
            break
    data["year_built"] = re_str(r"Built in (\d{4})", prop_info)
    data["unit_count"] = re_int(r"(\d+)\s+units", prop_info)
    data["stories"]    = re_int(r"(\d+)\s+stor", prop_info)

    # Lease terms
    ls = page.query_selector("text=Lease Term")
    data["lease_terms"] = ""
    if ls:
        parent = ls.evaluate_handle("el => el.parentElement")
        lis = parent.query_selector_all("li") if parent else []
        data["lease_terms"] = ", ".join(li.inner_text().strip() for li in lis)

    # Application fee & pet policy
    data["application_fee"] = re_int(r"Application Fee[^$]*\$(\d+)", content)
    pt = re.search(r"(No Pets Allowed|Dogs Allowed|Cats Allowed|Pets Allowed)", content)
    data["pet_policy"] = pt.group(1) if pt else ""

    # Apartment features
    apt_features = []
    for el in page.query_selector_all("*"):
        t = el.inner_text().strip()
        if t == "Apartment Features":
            ul = el.evaluate_handle("el => el.parentElement.nextElementSibling")
            if ul:
                lis = ul.query_selector_all("li")
                apt_features = [li.inner_text().strip() for li in lis]
            break
    data["apartment_features"] = apt_features

    # Community features
    comm_features = []
    for el in page.query_selector_all("*"):
        t = el.inner_text().strip()
        if t == "Community Features":
            ul = el.evaluate_handle("el => el.parentElement.nextElementSibling")
            if ul:
                lis = ul.query_selector_all("li")
                comm_features = [li.inner_text().strip() for li in lis]
            break
    data["community_features"] = comm_features

    # Hospitals — parse from page content
    hospitals = []
    hosp_match = re.findall(
        r'href="/hospital/[^"]+">([^<]+)</a>\s*</td>\s*<td[^>]*>([^<]+)</td>',
        content
    )
    for name, commute in hosp_match[:5]:
        hospitals.append({"name": name.strip(), "commute": commute.strip()})
    data["hospitals"] = hospitals

    # Scores
    for key, label in [
        ("walk_score","Walk Score"),("transit_score","Transit Score"),
        ("bike_score","Bike Score"),("sound_score","Soundscore"),
    ]:
        data[key] = re_int(rf"{label}[^0-9]*(\d+)\s*/\s*100", content)

    # Phone
    ph = re.search(r'tel:\+1(\d{10})', content)
    if ph:
        p = ph.group(1)
        data["phone"] = f"({p[:3]}) {p[3:6]}-{p[6:]}"
    else:
        data["phone"] = ""

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
        )
        # Hide webdriver flag
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        page = context.new_page()

        log.info("=== Phase 1: Collecting listing URLs ===")
        all_urls = collect_listing_urls(page)
        log.info(f"\nTotal unique listings found: {len(all_urls)}")

        log.info("\n=== Phase 2: Scraping property detail pages ===")
        batch, total = [], 0

        for i, url in enumerate(all_urls, 1):
            log.info(f"[{i}/{len(all_urls)}] {url}")
            prop = scrape_listing(page, url)
            if prop:
                batch.append(prop)
            time.sleep(DELAY_SECONDS)

            if len(batch) >= BATCH_SIZE:
                insert_rows(batch)
                total += len(batch)
                batch = []

        if batch:
            insert_rows(batch)
            total += len(batch)

        browser.close()

    log.info(f"\nDone. {total} properties inserted into Supabase.")


if __name__ == "__main__":
    main()
