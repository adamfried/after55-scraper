"""
after55.com Active Adult Properties Scraper → Supabase
=======================================================
Scrapes all ~750 active adult properties from after55.com and
inserts them into your Supabase `properties` table.
"""

import re
import time
import logging
import requests
from bs4 import BeautifulSoup

# ── Supabase credentials ──────────────────────────────────────────────────────
SUPABASE_URL = "https://qvaofqcvjrozsrbhixzc.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF2YW9mcWN2anJvenNyYmhpeHpjIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MzAzMTg2MCwiZXhwIjoyMDg4NjA3ODYwfQ.Y92P-jRJPrFfe9ZtDf0hO15qMvFdK0GqPLecDnXrlSE"
TABLE        = "properties"

# ── Configuration ─────────────────────────────────────────────────────────────
DELAY_SECONDS = 1.5
MAX_RETRIES   = 3
BATCH_SIZE    = 25
LOG_FILE      = "after55_scraper.log"

SCRAPE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

SUPA_HEADERS = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "return=minimal",
}

BASE_URL = "https://www.after55.com"
SEARCH_STATES = [
    "al","ak","az","ar","ca","co","ct","de","fl","ga",
    "hi","id","il","in","ia","ks","ky","la","me","md",
    "ma","mi","mn","ms","mo","mt","ne","nv","nh","nj",
    "nm","ny","nc","nd","oh","ok","or","pa","ri","sc",
    "sd","tn","tx","ut","vt","va","wa","wv","wi","wy","dc",
]

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger(__name__)

session = requests.Session()
session.headers.update(SCRAPE_HEADERS)


# ── HTTP helper ───────────────────────────────────────────────────────────────
def get(url):
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, timeout=15)
            r.raise_for_status()
            return r
        except requests.HTTPError:
            if r.status_code == 429:
                wait = 30 * (attempt + 1)
                log.warning(f"Rate limited. Waiting {wait}s …")
                time.sleep(wait)
            else:
                log.error(f"HTTP {r.status_code} for {url}")
                return None
        except Exception as e:
            log.warning(f"Attempt {attempt+1} failed for {url}: {e}")
            time.sleep(5)
    return None


def txt(el):
    return el.get_text(strip=True) if el else ""


# ── Supabase insert ───────────────────────────────────────────────────────────
def insert_rows(rows):
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}"
    r = requests.post(url, headers=SUPA_HEADERS, json=rows, timeout=30)
    if r.status_code in (200, 201):
        log.info(f"  ✓ Inserted {len(rows)} rows")
    else:
        log.error(f"  ✗ Insert failed {r.status_code}: {r.text[:300]}")


# ── Property page scraper ─────────────────────────────────────────────────────
def scrape_listing(url):
    r = get(url)
    if not r:
        return None
    soup = BeautifulSoup(r.text, "html.parser")
    data = {"listing_url": url}

    # Name
    data["name"] = txt(soup.find("h1"))

    # Address
    full_addr = ""
    for tag in soup.find_all(["p", "div", "span"]):
        t = tag.get_text(strip=True)
        if re.search(r"\d{5}", t) and len(t) < 80:
            full_addr = t
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

    # Rent / beds / baths / sqft
    tbl = soup.find("table")
    if tbl:
        cells = [txt(td) for td in tbl.find_all("td")]
        data["rent_range"] = cells[0] if len(cells) > 0 else ""
        data["bedrooms"]   = cells[1] if len(cells) > 1 else ""
        data["bathrooms"]  = cells[2] if len(cells) > 2 else ""
        data["sqft_range"] = cells[3] if len(cells) > 3 else ""
    else:
        data.update({"rent_range":"","bedrooms":"","bathrooms":"","sqft_range":""})

    # Year built / unit count / stories
    prop_info = ""
    for tag in soup.find_all(["p","li","div"]):
        t = tag.get_text(strip=True)
        if "Built in" in t and "units" in t:
            prop_info = t
            break
    my = re.search(r"Built in (\d{4})", prop_info)
    mu = re.search(r"(\d+)\s+units", prop_info)
    ms = re.search(r"(\d+)\s+stor", prop_info)
    data["year_built"] = my.group(1)       if my else None
    data["unit_count"] = int(mu.group(1))  if mu else None
    data["stories"]    = int(ms.group(1))  if ms else None

    # Lease terms
    ls = soup.find(string=re.compile("Lease Term"))
    data["lease_terms"] = ""
    if ls:
        p = ls.find_parent()
        if p:
            items = p.find_next_siblings("li") or p.parent.find_all("li")
            data["lease_terms"] = ", ".join(txt(li) for li in items if txt(li))

    # Fees & pets
    af = re.search(r"Application Fee[^$]*\$(\d+)", r.text)
    data["application_fee"] = int(af.group(1)) if af else None
    pt = re.search(r"(No Pets Allowed|Dogs Allowed|Cats Allowed|Pets Allowed)", r.text)
    data["pet_policy"] = pt.group(1) if pt else ""

    # Apartment features → JSON array
    apt_features = []
    a_sec = soup.find(string=re.compile("Apartment Features"))
    if a_sec:
        ul = a_sec.find_parent().find_next("ul")
        if ul:
            apt_features = [txt(li) for li in ul.find_all("li")]
    data["apartment_features"] = apt_features

    # Community features → JSON array
    comm_features = []
    c_sec = soup.find(string=re.compile("Community Features"))
    if c_sec:
        ul = c_sec.find_parent().find_next("ul")
        if ul:
            comm_features = [txt(li) for li in ul.find_all("li")]
    data["community_features"] = comm_features

    # Hospitals → JSON array of {name, commute}
    hospitals = []
    h_sec = soup.find(string=re.compile("Hospitals"))
    if h_sec:
        h_tbl = h_sec.find_parent().find_next("table")
        if h_tbl:
            for row in h_tbl.find_all("tr")[1:]:
                cols = row.find_all("td")
                if len(cols) >= 2:
                    hospitals.append({"name": txt(cols[0]), "commute": txt(cols[1])})
    data["hospitals"] = hospitals

    # Walk / Transit / Bike / Sound scores
    for key, label in [
        ("walk_score","Walk Score"),("transit_score","Transit Score"),
        ("bike_score","Bike Score"),("sound_score","Soundscore"),
    ]:
        m = re.search(rf"{label}[^0-9]*(\d+)\s*/\s*100", r.text)
        data[key] = int(m.group(1)) if m else None

    # Phone
    ph = re.search(r'tel:\+1(\d{10})', r.text)
    if ph:
        p = ph.group(1)
        data["phone"] = f"({p[:3]}) {p[3:6]}-{p[6:]}"
    else:
        data["phone"] = ""

    return data


# ── Search-page URL collector ─────────────────────────────────────────────────
def get_listing_urls_for_state(state):
    urls = []
    page = 1
    while True:
        url = (
            f"{BASE_URL}/search/{state}/specialties-active-adult"
            if page == 1
            else f"{BASE_URL}/search/{state}/specialties-active-adult/page-{page}"
        )
        r = get(url)
        if not r:
            break
        soup = BeautifulSoup(r.text, "html.parser")

        new_urls = []
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if re.match(r"^/[a-z]{2}/[^/]+/[^/]+/[a-z0-9]{7}$", href):
                full = BASE_URL + href
                if full not in urls and full not in new_urls:
                    new_urls.append(full)

        if not new_urls:
            break

        urls.extend(new_urls)
        log.info(f"  [{state.upper()}] p{page}: {len(new_urls)} found (total {len(urls)})")

        page_nums = [
            int(re.search(r"page-(\d+)", a["href"]).group(1))
            for a in soup.select("a[href*='page-']")
            if re.search(r"page-(\d+)", a.get("href",""))
        ]
        if not page_nums or page >= max(page_nums):
            break

        page += 1
        time.sleep(DELAY_SECONDS)

    return urls


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("=== Phase 1: Collecting listing URLs ===")
    all_urls = []
    for state in SEARCH_STATES:
        all_urls.extend(get_listing_urls_for_state(state))
        time.sleep(DELAY_SECONDS)

    all_urls = list(dict.fromkeys(all_urls))
    log.info(f"\n=== Phase 2: Scraping {len(all_urls)} properties ===")

    batch, total = [], 0
    for i, url in enumerate(all_urls, 1):
        log.info(f"[{i}/{len(all_urls)}] {url}")
        prop = scrape_listing(url)
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

    log.info(f"\nDone. {total} properties inserted into Supabase.")


if __name__ == "__main__":
    main()
