"""
India Dental Clinic Scraper v4
Sources: IndiaMart + Clinicspots
GitHub Actions pe daily run hota hai

v4 Fixes:
 - Better deduplication (MD5 hash: name + phone + address)
 - Timeout pe bhi retry hota hai (skip nahi)
 - has_more URL-based check (text-based nahi)
 - State cycle bug fix
 - Delay increase (2-4s) aur inter-city sleep
 - Email column note
 - Cleaner logging
"""

import os, re, time, random, json, logging, hashlib
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials

# ─── LOGGING ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ─── CONFIG ─────────────────────────────────────────────────
DAILY_LIMIT    = 200
SHEET_NAME     = "Business Data"
BATCH_SIZE     = 20
IM_MAX_PAGES   = 8      # IndiaMart max pages per city
CS_MAX_PAGES   = 10     # Clinicspots max pages per city
HEADERS_ROW    = [
    "Name", "Category", "Phone", "Email*", "Website",
    "Address", "City", "State", "Rating", "Reviews",
    "Source URL", "Fetched On", "Source"
]
# *Email: IndiaMart/Clinicspots publicly email nahi deta, column reserved hai

# ─── USER AGENTS ────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
]

# ─── ALL 50 CITIES ──────────────────────────────────────────
CITIES = [
    {"city":"Mumbai",             "state":"Maharashtra",    "im":"mumbai",             "cs":"mumbai"},
    {"city":"Delhi",              "state":"Delhi",          "im":"delhi",              "cs":"delhi"},
    {"city":"Bangalore",          "state":"Karnataka",      "im":"bangalore",          "cs":"bangalore"},
    {"city":"Hyderabad",          "state":"Telangana",      "im":"hyderabad",          "cs":"hyderabad"},
    {"city":"Chennai",            "state":"Tamil Nadu",     "im":"chennai",            "cs":"chennai"},
    {"city":"Kolkata",            "state":"West Bengal",    "im":"kolkata",            "cs":"kolkata"},
    {"city":"Pune",               "state":"Maharashtra",    "im":"pune",               "cs":"pune"},
    {"city":"Ahmedabad",          "state":"Gujarat",        "im":"ahmedabad",          "cs":"ahmedabad"},
    {"city":"Jaipur",             "state":"Rajasthan",      "im":"jaipur",             "cs":"jaipur"},
    {"city":"Lucknow",            "state":"Uttar Pradesh",  "im":"lucknow",            "cs":"lucknow"},
    {"city":"Surat",              "state":"Gujarat",        "im":"surat",              "cs":"surat"},
    {"city":"Kanpur",             "state":"Uttar Pradesh",  "im":"kanpur",             "cs":"kanpur"},
    {"city":"Nagpur",             "state":"Maharashtra",    "im":"nagpur",             "cs":"nagpur"},
    {"city":"Indore",             "state":"Madhya Pradesh", "im":"indore",             "cs":"indore"},
    {"city":"Bhopal",             "state":"Madhya Pradesh", "im":"bhopal",             "cs":"bhopal"},
    {"city":"Patna",              "state":"Bihar",          "im":"patna",              "cs":"patna"},
    {"city":"Vadodara",           "state":"Gujarat",        "im":"vadodara",           "cs":"vadodara"},
    {"city":"Ludhiana",           "state":"Punjab",         "im":"ludhiana",           "cs":"ludhiana"},
    {"city":"Agra",               "state":"Uttar Pradesh",  "im":"agra",               "cs":"agra"},
    {"city":"Nashik",             "state":"Maharashtra",    "im":"nashik",             "cs":"nashik"},
    {"city":"Faridabad",          "state":"Haryana",        "im":"faridabad",          "cs":"faridabad"},
    {"city":"Meerut",             "state":"Uttar Pradesh",  "im":"meerut",             "cs":"meerut"},
    {"city":"Rajkot",             "state":"Gujarat",        "im":"rajkot",             "cs":"rajkot"},
    {"city":"Varanasi",           "state":"Uttar Pradesh",  "im":"varanasi",           "cs":"varanasi"},
    {"city":"Amritsar",           "state":"Punjab",         "im":"amritsar",           "cs":"amritsar"},
    {"city":"Allahabad",          "state":"Uttar Pradesh",  "im":"allahabad",          "cs":"prayagraj"},
    {"city":"Ranchi",             "state":"Jharkhand",      "im":"ranchi",             "cs":"ranchi"},
    {"city":"Coimbatore",         "state":"Tamil Nadu",     "im":"coimbatore",         "cs":"coimbatore"},
    {"city":"Gwalior",            "state":"Madhya Pradesh", "im":"gwalior",            "cs":"gwalior"},
    {"city":"Vijayawada",         "state":"Andhra Pradesh", "im":"vijayawada",         "cs":"vijayawada"},
    {"city":"Jodhpur",            "state":"Rajasthan",      "im":"jodhpur",            "cs":"jodhpur"},
    {"city":"Raipur",             "state":"Chhattisgarh",   "im":"raipur",             "cs":"raipur"},
    {"city":"Chandigarh",         "state":"Chandigarh",     "im":"chandigarh",         "cs":"chandigarh"},
    {"city":"Mysore",             "state":"Karnataka",      "im":"mysore",             "cs":"mysore"},
    {"city":"Bhubaneswar",        "state":"Odisha",         "im":"bhubaneswar",        "cs":"bhubaneswar"},
    {"city":"Guwahati",           "state":"Assam",          "im":"guwahati",           "cs":"guwahati"},
    {"city":"Kochi",              "state":"Kerala",         "im":"kochi",              "cs":"kochi"},
    {"city":"Thiruvananthapuram", "state":"Kerala",         "im":"thiruvananthapuram", "cs":"thiruvananthapuram"},
    {"city":"Visakhapatnam",      "state":"Andhra Pradesh", "im":"visakhapatnam",      "cs":"visakhapatnam"},
    {"city":"Jalandhar",          "state":"Punjab",         "im":"jalandhar",          "cs":"jalandhar"},
    {"city":"Madurai",            "state":"Tamil Nadu",     "im":"madurai",            "cs":"madurai"},
    {"city":"Srinagar",           "state":"J&K",            "im":"srinagar",           "cs":"srinagar"},
    {"city":"Aurangabad",         "state":"Maharashtra",    "im":"aurangabad",         "cs":"aurangabad"},
    {"city":"Dehradun",           "state":"Uttarakhand",    "im":"dehradun",           "cs":"dehradun"},
    {"city":"Jabalpur",           "state":"Madhya Pradesh", "im":"jabalpur",           "cs":"jabalpur"},
    {"city":"Warangal",           "state":"Telangana",      "im":"warangal",           "cs":"warangal"},
    {"city":"Hubli",              "state":"Karnataka",      "im":"hubli",              "cs":"hubli"},
    {"city":"Tiruchirappalli",    "state":"Tamil Nadu",     "im":"tiruchirappalli",    "cs":"tiruchirappalli"},
    {"city":"Bareilly",           "state":"Uttar Pradesh",  "im":"bareilly",           "cs":"bareilly"},
    {"city":"Moradabad",          "state":"Uttar Pradesh",  "im":"moradabad",          "cs":"moradabad"},
    {"city":"Salem",              "state":"Tamil Nadu",     "im":"salem",              "cs":"salem"},
]

SOURCES = ["indiamart", "clinicspots"]

# ─── STATE ──────────────────────────────────────────────────
STATE_FILE = "state.json"

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"city_idx": 0, "source_idx": 0, "page": 1}

def save_state(s):
    with open(STATE_FILE, "w") as f:
        json.dump(s, f)

# ─── DEDUPLICATION ──────────────────────────────────────────
def make_key(name: str, phone: str, address: str) -> str:
    """
    FIX: Pehle sirf name+address tha — ab name+phone+address tino ka
    MD5 hash use hota hai. Duplicate accurate detect hoga.
    """
    raw = f"{name}|{phone}|{address}".lower().strip()
    return hashlib.md5(raw.encode("utf-8")).hexdigest()

# ─── HTTP ────────────────────────────────────────────────────
def get_html(url, extra_headers=None, timeout=25, retries=3):
    headers = {
        "User-Agent"      : random.choice(USER_AGENTS),
        "Accept-Language" : "en-IN,en;q=0.9",
        "Accept"          : "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding" : "gzip, deflate, br",
    }
    if extra_headers:
        headers.update(extra_headers)

    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code == 200:
                return r.text
            if r.status_code in (403, 404):
                log.warning(f"  {r.status_code} — skip")
                return None
            if r.status_code in (429, 503, 504):
                wait = (attempt + 1) * 6
                log.warning(f"  {r.status_code} — retry in {wait}s")
                time.sleep(wait)

        except requests.exceptions.Timeout:
            # FIX: Pehle ye seedha return None karta tha (no retry).
            # Ab retry hoga — sirf last attempt pe skip karega.
            wait = (attempt + 1) * 3
            log.warning(f"  Timeout (attempt {attempt+1}/{retries}) — wait {wait}s")
            if attempt < retries - 1:
                time.sleep(wait)
            else:
                log.warning("  Max retries on timeout — skipping URL")
                return None

        except Exception as e:
            log.warning(f"  Error: {e} — retry {attempt+1}/{retries}")
            time.sleep(3)

    return None

def now_ist():
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%d/%m/%Y %H:%M")

# ─── has_more HELPER ────────────────────────────────────────
def check_has_more_im(soup, page: int) -> bool:
    """
    FIX: Pehle soup.find("a", string="Next") tha — ye fragile tha.
    Ab next page ka URL directly check hota hai href mein.
    """
    if page >= IM_MAX_PAGES:
        return False
    next_page = page + 1
    found = soup.find("a", href=re.compile(rf"[?&]bpg={next_page}"))
    return bool(found)

def check_has_more_cs(soup, page: int) -> bool:
    """Same fix for Clinicspots — page= param check."""
    if page >= CS_MAX_PAGES:
        return False
    next_page = page + 1
    found = soup.find("a", href=re.compile(rf"[?&]page={next_page}"))
    return bool(found)


# ============================================================
#  SOURCE 1 — IndiaMart Directory
# ============================================================
def scrape_indiamart(city, page=1):
    url  = f"https://dir.indiamart.com/{city['im']}/dental-clinic.html?bpg={page}"
    html = get_html(url, extra_headers={"Referer": "https://dir.indiamart.com/"})
    if not html:
        return [], False

    soup     = BeautifulSoup(html, "html.parser")
    rows     = []
    has_more = check_has_more_im(soup, page)  # FIX: URL-based check

    # Method 1 — JSON-LD
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            obj   = json.loads(tag.string or "")
            items = obj.get("@graph", [obj] if isinstance(obj, dict) else obj)
            if isinstance(items, dict):
                items = [items]
            for item in items:
                name = item.get("name", "").strip()
                if not name or len(name) < 4:
                    continue
                if item.get("@type", "") in ("WebPage", "WebSite", "BreadcrumbList", "Organization"):
                    continue
                addr    = item.get("address", {})
                phone   = item.get("telephone", "")
                address = (addr.get("streetAddress", "") + " " + addr.get("addressLocality", "")).strip()
                rows.append([
                    name, "Dental Clinic",
                    phone, "",
                    item.get("url", ""),
                    address,
                    city["city"], city["state"],
                    item.get("aggregateRating", {}).get("ratingValue", ""),
                    item.get("aggregateRating", {}).get("reviewCount", ""),
                    url, now_ist(), "IndiaMart"
                ])
        except Exception:
            pass

    # Method 2 — HTML fallback
    if not rows:
        for card in soup.find_all("div", class_=re.compile(r"(card|listing|company|bname|imprd)", re.I)):
            name_tag = card.find(["h2", "h3", "a"], class_=re.compile(r"(name|title|bname|company)", re.I))
            if not name_tag:
                continue
            name = name_tag.get_text(strip=True)
            if not name or len(name) < 4:
                continue
            phone_match = re.search(r"[\+\d][\d\s\-]{9,14}", card.get_text())
            addr_tag    = card.find(class_=re.compile(r"addr|address|location", re.I))
            phone       = phone_match.group().strip() if phone_match else ""
            address     = addr_tag.get_text(strip=True) if addr_tag else city["city"]
            rows.append([
                name, "Dental Clinic",
                phone, "", "",
                address,
                city["city"], city["state"],
                "", "", url, now_ist(), "IndiaMart"
            ])

    log.info(f"  IndiaMart {city['city']} p{page}: {len(rows)} records | has_more={has_more}")
    return rows, has_more


# ============================================================
#  SOURCE 2 — Clinicspots
# ============================================================
def scrape_clinicspots(city, page=1):
    url  = f"https://www.clinicspots.com/in/{city['cs']}/dentist?page={page}"
    html = get_html(url, extra_headers={"Referer": "https://www.clinicspots.com/"})
    if not html:
        return [], False

    soup     = BeautifulSoup(html, "html.parser")
    rows     = []
    has_more = check_has_more_cs(soup, page)  # FIX: URL-based check

    # Method 1 — JSON-LD
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            obj   = json.loads(tag.string or "")
            items = obj.get("@graph", [obj] if isinstance(obj, dict) else obj)
            if isinstance(items, dict):
                items = [items]
            for item in items:
                name = item.get("name", "").strip()
                if not name or len(name) < 4:
                    continue
                if item.get("@type", "") in ("WebPage", "WebSite", "BreadcrumbList"):
                    continue
                addr    = item.get("address", {})
                phone   = item.get("telephone", "")
                address = (addr.get("streetAddress", "") + ", " + addr.get("addressLocality", "")).strip(", ")
                rows.append([
                    name, "Dental Clinic",
                    phone, "",
                    item.get("url", ""),
                    address,
                    city["city"], city["state"],
                    item.get("aggregateRating", {}).get("ratingValue", ""),
                    item.get("aggregateRating", {}).get("reviewCount", ""),
                    url, now_ist(), "Clinicspots"
                ])
        except Exception:
            pass

    # Method 2 — HTML fallback
    if not rows:
        for card in soup.find_all("div", class_=re.compile(r"(clinic|doctor|provider|card|listing)", re.I)):
            name_tag = card.find(["h2", "h3", "h4", "a"], class_=re.compile(r"(name|title|clinic)", re.I))
            if not name_tag:
                continue
            name = name_tag.get_text(strip=True)
            if not name or len(name) < 4:
                continue
            phone_match = re.search(r"[\+\d][\d\s\-]{9,14}", card.get_text())
            addr_tag    = card.find(class_=re.compile(r"addr|address|location|area", re.I))
            rating_tag  = card.find(class_=re.compile(r"rating|star|score", re.I))
            phone       = phone_match.group().strip() if phone_match else ""
            address     = addr_tag.get_text(strip=True) if addr_tag else city["city"]
            rows.append([
                name, "Dental Clinic",
                phone, "", "",
                address,
                city["city"], city["state"],
                rating_tag.get_text(strip=True) if rating_tag else "",
                "", url, now_ist(), "Clinicspots"
            ])

    log.info(f"  Clinicspots {city['city']} p{page}: {len(rows)} records | has_more={has_more}")
    return rows, has_more


# ============================================================
#  GOOGLE SHEETS
# ============================================================
def get_sheet():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    sheet_id   = os.environ.get("SHEET_ID")
    if not creds_json or not sheet_id:
        raise ValueError("GOOGLE_CREDENTIALS ya SHEET_ID env variable nahi mili!")
    creds  = Credentials.from_service_account_info(
        json.loads(creds_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(creds)
    sheet  = client.open_by_key(sheet_id)
    try:
        ws = sheet.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(SHEET_NAME, rows=50000, cols=13)
        ws.append_row(HEADERS_ROW)
        ws.format("A1:M1", {
            "textFormat"     : {"bold": True, "foregroundColor": {"red":1,"green":1,"blue":1}},
            "backgroundColor": {"red":0.1,"green":0.45,"blue":0.9}
        })
    return ws

def get_existing_keys(ws):
    """
    FIX: Pehle name+address ka simple string tha.
    Ab name+phone+address ka MD5 hash use hota hai — same make_key() function.
    """
    keys = set()
    for row in ws.get_all_values()[1:]:
        if len(row) >= 6:
            name    = row[0]
            phone   = row[2] if len(row) > 2 else ""
            address = row[5]
            k = make_key(name, phone, address)
            keys.add(k)
    return keys

def append_rows_to_sheet(ws, rows):
    if rows:
        ws.append_rows(rows, value_input_option="RAW")
        log.info(f"  ✓ {len(rows)} rows sheet mein likhe")


# ============================================================
#  MAIN
# ============================================================
def main():
    log.info("=== Dental Scraper v4 Start ===")

    ws       = get_sheet()
    existing = get_existing_keys(ws)
    log.info(f"Sheet mein already {len(existing)} records hain")

    state      = load_state()
    city_idx   = state["city_idx"]
    source_idx = state["source_idx"]
    page       = state["page"]

    collected = 0
    batch     = []

    while collected < DAILY_LIMIT:
        # FIX: city cycle complete hone pe source advance karo
        if city_idx >= len(CITIES):
            city_idx   = 0
            source_idx = (source_idx + 1) % len(SOURCES)
            page       = 1
            log.info(f"  All cities done — switching to source: {SOURCES[source_idx]}")

        city   = CITIES[city_idx]
        source = SOURCES[source_idx]

        log.info(f"[{collected}/{DAILY_LIMIT}] {source} | {city['city']} | page {page}")

        try:
            if source == "indiamart":
                rows, has_more = scrape_indiamart(city, page)
            else:
                rows, has_more = scrape_clinicspots(city, page)
        except Exception as e:
            log.error(f"  ERROR: {e}")
            city_idx += 1
            page      = 1
            time.sleep(3)
            continue

        for row in rows:
            if collected >= DAILY_LIMIT:
                break
            # FIX: MD5-based deduplication
            key = make_key(str(row[0]), str(row[2]), str(row[5]))
            if key in existing:
                continue
            batch.append(row)
            existing.add(key)
            collected += 1

        if len(batch) >= BATCH_SIZE:
            append_rows_to_sheet(ws, batch)
            batch = []

        if has_more and collected < DAILY_LIMIT:
            page += 1
            # FIX: Page ke beech delay thoda zyada (2-4s)
            time.sleep(random.uniform(2.0, 4.0))
        else:
            page      = 1
            city_idx += 1
            # FIX: City switch pe extra sleep — block hone ka risk kam hoga
            time.sleep(random.uniform(3.0, 6.0))

    # Remaining batch flush
    if batch:
        append_rows_to_sheet(ws, batch)

    # FIX: State save karo — cycle bug nahi aayega next run pe
    save_state({"city_idx": city_idx, "source_idx": source_idx, "page": page})
    log.info(f"=== DONE | +{collected} aaj | Total known: {len(existing)} ===")


if __name__ == "__main__":
    main()
