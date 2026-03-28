"""
India Dental Clinic Scraper v11
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Sources:
  1. Google Maps  — maps.google.com (NO API KEY) — real mobile numbers ✅
  2. Sulekha      — sulekha.com/dentists/{city}
  3. Clinicspots  — clinicspots.com/dentist/{city}

Google Maps se:
  - Real phone numbers (mobile + landline dono)
  - Website URL (blank = hot lead)
  - Address, Rating, Reviews
  - Koi API key nahi chahiye
"""

import os, re, time, random, json, logging, hashlib
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ─── CONFIG ─────────────────────────────────────────────────
DAILY_LIMIT  = 200
SHEET_NAME   = "Business Data"
BATCH_SIZE   = 20
MAX_PAGES    = 5     # Google Maps pe zyada pages nahi hote
HEADERS_ROW  = [
    "Name", "Category", "Phone", "Email", "Website",
    "Address", "City", "State", "Rating", "Reviews",
    "Source URL", "Fetched On", "Source"
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

CITIES = [
    {"city":"Mumbai",             "state":"Maharashtra",    "cs":"mumbai",             "sl":"mumbai"},
    {"city":"Delhi",              "state":"Delhi",          "cs":"delhi",              "sl":"delhi"},
    {"city":"Bangalore",          "state":"Karnataka",      "cs":"bangalore",          "sl":"bangalore"},
    {"city":"Hyderabad",          "state":"Telangana",      "cs":"hyderabad",          "sl":"hyderabad"},
    {"city":"Chennai",            "state":"Tamil Nadu",     "cs":"chennai",            "sl":"chennai"},
    {"city":"Kolkata",            "state":"West Bengal",    "cs":"kolkata",            "sl":"kolkata"},
    {"city":"Pune",               "state":"Maharashtra",    "cs":"pune",               "sl":"pune"},
    {"city":"Ahmedabad",          "state":"Gujarat",        "cs":"ahmedabad",          "sl":"ahmedabad"},
    {"city":"Jaipur",             "state":"Rajasthan",      "cs":"jaipur",             "sl":"jaipur"},
    {"city":"Lucknow",            "state":"Uttar Pradesh",  "cs":"lucknow",            "sl":"lucknow"},
    {"city":"Surat",              "state":"Gujarat",        "cs":"surat",              "sl":"surat"},
    {"city":"Kanpur",             "state":"Uttar Pradesh",  "cs":"kanpur",             "sl":"kanpur"},
    {"city":"Nagpur",             "state":"Maharashtra",    "cs":"nagpur",             "sl":"nagpur"},
    {"city":"Indore",             "state":"Madhya Pradesh", "cs":"indore",             "sl":"indore"},
    {"city":"Bhopal",             "state":"Madhya Pradesh", "cs":"bhopal",             "sl":"bhopal"},
    {"city":"Patna",              "state":"Bihar",          "cs":"patna",              "sl":"patna"},
    {"city":"Vadodara",           "state":"Gujarat",        "cs":"vadodara",           "sl":"vadodara"},
    {"city":"Ludhiana",           "state":"Punjab",         "cs":"ludhiana",           "sl":"ludhiana"},
    {"city":"Agra",               "state":"Uttar Pradesh",  "cs":"agra",               "sl":"agra"},
    {"city":"Nashik",             "state":"Maharashtra",    "cs":"nashik",             "sl":"nashik"},
    {"city":"Faridabad",          "state":"Haryana",        "cs":"faridabad",          "sl":"faridabad"},
    {"city":"Meerut",             "state":"Uttar Pradesh",  "cs":"meerut",             "sl":"meerut"},
    {"city":"Rajkot",             "state":"Gujarat",        "cs":"rajkot",             "sl":"rajkot"},
    {"city":"Varanasi",           "state":"Uttar Pradesh",  "cs":"varanasi",           "sl":"varanasi"},
    {"city":"Amritsar",           "state":"Punjab",         "cs":"amritsar",           "sl":"amritsar"},
    {"city":"Allahabad",          "state":"Uttar Pradesh",  "cs":"prayagraj",          "sl":"allahabad"},
    {"city":"Ranchi",             "state":"Jharkhand",      "cs":"ranchi",             "sl":"ranchi"},
    {"city":"Coimbatore",         "state":"Tamil Nadu",     "cs":"coimbatore",         "sl":"coimbatore"},
    {"city":"Gwalior",            "state":"Madhya Pradesh", "cs":"gwalior",            "sl":"gwalior"},
    {"city":"Vijayawada",         "state":"Andhra Pradesh", "cs":"vijayawada",         "sl":"vijayawada"},
    {"city":"Jodhpur",            "state":"Rajasthan",      "cs":"jodhpur",            "sl":"jodhpur"},
    {"city":"Raipur",             "state":"Chhattisgarh",   "cs":"raipur",             "sl":"raipur"},
    {"city":"Chandigarh",         "state":"Chandigarh",     "cs":"chandigarh",         "sl":"chandigarh"},
    {"city":"Mysore",             "state":"Karnataka",      "cs":"mysore",             "sl":"mysore"},
    {"city":"Bhubaneswar",        "state":"Odisha",         "cs":"bhubaneswar",        "sl":"bhubaneswar"},
    {"city":"Guwahati",           "state":"Assam",          "cs":"guwahati",           "sl":"guwahati"},
    {"city":"Kochi",              "state":"Kerala",         "cs":"kochi",              "sl":"kochi"},
    {"city":"Thiruvananthapuram", "state":"Kerala",         "cs":"thiruvananthapuram", "sl":"thiruvananthapuram"},
    {"city":"Visakhapatnam",      "state":"Andhra Pradesh", "cs":"visakhapatnam",      "sl":"visakhapatnam"},
    {"city":"Jalandhar",          "state":"Punjab",         "cs":"jalandhar",          "sl":"jalandhar"},
    {"city":"Madurai",            "state":"Tamil Nadu",     "cs":"madurai",            "sl":"madurai"},
    {"city":"Srinagar",           "state":"J&K",            "cs":"srinagar",           "sl":"srinagar"},
    {"city":"Aurangabad",         "state":"Maharashtra",    "cs":"aurangabad",         "sl":"aurangabad"},
    {"city":"Dehradun",           "state":"Uttarakhand",    "cs":"dehradun",           "sl":"dehradun"},
    {"city":"Jabalpur",           "state":"Madhya Pradesh", "cs":"jabalpur",           "sl":"jabalpur"},
    {"city":"Warangal",           "state":"Telangana",      "cs":"warangal",           "sl":"warangal"},
    {"city":"Hubli",              "state":"Karnataka",      "cs":"hubli",              "sl":"hubli"},
    {"city":"Tiruchirappalli",    "state":"Tamil Nadu",     "cs":"tiruchirappalli",    "sl":"tiruchirappalli"},
    {"city":"Bareilly",           "state":"Uttar Pradesh",  "cs":"bareilly",           "sl":"bareilly"},
    {"city":"Moradabad",          "state":"Uttar Pradesh",  "cs":"moradabad",          "sl":"moradabad"},
    {"city":"Salem",              "state":"Tamil Nadu",     "cs":"salem",              "sl":"salem"},
]

SOURCES = ["googlemaps", "sulekha", "clinicspots"]

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

# ─── UTILS ──────────────────────────────────────────────────
def make_key(name, phone, address):
    return hashlib.md5(f"{name}|{phone}|{address}".lower().strip().encode()).hexdigest()

def now_ist():
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%d/%m/%Y %H:%M")

MOBILE_RE = re.compile(r"(?<!\d)(?:\+91[\s\-]?)?([6-9]\d{9})(?!\d)")
PHONE_RE  = re.compile(r"(?<!\d)(\+?91[\s\-]?\d{10}|\d{10}|\d{3,5}[\s\-]\d{6,8})(?!\d)")

def extract_phone(text):
    """Mobile pehle try karo, nahi mila toh any phone."""
    t = re.sub(r"\s+", "", text)
    m = MOBILE_RE.search(t)
    if m:
        return m.group(1)
    m = PHONE_RE.search(t)
    return m.group(0) if m else ""

def new_session(referer):
    s = requests.Session()
    s.headers.update({
        "User-Agent"      : random.choice(USER_AGENTS),
        "Accept"          : "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language" : "en-IN,en;q=0.9",
        "Accept-Encoding" : "gzip, deflate, br",
        "Referer"         : referer,
        "Connection"      : "keep-alive",
    })
    return s

def get_html_req(session, url, timeout=25, retries=3):
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=timeout)
            if r.status_code == 200:
                return r.text
            if r.status_code in (403, 404):
                log.warning(f"  {r.status_code} — skip")
                return None
            if r.status_code in (429, 503, 504):
                wait = int(r.headers.get("Retry-After", (attempt+1)*10))
                log.warning(f"  {r.status_code} — wait {wait}s")
                time.sleep(wait)
        except requests.exceptions.Timeout:
            wait = (attempt+1)*4
            if attempt < retries-1:
                time.sleep(wait)
            else:
                return None
        except Exception as e:
            log.warning(f"  Err: {e}")
            time.sleep(3)
    return None

def parse_jsonld(soup, city, url, src):
    rows = []
    skip = {"WebPage","WebSite","BreadcrumbList","Organization","ItemList"}
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            obj   = json.loads(tag.string or "")
            items = obj.get("@graph",[obj] if isinstance(obj,dict) else obj)
            if isinstance(items, dict):
                items = [items]
            for item in items:
                name = item.get("name","").strip()
                if not name or len(name) < 4 or item.get("@type","") in skip:
                    continue
                addr  = item.get("address",{})
                phone = extract_phone(str(item.get("telephone","")))
                addr_str = (addr.get("streetAddress","")+" "+addr.get("addressLocality","")).strip() or city["city"]
                rows.append([name,"Dental Clinic",phone,"",item.get("url",""),
                    addr_str,city["city"],city["state"],
                    item.get("aggregateRating",{}).get("ratingValue",""),
                    item.get("aggregateRating",{}).get("reviewCount",""),
                    url,now_ist(),src])
        except Exception:
            pass
    return rows

def check_has_more(soup, page, param="page"):
    return page < MAX_PAGES and bool(soup.find("a", href=re.compile(rf"[?&]{param}={page+1}")))


# ─── PLAYWRIGHT ─────────────────────────────────────────────
_pw_inst = _pw_browser = _pw_ctx = None

def get_pw_ctx():
    global _pw_inst, _pw_browser, _pw_ctx
    if _pw_browser is None:
        _pw_inst    = sync_playwright().start()
        _pw_browser = _pw_inst.chromium.launch(
            headless=True,
            args=["--no-sandbox","--disable-setuid-sandbox",
                  "--disable-dev-shm-usage",
                  "--disable-blink-features=AutomationControlled"]
        )
        _pw_ctx = _pw_browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            locale="en-IN",
            viewport={"width":1280,"height":800},
        )
        _pw_ctx.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
        )
    return _pw_ctx

def close_pw():
    global _pw_inst, _pw_browser, _pw_ctx
    if _pw_browser:
        try:
            _pw_browser.close()
            _pw_inst.stop()
        except Exception:
            pass
        _pw_inst = _pw_browser = _pw_ctx = None


# ============================================================
#  SOURCE 1 — Google Maps (NO API KEY)
#  Playwright se real phone numbers milte hain
# ============================================================
def scrape_googlemaps(city, page=1):
    """
    Google Maps search → results scroll karo → har listing click karo → phone nikalo.
    Page = scroll count (har scroll ~20 results load karta hai)
    """
    query   = f"dental+clinic+{city['city']}+India"
    map_url = f"https://www.google.com/maps/search/{query}"
    rows    = []

    ctx = get_pw_ctx()
    pg  = ctx.new_page()

    try:
        pg.goto(map_url, timeout=40000, wait_until="domcontentloaded")

        # Cookie consent dismiss karo agar aaye
        try:
            pg.click("button[aria-label*='Accept'], button[jsname='b3VHJd']", timeout=4000)
        except Exception:
            pass

        # Results panel load hone do
        try:
            pg.wait_for_selector("div[role='feed'], div.Nv2PK", timeout=15000)
        except PWTimeout:
            log.warning(f"  GMaps {city['city']}: results load nahi hue")
            return [], False

        # Page ke hisaab se scroll karo — zyada results load karo
        feed_sel = "div[role='feed']"
        for _ in range(page * 3):
            try:
                pg.eval_on_selector(feed_sel,
                    "el => el.scrollBy(0, el.scrollHeight)")
                pg.wait_for_timeout(1500)
            except Exception:
                break

        # Saari listing cards collect karo
        cards = pg.query_selector_all("div.Nv2PK, div[jsaction*='mouseover:pane']")
        log.info(f"  GMaps {city['city']}: {len(cards)} cards mili")

        processed = 0
        for card in cards:
            if processed >= 20:   # ek page pe max 20
                break
            try:
                card.click()
                pg.wait_for_timeout(2000)

                # Detail panel mein phone dhundho
                # Google Maps phone aria-label mein hota hai
                phone = ""

                # Method 1: aria-label se
                phone_btn = pg.query_selector(
                    "button[data-tooltip='Copy phone number'], "
                    "button[aria-label*='phone'], "
                    "a[href^='tel:']"
                )
                if phone_btn:
                    label = phone_btn.get_attribute("aria-label") or \
                            phone_btn.get_attribute("data-value") or ""
                    href  = phone_btn.get_attribute("href") or ""
                    phone = extract_phone(label + " " + href)

                # Method 2: tel: link se
                if not phone:
                    tel = pg.query_selector("a[href^='tel:']")
                    if tel:
                        href  = tel.get_attribute("href") or ""
                        phone = extract_phone(href.replace("tel:",""))

                # Method 3: visible text se
                if not phone:
                    detail_html = pg.inner_html(
                        "div[role='main'], div.m6QErb[data-value]",
                    ) if pg.query_selector("div[role='main']") else ""
                    if detail_html:
                        phone = extract_phone(
                            BeautifulSoup(detail_html,"html.parser").get_text()
                        )

                # Name
                name = ""
                name_el = pg.query_selector("h1.DUwDvf, h1[class*='fontHeadline']")
                if name_el:
                    name = name_el.inner_text().strip()

                if not name or len(name) < 4:
                    processed += 1
                    continue

                # Address
                address = city["city"]
                addr_el = pg.query_selector(
                    "button[data-tooltip='Copy address'] div.rogA2c, "
                    "div[data-section-id='ad'] span"
                )
                if addr_el:
                    address = addr_el.inner_text().strip() or city["city"]

                # Website
                website = ""
                web_el  = pg.query_selector("a[data-tooltip='Open website'], a[aria-label*='website']")
                if web_el:
                    website = web_el.get_attribute("href") or ""

                # Rating
                rating = ""
                rat_el = pg.query_selector("div.F7nice span[aria-hidden='true']")
                if rat_el:
                    rating = rat_el.inner_text().strip()

                # Reviews count
                reviews = ""
                rev_el  = pg.query_selector("div.F7nice span[aria-label*='review']")
                if rev_el:
                    rev_text = rev_el.get_attribute("aria-label") or ""
                    rm = re.search(r"[\d,]+", rev_text)
                    if rm:
                        reviews = rm.group(0)

                rows.append([
                    name, "Dental Clinic",
                    phone, "", website,
                    address, city["city"], city["state"],
                    rating, reviews,
                    map_url, now_ist(), "Google Maps"
                ])

                processed += 1
                log.info(f"    ✓ {name} | {phone or 'no phone'}")
                time.sleep(random.uniform(1.0, 2.0))

            except Exception as e:
                log.warning(f"    Card error: {e}")
                processed += 1
                continue

    except Exception as e:
        log.error(f"  GMaps {city['city']} error: {e}")
    finally:
        pg.close()

    # has_more: agar 20 results mile toh aur ho sakte hain
    has_more = len(rows) >= 18 and page < MAX_PAGES
    phone_count = sum(1 for r in rows if r[2])
    log.info(f"  GMaps {city['city']} p{page}: {len(rows)} total | {phone_count} with phone")
    return rows, has_more


# ============================================================
#  SOURCE 2 — Sulekha (requests)
# ============================================================
def scrape_sulekha(city, page=1):
    base = "https://www.sulekha.com"
    url  = f"{base}/dentists/{city['sl']}?page={page}"
    sess = new_session(f"{base}/dentists/{city['sl']}")
    html = get_html_req(sess, url)
    if not html:
        return [], False
    soup = BeautifulSoup(html, "html.parser")
    rows = parse_jsonld(soup, city, url, "Sulekha")
    if not rows:
        for card in soup.find_all("div", class_=re.compile(r"(serv|listing|provider|biz|card)", re.I)):
            nt = (card.find(["h2","h3","h4","a"], class_=re.compile(r"(name|title|biz|heading)", re.I))
                  or card.find(["h2","h3"]))
            if not nt:
                continue
            name = nt.get_text(strip=True)
            if not name or len(name) < 4:
                continue
            phone = extract_phone(card.get_text(separator=" "))
            at    = card.find(class_=re.compile(r"addr|address|location|area|locality", re.I))
            rt    = card.find(class_=re.compile(r"rating|star|score", re.I))
            rows.append([name,"Dental Clinic",phone,"","",
                at.get_text(strip=True) if at else city["city"],
                city["city"],city["state"],
                rt.get_text(strip=True) if rt else "","",
                url,now_ist(),"Sulekha"])
    phone_count = sum(1 for r in rows if r[2])
    log.info(f"  Sulekha {city['city']} p{page}: {len(rows)} total | {phone_count} with phone")
    return rows, check_has_more(soup, page)


# ============================================================
#  SOURCE 3 — Clinicspots (requests)
# ============================================================
def scrape_clinicspots(city, page=1):
    base = "https://www.clinicspots.com"
    url  = f"{base}/dentist/{city['cs']}?page={page}"
    sess = new_session(f"{base}/dentist/{city['cs']}")
    html = get_html_req(sess, url)
    if not html:
        return [], False
    soup = BeautifulSoup(html, "html.parser")
    rows = parse_jsonld(soup, city, url, "Clinicspots")
    if not rows:
        for card in soup.find_all("div", class_=re.compile(r"(clinic|doctor|card|listing)", re.I)):
            nt = card.find(["h2","h3","h4","a"], class_=re.compile(r"(name|title|clinic)", re.I))
            if not nt:
                continue
            name = nt.get_text(strip=True)
            if not name or len(name) < 4:
                continue
            phone = extract_phone(card.get_text(separator=" "))
            at    = card.find(class_=re.compile(r"addr|address|location|area", re.I))
            rt    = card.find(class_=re.compile(r"rating|star|score", re.I))
            rows.append([name,"Dental Clinic",phone,"","",
                at.get_text(strip=True) if at else city["city"],
                city["city"],city["state"],
                rt.get_text(strip=True) if rt else "","",
                url,now_ist(),"Clinicspots"])
    phone_count = sum(1 for r in rows if r[2])
    log.info(f"  Clinicspots {city['city']} p{page}: {len(rows)} total | {phone_count} with phone")
    return rows, check_has_more(soup, page)


# ============================================================
#  GOOGLE SHEETS
# ============================================================
def get_sheet():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    sheet_id   = os.environ.get("SHEET_ID")
    if not creds_json or not sheet_id:
        raise ValueError("Env variables missing!")
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
            "textFormat"     : {"bold":True,"foregroundColor":{"red":1,"green":1,"blue":1}},
            "backgroundColor": {"red":0.1,"green":0.45,"blue":0.9}
        })
    return ws

def get_existing_keys(ws):
    keys = set()
    for row in ws.get_all_values()[1:]:
        if len(row) >= 6:
            k = make_key(row[0], row[2] if len(row) > 2 else "", row[5])
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
    log.info("=== Dental Scraper v11 Start ===")
    ws       = get_sheet()
    existing = get_existing_keys(ws)
    log.info(f"Sheet mein already {len(existing)} records hain")

    state      = load_state()
    city_idx   = state["city_idx"]
    source_idx = state["source_idx"]
    page       = state["page"]

    collected = 0
    batch     = []

    try:
        while collected < DAILY_LIMIT:
            if city_idx >= len(CITIES):
                city_idx   = 0
                source_idx = (source_idx + 1) % len(SOURCES)
                page       = 1
                log.info(f"  All cities done — switching to: {SOURCES[source_idx]}")

            city   = CITIES[city_idx]
            source = SOURCES[source_idx]
            log.info(f"[{collected}/{DAILY_LIMIT}] {source} | {city['city']} | page {page}")

            try:
                if source == "googlemaps":
                    rows, has_more = scrape_googlemaps(city, page)
                elif source == "sulekha":
                    rows, has_more = scrape_sulekha(city, page)
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
                time.sleep(random.uniform(3.0, 5.0))
            else:
                page      = 1
                city_idx += 1
                time.sleep(random.uniform(4.0, 7.0))  # Maps pe thoda zyada wait

    finally:
        close_pw()
        if batch:
            append_rows_to_sheet(ws, batch)
        save_state({"city_idx": city_idx, "source_idx": source_idx, "page": page})
        log.info(f"=== DONE | +{collected} aaj | Total known: {len(existing)} ===")


if __name__ == "__main__":
    main()
