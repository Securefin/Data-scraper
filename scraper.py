"""
India Dental Clinic Scraper v9
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Root cause fix: Practo JS-rendered hai, requests se 0 results aate the.

Sources:
  Playwright (headless Chrome — JS sites):
    1. Practo    — practo.com/{city}/clinics/dental-clinics
    2. JustDial  — justdial.com/{City}/Dentists/nct-10156331

  requests (fast — static HTML):
    3. Clinicspots — clinicspots.com/dentist/{city}
    4. Sulekha     — sulekha.com/dentists/{city}
"""

import os, re, time, random, json, logging, hashlib
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
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
DAILY_LIMIT  = 200
SHEET_NAME   = "Business Data"
BATCH_SIZE   = 20
MAX_PAGES    = 10
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

# ─── CITIES ─────────────────────────────────────────────────
CITIES = [
    {"city":"Mumbai",             "state":"Maharashtra",    "cs":"mumbai",             "pr":"mumbai",             "sl":"mumbai",             "jd":"Mumbai"},
    {"city":"Delhi",              "state":"Delhi",          "cs":"delhi",              "pr":"delhi",              "sl":"delhi",              "jd":"Delhi"},
    {"city":"Bangalore",          "state":"Karnataka",      "cs":"bangalore",          "pr":"bangalore",          "sl":"bangalore",          "jd":"Bangalore"},
    {"city":"Hyderabad",          "state":"Telangana",      "cs":"hyderabad",          "pr":"hyderabad",          "sl":"hyderabad",          "jd":"Hyderabad"},
    {"city":"Chennai",            "state":"Tamil Nadu",     "cs":"chennai",            "pr":"chennai",            "sl":"chennai",            "jd":"Chennai"},
    {"city":"Kolkata",            "state":"West Bengal",    "cs":"kolkata",            "pr":"kolkata",            "sl":"kolkata",            "jd":"Kolkata"},
    {"city":"Pune",               "state":"Maharashtra",    "cs":"pune",               "pr":"pune",               "sl":"pune",               "jd":"Pune"},
    {"city":"Ahmedabad",          "state":"Gujarat",        "cs":"ahmedabad",          "pr":"ahmedabad",          "sl":"ahmedabad",          "jd":"Ahmedabad"},
    {"city":"Jaipur",             "state":"Rajasthan",      "cs":"jaipur",             "pr":"jaipur",             "sl":"jaipur",             "jd":"Jaipur"},
    {"city":"Lucknow",            "state":"Uttar Pradesh",  "cs":"lucknow",            "pr":"lucknow",            "sl":"lucknow",            "jd":"Lucknow"},
    {"city":"Surat",              "state":"Gujarat",        "cs":"surat",              "pr":"surat",              "sl":"surat",              "jd":"Surat"},
    {"city":"Kanpur",             "state":"Uttar Pradesh",  "cs":"kanpur",             "pr":"kanpur",             "sl":"kanpur",             "jd":"Kanpur"},
    {"city":"Nagpur",             "state":"Maharashtra",    "cs":"nagpur",             "pr":"nagpur",             "sl":"nagpur",             "jd":"Nagpur"},
    {"city":"Indore",             "state":"Madhya Pradesh", "cs":"indore",             "pr":"indore",             "sl":"indore",             "jd":"Indore"},
    {"city":"Bhopal",             "state":"Madhya Pradesh", "cs":"bhopal",             "pr":"bhopal",             "sl":"bhopal",             "jd":"Bhopal"},
    {"city":"Patna",              "state":"Bihar",          "cs":"patna",              "pr":"patna",              "sl":"patna",              "jd":"Patna"},
    {"city":"Vadodara",           "state":"Gujarat",        "cs":"vadodara",           "pr":"vadodara",           "sl":"vadodara",           "jd":"Vadodara"},
    {"city":"Ludhiana",           "state":"Punjab",         "cs":"ludhiana",           "pr":"ludhiana",           "sl":"ludhiana",           "jd":"Ludhiana"},
    {"city":"Agra",               "state":"Uttar Pradesh",  "cs":"agra",               "pr":"agra",               "sl":"agra",               "jd":"Agra"},
    {"city":"Nashik",             "state":"Maharashtra",    "cs":"nashik",             "pr":"nashik",             "sl":"nashik",             "jd":"Nashik"},
    {"city":"Faridabad",          "state":"Haryana",        "cs":"faridabad",          "pr":"faridabad",          "sl":"faridabad",          "jd":"Faridabad"},
    {"city":"Meerut",             "state":"Uttar Pradesh",  "cs":"meerut",             "pr":"meerut",             "sl":"meerut",             "jd":"Meerut"},
    {"city":"Rajkot",             "state":"Gujarat",        "cs":"rajkot",             "pr":"rajkot",             "sl":"rajkot",             "jd":"Rajkot"},
    {"city":"Varanasi",           "state":"Uttar Pradesh",  "cs":"varanasi",           "pr":"varanasi",           "sl":"varanasi",           "jd":"Varanasi"},
    {"city":"Amritsar",           "state":"Punjab",         "cs":"amritsar",           "pr":"amritsar",           "sl":"amritsar",           "jd":"Amritsar"},
    {"city":"Allahabad",          "state":"Uttar Pradesh",  "cs":"prayagraj",          "pr":"allahabad",          "sl":"allahabad",          "jd":"Allahabad"},
    {"city":"Ranchi",             "state":"Jharkhand",      "cs":"ranchi",             "pr":"ranchi",             "sl":"ranchi",             "jd":"Ranchi"},
    {"city":"Coimbatore",         "state":"Tamil Nadu",     "cs":"coimbatore",         "pr":"coimbatore",         "sl":"coimbatore",         "jd":"Coimbatore"},
    {"city":"Gwalior",            "state":"Madhya Pradesh", "cs":"gwalior",            "pr":"gwalior",            "sl":"gwalior",            "jd":"Gwalior"},
    {"city":"Vijayawada",         "state":"Andhra Pradesh", "cs":"vijayawada",         "pr":"vijayawada",         "sl":"vijayawada",         "jd":"Vijayawada"},
    {"city":"Jodhpur",            "state":"Rajasthan",      "cs":"jodhpur",            "pr":"jodhpur",            "sl":"jodhpur",            "jd":"Jodhpur"},
    {"city":"Raipur",             "state":"Chhattisgarh",   "cs":"raipur",             "pr":"raipur",             "sl":"raipur",             "jd":"Raipur"},
    {"city":"Chandigarh",         "state":"Chandigarh",     "cs":"chandigarh",         "pr":"chandigarh",         "sl":"chandigarh",         "jd":"Chandigarh"},
    {"city":"Mysore",             "state":"Karnataka",      "cs":"mysore",             "pr":"mysore",             "sl":"mysore",             "jd":"Mysore"},
    {"city":"Bhubaneswar",        "state":"Odisha",         "cs":"bhubaneswar",        "pr":"bhubaneswar",        "sl":"bhubaneswar",        "jd":"Bhubaneswar"},
    {"city":"Guwahati",           "state":"Assam",          "cs":"guwahati",           "pr":"guwahati",           "sl":"guwahati",           "jd":"Guwahati"},
    {"city":"Kochi",              "state":"Kerala",         "cs":"kochi",              "pr":"kochi",              "sl":"kochi",              "jd":"Kochi"},
    {"city":"Thiruvananthapuram", "state":"Kerala",         "cs":"thiruvananthapuram", "pr":"thiruvananthapuram", "sl":"thiruvananthapuram", "jd":"Thiruvananthapuram"},
    {"city":"Visakhapatnam",      "state":"Andhra Pradesh", "cs":"visakhapatnam",      "pr":"visakhapatnam",      "sl":"visakhapatnam",      "jd":"Visakhapatnam"},
    {"city":"Jalandhar",          "state":"Punjab",         "cs":"jalandhar",          "pr":"jalandhar",          "sl":"jalandhar",          "jd":"Jalandhar"},
    {"city":"Madurai",            "state":"Tamil Nadu",     "cs":"madurai",            "pr":"madurai",            "sl":"madurai",            "jd":"Madurai"},
    {"city":"Srinagar",           "state":"J&K",            "cs":"srinagar",           "pr":"srinagar",           "sl":"srinagar",           "jd":"Srinagar"},
    {"city":"Aurangabad",         "state":"Maharashtra",    "cs":"aurangabad",         "pr":"aurangabad",         "sl":"aurangabad",         "jd":"Aurangabad"},
    {"city":"Dehradun",           "state":"Uttarakhand",    "cs":"dehradun",           "pr":"dehradun",           "sl":"dehradun",           "jd":"Dehradun"},
    {"city":"Jabalpur",           "state":"Madhya Pradesh", "cs":"jabalpur",           "pr":"jabalpur",           "sl":"jabalpur",           "jd":"Jabalpur"},
    {"city":"Warangal",           "state":"Telangana",      "cs":"warangal",           "pr":"warangal",           "sl":"warangal",           "jd":"Warangal"},
    {"city":"Hubli",              "state":"Karnataka",      "cs":"hubli",              "pr":"hubli",              "sl":"hubli",              "jd":"Hubli-Dharwad"},
    {"city":"Tiruchirappalli",    "state":"Tamil Nadu",     "cs":"tiruchirappalli",    "pr":"tiruchirappalli",    "sl":"tiruchirappalli",    "jd":"Tiruchirappalli"},
    {"city":"Bareilly",           "state":"Uttar Pradesh",  "cs":"bareilly",           "pr":"bareilly",           "sl":"bareilly",           "jd":"Bareilly"},
    {"city":"Moradabad",          "state":"Uttar Pradesh",  "cs":"moradabad",          "pr":"moradabad",          "sl":"moradabad",          "jd":"Moradabad"},
    {"city":"Salem",              "state":"Tamil Nadu",     "cs":"salem",              "pr":"salem",              "sl":"salem",              "jd":"Salem"},
]

SOURCES = ["practo", "justdial", "clinicspots", "sulekha"]

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
            log.warning(f"  Timeout ({attempt+1}/{retries}) — wait {wait}s")
            if attempt < retries-1:
                time.sleep(wait)
            else:
                return None
        except Exception as e:
            log.warning(f"  Err: {e} — retry {attempt+1}")
            time.sleep(3)
    return None

def parse_jsonld(soup, city, url, src):
    rows = []
    skip = {"WebPage","WebSite","BreadcrumbList","Organization","ItemList","SiteNavigationElement"}
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            obj = json.loads(tag.string or "")
            items = obj.get("@graph", [obj] if isinstance(obj, dict) else obj)
            if isinstance(items, dict):
                items = [items]
            for item in items:
                name = item.get("name","").strip()
                if not name or len(name) < 4 or item.get("@type","") in skip:
                    continue
                addr = item.get("address", {})
                phone = str(item.get("telephone",""))
                address = (addr.get("streetAddress","") + " " + addr.get("addressLocality","")).strip() or city["city"]
                rows.append([
                    name, "Dental Clinic", phone, "", item.get("url",""),
                    address, city["city"], city["state"],
                    item.get("aggregateRating",{}).get("ratingValue",""),
                    item.get("aggregateRating",{}).get("reviewCount",""),
                    url, now_ist(), src
                ])
        except Exception:
            pass
    return rows

def check_has_more(soup, page, param="page"):
    return page < MAX_PAGES and bool(soup.find("a", href=re.compile(rf"[?&]{param}={page+1}")))


# ─── PLAYWRIGHT BROWSER (shared instance) ───────────────────
_pw_inst = _pw_browser = _pw_ctx = None

def get_pw_ctx():
    global _pw_inst, _pw_browser, _pw_ctx
    if _pw_browser is None:
        _pw_inst    = sync_playwright().start()
        _pw_browser = _pw_inst.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-extensions",
            ]
        )
        _pw_ctx = _pw_browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            locale="en-IN",
            viewport={"width": 1280, "height": 800},
        )
        # webdriver flag hide karo
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

def pw_get_html(url, wait_selector=None, scroll=False, timeout=40000):
    """Playwright se page ka rendered HTML lo."""
    ctx = get_pw_ctx()
    pg  = ctx.new_page()
    try:
        pg.goto(url, timeout=timeout, wait_until="domcontentloaded")
        if wait_selector:
            try:
                pg.wait_for_selector(wait_selector, timeout=15000)
            except PWTimeout:
                pass  # jo mila wo lo
        if scroll:
            for _ in range(3):
                pg.evaluate("window.scrollBy(0, window.innerHeight)")
                pg.wait_for_timeout(700)
        return pg.content()
    except Exception as e:
        log.warning(f"  PW error: {e}")
        return None
    finally:
        pg.close()


# ============================================================
#  SOURCE 1 — Practo (Playwright)
#  JS-rendered hai — requests se blank milta tha
# ============================================================
def scrape_practo(city, page=1):
    url = f"https://www.practo.com/{city['pr']}/clinics/dental-clinics?page={page}"
    html = pw_get_html(
        url,
        wait_selector="div[data-qa-id='clinic_card'], div[class*='clinic'], h2",
        scroll=True
    )
    if not html:
        return [], False

    soup = BeautifulSoup(html, "html.parser")
    rows = parse_jsonld(soup, city, url, "Practo")

    # HTML fallback — Practo rendered cards
    if not rows:
        # data-qa-id based selectors
        for card in soup.find_all("div", attrs={"data-qa-id": re.compile(r"clinic_card|clinic", re.I)}):
            nt = (card.find(attrs={"data-qa-id": re.compile(r"clinic_name|name", re.I)})
                  or card.find(["h2","h3"]))
            if not nt:
                continue
            name = nt.get_text(strip=True)
            if not name or len(name) < 4:
                continue
            at = card.find(attrs={"data-qa-id": re.compile(r"locality|address", re.I)})
            rt = card.find(attrs={"data-qa-id": re.compile(r"rating", re.I)})
            rows.append([name, "Dental Clinic", "", "", "",
                at.get_text(strip=True) if at else city["city"],
                city["city"], city["state"],
                rt.get_text(strip=True) if rt else "", "",
                url, now_ist(), "Practo"])

        # class-based fallback
        if not rows:
            for card in soup.find_all("div", class_=re.compile(r"clinic.card|listing|u-con", re.I)):
                nt = card.find(["h2","h3","h4"])
                if not nt:
                    continue
                name = nt.get_text(strip=True)
                if not name or len(name) < 4:
                    continue
                at = card.find(class_=re.compile(r"addr|locality|location", re.I))
                rt = card.find(class_=re.compile(r"rating|score", re.I))
                rows.append([name, "Dental Clinic", "", "", "",
                    at.get_text(strip=True) if at else city["city"],
                    city["city"], city["state"],
                    rt.get_text(strip=True) if rt else "", "",
                    url, now_ist(), "Practo"])

    log.info(f"  Practo {city['city']} p{page}: {len(rows)}")
    return rows, check_has_more(soup, page)


# ============================================================
#  SOURCE 2 — JustDial (Playwright)
# ============================================================
def scrape_justdial(city, page=1):
    page_slug = f"/page-{page}" if page > 1 else ""
    url = f"https://www.justdial.com/{city['jd']}/Dentists/nct-10156331{page_slug}"

    html = pw_get_html(
        url,
        wait_selector="div.resultbox_info, li.cntanr, span.lng_cont_name",
        scroll=True,
        timeout=45000
    )
    if not html:
        return [], False

    soup  = BeautifulSoup(html, "html.parser")
    rows  = []
    cards = (
        soup.find_all("div", class_=re.compile(r"resultbox_info", re.I)) or
        soup.find_all("li",  class_=re.compile(r"cntanr", re.I)) or
        soup.find_all("div", class_=re.compile(r"jdcard|resultcard", re.I))
    )

    for card in cards:
        nt = (card.find(class_=re.compile(r"resultbox_title|fn|comp-name|lng_cont_name", re.I))
              or card.find(["h2","h3","h4","a"]))
        if not nt:
            continue
        name = nt.get_text(strip=True)
        if not name or len(name) < 4:
            continue
        pm = re.search(r"[\+\d][\d\s\-]{9,14}", card.get_text())
        at = card.find(class_=re.compile(r"resultbox_address|address|adr|locality", re.I))
        rt = card.find(class_=re.compile(r"green-box|rating|star|score", re.I))
        rows.append([name, "Dental Clinic",
            pm.group().strip() if pm else "", "", "",
            at.get_text(strip=True) if at else city["city"],
            city["city"], city["state"],
            rt.get_text(strip=True) if rt else "", "",
            url, now_ist(), "JustDial"])

    has_more = (page < MAX_PAGES and
                bool(soup.find("a", attrs={"title": re.compile(r"next", re.I)}) or
                     soup.find("a", href=re.compile(rf"page-{page+1}"))))

    log.info(f"  JustDial {city['city']} p{page}: {len(rows)}")
    return rows, has_more


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
            pm = re.search(r"[\+\d][\d\s\-]{9,14}", card.get_text())
            at = card.find(class_=re.compile(r"addr|address|location|area", re.I))
            rt = card.find(class_=re.compile(r"rating|star|score", re.I))
            rows.append([name, "Dental Clinic",
                pm.group().strip() if pm else "", "", "",
                at.get_text(strip=True) if at else city["city"],
                city["city"], city["state"],
                rt.get_text(strip=True) if rt else "", "",
                url, now_ist(), "Clinicspots"])
    log.info(f"  Clinicspots {city['city']} p{page}: {len(rows)}")
    return rows, check_has_more(soup, page)


# ============================================================
#  SOURCE 4 — Sulekha (requests)
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
            pm = re.search(r"[\+\d][\d\s\-]{9,14}", card.get_text())
            at = card.find(class_=re.compile(r"addr|address|location|area|locality", re.I))
            rt = card.find(class_=re.compile(r"rating|star|score", re.I))
            rows.append([name, "Dental Clinic",
                pm.group().strip() if pm else "", "", "",
                at.get_text(strip=True) if at else city["city"],
                city["city"], city["state"],
                rt.get_text(strip=True) if rt else "", "",
                url, now_ist(), "Sulekha"])
    log.info(f"  Sulekha {city['city']} p{page}: {len(rows)}")
    return rows, check_has_more(soup, page)


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
            "backgroundColor": {"red":0.1, "green":0.45, "blue":0.9}
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
    log.info("=== Dental Scraper v9 Start ===")
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
                if source == "practo":
                    rows, has_more = scrape_practo(city, page)
                elif source == "justdial":
                    rows, has_more = scrape_justdial(city, page)
                elif source == "clinicspots":
                    rows, has_more = scrape_clinicspots(city, page)
                else:
                    rows, has_more = scrape_sulekha(city, page)
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
                time.sleep(random.uniform(2.0, 4.0))
            else:
                page      = 1
                city_idx += 1
                time.sleep(random.uniform(3.0, 5.0))

    finally:
        close_pw()
        if batch:
            append_rows_to_sheet(ws, batch)
        save_state({"city_idx": city_idx, "source_idx": source_idx, "page": page})
        log.info(f"=== DONE | +{collected} aaj | Total known: {len(existing)} ===")


if __name__ == "__main__":
    main()
