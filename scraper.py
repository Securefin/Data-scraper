"""
India Dental Clinic Scraper v13
"""

import os, re, time, random, json, logging, hashlib, urllib.parse, asyncio
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
RUN_MINUTES  = int(os.environ.get("RUN_MINUTES", 10))
SHEET_NAME   = "Business Data"
STATE_SHEET  = "Scraper State"
BATCH_SIZE   = 10
MAX_PAGES    = 5
TIME_BUFFER  = 120   # state save + final batch ke liye enough buffer
SHEETS_RETRY = 5
SHEETS_WAIT  = 12

HEADERS_ROW = [
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

SOURCES = ["justdial", "clinicspots", "googlemaps"]

# ─── UTILS ──────────────────────────────────────────────────
def make_key(name, phone, address):
    return hashlib.md5(f"{name}|{phone}|{address}".lower().strip().encode()).hexdigest()

SKIP_WORDS = {
    "dental","clinic","care","centre","center","dr","doctor",
    "and","the","in","of","by","my","best","new","city",
    "multispeciality","multispecialty","super","speciality",
    "specialty","studio","smile","tooth","teeth","implant",
    "orthodontic","laser","advanced","family","health","india"
}

def is_own_website(business_name, url):
    if not url:
        return False
    m = re.search(r"https?://(?:www\.)?([^/]+)", url.lower())
    if not m:
        return False
    domain = m.group(1)
    words = re.findall(r"[a-z]+", business_name.lower())
    meaningful = [w for w in words if len(w) > 3 and w not in SKIP_WORDS]
    if not meaningful:
        return False
    return any(word in domain for word in meaningful)

def make_website_key(website, business_name=""):
    if not website or not is_own_website(business_name, website):
        return ""
    m = re.search(r"https?://(?:www\.)?([^/]+)", website.lower())
    return m.group(1) if m else website.lower().strip()

def now_ist():
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%d/%m/%Y %H:%M")

MOBILE_RE = re.compile(r"(?<!\d)(?:\+91[\s\-]?)?([6-9]\d{9})(?!\d)")
PHONE_RE  = re.compile(r"(?<!\d)(\+?91[\s\-]?\d{10}|\d{10}|\d{3,5}[\s\-]\d{6,8})(?!\d)")
WAME_RE   = re.compile(r"wa\.me/(?:91)?([6-9]\d{9})")

def extract_phone(text):
    m = WAME_RE.search(text)
    if m:
        return m.group(1)
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
                log.warning(f"  {r.status_code} — skip: {url}")
                return None
            if r.status_code in (429, 503, 504):
                wait = int(r.headers.get("Retry-After", (attempt+1)*10))
                log.warning(f"  {r.status_code} — wait {wait}s")
                time.sleep(wait)
        except requests.exceptions.Timeout:
            wait = (attempt+1)*4
            log.warning(f"  Timeout attempt {attempt+1} — wait {wait}s")
            if attempt < retries-1:
                time.sleep(wait)
            else:
                return None
        except Exception as e:
            log.warning(f"  Req err: {e}")
            time.sleep(3)
    return None

def parse_jsonld(soup, city, url, src):
    rows = []
    skip = {"WebPage","WebSite","BreadcrumbList","Organization","ItemList"}
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            obj   = json.loads(tag.string or "")
            items = obj.get("@graph", [obj] if isinstance(obj, dict) else obj)
            if isinstance(items, dict):
                items = [items]
            for item in items:
                name = item.get("name", "").strip()
                if not name or len(name) < 4 or item.get("@type", "") in skip:
                    continue
                addr     = item.get("address", {})
                phone    = extract_phone(str(item.get("telephone", "")))
                addr_str = (addr.get("streetAddress", "") + " " + addr.get("addressLocality", "")).strip() or city["city"]
                website  = item.get("url", "")
                if not is_own_website(name, website):
                    website = ""
                rows.append([name, "Dental Clinic", phone, "", website,
                    addr_str, city["city"], city["state"],
                    item.get("aggregateRating", {}).get("ratingValue", ""),
                    item.get("aggregateRating", {}).get("reviewCount", ""),
                    url, now_ist(), src])
        except Exception as e:
            log.warning(f"  JSON-LD parse warning ({src} {city['city']}): {e}")
    return rows

def check_has_more(soup, page, param="page"):
    if page >= MAX_PAGES:
        return False
    for p in [param, "pg", "p", "start"]:
        if soup.find("a", href=re.compile(rf"[?&]{p}={page+1}")):
            return True
    next_btn = soup.find("a", string=re.compile(r"next|›|»", re.I))
    if next_btn and next_btn.get("href"):
        return True
    return False


# ─── PLAYWRIGHT ─────────────────────────────────────────────
_pw_inst = _pw_browser = _pw_ctx = None

def get_pw_ctx():
    global _pw_inst, _pw_browser, _pw_ctx
    if _pw_browser is None:
        _pw_inst    = sync_playwright().start()
        _pw_browser = _pw_inst.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox",
                  "--disable-dev-shm-usage",
                  "--disable-blink-features=AutomationControlled"]
        )
        _pw_ctx = _pw_browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            locale="en-IN",
            viewport={"width": 1280, "height": 800},
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
#  SOURCE 1 — Google Maps (Place URL approach)
# ============================================================
def _gmaps_extract_place_data(pg, place_url, city):
    """Individual place URL pe jaao aur data nikalo."""
    try:
        pg.goto(place_url, timeout=20000, wait_until="domcontentloaded")
        pg.wait_for_timeout(2000)

        # Name
        name = ""
        for sel in ["h1.DUwDvf", "h1[class*='fontHeadline']", "h1"]:
            el = pg.query_selector(sel)
            if el:
                name = el.inner_text().strip()
                if name:
                    break
        if not name or len(name) < 4:
            return None

        # Phone — fresh page pe har baar naya number
        phone = ""

        # Method 1: tel: link
        tel = pg.query_selector("a[href^='tel:']")
        if tel:
            phone = extract_phone((tel.get_attribute("href") or "").replace("tel:", ""))

        # Method 2: Copy phone button
        if not phone:
            for sel in [
                "button[data-tooltip='Copy phone number']",
                "button[aria-label*='phone']",
                "button[data-item-id*='phone']",
            ]:
                btn = pg.query_selector(sel)
                if btn:
                    label = (btn.get_attribute("aria-label") or
                             btn.get_attribute("data-value") or
                             btn.inner_text() or "")
                    phone = extract_phone(label)
                    if phone:
                        break

        # Method 3: full page scan
        if not phone:
            try:
                html = pg.inner_html("div[role='main']")
                phone = extract_phone(
                    BeautifulSoup(html, "html.parser").get_text(separator=" ")
                )
            except Exception:
                pass

        # Address
        address = city["city"]
        for sel in [
            "button[data-tooltip='Copy address'] div.rogA2c",
            "button[data-item-id='address'] div.rogA2c",
            "div[data-section-id='ad'] span",
        ]:
            el = pg.query_selector(sel)
            if el:
                t = el.inner_text().strip()
                if t:
                    address = t
                    break

        # Website
        website = ""
        try:
            for sel in [
                "a[data-tooltip='Open website']",
                "a[aria-label*='website']",
                "a[data-item-id='authority']",
            ]:
                el = pg.query_selector(sel)
                if el:
                    href = el.get_attribute("href") or ""
                    if "/url?q=" in href:
                        website = urllib.parse.parse_qs(
                            urllib.parse.urlparse(href).query
                        ).get("q", [""])[0]
                    elif href.startswith("http") and "google.com" not in href:
                        website = href
                    if website:
                        break
            if not is_own_website(name, website):
                website = ""
        except Exception:
            website = ""

        # Rating + Reviews
        rating, reviews = "", ""
        rat_el = pg.query_selector("div.F7nice span[aria-hidden='true']")
        if rat_el:
            rating = rat_el.inner_text().strip()
        rev_el = pg.query_selector("div.F7nice span[aria-label*='review']")
        if rev_el:
            rm = re.search(r"[\d,]+", rev_el.get_attribute("aria-label") or "")
            if rm:
                reviews = rm.group(0)

        return [name, "Dental Clinic", phone, "", website,
                address, city["city"], city["state"],
                rating, reviews, place_url, now_ist(), "Google Maps"]

    except Exception as e:
        log.warning(f"    Place fetch error: {e}")
        return None


def scrape_googlemaps(city, page=1):
    query   = f"dental+clinic+{city['city']}+India"
    map_url = f"https://www.google.com/maps/search/{query}"
    rows    = []
    ctx     = get_pw_ctx()
    pg      = None

    try:
        pg = ctx.new_page()
        pg.goto(map_url, timeout=25000, wait_until="domcontentloaded")

        try:
            pg.click("button[aria-label*='Accept'], button[jsname='b3VHJd']", timeout=4000)
        except Exception:
            pass

        try:
            pg.wait_for_selector("div[role='feed'], div.Nv2PK", timeout=15000)
        except PWTimeout:
            log.warning(f"  GMaps {city['city']}: results load nahi hue")
            return [], False

        feed_sel = "div[role='feed']"
        for _ in range(page * 4):
            try:
                pg.eval_on_selector(feed_sel, "el => el.scrollBy(0, el.scrollHeight)")
                pg.wait_for_timeout(1200)
            except Exception:
                break

        # Sirf URLs collect karo — click nahi karna
        place_urls = pg.evaluate("""
            () => {
                const links = [];
                document.querySelectorAll('a[href*="/maps/place/"]').forEach(a => {
                    const h = a.href.split('?')[0];
                    if (h && !links.includes(h)) links.push(h);
                });
                return links.slice(0, 8);
            }
        """)

        log.info(f"  GMaps {city['city']}: {len(place_urls)} place URLs mili")

        pg.close()
        pg = None

        # Har URL ke liye fresh page
        processed = 0
        for url in place_urls:
            if processed >= 8:
                break
            detail_pg = None
            try:
                detail_pg = ctx.new_page()
                row = _gmaps_extract_place_data(detail_pg, url, city)
                if row:
                    rows.append(row)
                    log.info(f"    ✓ {row[0]} | {row[2] or 'no phone'}")
                processed += 1
                time.sleep(random.uniform(2.5, 4.0))
            except Exception as e:
                log.warning(f"    URL error: {e}")
                processed += 1
            finally:
                if detail_pg is not None:
                    try:
                        detail_pg.close()
                    except Exception:
                        pass

    except Exception as e:
        log.error(f"  GMaps {city['city']} error: {e}")
    finally:
        if pg is not None:
            try:
                pg.close()
            except Exception:
                pass

    has_more = False  # GMaps pe pagination nahi hoti — same URLs repeat hoti hain
    phone_count = sum(1 for r in rows if r[2])
    log.info(f"  GMaps {city['city']} p{page}: {len(rows)} total | {phone_count} with phone")
    return rows, has_more


# ============================================================
#  SOURCE 2 — JustDial
# ============================================================
def scrape_justdial(city, page=1):
    base = "https://www.justdial.com"
    url  = f"{base}/{city['cs']}/Dentists/nct-10215524/page-{page}"
    ctx  = get_pw_ctx()
    pg   = None
    rows = []

    try:
        pg = ctx.new_page()
        pg.goto(url, timeout=25000, wait_until="domcontentloaded")
        pg.wait_for_timeout(2500)

        for sel in [
            "button[class*='close']",
            "span[class*='close']",
            "div[class*='modal'] button",
            "[aria-label='Close']"
        ]:
            try:
                pg.click(sel, timeout=2000)
                break
            except Exception:
                pass

        try:
            pg.wait_for_selector("div[class*='resultbox_textbox']", timeout=12000)
        except PWTimeout:
            log.warning(f"  JustDial {city['city']}: cards load nahi hue")
            return [], False

        for _ in range(3):
            pg.evaluate("window.scrollBy(0, window.innerHeight)")
            pg.wait_for_timeout(1000)

        html = pg.content()
        soup = BeautifulSoup(html, "html.parser")
        cards = soup.find_all("div", class_=re.compile(r"resultbox_textbox", re.I))
        log.info(f"  JustDial {city['city']}: {len(cards)} cards mili")

        for card in cards:
            name_el = (
                card.find("span", class_=re.compile(r"resultbox_title_anchor", re.I)) or
                card.find(["h2", "h3"], class_=re.compile(r"resultbox_title", re.I))
            )
            if not name_el:
                continue
            name = name_el.get_text(strip=True)
            if not name or len(name) < 4:
                continue

            # Phone — callcontent class mein directly hota hai
            phone = ""
            cc_el = card.find(class_=re.compile(r"callcontent", re.I))
            if cc_el:
                phone = extract_phone(cc_el.get_text(strip=True))

            if not phone:
                ph_el = card.find(attrs={"data-phone": True})
                if ph_el:
                    phone = extract_phone(ph_el["data-phone"])

            if not phone:
                tel = card.find("a", href=re.compile(r"tel:"))
                if tel:
                    phone = extract_phone(tel["href"].replace("tel:", ""))

            addr_el = card.find(class_=re.compile(r"locatcity|addr|address|location", re.I))
            rat_el  = card.find(class_=re.compile(r"resultbox_totalrate|rating", re.I))
            rev_el  = card.find(class_=re.compile(r"resultbox_countrate|review", re.I))

            rows.append([
                name, "Dental Clinic", phone, "", "",
                addr_el.get_text(strip=True) if addr_el else city["city"],
                city["city"], city["state"],
                rat_el.get_text(strip=True) if rat_el else "",
                rev_el.get_text(strip=True) if rev_el else "",
                url, now_ist(), "JustDial"
            ])

        has_more = check_has_more(soup, page) or (len(rows) >= 8 and page < MAX_PAGES)
        phone_count = sum(1 for r in rows if r[2])
        log.info(f"  JustDial {city['city']} p{page}: {len(rows)} total | {phone_count} with phone")
        return rows, has_more

    except Exception as e:
        log.error(f"  JustDial {city['city']} error: {e}")
        return [], False
    finally:
        if pg is not None:
            try:
                pg.close()
            except Exception:
                pass


# ============================================================
#  SOURCE 3 — Clinicspots
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
            rows.append([name, "Dental Clinic", phone, "", "",
                at.get_text(strip=True) if at else city["city"],
                city["city"], city["state"],
                rt.get_text(strip=True) if rt else "", "",
                url, now_ist(), "Clinicspots"])
    phone_count = sum(1 for r in rows if r[2])
    log.info(f"  Clinicspots {city['city']} p{page}: {len(rows)} total | {phone_count} with phone")
    return rows, check_has_more(soup, page)


# ============================================================
#  GOOGLE SHEETS
# ============================================================
def sheets_call_with_retry(fn, *args, **kwargs):
    for attempt in range(SHEETS_RETRY):
        try:
            return fn(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            status = getattr(e.response, "status_code", None)
            if status == 429:
                wait = SHEETS_WAIT * (attempt + 1)
                log.warning(f"  Sheets quota — {wait}s wait (attempt {attempt+1}/{SHEETS_RETRY})")
                time.sleep(wait)
            else:
                log.error(f"  Sheets API error: {e}")
                raise
        except Exception as e:
            log.error(f"  Sheets call error: {e}")
            if attempt < SHEETS_RETRY - 1:
                time.sleep(SHEETS_WAIT)
            else:
                raise
    return None

def get_sheet():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    sheet_id   = os.environ.get("SHEET_ID")
    if not creds_json or not sheet_id:
        raise ValueError("Env variables missing: GOOGLE_CREDENTIALS or SHEET_ID")
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
        sheets_call_with_retry(ws.append_row, HEADERS_ROW)
        sheets_call_with_retry(ws.format, "A1:M1", {
            "textFormat"     : {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
            "backgroundColor": {"red": 0.1, "green": 0.45, "blue": 0.9}
        })

    try:
        state_ws = sheet.worksheet(STATE_SHEET)
    except gspread.WorksheetNotFound:
        state_ws = sheet.add_worksheet(STATE_SHEET, rows=10, cols=5)
        sheets_call_with_retry(state_ws.append_row, ["city_idx", "source_idx", "page", "updated_at"])
        sheets_call_with_retry(state_ws.append_row, [0, 0, 1, now_ist()])

    return ws, state_ws

def load_state_from_sheet(state_ws):
    try:
        rows = sheets_call_with_retry(state_ws.get_all_values)
        if rows and len(rows) >= 2:
            data = rows[1]
            return {
                "city_idx"  : int(data[0]),
                "source_idx": int(data[1]),
                "page"      : int(data[2])
            }
    except Exception as e:
        log.warning(f"State load error: {e}")
    return {"city_idx": 0, "source_idx": 0, "page": 1}

def save_state_to_sheet(state_ws, state):
    try:
        sheets_call_with_retry(state_ws.update, [[
            state["city_idx"],
            state["source_idx"],
            state["page"],
            now_ist()
        ]], "A2:D2")
        log.info(f"  State saved → city:{state['city_idx']} source:{state['source_idx']} page:{state['page']}")
    except Exception as e:
        log.error(f"State save FAILED: {e}")

def get_existing_keys(ws):
    keys = set()
    try:
        # Ek hi API call mein saare columns fetch karo (3 calls ki jagah 1)
        all_vals = sheets_call_with_retry(ws.get_all_values)
        for row in (all_vals or [])[1:]:   # header row skip
            n = row[0]  if len(row) > 0 else ""
            p = row[2]  if len(row) > 2 else ""
            a = row[5]  if len(row) > 5 else ""
            keys.add(make_key(n, p, a))
    except Exception as e:
        log.warning(f"  Existing keys fetch error: {e}")
    return keys

def append_rows_to_sheet(ws, rows):
    if rows:
        sheets_call_with_retry(ws.append_rows, rows, value_input_option="RAW")
        log.info(f"  ✓ {len(rows)} rows sheet mein likhe")


# ============================================================
#  MAIN
# ============================================================
def main():
    log.info("=== Dental Scraper v13 Start ===")
    log.info(f"=== Run limit: {RUN_MINUTES} minutes ===")

    ws, state_ws = get_sheet()
    existing     = get_existing_keys(ws)
    log.info(f"Sheet mein already {len(existing)} records hain")

    state      = load_state_from_sheet(state_ws)
    city_idx   = state["city_idx"]
    source_idx = state["source_idx"]
    page       = state["page"]
    log.info(f"Resume from → city:{city_idx} source:{source_idx} page:{page}")

    # State validate karo — invalid values ko immediately reset karo
    if source_idx >= len(SOURCES) or city_idx >= len(CITIES):
        log.warning(f"  Invalid state detected (city:{city_idx} source:{source_idx}) — resetting to 0,0,1")
        city_idx, source_idx, page = 0, 0, 1

    collected     = 0
    batch         = []
    seen_websites = set()
    start_time    = time.time()
    limit_secs    = RUN_MINUTES * 60
    total_sources = len(SOURCES)
    all_done      = False

    def time_up():
        return (limit_secs - (time.time() - start_time)) <= TIME_BUFFER

    try:
        while not time_up() and not all_done:
            if city_idx >= len(CITIES):
                city_idx    = 0
                source_idx += 1
                page        = 1
                if source_idx >= total_sources:
                    log.info("=== Saare sources aur cities complete! ===")
                    all_done = True
                    break
                log.info(f"  Switching source → {SOURCES[source_idx]}")

            city   = CITIES[city_idx]
            source = SOURCES[source_idx]
            elapsed = int(time.time() - start_time)
            log.info(f"[{elapsed}s/{limit_secs}s] {source} | {city['city']} | page {page}")

            try:
                if source == "justdial":
                    rows, has_more = scrape_justdial(city, page)
                elif source == "googlemaps":
                    rows, has_more = scrape_googlemaps(city, page)
                else:
                    rows, has_more = scrape_clinicspots(city, page)
            except Exception as e:
                log.error(f"  Scrape ERROR: {e}")
                city_idx += 1
                page      = 1
                time.sleep(3)
                continue

            for row in rows:
                if time_up():
                    break
                key     = make_key(str(row[0]), str(row[2]), str(row[5]))
                web_key = make_website_key(str(row[4]), str(row[0]))
                if key in existing:
                    continue
                if web_key and web_key in seen_websites:
                    log.info(f"    Skip duplicate website: {web_key}")
                    continue
                batch.append(row)
                existing.add(key)
                if web_key:
                    seen_websites.add(web_key)
                collected += 1

            if len(batch) >= BATCH_SIZE:
                try:
                    append_rows_to_sheet(ws, batch)
                    batch = []
                except Exception as e:
                    log.error(f"  Batch save failed: {e}")

            if has_more and not time_up():
                page += 1
                time.sleep(random.uniform(2.0, 4.0))
            else:
                page      = 1
                city_idx += 1
                time.sleep(random.uniform(3.0, 5.0))

    finally:
        close_pw()

        if batch:
            try:
                append_rows_to_sheet(ws, batch)
            except Exception as e:
                log.error(f"  Final batch save failed: {e}")

        # Saare sources complete ho gaye → next run ke liye fresh start
        if all_done or source_idx >= total_sources:
            city_idx   = 0
            source_idx = 0
            page       = 1
            log.info("  State reset → next run fresh start karega")

        try:
            save_state_to_sheet(state_ws, {
                "city_idx"  : city_idx,
                "source_idx": source_idx,
                "page"      : page
            })
        except Exception as e:
            log.error(f"  Final state save failed: {e}")

        elapsed = int(time.time() - start_time)
        log.info(f"=== DONE | +{collected} is run mein | {elapsed}s chala | Total known: {len(existing)} ===")


if __name__ == "__main__":
    main()
