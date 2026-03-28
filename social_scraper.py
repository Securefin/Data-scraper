"""
Social Media Dental Clinic Scraper v2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Sources:
  1. Instagram — cookies se login, dental clinic profiles scrape
  2. Facebook  — public pages, no login needed

GitHub Secret mein daalo:
  INSTAGRAM_COOKIES = [... cookie JSON ...]
  GOOGLE_CREDENTIALS = {...}
  SHEET_ID = your_sheet_id
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
DAILY_LIMIT  = 80           # social media pe kam — safe rahega
SHEET_NAME   = "Social Leads"
BATCH_SIZE   = 10
HEADERS_ROW  = [
    "Name", "Phone", "Email", "Website",
    "Address", "City", "State",
    "Followers", "Profile URL", "Fetched On", "Source"
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
]

CITIES = [
    {"city":"Mumbai",    "state":"Maharashtra"},
    {"city":"Delhi",     "state":"Delhi"},
    {"city":"Bangalore", "state":"Karnataka"},
    {"city":"Hyderabad", "state":"Telangana"},
    {"city":"Chennai",   "state":"Tamil Nadu"},
    {"city":"Kolkata",   "state":"West Bengal"},
    {"city":"Pune",      "state":"Maharashtra"},
    {"city":"Ahmedabad", "state":"Gujarat"},
    {"city":"Jaipur",    "state":"Rajasthan"},
    {"city":"Lucknow",   "state":"Uttar Pradesh"},
    {"city":"Surat",     "state":"Gujarat"},
    {"city":"Nagpur",    "state":"Maharashtra"},
    {"city":"Indore",    "state":"Madhya Pradesh"},
    {"city":"Chandigarh","state":"Chandigarh"},
    {"city":"Kochi",     "state":"Kerala"},
    {"city":"Coimbatore","state":"Tamil Nadu"},
    {"city":"Vadodara",  "state":"Gujarat"},
    {"city":"Patna",     "state":"Bihar"},
    {"city":"Ranchi",    "state":"Jharkhand"},
    {"city":"Bhopal",    "state":"Madhya Pradesh"},
]

SOURCES    = ["instagram", "facebook"]
STATE_FILE = "social_state.json"

# ─── UTILS ──────────────────────────────────────────────────
def make_key(name, phone, url):
    return hashlib.md5(f"{name}|{phone}|{url}".lower().strip().encode()).hexdigest()

def now_ist():
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%d/%m/%Y %H:%M")

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"city_idx": 0, "source_idx": 0}

def save_state(s):
    with open(STATE_FILE, "w") as f:
        json.dump(s, f)

# ─── REGEX ──────────────────────────────────────────────────
MOBILE_RE  = re.compile(r"(?<!\d)(?:\+91[\s\-]?)?([6-9]\d{9})(?!\d)")
EMAIL_RE   = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
SKIP_SITES = {"facebook.com","instagram.com","twitter.com","youtube.com",
              "wa.me","whatsapp.com","google.com","linktr.ee","linkedin.com"}

def extract_mobile(text):
    text = re.sub(r"\s+","",text)
    m = MOBILE_RE.search(text)
    return m.group(1) if m else ""

def extract_email(text):
    m = EMAIL_RE.search(text)
    return m.group(0) if m else ""

def extract_website(text):
    for m in re.finditer(r"https?://(?:www\.)?([^\s/\"\'<>]+\.[a-z]{2,})[^\s\"\'<>]*", text):
        domain = m.group(1).lower()
        if not any(s in domain for s in SKIP_SITES):
            return m.group(0)
    return ""

# ─── PLAYWRIGHT ─────────────────────────────────────────────
_pw_inst = _pw_browser = None
_ig_ctx  = None   # Instagram — cookies wala context
_fb_ctx  = None   # Facebook — fresh context

def _launch_browser():
    global _pw_inst, _pw_browser
    if _pw_browser is None:
        _pw_inst    = sync_playwright().start()
        _pw_browser = _pw_inst.chromium.launch(
            headless=True,
            args=["--no-sandbox","--disable-setuid-sandbox",
                  "--disable-dev-shm-usage",
                  "--disable-blink-features=AutomationControlled"]
        )

def get_ig_ctx():
    """Instagram context — cookies inject karo."""
    global _ig_ctx
    _launch_browser()
    if _ig_ctx is None:
        raw = os.environ.get("INSTAGRAM_COOKIES","[]")
        try:
            cookies = json.loads(raw)
        except Exception:
            log.error("INSTAGRAM_COOKIES parse error!")
            cookies = []

        # Cookie-Editor format → Playwright format convert
        pw_cookies = []
        for c in cookies:
            pw_c = {
                "name"    : c.get("name",""),
                "value"   : c.get("value",""),
                "domain"  : c.get("domain",".instagram.com"),
                "path"    : c.get("path","/"),
                "secure"  : c.get("secure", True),
                "httpOnly": c.get("httpOnly", False),
            }
            if "expirationDate" in c:
                pw_c["expires"] = int(c["expirationDate"])
            pw_cookies.append(pw_c)

        _ig_ctx = _pw_browser.new_context(
            user_agent=USER_AGENTS[0],
            locale="en-US",
            viewport={"width": 1280, "height": 800},
        )
        _ig_ctx.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
        )
        if pw_cookies:
            _ig_ctx.add_cookies(pw_cookies)
            log.info(f"  Instagram: {len(pw_cookies)} cookies injected ✓")
    return _ig_ctx

def get_fb_ctx():
    global _fb_ctx
    _launch_browser()
    if _fb_ctx is None:
        _fb_ctx = _pw_browser.new_context(
            user_agent=USER_AGENTS[0],
            locale="en-US",
            viewport={"width": 1280, "height": 800},
        )
        _fb_ctx.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
        )
    return _fb_ctx

def close_pw():
    global _pw_inst, _pw_browser, _ig_ctx, _fb_ctx
    if _pw_browser:
        try:
            _pw_browser.close()
            _pw_inst.stop()
        except Exception:
            pass
        _pw_inst = _pw_browser = _ig_ctx = _fb_ctx = None

def pw_fetch(ctx, url, wait_sel=None, scroll=False, timeout=35000):
    pg = ctx.new_page()
    try:
        pg.goto(url, timeout=timeout, wait_until="domcontentloaded")
        try:
            pg.wait_for_load_state("networkidle", timeout=7000)
        except PWTimeout:
            pass
        if wait_sel:
            try:
                pg.wait_for_selector(wait_sel, timeout=10000)
            except PWTimeout:
                pass
        if scroll:
            for _ in range(3):
                pg.evaluate("window.scrollBy(0, window.innerHeight)")
                pg.wait_for_timeout(800)
        return pg.content()
    except Exception as e:
        log.warning(f"  PW fetch error: {e}")
        return None
    finally:
        pg.close()

# ─── SEARCH — Bing (GitHub Actions pe kaam karta hai) ───────
def search_urls(query, max_results=8):
    """
    Bing search — DuckDuckGo GitHub Actions pe block karta hai.
    Bing publicly accessible hai, koi API key nahi chahiye.
    """
    import urllib.parse
    urls = []

    # Method 1 — Bing
    try:
        search_url = f"https://www.bing.com/search?q={urllib.parse.quote(query)}&count=10"
        r = requests.get(
            search_url,
            headers={
                "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
                "Accept-Language": "en-IN,en;q=0.9",
                "Accept"    : "text/html,application/xhtml+xml,*/*;q=0.8",
            },
            timeout=20
        )
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.startswith("http") and "bing.com" not in href and "microsoft.com" not in href:
                    if len(urls) < max_results:
                        urls.append(href)
    except Exception as e:
        log.warning(f"  Bing search error: {e}")

    # Method 2 — Fallback: Google search (simple HTTP)
    if not urls:
        try:
            search_url = f"https://www.google.com/search?q={urllib.parse.quote(query)}&num=10"
            r = requests.get(
                search_url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
                    "Accept-Language": "en-IN,en;q=0.9",
                },
                timeout=20
            )
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    # Google results /url?q= format mein hote hain
                    if "/url?q=" in href:
                        actual = urllib.parse.parse_qs(
                            urllib.parse.urlparse(href).query
                        ).get("q", [""])[0]
                        if actual.startswith("http") and len(urls) < max_results:
                            urls.append(actual)
        except Exception as e:
            log.warning(f"  Google search error: {e}")

    log.info(f"  Search results: {len(urls)} URLs found")
    return urls


# ============================================================
#  SOURCE 1 — Instagram
# ============================================================
def scrape_ig_profile(profile_url, city):
    """Ek Instagram profile scrape karo — cookies se logged in hai."""
    html = pw_fetch(
        get_ig_ctx(), profile_url,
        wait_sel="header section, div._aa_c, main article",
        scroll=False
    )
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ")

    # Login page aa gaya? — cookies expire ho gayi
    if "Log in" in text[:500] or "log_in" in html[:2000]:
        log.warning("  Instagram: cookies expire ho gayi! INSTAGRAM_COOKIES update karo.")
        return None

    # Name — og:title se
    name = ""
    og   = soup.find("meta", property="og:title")
    if og:
        name = og.get("content","").replace("• Instagram","").strip()
        name = re.sub(r"\s*\(@[^)]+\)","",name).strip()
    if not name or len(name) < 4:
        return None

    # Dental clinic check
    dental_kw = ["dental","dentist","teeth","orthodont","clinic","dent","smile","oral"]
    og_desc   = soup.find("meta", property="og:description")
    bio       = og_desc.get("content","") if og_desc else ""
    if not any(k in (name+bio).lower() for k in dental_kw):
        return None

    phone    = extract_mobile(bio + " " + text[:3000])
    email    = extract_email(bio + " " + text[:3000])
    website  = extract_website(bio)
    followers = ""
    fm = re.search(r"([\d,\.]+[KkMm]?)\s*Followers", bio+text[:500], re.I)
    if fm:
        followers = fm.group(1)

    log.info(f"    IG ✓ {name} | ph={phone} | em={email} | web={website or 'none'}")
    return [
        name, phone, email, website,
        city["city"], city["city"], city["state"],
        followers, profile_url, now_ist(), "Instagram"
    ]

def scrape_instagram_city(city, limit=10):
    query = f"dental clinic {city['city']} India site:instagram.com"
    log.info(f"  IG search: {query}")
    urls  = search_urls(query, max_results=12)    rows  = []

    for url in urls:
        if "instagram.com" not in url:
            continue
        # Sirf profile URLs — posts/reels/stories skip
        m = re.match(r"(https://(?:www\.)?instagram\.com/[A-Za-z0-9_.]+)/?", url)
        if not m:
            continue
        profile_url = m.group(1) + "/"
        if any(x in profile_url for x in ["/p/","/reel/","/stories/","/explore/"]):
            continue

        try:
            row = scrape_ig_profile(profile_url, city)
            if row:
                rows.append(row)
        except Exception as e:
            log.warning(f"    IG error: {e}")

        time.sleep(random.uniform(4.0, 7.0))  # Instagram pe zyada wait
        if len(rows) >= limit:
            break

    log.info(f"  Instagram {city['city']}: {len(rows)} records")
    return rows


# ============================================================
#  SOURCE 2 — Facebook Public Pages
# ============================================================
def scrape_fb_page(page_url, city):
    """Facebook page ka /about section scrape karo."""
    about_url = page_url.rstrip("/") + "/about"
    html = pw_fetch(
        get_fb_ctx(), about_url,
        wait_sel="div[data-pagelet='PageAbout'], div[role='main'], div#content_container",
    )
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ")

    # Name
    name = ""
    og = soup.find("meta", property="og:title")
    if og:
        name = og.get("content","").replace("| Facebook","").replace("- Facebook","").strip()
    if not name or len(name) < 4:
        return None

    dental_kw = ["dental","dentist","teeth","orthodont","clinic","dent","smile"]
    if not any(k in (name+text[:500]).lower() for k in dental_kw):
        return None

    phone    = extract_mobile(text)
    email    = extract_email(text)
    website  = extract_website(text)

    followers = ""
    fm = re.search(r"([\d,\.]+)\s*(?:followers|people like)", text, re.I)
    if fm:
        followers = fm.group(1)

    addr = city["city"]
    am = re.search(rf"([^.\n]{{5,80}}{re.escape(city['city'])}[^.\n]{{0,60}})", text, re.I)
    if am:
        addr = am.group(1).strip()[:100]

    log.info(f"    FB ✓ {name} | ph={phone} | em={email} | web={website or 'none'}")
    return [
        name, phone, email, website,
        addr, city["city"], city["state"],
        followers, page_url, now_ist(), "Facebook"
    ]

def scrape_facebook_city(city, limit=10):
    query = f"dental clinic {city['city']} India site:facebook.com"
    log.info(f"  FB search: {query}")
    urls = search_urls(query, max_results=12)
    rows = []

    for url in urls:
        if "facebook.com" not in url:
            continue
        if any(x in url for x in ["/events/","/photos/","/videos/","/posts/","/groups/","/marketplace/"]):
            continue
        try:
            row = scrape_fb_page(url, city)
            if row:
                rows.append(row)
        except Exception as e:
            log.warning(f"    FB error: {e}")

        time.sleep(random.uniform(3.0, 5.0))
        if len(rows) >= limit:
            break

    log.info(f"  Facebook {city['city']}: {len(rows)} records")
    return rows


# ============================================================
#  GOOGLE SHEETS
# ============================================================
def get_sheet():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    sheet_id   = os.environ.get("SHEET_ID")
    if not creds_json or not sheet_id:
        raise ValueError("GOOGLE_CREDENTIALS ya SHEET_ID nahi mili!")
    creds  = Credentials.from_service_account_info(
        json.loads(creds_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(creds)
    sheet  = client.open_by_key(sheet_id)
    try:
        ws = sheet.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(SHEET_NAME, rows=10000, cols=11)
        ws.append_row(HEADERS_ROW)
        ws.format("A1:K1", {
            "textFormat"     : {"bold": True, "foregroundColor": {"red":1,"green":1,"blue":1}},
            "backgroundColor": {"red":0.85, "green":0.33, "blue":0.1}
        })
    return ws

def get_existing_keys(ws):
    keys = set()
    for row in ws.get_all_values()[1:]:
        if len(row) >= 9:
            k = make_key(row[0], row[1], row[8])
            keys.add(k)
    return keys

def append_rows(ws, rows):
    if rows:
        ws.append_rows(rows, value_input_option="RAW")
        log.info(f"  ✓ {len(rows)} rows sheet mein likhe")


# ============================================================
#  MAIN
# ============================================================
def main():
    log.info("=== Social Scraper v2 Start ===")
    ws       = get_sheet()
    existing = get_existing_keys(ws)
    log.info(f"Sheet mein already {len(existing)} records hain")

    state      = load_state()
    city_idx   = state["city_idx"]
    source_idx = state["source_idx"]

    collected = 0
    batch     = []

    try:
        while collected < DAILY_LIMIT:
            if city_idx >= len(CITIES):
                city_idx   = 0
                source_idx = (source_idx + 1) % len(SOURCES)
                log.info(f"  All cities done — switching to: {SOURCES[source_idx]}")

            city   = CITIES[city_idx]
            source = SOURCES[source_idx]
            log.info(f"[{collected}/{DAILY_LIMIT}] {source} | {city['city']}")

            try:
                limit = min(10, DAILY_LIMIT - collected)
                if source == "instagram":
                    rows = scrape_instagram_city(city, limit)
                else:
                    rows = scrape_facebook_city(city, limit)
            except Exception as e:
                log.error(f"  ERROR: {e}")
                city_idx += 1
                time.sleep(3)
                continue

            for row in rows:
                if collected >= DAILY_LIMIT:
                    break
                key = make_key(str(row[0]), str(row[1]), str(row[8]))
                if key in existing:
                    continue
                batch.append(row)
                existing.add(key)
                collected += 1

            if len(batch) >= BATCH_SIZE:
                append_rows(ws, batch)
                batch = []

            city_idx += 1
            time.sleep(random.uniform(3.0, 6.0))

    finally:
        close_pw()
        if batch:
            append_rows(ws, batch)
        save_state({"city_idx": city_idx, "source_idx": source_idx})
        log.info(f"=== DONE | +{collected} aaj | Total: {len(existing)} ===")


if __name__ == "__main__":
    main()
