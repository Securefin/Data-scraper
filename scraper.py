"""
India Dental Clinic Scraper
Sources: Practo + JustDial + IndiaMart
Runs on GitHub Actions daily — saves to Google Sheets
"""

import os
import re
import time
import random
import json
import logging
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
DAILY_LIMIT  = 200          # Records per run
SHEET_NAME   = "Business Data"
HEADERS_ROW  = ["Name","Category","Phone","Email","Website",
                "Address","City","State","Rating","Reviews",
                "Source URL","Fetched On","Source"]

# ─── USER AGENTS ────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148 Safari/604.1",
]

# ─── ALL 50 CITIES ──────────────────────────────────────────
CITIES = [
    {"city": "Mumbai",              "state": "Maharashtra",    "practo": "mumbai",          "jd": "mumbai",       "im": "mumbai"},
    {"city": "Delhi",               "state": "Delhi",          "practo": "delhi",           "jd": "delhi",        "im": "delhi"},
    {"city": "Bangalore",           "state": "Karnataka",      "practo": "bangalore",       "jd": "bangalore",    "im": "bangalore"},
    {"city": "Hyderabad",           "state": "Telangana",      "practo": "hyderabad",       "jd": "hyderabad",    "im": "hyderabad"},
    {"city": "Chennai",             "state": "Tamil Nadu",     "practo": "chennai",         "jd": "chennai",      "im": "chennai"},
    {"city": "Kolkata",             "state": "West Bengal",    "practo": "kolkata",         "jd": "kolkata",      "im": "kolkata"},
    {"city": "Pune",                "state": "Maharashtra",    "practo": "pune",            "jd": "pune",         "im": "pune"},
    {"city": "Ahmedabad",           "state": "Gujarat",        "practo": "ahmedabad",       "jd": "ahmedabad",    "im": "ahmedabad"},
    {"city": "Jaipur",              "state": "Rajasthan",      "practo": "jaipur",          "jd": "jaipur",       "im": "jaipur"},
    {"city": "Lucknow",             "state": "Uttar Pradesh",  "practo": "lucknow",         "jd": "lucknow",      "im": "lucknow"},
    {"city": "Surat",               "state": "Gujarat",        "practo": "surat",           "jd": "surat",        "im": "surat"},
    {"city": "Kanpur",              "state": "Uttar Pradesh",  "practo": "kanpur",          "jd": "kanpur",       "im": "kanpur"},
    {"city": "Nagpur",              "state": "Maharashtra",    "practo": "nagpur",          "jd": "nagpur",       "im": "nagpur"},
    {"city": "Indore",              "state": "Madhya Pradesh", "practo": "indore",          "jd": "indore",       "im": "indore"},
    {"city": "Bhopal",              "state": "Madhya Pradesh", "practo": "bhopal",          "jd": "bhopal",       "im": "bhopal"},
    {"city": "Patna",               "state": "Bihar",          "practo": "patna",           "jd": "patna",        "im": "patna"},
    {"city": "Vadodara",            "state": "Gujarat",        "practo": "vadodara",        "jd": "vadodara",     "im": "vadodara"},
    {"city": "Ludhiana",            "state": "Punjab",         "practo": "ludhiana",        "jd": "ludhiana",     "im": "ludhiana"},
    {"city": "Agra",                "state": "Uttar Pradesh",  "practo": "agra",            "jd": "agra",         "im": "agra"},
    {"city": "Nashik",              "state": "Maharashtra",    "practo": "nashik",          "jd": "nashik",       "im": "nashik"},
    {"city": "Faridabad",           "state": "Haryana",        "practo": "faridabad",       "jd": "faridabad",    "im": "faridabad"},
    {"city": "Meerut",              "state": "Uttar Pradesh",  "practo": "meerut",          "jd": "meerut",       "im": "meerut"},
    {"city": "Rajkot",              "state": "Gujarat",        "practo": "rajkot",          "jd": "rajkot",       "im": "rajkot"},
    {"city": "Varanasi",            "state": "Uttar Pradesh",  "practo": "varanasi",        "jd": "varanasi",     "im": "varanasi"},
    {"city": "Amritsar",            "state": "Punjab",         "practo": "amritsar",        "jd": "amritsar",     "im": "amritsar"},
    {"city": "Allahabad",           "state": "Uttar Pradesh",  "practo": "allahabad",       "jd": "allahabad",    "im": "allahabad"},
    {"city": "Ranchi",              "state": "Jharkhand",      "practo": "ranchi",          "jd": "ranchi",       "im": "ranchi"},
    {"city": "Coimbatore",          "state": "Tamil Nadu",     "practo": "coimbatore",      "jd": "coimbatore",   "im": "coimbatore"},
    {"city": "Gwalior",             "state": "Madhya Pradesh", "practo": "gwalior",         "jd": "gwalior",      "im": "gwalior"},
    {"city": "Vijayawada",          "state": "Andhra Pradesh", "practo": "vijayawada",      "jd": "vijayawada",   "im": "vijayawada"},
    {"city": "Jodhpur",             "state": "Rajasthan",      "practo": "jodhpur",         "jd": "jodhpur",      "im": "jodhpur"},
    {"city": "Raipur",              "state": "Chhattisgarh",   "practo": "raipur",          "jd": "raipur",       "im": "raipur"},
    {"city": "Chandigarh",          "state": "Chandigarh",     "practo": "chandigarh",      "jd": "chandigarh",   "im": "chandigarh"},
    {"city": "Mysore",              "state": "Karnataka",      "practo": "mysuru",          "jd": "mysore",       "im": "mysore"},
    {"city": "Bhubaneswar",         "state": "Odisha",         "practo": "bhubaneswar",     "jd": "bhubaneswar",  "im": "bhubaneswar"},
    {"city": "Guwahati",            "state": "Assam",          "practo": "guwahati",        "jd": "guwahati",     "im": "guwahati"},
    {"city": "Kochi",               "state": "Kerala",         "practo": "ernakulam",       "jd": "kochi",        "im": "kochi"},
    {"city": "Thiruvananthapuram",  "state": "Kerala",         "practo": "trivandrum",      "jd": "thiruvananthapuram", "im": "thiruvananthapuram"},
    {"city": "Visakhapatnam",       "state": "Andhra Pradesh", "practo": "visakhapatnam",   "jd": "visakhapatnam","im": "visakhapatnam"},
    {"city": "Jalandhar",           "state": "Punjab",         "practo": "jalandhar",       "jd": "jalandhar",    "im": "jalandhar"},
    {"city": "Madurai",             "state": "Tamil Nadu",     "practo": "madurai",         "jd": "madurai",      "im": "madurai"},
    {"city": "Srinagar",            "state": "J&K",            "practo": "srinagar",        "jd": "srinagar",     "im": "srinagar"},
    {"city": "Aurangabad",          "state": "Maharashtra",    "practo": "aurangabad",      "jd": "aurangabad",   "im": "aurangabad"},
    {"city": "Dehradun",            "state": "Uttarakhand",    "practo": "dehradun",        "jd": "dehradun",     "im": "dehradun"},
    {"city": "Jabalpur",            "state": "Madhya Pradesh", "practo": "jabalpur",        "jd": "jabalpur",     "im": "jabalpur"},
    {"city": "Warangal",            "state": "Telangana",      "practo": "warangal",        "jd": "warangal",     "im": "warangal"},
    {"city": "Hubli",               "state": "Karnataka",      "practo": "hubli",           "jd": "hubli",        "im": "hubli"},
    {"city": "Tiruchirappalli",     "state": "Tamil Nadu",     "practo": "tiruchirappalli", "jd": "tiruchirappalli","im": "tiruchirappalli"},
    {"city": "Bareilly",            "state": "Uttar Pradesh",  "practo": "bareilly",        "jd": "bareilly",     "im": "bareilly"},
    {"city": "Moradabad",           "state": "Uttar Pradesh",  "practo": "moradabad",       "jd": "moradabad",    "im": "moradabad"},
    {"city": "Salem",               "state": "Tamil Nadu",     "practo": "salem",           "jd": "salem",        "im": "salem"},
]

# ─── STATE FILE — resume karne ke liye ──────────────────────
STATE_FILE = "state.json"

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"city_idx": 0, "source_idx": 0, "page": 1}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

# ─── HTTP HELPER ─────────────────────────────────────────────
def get_html(url, extra_headers=None, retries=3):
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
            resp = requests.get(url, headers=headers, timeout=20)
            if resp.status_code == 200:
                return resp.text
            if resp.status_code in (403, 404):
                log.warning(f"  {resp.status_code} — {url}")
                return None
            if resp.status_code in (429, 503, 504):
                wait = (attempt + 1) * 4
                log.warning(f"  {resp.status_code} — retry in {wait}s")
                time.sleep(wait)
                continue
        except Exception as e:
            log.warning(f"  Request error: {e} — retry {attempt+1}")
            time.sleep(3)
    return None

# ─── TIMESTAMP ──────────────────────────────────────────────
def now_ist():
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%d/%m/%Y %H:%M")

# ============================================================
#  SOURCE 1 — Practo (renders HTML server-side for Googlebot)
# ============================================================
def scrape_practo(city, page=1):
    url  = f"https://www.practo.com/{city['practo']}/dentist?page={page}"
    html = get_html(url, extra_headers={
        "User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
        "Referer"   : "https://www.google.com/"
    })
    if not html:
        return [], False

    soup    = BeautifulSoup(html, "html.parser")
    rows    = []
    has_more = page < 10

    # Method 1: JSON-LD
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            obj   = json.loads(tag.string or "")
            items = obj.get("@graph", [obj] if obj.get("@type") else [])
            for item in items:
                name = item.get("name", "")
                if not name or "practo" in name.lower():
                    continue
                addr = item.get("address", {})
                rows.append([
                    name, "Dental Clinic",
                    item.get("telephone", ""), "",
                    item.get("url", ""),
                    (addr.get("streetAddress","") + ", " + addr.get("addressLocality","")).strip(", "),
                    city["city"], city["state"],
                    item.get("aggregateRating", {}).get("ratingValue", ""),
                    item.get("aggregateRating", {}).get("reviewCount", ""),
                    url, now_ist(), "Practo"
                ])
        except Exception:
            pass

    # Method 2: Embedded JSON in page scripts
    if not rows:
        matches = re.findall(r'"name"\s*:\s*"([^"]{5,80})".*?"specialization"\s*:\s*"Dentist"', html)
        for name in matches:
            rows.append([name,"Dental Clinic","","","","",city["city"],city["state"],"","",url,now_ist(),"Practo"])

    # Check if more pages exist
    if "No doctors" in html or "0 results" in html or not rows:
        has_more = False

    log.info(f"  Practo {city['city']} p{page}: {len(rows)} records")
    return rows, has_more


# ============================================================
#  SOURCE 2 — JustDial (static HTML with JSON-LD)
# ============================================================
def scrape_justdial(city, page=1):
    keyword = "dental-clinic"
    url     = f"https://www.justdial.com/{city['jd']}/{keyword}/page-{page}"
    time.sleep(random.uniform(2, 4))  # bot detection se bachao

    html = get_html(url, extra_headers={
        "Referer"    : "https://www.justdial.com/",
        "Cache-Control": "no-cache"
    })
    if not html:
        return [], False

    soup     = BeautifulSoup(html, "html.parser")
    rows     = []
    has_more = "Next" in html and "No results" not in html and page < 5

    # Method 1: JSON-LD
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            obj  = json.loads(tag.string or "")
            objs = obj if isinstance(obj, list) else [obj]
            for item in objs:
                name = item.get("name","")
                if not name:
                    continue
                addr = item.get("address", {})
                rows.append([
                    name, "Dental Clinic",
                    item.get("telephone",""), "",
                    item.get("url",""),
                    addr.get("streetAddress", city["city"]),
                    city["city"], city["state"],
                    "", "", url, now_ist(), "JustDial"
                ])
        except Exception:
            pass

    # Method 2: HTML regex fallback
    if not rows:
        names  = re.findall(r'class="lng_cont_name"[^>]*><span[^>]*>([^<]+)</span>', html)
        phones = re.findall(r'tel:([0-9+\-\s]{7,15})"', html)
        addrs  = re.findall(r'class="cont_fl_addr"[^>]*>([\s\S]*?)</span>', html)
        addrs  = [re.sub(r"<[^>]+>","",a).strip() for a in addrs]
        for i, name in enumerate(names):
            rows.append([
                name.strip(), "Dental Clinic",
                phones[i] if i < len(phones) else "", "", "",
                addrs[i]  if i < len(addrs)  else city["city"],
                city["city"], city["state"],
                "", "", url, now_ist(), "JustDial"
            ])

    log.info(f"  JustDial {city['city']} p{page}: {len(rows)} records")
    return rows, has_more


# ============================================================
#  SOURCE 3 — IndiaMart Directory (static HTML)
# ============================================================
def scrape_indiamart(city, page=1):
    url  = f"https://dir.indiamart.com/{city['im']}/dental-clinic.html?bpg={page}"
    html = get_html(url, extra_headers={
        "Referer": "https://dir.indiamart.com/"
    })
    if not html:
        return [], False

    soup     = BeautifulSoup(html, "html.parser")
    rows     = []
    has_more = bool(soup.find("a", string=re.compile("Next", re.I))) and page < 8

    # Method 1: JSON-LD
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            obj   = json.loads(tag.string or "")
            items = obj.get("@graph", [obj] if isinstance(obj, dict) else obj)
            if isinstance(items, dict):
                items = [items]
            for item in items:
                name = item.get("name","")
                if not name or len(name) < 4:
                    continue
                addr = item.get("address", {})
                rows.append([
                    name, "Dental Clinic",
                    item.get("telephone",""), "",
                    item.get("url",""),
                    (addr.get("streetAddress","") + " " + addr.get("addressLocality","")).strip(),
                    city["city"], city["state"],
                    item.get("aggregateRating",{}).get("ratingValue",""),
                    item.get("aggregateRating",{}).get("reviewCount",""),
                    url, now_ist(), "IndiaMart"
                ])
        except Exception:
            pass

    # Method 2: HTML cards
    if not rows:
        cards = soup.find_all("div", class_=re.compile(r"(card|listing|bname|company)", re.I))
        for card in cards:
            name_tag = card.find(["h2","h3","a"], class_=re.compile(r"(name|title|bname)", re.I))
            if not name_tag:
                continue
            name = name_tag.get_text(strip=True)
            if not name or len(name) < 4:
                continue
            phone_tag = card.find(string=re.compile(r"\d{10}"))
            addr_tag  = card.find(class_=re.compile(r"addr|address|location", re.I))
            rows.append([
                name, "Dental Clinic",
                phone_tag.strip() if phone_tag else "", "", "",
                addr_tag.get_text(strip=True) if addr_tag else city["city"],
                city["city"], city["state"],
                "", "", url, now_ist(), "IndiaMart"
            ])

    log.info(f"  IndiaMart {city['city']} p{page}: {len(rows)} records")
    return rows, has_more


# ============================================================
#  GOOGLE SHEETS — save karo
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
        ws = sheet.add_worksheet(SHEET_NAME, rows=10000, cols=13)
        ws.append_row(HEADERS_ROW)
        ws.format("A1:M1", {
            "textFormat"     : {"bold": True, "foregroundColor": {"red":1,"green":1,"blue":1}},
            "backgroundColor": {"red":0.1,"green":0.45,"blue":0.9}
        })
    return ws

def get_existing_keys(ws):
    data = ws.get_all_values()
    keys = set()
    for row in data[1:]:  # skip header
        if len(row) >= 6:
            key = (row[0] + row[5]).lower().replace(" ","")
            if key:
                keys.add(key)
    return keys

def append_rows(ws, rows):
    if rows:
        ws.append_rows(rows, value_input_option="RAW")


# ============================================================
#  MAIN
# ============================================================
def main():
    log.info("=== Dental Scraper Start ===")

    # Google Sheet connect karo
    ws       = get_sheet()
    existing = get_existing_keys(ws)
    log.info(f"Sheet mein already {len(existing)} records hain")

    # State load karo
    state      = load_state()
    city_idx   = state["city_idx"]
    source_idx = state["source_idx"]
    page       = state["page"]

    sources = ["practo", "justdial", "indiamart"]
    collected = 0
    batch     = []  # ek saath 20 rows likhne ke liye

    while collected < DAILY_LIMIT:
        # Wrap around
        if city_idx >= len(CITIES):
            city_idx   = 0
            source_idx = (source_idx + 1) % len(sources)
            page       = 1

        city   = CITIES[city_idx]
        source = sources[source_idx]

        log.info(f"[{collected}/{DAILY_LIMIT}] {source} | {city['city']} | page {page}")

        try:
            if source == "practo":
                rows, has_more = scrape_practo(city, page)
            elif source == "justdial":
                rows, has_more = scrape_justdial(city, page)
            else:
                rows, has_more = scrape_indiamart(city, page)
        except Exception as e:
            log.error(f"  ERROR: {e}")
            city_idx += 1
            page      = 1
            time.sleep(2)
            continue

        # Deduplicate karo
        for row in rows:
            if collected >= DAILY_LIMIT:
                break
            key = (str(row[0]) + str(row[5])).lower().replace(" ","")
            if not key or key in existing:
                continue
            batch.append(row)
            existing.add(key)
            collected += 1

        # Batch mein 20 rows Google Sheet mein likho
        if len(batch) >= 20:
            append_rows(ws, batch)
            log.info(f"  {len(batch)} rows sheet mein likhe")
            batch = []

        # Next page ya next city
        if has_more and collected < DAILY_LIMIT:
            page += 1
        else:
            page      = 1
            city_idx += 1

        time.sleep(random.uniform(0.8, 1.5))

    # Bache hue rows likho
    if batch:
        append_rows(ws, batch)
        log.info(f"  Final {len(batch)} rows sheet mein likhe")

    # State save karo
    save_state({"city_idx": city_idx, "source_idx": source_idx, "page": page})

    total = len(existing)
    log.info(f"=== DONE | +{collected} aaj | Total: {total} ===")


if __name__ == "__main__":
    main()
