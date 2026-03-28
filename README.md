# India Dental Clinic Scraper
**Sources:** Practo + JustDial + IndiaMart  
**Schedule:** Daily 9 AM IST via GitHub Actions  
**Output:** Google Sheets  
**Expected:** 100-200 records/day, no API key needed

---

## SETUP — Ek baar karna hai, phir automatic

### STEP 1 — Google Sheet banao

1. **sheets.google.com** pe jaao → New Sheet banao
2. Sheet ka naam rakho: `Dental Clinics` (kuch bhi rakh sakte ho)
3. URL se **Sheet ID** copy karo:
   ```
   https://docs.google.com/spreadsheets/d/YAHAN_WALA_COPY_KARO/edit
   ```

---

### STEP 2 — Google Service Account banao (free)

1. **console.cloud.google.com** pe jaao
2. New Project banao → naam kuch bhi rakho
3. Left menu → **APIs & Services** → **Enable APIs**
4. Search karo **"Google Sheets API"** → Enable karo
5. Left menu → **APIs & Services** → **Credentials**
6. **"+ Create Credentials"** → **Service Account**
7. Naam do → Create karo
8. Service account pe click karo → **Keys** tab → **Add Key** → JSON
9. JSON file download hogi — yeh file sambhal ke rakho

---

### STEP 3 — Sheet mein Service Account ko access do

1. Downloaded JSON file kholо — `client_email` copy karo
   ```
   "client_email": "xyz@project.iam.gserviceaccount.com"
   ```
2. Apni Google Sheet kholо → **Share** button
3. Woh email paste karo → **Editor** permission do → Share

---

### STEP 4 — GitHub Repo banao

1. **github.com** → **New Repository**
2. Naam: `dental-scraper` (private rakho)
3. Yeh 3 files upload karo:
   ```
   scraper.py
   requirements.txt
   .github/workflows/scraper.yml
   ```

---

### STEP 5 — GitHub Secrets add karo

Repo mein jaao → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**

**Secret 1:**
```
Name:  GOOGLE_CREDENTIALS
Value: (poora JSON file ka content paste karo — { se } tak)
```

**Secret 2:**
```
Name:  SHEET_ID
Value: (Step 1 mein copy kiya tha woh ID)
```

---

### STEP 6 — Test run karo

1. Repo mein jaao → **Actions** tab
2. **"Daily Dental Scraper"** → **"Run workflow"** → **Run**
3. 2-3 minute baad log dekho
4. Google Sheet mein data check karo ✅

---

## File Structure

```
dental-scraper/
├── scraper.py                    # Main scraper
├── requirements.txt              # Python packages
└── .github/
    └── workflows/
        └── scraper.yml           # GitHub Actions schedule
```

---

## Expected Output

| Din | Records |
|-----|---------|
| Day 1 | ~150 |
| Day 7 | ~1,000 |
| Day 30 | ~4,000-5,000 |

---

## Sheet Columns

| Column | Data |
|--------|------|
| A | Clinic Name |
| B | Category |
| C | Phone |
| D | Email |
| E | Website |
| F | Address |
| G | City |
| H | State |
| I | Rating |
| J | Reviews |
| K | Source URL |
| L | Fetched On |
| M | Source |

---

## Troubleshooting

**0 records aa rahe hain:**
- Actions → Log dekho — kya error hai
- GOOGLE_CREDENTIALS sahi hai? JSON format valid hai?
- Sheet ID sahi copy kiya?

**403 errors:**
- Normal hai — scraper automatically skip karta hai
- JustDial kabhi kabhi block karta hai — Practo/IndiaMart se data aata rehega

**Sheet mein data nahi dikh raha:**
- Service account ko Sheet ka Editor access diya?
- Sheet ID sahi hai?# Data-scraper
