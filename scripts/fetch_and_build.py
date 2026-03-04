# scripts/fetch_and_build.py
# Builds data/data.json with full live data:
# - NSE ETF Market Watch → LTP & Volume (true trading premium vs iNAV)
# - AMCs (Mirae, Nippon, Zerodha) → iNAV & previous-day NAV
# - Computes: iNAV vs Prev NAV %, (LTP − iNAV)/iNAV %, categories
# - Saves sparkline history in history/

import json
from pathlib import Path
from datetime import datetime, timezone
import time
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# ---------------------------- Setup & Paths ----------------------------
DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True)
HIST_DIR = Path("history"); HIST_DIR.mkdir(exist_ok=True)

NOW = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
UA = {"User-Agent": "Mozilla/5.0"}

# Sources (public pages)
NSE_URL      = "https://www.nseindia.com/market-data/exchange-traded-funds-etf/"
MIRAE_URL    = "https://miraeassetetf.co.in/inav-baskets?tab=inav"
NIPPON_URL   = "https://etf.nipponindiaim.com/RealtimeNAV/nav/index"
ZERODHA_URL  = "https://www.zerodhafundhouse.com/inav-summary"

# ---------------------------- Helpers ----------------------------
def categorize(amc: str, scheme: str) -> str:
    s = f"{amc} {scheme}".lower()
    if re.search(r"nifty\s?(50|100|200|next\s?50|midcap|sensex)", s): return "Index"
    if re.search(r"bank|psu|it|pharma|auto|metal|energy|infra|financial", s): return "Sector"
    if re.search(r"fang|\bhang\s?sheng\b|s&p|top\s?50", s): return "International"
    if re.search(r"gold|silver", s): return "Commodity"
    if re.search(r"g-?sec|sdl|liquid|1d rate", s): return "Debt"
    if re.search(r"alpha|low volatility|momentum|quality|consumption|manufacturing|dividend|equal weight|internet|ev|new age", s): 
        return "Smart Beta / Thematic"
    return "Other"

def pct(a, b):
    if a is None or b is None or b == 0: return None
    return (a/b - 1.0) * 100.0

def to_float(x):
    if x is None: return None
    try:
        return float(str(x).replace("₹", "").replace(",", "").strip())
    except:
        return None

# ---------------------------- Fetchers ----------------------------
def fetch_nse_ltp_volume():
    """Use Playwright (Chromium) to render NSE ETF table and extract Symbol, LTP, Volume."""
    rows = []
    with sync_playwright() as p:
        browser = p.chromium.launch(args=["--disable-gpu", "--no-sandbox"])
        context = browser.new_context(user_agent=UA["User-Agent"])
        page = context.new_page()
        page.goto(NSE_URL, wait_until="networkidle", timeout=120_000)
        page.wait_for_timeout(4000)  # allow dynamic table to render
        html = page.content()
        browser.close()
    soup = BeautifulSoup(html, "lxml")

    table = None
    for t in soup.find_all("table"):
        ths = [th.get_text(strip=True).lower() for th in t.select("thead th")]
        if any("symbol" in x for x in ths) and any("ltp" in x for x in ths):
            table = t; break

    if not table:
        return pd.DataFrame(columns=["Symbol", "LTP", "Volume"])

    heads = [th.get_text(strip=True).lower() for th in table.select("thead th")]
    def col_idx(name): 
        for i, h in enumerate(heads):
            if name in h: return i
        return None

    i_sym = col_idx("symbol")
    i_ltp = col_idx("ltp")
    i_vol = col_idx("volume")

    for tr in table.select("tbody tr"):
        tds = [td.get_text(strip=True) for td in tr.find_all("td")]
        if not tds or i_sym is None: 
            continue
        symbol = tds[i_sym] if i_sym < len(tds) else None
        ltp    = to_float(tds[i_ltp]) if i_ltp is not None and i_ltp < len(tds) else None
        vol    = to_float(tds[i_vol]) if i_vol is not None and i_vol < len(tds) else None
        if symbol:
            rows.append({"Symbol": symbol, "LTP": ltp, "Volume": int(vol) if vol is not None else None})
    return pd.DataFrame(rows)

def fetch_mirae():
    """Mirae AMC iNAV & previous NAV table (dynamic)."""
    rows = []
    with sync_playwright() as p:
        browser = p.chromium.launch(args=["--disable-gpu", "--no-sandbox"])
        page = browser.new_page()
        page.goto(MIRAE_URL, wait_until="networkidle", timeout=120_000)
        page.wait_for_timeout(4000)
        html = page.content()
        browser.close()
    soup = BeautifulSoup(html, "lxml")
    for tr in soup.select("table tbody tr"):
        tds = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(tds) < 4:
            continue
        symbol = tds[0] or None
        scheme = tds[1]
        inav   = to_float(tds[2])
        nav    = to_float(tds[3])
        rows.append({"AMC": "Mirae Asset", "Scheme": scheme, "Symbol": symbol, "iNAV": inav, "Previous_NAV": nav, "Source": "Mirae"})
    return pd.DataFrame(rows)

def fetch_nippon():
    """Nippon India ETF iNAV page."""
    r = requests.get(NIPPON_URL, headers=UA, timeout=60)
    soup = BeautifulSoup(r.text, "lxml")
    rows = []
    for tr in soup.select("table tbody tr"):
        tds = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(tds) < 3:
            continue
        scheme = tds[0]
        inav   = to_float(tds[1])
        nav    = to_float(tds[2])
        rows.append({"AMC": "Nippon India Mutual Fund", "Scheme": scheme, "Symbol": None, "iNAV": inav, "Previous_NAV": nav, "Source": "Nippon"})
    return pd.DataFrame(rows)

def fetch_zerodha():
    """Zerodha Fund House iNAV summary."""
    r = requests.get(ZERODHA_URL, headers=UA, timeout=60)
    soup = BeautifulSoup(r.text, "lxml")
    rows = []
    for tr in soup.select("table tbody tr"):
        tds = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(tds) < 3:
            continue
        scheme = tds[0]
        inav   = to_float(tds[1])
        nav    = to_float(tds[2])
        rows.append({"AMC": "Zerodha Fund House", "Scheme": scheme, "Symbol": None, "iNAV": inav, "Previous_NAV": nav, "Source": "Zerodha"})
    return pd.DataFrame(rows)

# ---------------------------- Build Dataset ----------------------------
def build_dataset():
    # 1) Fetch from each source
    nse    = fetch_nse_ltp_volume()         # Symbol, LTP, Volume  (NSE)           [1](https://miraeassetetf.co.in/inav-baskets?tab=inav)
    mirae  = fetch_mirae()                  # iNAV, Previous_NAV    (Mirae AMC)     [2](blob:https://www.microsoft365.com/c207a0d6-cced-4edc-9027-d7b4aa0b0995)
    nippon = fetch_nippon()                 # iNAV, Previous_NAV    (Nippon AMC)    [3](https://www.morningstar.in/funds/mirae.aspx)
    zero   = fetch_zerodha()                # iNAV, Previous_NAV    (Zerodha AMC)   [4](https://www.etmoney.com/mutual-funds/mirae-asset-nifty-india-internet-etf/45600)

    amc = pd.concat([mirae, nippon, zero], ignore_index=True)
    # 2) Compute iNAV vs Previous NAV %
    amc["PremDiscPct"] = amc.apply(lambda r: pct(r["iNAV"], r["Previous_NAV"]), axis=1)

    # 3) Join with NSE by Symbol (best-effort)
    amc["Symbol"] = amc["Symbol"].fillna("")
    merged = amc.merge(nse, how="left", on="Symbol")

    # 4) Compute true trading premium/discount vs iNAV using LTP
    merged["PremLTPvsINAV"] = merged.apply(lambda r: pct(r["LTP"], r["iNAV"]), axis=1)
    merged["Category"] = merged.apply(lambda r: categorize(r["AMC"], r["Scheme"]), axis=1)
    merged["Timestamp"] = NOW

    # 5) Sparkline history: keep iNAV snapshots for last ~10 runs
    snap = {
        "timestamp": NOW,
        "records": merged[["Symbol", "Scheme", "iNAV", "AMC"]].to_dict(orient="records")
    }
    HIST_DIR.mkdir(exist_ok=True)
    hist_file = HIST_DIR / f"snapshot_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
    with open(hist_file, "w", encoding="utf-8") as f:
        json.dump(snap, f)

    # Build small spark arrays
    spark_map = {}
    snaps = sorted(HIST_DIR.glob("snapshot_*.json"))[-10:]
    for fp in snaps:
        j = json.loads(fp.read_text(encoding="utf-8"))
        for r in j["records"]:
            key = r.get("Symbol") or r.get("Scheme")
            if not key: 
                continue
            spark_map.setdefault(key, []).append(r.get("iNAV"))

    merged["iNAV_hist"] = merged.apply(lambda r: spark_map.get(r["Symbol"] or r["Scheme"], [r["iNAV"]]), axis=1)

    # 6) Output
    cols = ["AMC","Category","Scheme","Symbol","iNAV","Previous_NAV","PremDiscPct",
            "LTP","Volume","PremLTPvsINAV","iNAV_hist","Timestamp","Source"]
    final = merged[cols].sort_values(["AMC","Category","Scheme"]).fillna("")
    DATA_DIR.mkdir(exist_ok=True)
    with open(DATA_DIR/"data.json", "w", encoding="utf-8") as f:
        json.dump(final.to_dict(orient="records"), f, ensure_ascii=False)

    print(f"Built {DATA_DIR/'data.json'} with {len(final)} rows")

if __name__ == "__main__":
    build_dataset()
