# scripts/fetch_and_build.py  (resilient + debug prints, corrected)
# Builds data/data.json with live data and won't fail on transient errors.

import json, re, time
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True)
HIST_DIR = Path("history"); HIST_DIR.mkdir(exist_ok=True)
NOW = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
UA = {"User-Agent": "Mozilla/5.0"}

NSE_HOME   = "https://www.nseindia.com/"
NSE_URL    = "https://www.nseindia.com/market-data/exchange-traded-funds-etf/"
MIRAE_URL  = "https://miraeassetetf.co.in/inav-baskets?tab=inav"
NIPPON_URL = "https://etf.nipponindiaim.com/RealtimeNAV/nav/index"
ZERODHA_URL= "https://www.zerodhafundhouse.com/inav-summary"

def to_float(x):
    try: return float(str(x).replace("₹","").replace(",","").strip())
    except: return None

def pct(a,b):
    if a is None or b is None or b == 0: return None
    return (a/b - 1.0)*100.0

def categorize(amc, scheme):
    s = f"{amc} {scheme}".lower()
    if re.search(r"nifty\s?(50|100|200|next\s?50|midcap|sensex)", s): return "Index"
    if re.search(r"bank|psu|it|pharma|auto|metal|energy|infra|financial", s): return "Sector"
    if re.search(r"fang|hang\s?sheng|s&p|top\s?50", s): return "International"
    if re.search(r"gold|silver", s): return "Commodity"
    if re.search(r"g-?sec|sdl|liquid|1d rate", s): return "Debt"
    if re.search(r"alpha|low volatility|momentum|quality|consumption|manufacturing|dividend|equal weight|internet|ev|new age", s):
        return "Smart Beta / Thematic"
    return "Other"

# ---------- NSE (LTP/Volume) with cookie pre‑warm + retry ----------
def fetch_nse():
    tries = 2
    for attempt in range(1, tries+1):
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(args=["--disable-gpu","--no-sandbox"])
                ctx = browser.new_context(user_agent=UA["User-Agent"])
                page = ctx.new_page()
                # Pre‑warm cookies
                page.goto(NSE_HOME, wait_until="domcontentloaded", timeout=180000)
                page.wait_for_timeout(1500)
                # ETF page
                page.goto(NSE_URL, wait_until="domcontentloaded", timeout=180000)
                page.wait_for_selector("table", timeout=15000)
                html = page.content()
                browser.close()

            soup = BeautifulSoup(html, "lxml")
            table = None
            for t in soup.find_all("table"):
                ths = [th.get_text(strip=True).lower() for th in t.select("thead th")]
                if any("symbol" in x for x in ths) and any("ltp" in x for x in ths):
                    table = t; break
            if not table: raise RuntimeError("NSE table not found")

            heads = [th.get_text(strip=True).lower() for th in table.select("thead th")]
            def col_idx(name):
                for i,h in enumerate(heads):
                    if name in h: return i
                return None
            i_sym = col_idx("symbol"); i_ltp = col_idx("ltp"); i_vol = col_idx("volume")

            rows=[]
            for tr in table.select("tbody tr"):
                tds=[td.get_text(strip=True) for td in tr.find_all("td")]
                if not tds: continue
                sym = tds[i_sym] if i_sym is not None else None
                ltp = to_float(tds[i_ltp]) if i_ltp is not None else None
                vol = to_float(tds[i_vol]) if i_vol is not None else None
                if sym: rows.append({"Symbol": sym, "LTP": ltp, "Volume": int(vol) if vol else None})
            return pd.DataFrame(rows)
        except Exception as e:
            print(f"[WARN] NSE attempt {attempt}/{tries} failed: {e}")
            time.sleep(2)
    return pd.DataFrame(columns=["Symbol","LTP","Volume"])

# ---------- AMC fetchers ----------
def fetch_mirae():
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(args=["--disable-gpu","--no-sandbox"])
            pg = browser.new_page()
            pg.goto(MIRAE_URL, wait_until="domcontentloaded", timeout=180000)
            pg.wait_for_timeout(3000)
            html = pg.content()
            browser.close()
        soup = BeautifulSoup(html, "lxml")
        rows=[]
        for tr in soup.select("table tbody tr"):
            tds=[td.get_text(strip=True) for td in tr.find_all("td")]
            if len(tds) < 4: continue
            rows.append({
                "AMC":"Mirae Asset",
                "Scheme":tds[1],
                "Symbol":tds[0] or "",
                "iNAV":to_float(tds[2]),
                "Previous_NAV":to_float(tds[3]),
                "Source":"Mirae"
            })
        return pd.DataFrame(rows)
    except Exception as e:
        print(f"[WARN] Mirae fetch failed: {e}")
        return pd.DataFrame(columns=["AMC","Scheme","Symbol","iNAV","Previous_NAV","Source"])

def fetch_nippon():
    try:
        r=requests.get(NIPPON_URL, headers=UA, timeout=60)
        soup=BeautifulSoup(r.text, "lxml")
        rows=[]
        for tr in soup.select("table tbody tr"):
            tds=[td.get_text(strip=True) for td in tr.find_all("td")]
            if len(tds) < 3: continue
            rows.append({
                "AMC":"Nippon India Mutual Fund",
                "Scheme":tds[0],
                "Symbol":"",
                "iNAV":to_float(tds[1]),
                "Previous_NAV":to_float(tds[2]),
                "Source":"Nippon"
            })
        return pd.DataFrame(rows)
    except Exception as e:
        print(f"[WARN] Nippon fetch failed: {e}")
        return pd.DataFrame(columns=["AMC","Scheme","Symbol","iNAV","Previous_NAV","Source"])

def fetch_zerodha():
    try:
        r=requests.get(ZERODHA_URL, headers=UA, timeout=60)
        soup=BeautifulSoup(r.text, "lxml")
        rows=[]
        for tr in soup.select("table tbody tr"):
            tds=[td.get_text(strip=True) for td in tr.find_all("td")]
            if len(tds) < 3: continue
            rows.append({
                "AMC":"Zerodha Fund House",
                "Scheme":tds[0],
                "Symbol":"",
                "iNAV":to_float(tds[1]),
                "Previous_NAV":to_float(tds[2]),
                "Source":"Zerodha"
            })
        return pd.DataFrame(rows)
    except Exception as e:
        print(f"[WARN] Zerodha fetch failed: {e}")
        return pd.DataFrame(columns=["AMC","Scheme","Symbol","iNAV","Previous_NAV","Source"])

# ---------- Build dataset ----------
def build_dataset():
    nse    = fetch_nse()
    print(f"[COUNT] NSE rows: {len(nse)}")
    mirae  = fetch_mirae()
    print(f"[COUNT] Mirae rows: {len(mirae)}")
    nippon = fetch_nippon()
    print(f"[COUNT] Nippon rows: {len(nippon)}")
    zero   = fetch_zerodha()
    print(f"[COUNT] Zerodha rows: {len(zero)}")

    amc = pd.concat([mirae, nippon, zero], ignore_index=True)

    # If every AMC source is empty, keep site alive with previous data
    if amc.empty and (DATA_DIR/"data.json").exists():
        print("[FALLBACK] All AMC sources empty — using previous data.json")
        try:
            with open(DATA_DIR/"data.json","r",encoding="utf-8") as f:
                prev = json.load(f)
            amc = pd.DataFrame(prev)[["AMC","Category","Scheme","Symbol","iNAV","Previous_NAV","Source"]]
        except Exception as e:
            print(f"[WARN] fallback read failed: {e}")
            amc = pd.DataFrame(columns=["AMC","Scheme","Symbol","iNAV","Previous_NAV","Source"])
            amc["Category"]=""

    if "Category" not in amc.columns:
        amc["Category"] = amc.apply(lambda r: categorize(r.get("AMC",""), r.get("Scheme","")), axis=1)
    amc["Symbol"] = amc["Symbol"].fillna("")
    amc["PremDiscPct"] = amc.apply(lambda r: pct(r["iNAV"], r["Previous_NAV"]), axis=1)

    if not nse.empty:
        merged = amc.merge(nse, how="left", on="Symbol")
    else:
        merged = amc.copy(); merged["LTP"]=None; merged["Volume"]=None

    merged["PremLTPvsINAV"] = merged.apply(lambda r: pct(r["LTP"], r["iNAV"]), axis=1)
    merged["Timestamp"] = NOW

    # History for sparklines
    snap = {"timestamp": NOW, "records": merged[["Symbol","Scheme","iNAV","AMC"]].to_dict(orient="records")}
    hist = HIST_DIR / f"snapshot_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
    with open(hist,"w",encoding="utf-8") as f:
        json.dump(snap,f,ensure_ascii=False)

    # Build compact spark arrays
    spark_map={}
    snaps = sorted(HIST_DIR.glob("snapshot_*.json"))[-10:]
    for fp in snaps:
        try:
            j=json.loads(fp.read_text(encoding="utf-8"))
            for r in j.get("records",[]):
                key=r.get("Symbol") or r.get("Scheme")
                if key: spark_map.setdefault(key,[]).append(r.get("iNAV"))
        except: pass
    merged["iNAV_hist"] = merged.apply(lambda r: spark_map.get(r["Symbol"] or r["Scheme"], [r["iNAV"]]), axis=1)

    cols=["AMC","Category","Scheme","Symbol","iNAV","Previous_NAV","PremDiscPct",
          "LTP","Volume","PremLTPvsINAV","iNAV_hist","Timestamp","Source"]
    final = merged[cols].sort_values(["AMC","Category","Scheme"]).fillna("")
    with open(DATA_DIR/"data.json","w",encoding="utf-8") as f:
        json.dump(final.to_dict(orient="records"), f, ensure_ascii=False)

    print(f"[OK] Built data/data.json with {len(final)} rows")

if __name__ == "__main__":
    build_dataset()
